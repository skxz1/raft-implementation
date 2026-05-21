# Raft consensus, with Pre-Vote

A from-scratch implementation of the [Raft consensus algorithm](https://raft.github.io/) in Python, fronted by a replicated key-value store. The cluster keeps a consistent view of the data across five nodes despite message loss, reordering, delays, and network partitions. Built as coursework for a distributed systems module and extended with the Pre-Vote optimisation from Ongaro's PhD thesis.

## What it does

Five nodes form a Raft cluster behind a TCP message router. Clients issue `PUT`, `GET`, and `DELETE` requests; the leader appends each write to its log, replicates it to a majority of followers, and only then applies it to the key-value store and replies to the client. The implementation handles the full set of failure modes Raft is designed for:

- Leader election with randomised timeouts
- Log replication with `prevLogIndex` / `prevLogTerm` consistency checks and follower backtracking when logs diverge
- Commitment by majority, restricted to entries from the current term (the Figure 8 case)
- Linearisable reads on the leader via a heartbeat-quorum confirmation before the value is returned
- Idempotent client requests through `(client_id, request_id)` deduplication and cached responses
- Persistent state (`current_term`, `voted_for`, log) survives crashes
- Log compaction via periodic snapshots once the log passes a threshold
- The **Pre-Vote** extension: an isolated node cannot inflate its term and force a real leader to step down on rejoin

## Pre-Vote, briefly

Standard Raft has a subtle availability bug: a node that gets partitioned from the cluster will keep timing out and incrementing its term while isolated. When the partition heals, it returns with a much higher term than the rest of the cluster. The current leader sees the higher term, steps down, and the cluster has to run a fresh election — even though nothing was actually wrong.

Pre-Vote fixes this with a cheap probe round before a real election. On its election timeout, a candidate first broadcasts a `RequestVote` with a `pre_vote=True` flag and the *hypothetical* next term. Peers grant the pre-vote only if they would also vote for the candidate in a real election *and* they haven't heard from a healthy leader recently. Crucially, neither the candidate nor the peers update any persistent state during this probe. Only if a majority grants the pre-vote does the candidate increment its term and start a real election.

An isolated node now sees its pre-vote probes ignored (its peers can still hear the leader) and never inflates its term. When the partition heals, the cluster carries on without a leadership change.

This is the kind of optimisation that matters more in production than in textbooks: the algorithm is still "correct" without it, but availability degrades every time a flaky link blips.

## Architecture

```
                                ┌─────────────┐
                          ┌─────┤   Node 0    │
                          │     └─────────────┘
   ┌─────────┐            │     ┌─────────────┐
   │ Client  │◄──┐        ├─────┤   Node 1    │
   └─────────┘   │        │     └─────────────┘
                 │        │     ┌─────────────┐
   ┌─────────┐   ├──►┌────┴──┐──┤   Node 2    │
   │ Client  │◄──┤   │  Net  │  └─────────────┘
   └─────────┘   │   └────┬──┘  ┌─────────────┐
                 │        ├─────┤   Node 3    │
   ┌─────────┐   │        │     └─────────────┘
   │ Client  │◄──┘        │     ┌─────────────┐
   └─────────┘            └─────┤   Node 4    │
                                └─────────────┘
```

Every node and client connects to a central network process over TCP. The network process routes length-prefixed JSON messages by `dst` field — point-to-point, broadcast to all nodes, or broadcast to leader. Nodes never talk to each other directly.

The network process is swappable. There's a perfect variant for sanity-checking correctness, and progressively more adversarial variants that introduce variable delays, message loss, partitions, and finally all three at once. Running the test suite against each variant in turn is how the implementation was hardened.

| Layer | File | What's in it |
|---|---|---|
| Node logic | `node.py` | Election, replication, state machine, snapshots, persistence |
| Wire protocol | `message.py` | Length-prefixed JSON over TCP, message constructors |
| Network simulators | `network*.py` | Perfect / delayed / lossy / partitioned / chaos variants |
| Cluster launcher | `run_cluster.py` | Spawns the network process and five nodes |
| Test client | `client_test.py` | Automated correctness and stress tests |
| Interactive client | `client_interactive.py` | Manual REPL for debugging |
| Extension tests | `test_extension.py` | Pre-Vote–specific tests |

## Running it

Requires Python 3.10+. No external dependencies.

```bash
cd "Raft Implementation/CS3524Assignment"

# Start the cluster (perfect network) — leaves 5 nodes + the router running
python run_cluster.py

# In another terminal, drive it interactively
python client_interactive.py
```

Sample session:

```
[client-a1b2c3] > PUT user:42 sam
OK (45ms)
[client-a1b2c3] > GET user:42
user:42 = sam (38ms)
[client-a1b2c3] > DELETE user:42
DELETED (47ms)
[client-a1b2c3] > GET user:42
NOT FOUND (35ms)
```

Switch in a harder network at launch time:

```bash
python run_cluster.py network_partition.py    # periodic 2-vs-3 partitions
python run_cluster.py network_chaos.py        # drops + delays + partitions
```

Run the automated tests:

```bash
python client_test.py                  # all correctness levels
python client_test.py stress 10 200    # 10 concurrent clients × 200 ops each
python test_extension.py               # Pre-Vote-specific tests
```

The stress test reports throughput, p50/p95/p99 latency, and verifies a sample of keys after the storm settles.

## Design notes

A few things worth pulling out for anyone reading the code.

**Linearisable reads.** A naive `GET` served from the leader's local key-value store can return stale data if a new leader has been elected in another partition and committed a write. The implementation handles this by treating a `GET` like a tiny replication round: the leader broadcasts a heartbeat tagged with a read ID, waits for a majority of followers to acknowledge it (confirming this node is still leader), and only then reads from the local store. It's the ReadIndex pattern from §6.4 of Ongaro's thesis.

**Deduplication.** Clients retry on timeout. Without dedup, a retried `PUT` could be applied twice. Each request carries a `(client_id, request_id)` pair; the leader caches the response for the most recent request from each client and replays the cached response for any retry. The cache is bounded — only the latest request per client is kept.

**Persistence.** `current_term`, `voted_for`, and the log are flushed to disk on every change that the Raft paper marks as "before responding to RPCs". Snapshots are taken whenever the log grows past a threshold, and the prefix of the log up to the snapshot index is discarded. On restart, a node loads its snapshot first, then replays the remaining log entries.

**Commitment rule.** A leader will not advance `commit_index` past an entry from a previous term, even if that entry is now replicated on a majority. This avoids the Figure 8 anomaly where a write could appear committed and then be overwritten by a new leader. Commitment of older-term entries happens implicitly when an entry from the current term commits.

## What I'd add with more time

- **Membership changes.** The current implementation has a fixed five-node cluster. Joint consensus (§4 of the thesis) would let the cluster grow or shrink safely.
- **Batch and pipeline AppendEntries.** Right now each client write triggers its own replication round. Batching writes into a single AppendEntries when load is high would substantially improve throughput.
- **Leadership transfer.** A clean handoff (§3.10) would let an operator drain a node for maintenance without an election storm.
- **Witness / learner nodes.** Non-voting replicas for read scaling.

## References

- Ongaro, D. and Ousterhout, J. (2014). *In Search of an Understandable Consensus Algorithm*. USENIX ATC.
- Ongaro, D. (2014). *Consensus: Bridging Theory and Practice*. PhD thesis, Stanford University. (Chapter 4 is the Pre-Vote section.)
- Howard, H. (2014). *ARC: Analysis of Raft Consensus*. University of Cambridge Technical Report.


