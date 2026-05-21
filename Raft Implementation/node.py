import socket
import threading
import time
import random
import json
import os
import sys

from message import (
    send_message, recv_message, make_register, make_client_response, make_request_vote,
    make_vote_response, make_append_entries, make_append_entries_response,
    MSG_REGISTER_ACK, MSG_APPEND_ENTRIES, MSG_APPEND_ENTRIES_RESPONSE,
    MSG_REQUEST_VOTE, MSG_REQUEST_VOTE_RESPONSE,
    MSG_CLIENT_REQUEST,
)
from config import (
    NETWORK_HOST, NETWORK_PORT, CLUSTER_SIZE, NODE_IDS,
    HEARTBEAT_INTERVAL, ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX,
    SNAPSHOT_THRESHOLD, DATA_DIR,
)


# === Raft Roles ===
FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"


class RaftNode:
    """
    A single Raft node that connects to the network.

    Architecture:
        - Connects to the central network via TCP
        - Receives messages via _receive_loop
        - Election timeouts and heartbeats driven by _timer_loop
        - You implement the Raft logic in the TODO methods below

    State (all initialised for you):
        Raft persistent state:
            current_term, voted_for, log

        Raft volatile state:
            commit_index, last_applied, role, leader_id

        Leader-only state:
            next_index, match_index, votes_received

        Application state:
            kv_store
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.sock = None
        self.lock = threading.Lock()

        # === Raft Persistent State ===
        self.current_term = 0
        self.voted_for = None
        self.log = []

        # === Raft Volatile State ===
        self.commit_index = 0
        self.last_applied = 0
        self.role = FOLLOWER
        self.leader_id = None

        # === Leader-Only State ===
        self.next_index = {}
        self.match_index = {}
        self.votes_received = set()

        # === Election Timing ===
        self.last_heartbeat_time = time.time()
        self.election_timeout = self._random_election_timeout()

        # === Application State Machine ===
        self.kv_store = {}

        # Client Request Deduplication
        self.client_responses = {}
        self.pending_requests = set()
        self.unacked_responses = {}

        # Snapshot State
        self.snapshot = {
            "last_included_index": 0,
            "last_included_term": 0,
            "kv_store": {}
        }

        # === Linearizable read tracking (Part 4) ===
        # These are volatile — initialised before load_state so they exist
        # for the lifetime of the node. They are not persisted.
        self.pending_reads = {}
        self.read_seq = 0

        # === Pre-Vote state (Part 5) ===
        # Tracks which peers granted a pre-vote in the current probe round.
        # pre_vote_term holds the hypothetical term (current_term + 1) we sent
        # in our last pre-vote broadcast so stale responses can be discarded.
        self.pre_votes_received = set()
        self.pre_vote_term = 0

        # === Load Persisted State ===
        self.load_state()

    def _random_election_timeout(self):
        """Generate a random election timeout."""
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    # CONNECTION & MESSAGE HANDLING (do not modify)

    def start(self):
        """Connect to the network and start the node."""
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((NETWORK_HOST, NETWORK_PORT))
        self.sock.settimeout(0.1)

        # Register with the network
        send_message(self.sock, make_register(self.node_id, "node"))
        ack = recv_message(self.sock)
        if not ack or ack.get("type") != MSG_REGISTER_ACK:
            print(f"[{self.node_id}] Registration failed")
            return

        print(f"[{self.node_id}] Registered with network as {self.role}")

        # Start background threads
        threading.Thread(target=self._receive_loop, daemon=True).start()
        threading.Thread(target=self._timer_loop, daemon=True).start()

        # Keep main thread alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{self.node_id}] Shutting down")
            self.sock.close()

    def _receive_loop(self):
        """Continuously receive and dispatch messages from the network."""
        while True:
            try:
                msg = recv_message(self.sock)
                if msg is None:
                    print(f"[{self.node_id}] Connection to network lost")
                    break
                self._dispatch(msg)
            except socket.timeout:
                continue
            except (ConnectionResetError, BrokenPipeError, OSError):
                print(f"[{self.node_id}] Connection error")
                break

    def _dispatch(self, msg):
        """Route incoming messages to the appropriate handler."""
        msg_type = msg.get("type")

        if msg_type == MSG_APPEND_ENTRIES:
            self.handle_append_entries(msg)
        elif msg_type == MSG_APPEND_ENTRIES_RESPONSE:
            self.handle_append_entries_response(msg)
        elif msg_type == MSG_REQUEST_VOTE:
            self.handle_request_vote(msg)
        elif msg_type == MSG_REQUEST_VOTE_RESPONSE:
            self.handle_request_vote_response(msg)
        elif msg_type == MSG_CLIENT_REQUEST:
            self.handle_client_request(msg)
        # Ignore unknown message types silently

    def _send(self, msg):
        """
        Send a message through the network.

        All messages go through the central network, which routes them
        based on the 'dst' field.
        """
        try:
            send_message(self.sock, msg)
        except (BrokenPipeError, OSError):
            pass

    # TIMER LOOP (do not modify)

    def _timer_loop(self):
        """
        Periodic timer that drives election timeouts and heartbeats.

        Runs every 100ms. The lock is held before calling start_election()
        or send_heartbeats().
        """
        while True:
            time.sleep(0.1)

            with self.lock:
                now = time.time()
                elapsed = now - self.last_heartbeat_time

                if self.role == LEADER:
                    if elapsed >= HEARTBEAT_INTERVAL:
                        self.send_heartbeats()
                        self.last_heartbeat_time = now
                else:
                    if elapsed >= self.election_timeout:
                        self.start_election()
                        self.last_heartbeat_time = now
                        self.election_timeout = self._random_election_timeout()

    # RAFT LEADER ELECTION

    def start_election(self):
        """
        Pre-Vote Phase (Part 5 extension) followed by real Raft election.

        Instead of immediately incrementing the term and becoming a CANDIDATE,
        we first run a lightweight probe round.  We send REQUEST_VOTE messages
        with pre_vote=True and the *hypothetical* next term (current_term + 1).
        Peers respond without updating their own state — no persistent
        side-effects.  Only when a majority grants the pre-vote do we proceed
        to the real election inside handle_request_vote_response().

        This prevents term inflation from isolated nodes: a node behind a
        partition keeps firing timeouts, but peers still receiving heartbeats
        from the real leader will reject every pre-vote probe, so the isolated
        node's term never rises and it cannot disrupt the cluster on rejoin.

        The lock IS already held when _timer_loop calls this method.
        """
        hypothetical_term = self.current_term + 1

        # Start a fresh pre-vote round on every election timeout.
        # This allows retries on lossy networks or after temporary partitions.
        self.pre_vote_term = hypothetical_term
        self.pre_votes_received = {self.node_id}

        last_log_index = self._get_last_log_index()
        last_log_term = self._get_last_log_term()

        for n_node in NODE_IDS:
            if n_node != self.node_id:
                self._send(
                    make_request_vote(
                        self.node_id,
                        n_node,
                        hypothetical_term,
                        last_log_index,
                        last_log_term,
                        pre_vote=True,
                    )
                )

    def handle_request_vote(self, msg):
        """
        Handle REQUEST_VOTE from another node.

        This handler serves two purposes depending on the pre_vote flag:

        Pre-vote probe (pre_vote=True):
            The sender wants to know whether we *would* vote for it, without
            either side actually committing to the new term.  We grant the
            probe only if:
              - The hypothetical term >= our current term (not stale).
              - We have NOT heard from a live leader recently (still within our
                election timeout window), otherwise there is no need for an
                election.
              - The sender's log is at least as up-to-date as ours.
            Crucially we do NOT update current_term, voted_for, or any other
            persistent state.

        Real vote (pre_vote=False / absent):
            Standard Raft RequestVote handling — step down on higher term,
            grant vote if we haven't voted yet and the log is up-to-date.
        """
        with self.lock:
            is_pre_vote = msg.get("pre_vote", False)

            # ── Pre-vote probe path ──────────────────────────────────────────
            if is_pre_vote:
                candidate_term = msg["term"]       # hypothetical term (cur + 1)
                candidate_last_index = msg["last_log_index"]
                candidate_last_term = msg["last_log_term"]

                # Rule 1: hypothetical term must not be stale.
                if candidate_term < self.current_term:
                    self._send(
                        make_vote_response(
                            self.node_id, msg["src"], self.current_term,
                            False, pre_vote=True
                        )
                    )
                    return

                # Rule 2: reject if we are still receiving heartbeats from a
                # live leader — no election is needed.
                time_since_hb = time.time() - self.last_heartbeat_time
                leader_alive = (
                    self.role == FOLLOWER
                    and self.leader_id is not None
                    and time_since_hb < self.election_timeout
                )
                if leader_alive:
                    self._send(
                        make_vote_response(
                            self.node_id, msg["src"], self.current_term,
                            False, pre_vote=True
                        )
                    )
                    return

                # Rule 3: log up-to-date check (identical to real RequestVote).
                my_last_term = self._get_last_log_term()
                my_last_index = self._get_last_log_index()
                log_ok = (
                    candidate_last_term > my_last_term
                    or (candidate_last_term == my_last_term
                        and candidate_last_index >= my_last_index)
                )

                self._send(
                    make_vote_response(
                        self.node_id, msg["src"], self.current_term,
                        log_ok, pre_vote=True
                    )
                )
                return

            # ── Real vote path (standard Raft) ───────────────────────────────

            # Reject immediately if candidate term is stale
            if msg["term"] < self.current_term:
                self._send(
                    make_vote_response(
                        self.node_id,
                        msg["src"],
                        self.current_term,
                        False
                    )
                )
                return

            # If candidate has higher term --> step down
            if msg["term"] > self.current_term:
                self._step_down(msg["term"])

            # Can only vote once per term, unless it's the same candidate
            can_vote = (self.voted_for is None or self.voted_for == msg["src"])

            # Candidate log must be at least as up to date as mine
            candidate_last_term = msg["last_log_term"]
            candidate_last_index = msg["last_log_index"]
            my_last_term = self._get_last_log_term()
            my_last_index = self._get_last_log_index()

            log_ok = (
                candidate_last_term > my_last_term or
                (
                    candidate_last_term == my_last_term and
                    candidate_last_index >= my_last_index
                )
            )

            if can_vote and log_ok:
                self.last_heartbeat_time = time.time()
                self.election_timeout = self._random_election_timeout()
                self.voted_for = msg["src"]
                self.save_state()
                self._send(
                    make_vote_response(
                        self.node_id,
                        msg["src"],
                        self.current_term,
                        True
                    )
                )
            else:
                self._send(
                    make_vote_response(
                        self.node_id,
                        msg["src"],
                        self.current_term,
                        False
                    )
                )

    def handle_request_vote_response(self, msg):
        """
        Handle REQUEST_VOTE_RESPONSE — covers both pre-vote probes and real votes.

        Pre-vote response (pre_vote=True):
            Count granted pre-votes.  If we reach a majority, transition to a
            real Raft election: increment term, become CANDIDATE, vote for self,
            and broadcast real REQUEST_VOTE messages.  No persistent state is
            changed until we actually win the pre-vote quorum.

            If the response carries a term higher than ours we step down — our
            state is stale regardless of the pre-vote outcome.

        Real vote response (pre_vote=False / absent):
            Standard Raft: count votes, become LEADER on majority.
        """
        with self.lock:
            is_pre_vote = msg.get("pre_vote", False)

            # ── Pre-vote response path ───────────────────────────────────────
            if is_pre_vote:
                # A higher term means we are behind — step down and reset.
                if msg["term"] > self.current_term:
                    self._step_down(msg["term"])
                    self.pre_votes_received.clear()
                    self.pre_vote_term = 0
                    return

                # Discard stale responses (from a previous probe round).
                # The responder echoes its own current_term, which should be
                # <= pre_vote_term - 1 for a valid in-flight response.
                if msg["term"] > self.pre_vote_term:
                    return

                if not msg.get("vote_granted"):
                    return

                self.pre_votes_received.add(msg["src"])

                majority = (CLUSTER_SIZE // 2) + 1
                if len(self.pre_votes_received) >= majority:
                    # Pre-vote quorum reached — start the real election.
                    self.pre_votes_received.clear()
                    self.pre_vote_term = 0

                    self.current_term += 1
                    self.role = CANDIDATE
                    self.leader_id = None
                    self.voted_for = self.node_id
                    self.votes_received.clear()
                    self.votes_received.add(self.node_id)
                    self.save_state()

                    last_log_index = self._get_last_log_index()
                    last_log_term = self._get_last_log_term()

                    for n_node in NODE_IDS:
                        if n_node != self.node_id:
                            self._send(
                                make_request_vote(
                                    self.node_id,
                                    n_node,
                                    self.current_term,
                                    last_log_index,
                                    last_log_term,
                                    pre_vote=False,
                                )
                            )
                return

            # ── Real vote response path (standard Raft) ──────────────────────
            if self.role != CANDIDATE:
                return

            # Ignore stale responses
            if msg["term"] < self.current_term:
                return

            # If response has higher term --> step down
            if msg["term"] > self.current_term:
                self._step_down(msg["term"])
                self.votes_received.clear()
                self.save_state()
                return

            # Count granted votes in current term
            if msg["term"] == self.current_term and msg["vote_granted"] is True:
                self.votes_received.add(msg["src"])

            # Check majority
            majority = (CLUSTER_SIZE // 2) + 1
            if len(self.votes_received) >= majority:
                self.role = LEADER
                self.leader_id = self.node_id

                # Initialise leader state
                next_idx = self._get_last_log_index() + 1
                self.next_index = {}
                self.match_index = {}

                for n_node in NODE_IDS:
                    if n_node != self.node_id:
                        self.next_index[n_node] = next_idx
                        self.match_index[n_node] = 0

                # Send immediate heartbeats so followers know who the leader is
                self.votes_received.clear()
                self.send_heartbeats()
                self.last_heartbeat_time = time.time()
                self.election_timeout = self._random_election_timeout()

    # RAFT LOG REPLICATION

    def send_heartbeats(self, read_id=None):
        """
        Leader sends periodic heartbeats (AppendEntries RPCs) to all followers.
        Also expires any pending linearizable reads that have waited too long,
        so clients are not silently stuck. Timeout is 2x the heartbeat interval.
        """
        if self.role != LEADER:
            return

        # Retry previously sent client responses in case the network dropped them
        now = time.time()
        for request_key, info in list(self.unacked_responses.items()):
            if now - info["sent_at"] >= HEARTBEAT_INTERVAL:
                self._send(info["response"])
                info["sent_at"] = now


        # Retry pending reads that have not yet reached quorum.
        # On a lossy network a heartbeat round may lose responses and the read
        # never collects majority acks.  Instead of silently expiring the read
        # (which leaves the client hanging until its 5s socket timeout fires),
        # we re-broadcast the tagged heartbeats and reset the ack set so the
        # next round of responses can form a fresh quorum.
        # A read is only truly abandoned after GIVE_UP_TIMEOUT — at that point
        # we send a failure response so the client can retry immediately rather
        # than waiting out its full socket timeout.
        RETRY_TIMEOUT = HEARTBEAT_INTERVAL * 2   # re-send after 2 heartbeats
        GIVE_UP_TIMEOUT = HEARTBEAT_INTERVAL * 10  # give up after 10 heartbeats
        now = time.time()
        reads_to_retry = []
        reads_to_abandon = []
        for rid, rs in self.pending_reads.items():
            age = now - rs.get("created_at", now)
            if age > GIVE_UP_TIMEOUT:
                reads_to_abandon.append(rid)
            elif now - rs.get("last_retry_at", rs["created_at"]) >= RETRY_TIMEOUT:
                reads_to_retry.append(rid)

        for rid in reads_to_abandon:
            rs = self.pending_reads.pop(rid)
            cid, req_id = rs["client_id"], rs["request_id"]
            self.pending_requests.discard((cid, req_id))
            # Notify client so it can retry immediately instead of timing out
            self._send(make_client_response(
                self.node_id, cid, req_id, False, error="READ TIMEOUT"
            ))

        for rid in reads_to_retry:
            rs = self.pending_reads[rid]
            # Reset acks to just the leader and re-broadcast tagged heartbeats
            # for this specific read so it gets another chance at quorum.
            rs["acks"] = {self.node_id}
            rs["last_retry_at"] = now
            for n_node in NODE_IDS:
                if n_node == self.node_id:
                    continue
                prev_log_index = self.next_index[n_node] - 1
                prev_log_term = self._get_log_term(prev_log_index)
                entries = self._get_log_slice(self.next_index[n_node])
                self._send(make_append_entries(
                    self.node_id, n_node, self.current_term, self.node_id,
                    prev_log_index, prev_log_term, entries,
                    self.commit_index, read_id=rid,
                ))

        for n_node in NODE_IDS:
            if n_node == self.node_id:
                continue

            # Determine the previous log index for this follower
            prev_log_index = self.next_index[n_node] - 1

            # Get the term of the previous log entry
            prev_log_term = self._get_log_term(prev_log_index)

            next_idx = self.next_index[n_node]

            # Prepare entries to send
            entries = self._get_log_slice(next_idx)

            # Build AppendEntries message with any missing log entries
            msg = make_append_entries(
                self.node_id,
                n_node,
                self.current_term,
                self.node_id,
                prev_log_index,
                prev_log_term,
                entries,
                self.commit_index,
                read_id=read_id,
            )

            # Send heartbeat to follower
            self._send(msg)

    def handle_append_entries(self, msg):
        with self.lock:
            # Reject stale leader term
            if msg["term"] < self.current_term:
                self._send(
                    make_append_entries_response(
                        self.node_id,
                        msg["src"],
                        self.current_term,
                        False,
                        match_index=self._get_last_log_index(),
                        read_id=msg.get("read_id")
                    )
                )
                return

            # If newer term discovered, step down
            if msg["term"] > self.current_term:
                self._step_down(msg["term"])

            # Accept this leader and reset election timer
            self.role = FOLLOWER
            self.leader_id = msg["leader_id"]
            self.last_heartbeat_time = time.time()
            self.election_timeout = self._random_election_timeout()

            # Check log consistency using prev log index and prev log term
            prev_log_index = msg["prev_log_index"]
            prev_log_term = msg["prev_log_term"]

            if prev_log_index > self._get_last_log_index():
                self._send(
                    make_append_entries_response(
                        self.node_id,
                        msg["src"],
                        self.current_term,
                        False,
                        match_index=self._get_last_log_index(),
                        read_id=msg.get("read_id")
                    )
                )
                return

            if prev_log_index != 0:
                my_prev_term = self._get_log_term(prev_log_index)
                if my_prev_term != prev_log_term:
                    self._send(
                        make_append_entries_response(
                            self.node_id,
                            msg["src"],
                            self.current_term,
                            False,
                            match_index=self._get_last_log_index(),
                            read_id=msg.get("read_id")
                        )
                    )
                    return

            # Consistency checks passed, process incoming entries
            entries = msg["entries"]
            changed_log = False

            for entry in entries:
                existing_entry = self._get_log_entry(entry["index"])

                if existing_entry is None:
                    self.log.append(entry)
                    changed_log = True

                elif existing_entry["term"] != entry["term"]:
                    # Delete conflicting suffix, then append leader's entry
                    self.log = [e for e in self.log if e["index"] < entry["index"]]
                    self.log.append(entry)
                    changed_log = True

            if changed_log:
                self.save_state()

            # Update commit index from leader
            leader_commit = msg["leader_commit"]
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, self._get_last_log_index())

            # Apply any newly committed entries
            self.apply_committed()

            if entries:
                match_index = entries[-1]["index"]
            else:
                match_index = prev_log_index

            # Reply success
            self._send(
                make_append_entries_response(
                    self.node_id,
                    msg["src"],
                    self.current_term,
                    True,
                    match_index=match_index,
                    read_id=msg.get("read_id")
                )
            )

    def handle_append_entries_response(self, msg):
        """
        Handle follower response to AppendEntries (heartbeat).
        """
        with self.lock:
            # Only the leader deals with responses
            if self.role != LEADER:
                return

            if msg["term"] > self.current_term:
                self._step_down(msg["term"])
                # Clean up any in-flight read requests that will never resolve
                for read_state in self.pending_reads.values():
                    cid = read_state["client_id"]
                    rid = read_state["request_id"]
                    self.pending_requests.discard((cid, rid))
                self.pending_reads.clear()
                return

            # Ignore stale responses
            if msg["term"] < self.current_term:
                return

            follower = msg["src"]

            if msg["success"]:
                follower_match = msg.get("match_index", 0)
                self.match_index[follower] = max(
                    self.match_index.get(follower, 0),
                    follower_match
                )
                self.next_index[follower] = self.match_index[follower] + 1

                majority = (CLUSTER_SIZE // 2) + 1

                # Try to advance commit_index
                for idx in range(self.commit_index + 1, self._get_last_log_index() + 1):
                    entry = self._get_log_entry(idx)

                    if entry is None:
                        continue

                    # Only directly commit entries from current term
                    if entry["term"] != self.current_term:
                        continue

                    replicated_count = 1  # Leader itself

                    for n_node in NODE_IDS:
                        if n_node == self.node_id:
                            continue
                        if self.match_index.get(n_node, 0) >= idx:
                            replicated_count += 1

                    if replicated_count >= majority:
                        self.commit_index = idx

                self.apply_committed()

                read_id = msg.get("read_id")
                if read_id is not None and read_id in self.pending_reads:
                    read_state = self.pending_reads[read_id]
                    read_state["acks"].add(follower)

                    if len(read_state["acks"]) >= majority:
                        key = read_state["key"]
                        client_id = read_state["client_id"]
                        request_id = read_state["request_id"]

                        if key in self.kv_store:
                            response = make_client_response(
                                self.node_id,
                                client_id,
                                request_id,
                                True,
                                value=self.kv_store[key]
                            )
                        else:
                            response = make_client_response(
                                self.node_id,
                                client_id,
                                request_id,
                                False,
                                error="NOT FOUND"
                            )

                        request_key = (client_id, request_id)

                        self.client_responses[request_key] = response
                        self.pending_requests.discard(request_key)
                        self._send(response)

                        # Retry GET responses too, because on lossy networks
                        # the reply itself may be dropped even though the read succeeded.
                        self.unacked_responses[request_key] = {
                            "response": response,
                            "sent_at": time.time(),
                        }

                        del self.pending_reads[read_id]

            else:
                # Use follower's match_index hint to skip back faster
                follower_match = msg.get("match_index", 0)
                current = self.next_index.get(follower, 1)
                # Jump back to just after the follower's last known good entry,
                # but never below 1
                self.next_index[follower] = max(1, min(follower_match + 1, current - 1))

    # CLIENT REQUEST HANDLING

    def handle_client_request(self, msg):
        with self.lock:
            operation = msg["operation"]
            key = msg.get("key")
            value = msg.get("value")
            request_id = msg["request_id"]
            client_id = msg.get("client_id", msg["src"])

            request_key = (client_id, request_id)

            # Suppress in-flight duplicates on ALL nodes to avoid double-appending.
            if request_key in self.pending_requests:
                return

            # FOLLOWER BEHAVIOR
            if self.role != LEADER:
                # Always forward to the leader rather than replying directly.
                # This means even cached dedup responses are sent exactly once
                # (by the leader), preventing multiple followers from flooding
                # the client socket with duplicate replies.
                if self.leader_id is not None:
                    forwarded = dict(msg)
                    forwarded["dst"] = self.leader_id
                    forwarded["client_id"] = client_id
                    self._send(forwarded)
                else:
                    response = make_client_response(
                        self.node_id,
                        client_id,
                        request_id,
                        False,
                        error="NOT LEADER",
                        leader_hint=None
                    )
                    self._send(response)
                return

            # LEADER: deduplication — reply with cached response if we have one
            if request_key in self.client_responses:
                self._send(self.client_responses[request_key])
                return

            # LEADER: GET (linearizable — requires heartbeat quorum)
            if operation == "GET":
                self.read_seq += 1
                read_id = f"{self.node_id}-{self.read_seq}"

                # Leader counts as one ack; need majority total
                self.pending_reads[read_id] = {
                    "key": key,
                    "client_id": client_id,
                    "request_id": request_id,
                    "acks": {self.node_id},
                    "created_at": time.time(),
                }
                self.pending_requests.add(request_key)

                # Broadcast heartbeat tagged with this read_id to gather acks
                self.send_heartbeats(read_id=read_id)

                # If leader alone constitutes a majority (single-node cluster),
                # resolve the read immediately
                majority = (CLUSTER_SIZE // 2) + 1
                if len(self.pending_reads[read_id]["acks"]) >= majority:
                    if key in self.kv_store:
                        response = make_client_response(
                            self.node_id, client_id, request_id, True,
                            value=self.kv_store[key]
                        )
                    else:
                        response = make_client_response(
                            self.node_id, client_id, request_id, False,
                            error="NOT FOUND"
                        )
                    self.client_responses[request_key] = response
                    self.pending_requests.discard(request_key)
                    self._send(response)

                    self.unacked_responses[request_key] = {
                        "response": response,
                        "sent_at": time.time(),
                    }

                    del self.pending_reads[read_id]

                return

            # LEADER: PUT / DELETE
            if operation in ("PUT", "DELETE"):
                new_index = self._get_last_log_index() + 1

                entry = {
                    "index": new_index,
                    "term": self.current_term,
                    "operation": operation,
                    "key": key,
                    "value": value,
                    "client_id": client_id,
                    "request_id": request_id,
                }

                self.log.append(entry)
                self.pending_requests.add(request_key)
                self.save_state()
                self.send_heartbeats()

    # STATE MACHINE APPLICATION

    def apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self._get_log_entry(self.last_applied)

            if entry is None:
                continue

            operation = entry["operation"]
            key = entry["key"]
            value = entry.get("value")

            if operation == "PUT":
                self.kv_store[key] = value

            elif operation == "DELETE":
                self.kv_store.pop(key, None)

            client_id = entry.get("client_id")
            request_id = entry.get("request_id")

            if client_id is not None and request_id is not None:
                request_key = (client_id, request_id)

                response = make_client_response(
                    self.node_id,
                    client_id,
                    request_id,
                    True
                )
                self.client_responses[request_key] = response
                self.pending_requests.discard(request_key)

                if self.role == LEADER:
                    self._send(response)
                    self.unacked_responses[request_key] = {
                        "response": response,
                        "sent_at": time.time(),
                    }

        if len(self.log) >= SNAPSHOT_THRESHOLD:
            self.take_snapshot()

    # CHECKPOINTING / SNAPSHOTTING (Part 3)

    def take_snapshot(self):
        if self.last_applied == 0:
            return

        last_included_index = self.last_applied
        last_included_term = self._get_log_term(last_included_index)

        self.snapshot = {
            "last_included_index": last_included_index,
            "last_included_term": last_included_term,
            "kv_store": dict(self.kv_store),
        }

        self.log = [entry for entry in self.log if entry["index"] > last_included_index]
        self.save_state()

    def load_snapshot(self, snapshot_data):
        if not snapshot_data:
            self.snapshot = {
                "last_included_index": 0,
                "last_included_term": 0,
                "kv_store": {},
            }
            return

        self.snapshot = {
            "last_included_index": snapshot_data.get("last_included_index", 0),
            "last_included_term": snapshot_data.get("last_included_term", 0),
            "kv_store": dict(snapshot_data.get("kv_store", {}))
        }

        self.kv_store = dict(self.snapshot["kv_store"])
        self.last_applied = self.snapshot["last_included_index"]
        self.commit_index = self.snapshot["last_included_index"]

    # STATE PERSISTENCE (Part 3)

    def save_state(self):
        os.makedirs(DATA_DIR, exist_ok=True)

        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": self.log,
            "snapshot": self.snapshot,
        }

        path = os.path.join(DATA_DIR, f"{self.node_id}.json")
        tmp_path = path + ".tmp"

        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(state, f)

        os.replace(tmp_path, path)

    def load_state(self):
        os.makedirs(DATA_DIR, exist_ok=True)
        path = os.path.join(DATA_DIR, f"{self.node_id}.json")

        if not os.path.exists(path):
            return

        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)

        self.current_term = state.get("current_term", 0)
        self.voted_for = state.get("voted_for", None)
        self.log = state.get("log", [])

        snapshot_data = state.get("snapshot")
        self.load_snapshot(snapshot_data)

    # HELPER METHODS

    def _get_last_log_index(self):
        """Return the index of the last log entry, or 0 if log is empty."""
        if self.log:
            return self.log[-1]["index"]
        return self.snapshot["last_included_index"]

    def _get_last_log_term(self):
        """Return the term of the last log entry, or 0 if log is empty."""
        if self.log:
            return self.log[-1]["term"]
        return self.snapshot["last_included_term"]

    def _get_log_term(self, index):
        """Return the term of the log entry at the given index, or 0."""
        if index == 0:
            return 0

        if index == self.snapshot["last_included_index"]:
            return self.snapshot["last_included_term"]

        for entry in self.log:
            if entry["index"] == index:
                return entry["term"]
        return 0

    def _get_log_entry(self, index):
        """Return the log entry at the given index, or None."""
        for entry in self.log:
            if entry["index"] == index:
                return entry
        return None

    def _get_log_slice(self, from_index):
        """Return all log entries from from_index onward (inclusive)."""
        return [entry for entry in self.log if entry["index"] >= from_index]

    def _step_down(self, new_term):
        """Revert to follower state with a new term."""
        self.current_term = new_term
        self.role = FOLLOWER
        self.voted_for = None
        self.leader_id = None
        self.votes_received.clear()
        self.pre_votes_received.clear()
        self.pre_vote_term = 0
        self.save_state()


# ENTRY POINT

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python node.py <node-id>")
        print(f"  Valid node IDs: {NODE_IDS}")
        sys.exit(1)

    node_id = sys.argv[1]
    if node_id not in NODE_IDS:
        print(f"Invalid node ID '{node_id}'. Must be one of: {NODE_IDS}")
        sys.exit(1)

    node = RaftNode(node_id)
    node.start()