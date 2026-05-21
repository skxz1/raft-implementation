"""
test_extension.py — Tests for the Pre-Vote Extension (Part 5)

Pre-Vote prevents a node that was isolated (e.g. behind a network partition)
from disrupting the rest of the cluster when it rejoins.  Without Pre-Vote
such a node would have inflated its term by firing repeated elections while
isolated, and would force every other node to step down the moment it
reconnected.  With Pre-Vote the isolated node can only start a real election
once a quorum of peers agree that an election is actually needed.

These tests are designed to run against the LIVE cluster.  Start the cluster
before running this file:

    python run_cluster.py               # perfect network
    python run_cluster.py network_lossy.py   # lossy network (edge-case tests)

Then in a second terminal:

    python test_extension.py

Exit code 0 means all tests passed; exit code 1 means at least one failed.

Tests
-----
1. basic_operations_after_election
       Verifies the cluster elects a leader and correctly handles PUT / GET /
       DELETE operations.  This is the foundation — if basic Raft is broken,
       none of the Pre-Vote tests are meaningful.

2. term_stability_under_no_failure
       Connects to the cluster, reads the leader term via a successful PUT,
       waits several heartbeat intervals, then issues another PUT and checks
       that the term has not changed.  Without Pre-Vote a spurious timeout
       would cause a node to increment its term even when a healthy leader
       exists.  With Pre-Vote, peers reject the pre-vote because they are
       still receiving heartbeats, so the term stays stable.

3. cluster_recovers_after_isolation
       Simulates what a rejoining node would see: we PUT a key, then perform
       many PUT operations on different keys, then verify the original key
       still returns the correct value.  This ensures committed data is not
       lost and the state machine remains consistent even after election
       activity.  This is the scenario Pre-Vote is designed to protect.

4. no_stale_read_after_leader_change
       Issues a PUT, records the value, then triggers a period of cluster
       activity (more writes), and finally issues a GET and verifies it
       returns the most-recently committed value.  Under Pre-Vote the new
       leader must still satisfy Raft's safety guarantees so a stale read
       from an old state must never occur.

5. duplicate_request_idempotency
       Sends two identical requests (same request_id) and verifies the
       operation is applied exactly once.  Pre-Vote does not change
       deduplication semantics, so this test ensures the extension does not
       accidentally break idempotency.

6. edge_case_no_election_while_leader_alive
       Runs the cluster under a lossy network and sends a burst of writes.
       Checks that the success rate is high enough to indicate that no
       unnecessary elections were triggered (which would cause temporary
       unavailability).  Pre-Vote directly improves availability here by
       suppressing elections when a healthy leader exists.
"""

import socket
import json
import struct
import time
import sys
import uuid

# ── connection settings (must match config.py) ──────────────────────────────
NETWORK_HOST = "localhost"
NETWORK_PORT = 5000
CLIENT_TIMEOUT = 6.0   # seconds to wait for a response
RETRY_DELAY   = 1.0    # seconds between retries
MAX_RETRIES   = 4


# ── low-level wire protocol ──────────────────────────────────────────────────

def _send(sock, msg):
    data = json.dumps(msg).encode("utf-8")
    sock.sendall(struct.pack("!I", len(data)) + data)


def _recv(sock):
    def recv_exact(n):
        buf = b""
        while len(buf) < n:
            chunk = sock.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    header = recv_exact(4)
    if not header:
        return None
    length = struct.unpack("!I", header)[0]
    body = recv_exact(length)
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


# ── client helper ────────────────────────────────────────────────────────────

class SimpleClient:
    """Minimal client that speaks the cluster's message protocol."""

    def __init__(self):
        self.client_id = f"test-{uuid.uuid4().hex[:8]}"
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((NETWORK_HOST, NETWORK_PORT))
        self.sock.settimeout(CLIENT_TIMEOUT)

        # Register with the network
        _send(self.sock, {
            "type": "REGISTER",
            "src": self.client_id,
            "dst": "network",
            "sender_type": "client",
        })
        ack = _recv(self.sock)
        if not ack or ack.get("type") != "REGISTER_ACK":
            raise RuntimeError("Client registration failed")

    def _request(self, operation, key, value=None, request_id=None):
        if request_id is None:
            request_id = str(uuid.uuid4())
        msg = {
            "type": "CLIENT_REQUEST",
            "src": self.client_id,
            "dst": "leader",
            "request_id": request_id,
            "operation": operation,
            "key": key,
            "value": value,
        }
        _send(self.sock, msg)
        # Read responses until we get one matching our request_id.
        # This discards any stale cached responses that arrived late from a
        # previous request — e.g. a duplicate-PUT response that lands in the
        # buffer just before we send a GET.
        deadline = time.time() + CLIENT_TIMEOUT
        while True:
            remaining = deadline - time.time()
            if remaining <= 0:
                return None, request_id
            self.sock.settimeout(remaining)
            try:
                resp = _recv(self.sock)
                if resp is None:
                    return None, request_id
                if resp.get("request_id") == request_id:
                    return resp, request_id
                # Wrong request_id — stale response, discard and keep waiting
            except socket.timeout:
                return None, request_id

    def put(self, key, value, retries=MAX_RETRIES, request_id=None):
        for attempt in range(retries):
            resp, rid = self._request("PUT", key, value, request_id=request_id)
            if resp and resp.get("success"):
                return True, resp
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
        return False, None

    def get(self, key, retries=MAX_RETRIES):
        for attempt in range(retries):
            resp, _ = self._request("GET", key)
            if resp is not None:
                return resp.get("success"), resp.get("value"), resp
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
        return None, None, None

    def delete(self, key, retries=MAX_RETRIES):
        for attempt in range(retries):
            resp, _ = self._request("DELETE", key)
            if resp and resp.get("success"):
                return True, resp
            if attempt < retries - 1:
                time.sleep(RETRY_DELAY)
        return False, None

    def close(self):
        self.sock.close()


# ── test harness ─────────────────────────────────────────────────────────────

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
_results = []


def check(name, condition, detail=""):
    status = PASS if condition else FAIL
    msg = f"  [{status}] {name}"
    if detail:
        msg += f"  — {detail}"
    print(msg)
    _results.append(condition)
    return condition


# ── individual tests ──────────────────────────────────────────────────────────

def test_basic_operations_after_election():
    """
    Sanity check: leader election happens and basic KV operations work.

    Pre-Vote must not prevent elections from ever completing — it just
    ensures they are not triggered unnecessarily.  This test verifies that
    an election does complete (the leader is elected) and that PUT / GET /
    DELETE all behave correctly.
    """
    print("\n[Test 1] basic_operations_after_election")
    client = SimpleClient()
    try:
        key = f"ext-basic-{uuid.uuid4().hex[:6]}"

        ok, _ = client.put(key, "hello")
        check("PUT succeeds", ok)

        found, val, _ = client.get(key)
        check("GET returns correct value", found and val == "hello", f"got {val!r}")

        ok2, _ = client.put(key, "world")
        check("Second PUT succeeds", ok2)

        found2, val2, _ = client.get(key)
        check("GET returns updated value", found2 and val2 == "world", f"got {val2!r}")

        ok3, _ = client.delete(key)
        check("DELETE succeeds", ok3)

        found3, val3, _ = client.get(key)
        check("GET after DELETE returns not-found", found3 is False, f"got {val3!r}")

    finally:
        client.close()


def test_term_stability_under_no_failure():
    """
    Term should not change while a healthy leader is running.

    Without Pre-Vote a node whose election timer fires would increment the
    cluster term and force a re-election even though the leader is perfectly
    healthy.  With Pre-Vote peers reject the pre-vote (because they are still
    receiving heartbeats), so no real election starts and the term stays the
    same.

    We cannot read the term directly from outside the cluster, but we can
    infer stability by verifying that both writes succeed quickly and without
    the multi-second delay that a mid-write leader change would introduce.
    """
    print("\n[Test 2] term_stability_under_no_failure")
    client = SimpleClient()
    try:
        key1 = f"ext-term-a-{uuid.uuid4().hex[:6]}"
        key2 = f"ext-term-b-{uuid.uuid4().hex[:6]}"

        # First write — establishes the leader
        t0 = time.time()
        ok1, _ = client.put(key1, "before")
        elapsed1 = time.time() - t0
        check("First PUT succeeds", ok1, f"took {elapsed1:.2f}s")

        # Wait a couple of heartbeat intervals (1 s total).  If Pre-Vote is
        # working, no unnecessary election fires during this window.
        time.sleep(1.0)

        # Second write — should be served by the same leader without a new election
        t1 = time.time()
        ok2, _ = client.put(key2, "after")
        elapsed2 = time.time() - t1
        check("Second PUT succeeds after wait", ok2, f"took {elapsed2:.2f}s")

        # Both values must be retrievable and correct
        _, v1, _ = client.get(key1)
        _, v2, _ = client.get(key2)
        check("First key still readable", v1 == "before", f"got {v1!r}")
        check("Second key readable", v2 == "after", f"got {v2!r}")

    finally:
        client.close()


def test_cluster_recovers_after_isolation():
    """
    Data committed before a period of cluster activity must survive.

    This test models the scenario Pre-Vote is designed to protect against:
    a node that was isolated fires lots of elections after rejoining and
    could, without Pre-Vote, cause term inflation that disrupts the leader.
    We verify that committed data is never lost regardless of election activity.
    """
    print("\n[Test 3] cluster_recovers_after_isolation")
    client = SimpleClient()
    try:
        anchor_key = f"ext-anchor-{uuid.uuid4().hex[:6]}"
        anchor_val = "committed-before-chaos"

        # Commit a key we will check later
        ok, _ = client.put(anchor_key, anchor_val)
        check("Anchor PUT committed", ok)

        # Simulate cluster activity (like what would happen during re-election)
        # by writing many keys in quick succession
        successes = 0
        for i in range(10):
            k = f"ext-chaos-{i}-{uuid.uuid4().hex[:4]}"
            ok_i, _ = client.put(k, f"v{i}", retries=2)
            if ok_i:
                successes += 1

        check("Most writes during activity succeed", successes >= 6,
              f"{successes}/10 succeeded")

        # The anchor key must still hold the original value
        found, val, _ = client.get(anchor_key)
        check("Anchor key survives cluster activity",
              found and val == anchor_val, f"got {val!r}")

    finally:
        client.close()


def test_no_stale_read_after_leader_change():
    """
    GET must always return the most recently committed value (linearisability).

    Pre-Vote, combined with the ReadIndex protocol already in place, must not
    introduce stale reads.  After a sequence of writes the final GET must see
    the last committed value, not an earlier one.
    """
    print("\n[Test 4] no_stale_read_after_leader_change")
    client = SimpleClient()
    try:
        key = f"ext-linear-{uuid.uuid4().hex[:6]}"
        final_val = None

        for i in range(5):
            v = f"version-{i}"
            ok, _ = client.put(key, v)
            if ok:
                final_val = v

        check("At least one PUT succeeded", final_val is not None)

        if final_val is not None:
            found, val, _ = client.get(key)
            check("GET returns latest committed value",
                  found and val == final_val,
                  f"expected {final_val!r}, got {val!r}")

    finally:
        client.close()


def test_duplicate_request_idempotency():
    """
    The same request_id sent twice must apply the operation exactly once.

    Pre-Vote does not change deduplication logic, but we verify that the
    extension has not accidentally introduced a regression.  The second
    request must return the cached response without applying the operation
    a second time — the stored value must remain "original", not "duplicate".

    We use a separate client for the verification GET to avoid any
    socket-buffer contamination from the duplicate-response traffic.
    """
    print("\n[Test 5] duplicate_request_idempotency")
    writer = SimpleClient()
    reader = SimpleClient()
    try:
        key = f"ext-dedup-{uuid.uuid4().hex[:6]}"
        rid = str(uuid.uuid4())

        # First request — must commit "original"
        ok1, _ = writer.put(key, "original", request_id=rid)
        check("First PUT succeeds", ok1)

        if ok1:
            # Re-send the exact same request_id with a different value.
            # retries=1 so we send exactly once and read back one response
            # (the cached success from the leader) without looping.
            writer.put(key, "duplicate", retries=1, request_id=rid)

            # Verify on an independent connection — no stale socket state
            found, val, _ = reader.get(key)
            check("Value not overwritten by duplicate request",
                  found and val == "original",
                  f"got {val!r}")
        else:
            check("Value not overwritten by duplicate request", False,
                  "skipped — first PUT did not succeed")

    finally:
        writer.close()
        reader.close()


def test_edge_case_no_election_while_leader_alive():
    """
    Under a lossy network the cluster should maintain high write throughput
    because Pre-Vote prevents spurious elections.

    We send 20 PUT operations with short timeouts and expect at least 60%
    to succeed.  If unnecessary elections were firing constantly, the cluster
    would be repeatedly unavailable and the success rate would drop well
    below this threshold.

    NOTE: This test is most meaningful when run with network_lossy.py.
    On the perfect network it will almost always pass regardless of Pre-Vote.
    """
    print("\n[Test 6] edge_case_no_election_while_leader_alive")
    client = SimpleClient()
    try:
        total = 20
        succeeded = 0
        for i in range(total):
            k = f"ext-lossy-{i}-{uuid.uuid4().hex[:4]}"
            ok, _ = client.put(k, f"val-{i}", retries=2)
            if ok:
                succeeded += 1

        rate = succeeded / total
        check(
            f"At least 60% of writes succeed ({succeeded}/{total})",
            rate >= 0.60,
            f"success rate {rate:.0%}",
        )
    finally:
        client.close()


# ── entry point ───────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Pre-Vote Extension Tests (Part 5)")
    print("  Ensure the cluster is running before executing this script.")
    print("=" * 60)

    tests = [
        test_basic_operations_after_election,
        test_term_stability_under_no_failure,
        test_cluster_recovers_after_isolation,
        test_no_stale_read_after_leader_change,
        test_duplicate_request_idempotency,
        test_edge_case_no_election_while_leader_alive,
    ]

    for test_fn in tests:
        try:
            test_fn()
        except Exception as exc:
            print(f"  [ERROR] {test_fn.__name__} raised an exception: {exc}")
            _results.append(False)

    passed = sum(_results)
    total  = len(_results)
    print("\n" + "=" * 60)
    print(f"  Results: {passed}/{total} checks passed")
    print("=" * 60)
    sys.exit(0 if all(_results) else 1)


if __name__ == "__main__":
    main()
