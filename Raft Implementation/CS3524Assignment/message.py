# message.py - Message protocol for the distributed system
#
# This file defines the message types, serialisation protocol, and helper
# functions for constructing messages.
#
# The protocol uses length prefixed JSON over TCP, understanding the protocol
# probably only matters if you plan to change it in your extension
# Every message is sent as:
#   [4 bytes: big-endian uint32 length] [N bytes: UTF-8 JSON]
#
# Every message has at minimum these fields:
#   - "type" - one of the MSG_* constants below
#   - "src" - sender ID (e.g. "node-0", "client-abc123")
#   - "dst" - destination ID, "all_nodes", or "leader"

import json
import struct

# === Message Type Constants ===

# Registration (node/client <-> network)
MSG_REGISTER = "REGISTER"
MSG_REGISTER_ACK = "REGISTER_ACK"

# Client operations (client <-> node)
MSG_CLIENT_REQUEST = "CLIENT_REQUEST"
MSG_CLIENT_RESPONSE = "CLIENT_RESPONSE"

# Raft RPCs (node <-> node)
MSG_APPEND_ENTRIES = "APPEND_ENTRIES"
MSG_APPEND_ENTRIES_RESPONSE = "APPEND_ENTRIES_RESPONSE"
MSG_REQUEST_VOTE = "REQUEST_VOTE"
MSG_REQUEST_VOTE_RESPONSE = "REQUEST_VOTE_RESPONSE"


# === Wire Protocol ===

def send_message(sock, msg_dict):
    """Send a length prefixed JSON message over a TCP socket.

    Protocol: 4-byte big-endian length prefix followed by UTF-8 JSON bytes.

    Args:
        sock: TCP socket to send on.
        msg_dict: Dictionary to serialise and send.
    """
    json_bytes = json.dumps(msg_dict).encode("utf-8")
    header = struct.pack("!I", len(json_bytes))
    sock.sendall(header + json_bytes)


def recv_message(sock):
    """
    Receive a length prefixed JSON message from a TCP socket.

    Returns:
        dict: The deserialised message, or None if connection closed.
    """
    header = _recv_exact(sock, 4)
    if not header:
        return None
    length = struct.unpack("!I", header)[0]
    json_bytes = _recv_exact(sock, length)
    if not json_bytes:
        return None
    return json.loads(json_bytes.decode("utf-8"))


def _recv_exact(sock, num_bytes):
    """
    Receive exactly num_bytes from the socket.

    Returns:
        bytes: The received data, or None if connection closed before
               all bytes were received.
    """
    data = b""
    while len(data) < num_bytes:
        chunk = sock.recv(num_bytes - len(data))
        if not chunk:
            return None
        data += chunk
    return data


# === Provided Message Constructors ===

def make_register(sender_id, sender_type):
    """
    Create a REGISTER message.

    Sent by a node or client when it first connects to the network.

    Args:
        sender_id: e.g. "node-0" or "client-1"
        sender_type: "node" or "client"
    """
    return {
        "type": MSG_REGISTER,
        "src": sender_id,
        "dst": "network",
        "sender_type": sender_type,
    }


def make_client_request(client_id, request_id, operation, key, value=None):
    """
    Create a CLIENT_REQUEST message.

    Sent by a client to the cluster. The network delivers this to all nodes;
    only the leader should process it.

    Args:
        client_id: The client's identifier.
        request_id: Unique ID for this request (for deduplication).
        operation: "PUT", "GET", or "DELETE".
        key: The key to operate on.
        value: The value (for PUT only).
    """
    return {
        "type": MSG_CLIENT_REQUEST,
        "src": client_id,
        "dst": "leader",
        "request_id": request_id,
        "operation": operation,
        "key": key,
        "value": value,
    }


def make_client_response(node_id, client_id, request_id, success,
                         value=None, leader_hint=None, error=None):
    """
    Create a CLIENT_RESPONSE message.

    Sent by a node back to the client.

    Args:
        node_id: The responding node's ID.
        client_id: The destination client's ID.
        request_id: Echoed from the original request (for matching).
        success: True if the operation succeeded.
        value: The value returned by a GET operation.
        leader_hint: If this node is not the leader, hint at who is.
        error: Error message string if the operation failed.
    """
    return {
        "type": MSG_CLIENT_RESPONSE,
        "src": node_id,
        "dst": client_id,
        "request_id": request_id,
        "success": success,
        "value": value,
        "leader_hint": leader_hint,
        "error": error,
    }

def make_request_vote(sender_id, receiver_id, current_term, last_log_index, last_log_term,
                      pre_vote=False):
    """
    Create a REQUEST_VOTE message.

    The optional pre_vote flag is used by the Pre-Vote extension (Part 5).
    When True, the message is a probe — the receiver should decide whether it
    *would* grant a vote without actually updating its term or voted_for.
    The term field carries current_term + 1 (the hypothetical next term) so
    the receiver can apply the same up-to-date log check it would for a real
    RequestVote.
    """
    return {
        "type": MSG_REQUEST_VOTE,
        "src": sender_id,
        "dst": receiver_id,
        "term": current_term,
        "last_log_index": last_log_index,
        "last_log_term": last_log_term,
        "pre_vote": pre_vote,
    }

def make_vote_response(sender_id, receiver_id, current_term, vote_granted, pre_vote=False):
    """
    Create a REQUEST_VOTE_RESPONSE message.

    The pre_vote flag mirrors the flag from the corresponding REQUEST_VOTE so
    the requester knows whether this is a pre-vote probe response or a real
    vote grant.
    """
    return {
        "type": MSG_REQUEST_VOTE_RESPONSE,
        "src": sender_id,
        "dst": receiver_id,
        "term": current_term,
        "vote_granted": vote_granted,
        "pre_vote": pre_vote,
    }

def make_append_entries(sender_id, receiver_id, current_term, leader_id, prev_log_index, prev_log_term, entries, leader_commit_index, read_id=None):
    return{
        "type": MSG_APPEND_ENTRIES,
        "src": sender_id,
        "dst": receiver_id,
        "term": current_term,
        "leader_id": leader_id,
        "prev_log_index": prev_log_index,
        "prev_log_term": prev_log_term,
        "entries": entries,
        "leader_commit": leader_commit_index,
        "read_id": read_id,

    }

def make_append_entries_response(node_id, receiver_id, term, success, match_index=0, read_id=None):
    """
    Create an APPEND_ENTRIES_RESPONSE message.

    Sent by a follower back to the leader.

    Args:
        node_id: Sender node ID (the follower).
        receiver_id: Destination leader node ID.
        term: Follower's current term.
        success: True if heartbeat/append was accepted.
    """
    return {
        "type": MSG_APPEND_ENTRIES_RESPONSE,
        "src": node_id,
        "dst": receiver_id,
        "term": term,
        "success": success,
        "match_index": match_index,
        "read_id": read_id,
    }