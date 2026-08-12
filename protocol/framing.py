#protocol/framing.py

"""
ClusterTalk wire protocol — length-prefixed binary framing.

Frame layout (on the wire):

    +------------------+------------------+------------------------+
    |  LENGTH (4 bytes)|  TYPE (1 byte)   |  PAYLOAD (LENGTH-1 B)  |
    |  uint32, big-end |  MessageType     |  orjson-encoded bytes  |
    +------------------+------------------+------------------------+

LENGTH covers TYPE + PAYLOAD (i.e. everything after the length field
itself). This lets a reader always know exactly how many more bytes
to pull off the socket before it has a complete frame.

Design constraints this module is built around:
  - Single-threaded asyncio event loop per node -> no blocking calls,
    no threads. Everything here is either pure/sync (encode/decode)
    or an `async def` built on asyncio.StreamReader/Writer.
  - 10k+ concurrent connections per node -> avoid unnecessary copies,
    branch on the 1-byte type before touching orjson where possible,
    and reject oversized frames before allocating a buffer for them.
  - Zero message loss -> MESSAGE frames carry a monotonic `seq`;
    see protocol.py / SPEC.md for the ack/replay contract. This
    module only concerns itself with getting bytes on and off the
    wire correctly.

--- NEW FRAME TYPES (Feature additions) ---

FEATURE 1 — REAL AUTHENTICATION:
    REGISTER      0x12  client -> node   {username, password}
    REGISTER_ACK  0x13  node -> client   {ok: bool, reason?: str}
    LOGIN         0x14  client -> node   {username, password}
    LOGIN_ACK     0x15  node -> client   {ok: bool, session_id?: str,
                                          reason?: str}

    Auth flow (replaces the old HELLO-first approach):
        1. Client connects and sends REGISTER (new user) or LOGIN
           (existing user) as the VERY FIRST frame.
        2. Node validates credentials against the users table.
        3. On success:  node sends LOGIN_ACK / REGISTER_ACK with
           ok=True and an assigned session_id, then continues with
           the normal HELLO / HELLO_ACK handshake internally using
           that session_id.
        4. On failure:  node sends LOGIN_ACK / REGISTER_ACK with
           ok=False and a human-readable reason, then closes the
           connection.
        ERROR frames are used for protocol violations (e.g. sending
        MESSAGE before authenticating).

FEATURE 2 — ROOM MANAGEMENT API:
    GET_ROOMS     0x16  client -> node   {}   (empty payload)
    ROOMS_LIST    0x17  node -> client   {rooms: [{room_id, name,
                                          max_capacity, current_count,
                                          created_by}]}

    Room capacity:
        JOIN_ROOM now returns ERROR with code "room_full" if the
        room has reached its max_capacity. max_capacity defaults
        to 50 (configurable per room at creation, future work).

    The existing JOIN_ROOM (0x0C) and ROOM_JOINED (0x0D) frame
    types are unchanged — new capacity metadata is stored in
    SQLite but the wire format of those frames is backwards-
    compatible.
"""

from __future__ import annotations

import asyncio
import struct
from enum import IntEnum

import orjson

# --- Wire constants ---------------------------------------------------

# 4-byte unsigned big-endian length prefix.
_LENGTH_STRUCT = struct.Struct(">I")
LENGTH_PREFIX_SIZE = _LENGTH_STRUCT.size  # 4

# 1-byte message type, immediately after the length prefix.
_TYPE_STRUCT = struct.Struct(">B")
TYPE_SIZE = _TYPE_STRUCT.size  # 1

# Hard ceiling on (TYPE + PAYLOAD) size. Chosen to comfortably fit a
# chat message plus metadata while making a single frame unable to
# blow up per-connection memory. Override per-deployment if needed,
# but this is the default a fresh node ships with.
DEFAULT_MAX_FRAME_SIZE = 1 * 1024 * 1024  # 1 MiB


class MessageType(IntEnum):
    # ── Original frame types (unchanged) ──────────────────────────────
    HELLO         = 0x01  # client -> node: handshake
    HELLO_ACK     = 0x02  # node -> client: handshake accepted
    PING          = 0x03  # either direction: liveness check
    PONG          = 0x04  # either direction: liveness reply
    MESSAGE       = 0x05  # either direction: chat payload, carries seq
    ACK           = 0x06  # either direction: acks a MESSAGE by seq
    ERROR         = 0x07  # node -> client: structured error
    NODE_ANNOUNCE = 0x08  # node -> LB: registers + reports capacity
    NODE_STATUS   = 0x09  # node -> LB: periodic health/load report
    SESSION_BIND  = 0x0A  # LB -> node: routes a client session
    DISCONNECT    = 0x0B  # either direction: graceful close
    JOIN_ROOM     = 0x0C  # client -> node: switch/join a chat room
    ROOM_JOINED   = 0x0D  # node -> client: room switch confirmed
    PEER_HELLO    = 0x0E  # node -> node: mesh handshake (node_id)
    RELAY         = 0x0F  # node -> node: propagate room broadcast
    SESSION_QUERY = 0x10  # node -> node: "does anyone have session X?"
    SESSION_STATE = 0x11  # node -> node: reply with session snapshot

    # ── Feature 1: Real Authentication ────────────────────────────────
    REGISTER      = 0x12  # client -> node: {username, password}
    REGISTER_ACK  = 0x13  # node -> client: {ok, reason?}
    LOGIN         = 0x14  # client -> node: {username, password}
    LOGIN_ACK     = 0x15  # node -> client: {ok, session_id?, reason?}

    # ── Feature 2: Room Discovery API ─────────────────────────────────
    GET_ROOMS     = 0x16  # client -> node: {} — request room list
    ROOMS_LIST    = 0x17  # node -> client: {rooms: [...]}


class FrameError(Exception):
    """Base class for framing-level errors."""


class FrameTooLarge(FrameError):
    """Raised when a declared frame length exceeds the configured max."""

    def __init__(self, declared_size: int, max_size: int):
        self.declared_size = declared_size
        self.max_size = max_size
        super().__init__(
            f"frame size {declared_size} exceeds max {max_size}"
        )


class ConnectionClosed(FrameError):
    """Raised when the peer closes the connection mid-frame."""


# --- Encoding -----------------------------------------------------------

def encode_frame(msg_type: MessageType, payload: dict | None = None) -> bytes:
    """
    Build a complete frame ready to write to a socket.

    payload=None encodes as an empty payload (valid for PING/PONG/ACK
    where no body is needed).
    """
    body = orjson.dumps(payload) if payload is not None else b""
    type_and_body = _TYPE_STRUCT.pack(int(msg_type)) + body
    length_prefix = _LENGTH_STRUCT.pack(len(type_and_body))
    return length_prefix + type_and_body


# --- Decoding -------------------------------------------------------------

async def read_frame(
    reader: asyncio.StreamReader,
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
) -> tuple[MessageType, dict | None]:
    """
    Read exactly one complete frame from an asyncio StreamReader and
    decode it fully.

    Blocks (awaits) until a full frame is available, the connection
    closes, or the declared length exceeds max_frame_size.

    Raises:
        ConnectionClosed: peer closed the socket before a full frame
            arrived (including a clean close with 0 bytes read).
        FrameTooLarge: declared length exceeds max_frame_size. Caller
            should treat this as a protocol violation and disconnect
            the peer -- do NOT attempt to read/discard the declared
            body, since that number is attacker/bug controlled.
    """
    raw = await read_raw_frame(reader, max_frame_size)
    return decode_frame_body(raw)


async def read_raw_frame(
    reader: asyncio.StreamReader,
    max_frame_size: int = DEFAULT_MAX_FRAME_SIZE,
) -> bytes:
    """
    Read exactly one complete frame WITHOUT decoding it -- returns
    the raw bytes (length prefix + type + payload) exactly as they
    arrived on the wire.

    This exists for the Ingress Layer: once it's made a routing
    decision (which backend a connection goes to), it relays bytes
    between client and backend without touching orjson at all, which
    is what "raw bytes relay" in the architecture means in practice
    -- the LB shouldn't pay JSON-parse cost on every chat message
    just to move bytes from one socket to another.

    Same error semantics as read_frame (ConnectionClosed, FrameTooLarge).
    """
    try:
        length_bytes = await reader.readexactly(LENGTH_PREFIX_SIZE)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionClosed(
            "peer closed before sending a length prefix"
        ) from exc

    (declared_size,) = _LENGTH_STRUCT.unpack(length_bytes)

    if declared_size < TYPE_SIZE:
        raise FrameError(
            f"declared size {declared_size} smaller than type byte"
        )
    if declared_size > max_frame_size:
        raise FrameTooLarge(declared_size, max_frame_size)

    try:
        body = await reader.readexactly(declared_size)
    except asyncio.IncompleteReadError as exc:
        raise ConnectionClosed(
            "peer closed mid-frame"
        ) from exc

    return length_bytes + body


def decode_frame_body(raw_frame: bytes) -> tuple[MessageType, dict | None]:
    """
    Decode a raw frame (as returned by read_raw_frame) into its type
    and payload. Split out from read_raw_frame so a caller can choose
    to decode only the frames it actually needs to inspect (e.g. just
    the first HELLO, for routing) and relay everything else as
    opaque bytes.
    """
    body = raw_frame[LENGTH_PREFIX_SIZE:]
    (type_byte,) = _TYPE_STRUCT.unpack_from(body, 0)
    try:
        msg_type = MessageType(type_byte)
    except ValueError as exc:
        raise FrameError(f"unknown message type byte: {type_byte:#x}") from exc

    payload_bytes = body[TYPE_SIZE:]
    payload = orjson.loads(payload_bytes) if payload_bytes else None
    return msg_type, payload


# --- Convenience write helper --------------------------------------------

async def write_frame(
    writer: asyncio.StreamWriter,
    msg_type: MessageType,
    payload: dict | None = None,
) -> None:
    """Encode and write a frame, then drain (respect backpressure)."""
    writer.write(encode_frame(msg_type, payload))
    await writer.drain()
    