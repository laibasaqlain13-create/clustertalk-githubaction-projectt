"""
ClusterTalk — WebSocket-to-TCP Bridge
======================================

WHY THIS EXISTS
---------------
Browsers cannot open raw TCP sockets directly. ClusterTalk's backend
(server.py + lb.py) speaks a custom binary length-prefixed protocol
over raw TCP:

    [4 bytes: length BE] [1 byte: message type] [N bytes: orjson payload]

This bridge sits between the browser frontend (app.js) and the TCP
backend. It:

    1. Accepts a WebSocket connection from the browser.
    2. Opens a matching raw TCP connection to the ClusterTalk backend
       (either the Load Balancer or a node directly).
    3. Translates in both directions:
         Browser  →  WS JSON text frame  →  Bridge  →  TCP binary frame  →  Backend
         Backend  →  TCP binary frame    →  Bridge  →  WS JSON text frame →  Browser

WIRE PROTOCOL RECAP (framing.py)
---------------------------------
Every TCP frame:
    Bytes 0-3  : uint32 big-endian = len(type_byte + payload_bytes)
    Byte  4    : uint8  MessageType enum value
    Bytes 5+   : orjson-encoded JSON payload (may be empty)

BRIDGE JSON CONTRACT (what the browser sends/receives over WebSocket)
----------------------------------------------------------------------
All WebSocket frames are JSON text. The bridge uses the same field names
as framing.py's payload keys, plus a "type" field that maps directly to
MessageType enum names.

Browser → Bridge (send):
    { "type": "LOGIN",    "username": "ali",   "password": "secret" }
    { "type": "REGISTER", "username": "ali",   "password": "secret" }
    { "type": "JOIN_ROOM","room_id": "general" }
    { "type": "MESSAGE",  "room_id": "general","text": "hello",
                          "client_msg_id": "c1abc",  "seq": <auto> }
    { "type": "GET_ROOMS" }
    { "type": "PING" }
    { "type": "DISCONNECT" }

Bridge → Browser (receive):
    All backend responses translated to JSON with a "type" field added:
    { "type": "LOGIN_ACK",    "ok": true,  "session_id": "...",
                               "room_id": "general", "last_processed_seq": 0 }
    { "type": "REGISTER_ACK", "ok": true }
    { "type": "HELLO_ACK",    "session_id": "...", "room_id": "...",
                               "last_processed_seq": 0 }
    { "type": "ROOM_JOINED",  "room_id": "general" }
    { "type": "ROOMS_LIST",   "rooms": [...] }
    { "type": "MESSAGE",      "seq": 1, "text": "...", "from": "..." }
    { "type": "ACK",          "seq": 1 }
    { "type": "PONG" }
    { "type": "ERROR",        "code": "...", "reason": "..." }
    { "type": "DISCONNECT",   "reason": "..." }

URL SCHEME
----------
    ws://localhost:8080/bridge          → connects to LB at TCP_HOST:TCP_PORT
    ws://localhost:8080/bridge?node=1   → (future: connect to specific node)

ARCHITECTURE
------------
    Browser
      │  WebSocket (text JSON)
      ▼
    WsBridge (this file)   ← asyncio, one task pair per connection
      │  Raw TCP (binary framing.py frames)
      ▼
    ClusterTalk LB (lb.py)  or  ChatNode (server.py)
      │  Mesh TCP
      ▼
    Peer nodes...

Each browser connection spawns:
    - 1 asyncio Task: browser_to_tcp   (WS → TCP)
    - 1 asyncio Task: tcp_to_browser   (TCP → WS)
Both tasks share a single TCP StreamReader/Writer pair and are
cancelled together when either side closes.

RUNNING
-------
    python ws_bridge.py                          # default: LB on 127.0.0.1:9000
    python ws_bridge.py --tcp-host 127.0.0.1 --tcp-port 9000 --ws-port 8080

    Then in app.js set:
        const MOCK_MODE = false;
        const BRIDGE_URL = 'ws://localhost:8080/bridge';
"""


#ingresss/ws_bridge.py
from __future__ import annotations

import argparse
import asyncio
import logging
import struct
import signal
import sys
from typing import Optional

import orjson
import websockets
try:
    from websockets.asyncio.server import ServerConnection as WebSocketServerProtocol
except ImportError:
    from websockets.server import WebSocketServerProtocol  # websockets < 14

# ── Logging ────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("clustertalk.bridge")

# ── Wire protocol constants (must match framing.py) ────────────────────────

_LENGTH_STRUCT = struct.Struct(">I")   # 4-byte big-endian uint32
LENGTH_PREFIX_SIZE = 4
TYPE_SIZE = 1
DEFAULT_MAX_FRAME_BYTES = 1 * 1024 * 1024   # 1 MiB — same as framing.py

# MessageType enum values (must stay in sync with framing.py)
# We keep a local mapping so the bridge has no import dependency on
# the node/ package — it can run standalone from the ingress/ folder.
_NAME_TO_TYPE: dict[str, int] = {
    "HELLO":         0x01,
    "HELLO_ACK":     0x02,
    "PING":          0x03,
    "PONG":          0x04,
    "MESSAGE":       0x05,
    "ACK":           0x06,
    "ERROR":         0x07,
    "NODE_ANNOUNCE": 0x08,
    "NODE_STATUS":   0x09,
    "SESSION_BIND":  0x0A,
    "DISCONNECT":    0x0B,
    "JOIN_ROOM":     0x0C,
    "ROOM_JOINED":   0x0D,
    "PEER_HELLO":    0x0E,
    "RELAY":         0x0F,
    "SESSION_QUERY": 0x10,
    "SESSION_STATE": 0x11,
    # Feature 1 — Auth
    "REGISTER":      0x12,
    "REGISTER_ACK":  0x13,
    "LOGIN":         0x14,
    "LOGIN_ACK":     0x15,
    # Feature 2 — Room discovery
    "GET_ROOMS":     0x16,
    "ROOMS_LIST":    0x17,
}
_TYPE_TO_NAME: dict[int, str] = {v: k for k, v in _NAME_TO_TYPE.items()}


# ── Frame codec ────────────────────────────────────────────────────────────

def encode_tcp_frame(msg_type_name: str, payload: dict | None = None) -> bytes:
    """
    Build a binary TCP frame from a message type name and optional payload dict.
    Raises KeyError if msg_type_name is unknown.
    """
    type_byte = _NAME_TO_TYPE[msg_type_name]
    body = orjson.dumps(payload) if payload else b""
    type_and_body = bytes([type_byte]) + body
    length_prefix = _LENGTH_STRUCT.pack(len(type_and_body))
    return length_prefix + type_and_body


async def read_tcp_frame(
    reader: asyncio.StreamReader,
    max_size: int = DEFAULT_MAX_FRAME_BYTES,
) -> tuple[str, dict | None]:
    """
    Read one complete frame from the TCP backend.
    Returns (type_name, payload_dict_or_None).
    Raises ConnectionError on EOF or oversized frame.
    """
    try:
        length_bytes = await reader.readexactly(LENGTH_PREFIX_SIZE)
    except asyncio.IncompleteReadError:
        raise ConnectionError("TCP backend closed (reading length prefix)")

    (declared_size,) = _LENGTH_STRUCT.unpack(length_bytes)

    if declared_size < TYPE_SIZE:
        raise ConnectionError(f"Frame too small: {declared_size} bytes")
    if declared_size > max_size:
        raise ConnectionError(
            f"Frame too large: {declared_size} > {max_size} bytes"
        )

    try:
        body = await reader.readexactly(declared_size)
    except asyncio.IncompleteReadError:
        raise ConnectionError("TCP backend closed mid-frame")

    type_byte = body[0]
    type_name = _TYPE_TO_NAME.get(type_byte, f"UNKNOWN_0x{type_byte:02X}")
    payload_bytes = body[TYPE_SIZE:]
    payload = orjson.loads(payload_bytes) if payload_bytes else None
    return type_name, payload


def frame_to_ws_json(type_name: str, payload: dict | None) -> str:
    """
    Convert a decoded TCP frame into the JSON string the browser expects.
    Merges the type name into the payload dict as "type".
    """
    data = dict(payload) if payload else {}
    data["type"] = type_name
    return orjson.dumps(data).decode("utf-8")


def ws_json_to_frame(raw_json: str) -> bytes | None:
    """
    Parse a JSON string from the browser and encode it as a TCP frame.
    Returns None if the message type is unknown (logs a warning).
    """
    try:
        data = orjson.loads(raw_json)
    except Exception:
        logger.warning("bridge: invalid JSON from browser: %r", raw_json[:200])
        return None

    msg_type = data.pop("type", None)
    if not msg_type:
        logger.warning("bridge: browser message missing 'type' field")
        return None

    if msg_type not in _NAME_TO_TYPE:
        logger.warning("bridge: unknown message type from browser: %r", msg_type)
        return None

    # Strip client-only fields that the backend doesn't understand.
    data.pop("client_msg_id", None)

    return encode_tcp_frame(msg_type, data if data else None)


# ── Bridge session ─────────────────────────────────────────────────────────

class BridgeSession:
    """
    Manages one browser ↔ backend connection pair.

    Lifecycle:
        1. Browser opens WebSocket to /bridge
        2. BridgeSession opens a TCP connection to the ClusterTalk backend
        3. Two asyncio Tasks run concurrently:
             browser_to_tcp:  WS message → TCP frame
             tcp_to_browser:  TCP frame  → WS message
        4. Either side disconnecting cancels both tasks and closes both
           connections cleanly.
    """

    def __init__(
        self,
        ws: WebSocketServerProtocol,
        tcp_host: str,
        tcp_port: int,
    ):
        self._ws = ws
        self._tcp_host = tcp_host
        self._tcp_port = tcp_port
        self._tcp_reader: Optional[asyncio.StreamReader] = None
        self._tcp_writer: Optional[asyncio.StreamWriter] = None
        self._peer = ws.remote_address

    async def run(self) -> None:
        """
        Connect to the TCP backend, then pump frames in both directions
        until either side closes.
        """
        try:
            self._tcp_reader, self._tcp_writer = await asyncio.wait_for(
                asyncio.open_connection(self._tcp_host, self._tcp_port),
                timeout=5.0,
            )
        except (OSError, asyncio.TimeoutError) as exc:
            logger.error(
                "bridge: cannot connect to backend %s:%d — %s",
                self._tcp_host, self._tcp_port, exc,
            )
            try:
                # A terminal close is vital: a browser must see a real WS
                # close event so its exponential reconnect loop continues
                # while the cluster is temporarily empty.
                await self._ws.send(orjson.dumps({
                    "type": "DISCONNECT",
                    "reason": "backend_unavailable",
                    "retryable": True,
                }).decode())
                await self._ws.close(code=1013, reason="backend temporarily unavailable")
            except Exception:
                pass
            return

        logger.info(
            "bridge: %s connected → backend %s:%d",
            self._peer, self._tcp_host, self._tcp_port,
        )

        b2t = asyncio.create_task(
            self._browser_to_tcp(), name=f"b2t-{self._peer}"
        )
        t2b = asyncio.create_task(
            self._tcp_to_browser(), name=f"t2b-{self._peer}"
        )

        try:
            # Run until either direction closes.
            done, pending = await asyncio.wait(
                {b2t, t2b}, return_when=asyncio.FIRST_COMPLETED
            )
            # Check for exceptions from the completed task.
            for task in done:
                exc = task.exception()
                if exc:
                    logger.debug("bridge task ended with: %s", exc)
        finally:
            for task in (b2t, t2b):
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except (asyncio.CancelledError, Exception):
                        pass
            self._close_tcp()
            logger.info("bridge: %s disconnected", self._peer)

    async def _browser_to_tcp(self) -> None:
        """
        Relay: Browser WebSocket → TCP Backend.
        Each WS message is parsed as JSON, converted to a binary TCP frame,
        and written to the backend socket.
        """
        async for raw_msg in self._ws:
            if isinstance(raw_msg, bytes):
                # Browser sent raw bytes — unusual but forward as-is
                # (could be a future binary optimisation).
                self._tcp_writer.write(raw_msg)
                await self._tcp_writer.drain()
                continue

            tcp_frame = ws_json_to_frame(raw_msg)
            if tcp_frame is None:
                continue   # logged inside ws_json_to_frame

            logger.debug(
                "bridge: browser→tcp  %s",
                raw_msg[:120].replace("\n", " "),
            )
            self._tcp_writer.write(tcp_frame)
            await self._tcp_writer.drain()

    async def _tcp_to_browser(self) -> None:
        """
        Relay: TCP Backend → Browser WebSocket.
        Reads complete binary TCP frames, decodes them, serialises to JSON,
        and forwards to the browser over WebSocket.
        """
        while True:
            try:
                type_name, payload = await read_tcp_frame(self._tcp_reader)
            except ConnectionError as exc:
                logger.info("bridge: TCP backend closed — %s", exc)
                # Notify browser that the backend disconnected.
                try:
                    bye = orjson.dumps({
                        "type": "DISCONNECT",
                        "reason": "backend_closed",
                        "retryable": True,
                    }).decode()
                    await self._ws.send(bye)
                    await self._ws.close(
                        code=1012,
                        reason="backend closed"
                        )
                    
                    
                    
                except Exception:
                    pass
                return

            ws_msg = frame_to_ws_json(type_name, payload)
            logger.debug("bridge: tcp→browser  %s", ws_msg[:120])

            try:
                await self._ws.send(ws_msg)
            except websockets.exceptions.ConnectionClosed:
                return   # browser already gone

    def _close_tcp(self) -> None:
        if self._tcp_writer and not self._tcp_writer.is_closing():
            try:
                self._tcp_writer.close()
            except Exception:
                pass


# ── WebSocket server ───────────────────────────────────────────────────────

async def ws_handler(
    ws: WebSocketServerProtocol,
    tcp_host: str,
    tcp_port: int,
) -> None:
    """
    WebSocket connection handler. One BridgeSession per connected browser.
    """
    session = BridgeSession(ws, tcp_host, tcp_port)
    await session.run()


async def run_bridge(
    ws_host: str,
    ws_port: int,
    tcp_host: str,
    tcp_port: int,
) -> None:
    """
    Start the WebSocket server and run until SIGINT/SIGTERM.
    """
    import functools

    handler = functools.partial(ws_handler, tcp_host=tcp_host, tcp_port=tcp_port)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # Windows does not support add_signal_handler
            pass

    async with websockets.serve(
        handler,
        ws_host,
        ws_port,
        # Allow all origins (browser opens from file:// or localhost)
        origins=None,
        # Ping/pong keepalive to detect stale browser connections
        ping_interval=20,
        ping_timeout=10,
        # Increase max message size to match framing.py's 1 MiB cap
        max_size=DEFAULT_MAX_FRAME_BYTES,
    ) as server:
        logger.info("=" * 60)
        logger.info("  ClusterTalk WebSocket-to-TCP Bridge")
        logger.info("  WebSocket  : ws://%s:%d/bridge", ws_host, ws_port)
        logger.info("  TCP Backend: %s:%d", tcp_host, tcp_port)
        logger.info("=" * 60)
        logger.info("  Browser: set MOCK_MODE = false  in app.js")
        logger.info("  Browser: set BRIDGE_URL = 'ws://%s:%d/bridge'", ws_host, ws_port)
        logger.info("=" * 60)
        logger.info("  Press Ctrl+C to stop.")
        logger.info("")

        await stop_event.wait()

    logger.info("bridge: shut down cleanly.")


# ── CLI ────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="ClusterTalk WebSocket-to-TCP Bridge",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--ws-host", default="0.0.0.0",
        help="Host the WebSocket server listens on",
    )
    p.add_argument(
        "--ws-port", type=int, default=8080,
        help="Port the WebSocket server listens on",
    )
    p.add_argument(
        "--tcp-host", default="127.0.0.1",
        help="ClusterTalk backend host (LB or direct node)",
    )
    p.add_argument(
        "--tcp-port", type=int, default=9000,
        help="ClusterTalk backend port (LB=9000, node=8765)",
    )
    p.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging verbosity",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    logging.getLogger().setLevel(args.log_level)
    logging.getLogger("websockets").setLevel(logging.WARNING)

    try:
        asyncio.run(
            run_bridge(
                ws_host=args.ws_host,
                ws_port=args.ws_port,
                tcp_host=args.tcp_host,
                tcp_port=args.tcp_port,
            )
        )
    except KeyboardInterrupt:
        pass
