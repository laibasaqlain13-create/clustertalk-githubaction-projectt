"""
Backend Mesh node: the asyncio TCP server that ties framing.py,
session_state.py, persistence.py, and session_manager.py together
into something that actually accepts connections and moves chat
messages around.

Wire-level contract this handler implements:

    ── STEP 1: Authentication (NEW — must happen FIRST) ──────────────
    Client -> Node    REGISTER {username, password}
    Node -> Client    REGISTER_ACK {ok, reason?}
                      -- on ok=True: continue to HELLO
                      -- on ok=False: connection closed

    Client -> Node    LOGIN {username, password}
    Node -> Client    LOGIN_ACK {ok, session_id?, reason?}
                      -- on ok=True: session_id is assigned, skip HELLO
                         (session is already set up internally)
                      -- on ok=False: connection closed

    ── STEP 2: Normal chat session (unchanged) ───────────────────────
    Client -> Node    HELLO {session_id?, room_id?, last_received_seq}
    Node -> Client    HELLO_ACK {session_id, room_id, last_processed_seq}
    Client -> Node    JOIN_ROOM {room_id}
    Node -> Client    ROOM_JOINED {room_id}
                   OR ERROR {code: "room_full", reason: "..."}
    Client -> Node    GET_ROOMS {}
    Node -> Client    ROOMS_LIST {rooms: [{room_id, name, max_capacity,
                                           current_count, created_by}]}
    Node -> Client    MESSAGE {seq, text, from}
    Client -> Node    MESSAGE {seq, text}
    Node -> Client    ACK {seq}
    Client -> Node    ACK {seq}
    Client -> Node    PING {}
    Node -> Client    PONG {}
    either             DISCONNECT {reason?}

Authentication flow:
    A newly-connected client MUST send REGISTER or LOGIN as its very
    first frame.  Sending any other frame type before auth results in
    an ERROR frame and immediate disconnect.

    After successful LOGIN the node assigns a stable session_id
    derived from the username (so the same user always gets the same
    persistent session across reconnects -- outbox replay, room
    membership, and seq counters all survive disconnects).

    After successful REGISTER + automatic LOGIN, the client receives
    REGISTER_ACK {ok: True} followed by LOGIN_ACK {ok: True, session_id}.

    Health-check probes (HELLO {health_check: True}) bypass auth
    entirely -- they are detected before the auth gate.

Room capacity:
    Rooms are now backed by the 'rooms' SQLite table. Each room has a
    max_capacity (default 50). When a session tries to JOIN_ROOM:
      1. The room row is looked up (or created with defaults).
      2. The current active_count is taken from SessionManager
         (how many sessions are currently in that room on this node).
         NOTE: this is per-node count. A full cross-cluster count
         would need mesh coordination -- tracked as future work.
      3. If active_count >= max_capacity: send ERROR {code: "room_full"}
         and leave the session in its current room.
      4. Otherwise: move the session to the new room, upsert the room
         row in SQLite, confirm with ROOM_JOINED.

Password security:
    hashlib.pbkdf2_hmac with SHA-256, a 16-byte os.urandom() salt,
    100,000 iterations. The stored format is "salt_hex:hash_hex" --
    no external bcrypt dependency required (Python stdlib only).
    bcrypt would be slightly safer for large deployments (adaptive
    cost), but PBKDF2-SHA256 at 100k iterations is solid and keeps
    the dependency list minimal.

Scope (still deliberately narrow):
    - Auth is per-node -- there is no cross-node user database sync.
      A user registered on node A cannot log in on node B unless the
      nodes share the same SQLite file (via NFS/shared volume) or
      user data is separately replicated. This is documented as a
      known limitation for the distributed case.
    - Room capacity is per-node active_count. Cross-node capacity
      enforcement requires mesh gossip (future work).
    - No access tokens / JWT -- the session_id IS the bearer token
      for now. A real deployment should add a time-limited token layer
      on top of this session_id.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "protocol"))

from framing import (
    ConnectionClosed,
    FrameError,
    MessageType,
    read_frame,
    write_frame,
)
from mesh import PeerManager
from auth_store import AuthStore
from database import ensure_database_parent
from persistence import OutboxStore
from session_manager import SessionManager

logger = logging.getLogger("clustertalk.node")

AUTH_TIMEOUT_SECONDS    = 10.0   # time to send LOGIN or REGISTER
HELLO_TIMEOUT_SECONDS   = 5.0
RETRY_CHECK_INTERVAL_SECONDS = 1.0
RETRY_AFTER_SECONDS     = 3.0
NODE_STATUS_INTERVAL_SECONDS = 3.0
LB_REGISTER_RECONNECT_DELAY_SECONDS = 2.0

# Default max room capacity used when a room is first created via
# JOIN_ROOM (no explicit capacity management frame in v1 -- future
# work adds a CREATE_ROOM frame with custom capacity).
DEFAULT_ROOM_CAPACITY = 50

# PBKDF2 parameters. 100k iterations is NIST's 2023 minimum for
# SHA-256; increase over time as hardware gets faster.
_PBKDF2_ITERATIONS = 100_000
_PBKDF2_HASH = "sha256"
_SALT_BYTES = 16


# ── Password utilities ────────────────────────────────────────────────────

def _hash_password(plaintext: str) -> str:
    """
    Hash a plaintext password with a fresh random salt using
    PBKDF2-HMAC-SHA256. Returns "salt_hex:hash_hex" for storage.
    """
    salt = os.urandom(_SALT_BYTES)
    dk = hashlib.pbkdf2_hmac(
        _PBKDF2_HASH,
        plaintext.encode("utf-8"),
        salt,
        _PBKDF2_ITERATIONS,
    )
    return f"{salt.hex()}:{dk.hex()}"


def _verify_password(plaintext: str, stored_hash: str) -> bool:
    """
    Constant-time verification of plaintext against a stored
    "salt_hex:hash_hex" value produced by _hash_password().
    Returns True if the password matches.
    """
    try:
        salt_hex, dk_hex = stored_hash.split(":", 1)
        salt = bytes.fromhex(salt_hex)
        expected_dk = bytes.fromhex(dk_hex)
    except (ValueError, AttributeError):
        return False

    actual_dk = hashlib.pbkdf2_hmac(
        _PBKDF2_HASH,
        plaintext.encode("utf-8"),
        salt,
        _PBKDF2_ITERATIONS,
    )
    # hmac.compare_digest is constant-time -- prevents timing attacks.
    import hmac
    return hmac.compare_digest(actual_dk, expected_dk)


def _session_id_for_user(username: str) -> str:
    """
    Derive a stable, deterministic session_id from a username using
    UUID v5 (SHA-1 namespace hash). The same user always gets the same
    session_id, so their outbox, room, and seq counters persist across
    reconnects -- no need for the client to store and re-send a token.

    UUID v5 with a fixed namespace means the mapping is deterministic
    but not reversible to the original username without the namespace
    (it's a one-way hash, not plain text).
    """
    namespace = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")  # UUID_NAMESPACE_DNS
    return str(uuid.uuid5(namespace, f"clustertalk.user.{username}"))


# ── ChatNode ─────────────────────────────────────────────────────────────

class ChatNode:
    """
    One backend mesh node. Owns:
      - the SessionManager (in-memory state + persistence)
      - a registry of currently-connected sessions on this node
      - the retry loop that resends unacked messages
      - the auth layer (Feature 1) -- validates REGISTER / LOGIN
      - room capacity enforcement (Feature 2)
    """

    def __init__(
        self,
        db_path: str,
        host: str = "0.0.0.0",
        port: int = 8765,
        node_id: str | None = None,
        mesh_host: str = "0.0.0.0",
        mesh_port: int | None = None,
        peer_addresses: list[tuple[str, int]] | None = None,
        advertise_host: str | None = None,
        lb_register_host: str | None = None,
        lb_register_port: int | None = None,
        auth_db_path: str | None = None,
    ):
        self.host = host
        self.port = port
        self._actual_port: int | None = None
        self._db_path = ensure_database_parent(db_path)
        self._store: OutboxStore | None = None
        self._auth_store = AuthStore(auth_db_path or self._db_path)
        self._session_manager: SessionManager | None = None

        self._connections: dict[str, asyncio.StreamWriter] = {}

        # session_id -> username, for authenticated (LOGIN/REGISTER)
        # sessions. Lets broadcasts carry a human-readable author name
        # instead of the opaque session_id. HELLO-only sessions (tests,
        # legacy reconnects) have no entry and fall back to session_id.
        self._session_usernames: dict[str, str] = {}

        self._retry_task: asyncio.Task | None = None
        self._server: asyncio.AbstractServer | None = None

        self.node_id = node_id or str(uuid.uuid4())
        self._mesh_host = mesh_host
        self._mesh_port = mesh_port
        self._peer_addresses = peer_addresses or []
        self._mesh: PeerManager | None = None

        self._advertise_host = advertise_host
        self._lb_register_host = lb_register_host
        self._lb_register_port = lb_register_port
        self._register_task: asyncio.Task | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        self._store = OutboxStore(self._db_path)
        self._store.start()
        self._auth_store.start()
        admin_user = os.getenv("CLUSTERTALK_ADMIN_USERNAME", "admin")
        admin_password = os.getenv("CLUSTERTALK_ADMIN_PASSWORD", "123456")
        if admin_user and admin_password:
            await self._auth_store.bootstrap_admin(
                admin_user, await asyncio.to_thread(_hash_password, admin_password)
            )
        self._session_manager = SessionManager(self._store)

        bind_host = self.host or "0.0.0.0"
        bind_port = self.port if self.port is not None else int(os.environ.get("PORT", 8080))
        self.host = bind_host
        self.port = bind_port

        try:
            self._server = await asyncio.start_server(
                self._handle_client, bind_host, bind_port
            )
        except OSError:
            logger.warning(
                "node bind failed on %s:%s; retrying with an ephemeral local port",
                bind_host,
                bind_port,
            )
            self._server = await asyncio.start_server(
                self._handle_client, bind_host, 0
            )
        addr = self._server.sockets[0].getsockname()
        self._actual_port = addr[1]
        self.port = self._actual_port
        logger.info("ChatNode listening on %s:%s", addr[0], addr[1])

        self._retry_task = asyncio.create_task(self._retry_loop())

        if self._mesh_port is not None:
            self._mesh = PeerManager(
                node_id=self.node_id,
                mesh_host=self._mesh_host,
                mesh_port=self._mesh_port,
                peer_addresses=self._peer_addresses,
                deliver_locally=self._deliver_relayed_message,
                session_query_provider=self._session_manager.snapshot_session,
            )
            await self._mesh.start()

        if self._lb_register_port is not None:
            self._register_task = asyncio.create_task(self._register_with_lb_loop())

    async def serve_forever(self) -> None:
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        for task_attr in ("_retry_task", "_register_task"):
            task = getattr(self, task_attr)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._mesh is not None:
            await self._mesh.stop()
        if self._store is not None:
            self._store.stop()

    # ── Per-connection handling ───────────────────────────────────────────

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer = writer.get_extra_info("peername")
        session_id: str | None = None
        try:
            # ── Step 0: Detect health check probe early (no auth needed) ─
            # We peek at the first frame; if it's HELLO with health_check=True
            # we handle it before touching the auth layer at all.
            session_id = await self._auth_gate(reader, writer, peer)
            if session_id is None:
                return  # auth failed, health-check probe, or timeout

            # Fresh connection for this session: reset the inbound seq
            # watermark. Clients (including the browser frontend) restart
            # their outbound seq at 1 on every new connection / page load,
            # so a watermark persisted from a previous connection would
            # otherwise make the first messages look like already-seen
            # duplicates and silently drop them instead of broadcasting.
            session = await self._session_manager.get_or_create(session_id)
            session.reset_inbound()

            await self._replay_unacked(session_id, writer)
            await self._read_loop(session_id, reader, writer)

        except (ConnectionClosed, ConnectionResetError):
            logger.info("session %s disconnected (peer %s)", session_id, peer)
        except FrameError as exc:
            logger.warning("protocol violation from %s (session %s): %s", peer, session_id, exc)
        finally:
            await self._cleanup_connection(session_id, writer)

    # ── Feature 1: Auth gate ──────────────────────────────────────────────

    async def _auth_gate(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        peer: object,
    ) -> str | None:
        """
        Read the first frame from a newly-connected client. It MUST be
        one of:
            HELLO (health_check=True)  -- health probe, handled inline
            LOGIN                      -- authenticate existing user
            REGISTER                   -- create + authenticate new user

        Any other frame type causes an ERROR + disconnect.

        This is a LOOP, not a single read: a REGISTER whose username is
        already taken (or otherwise invalid) replies REGISTER_ACK{ok:false}
        but keeps the connection open, so the client can immediately follow
        up with LOGIN on the SAME socket. This is exactly the frontend's
        "try REGISTER first, fall back to LOGIN" flow — closing the socket
        after a failed REGISTER (the previous behaviour) meant returning
        users could never log in, because their LOGIN landed on a dead
        connection.

        Returns the session_id on success, None on failure/probe/timeout.
        """
        while True:
            try:
                msg_type, payload = await asyncio.wait_for(
                    read_frame(reader), timeout=AUTH_TIMEOUT_SECONDS
                )
            except asyncio.TimeoutError:
                logger.info("auth timeout from peer %s", peer)
                writer.close()
                return None

            payload = payload or {}

            # ── Health check probe (HELLO with health_check flag) ─────────
            # The LB uses this -- it must work without auth.
            if msg_type == MessageType.HELLO and payload.get("health_check"):
                await write_frame(writer, MessageType.HELLO_ACK, {"healthy": True})
                writer.close()
                return None

            # ── REGISTER ──────────────────────────────────────────────────
            if msg_type == MessageType.REGISTER:
                session_id = await self._handle_register(writer, payload)
                if session_id is not None:
                    return session_id
                # Registration didn't establish a session (username taken
                # or invalid fields). Keep the connection open and loop so
                # the client can retry with LOGIN on the same socket.
                continue

            # ── LOGIN ─────────────────────────────────────────────────────
            if msg_type == MessageType.LOGIN:
                session_id = await self._handle_login(writer, payload)
                if session_id is not None:
                    return session_id
                # Bad credentials -- close. The client retries by opening a
                # fresh connection (the frontend re-connects on each attempt).
                writer.close()
                return None

            # ── HELLO without health_check (legacy / direct connect) ──────
            # Keep backward compatibility: a raw HELLO (session_id + room_id)
            # without going through LOGIN is allowed. This covers reconnects
            # from clients that went through auth on a previous connection
            # and already have a session_id, plus the test/tooling clients.
            if msg_type == MessageType.HELLO:
                return await self._handle_hello_after_auth(reader, writer, payload)

            # ── Anything else before auth is a protocol violation ─────────
            await write_frame(writer, MessageType.ERROR, {
                "code": "auth_required",
                "reason": "first frame must be LOGIN, REGISTER, or HELLO (reconnect)",
            })
            writer.close()
            return None

    async def _handle_register(
        self,
        writer: asyncio.StreamWriter,
        payload: dict,
    ) -> str | None:
        """
        REGISTER flow:
            1. Validate payload fields.
            2. Check username is not already taken.
            3. Hash the password and store it.
            4. Send REGISTER_ACK {ok: True}.
            5. Continue with LOGIN internally (reuse _setup_session_for_user).

        On any failure this sends REGISTER_ACK{ok:false} and returns None
        WITHOUT closing the connection — the caller (_auth_gate) keeps the
        socket open so the client can retry with LOGIN.
        """
        username = (payload.get("username") or "").strip()
        password = payload.get("password") or ""

        # ── Basic field validation ─────────────────────────────────────────
        if not username or len(username) < 3 or len(username) > 32:
            await write_frame(writer, MessageType.REGISTER_ACK, {
                "ok": False,
                "reason": "username must be 3-32 characters",
            })
            return None

        if not password or len(password) < 6:
            await write_frame(writer, MessageType.REGISTER_ACK, {
                "ok": False,
                "reason": "password must be at least 6 characters",
            })
            return None

        # ── Check duplicate ───────────────────────────────────────────────
        # ── Hash + persist ────────────────────────────────────────────────
        password_hash = await asyncio.to_thread(_hash_password, password)
        if not await self._auth_store.create_user(username, password_hash):
            await write_frame(writer, MessageType.REGISTER_ACK, {
                "ok": False,
                "reason": "username already taken",
            })
            return None

        await write_frame(writer, MessageType.REGISTER_ACK, {"ok": True})
        logger.info("new user registered: %s", username)

        # Auto-login after registration.
        session_id = await self._setup_session_for_user(username, writer, role="client")
        return session_id

    async def _handle_login(
        self,
        writer: asyncio.StreamWriter,
        payload: dict,
    ) -> str | None:
        """
        LOGIN flow:
            1. Look up user by username.
            2. Verify password (PBKDF2 -- runs in a thread to avoid
               blocking the event loop during 100k iterations).
            3. On success: derive session_id, send LOGIN_ACK, return sid.
            4. On failure: send LOGIN_ACK {ok: False}, return None (the
               caller closes the connection).
        """
        username = (payload.get("username") or "").strip()
        password = payload.get("password") or ""
        requested_role = payload.get("role") or "client"

        user_row = await self._auth_store.get_user(username)
        if user_row is None:
            # User not found -- same error message as wrong password
            # to avoid username enumeration.
            await write_frame(writer, MessageType.LOGIN_ACK, {
                "ok": False,
                "reason": "invalid username or password",
            })
            return None

        # Verify in a thread -- PBKDF2 at 100k iterations is ~10ms of CPU.
        password_ok = await asyncio.to_thread(
            _verify_password, password, user_row["password_hash"]
        )
        if not password_ok:
            await write_frame(writer, MessageType.LOGIN_ACK, {
                "ok": False,
                "reason": "invalid username or password",
            })
            return None

        if requested_role != user_row["role"]:
            await write_frame(writer, MessageType.LOGIN_ACK, {
                "ok": False,
                "reason": "this account is not authorized for the requested portal",
            })
            return None

        session_id = await self._setup_session_for_user(username, writer, role=user_row["role"])
        logger.info("user %s logged in (session %s)", username, session_id)
        return session_id

    async def _setup_session_for_user(
        self, username: str, writer: asyncio.StreamWriter, role: str = "client"
    ) -> str:
        """
        After successful auth: derive a deterministic session_id for
        this user, set up their session in SessionManager, register
        the connection, and send LOGIN_ACK with the session_id.
        """
        session_id = _session_id_for_user(username)
        # Remember the display name for this session so broadcasts can
        # attribute messages to "alice" rather than her opaque session_id.
        self._session_usernames[session_id] = username

        # Cross-node recovery: if the mesh is up and this node doesn't
        # have a local record, try to pull the session state from a peer.
        if self._mesh is not None:
            if not await self._session_manager.has_local_record(session_id):
                snapshot = await self._mesh.query_session(session_id)
                if snapshot is not None:
                    await self._session_manager.hydrate_from_snapshot(
                        session_id, snapshot
                    )
                    logger.info("recovered session %s from mesh peer", session_id)

        # Evict any stale connection for this session (e.g. client
        # reconnecting before the old socket fully closed).
        old_writer = self._connections.get(session_id)
        if old_writer is not None and old_writer is not writer:
            async def _evict(w: asyncio.StreamWriter):
                try:
                    await write_frame(w, MessageType.DISCONNECT, {"reason": "logged_in_elsewhere"})
                    await asyncio.sleep(0.05)  # flush
                except Exception:
                    pass
                finally:
                    w.close()
            asyncio.create_task(_evict(old_writer))

        self._connections[session_id] = writer

        session = await self._session_manager.get_or_create(session_id)
        await write_frame(writer, MessageType.LOGIN_ACK, {
            "ok": True,
            "session_id": session_id,
            "room_id": session.room_id,
            "last_processed_seq": session.last_processed_seq,
            "role": role,
        })
        return session_id

    async def _handle_hello_after_auth(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        payload: dict,
    ) -> str | None:
        """
        Handle a legacy HELLO frame on a connection that didn't go
        through LOGIN/REGISTER (e.g. client already has a session_id
        from a previous authenticated connection and is reconnecting).
        Falls back to old behavior: accepts the session_id and sets up
        the session, then replays any buffered state for that session.
        """
        requested_session_id = payload.get("session_id")
        requested_room = payload.get("room_id") or "general"

        if requested_session_id and self._mesh is not None:
            if not await self._session_manager.has_local_record(requested_session_id):
                snapshot = await self._mesh.query_session(requested_session_id)
                if snapshot is not None:
                    await self._session_manager.hydrate_from_snapshot(
                        requested_session_id, snapshot
                    )
                    logger.info(
                        "recovered session %s from mesh peer (HELLO path)",
                        requested_session_id,
                    )

        session_id = requested_session_id or str(uuid.uuid4())

        old_writer = self._connections.get(session_id)
        if old_writer is not None and old_writer is not writer:
            async def _evict(w: asyncio.StreamWriter):
                try:
                    await write_frame(w, MessageType.DISCONNECT, {"reason": "logged_in_elsewhere"})
                    await asyncio.sleep(0.05)  # flush
                except Exception:
                    pass
                finally:
                    w.close()
            asyncio.create_task(_evict(old_writer))

        self._connections[session_id] = writer

        session = await self._session_manager.get_or_create(
            session_id, room_id=requested_room
        )
        session.reset_inbound()
        await write_frame(
            writer, MessageType.HELLO_ACK,
            {
                "session_id": session_id,
                "room_id": session.room_id,
                "last_processed_seq": session.last_processed_seq,
            },
        )
        return session_id

    # ── Replay unacked messages on reconnect ──────────────────────────────

    async def _replay_unacked(
        self, session_id: str, writer: asyncio.StreamWriter
    ) -> None:
        session = await self._session_manager.get_or_create(session_id)

        # Replay any unacked messages still sitting in the outbox.
        # This is the only reconnect replay path needed for clients that
        # were disconnected by a server/node outage: they should receive
        # the messages that never made it to them, without re-sending the
        # entire room history again.
        for msg in session.replay_from(last_received_seq=0):
            await write_frame(writer, MessageType.MESSAGE, {
                "seq": msg.seq,
                **msg.payload,
            })

    # ── Main read loop ────────────────────────────────────────────────────

    async def _read_loop(
        self,
        session_id: str,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        while True:
            try:
                msg_type, payload = await asyncio.wait_for(read_frame(reader), timeout=0.5)
            except asyncio.TimeoutError:
                if writer.is_closing():
                    return
                continue
            except (ConnectionClosed, ConnectionResetError, OSError):
                return
            payload = payload or {}

            if msg_type == MessageType.MESSAGE:
                await self._handle_inbound_message(session_id, payload, writer)
            elif msg_type == MessageType.ACK:
                await self._session_manager.handle_ack(session_id, payload["seq"])
            elif msg_type == MessageType.JOIN_ROOM:
                await self._handle_join_room(session_id, payload, writer)
            elif msg_type == MessageType.GET_ROOMS:
                await self._handle_get_rooms(session_id, writer)
            elif msg_type == MessageType.PING:
                await write_frame(writer, MessageType.PONG)
            elif msg_type == MessageType.DISCONNECT:
                return
            else:
                logger.warning(
                    "session %s sent unexpected frame type %s", session_id, msg_type
                )

    # ── Feature 2: JOIN_ROOM with capacity check ──────────────────────────

    async def _handle_join_room(
        self,
        session_id: str,
        payload: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Join / switch to a room. Enforces max_capacity before moving
        the session. Creates the room row in SQLite if it doesn't
        exist yet (using DEFAULT_ROOM_CAPACITY).
        """
        room_id = (payload.get("room_id") or "general").strip()

        # Get or initialise the room metadata in SQLite.
        room_meta = await self._store.get_room(room_id)
        if room_meta is None:
            # First JOIN for this room -- create it with defaults.
            # created_by = the session's username is not tracked here
            # (session_id is opaque at this layer); use the session_id
            # as a stand-in creator identifier.
            await self._store.upsert_room(
                room_id=room_id,
                name=room_id,
                max_capacity=DEFAULT_ROOM_CAPACITY,
                created_by=session_id,
            )
            max_capacity = DEFAULT_ROOM_CAPACITY
        else:
            max_capacity = room_meta["max_capacity"]

        # Count how many sessions are ACTIVELY CONNECTED in this room.
        # This excludes disconnected sessions that are still in memory
        # for recovery -- only live sockets count toward capacity.
        current_count = len(self._get_connected_session_ids_in_room(room_id))

        # If the joiner is already IN this room, current_count already
        # includes them -- don't double-count. If they're switching
        # from another room, they're not counted yet.
        sender_session = self._session_manager.get_loaded(session_id)
        already_in_room = (
            sender_session is not None and sender_session.room_id == room_id
        )
        if already_in_room:
            # Not changing rooms -- still confirm with ROOM_JOINED.
            await write_frame(writer, MessageType.ROOM_JOINED, {"room_id": room_id})
            return

        if current_count >= max_capacity:
            await write_frame(writer, MessageType.ERROR, {
                "code": "room_full",
                "reason": (
                    f"room '{room_id}' is at capacity "
                    f"({current_count}/{max_capacity} users)"
                ),
            })
            return

        # Capacity OK -- move the session.
        new_room = await self._session_manager.set_room(session_id, room_id)
        await write_frame(writer, MessageType.ROOM_JOINED, {"room_id": new_room})
        logger.info(
            "session %s joined room %s (%d/%d)", session_id, new_room,
            current_count + 1, max_capacity
        )

    # ── Feature 2: GET_ROOMS discovery ───────────────────────────────────

    async def _handle_get_rooms(
        self,
        session_id: str,
        writer: asyncio.StreamWriter,
    ) -> None:
        """
        Return a ROOMS_LIST frame with all known rooms and their
        current live occupancy on this node.
        """
        room_rows = await self._store.get_all_rooms()

        rooms = []
        for row in room_rows:
            rid = row["room_id"]
            # Live count: only actively connected sessions (not disconnected ones).
            live_count = len(self._get_connected_session_ids_in_room(rid))
            rooms.append({
                "room_id": rid,
                "name": row["name"],
                "max_capacity": row["max_capacity"],
                "current_count": live_count,
                "created_by": row["created_by"],
            })

        await write_frame(writer, MessageType.ROOMS_LIST, {"rooms": rooms})

    # ── Message handling and broadcast ────────────────────────────────────

    async def _handle_inbound_message(
        self,
        session_id: str,
        payload: dict,
        writer: asyncio.StreamWriter,
    ) -> None:
        seq = payload["seq"]
        # Always ACK, even for duplicates.
        await write_frame(writer, MessageType.ACK, {"seq": seq})

        should_process = await self._session_manager.receive_message(
            session_id, seq, payload
        )

        if should_process:
            text = payload.get("text", "")
            sender_session = self._session_manager.get_loaded(session_id)
            room_id = sender_session.room_id if sender_session else "general"
            # Display name for the author: the logged-in username if we
            # have it, otherwise the session_id (HELLO-only clients).
            from_name = self._session_usernames.get(session_id, session_id)
            sent_at = payload.get("sent_at") or time.time()

            # Persist to message_log keyed by room so replay on reconnect
            # can load messages by room_id. Each message is stored once at
            # send time; the client ACKs are tracked in the outbox table.
            message_payload = {
                "text": text,
                "from": from_name,
                "room_id": room_id,
                "sent_at": sent_at,
            }
            await self._store.save_message_log(
                f"room:{room_id}", seq, message_payload, sent_at
            )

            await self._broadcast(
                from_session=session_id,
                from_name=from_name,
                room_id=room_id,
                text=text,
                sent_at=sent_at,
            )

    async def _broadcast(
        self, from_session: str, from_name: str, room_id: str,
        text: str, sent_at: float | None = None,
    ) -> None:
        await self._broadcast_local(
            room_id, text, from_session, from_name, sent_at=sent_at
        )
        if self._mesh is not None:
            # The mesh only needs a display identity to attribute the
            # message on remote nodes -- pass the author name as the
            # relay's "from_session". Remote nodes have no local session
            # with that value, so nobody is wrongly skipped there.
            await self._mesh.relay_message(room_id, text, from_name, sent_at=sent_at)

    async def _broadcast_local(
        self, room_id: str, text: str, from_session: str,
        from_name: str | None = None,
        sent_at: float | None = None,
    ) -> None:
        # `from_session` is the sender's real session_id, used to skip
        # echoing the message back to them. `from_name` is what recipients
        # see as the author; it defaults to from_session for callers (like
        # mesh relay delivery) that only have a display identity.
        display_from = from_name if from_name is not None else from_session
        payload = {
            "text": text,
            "from": display_from,
            "room_id": room_id,
        }
        if sent_at is not None:
            payload["sent_at"] = sent_at
        # Only send to ACTIVELY CONNECTED sessions. Stale/disconnected
        # sessions will get the message via outbox replay if they reconnect.
        for session_id in self._session_manager.session_ids_in_room(room_id):
            if session_id == from_session:
                continue
            seq = await self._session_manager.send_message(session_id, payload)
            target_writer = self._connections.get(session_id)
            if target_writer is not None:
                await write_frame(target_writer, MessageType.MESSAGE, {
                    "seq": seq,
                    **payload,
                })

    async def _deliver_relayed_message(
        self, msg_id: str, room_id: str, text: str, from_session: str,
        sent_at: float | None = None,
    ) -> None:
        logger.info(
            "delivering relayed message %s into room %s (from %s)",
            msg_id, room_id, from_session,
        )
        # `from_session` here is the origin node's display identity
        # (username or its session_id); use it directly as the author.
        await self._broadcast_local(
            room_id,
            text,
            from_session=from_session,
            from_name=from_session,
            sent_at=sent_at,
        )

    # ── LB dynamic registration ───────────────────────────────────────────

    async def _register_with_lb_loop(self) -> None:
        advertise_host = self._advertise_host or self.host

        while True:
            try:
                reader, writer = await asyncio.open_connection(
                    self._lb_register_host, self._lb_register_port
                )
                await write_frame(writer, MessageType.NODE_ANNOUNCE, {
                    "node_id": self.node_id,
                    "host": advertise_host,
                    "port": self._actual_port or self.port,
                })
                logger.info(
                    "registered with LB at %s:%s",
                    self._lb_register_host, self._lb_register_port,
                )
                while True:
                    await write_frame(writer, MessageType.NODE_STATUS, {
                        "healthy": True,
                        "connected_sessions": len(self._connections),
                    })
                    await asyncio.sleep(NODE_STATUS_INTERVAL_SECONDS)
            except (ConnectionError, OSError, asyncio.TimeoutError, FrameError, ConnectionClosed):
                pass
            await asyncio.sleep(LB_REGISTER_RECONNECT_DELAY_SECONDS)

    # ── Retry loop ────────────────────────────────────────────────────────

    async def _retry_loop(self) -> None:
        while True:
            await asyncio.sleep(RETRY_CHECK_INTERVAL_SECONDS)
            for session_id, writer in list(self._connections.items()):
                session = self._session_manager.get_loaded(session_id)
                if session is None:
                    continue
                for msg in session.pending_for_retry(RETRY_AFTER_SECONDS):
                    try:
                        await write_frame(writer, MessageType.MESSAGE, {
                            "seq": msg.seq, **msg.payload,
                        })
                        session.mark_retried(msg.seq)
                    except (ConnectionError, OSError):
                        pass

    # ── Cleanup ───────────────────────────────────────────────────────────

    async def _cleanup_connection(
        self, session_id: str | None, writer: asyncio.StreamWriter
    ) -> None:
        if session_id is not None:
            if self._connections.get(session_id) is writer:
                del self._connections[session_id]
        if not writer.is_closing():
            writer.close()

    def _get_connected_session_ids_in_room(self, room_id: str) -> list[str]:
        """
        Return only sessions that are ACTIVELY CONNECTED in room_id.
        Used for room member counts so dropped connections don't inflate
        the member list. Persistent (but disconnected) sessions are still
        in SessionManager._sessions for cross-node recovery and outbox
        replay, but only count here if they have a live socket.
        """
        return [
            session_id for session_id in self._session_manager.session_ids_in_room(room_id)
            if session_id in self._connections
        ]
