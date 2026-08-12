
"""
Crash-safe persistence for outbox + session metadata, backed by
SQLite in WAL mode.
s
What this solves: session_state.py holds everything in RAM. If a
node process crashes or restarts, every unacked outbouSSSSSnd message
and every session's seq counters are gone -- silently breaking the
zero-message-loss guarantee. This module makes that state durable.

What this does NOT solve: if a client gets routed to a *different*
node after the original node dies (ingress failover), the new node
has no access to this SQLite file -- it's local to one node's disk.
Cross-node recovery needs replication or a shared store, which is a
separate piece of work. This module only protects against
same-node crash-and-restart.

Design constraints:
    - SQLite calls are blocking. We must never call them directly on
      the asyncio event loop thread, or we stall every connection on
      this node during disk I/O. All DB access goes through a single
      dedicated worker thread (not the default thread pool) so that
      the one sqlite3.Connection object is only ever touched from
      one thread, which is a hard SQLite requirement.
    - Writes are batched: individual message saves/deletes are queued
      and committed together on a timer, not fsync'd one at a time.
      At 10k+ connections, per-message fsync would dominate latency.
      The tradeoff: a crash can lose up to one batch interval's worth
      of writes. Default flush interval is 50ms -- tune per deployment.

--- FEATURE 1: REAL AUTHENTICATION ---
    Added 'users' table:
        username       TEXT PRIMARY KEY
        password_hash  TEXT NOT NULL      (bcrypt hash)
        created_at     REAL NOT NULL

    New async methods:
        create_user(username, password_hash) -> bool
            Inserts a new user. Returns False if username already
            exists (UNIQUE constraint). Runs synchronously via the
            write queue since user creation is a write op.

        get_user(username) -> dict | None
            Returns {username, password_hash} or None. Read, so
            runs via asyncio.to_thread (same as load_session).

--- FEATURE 2: ROOM MANAGEMENT ---
    Added 'rooms' table:
        room_id        TEXT PRIMARY KEY
        name           TEXT NOT NULL
        max_capacity   INTEGER NOT NULL DEFAULT 50
        created_by     TEXT NOT NULL      (username of creator)
        created_at     REAL NOT NULL

    New async methods:
        upsert_room(room_id, name, max_capacity, created_by)
            Creates the room if it doesn't exist yet. Called by
            ChatNode the first time a session JOINs a named room.
            Runs via write queue (batched).

        get_room(room_id) -> dict | None
            Returns room metadata or None.

        get_all_rooms() -> list[dict]
            Returns all rooms for the ROOMS_LIST discovery frame.
            active_count is computed from the in-memory session
            state in SessionManager and injected at query time --
            it is NOT persisted here, since it's always derivable
            from who is currently connected.

    Both upsert_room and get_room/get_all_rooms use
    asyncio.to_thread for reads and the write queue for writes,
    consistent with every other operation in this module.
"""

from __future__ import annotations

import asyncio
import queue
import sqlite3
import threading
import time
from dataclasses import dataclass

import orjson

# ── Schema ────────────────────────────────────────────────────────────────

_SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    session_id         TEXT PRIMARY KEY,
    next_send_seq      INTEGER NOT NULL,
    last_processed_seq INTEGER NOT NULL,
    room_id            TEXT NOT NULL DEFAULT 'general',
    updated_at         REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox (
    session_id  TEXT    NOT NULL,
    seq         INTEGER NOT NULL,
    payload     BLOB    NOT NULL,
    sent_at     REAL    NOT NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (session_id, seq)
);

-- Feature 1: Real Authentication
CREATE TABLE IF NOT EXISTS users (
    username      TEXT PRIMARY KEY,
    password_hash TEXT NOT NULL,
    created_at    REAL NOT NULL
);

-- Feature 2: Room Management
-- active_count is NOT stored here -- it is always computed from
-- the live SessionManager (who is currently in the room) so it's
-- always accurate and never needs a separate update path.
CREATE TABLE IF NOT EXISTS rooms (
    room_id      TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    max_capacity INTEGER NOT NULL DEFAULT 50,
    created_by   TEXT NOT NULL,
    created_at   REAL NOT NULL
);

-- Store all messages per session for history replay on reconnect.
-- Each message is stored once at send time; the client ACKs are
-- tracked in the outbox table. On reconnect we replay everything
-- from this table starting after last_processed_seq.
CREATE TABLE IF NOT EXISTS message_log (
    session_id  TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    payload     BLOB NOT NULL,
    sent_at     REAL NOT NULL,
    PRIMARY KEY (session_id, seq)
);
"""

# ── Internal write-op dataclass ───────────────────────────────────────────

@dataclass
class _WriteOp:
    kind: str   # see _apply_batch for the full set
    args: tuple


class OutboxStore:
    """
    Async-facing handle to a SQLite-backed outbox. All actual SQLite
    work happens on a dedicated background thread; this class only
    queues work and exposes async methods the event loop can await.
    """

    def __init__(self, db_path: str, flush_interval_seconds: float = 0.05):
        self._db_path = db_path
        self._flush_interval = flush_interval_seconds
        self._write_queue: queue.Queue[_WriteOp | None] = queue.Queue()
        self._worker: threading.Thread | None = None
        self._stop_event = threading.Event()

    # ── Lifecycle ─────────────────────────────────────────────────────────

    def start(self) -> None:
        """Open the DB, apply schema/WAL pragma, start the writer thread."""
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.executescript(_SCHEMA)
        conn.commit()
        conn.close()

        self._worker = threading.Thread(
            target=self._run_writer_loop, name="outbox-writer", daemon=True
        )
        self._worker.start()

    def stop(self) -> None:
        """Signal the writer thread to flush remaining writes and exit."""
        self._stop_event.set()
        self._write_queue.put(None)  # wake the thread if it's blocked
        if self._worker is not None:
            self._worker.join(timeout=5)

    # ── Writer thread ─────────────────────────────────────────────────────

    def _run_writer_loop(self) -> None:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")

        pending: list[_WriteOp] = []
        last_flush = time.monotonic()

        while not self._stop_event.is_set() or pending or not self._write_queue.empty():
            timeout = max(0.0, self._flush_interval - (time.monotonic() - last_flush))
            try:
                op = self._write_queue.get(timeout=timeout or 0.001)
                if op is not None:
                    pending.append(op)
            except queue.Empty:
                pass

            now = time.monotonic()
            should_flush = pending and (
                now - last_flush >= self._flush_interval
                or self._stop_event.is_set()
            )
            if should_flush:
                self._apply_batch(conn, pending)
                pending = []
                last_flush = now

        conn.close()

    def _apply_batch(self, conn: sqlite3.Connection, ops: list[_WriteOp]) -> None:
        try:
            for op in ops:
                if op.kind == "save_session":
                    session_id, next_send_seq, last_processed_seq, room_id = op.args
                    conn.execute(
                        """
                        INSERT INTO sessions
                            (session_id, next_send_seq, last_processed_seq, room_id, updated_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(session_id) DO UPDATE SET
                            next_send_seq      = excluded.next_send_seq,
                            last_processed_seq = excluded.last_processed_seq,
                            room_id            = excluded.room_id,
                            updated_at         = excluded.updated_at
                        """,
                        (session_id, next_send_seq, last_processed_seq, room_id, time.time()),
                    )

                elif op.kind == "save_outbound":
                    session_id, seq, payload_bytes, sent_at = op.args
                    conn.execute(
                        """
                        INSERT INTO outbox (session_id, seq, payload, sent_at, retry_count)
                        VALUES (?, ?, ?, ?, 0)
                        ON CONFLICT(session_id, seq) DO UPDATE SET
                            sent_at     = excluded.sent_at,
                            retry_count = outbox.retry_count + 1
                        """,
                        (session_id, seq, payload_bytes, sent_at),
                    )

                elif op.kind == "delete_outbound":
                    session_id, seq = op.args
                    conn.execute(
                        "DELETE FROM outbox WHERE session_id = ? AND seq = ?",
                        (session_id, seq),
                    )

                # ── Feature 1 write ops ───────────────────────────────────
                elif op.kind == "create_user":
                    username, password_hash = op.args
                    # INSERT OR IGNORE: duplicate usernames are silently
                    # dropped here; the caller checks via a read first
                    # or handles the UNIQUE violation via the result flag.
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO users (username, password_hash, created_at)
                        VALUES (?, ?, ?)
                        """,
                        (username, password_hash, time.time()),
                    )

                # ── Feature 2 write ops ───────────────────────────────────
                elif op.kind == "upsert_room":
                    room_id, name, max_capacity, created_by = op.args
                    # Only creates the row if it doesn't exist -- never
                    # overwrites an existing room's capacity or owner.
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO rooms
                            (room_id, name, max_capacity, created_by, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (room_id, name, max_capacity, created_by, time.time()),
                    )

                # ── Message log write ops ───────────────────────────────────
                elif op.kind == "save_message_log":
                    session_id, seq, payload_bytes, sent_at = op.args
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO message_log (session_id, seq, payload, sent_at)
                        VALUES (?, ?, ?, ?)
                        """,
                        (session_id, seq, payload_bytes, sent_at),
                    )

            conn.commit()
        except sqlite3.Error:
            conn.rollback()
            raise

    # ── Async write API (called from the event loop) ──────────────────────

    async def save_session(
        self,
        session_id: str,
        next_send_seq: int,
        last_processed_seq: int,
        room_id: str = "general",
    ) -> None:
        self._write_queue.put(
            _WriteOp("save_session", (session_id, next_send_seq, last_processed_seq, room_id))
        )

    async def save_outbound(
        self, session_id: str, seq: int, payload: dict, sent_at: float
    ) -> None:
        payload_bytes = orjson.dumps(payload)
        self._write_queue.put(
            _WriteOp("save_outbound", (session_id, seq, payload_bytes, sent_at))
        )

    async def delete_outbound(self, session_id: str, seq: int) -> None:
        self._write_queue.put(_WriteOp("delete_outbound", (session_id, seq)))

    # ── Feature 1: User auth write API ───────────────────────────────────

    async def create_user(self, username: str, password_hash: str) -> bool:
        """
        Queue a user INSERT. Returns True immediately (fire-and-forget
        into the batch queue). The caller must check get_user() first
        to detect duplicates, since the batch queue is async -- there
        is a tiny race window between the check and the write, which
        is acceptable for a registration flow (the UNIQUE constraint
        in SQLite is the final guard).
        """
        self._write_queue.put(_WriteOp("create_user", (username, password_hash)))
        return True

    # ── Feature 2: Room write API ─────────────────────────────────────────

    async def upsert_room(
        self,
        room_id: str,
        name: str,
        max_capacity: int,
        created_by: str,
    ) -> None:
        """
        Ensure the room row exists. Idempotent -- if the room already
        exists, this is a no-op (INSERT OR IGNORE).
        """
        self._write_queue.put(
            _WriteOp("upsert_room", (room_id, name, max_capacity, created_by))
        )

    # ── Async read API (recovery on startup) ──────────────────────────────
    #
    # Reads bypass the write queue (they need the answer now) and run
    # via asyncio.to_thread instead. SQLite WAL mode allows a reader
    # to run concurrently with the writer thread's connection safely.

    async def load_session(self, session_id: str) -> dict | None:
        return await asyncio.to_thread(self._load_session_sync, session_id)

    def _load_session_sync(self, session_id: str) -> dict | None:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        try:
            row = conn.execute(
                "SELECT next_send_seq, last_processed_seq, room_id "
                "FROM sessions WHERE session_id = ?",
                (session_id,),
            ).fetchone()
            if row is None:
                return None
            return {
                "next_send_seq": row[0],
                "last_processed_seq": row[1],
                "room_id": row[2],
            }
        finally:
            conn.close()

    async def load_outbox(self, session_id: str) -> list[dict]:
        return await asyncio.to_thread(self._load_outbox_sync, session_id)

    def _load_outbox_sync(self, session_id: str) -> list[dict]:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        try:
            rows = conn.execute(
                "SELECT seq, payload, sent_at, retry_count FROM outbox "
                "WHERE session_id = ? ORDER BY seq",
                (session_id,),
            ).fetchall()
            return [
                {
                    "seq": seq,
                    "payload": orjson.loads(payload),
                    "sent_at": sent_at,
                    "retry_count": retry_count,
                }
                for seq, payload, sent_at, retry_count in rows
            ]
        finally:
            conn.close()

    # ── Feature 1: User auth read API ────────────────────────────────────

    async def get_user(self, username: str) -> dict | None:
        """
        Look up a user by username. Returns {username, password_hash}
        or None if not found. Used by the LOGIN handler to verify
        credentials.
        """
        return await asyncio.to_thread(self._get_user_sync, username)

    def _get_user_sync(self, username: str) -> dict | None:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        try:
            row = conn.execute(
                "SELECT username, password_hash FROM users WHERE username = ?",
                (username,),
            ).fetchone()
            if row is None:
                return None
            return {"username": row[0], "password_hash": row[1]}
        finally:
            conn.close()

    # ── Feature 2: Room read API ──────────────────────────────────────────

    async def get_room(self, room_id: str) -> dict | None:
        """
        Return {room_id, name, max_capacity, created_by} or None.
        Used by JOIN_ROOM handler to enforce capacity limits.
        """
        return await asyncio.to_thread(self._get_room_sync, room_id)

    def _get_room_sync(self, room_id: str) -> dict | None:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        try:
            row = conn.execute(
                "SELECT room_id, name, max_capacity, created_by "
                "FROM rooms WHERE room_id = ?",
                (room_id,),
            ).fetchone()
            if row is None:
                return None
            return {
                "room_id": row[0],
                "name": row[1],
                "max_capacity": row[2],
                "created_by": row[3],
            }
        finally:
            conn.close()

    async def get_all_rooms(self) -> list[dict]:
        """
        Return all room rows (without active_count -- that's injected
        by the caller from SessionManager's live data).
        """
        return await asyncio.to_thread(self._get_all_rooms_sync)

    def _get_all_rooms_sync(self) -> list[dict]:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        try:
            rows = conn.execute(
                "SELECT room_id, name, max_capacity, created_by FROM rooms ORDER BY name"
            ).fetchall()
            return [
                {
                    "room_id": row[0],
                    "name": row[1],
                    "max_capacity": row[2],
                    "created_by": row[3],
                }
                for row in rows
            ]
        finally:
            conn.close()

    # ── Message log (persistent message history) ────────────────────────

    async def save_message_log(
        self, session_id: str, seq: int, payload: dict, sent_at: float
    ) -> None:
        """Persist a message to the log so it can be replayed on reconnect."""
        payload_bytes = orjson.dumps(payload)
        self._write_queue.put(
            _WriteOp("save_message_log", (session_id, seq, payload_bytes, sent_at))
        )

    async def load_message_history(
        self, session_id: str, after_seq: int = 0
    ) -> list[dict]:
        """
        Return all messages for session_id with seq > after_seq.
        Used to replay message history on reconnect.
        """
        return await asyncio.to_thread(
            self._load_message_history_sync, session_id, after_seq
        )

    def _load_message_history_sync(
        self, session_id: str, after_seq: int
    ) -> list[dict]:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        try:
            rows = conn.execute(
                "SELECT seq, payload, sent_at FROM message_log "
                "WHERE session_id = ? AND seq > ? ORDER BY seq",
                (session_id, after_seq),
            ).fetchall()
            return [
                {
                    "seq": row[0],
                    "payload": orjson.loads(row[1]),
                    "sent_at": row[2],
                }
                for row in rows
            ]
        finally:
            conn.close()

    async def save_message_log_by_key(
        self, key: str, seq: int, payload: dict, sent_at: float
    ) -> None:
        """Persist a message to the log using an arbitrary key (e.g. 'room:general')."""
        payload_bytes = orjson.dumps(payload)
        self._write_queue.put(
            _WriteOp("save_message_log", (key, seq, payload_bytes, sent_at))
        )

    async def load_message_history_for_room(
        self, room_id: str, after_seq: int = 0
    ) -> list[dict]:
        """
        Return all messages for a room with seq > after_seq.
        Used to replay message history on reconnect.
        """
        return await asyncio.to_thread(
            self._load_message_history_for_room_sync, room_id, after_seq
        )

    def _load_message_history_for_room_sync(
        self, room_id: str, after_seq: int
    ) -> list[dict]:
        conn = sqlite3.connect(self._db_path, check_same_thread=False)
        try:
            # Match keys like "room:general", "room:engineering", etc.
            rows = conn.execute(
                "SELECT seq, payload, sent_at FROM message_log "
                "WHERE session_id LIKE ? AND seq > ? ORDER BY seq",
                (f"room:{room_id}%", after_seq),
            ).fetchall()
            return [
                {
                    "seq": row[0],
                    "payload": orjson.loads(row[1]),
                    "sent_at": row[2],
                }
                for row in rows
            ]
        finally:
            conn.close()
