"""
Wires SessionState (in-memory bookkeeping) to OutboxStore (SQLite
persistence) so that:

  1. Every state-changing operation (send, ack, process-inbound)
     updates both the in-memory SessionState AND queues a durable
     write, in one call -- callers never have to remember to do both.
  2. On node startup, get_or_create() transparently recovers a
     session's outbox and seq counters from disk if they exist,
     instead of the caller having to know whether this is a fresh
     session or a recovered one.

This is the integration point referenced as "not built yet" in both
session_state.py and persistence.py's original docstrings.

Cross-node recovery: when a client reconnects with a session_id this
node has never seen (fresh disk AND fresh memory), that no longer
means the session is lost -- see mesh.py's SESSION_QUERY/SESSION_STATE
protocol. snapshot_session() / hydrate_from_snapshot() below are the
two halves of that handshake on THIS node's side: snapshot_session()
answers "do I have it, and if so here it is" when another node asks;
hydrate_from_snapshot() adopts a snapshot handed to us by another node
as if it were our own, including persisting it to our OWN disk so a
later reconnect finds it locally without another mesh round trip.
"""

from __future__ import annotations

import time

from persistence import OutboxStore
from session_state import SessionState


class SessionManager:
    """
    Owns all live SessionState objects for one node process, backed
    by a single OutboxStore. One instance per node.
    """

    def __init__(self, store: OutboxStore):
        self._store = store
        self._sessions: dict[str, SessionState] = {}

    async def get_or_create(self, session_id: str, room_id: str = "general") -> SessionState:
        """
        Returns the live SessionState for session_id. If it's already
        in memory (e.g. client reconnected without the node
        restarting), returns it as-is -- room_id is ignored in that
        case, since an existing session's room comes from its own
        persisted state, not from whatever a later caller happens to
        pass. Use set_room() to actually change a session's room.

        Otherwise attempts recovery from disk; if nothing is on disk
        either, creates a fresh session in room_id (or "general" by
        default).
        """
        existing = self._sessions.get(session_id)
        if existing is not None:
            return existing

        session = await self._recover_or_create(session_id, room_id)
        self._sessions[session_id] = session
        return session

    async def _recover_or_create(self, session_id: str, room_id: str) -> SessionState:
        saved = await self._store.load_session(session_id)
        outbox_rows = await self._store.load_outbox(session_id)

        if saved is None:
            # Nothing on disk -- brand new session, in the caller-requested room.
            return SessionState(session_id, room_id=room_id)

        session = SessionState(
            session_id,
            next_send_seq=saved["next_send_seq"],
            last_processed_seq=saved["last_processed_seq"],
            room_id=saved["room_id"],
        )
        session.load_outbox_rows(outbox_rows)

        # Load message history from the message_log table so we can replay
        # messages that were sent to this session but not yet ACKed on a
        # previous connection. This gives users their chat history back
        # when they reconnect after a disconnect or server restart.
        if saved["last_processed_seq"] > 0:
            history_rows = await self._store.load_message_history(
                session_id, after_seq=saved["last_processed_seq"]
            )
            for row in history_rows:
                session.outbox[row["seq"]] = type(
                    "OutboundMsg", (), {
                        "seq": row["seq"],
                        "payload": row["payload"],
                        "sent_at": row["sent_at"],
                        "retry_count": 0,
                    }
                )()

        return session

    async def set_room(self, session_id: str, room_id: str) -> str:
        """
        Move a session to a different room and persist it. Returns
        the room_id it's now in (mirrors the input, but keeps the
        call site symmetrical with other methods that return state).
        """
        session = await self.get_or_create(session_id)
        session.room_id = room_id
        await self._persist_session_meta(session)
        return session.room_id

    # --- Outbound: sending a message to the client -------------------------

    async def send_message(self, session_id: str, payload: dict) -> int:
        """
        Allocates a seq, buffers it in memory, and durably persists
        it. Returns the seq so the caller can write the actual
        MESSAGE frame to the socket. Order matters at the call site:
        persist before -- or at minimum no later than -- writing to
        the socket, so a crash can't produce a message the client
        saw but the node has no record of resending if needed.

        Also persists to the message_log table for history replay on reconnect.
        """
        session = await self.get_or_create(session_id)
        msg = session.prepare_outbound(payload)
        await self._store.save_outbound(session_id, msg.seq, msg.payload, msg.sent_at)
        # Persist to message_log so the client can see their sent messages on reconnect.
        await self._store.save_message_log(session_id, msg.seq, msg.payload, msg.sent_at)
        await self._persist_session_meta(session)
        return msg.seq

    async def handle_ack(self, session_id: str, seq: int) -> None:
        session = await self.get_or_create(session_id)
        if session.handle_ack(seq):
            await self._store.delete_outbound(session_id, seq)

    # --- Inbound: receiving a message from the client -----------------------

    async def receive_message(self, session_id: str, seq: int, payload: dict | None = None) -> bool:
        """
        Returns True if this message should actually be processed
        (delivered to other clients, etc). Returns False if it's a
        duplicate that should be ACKed again but not reprocessed.
        Either way, the watermark is persisted if it advanced.

        Also persists inbound messages to message_log so they can be
        replayed when this session reconnects from another node.
        """
        session = await self.get_or_create(session_id)
        should_process = session.should_process_inbound(seq)
        if should_process:
            session.mark_inbound_processed(seq)
            # Persist inbound messages for history replay on reconnect.
            # Use the actual payload from the sender so replayed messages
            # have real text/from/room_id instead of empty strings.
            msg_payload = payload or {"text": "", "from": "", "room_id": "general"}
            await self._store.save_message_log(
                session_id, seq, msg_payload, time.time()
            )
            await self._persist_session_meta(session)
        return should_process

    async def _persist_session_meta(self, session: SessionState) -> None:
        await self._store.save_session(
            session.session_id,
            next_send_seq=session._next_send_seq,
            last_processed_seq=session.last_processed_seq,
            room_id=session.room_id,
        )

    # --- Cross-node recovery (mesh SESSION_QUERY / SESSION_STATE) --------

    async def has_local_record(self, session_id: str) -> bool:
        """
        True if THIS node already knows about session_id -- either
        loaded in memory or recoverable from its own disk -- without
        creating anything. Callers (ChatNode's handshake) use this to
        decide whether a mesh-wide SESSION_QUERY is even worth the
        round trip: no point asking other nodes for state we already
        have right here.
        """
        if session_id in self._sessions:
            return True
        saved = await self._store.load_session(session_id)
        return saved is not None

    async def snapshot_session(self, session_id: str) -> dict | None:
        """
        A wire-transferable snapshot of session_id's full state, or
        None if this node has never heard of it. This is a pure read
        -- unlike get_or_create(), it never creates a session or
        touches _sessions/disk. Used as the reply when another node's
        SESSION_QUERY asks whether we have this session.
        """
        session = self._sessions.get(session_id)
        if session is not None:
            return {
                "next_send_seq": session._next_send_seq,
                "last_processed_seq": session.last_processed_seq,
                "room_id": session.room_id,
                "outbox": [
                    {
                        "seq": msg.seq,
                        "payload": msg.payload,
                        "sent_at": msg.sent_at,
                        "retry_count": msg.retry_count,
                    }
                    for msg in session.outbox.values()
                ],
            }

        saved = await self._store.load_session(session_id)
        if saved is None:
            return None
        outbox_rows = await self._store.load_outbox(session_id)
        return {**saved, "outbox": outbox_rows}

    async def hydrate_from_snapshot(self, session_id: str, snapshot: dict) -> SessionState:
        """
        Adopt a snapshot recovered from ANOTHER node (via a mesh
        SESSION_STATE reply) as our own live SessionState -- and
        persist it to THIS node's disk too, so a client that keeps
        bouncing between nodes doesn't pay a mesh round trip on every
        single reconnect, only the first one after landing somewhere
        new. Callers are expected to have already checked
        has_local_record() came back False before fetching a snapshot
        to hydrate, so this should never clobber live local state.
        """
        session = SessionState(
            session_id,
            next_send_seq=snapshot["next_send_seq"],
            last_processed_seq=snapshot["last_processed_seq"],
            room_id=snapshot["room_id"],
        )
        session.load_outbox_rows(snapshot["outbox"])
        self._sessions[session_id] = session

        await self._persist_session_meta(session)
        for msg in session.outbox.values():
            await self._store.save_outbound(session_id, msg.seq, msg.payload, msg.sent_at)

        return session

    # --- Introspection --------------------------------------------------

    def is_loaded(self, session_id: str) -> bool:
        """Whether this session is currently held in memory on this node."""
        return session_id in self._sessions

    def all_session_ids(self) -> list[str]:
        """
        Every session this node currently knows about, connected or
        not. Used for fan-out (broadcast) so offline sessions still
        get messages buffered for them, instead of only sessions
        that happen to have an open socket right now.
        """
        return list(self._sessions.keys())

    def session_ids_in_room(self, room_id: str) -> list[str]:
        """
        Every known session (connected or not) currently in room_id.
        Used to scope broadcast to one room instead of every session
        on the node.
        """
        return [sid for sid, s in self._sessions.items() if s.room_id == room_id]

    def get_loaded(self, session_id: str) -> SessionState | None:
        """
        Returns the in-memory SessionState if already loaded, without
        triggering recovery from disk. Used by callers (like the
        retry loop) that only care about sessions already active and
        shouldn't pay for a disk round-trip just to find out a
        session isn't loaded.
        """
        return self._sessions.get(session_id)