# node/mesh.py

"""
Node-to-node mesh layer.

PROBLEM THIS SOLVES: server.py's docstring is explicit that rooms are
local to one node -- two sessions in the same named room but on
different backend nodes cannot see each other, because ChatNode._broadcast
only ever looks at SessionManager.session_ids_in_room(), which only knows
about sessions THIS node has handled. This module removes that limitation
by giving every ChatNode a live link to every other ChatNode, so a
room-scoped broadcast reaches every node, not just the one the sender
happened to be routed to.

ARCHITECTURE: full mesh, not hub-and-spoke. Every node dials every other
configured peer AND accepts inbound connections from them -- there is no
central broker process. This matches the "Peer-to-Peer Mesh pattern for
backend state synchronization" half of the Stateful Proxy-Mesh
Architecture (the Ingress/LB layer is the OTHER half -- reverse proxy for
client-facing routing; this module never talks to clients at all).

WIRE PROTOCOL (separate TCP connections from the client-facing port --
each node listens on a second "mesh port" just for this):

    Node -> Node   PEER_HELLO {node_id}     (both sides send this first,
                                              immediately, symmetric
                                              regardless of who dialed)
    Node -> Node   RELAY {msg_id, room_id, text, from_session}
    Node -> Node   PING {}                   (active liveness probe, sent
                                              periodically by both sides)
    Node -> Node   PONG {}                    (reply to PING)
    Node -> Node   SESSION_QUERY {request_id, session_id}
                                              (cross-node session recovery --
                                               "does anyone have this session's
                                               state?", flooded to every peer)
    Node -> Node   SESSION_STATE {request_id, session_id, snapshot}
                                              (direct reply, ONLY from a node
                                               that actually has session_id --
                                               see CROSS-NODE RECOVERY below)

LIVENESS: a live TCP connection alone doesn't prove the peer process is
actually still doing useful work -- it could be hung (deadlocked, stuck in
a blocking call) while its socket stays technically open. Both sides send
PING on a timer and expect a PONG back within a grace period; missing it
means we forcibly close the link (see PEER_PING_INTERVAL_SECONDS /
PEER_PONG_GRACE_SECONDS below) so a hung peer stops silently absorbing
relayed messages that will never actually reach anyone.

LOOP PREVENTION: see dedup_cache.py's docstring for the full reasoning.
Short version: every RELAY carries a msg_id minted once at the origin
node; every node remembers msg_ids it has processed and silently drops
repeats instead of re-flooding them forever.

CROSS-NODE RECOVERY: persistence.py's OutboxStore and session_manager.py
are both explicit that a session's durable state lives on ONE node's
disk -- if ingress (lb.py) routes a reconnecting client to a
DIFFERENT node (LB restart, that node was briefly unhealthy, no
sticky mapping yet, etc), the new node has no record of it and would
otherwise treat it as brand new, silently dropping whatever was
buffered in the old node's outbox. query_session() closes that gap:
since this is a full mesh (every node already holds a direct link to
every other node -- see ARCHITECTURE above), a node that doesn't
recognize a session_id floods a SESSION_QUERY to every peer it's
connected to and waits up to a short timeout for the first
SESSION_STATE reply. Only the node that actually has the session
answers (see _handle_session_query below); everyone else stays
silent rather than adding noise for a session they don't have. No
further re-flooding is needed to reach the rest of the mesh, because
direct peers already ARE the whole mesh.
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
import uuid
from pathlib import Path
from typing import Awaitable, Callable

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "protocol"))

from framing import (  # noqa: E402
    ConnectionClosed,
    FrameError,
    MessageType,
    read_frame,
    write_frame,
)
from dedup_cache import DedupCache  # noqa: E402

logger = logging.getLogger("clustertalk.mesh")

PEER_RECONNECT_DELAY_SECONDS = 2.0
PEER_HELLO_TIMEOUT_SECONDS = 5.0

# How often we actively probe each peer, and how long we tolerate silence
# before declaring the link dead. PONG_GRACE is deliberately more than one
# PING_INTERVAL -- a single slow round trip (a GC pause, a burst of RELAY
# traffic queued ahead of the PONG, brief network jitter) shouldn't trip a
# false positive. Missing TWO consecutive PING intervals' worth of grace is
# a much stronger signal that the peer is actually gone (crashed, hung, or
# network-partitioned), not just momentarily slow.
PEER_PING_INTERVAL_SECONDS = 5.0
PEER_PONG_GRACE_SECONDS = 12.0

# How long query_session() waits for a SESSION_STATE reply before
# giving up and treating the session as genuinely not found anywhere
# in the mesh. Short on purpose -- this blocks a client's HELLO
# handshake, and every currently-connected peer answers (or doesn't)
# within one local dict lookup, so a slow reply almost always means
# that peer link is dead, not that it's still thinking.
SESSION_QUERY_TIMEOUT_SECONDS = 2.0

# Called when a NEW (non-duplicate) message arrives from the mesh and
# needs to be delivered to this node's own locally-connected sessions.
# Signature: (msg_id, room_id, text, from_session, sent_at) -> None
DeliverCallback = Callable[[str, str, str, str, float | None], Awaitable[None]]

# Called when a SESSION_QUERY arrives asking "do you have session_id?".
# Signature: (session_id) -> snapshot dict, or None if we don't have it.
SessionQueryProvider = Callable[[str], Awaitable[dict | None]]


class PeerManager:
    """
    Owns every live connection this node has to its mesh peers, in both
    directions (outbound dial-out and inbound accept). Once the
    PEER_HELLO handshake completes, a link is just "a connection to peer
    X" -- relay_message() (sending) and the internal read loop
    (receiving) both treat every link identically regardless of which
    side originally dialed.
    """

    def __init__(
        self,
        node_id: str,
        mesh_host: str,
        mesh_port: int,
        peer_addresses: list[tuple[str, int]],
        deliver_locally: DeliverCallback,
        session_query_provider: SessionQueryProvider | None = None,
    ):
        self.node_id = node_id
        self._mesh_host = mesh_host
        self._mesh_port = mesh_port
        self._peer_addresses = peer_addresses
        self._deliver_locally = deliver_locally
        # Optional: answers "do you have session_id's state?" for
        # incoming SESSION_QUERY frames. None (the default) means this
        # node never answers session queries -- e.g. a test harness
        # that only cares about RELAY behavior can skip wiring it up.
        self._session_query_provider = session_query_provider

        # request_id -> Future, resolved by the first SESSION_STATE
        # reply that names it. See query_session() / _handle_session_state.
        self._pending_queries: dict[str, asyncio.Future] = {}

        self._dedup = DedupCache()
        # peer_id -> writer. An entry only exists once handshake is done --
        # there's no half-registered/pending state visible to relay_message.
        # NOTE: since peer lists are usually configured symmetrically (both
        # A and B dial each other), it's normal to briefly get TWO live TCP
        # connections for the same peer_id (one outbound-dialed, one
        # inbound-accepted). The dict below only ever holds the most
        # recently established one per peer_id -- that's fine for
        # relay_message/_flood (a duplicate RELAY on the older link just
        # gets dropped by the receiver's dedup cache, no correctness
        # impact). But it means the OLDER link's writer isn't reachable
        # through this dict anymore once overwritten, so we separately
        # track every live writer in _all_writers purely so stop() can
        # actually close every connection -- without this, an "orphaned"
        # duplicate link stays open forever and asyncio's Server.wait_closed()
        # (which waits for ALL active connections, not just registered
        # ones) hangs indefinitely on shutdown.
        self._peer_writers: dict[str, asyncio.StreamWriter] = {}
        self._all_writers: set[asyncio.StreamWriter] = set()

        self._server: asyncio.AbstractServer | None = None
        self._connect_tasks: list[asyncio.Task] = []

    # --- lifecycle -----------------------------------------------------

    async def start(self) -> None:
        self._server = await asyncio.start_server(
            self._handle_inbound, self._mesh_host, self._mesh_port
        )
        addr = self._server.sockets[0].getsockname()
        logger.info("mesh listener on %s:%s (node_id=%s)", addr[0], addr[1], self.node_id)

        # Dial every configured peer in the background, forever-retrying.
        # Peers can start in any order; a peer that's temporarily down
        # should reconnect automatically once it's back, not stay down.
        for host, port in self._peer_addresses:
            self._connect_tasks.append(asyncio.create_task(self._connect_loop(host, port)))

    async def stop(self) -> None:
        for task in self._connect_tasks:
            task.cancel()
        for task in self._connect_tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Close EVERY live connection -- including any "orphaned" duplicate
        # link (see _all_writers' docstring above) -- before asking the
        # server to finish closing. asyncio's Server.wait_closed() waits
        # for all active connections to actually end, not just for the
        # listening socket to stop accepting; leaving even one open would
        # hang this call forever.
        for writer in list(self._all_writers):
            writer.close()

        if self._server is not None:
            self._server.close()
            try:
                # wait_closed() waits for every active connection's
                # handler to fully unwind, not just for the listening
                # socket to stop accepting. We've already closed every
                # writer above, so this should resolve almost instantly
                # -- but a bounded timeout here is a deliberate safety
                # net: a stuck connection handler must NEVER be able to
                # make node shutdown hang forever. 3 seconds is generous
                # for local cleanup to finish; if it's still not done by
                # then, we log it and move on rather than block whatever
                # called stop() (e.g. a supervisor process trying to
                # restart this node) indefinitely.
                await asyncio.wait_for(self._server.wait_closed(), timeout=3.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "mesh server did not report fully closed within 3s -- "
                    "proceeding with shutdown anyway"
                )

    # --- outbound: this node dials a configured peer --------------------

    async def _connect_loop(self, host: str, port: int) -> None:
        while True:
            try:
                reader, writer = await asyncio.open_connection(host, port)
                await self._run_handshake_and_serve(reader, writer)
            except (ConnectionError, OSError, asyncio.TimeoutError, FrameError, ConnectionClosed):
                pass
            # No backoff growth on purpose -- on a LAN, cheap retries and
            # fast recovery matter more than being polite about load.
            await asyncio.sleep(PEER_RECONNECT_DELAY_SECONDS)

    # --- inbound: a peer dials into this node's mesh port ---------------

    async def _handle_inbound(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            await self._run_handshake_and_serve(reader, writer)
        except (ConnectionError, OSError, asyncio.TimeoutError, FrameError, ConnectionClosed):
            pass

    # --- shared: handshake once, then read RELAY frames forever ---------

    async def _run_handshake_and_serve(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        peer_id = await self._do_peer_handshake(reader, writer)
        self._peer_writers[peer_id] = writer
        self._all_writers.add(writer)
        logger.info("mesh link established with peer_id=%s", peer_id)

        # Handshake success is itself fresh evidence of liveness -- start
        # the clock now rather than at "now minus something", so we never
        # false-positive a brand new link before it's even had a chance
        # to exchange one PING/PONG round trip.
        liveness = {"last_pong_at": time.monotonic()}

        read_task = asyncio.create_task(self._peer_read_loop(peer_id, reader, writer, liveness))
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(peer_id, writer, liveness))
        try:
            # Either the read loop dying (peer closed / protocol error)
            # or the heartbeat loop giving up (no PONG within grace) means
            # this link is done -- whichever happens first, tear down the
            # other half too rather than leaving a lopsided connection
            # (e.g. a heartbeat task still faithfully PINGing a socket
            # whose read side already errored out).
            done, pending = await asyncio.wait(
                {read_task, heartbeat_task}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            first_exc = None
            for task in done:
                exc = task.exception()
                if exc is not None and first_exc is None:
                    first_exc = exc
            if first_exc is not None:
                raise first_exc
        finally:
            # This block must guarantee both sub-tasks are cancelled and
            # awaited NO MATTER how we got here -- including the case
            # where THIS coroutine itself is cancelled while still inside
            # the `await asyncio.wait(...)` above (e.g. stop() cancelling
            # the outbound _connect_loop task). In that case the `pending`
            # cleanup above never runs at all, since we never reached past
            # the interrupted await -- leaving read_task/heartbeat_task
            # orphaned, still running detached from anything that awaits
            # them. An orphaned task that later raises (e.g. once we close
            # the writer two lines down) logs a noisy, unhandled "Task
            # exception was never retrieved" instead of failing cleanly.
            for task in (read_task, heartbeat_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(read_task, heartbeat_task, return_exceptions=True)

            # Only clear the registration if OUR writer is still the one
            # on file -- guards against a simultaneous-reconnect race
            # (both sides dial each other at once) clobbering a newer,
            # healthy link when the old one finally tears down.
            if self._peer_writers.get(peer_id) is writer:
                del self._peer_writers[peer_id]
            self._all_writers.discard(writer)
            writer.close()
            logger.info("mesh link with peer_id=%s closed", peer_id)

    async def _do_peer_handshake(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> str:
        """Both sides send PEER_HELLO immediately, then both read the
        other side's PEER_HELLO -- symmetric, regardless of who dialed."""
        await write_frame(writer, MessageType.PEER_HELLO, {"node_id": self.node_id})
        msg_type, payload = await asyncio.wait_for(
            read_frame(reader), timeout=PEER_HELLO_TIMEOUT_SECONDS
        )
        if msg_type != MessageType.PEER_HELLO or not payload or "node_id" not in payload:
            raise FrameError("expected PEER_HELLO as first mesh frame")
        return payload["node_id"]

    async def _peer_read_loop(
        self, peer_id: str, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
        liveness: dict,
    ) -> None:
        while True:
            msg_type, payload = await read_frame(reader)
            payload = payload or {}

            if msg_type == MessageType.PONG:
                # Peer answered our PING -- reset the silence clock the
                # heartbeat task is watching.
                liveness["last_pong_at"] = time.monotonic()
                continue

            if msg_type == MessageType.PING:
                # Peer is probing US -- reply immediately so ITS heartbeat
                # task sees us as alive too. Liveness checking is
                # symmetric: both sides ping, both sides must answer.
                await write_frame(writer, MessageType.PONG)
                continue

            if msg_type == MessageType.SESSION_QUERY:
                await self._handle_session_query(writer, payload)
                continue

            if msg_type == MessageType.SESSION_STATE:
                self._handle_session_state(payload)
                continue

            if msg_type != MessageType.RELAY:
                logger.warning("peer %s sent unexpected mesh frame type %s", peer_id, msg_type)
                continue

            msg_id = payload["msg_id"]
            if self._dedup.seen_before(msg_id):
                continue  # already processed this one -- mesh loop guard

            await self._deliver_locally(
                msg_id,
                payload["room_id"],
                payload["text"],
                payload["from_session"],
                payload.get("sent_at"),
            )
            # Flood onward to every OTHER peer we know about (not the one
            # that just sent it to us) -- this is what makes it a real
            # mesh instead of a hub-and-spoke star: a message can reach
            # nodes that aren't directly linked to the original sender.
            await self._flood(payload, exclude_peer_id=peer_id)

    async def _heartbeat_loop(
        self, peer_id: str, writer: asyncio.StreamWriter, liveness: dict,
    ) -> None:
        """
        Runs for the lifetime of one peer connection, concurrently with
        _peer_read_loop. Actively probes on a timer rather than passively
        waiting for the OS to notice a dead socket (which can take a long
        time, or never happen at all for a hung-but-still-open peer).
        """
        while True:
            await asyncio.sleep(PEER_PING_INTERVAL_SECONDS)
            try:
                await write_frame(writer, MessageType.PING)
            except (ConnectionError, OSError) as exc:
                raise ConnectionError(f"failed to send PING to peer {peer_id}") from exc

            silence = time.monotonic() - liveness["last_pong_at"]
            if silence > PEER_PONG_GRACE_SECONDS:
                raise ConnectionError(
                    f"peer {peer_id} unresponsive: no PONG in {silence:.1f}s "
                    f"(grace={PEER_PONG_GRACE_SECONDS}s) -- treating link as dead"
                )

    # --- sending: called by ChatNode when a local broadcast needs to
    # reach other nodes ---------------------------------------------------

    async def relay_message(self, room_id: str, text: str, from_session: str, sent_at: float | None = None) -> None:
        """
        Mints a fresh msg_id (this node is the origin of this hop across
        the mesh), records it in our own dedup cache (so if it ever loops
        back here, we recognize and drop it), and floods it to every
        currently-connected peer.
        """
        msg_id = str(uuid.uuid4())
        self._dedup.seen_before(msg_id)  # record origin; return value irrelevant here
        payload = {"msg_id": msg_id, "room_id": room_id, "text": text, "from_session": from_session}
        if sent_at is not None:
            payload["sent_at"] = sent_at
        await self._flood(payload, exclude_peer_id=None)

    async def _flood(self, payload: dict, exclude_peer_id: str | None) -> None:
        for peer_id, writer in list(self._peer_writers.items()):
            if peer_id == exclude_peer_id:
                continue
            try:
                await write_frame(writer, MessageType.RELAY, payload)
            except (ConnectionError, OSError):
                # That link is dying -- its own read loop will notice and
                # clean up the registration; nothing to do here.
                pass

    def known_peer_count(self) -> int:
        """Introspection for health/status reporting and tests."""
        return len(self._peer_writers)

    # --- cross-node session recovery -------------------------------------

    async def query_session(
        self, session_id: str, timeout: float = SESSION_QUERY_TIMEOUT_SECONDS
    ) -> dict | None:
        """
        Asks every currently-connected peer whether IT has session_id's
        state, and returns the first snapshot that comes back within
        `timeout` seconds. Returns None if nobody answers in time --
        either the session genuinely doesn't exist anywhere in the
        mesh, or every node that had it happens to be unreachable
        right now (caller falls back to treating it as brand new,
        same as today).

        No peers connected at all -- e.g. a single-node deployment, or
        this node just started and hasn't finished dialing yet --
        short-circuits to None immediately rather than waiting out the
        full timeout for a question nobody could possibly answer.
        """
        if not self._peer_writers:
            return None

        request_id = str(uuid.uuid4())
        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending_queries[request_id] = future
        try:
            payload = {"request_id": request_id, "session_id": session_id}
            for peer_id, writer in list(self._peer_writers.items()):
                try:
                    await write_frame(writer, MessageType.SESSION_QUERY, payload)
                except (ConnectionError, OSError):
                    # That link is dying -- just skip it, other peers
                    # may still be able to answer.
                    pass
            try:
                return await asyncio.wait_for(future, timeout=timeout)
            except asyncio.TimeoutError:
                return None
        finally:
            self._pending_queries.pop(request_id, None)

    async def _handle_session_query(
        self, writer: asyncio.StreamWriter, payload: dict
    ) -> None:
        """
        A peer is asking whether WE have session_id. Reply directly to
        THEM (not a flood -- this is point-to-point, unlike RELAY) only
        if we actually have it; otherwise stay silent, per the module
        docstring's "no unnecessary traffic" reasoning -- a mesh-wide
        query storm shouldn't turn into a mesh-wide reply storm too.
        """
        if self._session_query_provider is None:
            return
        session_id = payload.get("session_id")
        request_id = payload.get("request_id")
        if not session_id or not request_id:
            return

        snapshot = await self._session_query_provider(session_id)
        if snapshot is None:
            return

        try:
            await write_frame(writer, MessageType.SESSION_STATE, {
                "request_id": request_id,
                "session_id": session_id,
                "snapshot": snapshot,
            })
        except (ConnectionError, OSError):
            pass  # link died between the query arriving and our reply

    def _handle_session_state(self, payload: dict) -> None:
        """
        A SESSION_STATE reply arrived -- resolve the matching pending
        future, if we're still waiting on it. If query_session() has
        already timed out and moved on (request_id no longer pending),
        or a second, later reply for the same request_id shows up
        (multiple nodes rarely could both have stale copies), this is
        a harmless no-op: first reply wins, everything after is dropped.
        """
        request_id = payload.get("request_id")
        future = self._pending_queries.get(request_id)
        if future is not None and not future.done():
            future.set_result(payload.get("snapshot"))