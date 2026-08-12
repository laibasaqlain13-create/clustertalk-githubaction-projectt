"""
Ingress Layer: reverse proxy / load balancer sitting in front of the
Backend Mesh nodes.

Responsibilities:
    - Connection acceptance
    - Sticky session assignment (session_id -> backend node)
    - Health-checked backend selection
    - Raw bytes relay to the backend
    - Dynamic backend registration (NODE_ANNOUNCE / NODE_STATUS)

─────────────────────────────────────────────────────────────────────
FEATURE 3: REDIS-BACKED STICKY SESSIONS
─────────────────────────────────────────────────────────────────────

Before (in-memory only):
    self._sticky: dict[str, Backend] = {}
    Problem: LB restart loses all session→backend mappings. A
    reconnecting client gets load-balanced fresh to any healthy
    backend, which may not have their session state (unless mesh
    cross-node recovery is enabled -- it helps, but adds latency
    and still misses the in-flight-at-crash window).

After (Redis-backed):
    Sticky mappings are persisted in Redis as:
        key:   "ct:sticky:{session_id}"
        value: "host:port"   (e.g. "127.0.0.1:8765")
        TTL:   configurable (default 24 h)

    On every new session routing decision:
        Redis SET ct:sticky:{session_id} "host:port" EX {ttl}

    On every lookup (client reconnect or known session):
        Redis GET ct:sticky:{session_id}
        -> parse "host:port" -> find matching Backend object
        -> use it if healthy, fall through to round-robin if not

    On LB restart:
        No in-memory state to rebuild. Redis holds everything.
        First request for a session reads Redis, finds the backend,
        routes correctly -- zero stickiness loss.

    On backend deregistration (dynamic node going away):
        All sticky keys pointing to that backend are invalidated
        in Redis immediately (DEL in a pipeline) so they don't
        route future reconnects to a dead node.

Redis connection:
    Uses redis.asyncio (async Redis client, same event loop). The
    connection is a single client instance (not a pool) -- redis.asyncio
    handles connection multiplexing internally. The client is created
    lazily in start() and closed in stop().

    If Redis is unavailable at startup or during operation, the LB
    falls back to round-robin selection (no stickiness) and logs a
    WARNING. It does NOT crash. This makes Redis a "best-effort
    durability" layer, not a hard dependency -- the LB can still route
    traffic without it, just without cross-restart stickiness.

Redis key schema:
    ct:sticky:{session_id}  -> "{host}:{port}"  TTL: STICKY_TTL_SECONDS
    ct:lb:backends           -> not used (backends are config/dynamic,
                                not stored in Redis)

─────────────────────────────────────────────────────────────────────

How routing works, frame-by-frame (unchanged from before):
    1. Client connects. LB reads first frame RAW via read_raw_frame.
    2. LB decodes just that one frame to check it's a HELLO / LOGIN /
       REGISTER and pull out session_id, if present.
    3. LB picks a backend: check Redis for sticky mapping first,
       fall back to round-robin across healthy backends.
    4. LB opens a connection to the chosen backend, relays the raw
       bytes unmodified, reads the backend's HELLO_ACK / LOGIN_ACK,
       decodes it (to learn the session_id), saves to Redis.
    5. From here on: pure raw byte relay, no more frame parsing.

Note on Feature 1 (Auth) interaction:
    The LB now also accepts LOGIN (0x14) and REGISTER (0x12) as valid
    first frames from the client, not just HELLO (0x01). For LOGIN and
    REGISTER, session_id is not in the initial frame (it's returned in
    LOGIN_ACK by the backend). The LB relays the first frame raw, reads
    the backend's response raw, then decodes it to extract session_id
    from the LOGIN_ACK payload and write the sticky mapping to Redis.
    This is exactly the same as before for HELLO/HELLO_ACK -- just
    extended to handle the new frame types.
"""

# ingress/lb.py
from __future__ import annotations

import asyncio
import logging
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "protocol"))

from framing import (  # noqa: E402
    ConnectionClosed,
    FrameError,
    MessageType,
    decode_frame_body,
    read_frame,
    read_raw_frame,
    write_frame,
)

logger = logging.getLogger("clustertalk.ingress")

HELLO_TIMEOUT_SECONDS        = 5.0
HEALTH_CHECK_INTERVAL_SECONDS = 6.0
HEALTH_CHECK_TIMEOUT_SECONDS  = 4.0
RELAY_CHUNK_SIZE              = 65536

# Redis key prefix and TTL for sticky session entries.
# 24 hours is a reasonable default: long enough to survive overnight
# LB restarts without losing stickiness, short enough to auto-expire
# abandoned sessions that will never reconnect.
STICKY_KEY_PREFIX    = "ct:sticky:"
STICKY_TTL_SECONDS   = 86_400   # 24 hours


@dataclass
class Backend:
    host: str
    port: int
    healthy: bool = True
    # True for dynamically-registered backends (removed from pool on
    # disconnect); False for static --backend entries (never removed,
    # only marked healthy/unhealthy).
    dynamic: bool = False
    # Admin override: when True the operator has taken this server out of
    # rotation from the dashboard. The health-check loop leaves this flag
    # alone; routing skips the server regardless of its live health. This
    # is what powers the "take a server down / bring it up" control.
    manual_down: bool = False
    # A reconnecting node replaces its registration lease.  This prevents
    # an older socket's late cleanup from deleting the fresh registration.
    registration_epoch: int = 0

    @property
    def routable(self) -> bool:
        """A backend receives traffic only if it's both healthy AND not
        manually taken down by an admin."""
        return self.healthy and not self.manual_down

    @property
    def address(self) -> tuple[str, int]:
        return (self.host, self.port)

    @property
    def address_str(self) -> str:
        """Redis-storable "host:port" string."""
        return f"{self.host}:{self.port}"

    def __hash__(self):
        return hash(self.address)


class LoadBalancer:
    def __init__(
        self,
        backends: list[tuple[str, int]],
        host: str = "0.0.0.0",
        port: int = 9000,
        register_host: str = "0.0.0.0",
        register_port: int | None = None,
        # ── Feature 3: Redis config ─────────────────────────────────
        redis_host: str = "127.0.0.1",
        redis_port: int = 6379,
        redis_db: int = 0,
        redis_password: str | None = None,
        sticky_ttl_seconds: int = STICKY_TTL_SECONDS,
    ):
        self.host = host
        self.port = port
        self._backends: list[Backend] = [Backend(h, p) for h, p in backends]
        self._rr_index = 0

        # ── Admin telemetry ──────────────────────────────────────────
        # Live counters surfaced by the admin dashboard. The admin HTTP
        # server reads these from a SEPARATE thread, so every read/write
        # of the shared counters + the _backends list goes through this
        # lock to avoid "dict/list changed size during iteration" races.
        self._stats_lock = threading.Lock()
        self._route_counts: dict[str, int] = {}   # backend addr -> times routed
        self._total_routed = 0
        self._started_monotonic = time.monotonic()

        # Connected-client model (drives the dashboard's sticky/failover
        # demo and now also tracks real browser clients routed through the
        # LB). Each client has a "home" server it's stuck to, and a
        # "current" server it's routed to right now. Failover moves current
        # off a down server; failback returns it home when it recovers.
        self._clients: dict[str, dict] = {}
        self._client_seq = 0

        # ── Feature 3: In-memory cache + Redis ───────────────────────
        # _sticky_cache is a local write-through cache so frequent
        # reconnects from the same session_id don't hit Redis every
        # time. It is populated on every successful Redis read and
        # invalidated whenever a backend is deregistered.
        # The authoritative store is Redis.
        self._sticky_cache: dict[str, Backend] = {}

        self._redis_host = redis_host
        self._redis_port = redis_port
        self._redis_db = redis_db
        self._redis_password = redis_password
        self._sticky_ttl = sticky_ttl_seconds
        self._redis: Optional["redis.asyncio.Redis"] = None   # set in start()
        self._redis_available = False   # tracks whether Redis is reachable

        self._server: asyncio.AbstractServer | None = None
        self._health_task: asyncio.Task | None = None

        self._register_host = register_host
        self._register_port = register_port
        self._register_server: asyncio.AbstractServer | None = None

        # ── Live relay tracking (fixes: "servers down" doesn't disconnect
        # already-connected clients) ─────────────────────────────────────
        # Maps each Backend -> the set of asyncio Tasks currently relaying
        # bytes for a client pinned to it. Populated in _pump_bidirectional,
        # cleared as tasks finish. When a backend goes down (admin_set_down,
        # a failed health check, or dynamic deregistration) we cancel every
        # task in its set, which closes that client's socket and forces it
        # to reconnect and get routed to a live backend instead.
        self._active_relays: dict[Backend, set[asyncio.Task]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

        # ── Live relay tracking (fixes: "servers down" doesn't disconnect
        # already-connected clients) ─────────────────────────────────────
        # Maps each Backend -> the set of asyncio Tasks currently relaying
        # bytes for a client pinned to it. Populated in _pump_bidirectional,
        # cleared as tasks finish. When a backend goes down (admin_set_down,
        # a failed health check, or dynamic deregistration) we cancel every
        # task in its set, which closes that client's socket and forces it
        # to reconnect and get routed to a live backend instead.
        self._active_relays: dict[Backend, set[asyncio.Task]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def start(self) -> None:
        # Needed so admin_set_down (called from the admin HTTP thread) can
        # safely schedule task cancellation on this event loop.
        self._loop = asyncio.get_running_loop()

        # Try to connect to Redis. Non-fatal if it's unavailable.
        await self._init_redis()

        self._health_task = asyncio.create_task(self._health_check_loop())
        await self._check_all_backends()

        self._server = await asyncio.start_server(
            self._handle_client, self.host, self.port
        )
        addr = self._server.sockets[0].getsockname()
        logger.info(
            "Ingress listening on %s:%s (backends: %s, redis: %s)",
            addr[0], addr[1],
            [b.address for b in self._backends],
            "connected" if self._redis_available else "unavailable (in-memory fallback)",
        )

        if self._register_port is not None:
            self._register_server = await asyncio.start_server(
                self._handle_registration, self._register_host, self._register_port
            )
            reg_addr = self._register_server.sockets[0].getsockname()
            logger.info(
                "Ingress node-registration listener on %s:%s", reg_addr[0], reg_addr[1]
            )

    async def serve_forever(self) -> None:
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._health_task is not None:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        if self._register_server is not None:
            self._register_server.close()
            await self._register_server.wait_closed()
        if self._redis is not None:
            try:
                await self._redis.aclose()
            except Exception:
                pass

    # ── Feature 3: Redis helpers ──────────────────────────────────────────

    async def _init_redis(self) -> None:
        """
        Attempt to connect to Redis. Sets self._redis_available.
        If Redis is not reachable, the LB continues without stickiness
        persistence (falls back to in-memory cache only).
        """
        try:
            import redis.asyncio as aioredis
        except ImportError:
            logger.warning(
                "redis package not installed -- sticky sessions will be in-memory only. "
                "Install with: pip install redis"
            )
            self._redis_available = False
            return

        try:
            self._redis = aioredis.Redis(
                host=self._redis_host,
                port=self._redis_port,
                db=self._redis_db,
                password=self._redis_password,
                decode_responses=True,   # keys/values as str, not bytes
                socket_connect_timeout=2,
                socket_timeout=2,
            )
            # Ping to verify the connection actually works.
            await self._redis.ping()
            self._redis_available = True
            logger.info(
                "Redis connected at %s:%s (db=%d)",
                self._redis_host, self._redis_port, self._redis_db,
            )
        except Exception as exc:
            logger.warning(
                "Redis not available (%s) -- sticky sessions in-memory only.", exc
            )
            self._redis = None
            self._redis_available = False

    async def _redis_get_sticky(self, session_id: str) -> Backend | None:
        """
        Look up a sticky backend for session_id. Checks local cache
        first (fast path), then Redis (slow path on cache miss).
        Returns None if not found or if the stored backend no longer
        exists / is unhealthy.
        """
        # ── Local cache hit ───────────────────────────────────────────────
        cached = self._sticky_cache.get(session_id)
        if cached is not None and cached.routable:
            return cached

        # ── Redis lookup ──────────────────────────────────────────────────
        if not self._redis_available or self._redis is None:
            return None

        try:
            key = f"{STICKY_KEY_PREFIX}{session_id}"
            value = await self._redis.get(key)
            if value is None:
                return None

            # value is "host:port"
            host, _, port_str = value.rpartition(":")
            if not host or not port_str.isdigit():
                return None

            backend = self._find_backend(host, int(port_str))
            if backend is not None and backend.routable:
                # Populate local cache so the next lookup is fast.
                self._sticky_cache[session_id] = backend
                return backend

            return None

        except Exception as exc:
            logger.debug("Redis GET error for sticky lookup: %s", exc)
            return None

    async def _redis_set_sticky(self, session_id: str, backend: Backend) -> None:
        """
        Persist session_id -> backend in Redis (with TTL) and update
        the local cache. Fire-and-forget: a Redis write failure does NOT
        fail the routing -- the connection is already established at this
        point, and a missing sticky entry just means the next reconnect
        might hit a different backend (which mesh recovery handles).
        """
        # Always update local cache.
        self._sticky_cache[session_id] = backend

        if not self._redis_available or self._redis is None:
            return

        try:
            key = f"{STICKY_KEY_PREFIX}{session_id}"
            await self._redis.set(key, backend.address_str, ex=self._sticky_ttl)
        except Exception as exc:
            logger.debug("Redis SET error for sticky write: %s", exc)

    async def _redis_delete_sticky_for_backend(self, backend: Backend) -> None:
        """
        When a backend is deregistered, invalidate all Redis sticky keys
        pointing to it. We scan for keys with the backend's address as
        value. This is O(n) in the number of sticky entries -- acceptable
        for a node going away (a rare event), not for the hot path.
        Also clears local cache entries for this backend.
        """
        # Clear local cache.
        self._sticky_cache = {
            sid: b for sid, b in self._sticky_cache.items() if b is not backend
        }

        if not self._redis_available or self._redis is None:
            return

        try:
            target_value = backend.address_str
            # SCAN for all ct:sticky:* keys pointing to this backend.
            # We can't do a reverse index without a secondary data
            # structure -- SCAN is the right tradeoff here.
            keys_to_delete = []
            async for key in self._redis.scan_iter(f"{STICKY_KEY_PREFIX}*"):
                value = await self._redis.get(key)
                if value == target_value:
                    keys_to_delete.append(key)

            if keys_to_delete:
                await self._redis.delete(*keys_to_delete)
                logger.info(
                    "invalidated %d Redis sticky key(s) for deregistered backend %s",
                    len(keys_to_delete), backend.address_str,
                )
        except Exception as exc:
            logger.warning(
                "Redis cleanup error for deregistered backend %s: %s",
                backend.address_str, exc,
            )

    def _find_backend(self, host: str, port: int) -> Backend | None:
        """Find a Backend object by host:port. Used when deserialising Redis values."""
        for b in self._backends:
            if b.host == host and b.port == port:
                return b
        return None

    # ── Health checking ───────────────────────────────────────────────────

    async def _health_check_loop(self) -> None:
        while True:
            await asyncio.sleep(HEALTH_CHECK_INTERVAL_SECONDS)
            await self._check_all_backends()

    async def _check_all_backends(self) -> None:
        with self._stats_lock:
            backends = list(self._backends)
        await asyncio.gather(*(self._check_backend(b) for b in backends))

    async def _open_connection_with_timeout(self, host: str, port: int, timeout: float) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        return await asyncio.wait_for(asyncio.open_connection(host, port), timeout=timeout)

    async def _check_backend(self, backend: Backend) -> None:
        was_healthy = backend.healthy
        try:
            reader, writer = await self._open_connection_with_timeout(
                backend.host,
                backend.port,
                HEALTH_CHECK_TIMEOUT_SECONDS,
            )
            try:
                await write_frame(writer, MessageType.HELLO, {"health_check": True})
                await asyncio.wait_for(
                    read_raw_frame(reader), timeout=HEALTH_CHECK_TIMEOUT_SECONDS
                )
            finally:
                writer.close()
            backend.healthy = True
        except (OSError, asyncio.TimeoutError, ConnectionClosed, FrameError, TypeError):
            backend.healthy = False

        if backend.healthy != was_healthy:
            logger.info(
                "backend %s is now %s",
                backend.address,
                "healthy" if backend.healthy else "UNHEALTHY",
            )
            if not backend.healthy:
                # Real failure detected (not just an admin toggle) -- cut
                # loose anyone still relaying through this backend so they
                # reconnect and get routed to a live one.
                self._disconnect_backend_clients(backend)
            self._rebalance_clients()

    def _cancel_relay_tasks(self, backend: Backend) -> None:
        """Runs on the event loop thread. Cancels every active client<->
        backend relay task for this backend. Cancelling closes both ends
        of that client's connection (see _pump_bidirectional's finally
        block), so the client observes a disconnect and, per its own
        reconnect logic, dials back in and gets routed fresh."""
        tasks = self._active_relays.pop(backend, set())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            logger.info(
                "forcibly disconnected %d live client relay(s) on backend %s",
                len(tasks), backend.address,
            )

    def _disconnect_backend_clients(self, backend: Backend) -> None:
        """Thread-safe entry point. Safe to call from the admin HTTP
        server's thread (admin_set_down) or from the event loop itself
        (_check_backend, _deregister_dynamic_backend)."""
        if self._loop is None:
            return
        self._loop.call_soon_threadsafe(self._cancel_relay_tasks, backend)

    def _client_key(self, peer: tuple[str, int] | None, session_id: str | None) -> str:
        if session_id:
            return session_id
        if peer is None:
            return f"peer:{self._client_seq}"
        return f"{peer[0]}:{peer[1]}"

    def _remember_client(
        self,
        client_id: str,
        backend: Backend,
        *,
        peer: tuple[str, int] | None = None,
        session_id: str | None = None,
        active: bool = True,
        connection_id: str | None = None,
    ) -> None:
        """Track a real or simulated client so the admin dashboard can
        show where it is currently routed and where its home is."""
        with self._stats_lock:
            entry = self._clients.get(client_id, {})
            if not entry.get("home"):
                entry["home"] = backend.address_str
            entry["current"] = backend.address_str
            entry["active"] = active
            entry["backend"] = backend.address_str
            if session_id:
                entry["session_id"] = session_id
            if peer is not None:
                entry["peer"] = f"{peer[0]}:{peer[1]}"
            if connection_id is not None:
                entry["connection_id"] = connection_id
            self._clients[client_id] = entry

    def _migrate_client_state(self, old_id: str, new_id: str) -> None:
        """Move a client entry from a temporary peer-based identifier to a
        stable session_id once the backend has assigned one."""
        with self._stats_lock:
            entry = self._clients.pop(old_id, None)
            if entry is None:
                return
            entry["session_id"] = new_id
            self._clients[new_id] = entry

    def _drop_client(self, client_id: str, connection_id: str | None = None) -> None:
        """Mark a client as inactive after its relay closes.

        A reconnect can create a brand-new active connection for the same
        session. The stale connection must not deactivate that newer entry,
        so we only clear active state when the closing socket still owns the
        current connection_id.
        """
        with self._stats_lock:
            entry = self._clients.get(client_id)
            if entry is None:
                return
            if connection_id is not None and entry.get("connection_id") != connection_id:
                return
            entry["active"] = False
            self._clients[client_id] = entry

    # ── Dynamic node registration ─────────────────────────────────────────

    async def _handle_registration(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        backend: Backend | None = None
        registration_epoch: int | None = None
        try:
            msg_type, payload = await asyncio.wait_for(
                read_frame(reader), timeout=HELLO_TIMEOUT_SECONDS
            )
            if msg_type != MessageType.NODE_ANNOUNCE or not payload:
                writer.close()
                return

            node_host = payload.get("host")
            node_port = payload.get("port")
            node_id = payload.get("node_id", "?")
            if not node_host or not node_port:
                writer.close()
                return

            backend, registration_epoch = self._register_dynamic_backend(node_host, node_port)
            logger.info(
                "node %s registered dynamically as backend %s:%s",
                node_id, node_host, node_port,
            )

            while True:
                msg_type, payload = await read_frame(reader)
                if msg_type == MessageType.NODE_STATUS:
                    backend.healthy = (payload or {}).get("healthy", True)

        except (ConnectionError, OSError, asyncio.TimeoutError, FrameError, ConnectionClosed):
            pass
        finally:
            if backend is not None:
                await self._deregister_dynamic_backend(backend, registration_epoch)
            writer.close()

    def _register_dynamic_backend(self, host: str, port: int) -> tuple[Backend, int]:
        with self._stats_lock:   # admin thread may be iterating _backends
            for existing in self._backends:
                if existing.address == (host, port):
                    existing.healthy = True
                    existing.dynamic = True
                    existing.registration_epoch += 1
                    return existing, existing.registration_epoch
            backend = Backend(host, port, healthy=True, dynamic=True, registration_epoch=1)
            self._backends.append(backend)
            return backend, backend.registration_epoch

    async def _deregister_dynamic_backend(self, backend: Backend, registration_epoch: int | None) -> None:
        with self._stats_lock:   # admin thread may be iterating _backends
            if registration_epoch != backend.registration_epoch:
                logger.info("ignoring stale registration teardown for backend %s", backend.address)
                return
            if backend in self._backends:
                self._backends.remove(backend)
        # Invalidate Redis entries pointing to this backend.
        await self._redis_delete_sticky_for_backend(backend)
        # A dynamic node disappearing is a real failure -- cut its clients
        # loose so they reconnect through a live node.
        self._disconnect_backend_clients(backend)
        logger.info(
            "backend %s deregistered (registration connection closed)",
            backend.address,
        )

    # ── Admin telemetry ───────────────────────────────────────────────────

    def _record_route(self, backend: Backend) -> None:
        """Count one client routed to `backend` (thread-safe)."""
        with self._stats_lock:
            self._route_counts[backend.address_str] = (
                self._route_counts.get(backend.address_str, 0) + 1
            )
            self._total_routed += 1

    def admin_snapshot(self) -> dict:
        """
        A JSON-serialisable snapshot of the LB's live state for the admin
        dashboard. Safe to call from another thread (the admin HTTP server).
        """
        with self._stats_lock:
            backends = list(self._backends)
            route_counts = dict(self._route_counts)
            total_routed = self._total_routed
            active_clients = [c for c in self._clients.values() if c.get("active", True)]
            client_currents = [c.get("current") for c in active_clients if c.get("current")]
            client_homes = [c.get("home") for c in active_clients if c.get("home")]
        # sticky_cache is written on the event-loop thread without this
        # lock; copy defensively so a concurrent mutation can't crash us.
        try:
            sticky_items = list(self._sticky_cache.items())
        except RuntimeError:
            sticky_items = []
        sticky_per: dict[str, int] = {}
        for _sid, b in sticky_items:
            sticky_per[b.address_str] = sticky_per.get(b.address_str, 0) + 1

        clients_per: dict[str, int] = {}
        for cur in client_currents:
            if cur:
                clients_per[cur] = clients_per.get(cur, 0) + 1
        homes_per: dict[str, int] = {}
        for home in client_homes:
            homes_per[home] = homes_per.get(home, 0) + 1

        servers = [
            {
                "address": b.address_str,
                "host": b.host,
                "port": b.port,
                "healthy": b.healthy,
                "manual_down": b.manual_down,
                "routable": b.routable,
                # status the dashboard renders: up / down (crashed) / paused (admin)
                "status": ("paused" if b.manual_down else ("up" if b.healthy else "down")),
                "dynamic": b.dynamic,
                "requests_routed": clients_per.get(b.address_str, 0),
                "sticky_sessions": sticky_per.get(b.address_str, 0),
                "clients": clients_per.get(b.address_str, 0),      # routed here now
                "clients_home": homes_per.get(b.address_str, 0),   # belong here (sticky)
            }
            for b in backends
        ]
        routable_count = sum(1 for b in backends if b.routable)
        return {
            "algorithm": "sticky-session, then round-robin",
            "listen": f"{self.host}:{self.port}",
            "total_requests": total_routed,
            "server_count": len(backends),
            "healthy_count": routable_count,
            "sticky_total": len(sticky_items),
            "client_total": len(client_currents),
            "redis": self._redis_available,
            "uptime_seconds": round(time.monotonic() - self._started_monotonic, 1),
            "health_check_interval": HEALTH_CHECK_INTERVAL_SECONDS,
            "servers": servers,
        }

    def admin_set_down(self, address: str, down: bool) -> dict:
        """Admin control: take a server out of rotation (down=True) or put
        it back (down=False), by host:port. The process keeps running --
        the LB stops (or resumes) routing NEW traffic to it, and now also
        forcibly disconnects any REAL clients already relaying through it
        (previously only the dashboard's simulated clients "failed over";
        real client sockets were left untouched)."""
        with self._stats_lock:
            for b in self._backends:
                if b.address_str == address:
                    b.manual_down = bool(down)
                    self._rebalance_clients()
                    if down:
                        # Invalidate sticky sessions so reconnecting clients
                        # get routed to a healthy server instead of coming back
                        # to the manually-downed one.
                        self._sticky_cache = {
                            sid: bk for sid, bk in self._sticky_cache.items()
                            if bk is not b
                        }
                        # Use call_soon_threadsafe since admin_set_down may be
                        # called from the admin HTTP server's thread (no event
                        # loop running there). The coroutine runs on the LB's
                        # event loop where _redis_delete_sticky_for_backend can
                        # actually await properly.
                        async def _invalidate():
                            await self._redis_delete_sticky_for_backend(b)
                        self._loop.call_soon_threadsafe(
                            lambda: asyncio.ensure_future(_invalidate())
                        )
                        self._disconnect_backend_clients(b)
                    return {"ok": True, "address": address, "manual_down": b.manual_down}
        return {"ok": False, "reason": f"no server {address}"}

    def admin_remove_backend(self, address: str) -> bool:
        """Drop a backend from the routing pool immediately (used when an
        admin removes a server the dashboard spawned). Returns True if it
        was present."""
        with self._stats_lock:
            before = len(self._backends)
            self._backends = [b for b in self._backends if b.address_str != address]
            removed = len(self._backends) < before
            if removed:
                self._rebalance_clients()
        if removed:
            self._sticky_cache = {
                sid: b for sid, b in self._sticky_cache.items() if b.address_str != address
            }
        return removed

    # ── Connected-client pool (sticky + failover/failback demo) ───────────

    def admin_add_clients(self, n: int) -> dict:
        """Connect N new clients, distributed evenly (round-robin) across
        the healthy servers. Each one's assigned server becomes its sticky
        'home'."""
        with self._stats_lock:
            routable = [b.address_str for b in self._backends if b.routable]
            if not routable:
                return {"ok": False, "reason": "no healthy servers to connect to"}
            n = max(1, min(n, 500))
            for i in range(n):
                cid = f"c{self._client_seq}"
                self._client_seq += 1
                home = routable[i % len(routable)]
                self._clients[cid] = {"home": home, "current": home}
            return {"ok": True, "added": n, "total": len(self._clients)}

    def admin_clear_clients(self) -> dict:
        with self._stats_lock:
            self._clients = {}
            self._client_seq = 0
        return {"ok": True}

    def _rebalance_clients(self) -> None:
        """Reassign connected clients whenever the server set changes.
        A client whose home server is routable goes (back) home — that's
        sticky routing and failback. Clients whose home is down are spread
        evenly across the remaining routable servers — that's failover.
        Assumes the stats lock is already held."""
        routable = [b.address_str for b in self._backends if b.routable]
        rset = set(routable)
        displaced = []
        for cid, c in self._clients.items():
            if not c.get("active", True):
                continue
            if c.get("home") in rset:
                c["current"] = c["home"]       # sticky / failback
            else:
                displaced.append(cid)
        for i, cid in enumerate(displaced):     # failover, evenly spread
            self._clients[cid]["current"] = routable[i % len(routable)] if routable else None

    def admin_simulate(self, n: int) -> dict:
        """
        Simulate `n` fresh clients arriving at once, and route them across
        the currently-healthy servers exactly as the real round-robin path
        would. Increments the same counters the dashboard reads, so the
        traffic-distribution bars visibly react — this is how we demo the
        LB's behaviour under a burst of load without opening real sockets.
        """
        with self._stats_lock:
            healthy = [b for b in self._backends if b.routable]
            distribution: dict[str, int] = {}
            if not healthy:
                return {"ok": False, "reason": "no healthy servers", "distribution": {}}
            for i in range(n):
                b = healthy[i % len(healthy)]
                self._route_counts[b.address_str] = (
                    self._route_counts.get(b.address_str, 0) + 1
                )
                self._total_routed += 1
                distribution[b.address_str] = distribution.get(b.address_str, 0) + 1
        return {
            "ok": True,
            "simulated": n,
            "healthy_servers": len(healthy),
            "distribution": distribution,
        }

    # ── Routing ───────────────────────────────────────────────────────────

    async def _pick_backend(
        self, session_id: str | None, excluded: set[Backend] | None = None
    ) -> Backend | None:
        """
        Pick a backend for this session.
        1. If session_id is known: check Redis (via local cache) for a
           sticky mapping -- use it if the backend is still healthy.
        2. Otherwise: round-robin across currently healthy backends.
        """
        excluded = excluded or set()
        if session_id is not None:
            sticky_backend = await self._redis_get_sticky(session_id)
            if sticky_backend is not None and sticky_backend not in excluded:
                return sticky_backend
            # Sticky mapping missing or backend not routable -- fall through.

        routable = [b for b in self._backends if b.routable and b not in excluded]
        if not routable:
            return None
        backend = routable[self._rr_index % len(routable)]
        self._rr_index += 1
        return backend

    # ── Per-connection handling ───────────────────────────────────────────

    async def _handle_client(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
    ) -> None:
        peer = client_writer.get_extra_info("peername")
        backend_writer: asyncio.StreamWriter | None = None
        try:
            # ── Read the first frame raw ──────────────────────────────────
            try:
                raw_first = await asyncio.wait_for(
                    read_raw_frame(client_reader), timeout=HELLO_TIMEOUT_SECONDS
                )
            except (asyncio.TimeoutError, ConnectionClosed, FrameError):
                client_writer.close()
                return

            msg_type, payload = decode_frame_body(raw_first)

            # ── Accept HELLO, LOGIN, REGISTER as valid first frames ───────
            # Any other type is a protocol violation.
            valid_first = {
                MessageType.HELLO,
                MessageType.LOGIN,      # Feature 1
                MessageType.REGISTER,   # Feature 1
            }
            if msg_type not in valid_first:
                await write_frame(client_writer, MessageType.ERROR, {
                    "code": "expected_hello",
                    "reason": "first frame must be HELLO, LOGIN, or REGISTER",
                })
                client_writer.close()
                return

            # For HELLO: session_id may be in the payload (reconnect).
            # For LOGIN/REGISTER: session_id is not in the first frame --
            # it'll come back in LOGIN_ACK from the backend.
            session_id = (payload or {}).get("session_id")

            # ── Try to connect to backend (with retries if all are down) ──────
            backend = None
            backend_reader = None
            backend_writer = None
            current_frame_type = msg_type
            current_payload = payload
            current_raw = raw_first
            client_key = self._client_key(peer, session_id)
            connection_id = f"{peer[0]}:{peer[1]}:{time.monotonic_ns()}"
            
            # Keep looping to handle the case where client sends new frames
            # (e.g., LOGIN fallback after HELLO fails)
            while True:
                # ── Pick backend for current frame ────────────────────────────
                session_id = (current_payload or {}).get("session_id")
                backend = await self._pick_backend(session_id)
                if backend is not None:
                    self._remember_client(
                        client_key,
                        backend,
                        peer=peer,
                        session_id=session_id,
                        connection_id=connection_id,
                    )
                
                if backend is None:
                    # No healthy backends -- notify client and wait for next frame
                    await write_frame(client_writer, MessageType.ERROR, {
                        "code": "no_healthy_backend",
                        "reason": "all backend nodes are unavailable",
                    })
                    
                    # Try to read a new frame from the client (e.g., LOGIN fallback)
                    try:
                        current_raw = await asyncio.wait_for(
                            read_raw_frame(client_reader),
                            timeout=5.0
                        )
                        current_frame_type, current_payload = decode_frame_body(current_raw)
                        # Got a new frame -- loop back and try to route it
                        continue
                    except (asyncio.TimeoutError, ConnectionClosed, FrameError):
                        # Client didn't send a new frame in time, give up
                        client_writer.close()
                        return

                # ── Try to open backend connection ────────────────────────────
                attempted: set[Backend] = set()
                while backend is not None:
                    attempted.add(backend)
                    try:
                        backend_reader, backend_writer = await self._open_connection_with_timeout(
                            backend.host,
                            backend.port,
                            HEALTH_CHECK_TIMEOUT_SECONDS,
                        )
                        break
                    except (OSError, asyncio.TimeoutError):
                        backend.healthy = False
                        self._disconnect_backend_clients(backend)
                        backend = await self._pick_backend(session_id, attempted)

                # Check if we successfully connected
                if backend is not None and backend_reader is not None and backend_writer is not None:
                    break  # Success!
                
                # Connection failed, notify client and wait for retry
                await write_frame(client_writer, MessageType.ERROR, {
                    "code": "no_healthy_backend",
                    "reason": "all backend nodes are unavailable",
                })
                
                try:
                    current_raw = await asyncio.wait_for(
                        read_raw_frame(client_reader),
                        timeout=5.0
                    )
                    current_frame_type, current_payload = decode_frame_body(current_raw)
                except (asyncio.TimeoutError, ConnectionClosed, FrameError):
                    client_writer.close()
                    return

            self._record_route(backend)
            backend_writer.write(current_raw)
            await backend_writer.drain()

            # ── Read backend response + extract session_id ────────────────
            raw_ack = await read_raw_frame(backend_reader)
            ack_type, ack_payload = decode_frame_body(raw_ack)

            # Both HELLO_ACK and LOGIN_ACK carry session_id on success.
            if ack_type in (MessageType.HELLO_ACK, MessageType.LOGIN_ACK, MessageType.REGISTER_ACK):
                if ack_payload:
                    assigned_sid = ack_payload.get("session_id")
                    logger.info(
                            "LOGIN_ACK session=%s backend=%s",
                            assigned_sid,
                            backend.address
                        )
                    if assigned_sid:
                        if assigned_sid != client_key:
                            self._migrate_client_state(client_key, assigned_sid)
                            client_key = assigned_sid
                        self._remember_client(
                            client_key,
                            backend,
                            peer=peer,
                            session_id=assigned_sid,
                            connection_id=connection_id,
                        )
                        # Write the sticky mapping to Redis.
                        await self._redis_set_sticky(assigned_sid, backend)

            client_writer.write(raw_ack)
            await client_writer.drain()

            logger.info(
                "session routed to backend %s (peer %s, type=%s)",
                backend.address, peer, msg_type.name,
            )

            # ── Pure byte relay from here on ──────────────────────────────
            await self._pump_bidirectional(
                client_reader, client_writer, backend_reader, backend_writer, backend, client_key
            )

        except (ConnectionError, OSError):
            pass
        finally:
            if client_key:
                self._drop_client(client_key, connection_id)
            client_writer.close()
            if backend_writer is not None:
                backend_writer.close()

    async def _pump_bidirectional(
        self,
        client_reader: asyncio.StreamReader,
        client_writer: asyncio.StreamWriter,
        backend_reader: asyncio.StreamReader,
        backend_writer: asyncio.StreamWriter,
        backend: Backend,
        client_key: str,
    ) -> None:
        async def pump(
            src: asyncio.StreamReader, dst: asyncio.StreamWriter
        ) -> None:
            try:
                while True:
                    chunk = await src.read(RELAY_CHUNK_SIZE)
                    if not chunk:
                        return
                    dst.write(chunk)
                    await dst.drain()
            except (ConnectionError, OSError):
                return

        c2b = asyncio.create_task(pump(client_reader, backend_writer))
        b2c = asyncio.create_task(pump(backend_reader, client_writer))

        # Register so admin_set_down / a failed health check / dynamic
        # deregistration can forcibly cut this client over later.
        self._active_relays.setdefault(backend, set()).update({c2b, b2c})
        try:
            done, pending = await asyncio.wait(
                {c2b, b2c}, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
        finally:
            for task in (c2b, b2c):
                if not task.done():
                    task.cancel()
            relays = self._active_relays.get(backend)
            if relays is not None:
                relays.discard(c2b)
                relays.discard(b2c)
                if not relays:
                    self._active_relays.pop(backend, None)
