"""
Unit tests for mesh.py's PeerManager, isolated from ChatNode/server.py
entirely -- these spin up two PeerManagers directly and exercise the
mesh wire protocol (RELAY loop-prevention and the SESSION_QUERY /
SESSION_STATE cross-node recovery handshake) against fake
deliver_locally / session_query_provider callbacks, so a failure here
localizes to the mesh layer itself rather than the whole node stack.
"""

import asyncio
import socket

from mesh import PeerManager

SETTLE_SECONDS = 0.3


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _start_pair(provider_a=None, provider_b=None) -> tuple[PeerManager, PeerManager, list, list]:
    port_a, port_b = _free_port(), _free_port()
    delivered_a: list = []
    delivered_b: list = []

    async def deliver_a(msg_id, room_id, text, from_session, sent_at=None):
        delivered_a.append((room_id, text, from_session, sent_at))

    async def deliver_b(msg_id, room_id, text, from_session, sent_at=None):
        delivered_b.append((room_id, text, from_session, sent_at))

    mesh_a = PeerManager(
        node_id="a", mesh_host="127.0.0.1", mesh_port=port_a,
        peer_addresses=[("127.0.0.1", port_b)],
        deliver_locally=deliver_a, session_query_provider=provider_a,
    )
    mesh_b = PeerManager(
        node_id="b", mesh_host="127.0.0.1", mesh_port=port_b,
        peer_addresses=[("127.0.0.1", port_a)],
        deliver_locally=deliver_b, session_query_provider=provider_b,
    )
    await mesh_a.start()
    await mesh_b.start()
    await asyncio.sleep(SETTLE_SECONDS)
    return mesh_a, mesh_b, delivered_a, delivered_b


async def test_query_session_gets_reply_from_peer_that_has_it():
    async def provider_b(session_id: str):
        if session_id == "sess-1":
            return {"next_send_seq": 3, "last_processed_seq": 2, "room_id": "general", "outbox": []}
        return None

    mesh_a, mesh_b, _, _ = await _start_pair(provider_a=None, provider_b=provider_b)
    try:
        snapshot = await mesh_a.query_session("sess-1")
        assert snapshot is not None
        assert snapshot["room_id"] == "general"
        assert snapshot["next_send_seq"] == 3
        print("PASS: query_session returns the snapshot from the peer that has it")
    finally:
        await mesh_a.stop()
        await mesh_b.stop()


async def test_query_session_returns_none_when_nobody_has_it():
    async def provider_that_never_has_it(session_id: str):
        return None

    mesh_a, mesh_b, _, _ = await _start_pair(
        provider_a=provider_that_never_has_it, provider_b=provider_that_never_has_it
    )
    try:
        snapshot = await mesh_a.query_session("nobody-has-this", timeout=0.5)
        assert snapshot is None
        print("PASS: query_session returns None when no peer has the session")
    finally:
        await mesh_a.stop()
        await mesh_b.stop()


async def test_query_session_with_no_peers_returns_none_immediately():
    port = _free_port()

    async def deliver(msg_id, room_id, text, from_session, sent_at=None):
        pass

    lone = PeerManager(
        node_id="lonely", mesh_host="127.0.0.1", mesh_port=port,
        peer_addresses=[], deliver_locally=deliver,
    )
    await lone.start()
    try:
        loop = asyncio.get_running_loop()
        start = loop.time()
        snapshot = await lone.query_session("whatever", timeout=5.0)
        elapsed = loop.time() - start
        assert snapshot is None
        assert elapsed < 1.0, "should short-circuit instead of waiting out the full timeout"
        print("PASS: query_session with zero connected peers returns None without waiting")
    finally:
        await lone.stop()


async def test_relay_still_dedups_and_reaches_the_other_node():
    mesh_a, mesh_b, delivered_a, delivered_b = await _start_pair()
    try:
        await mesh_a.relay_message(room_id="general", text="hello mesh", from_session="alice")
        await asyncio.sleep(0.2)
        assert delivered_b == [("general", "hello mesh", "alice", None)]
        assert delivered_a == []  # origin node doesn't re-deliver to itself
        print("PASS: RELAY still reaches the other node correctly alongside the new query handling")
    finally:
        await mesh_a.stop()
        await mesh_b.stop()


async def main():
    await test_query_session_gets_reply_from_peer_that_has_it()
    await test_query_session_returns_none_when_nobody_has_it()
    await test_query_session_with_no_peers_returns_none_immediately()
    await test_relay_still_dedups_and_reaches_the_other_node()
    print("\nAll mesh tests passed.")


if __name__ == "__main__":
    asyncio.run(main())