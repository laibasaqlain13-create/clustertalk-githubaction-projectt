"""
Integration tests for lb.py. Spins up two real ChatNode backends and
one LoadBalancer, then connects real TCP clients through the LB --
same style as node/test_server.py, but one layer up.
"""

#ingress/test_lb.py
import asyncio
import os
import socket
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "protocol"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "node"))

from framing import MessageType, read_frame, write_frame  # noqa: E402
from server import ChatNode  # noqa: E402
from lb import LoadBalancer  # noqa: E402

READ_TIMEOUT = 3.0


def _free_port() -> int:
    """Reserve an ephemeral port. Same pattern as the other integration
    tests -- avoids collisions with hardcoded ports left in TIME_WAIT or
    held by a previous run."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class TestClient:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.session_id = None

    @classmethod
    async def connect(cls, host, port):
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def hello(self, session_id=None):
        await write_frame(self.writer, MessageType.HELLO, {"session_id": session_id})
        msg_type, payload = await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)
        assert msg_type == MessageType.HELLO_ACK, (msg_type, payload)
        self.session_id = payload["session_id"]
        return payload

    async def send_message(self, seq, text):
        await write_frame(self.writer, MessageType.MESSAGE, {"seq": seq, "text": text})

    async def recv(self):
        return await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)

    def close(self):
        self.writer.close()


async def _start_backend(port: int) -> ChatNode:
    tmp_dir = tempfile.mkdtemp()
    db_path = os.path.join(tmp_dir, "node.db")
    node = ChatNode(db_path, host="127.0.0.1", port=port)
    await node.start()
    return node


async def _start_lb(backend_ports: list[int]) -> LoadBalancer:
    backends = [("127.0.0.1", p) for p in backend_ports]
    lb = LoadBalancer(backends, host="127.0.0.1", port=0)
    await lb.start()
    return lb


async def test_client_gets_routed_and_completes_handshake():
    port = _free_port()
    backend = await _start_backend(port)
    lb = await _start_lb([port])
    try:
        addr = lb._server.sockets[0].getsockname()
        client = await TestClient.connect(addr[0], addr[1])
        ack = await client.hello()
        assert ack["session_id"]
        client.close()
        print("PASS: client routed through LB completes handshake with backend")
    finally:
        await lb.stop()
        await backend.stop()


async def test_broadcast_works_through_lb_when_same_backend():
    port = _free_port()
    backend = await _start_backend(port)
    lb = await _start_lb([port])
    try:
        addr = lb._server.sockets[0].getsockname()
        alice = await TestClient.connect(addr[0], addr[1])
        await alice.hello()
        bob = await TestClient.connect(addr[0], addr[1])
        await bob.hello()

        await alice.send_message(seq=1, text="hi through the LB")
        msg_type, payload = await alice.recv()  # her own ACK
        assert msg_type == MessageType.ACK

        msg_type, payload = await bob.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "hi through the LB"

        alice.close()
        bob.close()
        print("PASS: end-to-end broadcast works through the LB's raw relay")
    finally:
        await lb.stop()
        await backend.stop()


async def test_sticky_routing_same_backend_on_reconnect():
    """
    With two backends, a session that connects once must always come
    back to the SAME backend on reconnect -- otherwise its buffered
    messages (which live only on that one backend, per the current
    architecture) would be unreachable.
    """
    port_a, port_b = _free_port(), _free_port()
    backend_a = await _start_backend(port_a)
    backend_b = await _start_backend(port_b)
    lb = await _start_lb([port_a, port_b])
    try:
        addr = lb._server.sockets[0].getsockname()

        client = await TestClient.connect(addr[0], addr[1])
        ack = await client.hello()
        session_id = ack["session_id"]
        client.close()
        await asyncio.sleep(0.1)

        # Reconnect several times -- every single one must land on
        # the same backend the LB picked the first time.
        seen_backends = set()
        for _ in range(6):
            c = await TestClient.connect(addr[0], addr[1])
            ack = await c.hello(session_id=session_id)
            assert ack["session_id"] == session_id
            seen_backends.add(lb._sticky_cache[session_id].address)
            c.close()
            await asyncio.sleep(0.05)

        assert len(seen_backends) == 1, seen_backends
        print("PASS: sticky routing keeps a session on the same backend across reconnects")
    finally:
        await lb.stop()
        await backend_a.stop()
        await backend_b.stop()


async def test_unhealthy_backend_is_not_routed_to():
    # Start the LB pointed at a port with NOTHING listening -- it
    # should mark that backend unhealthy and refuse to route there.
    dead_port = _free_port()  # reserved then released -- nothing listens here
    lb = LoadBalancer([("127.0.0.1", dead_port)], host="127.0.0.1", port=0)
    await lb.start()  # runs an initial health check before accepting traffic
    try:
        assert lb._backends[0].healthy is False

        addr = lb._server.sockets[0].getsockname()
        client = await TestClient.connect(addr[0], addr[1])
        await write_frame(client.writer, MessageType.HELLO, {"session_id": None})
        msg_type, payload = await asyncio.wait_for(read_frame(client.reader), READ_TIMEOUT)
        assert msg_type == MessageType.ERROR
        assert payload["code"] == "no_healthy_backend"
        client.close()
        print("PASS: unhealthy backend is detected and not routed to")
    finally:
        await lb.stop()


class LBIntegrationTests(unittest.IsolatedAsyncioTestCase):
    def test_stale_connection_cannot_deactivate_newer_reconnect(self):
        lb = LoadBalancer([("127.0.0.1", 1)], host="127.0.0.1", port=0)
        backend = lb._backends[0]
        lb._remember_client("session-1", backend, session_id="session-1", connection_id="old")
        lb._remember_client("session-1", backend, session_id="session-1", connection_id="new")
        lb._drop_client("session-1", "old")
        self.assertTrue(lb._clients["session-1"]["active"])

    async def test_real_client_is_visible_and_rebalances_when_backend_is_taken_down(self):
        port_a, port_b = _free_port(), _free_port()
        backend_a = await _start_backend(port_a)
        backend_b = await _start_backend(port_b)
        lb = await _start_lb([port_a, port_b])
        try:
            addr = lb._server.sockets[0].getsockname()
            client = await TestClient.connect(addr[0], addr[1])
            await client.hello()

            snap = lb.admin_snapshot()
            self.assertEqual(snap["client_total"], 1)
            server_a = next(s for s in snap["servers"] if s["address"] == f"{backend_a.host}:{backend_a.port}")
            server_b = next(s for s in snap["servers"] if s["address"] == f"{backend_b.host}:{backend_b.port}")
            self.assertGreater(server_a["clients"], 0)
            self.assertEqual(server_b["clients"], 0)

            lb.admin_set_down(backend_a.address_str, True)
            snap = lb.admin_snapshot()
            self.assertEqual(next(s for s in snap["servers"] if s["address"] == f"{backend_a.host}:{backend_a.port}")["clients"], 0)
            self.assertGreater(next(s for s in snap["servers"] if s["address"] == f"{backend_b.host}:{backend_b.port}")["clients"], 0)

            client.close()
        finally:
            await lb.stop()
            await backend_a.stop()
            await backend_b.stop()

    def test_admin_snapshot_shows_audience_shift_for_manually_taken_down_server(self):
        lb = LoadBalancer([("127.0.0.1", 10001), ("127.0.0.1", 10002)], host="127.0.0.1", port=0)
        try:
            lb.admin_add_clients(4)
            snap = lb.admin_snapshot()
            server_a = next(s for s in snap["servers"] if s["address"] == "127.0.0.1:10001")
            server_b = next(s for s in snap["servers"] if s["address"] == "127.0.0.1:10002")
            self.assertEqual(server_a["clients"], 2)
            self.assertEqual(server_b["clients"], 2)
            self.assertEqual(server_a["requests_routed"], 2)
            self.assertEqual(server_b["requests_routed"], 2)

            lb.admin_set_down("127.0.0.1:10001", True)
            snap = lb.admin_snapshot()
            server_a = next(s for s in snap["servers"] if s["address"] == "127.0.0.1:10001")
            server_b = next(s for s in snap["servers"] if s["address"] == "127.0.0.1:10002")
            self.assertEqual(server_a["clients"], 0)
            self.assertEqual(server_b["clients"], 4)
            self.assertEqual(server_a["requests_routed"], 0)
            self.assertEqual(server_b["requests_routed"], 4)

            lb.admin_set_down("127.0.0.1:10001", False)
            snap = lb.admin_snapshot()
            server_a = next(s for s in snap["servers"] if s["address"] == "127.0.0.1:10001")
            server_b = next(s for s in snap["servers"] if s["address"] == "127.0.0.1:10002")
            self.assertEqual(server_a["clients"], 2)
            self.assertEqual(server_b["clients"], 2)
            self.assertEqual(server_a["requests_routed"], 2)
            self.assertEqual(server_b["requests_routed"], 2)
        finally:
            lb._backends = []


async def main():
    await test_client_gets_routed_and_completes_handshake()
    await test_broadcast_works_through_lb_when_same_backend()
    await test_sticky_routing_same_backend_on_reconnect()
    await test_unhealthy_backend_is_not_routed_to()
    print("\nAll ingress LB tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
