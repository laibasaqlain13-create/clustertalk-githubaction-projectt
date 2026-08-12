"""
Integration test for dynamic backend registration: a ChatNode
registers itself with a running LoadBalancer via NODE_ANNOUNCE /
NODE_STATUS (see lb.py's DYNAMIC REGISTRATION) instead of being passed
as a static --backend at LB startup, and a client routed through the
LB actually reaches it. Also proves the node is removed from the LB's
routing pool the moment it goes away, without needing to wait for the
next active health-check cycle.

Same style as test_lb.py, one layer up: instead of constructing the
LB with a static backend list, we start the LB with register_port
set and let the node dial in on its own.
"""

import asyncio
import os
import socket
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "protocol"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingress"))

from framing import MessageType, read_frame, write_frame  # noqa: E402
from server import ChatNode  # noqa: E402
from lb import LoadBalancer  # noqa: E402

READ_TIMEOUT = 3.0
REGISTER_SETTLE_SECONDS = 0.5


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

    def close(self):
        self.writer.close()


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


async def _start_lb(register_port: int) -> LoadBalancer:
    lb = LoadBalancer([], host="127.0.0.1", port=0, register_port=register_port)
    await lb.start()
    return lb


async def _start_registering_node(register_port: int) -> ChatNode:
    tmp_dir = tempfile.mkdtemp()
    node = ChatNode(
        db_path=os.path.join(tmp_dir, "node.db"), host="127.0.0.1", port=0,
        advertise_host="127.0.0.1",
        lb_register_host="127.0.0.1", lb_register_port=register_port,
    )
    await node.start()
    return node


async def test_dynamically_registered_node_receives_routed_client():
    register_port = _free_port()
    lb = await _start_lb(register_port)
    node = await _start_registering_node(register_port)
    try:
        await asyncio.sleep(REGISTER_SETTLE_SECONDS)  # let NODE_ANNOUNCE land

        assert len(lb._backends) == 1
        assert lb._backends[0].dynamic is True
        assert lb._backends[0].healthy is True

        lb_addr = lb._server.sockets[0].getsockname()
        client = await TestClient.connect(*lb_addr)
        ack = await client.hello()
        assert ack["session_id"]

        client.close()
        print("PASS: dynamically registered node is routed to by the LB")
    finally:
        await node.stop()
        await lb.stop()


async def test_node_going_away_deregisters_immediately():
    register_port = _free_port()
    lb = await _start_lb(register_port)
    node = await _start_registering_node(register_port)
    try:
        await asyncio.sleep(REGISTER_SETTLE_SECONDS)
        assert len(lb._backends) == 1

        await node.stop()  # closes the registration connection too
        await asyncio.sleep(REGISTER_SETTLE_SECONDS)

        assert len(lb._backends) == 0, "backend should be removed as soon as it disconnects"
        print("PASS: node going away deregisters it from the LB immediately")
    finally:
        await lb.stop()


async def test_static_and_dynamic_backends_coexist():
    register_port = _free_port()
    tmp_dir = tempfile.mkdtemp()
    static_node = ChatNode(db_path=os.path.join(tmp_dir, "static.db"), host="127.0.0.1", port=0)
    await static_node.start()
    static_addr = static_node._server.sockets[0].getsockname()

    lb = LoadBalancer(
        [("127.0.0.1", static_addr[1])], host="127.0.0.1", port=0, register_port=register_port,
    )
    await lb.start()
    dynamic_node = await _start_registering_node(register_port)
    try:
        await asyncio.sleep(REGISTER_SETTLE_SECONDS)
        assert len(lb._backends) == 2
        assert sum(1 for b in lb._backends if b.dynamic) == 1
        assert sum(1 for b in lb._backends if not b.dynamic) == 1

        # Both should be reachable through round-robin -- just prove
        # a couple of fresh (non-sticky) connections don't error out.
        lb_addr = lb._server.sockets[0].getsockname()
        for _ in range(2):
            client = await TestClient.connect(*lb_addr)
            await client.hello()
            client.close()

        print("PASS: static --backend entries and dynamically registered nodes coexist")
    finally:
        await dynamic_node.stop()
        await static_node.stop()
        await lb.stop()


async def main():
    await test_dynamically_registered_node_receives_routed_client()
    await test_node_going_away_deregisters_immediately()
    await test_static_and_dynamic_backends_coexist()
    print("\nAll dynamic registration tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
