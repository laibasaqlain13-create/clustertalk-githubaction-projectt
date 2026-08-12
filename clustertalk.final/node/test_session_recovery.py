"""
End-to-end test for cross-node session recovery: a client whose
session lives on one node gets routed to a DIFFERENT node (simulating
what the Ingress LB does on failover, or when there's no sticky
mapping yet) and proves that node recovers the session's full state
-- room, seq counters, and any buffered-but-unacked outbox messages --
via the mesh's SESSION_QUERY / SESSION_STATE protocol (mesh.py), not
by starting the session over from scratch.

Same style as test_server.py (real sockets, real ChatNode processes)
and test_lb.py (multiple real nodes), one layer up: two full ChatNode
mesh peers, no LB involved -- we connect directly to each node's
client port to control precisely which node a client lands on.
"""
# node/test_session_recovery.py
import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "protocol"))

from framing import MessageType, read_frame, write_frame  # noqa: E402
from server import ChatNode  # noqa: E402

READ_TIMEOUT = 3.0
# Give the mesh handshake + first heartbeat a moment to complete
# before a test relies on query_session() reaching the other side.
MESH_SETTLE_SECONDS = 0.3


class TestClient:
    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.session_id: str | None = None

    @classmethod
    async def connect(cls, host: str, port: int) -> "TestClient":
        reader, writer = await asyncio.open_connection(host, port)
        return cls(reader, writer)

    async def hello(self, session_id: str | None = None, room_id: str | None = None) -> dict:
        await write_frame(self.writer, MessageType.HELLO, {"session_id": session_id, "room_id": room_id})
        msg_type, payload = await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)
        assert msg_type == MessageType.HELLO_ACK, (msg_type, payload)
        self.session_id = payload["session_id"]
        return payload

    async def send_message(self, seq: int, text: str) -> None:
        await write_frame(self.writer, MessageType.MESSAGE, {"seq": seq, "text": text})

    async def recv(self) -> tuple:
        return await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)

    def close(self) -> None:
        self.writer.close()


async def _start_meshed_pair() -> tuple[ChatNode, ChatNode]:
    """Two ChatNodes, fully meshed to each other, each on its own
    ephemeral client port and mesh port."""
    tmp_dir = tempfile.mkdtemp()

    # Reserve mesh ports up front (ephemeral, host-assigned) so each
    # node's peer_addresses can point at the other's real mesh port
    # before either one has started.
    import socket

    def _free_port() -> int:
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        return port

    mesh_port_a = _free_port()
    mesh_port_b = _free_port()

    node_a = ChatNode(
        db_path=os.path.join(tmp_dir, "a.db"), host="127.0.0.1", port=0,
        node_id="node-a", mesh_port=mesh_port_a,
        peer_addresses=[("127.0.0.1", mesh_port_b)],
    )
    node_b = ChatNode(
        db_path=os.path.join(tmp_dir, "b.db"), host="127.0.0.1", port=0,
        node_id="node-b", mesh_port=mesh_port_b,
        peer_addresses=[("127.0.0.1", mesh_port_a)],
    )
    await node_a.start()
    await node_b.start()

    addr_a = node_a._server.sockets[0].getsockname()
    addr_b = node_b._server.sockets[0].getsockname()

    await asyncio.sleep(MESH_SETTLE_SECONDS)  # let PEER_HELLO complete
    return node_a, node_b, addr_a, addr_b


async def test_recovers_session_and_replays_buffered_message_on_failover():
    node_a, node_b, addr_a, addr_b = await _start_meshed_pair()
    try:
        # Alice and Bob both land on node A, in the same room.
        alice = await TestClient.connect(*addr_a)
        await alice.hello(room_id="watercooler")
        bob = await TestClient.connect(*addr_a)
        bob_ack = await bob.hello(room_id="watercooler")
        bob_session_id = bob_ack["session_id"]

        # Bob drops (e.g. crash, network blip) while still "owned" by node A.
        bob.close()
        await asyncio.sleep(0.1)

        # Alice sends while Bob is offline -- buffered in node A's outbox.
        await alice.send_message(seq=1, text="are you still there")
        await alice.recv()  # her own ACK, drain it

        await asyncio.sleep(0.1)

        # Bob reconnects, but this time lands on node B instead of node
        # A (simulating the Ingress LB routing him to a different
        # backend after failover / lost stickiness). Node B has never
        # seen bob_session_id -- it must recover it from node A via
        # the mesh rather than treating Bob as a brand new session.
        bob2 = await TestClient.connect(*addr_b)
        ack = await bob2.hello(session_id=bob_session_id)
        assert ack["session_id"] == bob_session_id
        # Room must have come from the RECOVERED state, not the
        # HELLO's room_id=None default -- proves this came from node
        # A's snapshot, not a fresh session created locally on B.
        assert ack["room_id"] == "watercooler", ack

        # The message Alice sent while Bob was offline must still be
        # delivered -- to Bob, now connected to a DIFFERENT node than
        # the one that originally buffered it.
        msg_type, payload = await bob2.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "are you still there"

        alice.close()
        bob2.close()
        print("PASS: session recovered cross-node and buffered message replayed after failover")
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_recovered_session_can_send_and_broadcast_across_mesh():
    node_a, node_b, addr_a, addr_b = await _start_meshed_pair()
    try:
        alice = await TestClient.connect(*addr_a)
        await alice.hello(room_id="general")
        bob = await TestClient.connect(*addr_a)
        bob_ack = await bob.hello(room_id="general")
        bob_session_id = bob_ack["session_id"]
        bob.close()
        await asyncio.sleep(0.1)

        # Bob comes back on node B (recovered), then sends a message --
        # it must reach Alice, who's still on node A, via mesh relay.
        bob2 = await TestClient.connect(*addr_b)
        await bob2.hello(session_id=bob_session_id)

        await bob2.send_message(seq=1, text="hi from node B")
        msg_type, payload = await bob2.recv()  # his own ACK
        assert msg_type == MessageType.ACK

        msg_type, payload = await alice.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "hi from node B"

        alice.close()
        bob2.close()
        print("PASS: recovered session can send new messages that broadcast across the mesh")
    finally:
        await node_a.stop()
        await node_b.stop()


async def test_brand_new_session_id_is_not_recovered_from_nowhere():
    """A session_id nobody has ever seen (not on THIS node, not on any
    peer) must fall through to being treated as genuinely new -- the
    mesh query returns None and the handshake proceeds normally rather
    than hanging or erroring."""
    node_a, node_b, addr_a, addr_b = await _start_meshed_pair()
    try:
        client = await TestClient.connect(*addr_b)
        ack = await client.hello(session_id="nobody-has-ever-seen-this-one")
        assert ack["session_id"] == "nobody-has-ever-seen-this-one"
        assert ack["room_id"] == "general"
        assert ack["last_processed_seq"] == 0
        client.close()
        print("PASS: unknown session_id with no owner anywhere falls through to a fresh session")
    finally:
        await node_a.stop()
        await node_b.stop()


async def main():
    await test_recovers_session_and_replays_buffered_message_on_failover()
    await test_recovered_session_can_send_and_broadcast_across_mesh()
    await test_brand_new_session_id_is_not_recovered_from_nowhere()
    print("\nAll cross-node session recovery tests passed.")


if __name__ == "__main__":
    asyncio.run(main())