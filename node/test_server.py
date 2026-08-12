"""
End-to-end test for server.py: actually opens TCP connections to a
running ChatNode and exercises the wire protocol as a real client
would, rather than calling internal methods directly. This is the
first test in the project that proves all the pieces work together
over an actual socket, not just in isolation.
"""
# node/test_server.py
import asyncio
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "protocol"))
from framing import MessageType, read_frame, write_frame  # noqa: E402

from server import ChatNode  # noqa: E402

READ_TIMEOUT = 2.0


class TestClient:
    """Thin wrapper so test code reads like a client, not raw asyncio."""

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
        assert msg_type == MessageType.HELLO_ACK, msg_type
        self.session_id = payload["session_id"]
        return payload

    async def join_room(self, room_id: str) -> dict:
        await write_frame(self.writer, MessageType.JOIN_ROOM, {"room_id": room_id})
        msg_type, payload = await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)
        assert msg_type == MessageType.ROOM_JOINED, msg_type
        return payload

    async def send_message(self, seq: int, text: str) -> None:
        await write_frame(self.writer, MessageType.MESSAGE, {"seq": seq, "text": text})

    async def recv(self) -> tuple:
        return await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)

    async def ack(self, seq: int) -> None:
        await write_frame(self.writer, MessageType.ACK, {"seq": seq})

    async def authenticate(self, username: str, password: str) -> dict:
        """
        Mirror the browser frontend's auth flow: try REGISTER first, and
        if the username is already taken, fall back to LOGIN on the SAME
        socket. Returns the LOGIN_ACK/REGISTER_ACK payload once the
        session is live.
        """
        await write_frame(self.writer, MessageType.REGISTER,
                          {"username": username, "password": password})
        msg_type, payload = await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)
        assert msg_type == MessageType.REGISTER_ACK, (msg_type, payload)

        if payload.get("ok"):
            # New user -- server auto-sends LOGIN_ACK next.
            msg_type, payload = await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)
        else:
            # Existing user -- log in on the same connection.
            await write_frame(self.writer, MessageType.LOGIN,
                              {"username": username, "password": password})
            msg_type, payload = await asyncio.wait_for(read_frame(self.reader), READ_TIMEOUT)

        assert msg_type == MessageType.LOGIN_ACK, (msg_type, payload)
        assert payload.get("ok") is True, payload
        self.session_id = payload["session_id"]
        return payload

    def close(self) -> None:
        self.writer.close()


async def _start_node() -> tuple[ChatNode, str, int]:
    tmp_dir = tempfile.mkdtemp()
    db_path = os.path.join(tmp_dir, "node.db")
    node = ChatNode(db_path, host="127.0.0.1", port=0)
    await node.start()
    addr = node._server.sockets[0].getsockname()
    return node, addr[0], addr[1]


async def test_handshake_assigns_session_id():
    node, host, port = await _start_node()
    try:
        client = await TestClient.connect(host, port)
        ack = await client.hello()
        assert ack["session_id"]
        assert ack["last_processed_seq"] == 0
        client.close()
        print("PASS: handshake assigns a fresh session id")
    finally:
        await node.stop()


async def test_message_broadcasts_to_other_client_and_acks_sender():
    node, host, port = await _start_node()
    try:
        alice = await TestClient.connect(host, port)
        await alice.hello()
        bob = await TestClient.connect(host, port)
        await bob.hello()

        await alice.send_message(seq=1, text="hello bob")

        # Alice should get an ACK for her own message
        msg_type, payload = await alice.recv()
        assert msg_type == MessageType.ACK
        assert payload["seq"] == 1

        # Bob should receive it as a broadcast MESSAGE
        msg_type, payload = await bob.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "hello bob"
        assert payload["from"] == alice.session_id

        alice.close()
        bob.close()
        print("PASS: message broadcasts to other client, sender gets acked")
    finally:
        await node.stop()


async def test_disconnected_client_gets_replay_on_reconnect():
    node, host, port = await _start_node()
    try:
        alice = await TestClient.connect(host, port)
        await alice.authenticate("alice", "password")
        bob = await TestClient.connect(host, port)
        bob_ack = await bob.authenticate("bob", "password")
        bob_session_id = bob_ack["session_id"]

        # Bob goes offline
        bob.close()
        await asyncio.sleep(0.1)

        # Alice sends while Bob is offline -- must be buffered, not lost
        await alice.send_message(seq=1, text="are you there")
        await alice.recv()  # her own ACK, drain it

        await asyncio.sleep(0.1)

        # Bob reconnects with the SAME session id, but since it's an authenticated
        # session, we must use authenticate again, just like a real client falling back to LOGIN.
        bob2 = await TestClient.connect(host, port)
        ack = await bob2.authenticate("bob", "password")
        assert ack["session_id"] == bob_session_id

        # The buffered message must be replayed immediately
        msg_type, payload = await bob2.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "are you there"

        alice.close()
        bob2.close()
        print("PASS: message sent while offline is replayed on reconnect")
    finally:
        await node.stop()


async def test_ping_pong():
    node, host, port = await _start_node()
    try:
        client = await TestClient.connect(host, port)
        await client.hello()
        await write_frame(client.writer, MessageType.PING)
        msg_type, _ = await client.recv()
        assert msg_type == MessageType.PONG
        client.close()
        print("PASS: PING gets a PONG")
    finally:
        await node.stop()


async def test_different_rooms_dont_see_each_other():
    node, host, port = await _start_node()
    try:
        alice = await TestClient.connect(host, port)
        await alice.hello(room_id="room-a")
        bob = await TestClient.connect(host, port)
        await bob.hello(room_id="room-b")

        await alice.send_message(seq=1, text="hello room-a")
        msg_type, _ = await alice.recv()  # her own ACK
        assert msg_type == MessageType.ACK

        # Bob is in a different room -- must NOT receive it. Prove
        # this by having bob send his own message and checking HE
        # only sees his own ACK next, not alice's leaked message.
        await bob.send_message(seq=1, text="hello room-b")
        msg_type, payload = await bob.recv()
        assert msg_type == MessageType.ACK
        assert payload["seq"] == 1

        alice.close()
        bob.close()
        print("PASS: sessions in different rooms don't see each other's messages")
    finally:
        await node.stop()


async def test_same_room_sees_broadcast():
    node, host, port = await _start_node()
    try:
        alice = await TestClient.connect(host, port)
        await alice.hello(room_id="watercooler")
        bob = await TestClient.connect(host, port)
        await bob.hello(room_id="watercooler")

        await alice.send_message(seq=1, text="hi from the watercooler")
        await alice.recv()  # her ACK

        msg_type, payload = await bob.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "hi from the watercooler"

        alice.close()
        bob.close()
        print("PASS: sessions explicitly in the same room see each other's broadcasts")
    finally:
        await node.stop()


async def test_join_room_switches_and_receives_new_room_traffic():
    node, host, port = await _start_node()
    try:
        alice = await TestClient.connect(host, port)
        await alice.hello(room_id="room-a")
        bob = await TestClient.connect(host, port)
        await bob.hello(room_id="room-b")  # starts in a different room from alice

        # Bob switches into alice's room
        ack = await bob.join_room("room-a")
        assert ack["room_id"] == "room-a"

        await alice.send_message(seq=1, text="welcome to room-a")
        await alice.recv()  # her ACK

        msg_type, payload = await bob.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "welcome to room-a"

        alice.close()
        bob.close()
        print("PASS: JOIN_ROOM switches rooms and the client starts receiving that room's traffic")
    finally:
        await node.stop()


async def test_returning_user_can_login_after_taken_register():
    """
    Regression test for the central bug: a returning user's connection
    must survive a REGISTER that fails with "username already taken" so
    the follow-up LOGIN (sent on the SAME socket, exactly as the frontend
    does) succeeds. Previously the node closed the socket after the failed
    REGISTER, locking every returning user out at the login screen.
    """
    node, host, port = await _start_node()
    try:
        # First visit -- registers successfully.
        first = await TestClient.connect(host, port)
        ack1 = await first.authenticate("returning_user", "hunter2pw")
        assert ack1["session_id"]
        first.close()
        await asyncio.sleep(0.1)

        # Second visit -- same credentials. REGISTER now fails as taken,
        # and authenticate() must recover via LOGIN on the same socket.
        second = await TestClient.connect(host, port)
        ack2 = await second.authenticate("returning_user", "hunter2pw")
        assert ack2["session_id"] == ack1["session_id"]  # stable session
        second.close()
        print("PASS: returning user logs in after a 'username taken' REGISTER on the same socket")
    finally:
        await node.stop()


async def test_authenticated_message_shows_username_as_author():
    """Broadcast messages must be attributed to the sender's username,
    not their opaque session_id."""
    node, host, port = await _start_node()
    try:
        alice = await TestClient.connect(host, port)
        await alice.authenticate("alice", "alicepw123")
        bob = await TestClient.connect(host, port)
        await bob.authenticate("bob", "bobpw12345")

        await alice.send_message(seq=1, text="hi bob, it's me")
        msg_type, payload = await alice.recv()  # her own ACK
        assert msg_type == MessageType.ACK

        msg_type, payload = await bob.recv()
        assert msg_type == MessageType.MESSAGE
        assert payload["text"] == "hi bob, it's me"
        assert payload["from"] == "alice", payload  # username, not a UUID
        assert payload["room_id"] == "general", payload

        alice.close()
        bob.close()
        print("PASS: authenticated broadcast is attributed to the username, carries room_id")
    finally:
        await node.stop()


async def test_wrong_password_is_rejected():
    node, host, port = await _start_node()
    try:
        setup = await TestClient.connect(host, port)
        await setup.authenticate("secured_user", "correct-horse")
        setup.close()
        await asyncio.sleep(0.1)

        attacker = await TestClient.connect(host, port)
        # REGISTER (taken) then LOGIN with the WRONG password.
        await write_frame(attacker.writer, MessageType.REGISTER,
                          {"username": "secured_user", "password": "whatever-wrong"})
        mt, _ = await asyncio.wait_for(read_frame(attacker.reader), READ_TIMEOUT)
        assert mt == MessageType.REGISTER_ACK
        await write_frame(attacker.writer, MessageType.LOGIN,
                          {"username": "secured_user", "password": "whatever-wrong"})
        mt, payload = await asyncio.wait_for(read_frame(attacker.reader), READ_TIMEOUT)
        assert mt == MessageType.LOGIN_ACK
        assert payload.get("ok") is False, payload
        attacker.close()
        print("PASS: wrong password is rejected with LOGIN_ACK ok=false")
    finally:
        await node.stop()


async def main():
    await test_handshake_assigns_session_id()
    await test_message_broadcasts_to_other_client_and_acks_sender()
    await test_disconnected_client_gets_replay_on_reconnect()
    await test_ping_pong()
    await test_different_rooms_dont_see_each_other()
    await test_same_room_sees_broadcast()
    await test_join_room_switches_and_receives_new_room_traffic()
    await test_returning_user_can_login_after_taken_register()
    await test_authenticated_message_shows_username_as_author()
    await test_wrong_password_is_rejected()
    print("\nAll server integration tests passed.")


if __name__ == "__main__":
    asyncio.run(main())