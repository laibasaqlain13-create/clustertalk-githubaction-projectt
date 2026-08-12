# protocol/test_framing.py

"""
Sanity tests for framing.py. Not a full test suite yet -- just
enough to prove the round trip, partial-read handling, and the
oversized-frame guard actually work before we build anything on
top of this.
"""

import asyncio

from framing import (
    ConnectionClosed,
    FrameTooLarge,
    MessageType,
    decode_frame_body,
    encode_frame,
    read_frame,
    read_raw_frame,
)


async def _reader_from_bytes(data: bytes) -> asyncio.StreamReader:
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    return reader


async def test_round_trip_with_payload():
    frame = encode_frame(MessageType.MESSAGE, {"seq": 1, "text": "hello"})
    reader = await _reader_from_bytes(frame)
    msg_type, payload = await read_frame(reader)
    assert msg_type == MessageType.MESSAGE
    assert payload == {"seq": 1, "text": "hello"}
    print("PASS: round trip with payload")


async def test_round_trip_empty_payload():
    frame = encode_frame(MessageType.PING)
    reader = await _reader_from_bytes(frame)
    msg_type, payload = await read_frame(reader)
    assert msg_type == MessageType.PING
    assert payload is None
    print("PASS: round trip empty payload")


async def test_multiple_frames_back_to_back():
    # Simulates two messages arriving in the same TCP read -- this is
    # the exact scenario length-prefixing exists to handle.
    frame1 = encode_frame(MessageType.MESSAGE, {"seq": 1})
    frame2 = encode_frame(MessageType.MESSAGE, {"seq": 2})
    reader = await _reader_from_bytes(frame1 + frame2)

    _, payload1 = await read_frame(reader)
    _, payload2 = await read_frame(reader)
    assert payload1 == {"seq": 1}
    assert payload2 == {"seq": 2}
    print("PASS: multiple frames back to back")


async def test_connection_closed_mid_frame():
    frame = encode_frame(MessageType.MESSAGE, {"seq": 1, "text": "x" * 100})
    truncated = frame[:10]  # cut it off partway through the payload
    reader = await _reader_from_bytes(truncated)
    try:
        await read_frame(reader)
        assert False, "expected ConnectionClosed"
    except ConnectionClosed:
        print("PASS: connection closed mid-frame raises correctly")


async def test_oversized_frame_rejected():
    frame = encode_frame(MessageType.MESSAGE, {"text": "x" * 1000})
    reader = await _reader_from_bytes(frame)
    try:
        await read_frame(reader, max_frame_size=100)
        assert False, "expected FrameTooLarge"
    except FrameTooLarge as exc:
        assert exc.max_size == 100
        print("PASS: oversized frame rejected before body read")


async def test_raw_frame_round_trip():
    frame = encode_frame(MessageType.HELLO, {"session_id": "abc"})
    reader = await _reader_from_bytes(frame)
    raw = await read_raw_frame(reader)
    assert raw == frame  # byte-identical, nothing decoded/re-encoded
    msg_type, payload = decode_frame_body(raw)
    assert msg_type == MessageType.HELLO
    assert payload == {"session_id": "abc"}
    print("PASS: raw frame read is byte-identical, decodes correctly on demand")


async def test_raw_relay_then_decode_only_first_frame():
    # Simulates what the ingress LB does: read the first frame raw
    # AND decode it (for routing), then read subsequent frames raw
    # only, never touching orjson for them.
    hello = encode_frame(MessageType.HELLO, {"session_id": "sess-1"})
    msg1 = encode_frame(MessageType.MESSAGE, {"seq": 1, "text": "hi"})
    reader = await _reader_from_bytes(hello + msg1)

    raw_hello = await read_raw_frame(reader)
    msg_type, payload = decode_frame_body(raw_hello)
    assert msg_type == MessageType.HELLO
    assert payload["session_id"] == "sess-1"

    raw_msg1 = await read_raw_frame(reader)
    assert raw_msg1 == msg1  # relayed untouched, never decoded
    print("PASS: can decode first frame for routing, relay rest raw")


async def main():
    await test_round_trip_with_payload()
    await test_round_trip_empty_payload()
    await test_multiple_frames_back_to_back()
    await test_connection_closed_mid_frame()
    await test_oversized_frame_rejected()
    await test_raw_frame_round_trip()
    await test_raw_relay_then_decode_only_first_frame()
    print("\nAll framing tests passed.")


if __name__ == "__main__":
    asyncio.run(main())
