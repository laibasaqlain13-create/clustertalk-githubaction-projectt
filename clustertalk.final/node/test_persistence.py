# node/test_persistence.py

import asyncio
import os
import tempfile

from persistence import OutboxStore


async def _with_store(flush_interval, fn):
    """Helper: spin up a store against a temp db file, run fn, tear down."""
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store = OutboxStore(db_path, flush_interval_seconds=flush_interval)
        store.start()
        try:
            await fn(store)
        finally:
            store.stop()


async def test_save_and_load_outbound_round_trip():
    async def fn(store):
        await store.save_outbound("sess-1", 1, {"text": "hello"}, sent_at=100.0)
        await store.save_outbound("sess-1", 2, {"text": "world"}, sent_at=101.0)
        # give the writer thread time to flush (interval + margin)
        await asyncio.sleep(0.15)

        rows = await store.load_outbox("sess-1")
        assert [r["seq"] for r in rows] == [1, 2]
        assert rows[0]["payload"] == {"text": "hello"}
        assert rows[1]["payload"] == {"text": "world"}
        print("PASS: save/load outbound round trip")

    await _with_store(flush_interval=0.05, fn=fn)


async def test_delete_removes_from_outbox():
    async def fn(store):
        await store.save_outbound("sess-1", 1, {"text": "hi"}, sent_at=100.0)
        await asyncio.sleep(0.15)
        assert len(await store.load_outbox("sess-1")) == 1

        await store.delete_outbound("sess-1", 1)
        await asyncio.sleep(0.15)
        assert await store.load_outbox("sess-1") == []
        print("PASS: delete removes from outbox after flush")

    await _with_store(flush_interval=0.05, fn=fn)


async def test_session_metadata_round_trip():
    async def fn(store):
        assert await store.load_session("sess-1") is None  # nothing saved yet
        await store.save_session("sess-1", next_send_seq=5, last_processed_seq=3, room_id="lobby")
        await asyncio.sleep(0.15)

        loaded = await store.load_session("sess-1")
        assert loaded == {"next_send_seq": 5, "last_processed_seq": 3, "room_id": "lobby"}
        print("PASS: session metadata round trip (including room_id)")

    await _with_store(flush_interval=0.05, fn=fn)


async def test_session_room_defaults_to_general():
    async def fn(store):
        # save_session's room_id param defaults to "general" if omitted
        await store.save_session("sess-1", next_send_seq=1, last_processed_seq=0)
        await asyncio.sleep(0.15)
        loaded = await store.load_session("sess-1")
        assert loaded["room_id"] == "general"
        print("PASS: room_id defaults to 'general' when not specified")

    await _with_store(flush_interval=0.05, fn=fn)


async def test_multiple_sessions_isolated():
    async def fn(store):
        await store.save_outbound("sess-A", 1, {"text": "a"}, sent_at=1.0)
        await store.save_outbound("sess-B", 1, {"text": "b"}, sent_at=1.0)
        await asyncio.sleep(0.15)

        a = await store.load_outbox("sess-A")
        b = await store.load_outbox("sess-B")
        assert [r["payload"] for r in a] == [{"text": "a"}]
        assert [r["payload"] for r in b] == [{"text": "b"}]
        print("PASS: multiple sessions stay isolated")

    await _with_store(flush_interval=0.05, fn=fn)


async def test_stop_drains_pending_writes():
    """
    Critical case: if the node shuts down (or crashes gracefully)
    right after a write is queued but before the flush timer fires,
    stop() must still persist it -- otherwise the whole point of
    this module (crash safety) is undermined by the batching itself.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store = OutboxStore(db_path, flush_interval_seconds=10.0)  # long interval on purpose
        store.start()
        await store.save_outbound("sess-1", 1, {"text": "urgent"}, sent_at=1.0)
        store.stop()  # should flush immediately, not wait 10s

        # open a fresh store against the same file to confirm durability
        store2 = OutboxStore(db_path, flush_interval_seconds=0.05)
        store2.start()
        try:
            rows = await store2.load_outbox("sess-1")
            assert [r["payload"] for r in rows] == [{"text": "urgent"}]
            print("PASS: stop() drains pending writes before exit")
        finally:
            store2.stop()


async def main():
    await test_save_and_load_outbound_round_trip()
    await test_delete_removes_from_outbox()
    await test_session_metadata_round_trip()
    await test_session_room_defaults_to_general()
    await test_multiple_sessions_isolated()
    await test_stop_drains_pending_writes()
    print("\nAll persistence tests passed.")


if __name__ == "__main__":
    asyncio.run(main())