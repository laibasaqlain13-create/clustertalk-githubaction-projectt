# node/test_session_manager.py

import asyncio
import os
import tempfile

from persistence import OutboxStore
from session_manager import SessionManager


async def _fresh_manager(db_path, flush_interval=0.05):
    store = OutboxStore(db_path, flush_interval_seconds=flush_interval)
    store.start()
    manager = SessionManager(store)
    return store, manager


async def test_fresh_session_starts_at_seq_1():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store, manager = await _fresh_manager(db_path)
        try:
            seq = await manager.send_message("sess-1", {"text": "hi"})
            assert seq == 1
            print("PASS: fresh session starts at seq 1")
        finally:
            store.stop()


async def test_ack_removes_from_memory_and_disk():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store, manager = await _fresh_manager(db_path)
        try:
            seq = await manager.send_message("sess-1", {"text": "hi"})
            await manager.handle_ack("sess-1", seq)
            await asyncio.sleep(0.15)

            rows = await store.load_outbox("sess-1")
            assert rows == []
            print("PASS: ack clears both in-memory and persisted outbox")
        finally:
            store.stop()


async def test_crash_and_recover_unacked_messages():
    """
    The scenario this whole layer exists for: node sends messages,
    some get acked, then the node "crashes" (we just throw away the
    manager/store without a clean stop, then reopen against the same
    file) -- a fresh manager should recover exactly the unacked
    messages and correct seq counters.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")

        # --- "node instance 1" ---
        store1, manager1 = await _fresh_manager(db_path, flush_interval=0.02)
        seq1 = await manager1.send_message("sess-1", {"text": "a"})
        seq2 = await manager1.send_message("sess-1", {"text": "b"})
        seq3 = await manager1.send_message("sess-1", {"text": "c"})
        await manager1.handle_ack("sess-1", seq1)  # only "a" gets acked
        await asyncio.sleep(0.1)  # let writes flush -- simulates writes
        # landing on disk before the crash, NOT a clean shutdown
        # (no store1.stop() call -- that's the point)

        # --- "node instance 2" (post-crash restart) ---
        store2, manager2 = await _fresh_manager(db_path, flush_interval=0.02)
        try:
            recovered = await manager2.get_or_create("sess-1")

            # "a" was acked before the crash -- must NOT reappear
            recovered_seqs = set(recovered.outbox.keys())
            assert recovered_seqs == {seq2, seq3}, recovered_seqs

            # next send must continue from where it left off, not
            # restart at 1 and collide with seq2/seq3
            next_seq = await manager2.send_message("sess-1", {"text": "d"})
            assert next_seq == 4
            print("PASS: crash recovery restores exactly the unacked messages, seq continues correctly")
        finally:
            store2.stop()
            store1.stop()


async def test_inbound_watermark_survives_recovery():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")

        store1, manager1 = await _fresh_manager(db_path, flush_interval=0.02)
        await manager1.receive_message("sess-1", 1)
        await manager1.receive_message("sess-1", 2)
        await asyncio.sleep(0.1)

        store2, manager2 = await _fresh_manager(db_path, flush_interval=0.02)
        try:
            recovered = await manager2.get_or_create("sess-1")
            assert recovered.last_processed_seq == 2

            # a retransmit of seq 2 (client didn't know it was acked
            # before reconnecting) must not be reprocessed
            should_process = await manager2.receive_message("sess-1", 2)
            assert should_process is False
            print("PASS: inbound watermark survives recovery, duplicate correctly ignored")
        finally:
            store2.stop()
            store1.stop()


async def test_session_cached_after_first_load():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store, manager = await _fresh_manager(db_path)
        try:
            assert manager.is_loaded("sess-1") is False
            await manager.get_or_create("sess-1")
            assert manager.is_loaded("sess-1") is True
            print("PASS: session stays cached in memory after first load")
        finally:
            store.stop()


async def test_room_defaults_and_explicit_assignment():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store, manager = await _fresh_manager(db_path)
        try:
            default_session = await manager.get_or_create("sess-default")
            assert default_session.room_id == "general"

            lobby_session = await manager.get_or_create("sess-lobby", room_id="lobby")
            assert lobby_session.room_id == "lobby"
            print("PASS: sessions default to 'general', explicit room_id honored for new sessions")
        finally:
            store.stop()


async def test_set_room_switches_and_persists():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store, manager = await _fresh_manager(db_path)
        try:
            await manager.get_or_create("sess-1")  # starts in "general"
            await manager.set_room("sess-1", "vip-lounge")

            session = manager.get_loaded("sess-1")
            assert session.room_id == "vip-lounge"
            assert manager.session_ids_in_room("vip-lounge") == ["sess-1"]
            assert manager.session_ids_in_room("general") == []
            print("PASS: set_room switches the in-memory room and updates room lookup")
        finally:
            store.stop()


async def test_room_survives_crash_recovery():
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")

        store1, manager1 = await _fresh_manager(db_path, flush_interval=0.02)
        await manager1.get_or_create("sess-1", room_id="general")
        await manager1.set_room("sess-1", "watercooler")
        await asyncio.sleep(0.1)  # flush before "crash" (no clean stop)

        store2, manager2 = await _fresh_manager(db_path, flush_interval=0.02)
        try:
            recovered = await manager2.get_or_create("sess-1")
            assert recovered.room_id == "watercooler"
            print("PASS: room assignment survives crash recovery")
        finally:
            store2.stop()
            store1.stop()


async def test_existing_in_memory_session_ignores_room_id_arg():
    """
    get_or_create's room_id param only applies to brand-new sessions.
    An already-loaded session must NOT get silently moved to a
    different room just because some caller passed a different
    room_id -- that would make set_room() pointless and make room
    membership depend on accidental call ordering.
    """
    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "test.db")
        store, manager = await _fresh_manager(db_path)
        try:
            await manager.get_or_create("sess-1", room_id="lobby")
            session = await manager.get_or_create("sess-1", room_id="somewhere-else")
            assert session.room_id == "lobby"
            print("PASS: get_or_create doesn't reassign room for an already-loaded session")
        finally:
            store.stop()


async def main():
    await test_fresh_session_starts_at_seq_1()
    await test_ack_removes_from_memory_and_disk()
    await test_crash_and_recover_unacked_messages()
    await test_inbound_watermark_survives_recovery()
    await test_session_cached_after_first_load()
    await test_room_defaults_and_explicit_assignment()
    await test_set_room_switches_and_persists()
    await test_room_survives_crash_recovery()
    await test_existing_in_memory_session_ignores_room_id_arg()
    print("\nAll session_manager tests passed.")


if __name__ == "__main__":
    asyncio.run(main())