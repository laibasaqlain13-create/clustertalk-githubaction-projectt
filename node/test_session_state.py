#node/test_session_state.py

from session_state import SessionState


def test_outbound_seq_increments():
    s = SessionState("sess-1")
    m1 = s.prepare_outbound({"text": "hi"})
    m2 = s.prepare_outbound({"text": "there"})
    assert m1.seq == 1
    assert m2.seq == 2
    assert set(s.outbox.keys()) == {1, 2}
    print("PASS: outbound seq increments and buffers")


def test_ack_removes_from_outbox():
    s = SessionState("sess-1")
    m1 = s.prepare_outbound({"text": "hi"})
    assert s.handle_ack(m1.seq) is True
    assert m1.seq not in s.outbox
    # duplicate ack is harmless
    assert s.handle_ack(m1.seq) is False
    print("PASS: ack removes from outbox, duplicate ack is safe")


def test_replay_after_reconnect():
    s = SessionState("sess-1")
    m1 = s.prepare_outbound({"text": "a"})
    m2 = s.prepare_outbound({"text": "b"})
    m3 = s.prepare_outbound({"text": "c"})
    # client confirms it received seq 1, but connection dropped
    # before 2 and 3 got through
    s.handle_ack(m1.seq)

    replay = s.replay_from(last_received_seq=1)
    assert [m.seq for m in replay] == [2, 3]
    print("PASS: replay returns only unacked messages, in order")


def test_retry_after_timeout():
    s = SessionState("sess-1")
    m1 = s.prepare_outbound({"text": "hi"})
    # not enough time has passed
    assert s.pending_for_retry(retry_after_seconds=100) == []
    # simulate time passing by backdating sent_at
    s.outbox[m1.seq].sent_at -= 10
    due = s.pending_for_retry(retry_after_seconds=5)
    assert [m.seq for m in due] == [1]
    s.mark_retried(m1.seq)
    assert s.outbox[m1.seq].retry_count == 1
    print("PASS: retry picks up stale unacked messages")


def test_inbound_duplicate_not_reprocessed():
    s = SessionState("sess-1")
    assert s.should_process_inbound(1) is True
    s.mark_inbound_processed(1)
    # retransmit of the same seq -- must not be reprocessed
    assert s.should_process_inbound(1) is False
    print("PASS: duplicate inbound seq is not reprocessed")


def test_inbound_out_of_order_and_watermark_advance():
    s = SessionState("sess-1")
    # seq 2 arrives before seq 1 (reordering)
    assert s.should_process_inbound(2) is True
    s.mark_inbound_processed(2)
    assert s.last_processed_seq == 0  # can't advance watermark yet, 1 missing

    assert s.should_process_inbound(1) is True
    s.mark_inbound_processed(1)
    # now watermark should jump to 2 since 1 and 2 are both in
    assert s.last_processed_seq == 2
    print("PASS: out-of-order inbound handled, watermark advances correctly")


def main():
    test_outbound_seq_increments()
    test_ack_removes_from_outbox()
    test_replay_after_reconnect()
    test_retry_after_timeout()
    test_inbound_duplicate_not_reprocessed()
    test_inbound_out_of_order_and_watermark_advance()
    print("\nAll session_state tests passed.")


if __name__ == "__main__":
    main()
