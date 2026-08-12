"""
Per-connection session state: sequence numbers, outbox buffering,
and ACK tracking.

This is the piece that actually delivers the "zero message loss"
requirement. The framing layer (protocol/framing.py) only guarantees
that a MESSAGE frame arrives intact -- it says nothing about what
happens if the connection drops before the frame arrives, or before
we find out whether it arrived.

Contract:
    - Every outbound MESSAGE gets a monotonically increasing `seq`
      (per connection, starting at 1).
    - The sender buffers every unacked MESSAGE in `outbox`, keyed by
      seq, until a matching ACK arrives.
    - On reconnect, the client reports the highest seq it has fully
      processed (`last_received_seq`). The node replays everything in
      `outbox` with seq > that number.
    - Receiving a duplicate seq (a retransmit of something already
      processed) must ACK it again but must NOT reprocess it -- this
      is what makes replay safe (at-least-once delivery, exactly-once
      processing).

This module only holds in-memory state + the bookkeeping logic. Persisting
`outbox` / `last_processed_seq` to SQLite (so a node crash doesn't lose
unacked messages) is a separate concern, layered on top -- see the
`persistence` module (not built yet).
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class OutboundMessage:
    """A MESSAGE frame we've sent but haven't had ACKed yet."""
    seq: int
    payload: dict
    sent_at: float
    retry_count: int = 0


class SessionState:
    """
    Tracks sequencing state for one client connection.

    One instance per connected client, held by the backend node for
    as long as that client's session is alive (which may outlive any
    single TCP connection, if the client reconnects after a drop).
    """

    def __init__(
        self,
        session_id: str,
        next_send_seq: int = 1,
        last_processed_seq: int = 0,
        room_id: str = "general",
    ):
        self.session_id = session_id
        self.room_id = room_id

        # Outbound side: messages we're sending to the client.
        # Overridable so a recovered session resumes numbering where
        # it left off, instead of restarting at 1 and colliding with
        # seqs the client may have already seen.
        self._next_send_seq: int = next_send_seq
        self.outbox: dict[int, OutboundMessage] = {}

        # Inbound side: messages the client is sending to us.
        self.last_processed_seq: int = last_processed_seq
        # seqs we've ACKed but might see again as retransmits --
        # bounded so this doesn't grow forever on a long-lived
        # connection. See _prune_seen below.
        self._seen_inbound_seqs: set[int] = set()

    def load_outbox_rows(self, rows: list[dict]) -> None:
        """
        Bulk-hydrate the outbox from persisted rows (as returned by
        PersistentStore/OutboxStore.load_outbox). Used only during
        recovery, before the session starts taking live traffic --
        not a substitute for prepare_outbound() during normal operation.
        """
        for row in rows:
            self.outbox[row["seq"]] = OutboundMessage(
                seq=row["seq"],
                payload=row["payload"],
                sent_at=row["sent_at"],
                retry_count=row.get("retry_count", 0),
            )

    # --- Outbound: sending messages to the client ------------------------

    def prepare_outbound(self, payload: dict) -> OutboundMessage:
        """
        Allocate the next seq for an outbound message and buffer it.
        Caller is responsible for actually writing the frame to the
        socket; this just does the bookkeeping.
        """
        msg = OutboundMessage(
            seq=self._next_send_seq,
            payload=payload,
            sent_at=time.monotonic(),
        )
        self.outbox[msg.seq] = msg
        self._next_send_seq += 1
        return msg

    def handle_ack(self, seq: int) -> bool:
        """
        Remove an acked message from the outbox.
        Returns True if it was present (normal case), False if it
        was already removed (duplicate ACK -- harmless, just ignore).
        """
        return self.outbox.pop(seq, None) is not None

    def pending_for_retry(self, retry_after_seconds: float) -> list[OutboundMessage]:
        """
        Messages that have been unacked longer than retry_after_seconds.
        Caller resends these and should call mark_retried() after.
        """
        now = time.monotonic()
        return [
            msg for msg in self.outbox.values()
            if (now - msg.sent_at) >= retry_after_seconds
        ]

    def mark_retried(self, seq: int) -> None:
        msg = self.outbox.get(seq)
        if msg is not None:
            msg.sent_at = time.monotonic()
            msg.retry_count += 1

    def replay_from(self, last_received_seq: int) -> list[OutboundMessage]:
        """
        On reconnect: return every buffered outbound message the
        client hasn't confirmed, in seq order, so they can be
        retransmitted on the new connection.
        """
        return sorted(
            (msg for msg in self.outbox.values() if msg.seq > last_received_seq),
            key=lambda m: m.seq,
        )

    # --- Inbound: receiving messages from the client ----------------------

    def should_process_inbound(self, seq: int) -> bool:
        """
        True if this is the first time we've seen this seq (process
        it). False if it's a duplicate retransmit (still ACK it, but
        don't reprocess -- e.g. don't re-deliver a chat message to
        other room members twice).
        """
        if seq <= self.last_processed_seq or seq in self._seen_inbound_seqs:
            return False
        return True

    def reset_inbound(self) -> None:
        """
        Clear inbound (client -> node) sequence tracking so the next
        message stream is processed from scratch.

        Called when a NEW connection is established for this session.
        Clients restart their outbound seq at 1 on every connection /
        page reload, so without this reset a watermark left over from a
        previous connection would make those fresh messages look like
        already-processed duplicates and drop them. Only the inbound side
        is reset -- the outbound seq (_next_send_seq) and outbox are left
        intact so buffered server->client messages still replay correctly.
        """
        self.last_processed_seq = 0
        self._seen_inbound_seqs.clear()

    def mark_inbound_processed(self, seq: int) -> None:
        self._seen_inbound_seqs.add(seq)
        # Advance the low-water mark and prune anything below it --
        # once last_processed_seq passes a value, we don't need to
        # remember it individually anymore.
        while (self.last_processed_seq + 1) in self._seen_inbound_seqs:
            self.last_processed_seq += 1
            self._seen_inbound_seqs.discard(self.last_processed_seq)