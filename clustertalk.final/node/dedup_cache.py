#node/dedup_cache.py

"""
Prevents relay storms in the peer-to-peer mesh.

The problem: Node A broadcasts a room message across the mesh by
sending RELAY to every peer it's connected to (Node B, Node C). If
Node B then also relays it onward to Node C and Node A (because it
doesn't know who has already seen it), and Node C does the same back
to A and B, the same message circulates forever, doubling in flight
copies with every hop. On a LAN this can saturate every mesh link in
well under a second.

The fix: every RELAY carries a msg_id (a UUID minted once, by the
node where the message originated). Every node remembers msg_ids
it has already relayed. Before relaying (or delivering) a RELAY,
check: have I handled this msg_id before? If yes, drop it silently
-- it's already been propagated and/or delivered by this node. If
no, record it, then process it.

Bounded (LRU) rather than an ever-growing set: a msg_id only needs to
be remembered long enough to survive its realistic propagation
window across the mesh (low milliseconds on a LAN). Keeping every
msg_id forever would leak memory on a long-running node for no
benefit.
"""

from __future__ import annotations

from collections import OrderedDict


class DedupCache:
    """Bounded LRU set of recently-seen msg_ids. One instance per node
    (mesh.py owns it) -- it is NOT shared with SessionManager's seq
    numbers, which solve a different problem (client<->node exactly-once)
    at a different layer (node<->node mesh flood control)."""

    def __init__(self, max_size: int = 20_000):
        self._max_size = max_size
        self._seen: "OrderedDict[str, None]" = OrderedDict()

    def seen_before(self, msg_id: str) -> bool:
        """Atomic check-and-record: returns True if msg_id was already
        recorded (caller should drop the message), False if this is the
        first time (caller should process/relay it, and it's now recorded
        so a repeat gets True)."""
        if msg_id in self._seen:
            self._seen.move_to_end(msg_id)
            return True

        self._seen[msg_id] = None
        if len(self._seen) > self._max_size:
            self._seen.popitem(last=False)  # evict oldest
        return False

    def __len__(self) -> int:
        return len(self._seen)
