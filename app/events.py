"""In-memory event sourcing + fan-out used by the SSE endpoint.

Design notes
------------
* Every run owns one :class:`EventBus`.
* ``publish`` is synchronous: it assigns the next per-run ``seq``, stores the
  event in a bounded ring buffer (default 500) and fans it out to all live
  subscriber queues.  Being synchronous makes the "replay then subscribe"
  sequence in ``subscribe`` atomic with respect to the asyncio event loop,
  which removes the classic replay/live race.
* ``subscribe(since)`` returns the subscriber queue plus the buffered events the
  client missed, so the SSE endpoint can replay them before it starts reading
  the live queue.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Optional, Set


@dataclass
class Event:
    seq: int
    run_id: str
    type: str
    ts: float
    node_id: Optional[str] = None
    data: Dict[str, Any] = field(default_factory=dict)

    def payload(self) -> Dict[str, Any]:
        body: Dict[str, Any] = {
            "seq": self.seq,
            "run_id": self.run_id,
            "type": self.type,
            "ts": round(self.ts, 6),
        }
        if self.node_id is not None:
            body["node_id"] = self.node_id
        body.update(self.data)
        return body

    def to_sse(self) -> str:
        return (
            f"id: {self.seq}\n"
            f"event: {self.type}\n"
            f"data: {json.dumps(self.payload(), ensure_ascii=False, separators=(',', ':'))}\n\n"
        )


class EventBus:
    """Bounded, replayable, fan-out event bus for one run."""

    def __init__(self, run_id: str, capacity: int = 500) -> None:
        self.run_id = run_id
        self.capacity = capacity
        self._events: Deque[Event] = deque(maxlen=capacity)
        self._subscribers: Set[asyncio.Queue] = set()
        self._last_seq = 0
        self.published = 0

    @property
    def last_seq(self) -> int:
        return self._last_seq

    def publish(self, event_type: str, node_id: Optional[str] = None, **data: Any) -> Event:
        self._last_seq += 1
        event = Event(
            seq=self._last_seq,
            run_id=self.run_id,
            type=event_type,
            ts=time.time(),
            node_id=node_id,
            data=data,
        )
        self._events.append(event)
        self.published += 1
        # Synchronous fan-out, no await: preserves ordering per subscriber.
        for queue in list(self._subscribers):
            queue.put_nowait(event)
        return event

    def subscribe(self, since: int) -> tuple[asyncio.Queue, List[Event], bool]:
        """Atomically snapshot missed events and register a live queue.

        ``since`` is the last seq the client already has (0 = from scratch).
        The third return value is True when the requested history has already
        been evicted from the ring buffer (client must reset its local state).
        """
        queue: asyncio.Queue = asyncio.Queue()
        missed = [event for event in self._events if event.seq > since]
        evicted = bool(missed) and missed[0].seq != since + 1
        self._subscribers.add(queue)
        return queue, missed, evicted

    def unsubscribe(self, queue: asyncio.Queue) -> None:
        self._subscribers.discard(queue)

    def buffered(self) -> List[Event]:
        return list(self._events)

    def oldest_seq(self) -> Optional[int]:
        return self._events[0].seq if self._events else None


TERMINAL_EVENT_TYPES = frozenset({"run_end", "run_cancelled"})
