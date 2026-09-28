"""Bounded replay buffer with drop-oldest semantics (mirrors the Go SDK)."""

from __future__ import annotations

import threading
from collections import deque
from typing import List


class EventBuffer:
    def __init__(self, capacity: int):
        self._capacity = max(1, capacity)
        self._events = deque(maxlen=self._capacity)
        self._base = 1  # seq of events[0]
        self._dropped = 0
        self._lock = threading.Lock()

    def add(self, ev) -> int:
        """Append an event, stamping the next sequence number."""
        with self._lock:
            seq = self._base + len(self._events)
            ev.seq = seq
            if len(self._events) == self._events.maxlen:
                self._events.popleft()
                self._base += 1
                self._dropped += 1
            self._events.append(ev)
            return seq

    def after(self, acked: int) -> List:
        """Events with seq > acked (the replay window)."""
        with self._lock:
            offset = max(acked + 1 - self._base, 0)
            if offset >= len(self._events):
                return []
            return list(self._events)[offset:]

    def acked(self, seq: int) -> None:
        """Trim everything up to and including seq."""
        with self._lock:
            drop = min(max(seq + 1 - self._base, 0), len(self._events))
            for _ in range(drop):
                self._events.popleft()
            self._base += drop

    def __len__(self) -> int:
        with self._lock:
            return len(self._events)
