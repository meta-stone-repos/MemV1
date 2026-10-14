"""Single-event-loop admission + per-user ordering, with bounded execution."""

import asyncio
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass


class QueueTimeout(TimeoutError):
    pass


class AtCapacity(Exception):
    pass


@dataclass
class _KeyLock:
    lock: asyncio.Lock
    references: int = 0


class RequestScheduler:
    def __init__(self, *, max_active, max_inflight, queue_timeout):
        self.max_active = max_active
        self.max_inflight = max_inflight
        self.queue_timeout = queue_timeout
        self.active = 0
        self.inflight = 0
        self._slots = asyncio.Semaphore(max_active)
        self._keys = {}

    def snapshot(self):
        return {"max_active": self.max_active, "max_inflight": self.max_inflight,
                "active": self.active, "queued": self.inflight - self.active,
                "inflight": self.inflight, "lock_keys": len(self._keys)}

    @asynccontextmanager
    async def slot(self, key):
        # No awaits between admission/check/increment: atomic in one event loop.
        if self.inflight >= self.max_inflight:
            raise AtCapacity
        self.inflight += 1
        entry = self._keys.setdefault(key, _KeyLock(asyncio.Lock()))
        entry.references += 1
        try:
            async with AsyncExitStack() as stack:
                try:
                    async with asyncio.timeout(self.queue_timeout):
                        # Acquire user lock FIRST, so same-user waiters do not
                        # reserve scarce execution slots needed by other users.
                        await stack.enter_async_context(entry.lock)
                        await stack.enter_async_context(self._slots)
                except TimeoutError as error:
                    raise QueueTimeout from error
                self.active += 1
                try:
                    yield
                finally:
                    self.active -= 1
        finally:
            self.inflight -= 1
            entry.references -= 1
            if not entry.references:
                del self._keys[key]
