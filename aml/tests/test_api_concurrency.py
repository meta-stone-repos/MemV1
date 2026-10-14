"""Actual ASGI requests with barriers, not timing-based throughput assertions."""

import asyncio
import tempfile
import unittest
from pathlib import Path

import httpx

from aml_api import Settings, create_app
from backend import MemoryBackend


class BarrierBackend(MemoryBackend):
    supports_user_concurrency = True

    def __init__(self):
        self.release = asyncio.Event()
        self.changed = asyncio.Event()
        self.entries = []
        self.active = 0
        self.peak = 0

    async def add(self, request, context):
        self.entries.append(("add", request.user_id, request.request_id))
        self.active += 1
        self.peak = max(self.peak, self.active)
        self.changed.set()
        try:
            await self.release.wait()
            context.checkpoint(0, 3)
        finally:
            self.active -= 1

    async def search(self, request):
        self.entries.append(("search", request.user_id, None))
        self.changed.set()
        return []


class APIConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.backend = BarrierBackend()
        self.tasks = []
        await self.open_app()

    async def open_app(self, **overrides):
        if hasattr(self, "client"):
            await self.client.aclose()
            await self.lifespan.__aexit__(None, None, None)
        self.app = create_app(self.backend, settings=Settings(
            state_dir=Path(self.directory.name), max_inflight=32, **overrides))
        self.lifespan = self.app.router.lifespan_context(self.app)
        await self.lifespan.__aenter__()
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://aml.test")

    async def asyncTearDown(self):
        self.backend.release.set()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        await self.client.aclose()
        await self.lifespan.__aexit__(None, None, None)
        self.directory.cleanup()

    def add(self, user, request_id=None):
        task = asyncio.create_task(self.client.post("/add", json={
            "user_id": user, "request_id": request_id or user, "session_id": "s1",
            "messages": [{"role": "user", "content": "unchanged input"}],
        }))
        self.tasks.append(task)
        return task

    async def entered(self, count):
        async with asyncio.timeout(2):
            while len(self.backend.entries) < count:
                self.backend.changed.clear()
                await self.backend.changed.wait()

    async def stats(self, **expected):
        async with asyncio.timeout(2):
            while True:
                response = await self.client.get("/health")
                counts = response.json().get("concurrency", {})
                if all(counts.get(key) == value for key, value in expected.items()):
                    return counts
                await asyncio.sleep(0.001)

    async def test_sixteen_distinct_users_reach_backend_before_any_finishes(self):
        tasks = [self.add(f"user-{i}") for i in range(16)]
        await self.entered(16)
        self.assertEqual(self.backend.peak, 16)
        self.assertFalse(any(task.done() for task in tasks))
        self.backend.release.set()
        self.assertEqual([r.status_code for r in await asyncio.gather(*tasks)], [200] * 16)

    async def test_same_user_add_search_and_duplicate_are_ordered_and_exactly_once(self):
        first = self.add("same", "stable-id")
        await self.entered(1)
        duplicate = self.add("same", "stable-id")
        search = asyncio.create_task(self.client.post("/search", json={"user_id": "same", "query": "q", "top_k": 1}))
        self.tasks.append(search)
        other = self.add("other")
        await self.entered(2)
        self.assertEqual([e[1] for e in self.backend.entries], ["same", "other"])
        self.backend.release.set()
        responses = await asyncio.gather(first, duplicate, search, other)
        self.assertEqual([r.status_code for r in responses], [200] * 4)
        self.assertEqual(len([e for e in self.backend.entries if e[0] == "add" and e[1] == "same"]), 1)
        await self.stats(active=0, inflight=0, lock_keys=0)

    async def test_sixteen_active_thirty_two_inflight_and_overflow_is_429(self):
        tasks = [self.add(f"limit-{i}") for i in range(32)]
        counts = await self.stats(active=16, queued=16, inflight=32)
        self.assertEqual((counts["max_active"], counts["max_inflight"]), (16, 32))
        overflow = await self.add("overflow")
        self.assertEqual(overflow.status_code, 429)
        self.assertEqual(overflow.headers["retry-after"], "5")
        self.backend.release.set()
        self.assertEqual([r.status_code for r in await asyncio.gather(*tasks)], [200] * 32)
        self.assertEqual(self.backend.peak, 16)
        await self.stats(active=0, queued=0, inflight=0, lock_keys=0)

    async def test_waiting_same_user_does_not_consume_execution_slots(self):
        self.add("same", "first")
        await self.entered(1)
        for i in range(15):
            self.add("same", f"waiting-{i}")
        for i in range(15):
            self.add(f"independent-{i}")
        await self.stats(active=16, queued=15, inflight=31)
        self.assertEqual(self.backend.peak, 16)

    async def test_queue_wait_does_not_use_execution_timeout(self):
        await self.open_app(timeout=0.12, queue_timeout=1, max_active=1)
        first = self.add("slow")
        await self.entered(1)
        second = self.add("next")
        # First expires; second must receive its own full execution budget.
        self.assertEqual((await first).status_code, 504)
        await self.entered(2)
        self.assertFalse(second.done())
        self.backend.release.set()
        self.assertEqual((await second).status_code, 200)
        await self.stats(active=0, inflight=0, lock_keys=0)

    async def test_queue_timeout_is_distinct_and_does_not_write_receipt(self):
        await self.open_app(timeout=2, queue_timeout=0.08, max_active=1)
        self.add("occupying")
        await self.entered(1)
        response = await self.add("queued")
        self.assertEqual(response.status_code, 504)
        self.assertIn("queue", response.json()["detail"]["reason"])
        row = self.app.state.receipts.db.execute(
            "SELECT 1 FROM backend_requests WHERE request_id='queued'").fetchone()
        self.assertIsNone(row)
        await self.stats(active=1, queued=0, inflight=1, lock_keys=1)

    async def test_cancelled_waiters_and_active_request_release_all_capacity(self):
        await self.open_app(max_active=1)
        active = self.add("first")
        await self.entered(1)
        same_user = self.add("first", "second")
        independent = self.add("other")
        await self.stats(active=1, queued=2, inflight=3)
        for task in (same_user, independent, active):
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.stats(active=0, queued=0, inflight=0, lock_keys=0)
        retry = self.add("first")
        await self.entered(2)
        self.backend.release.set()
        self.assertEqual((await retry).status_code, 200)

    async def test_custom_backends_stay_serial_unless_they_opt_in(self):
        self.backend.supports_user_concurrency = False
        first = self.add("one")
        await self.entered(1)
        second = self.add("two")
        await self.stats(active=1, queued=1, inflight=2)
        self.backend.release.set()
        self.assertEqual([r.status_code for r in await asyncio.gather(first, second)], [200, 200])
        self.assertEqual(self.backend.peak, 1)

    async def test_invalid_limits_fail_at_startup(self):
        for values in ({"max_active": 0}, {"max_inflight": -1},
                       {"max_active": 33, "max_inflight": 32},
                       {"timeout": 0}, {"queue_timeout": float("nan")}):
            with self.assertRaises(ValueError):
                create_app(self.backend, settings=Settings(state_dir=Path(self.directory.name), **values))

    async def test_older_small_inflight_setting_remains_valid_without_explicit_active_limit(self):
        for cap in (1, 8):
            app = create_app(self.backend, settings=Settings(state_dir=Path(self.directory.name), max_inflight=cap))
            self.assertEqual(app.state.scheduler.snapshot()["max_active"], cap)
