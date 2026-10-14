"""Stable AML HTTP protocol; all memory-system behavior is injected."""

import asyncio
import hmac
import logging
import math
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request

from backend import AddContext, MemoryBackend
from contracts import AddRequest, MemoryRecord, SearchRequest
from receipts import ReceiptConflict, Receipts
from request_scheduler import AtCapacity, QueueTimeout, RequestScheduler

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    state_dir: Path = Path(os.getenv("AML_STATE_DIR", "./data"))
    api_key: str = os.getenv("AML_API_KEY", "")
    timeout: float = float(os.getenv("AML_REQUEST_TIMEOUT", "1800"))
    max_inflight: int = int(os.getenv("AML_MAX_INFLIGHT", "32"))
    max_active: int | None = int(os.environ["AML_MAX_ACTIVE"]) if "AML_MAX_ACTIVE" in os.environ else None
    queue_timeout: float = float(os.getenv("AML_QUEUE_TIMEOUT", "1800"))
    legacy_receipt_backend_id: str | None = None


def ranked_response(candidates, top_k: int):
    data = []
    seen = set()
    for candidate in candidates:
        item = MemoryRecord.model_validate(candidate)
        if item.id in seen:
            continue
        seen.add(item.id)
        data.append(item.model_dump(mode="json", exclude_none=True))
    data.sort(key=lambda item: item.get("score", -math.inf), reverse=True)
    return {"data": data[:top_k]}


def create_app(backend: MemoryBackend | None = None, *, settings: Settings | None = None) -> FastAPI:
    """Inject a backend, or expose the protocol/docs with operations unavailable."""
    if backend is not None and not isinstance(backend, MemoryBackend):
        raise TypeError("backend must inherit from MemoryBackend")
    if backend is not None and (not isinstance(backend.backend_id, str) or not backend.backend_id.strip()):
        raise ValueError("backend_id must be a stable non-empty string")
    settings = settings or Settings()
    for value in (settings.timeout, settings.queue_timeout):
        if not math.isfinite(value) or value <= 0:
            raise ValueError("execution and queue timeouts must be finite and positive")
    max_active = min(16, settings.max_inflight) if settings.max_active is None else settings.max_active
    if not 1 <= max_active <= settings.max_inflight:
        raise ValueError("require 1 <= max_active <= max_inflight")

    @asynccontextmanager
    async def lifespan(app):
        if backend is None:
            yield
            return
        app.state.receipts = Receipts(settings.state_dir, backend.backend_id,
                                     settings.legacy_receipt_backend_id)
        app.state.backend = backend
        try:
            await backend.start()
            app.state.ready = True
            yield
        finally:
            app.state.ready = False
            try:
                await backend.close()
            finally:
                app.state.receipts.close()

    app = FastAPI(title="AML Memory API", version="1.1.0", lifespan=lifespan)
    app.state.ready = False
    scheduler = RequestScheduler(max_active=max_active, max_inflight=settings.max_inflight,
                                 queue_timeout=settings.queue_timeout)
    app.state.scheduler = scheduler

    def authenticate(request: Request):
        if not settings.api_key:
            return
        authorization = request.headers.get("authorization", "")
        scheme, _, token = authorization.partition(" ")
        supplied = token if scheme.lower() in ("bearer", "token") else ""
        supplied = supplied or request.headers.get("x-api-key", "")
        if not hmac.compare_digest(supplied.encode(), settings.api_key.encode()):
            raise HTTPException(401, detail={"reason": "invalid API credential"})

    @asynccontextmanager
    async def operation(request: Request, user_id: str):
        authenticate(request)
        require_backend()
        key = user_id if backend.supports_user_concurrency else None
        try:
            async with scheduler.slot(key):
                async with asyncio.timeout(settings.timeout):
                    yield
        except AtCapacity as error:
            raise HTTPException(429, detail={"reason": "adapter is at capacity"}, headers={"Retry-After": "5"}) from error
        except QueueTimeout as error:
            raise HTTPException(504, detail={"reason": "memory queue wait timed out; retry the same request_id"}) from error
        except HTTPException:
            raise
        except ReceiptConflict as error:
            raise HTTPException(409, detail={"reason": str(error)}) from error
        except TimeoutError as error:
            raise HTTPException(504, detail={"reason": "memory operation timed out; retry the same request_id"}) from error
        except Exception as error:
            logger.error("Memory backend operation failed: %s", type(error).__name__)
            raise HTTPException(502, detail={"reason": "memory backend operation failed; retry or check server logs"}) from error

    def require_backend():
        if backend is None:
            raise HTTPException(503, detail={"reason": "memory backend is not configured"})
        if not app.state.ready:
            raise HTTPException(503, detail={"reason": "backend is not ready"})

    @app.get("/health")
    async def health():
        require_backend()
        return {"status": "ok", "backend": backend.name, "tracks": ["text", "coding"],
                "authentication": "configured" if settings.api_key else "none",
                "concurrency": scheduler.snapshot()}

    @app.post("/add")
    async def add(payload: AddRequest, request: Request):
        async with operation(request, payload.user_id):
            ledger = app.state.receipts
            created_at, complete = ledger.begin(payload)
            if not complete:
                context = AddContext(
                    request=payload, created_at=created_at,
                    _phase_reader=lambda index: ledger.phase(payload.request_id, index),
                    _phase_writer=lambda index, phase: ledger.advance(payload.request_id, index, phase),
                )
                await backend.add(payload, context)
                ledger.complete(payload.request_id)
        return {"success": True, "request_id": payload.request_id,
                "user_id": payload.user_id, "session_id": payload.session_id}

    @app.post("/search")
    async def search(payload: SearchRequest, request: Request):
        async with operation(request, payload.user_id):
            return ranked_response(await backend.search(payload), payload.top_k)

    return app
