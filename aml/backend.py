"""Implement this interface to serve any memory system through the same AML API."""

import json
from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid5

from contracts import AddRequest, MemoryRecord, SearchRequest

MESSAGE_NAMESPACE = UUID("acee9a0c-3c43-4d48-9688-f5b2784b9909")


@dataclass(frozen=True)
class AddContext:
    """Retry-stable IDs/times and optional progress for a backend's own steps.

    These helpers are internal; they do not add fields to the HTTP protocol.
    Checkpoint only after a step is durably complete. A step interrupted before
    checkpointing can run again, so backend writes must support idempotent retry.
    """

    request: AddRequest
    created_at: datetime
    _phase_reader: Callable[[int], int]
    _phase_writer: Callable[[int, int], None]

    def message_id(self, index: int) -> UUID:
        identity = json.dumps([self.request.user_id, self.request.request_id, index], ensure_ascii=False)
        return uuid5(MESSAGE_NAMESPACE, identity)

    def timestamp(self, index: int) -> datetime:
        value = self.request.messages[index].timestamp
        if value is not None:
            return datetime.fromtimestamp(value / 1000, UTC)
        return self.created_at + timedelta(microseconds=index)

    def phase(self, index: int) -> int:
        return self._phase_reader(index)

    def checkpoint(self, index: int, phase: int) -> None:
        self._phase_writer(index, phase)


class MemoryBackend(ABC):
    """Minimal interface: implement add and search; lifecycle hooks are optional.

    Each backend owns user isolation and its storage. user_id is the exact
    retrieval scope; session_id is source metadata and must not restrict search.
    Use a stable backend_id per method/storage version. Changing it gives the
    replacement backend independent request receipts, not automatic migration
    of memories; use a separate database/index directory for a different method.
    """

    # Opt in only when distinct user scopes can safely run simultaneously.
    # Custom backends retain the original whole-request serialization by default.
    supports_user_concurrency = False

    @property
    def backend_id(self) -> str:
        return f"{type(self).__module__}.{type(self).__qualname__}"

    @property
    def name(self) -> str:
        return type(self).__name__

    async def start(self) -> None:
        """Optional: connect clients/load resources at FastAPI startup."""

    async def close(self) -> None:
        """Optional: release resources at FastAPI shutdown."""

    @abstractmethod
    async def add(self, request: AddRequest, context: AddContext) -> None:
        """Persist ordered messages and await retrieval readiness before return.

        HTTP retries can invoke this after partial failure. Use stable message
        IDs, context checkpoints or the memory system's own idempotency mechanism.
        """
        raise NotImplementedError

    @abstractmethod
    async def search(self, request: SearchRequest) -> Sequence[MemoryRecord]:
        """Return evidence from this user only; no final answers or text clipping.

        The protocol layer validates records, removes duplicate IDs, sorts
        optional scores descending and enforces the global top_k limit.
        It also accepts dictionaries with the MemoryRecord fields.
        """
        raise NotImplementedError
