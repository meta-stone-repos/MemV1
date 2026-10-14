"""Memory-system-independent AML request and result types."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StrictStr, field_validator


def nonempty(value: str) -> str:
    if not value.strip():
        raise ValueError("must contain non-whitespace text")
    return value


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["user", "assistant"]
    content: StrictStr
    timestamp: Annotated[StrictInt, Field(ge=0)] | None = None
    _content = field_validator("content")(nonempty)

    @field_validator("timestamp")
    @classmethod
    def valid_timestamp(cls, value):
        if value is not None:
            try:
                datetime.fromtimestamp(value / 1000, UTC)
            except (OverflowError, OSError, ValueError) as error:
                raise ValueError("timestamp must be representable Unix milliseconds") from error
        return value


class AddRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: StrictStr
    user_id: StrictStr
    session_id: StrictStr
    messages: Annotated[list[Message], Field(min_length=1)]
    _identifiers = field_validator("request_id", "user_id", "session_id")(nonempty)


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: StrictStr
    user_id: StrictStr
    top_k: Annotated[StrictInt, Field(gt=0, le=1000)]
    options: list[StrictStr] | None = None
    _text = field_validator("query", "user_id")(nonempty)


class MemoryRecord(BaseModel):
    """A retrieved memory, not a generated answer. Higher scores rank first."""

    model_config = ConfigDict(extra="ignore")
    id: StrictStr
    content: StrictStr
    score: Annotated[float, Field(strict=True, allow_inf_nan=False)] | None = None
    created_at: StrictStr | datetime | None = None
    _text = field_validator("id", "content")(nonempty)
