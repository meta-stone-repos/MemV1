"""Small persistent alternative backend demonstrating the two-method contract."""

import sqlite3
from pathlib import Path

from backend import AddContext, MemoryBackend
from contracts import AddRequest, MemoryRecord, SearchRequest


class KeywordMemoryBackend(MemoryBackend):
    """SQLite keyword retrieval example; no MemMachine or model dependency."""

    backend_id = "sqlite-keyword-v1"

    def __init__(self, database: Path):
        self.database = database
        self.db = None

    async def start(self):
        self.database.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.database)
        self.db.execute("""CREATE TABLE IF NOT EXISTS memories (
            id TEXT PRIMARY KEY,user_id TEXT NOT NULL,session_id TEXT NOT NULL,
            content TEXT NOT NULL,created_at TEXT NOT NULL)""")
        self.db.commit()

    async def close(self):
        if self.db is not None:
            self.db.close()

    async def add(self, request: AddRequest, context: AddContext) -> None:
        self.db.executemany("INSERT OR IGNORE INTO memories VALUES(?,?,?,?,?)", [
            (str(context.message_id(index)), request.user_id, request.session_id,
             message.content, context.timestamp(index).isoformat())
            for index, message in enumerate(request.messages)
        ])
        self.db.commit()

    async def search(self, request: SearchRequest) -> list[MemoryRecord]:
        terms = set(request.query.casefold().split())
        candidates = []
        for identifier, content, created_at in self.db.execute(
            "SELECT id,content,created_at FROM memories WHERE user_id=?", (request.user_id,)
        ):
            score = sum(term in content.casefold() for term in terms) / len(terms)
            if score:
                candidates.append(MemoryRecord(id=identifier, content=content,
                                               score=score, created_at=created_at))
        return candidates
