"""Persistent, backend-scoped request identity and optional write checkpoints."""

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from contracts import AddRequest


class ReceiptConflict(ValueError):
    pass


class Receipts:
    def __init__(self, directory: Path, backend_id: str, legacy_backend_id: str | None = None):
        directory.mkdir(parents=True, exist_ok=True)
        self.backend_id = backend_id
        self.db = sqlite3.connect(directory / "requests.sqlite3", timeout=30)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS backend_requests (
                backend_id TEXT NOT NULL, request_id TEXT NOT NULL,
                digest TEXT NOT NULL, created_at TEXT NOT NULL,
                complete INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (backend_id, request_id)
            );
            CREATE TABLE IF NOT EXISTS backend_messages (
                backend_id TEXT NOT NULL, request_id TEXT NOT NULL,
                message_index INTEGER NOT NULL, phase INTEGER NOT NULL,
                PRIMARY KEY (backend_id, request_id, message_index)
            );
        """)
        if legacy_backend_id is not None:
            tables = {row[0] for row in self.db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            # Keep the legacy tables intact and never overwrite newer progress.
            if "requests" in tables:
                self.db.execute("""INSERT OR IGNORE INTO backend_requests
                    SELECT ?,request_id,digest,created_at,complete FROM requests""", (legacy_backend_id,))
            if "messages" in tables:
                self.db.execute("""INSERT OR IGNORE INTO backend_messages
                    SELECT ?,request_id,message_index,phase FROM messages""", (legacy_backend_id,))
        self.db.commit()

    def begin(self, request: AddRequest):
        serialized = json.dumps(request.model_dump(), sort_keys=True,
                                separators=(",", ":"), ensure_ascii=False)
        digest = hashlib.sha256(serialized.encode()).hexdigest()
        row = self.db.execute(
            "SELECT digest,created_at,complete FROM backend_requests WHERE backend_id=? AND request_id=?",
            (self.backend_id, request.request_id),
        ).fetchone()
        if row:
            if row[0] != digest:
                raise ReceiptConflict("request_id was used with a different payload")
            return datetime.fromisoformat(row[1]), bool(row[2])
        created_at = datetime.now(UTC)
        self.db.execute("""INSERT INTO backend_requests(backend_id,request_id,digest,created_at)
                           VALUES(?,?,?,?)""",
                        (self.backend_id, request.request_id, digest, created_at.isoformat()))
        self.db.commit()
        return created_at, False

    def phase(self, request_id: str, index: int) -> int:
        row = self.db.execute("""SELECT phase FROM backend_messages
                                 WHERE backend_id=? AND request_id=? AND message_index=?""",
                              (self.backend_id, request_id, index)).fetchone()
        return row[0] if row else 0

    def advance(self, request_id: str, index: int, phase: int):
        self.db.execute("""INSERT INTO backend_messages VALUES(?,?,?,?)
            ON CONFLICT(backend_id,request_id,message_index) DO UPDATE SET phase=excluded.phase""",
                        (self.backend_id, request_id, index, phase))
        self.db.commit()

    def complete(self, request_id: str):
        self.db.execute("UPDATE backend_requests SET complete=1 WHERE backend_id=? AND request_id=?",
                        (self.backend_id, request_id))
        self.db.commit()

    def close(self):
        self.db.close()
