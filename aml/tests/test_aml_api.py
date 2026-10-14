"""HTTP contract and retry tests, without contacting deployed models."""

import tempfile
import unittest
import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

from fastapi.testclient import TestClient

from aml_api import Settings, create_app
from backend import MemoryBackend
from contracts import AddRequest, MemoryRecord
from tests.keyword_fixture import KeywordMemoryBackend


class TestMemoryBackend(MemoryBackend):
    backend_id = "test-memory-v1"
    def __init__(self):
        self.records = {}
        self.indexed = set()
        self.ingested = set()
        self.index_failure = False

    async def start(self):
        pass

    async def close(self):
        pass

    async def prepare(self, scope):
        pass

    async def persist(self, scope, uid, message, metadata, created_at):
        self.records.setdefault(str(uid), {
            "scope": scope,
            "id": str(uid),
            "content": message.content,
            "created_at": created_at.isoformat(),
            "metadata": metadata,
            "score": 0.8 if message.role == "user" else 0.6,
        })

    async def index(self, scope, uid):
        if self.index_failure:
            self.index_failure = False
            raise RuntimeError("test index failure")
        self.indexed.add(str(uid))

    async def ingest(self, scope, uid):
        self.ingested.add(str(uid))

    async def add(self, request, context):
        await self.prepare(request.user_id)
        for index, message in enumerate(request.messages):
            uid = context.message_id(index)
            phase = context.phase(index)
            if phase < 1:
                await self.persist(request.user_id, uid, message,
                                   {"aml_message_index": str(index)}, context.timestamp(index))
                context.checkpoint(index, 1)
            if phase < 2:
                await self.index(request.user_id, uid)
                context.checkpoint(index, 2)
            if phase < 3:
                await self.ingest(request.user_id, uid)
                context.checkpoint(index, 3)

    async def search(self, request):
        return [dict(record) for uid, record in self.records.items()
                if record["scope"] == request.user_id and uid in self.indexed]


class AMLContractTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.settings = Settings(state_dir=Path(self.directory.name))
        self.backend = TestMemoryBackend()
        self.context = TestClient(create_app(self.backend, settings=self.settings))
        self.client = self.context.__enter__()

    def tearDown(self):
        self.context.__exit__(None, None, None)
        self.directory.cleanup()

    def payload(self, request_id="req-1", user_id="Alice/汉字", session_id="s1"):
        return {
            "request_id": request_id, "user_id": user_id, "session_id": session_id,
            "messages": [{"role": "user", "content": "I prefer aisle seats",
                          "timestamp": 1704067200123}],
        }

    def search(self, user_id="Alice/汉字", top_k=100, **extra):
        return self.client.post("/search", json={
            "user_id": user_id, "query": "What are my preferences?",
            "top_k": top_k, **extra,
        })

    def test_add_echoes_identifiers_and_waits_for_index_and_profile(self):
        payload = self.payload()
        response = self.client.post("/add", json=payload)
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json(), {
            "success": True, "request_id": "req-1", "user_id": "Alice/汉字",
            "session_id": "s1",
        })
        self.assertEqual(len(self.backend.records), 1)
        self.assertEqual(self.backend.indexed, set(self.backend.records))
        self.assertEqual(self.backend.ingested, set(self.backend.records))
        result = self.search().json()["data"][0]
        self.assertEqual(result["content"], "I prefer aisle seats")
        self.assertEqual(datetime.fromisoformat(result["created_at"]),
                         datetime(2024, 1, 1, 0, 0, 0, 123000, tzinfo=UTC))

    def test_same_user_can_retrieve_across_source_sessions_but_other_user_cannot(self):
        self.client.post("/add", json=self.payload())
        second = self.payload("req-2", session_id="s2")
        second["messages"][0]["content"] = "I avoid red-eye flights"
        self.client.post("/add", json=second)
        contents = {item["content"] for item in self.search().json()["data"]}
        self.assertEqual(contents, {"I prefer aisle seats", "I avoid red-eye flights"})
        self.assertEqual(self.search(user_id="another-user").json(), {"data": []})

    def test_repeated_request_and_adapter_restart_do_not_add_duplicate_memories(self):
        first = self.client.post("/add", json=self.payload())
        self.assertEqual(first.status_code, 200)
        self.assertEqual(self.client.post("/add", json=self.payload()).json(), first.json())
        with TestClient(create_app(self.backend, settings=self.settings)) as restarted:
            self.assertEqual(restarted.post("/add", json=self.payload()).status_code, 200)
        self.assertEqual(len(self.backend.records), 1)

    def test_request_id_reused_with_changed_content_is_rejected(self):
        self.client.post("/add", json=self.payload())
        payload = self.payload()
        payload["messages"][0]["content"] = "changed"
        response = self.client.post("/add", json=payload)
        self.assertEqual(response.status_code, 409)
        self.assertEqual(len(self.backend.records), 1)

    def test_retry_resumes_after_partial_failure_without_duplicate_raw_message(self):
        self.backend.index_failure = True
        self.assertEqual(self.client.post("/add", json=self.payload()).status_code, 502)
        self.assertEqual(self.client.post("/add", json=self.payload()).status_code, 200)
        self.assertEqual(len(self.backend.records), 1)
        self.assertEqual(len(self.backend.indexed), 1)
        self.assertEqual(len(self.backend.ingested), 1)

    def test_order_and_text_are_preserved_including_code_and_long_input(self):
        payload = self.payload()
        payload["messages"] = [
            {"role": "user", "content": "  def f():\n    return 42\n"},
            {"role": "assistant", "content": "long message " * 5000},
        ]
        response = self.client.post("/add", json=payload)
        self.assertEqual(response.status_code, 200, response.text)
        records = list(self.backend.records.values())
        self.assertEqual([record["content"] for record in records],
                         [message["content"] for message in payload["messages"]])
        self.assertEqual([r["metadata"]["aml_message_index"] for r in records], ["0", "1"])

    def test_search_is_ranked_and_globally_limited_and_accepts_options(self):
        payload = self.payload()
        payload["messages"] = [
            {"role": "assistant", "content": "lower score"},
            {"role": "user", "content": "higher score"},
        ]
        self.client.post("/add", json=payload)
        response = self.search(top_k=1, options=["A. Window", "B. Aisle"])
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(response.json()["data"]), 1)
        self.assertEqual(response.json()["data"][0]["content"], "higher score")
        self.assertEqual(set(response.json()["data"][0]), {"id", "content", "score", "created_at"})

    def test_invalid_and_unsupported_payloads_fail_without_writes(self):
        for field in ("request_id", "user_id", "session_id", "messages"):
            payload = self.payload()
            del payload[field]
            self.assertEqual(self.client.post("/add", json=payload).status_code, 422)
        for content in ("", "   ", [{"type": "image_url", "image_url": {"url": "data:image/png;base64,AA=="}}]):
            payload = self.payload()
            payload["messages"][0]["content"] = content
            self.assertEqual(self.client.post("/add", json=payload).status_code, 422)
        for value in (0, -1, True, "100"):
            self.assertEqual(self.search(top_k=value).status_code, 422)
        payload = self.payload()
        payload["messages"][0]["timestamp"] = True
        self.assertEqual(self.client.post("/add", json=payload).status_code, 422)
        payload["messages"][0]["timestamp"] = 10 ** 30
        self.assertEqual(self.client.post("/add", json=payload).status_code, 422)
        self.assertEqual(len(self.backend.records), 0)

    def test_optional_auth_supports_all_aml_schemes_and_health_is_public(self):
        settings = Settings(state_dir=Path(self.directory.name) / "auth", api_key="test-secret")
        with TestClient(create_app(self.backend, settings=settings)) as client:
            self.assertEqual(client.get("/health").status_code, 200)
            self.assertEqual(client.post("/add", json=self.payload()).status_code, 401)
            for headers in ({"Authorization": "Bearer test-secret"},
                            {"Authorization": "Token test-secret"},
                            {"X-Api-Key": "test-secret"}):
                self.assertEqual(client.post("/add", json=self.payload(), headers=headers).status_code, 200)

    def test_same_request_is_written_to_a_replacement_backend_and_old_receipts_remain(self):
        self.assertEqual(self.client.post("/add", json=self.payload()).status_code, 200)
        alternative = TestMemoryBackend()
        alternative.backend_id = "replacement-memory-v1"
        with TestClient(create_app(alternative, settings=self.settings)) as client:
            self.assertEqual(client.post("/add", json=self.payload()).status_code, 200)
            result = client.post("/search", json={"query": "preferences", "user_id": "Alice/汉字", "top_k": 100})
            self.assertEqual(result.json()["data"][0]["content"], "I prefer aisle seats")
        self.assertEqual(len(alternative.records), 1)
        self.assertEqual(self.client.post("/add", json=self.payload()).status_code, 200)
        self.assertEqual(len(self.backend.records), 1)

    def test_two_method_backend_gets_original_user_session_and_search_options(self):
        class MinimalBackend(MemoryBackend):
            async def add(self, request, context):
                self.saved = (request.user_id, request.session_id, request.messages[0].content)

            async def search(self, request):
                return [MemoryRecord(id="custom-memory", content=json.dumps({
                    "saved": self.saved, "query": request.query, "options": request.options,
                }, ensure_ascii=False))]

        with TestClient(create_app(MinimalBackend(), settings=self.settings)) as client:
            self.assertEqual(client.get("/health").json()["backend"], "MinimalBackend")
            self.assertEqual(client.post("/add", json=self.payload()).status_code, 200)
            result = client.post("/search", json={"query": "original question", "user_id": "Alice/汉字",
                                                  "top_k": 1, "options": ["A", "B"]})
            self.assertEqual(json.loads(result.json()["data"][0]["content"]), {
                "saved": ["Alice/汉字", "s1", "I prefer aisle seats"],
                "query": "original question", "options": ["A", "B"],
            })

    def test_keyword_backend_runs_without_memmachine_and_preserves_data_after_restart(self):
        database = Path(self.directory.name) / "keyword.sqlite3"
        settings = Settings(state_dir=Path(self.directory.name) / "keyword-receipts")
        for attempt in range(2):
            with TestClient(create_app(KeywordMemoryBackend(database), settings=settings)) as client:
                if attempt == 0:
                    self.assertEqual(client.post("/add", json=self.payload()).status_code, 200)
                result = client.post("/search", json={"query": "aisle seats", "user_id": "Alice/汉字", "top_k": 1})
                self.assertEqual(result.status_code, 200, result.text)
                self.assertEqual(result.json()["data"][0]["content"], "I prefer aisle seats")
                isolated = client.post("/search", json={"query": "aisle seats", "user_id": "another-user", "top_k": 1})
                self.assertEqual(isolated.json(), {"data": []})

    def test_legacy_receipts_are_imported_without_repeating_completed_writes(self):
        directory = Path(self.directory.name) / "legacy"
        directory.mkdir()
        request = AddRequest.model_validate(self.payload())
        digest = hashlib.sha256(json.dumps(request.model_dump(), sort_keys=True,
            separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        with sqlite3.connect(directory / "requests.sqlite3") as db:
            db.executescript("""
                CREATE TABLE requests(request_id TEXT PRIMARY KEY,digest TEXT,created_at TEXT,complete INTEGER);
                CREATE TABLE messages(request_id TEXT,message_index INTEGER,phase INTEGER,
                                      PRIMARY KEY(request_id,message_index));
            """)
            db.execute("INSERT INTO requests VALUES(?,?,?,?)", ("req-1", digest, "2024-01-01T00:00:00+00:00", 1))
            db.execute("INSERT INTO messages VALUES(?,?,?)", ("req-1", 0, 3))
        backend = TestMemoryBackend()
        settings = Settings(state_dir=directory, legacy_receipt_backend_id=backend.backend_id)
        with TestClient(create_app(backend, settings=settings)) as client:
            self.assertEqual(client.post("/add", json=self.payload()).status_code, 200)
            changed = self.payload()
            changed["messages"][0]["content"] = "changed content"
            self.assertEqual(client.post("/add", json=changed).status_code, 409)
        self.assertEqual(backend.records, {})
        with sqlite3.connect(directory / "requests.sqlite3") as db:
            self.assertEqual(db.execute("SELECT count(*) FROM requests").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT phase FROM backend_messages WHERE backend_id=?",
                                        (backend.backend_id,)).fetchone()[0], 3)


if __name__ == "__main__":
    unittest.main()
