"""The public entry point must run without any memory framework installed."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


class StandaloneTests(unittest.TestCase):
    def test_unconfigured_server_exposes_docs_but_never_acknowledges_writes(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory) / "state"
            result = subprocess.run(
                [sys.executable, "-c", '''
from fastapi.testclient import TestClient
from server import app

with TestClient(app) as client:
    assert client.get("/docs").status_code == 200
    schema = client.get("/openapi.json").json()
    assert "/add" in schema["paths"] and "/search" in schema["paths"]
    assert client.get("/health").status_code == 503
    payload = {"request_id": "r1", "user_id": "u1", "session_id": "s1",
               "messages": [{"role": "user", "content": "keep this"}]}
    for path, data in [("/add", payload),
                       ("/search", {"user_id": "u1", "query": "keep", "top_k": 1})]:
        response = client.post(path, json=data)
        assert response.status_code == 503, response.text
        assert "not configured" in response.json()["detail"]["reason"]
'''],
                cwd=Path(__file__).resolve().parents[1],
                env={**os.environ, "AML_STATE_DIR": str(state), "AML_API_KEY": ""},
                capture_output=True, text=True, timeout=20,
            )
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            self.assertFalse(state.exists(), "an unconfigured server must not create write receipts")
