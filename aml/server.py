"""Composition only: replace None with your MemoryBackend instance."""

from aml_api import create_app

app = create_app(backend=None)
