"""Offline native /bg execution; first phase deliberately dies after its wire receipt."""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch
from tests.gateway.test_gateway_background_route_replay import BackgroundRuntime
from tests.gateway.test_fast_consumer_execution import response_sink
from run_agent import AIAgent as RealAgent
import run_agent
import hermes_cli.runtime_provider as providers

sessions, record, phase = sys.argv[1:]
seen = []
built = []
with BackgroundRuntime(sessions, register=phase == "capture") as r:
    r.manager.discover_and_load()
    r.pick = {"provider": "openai-codex", "model": "gpt-6-astra", "tier": "priority"}
    r.prompt = "example-action-b."

    def credentials(**kw):
        return dict(
            provider=kw.get("requested") or "openai-codex",
            api_key="offline-fixture",
            base_url="https://chatgpt.com/backend-api/codex",
            api_mode="codex_responses",
        )

    class OfflineAgent(RealAgent):
        def __init__(self, **kwargs):
            built.append({
                key: kwargs[key] for key in ("provider", "model", "service_tier")
            })
            super().__init__(**kwargs)
            response_sink(self, seen)

    with (
        patch.object(providers, "resolve_runtime_provider", credentials),
        patch.object(run_agent, "AIAgent", OfflineAgent),
    ):
        asyncio.run(r.attempt())
    assert len(seen) == 1, r.adapter.send.await_args_list
    with open(record, "x") as f:
        json.dump(
            {
                "built": built,
                "wire": seen,
                "decisions": r.decisions,
                "pid": os.getpid(),
            },
            f,
        )
        f.flush()
        os.fsync(f.fileno())
    if phase == "capture":
        os._exit(73)
