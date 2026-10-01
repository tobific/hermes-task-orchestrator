"""Offline SDK/HTTP receipts for native consumer executions."""

import importlib.util
import json
import pytest

# These tests drive the gateway through gateway/route_policy.py, a separate "gateway route policy" seam that is
# NOT part of this patch series (README "Known coupling"). Without it they are expected to fail; with it they run.
needs_route_policy = pytest.mark.xfail(
    importlib.util.find_spec("gateway.route_policy") is None,
    reason="needs the separate gateway route-policy seam (gateway/route_policy.py), not in this series",
    strict=True,
)
from unittest.mock import patch
from tests.gateway.test_fast_consumer_execution import overlay, real_agent


@pytest.fixture
def http_sink(monkeypatch):
    import httpx
    from run_agent import AIAgent

    seen = []

    def respond(request):
        payload = json.loads(request.content)
        seen.append(payload)
        from tests.fast_consumer_receipts import record

        record("sdk-http-body", payload)
        response = {
            "id": "resp_offline",
            "object": "response",
            "created_at": 0,
            "status": "completed",
            "model": payload["model"],
            "output": [
                {
                    "type": "message",
                    "id": "msg_offline",
                    "role": "assistant",
                    "status": "completed",
                    "content": [
                        {
                            "type": "output_text",
                            "text": "offline complete",
                            "annotations": [],
                        }
                    ],
                }
            ],
            "usage": {"input_tokens": 10, "output_tokens": 2, "total_tokens": 12},
        }
        if payload.get("stream"):
            events = [
                {
                    "type": "response.output_item.done",
                    "output_index": 0,
                    "item": response["output"][0],
                },
                {"type": "response.completed", "response": response},
            ]
            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content="".join(
                    "data: " + json.dumps(event) + "\n\n" for event in events
                )
                + "data: [DONE]\n\n",
            )
        return httpx.Response(200, json=response)

    def client(*args, **kwargs):
        return httpx.Client(transport=httpx.MockTransport(respond))

    monkeypatch.setattr(AIAgent, "_build_keepalive_http_client", staticmethod(client))
    return seen


@needs_route_policy
def test_foreground_real_loop_reaches_sdk_wire(overlay, http_sink):
    from gateway.route_policy import turn_route_scope

    a = real_agent(max_iterations=2)
    try:
        for index in range(2):
            if index == 1:
                overlay.toggle()
            with turn_route_scope(
                object(),
                session_key="s",
                turn_id=str(index),
                generation=index,
                message="hello",
                is_current=lambda: True,
            ):
                result = a.run_conversation("hello")
                assert result["final_response"] == "offline complete", result
        assert [request.get("service_tier") for request in http_sink] == [
            None,
            "priority",
        ]
        assert http_sink[0]["instructions"] == http_sink[1]["instructions"]
    finally:
        a.close()


def test_cron_run_job_reaches_sdk_wire(overlay, http_sink, monkeypatch):
    from cron.scheduler import run_job
    import hermes_cli.runtime_provider as providers

    with (overlay.home / "config.yaml").open("a") as f:
        f.write("cron:\n  preflight: false\nagent:\n  max_turns: 2\n")
    monkeypatch.setattr(
        providers,
        "resolve_runtime_provider",
        lambda **kw: dict(
            provider="openai-codex",
            api_key="offline-fixture",
            base_url="https://chatgpt.com/backend-api/codex",
            api_mode="codex_responses",
            request_overrides={"service_tier": "priority"},
        ),
    )
    job = {
        "id": "proof-job",
        "name": "example-weekly-job",
        "prompt": "hello",
        "model": "gpt-6-astra",
        "provider": "openai-codex",
    }
    for index in range(2):
        if index == 1:
            overlay.toggle()
        result = run_job(job)
        assert result[0] and result[2] == "offline complete", result
    assert [request.get("service_tier") for request in http_sink] == [None, "priority"]


@needs_route_policy
@pytest.mark.parametrize(
    "message", ["hello", "example trigger-a.", "example trigger-b.", "example-action-a.", "example-action-b."]
)
def test_foreground_route_replay_constructor_and_sdk(
    overlay, http_sink, monkeypatch, tmp_path, message
):
    from types import SimpleNamespace as NS
    from tests.gateway.test_gateway_route_replay_proof import Runtime
    from gateway.route_policy import turn_route_scope, bind_gateway_session_identity
    from gateway.run_turn_runner import TurnRunner
    from run_agent import AIAgent
    import hermes_cli.runtime_provider as providers

    constructed = []
    with Runtime(tmp_path / "sessions") as r:
        r.manager.discover_and_load()
        r.pick = {
            "provider": "openai-codex",
            "model": "gpt-6-astra",
            "tier": "priority",
        }
        r.stack.enter_context(
            patch.object(
                providers,
                "resolve_runtime_provider",
                lambda **kw: dict(
                    provider="openai-codex",
                    api_key="offline-fixture",
                    base_url="https://chatgpt.com/backend-api/codex",
                    api_mode="codex_responses",
                ),
            )
        )
        r.runner._prefill_messages = []
        r.runner._session_db = None
        t = object.__new__(TurnRunner)
        t._runner = r.runner
        t._ctx = NS(
            source=r.source,
            session_key=r.key,
            session_id=r.entry.session_id,
            user_config={},
            enabled_toolsets=[],
            disabled_toolsets=[],
            AIAgent=AIAgent,
        )
        for index in range(2):
            if index == 0:
                overlay.toggle()
            else:
                overlay.toggle()
                r.handle.release()
                r.handle = None
            with turn_route_scope(
                r.runner,
                session_key=r.key,
                turn_id="stable-event",
                generation=index,
                message=message,
                is_current=lambda: True,
            ):
                bind_gateway_session_identity(r.runner, r.key, r.entry.session_id)
                model, runtime = r.runner._resolve_session_agent_runtime(
                    source=r.source, session_key=r.key, user_config=r.config
                )
                route = r.runner._resolve_turn_agent_config(message, model, runtime)
                constructed.append((
                    route["model"],
                    route["runtime"]["provider"],
                    route["service_tier"],
                ))
                a = t._build_fresh_agent(route, "telegram", "", 2, None, {}, True)
                try:
                    assert (
                        a.run_conversation(message)["final_response"]
                        == "offline complete"
                    )
                finally:
                    a.close()
        assert constructed == [("gpt-6-astra", "openai-codex", "priority")] * 2
        assert len(r.decisions) == 1
        assert [request.get("service_tier") for request in http_sink] == [
            "priority",
            "priority" if message == "example trigger-b." else None,
        ]


@needs_route_policy
@pytest.mark.asyncio
async def test_native_completion_admission_to_sdk(overlay, http_sink, monkeypatch):
    from unittest.mock import AsyncMock
    from gateway.config import GatewayConfig, Platform, PlatformConfig
    from gateway.platforms.base import SendResult
    from gateway.run import GatewayRunner
    from gateway.session import SessionSource, build_session_key
    from plugins.platforms.discord.adapter import DiscordAdapter
    from tests.gateway.test_completion_admission import pending, drain
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    import hermes_cli.runtime_provider as providers

    token = set_hermes_home_override(overlay.home)
    with (overlay.home / "config.yaml").open("a") as f:
        f.write(
            "model:\n  default: gpt-6-astra\n  provider: openai-codex\nagent:\n  max_turns: 2\n"
        )
    monkeypatch.setattr(
        providers,
        "resolve_runtime_provider",
        lambda **kw: dict(
            provider="openai-codex",
            api_key="offline-fixture",
            base_url="https://chatgpt.com/backend-api/codex",
            api_mode="codex_responses",
        ),
    )
    runner = GatewayRunner(GatewayConfig())
    adapter = DiscordAdapter(PlatformConfig(enabled=True, typing_indicator=False))
    adapter.send = AsyncMock(
        return_value=SendResult(success=True, message_id="offline-send")
    )
    runner.adapters = {Platform.DISCORD: adapter}
    adapter.set_message_handler(runner._handle_message)
    source = SessionSource(
        platform=Platform.DISCORD, chat_type="dm", chat_id="42", user_id="42"
    )
    key = build_session_key(source)
    try:
        for index in range(2):
            if index == 1:
                overlay.toggle()
            event = pending(key, f"fast-completion-{index}")
            assert await runner._deliver_async_delegation_group([event]) is True
            await drain(adapter)
        assert [request.get("service_tier") for request in http_sink] == [
            None,
            "priority",
        ], adapter.send.await_args_list
    finally:
        await drain(adapter)
        await runner._cancel_process_completion_batch_tasks()
        reset_hermes_home_override(token)


@needs_route_policy
@pytest.mark.asyncio
async def test_native_completion_with_required_route_policy(
    overlay, http_sink, monkeypatch
):
    from hermes_cli.plugins import PluginContext, PluginManifest
    from gateway.route_policy import RouteProposal

    ctx = PluginContext(PluginManifest(name="completion-route"), overlay.manager)
    handle = ctx.register_gateway_route_policy(
        "route", lambda request: RouteProposal("openai-codex", "gpt-6-astra", "normal")
    )
    try:
        await test_native_completion_admission_to_sdk(overlay, http_sink, monkeypatch)
    finally:
        handle.release()
