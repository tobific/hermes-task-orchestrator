"""Consumer conformance through native routing, ownership and real Responses wire."""

import asyncio
from pathlib import Path
import shutil
from types import SimpleNamespace as NS
from unittest.mock import patch
import pytest
from tests.hermes_cli.test_service_tier_consumption import agent, wire, terminal


@pytest.fixture
def overlay(tmp_path, monkeypatch):
    import socket
    import hermes_cli.plugins as plugins
    from tests.fixtures.builtin_policy import fast_state

    monkeypatch.setattr(
        socket.socket, "connect", lambda *a: pytest.fail("external network forbidden")
    )
    home = tmp_path / "profile"
    path = home / "plugins" / "fast-consumers"
    shutil.copytree(Path(__file__).parents[1] / "fixtures" / "fast_consumers", path)
    shutil.copyfile(
        Path(__file__).parents[1] / "fixtures" / "builtin_policy" / "fast_state.py",
        path / "fast_state.py",
    )
    (home / "config.yaml").write_text(
        "plugins:\n  enabled: [fast-consumers]\nservice_tier_policy: fast-consumers:overlay\n"
    )
    monkeypatch.setenv("HERMES_HOME", str(home))
    manager = plugins.PluginManager()
    monkeypatch.setattr(plugins, "get_plugin_manager", lambda: manager)
    manager.discover_and_load()
    state_path = home / "fast-mode.json"
    fast_state.initialize_fast_mode_state(state_path=state_path)
    counter = [0]

    def toggle():
        counter[0] += 1
        return fast_state.toggle_fast_mode(
            command_key=str(counter[0]), state_path=state_path
        )

    return NS(home=home, manager=manager, toggle=toggle, state_path=state_path)


@pytest.mark.parametrize(
    "message", ["hello", "example trigger-a.", "example trigger-b.", "example-action-a.", "example-action-b."]
)
def test_gateway_capture_live_overlay_and_wire(overlay, message):
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn

    runner = object()
    a = agent()
    # OFF at route capture; ON at execution; stable after another toggle within run.
    with turn_route_scope(
        runner,
        session_key="s",
        turn_id="event",
        generation=1,
        message=message,
        is_current=lambda: True,
    ):
        overlay.toggle()
        begin_turn(a, [])
        sent = terminal(a, wire(a))[0]
        assert sent.get("service_tier") == "priority"
        overlay.toggle()
        assert terminal(a, wire(a))[0].get("service_tier") == "priority"
    with turn_route_scope(
        runner,
        session_key="s",
        turn_id="event",
        generation=2,
        message=message,
        is_current=lambda: True,
    ):
        begin_turn(a, [])
        sent = terminal(a, wire(a))[0]
        assert sent.get("service_tier") == (
            "priority" if message == "example trigger-b." else None
        )
        assert sent["instructions"] == "unchanged"


def real_agent(**kwargs):
    from run_agent import AIAgent

    return AIAgent(
        api_key="offline-fixture",
        base_url="https://chatgpt.com/backend-api/codex",
        provider="openai-codex",
        api_mode="codex_responses",
        model="gpt-6-astra",
        platform="telegram",
        quiet_mode=True,
        skip_context_files=True,
        skip_memory=True,
        save_trajectories=False,
        enabled_toolsets=[],
        **kwargs,
    )


@pytest.mark.parametrize("model", ["gpt-6-luna", "gpt-6-astra"])
def test_native_child_start_owns_live_overlay(overlay, model):
    from tools.delegate_tool import _build_child_agent
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn

    parent = real_agent(request_overrides={"service_tier": "priority"})
    child = None
    try:
        overlay.toggle()
        with turn_route_scope(
            object(),
            session_key="s",
            turn_id="fast-parent",
            generation=1,
            message="example trigger-b.",
            is_current=lambda: True,
        ):
            child = _build_child_agent(
                task_index=0,
                goal="goal",
                context=None,
                toolsets=[],
                model=model,
                max_iterations=1,
                task_count=1,
                parent_agent=parent,
            )
        overlay.toggle()
        begin_turn(child, [])
        assert terminal(child, wire(child))[0].get("service_tier") is None
        overlay.toggle()
        begin_turn(child, [])
        assert terminal(child, wire(child))[0].get("service_tier") == "priority"
        assert parent.request_overrides["service_tier"] == "priority"
    finally:
        if child is not None:
            child.close()
        parent.close()


@pytest.mark.parametrize(
    "job_name", ["example-weekly-job", "other-job"]
)
def test_native_cron_identity_controls_overlay(overlay, job_name):
    from run_agent import AIAgent
    from cron.scheduler import _CronAgentSetup
    from cron.scheduler_agent import construct_cron_agent
    from agent.fast_mode import begin_turn

    setup = _CronAgentSetup(
        model="gpt-6-astra",
        runtime=dict(
            provider="openai-codex",
            api_mode="codex_responses",
            base_url="https://chatgpt.com/backend-api/codex",
            api_key="offline-fixture",
            request_overrides={"service_tier": "priority"},
        ),
        max_iterations=1,
    )
    a = construct_cron_agent(
        AIAgent,
        {"id": "job", "name": job_name},
        {},
        setup,
        workdir=None,
        session_id="cron-session",
        session_db=None,
    )
    try:
        begin_turn(a, [])
        assert terminal(a, wire(a))[0].get("service_tier") == (
            None if job_name == "example-weekly-job" else "priority"
        )
        overlay.toggle()
        begin_turn(a, [])
        assert terminal(a, wire(a))[0].get("service_tier") == "priority"
    finally:
        a.close()


def test_expired_gateway_context_cannot_consume_policy(overlay):
    from contextvars import copy_context
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn
    from agent.service_tier_policy import ServiceTierPolicyError

    a = agent()
    overlay.toggle()
    with turn_route_scope(
        object(),
        session_key="s",
        turn_id="e",
        generation=1,
        message="hello",
        is_current=lambda: True,
    ):
        begin_turn(a, [])
        context = copy_context()
    with pytest.raises(ServiceTierPolicyError):
        context.run(wire, a)


def response_sink(a, seen):
    a._disable_streaming = True

    def call(request):
        seen.append(request)
        from tests.fast_consumer_receipts import record

        record("native-loop-wire", request)
        return NS(
            id="offline",
            status="completed",
            model=a.model,
            output=[
                NS(
                    type="message",
                    id="msg",
                    role="assistant",
                    status="completed",
                    content=[
                        NS(type="output_text", text="offline complete", annotations=[])
                    ],
                )
            ],
            usage=NS(input_tokens=10, output_tokens=2, total_tokens=12),
        )

    a._interruptible_api_call = call


def test_native_child_full_loop_and_completion(overlay):
    from tools.delegate_tool import _build_child_agent, _run_single_child

    overlay.toggle()
    parent = real_agent(request_overrides={"service_tier": "priority"})
    child = _build_child_agent(
        task_index=0,
        goal="return done",
        context=None,
        toolsets=[],
        model="gpt-6-luna",
        max_iterations=2,
        task_count=1,
        parent_agent=parent,
    )
    seen = []
    response_sink(child, seen)
    overlay.toggle()
    try:
        result = _run_single_child(0, "return done", child, parent)
        assert result["status"] == "completed", result
        assert seen and all(request.get("service_tier") is None for request in seen)
    finally:
        parent.close()


@pytest.mark.parametrize(
    "case",
    [
        {"platform": "local"},
        {"platform": "cli"},
        {"platform": "desktop"},
        {
            "provider": "openrouter",
            "base_url": "https://openrouter.ai/api/v1",
            "api_mode": "chat_completions",
        },
        {
            "provider": "claude-code",
            "model": "claude-opus-5",
            "base_url": "acp://claude-code",
            "api_mode": "chat_completions",
        },
        {"base_url": "https://chatgpt.com.evil.invalid/backend-api/codex"},
        {"base_url": "https://user@chatgpt.com/backend-api/codex"},
        {"base_url": "https://chatgpt.com/backend-api/codex?proxy=1"},
        {"base_url": "https://chatgpt.com:bad/backend-api/codex"},
        {"api_mode": "chat_completions"},
        {"model": "unlisted"},
    ],
    ids=[
        "local",
        "cli",
        "desktop",
        "openrouter",
        "claude",
        "proxy",
        "userinfo",
        "query",
        "badport",
        "mode",
        "model",
    ],
)
def test_exclusions_keep_native_overrides(overlay, case):
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn, effective_request_overrides

    overlay.toggle()
    a = agent()
    for key, value in case.items():
        setattr(a, key, value)
    before = dict(a.request_overrides)
    with turn_route_scope(
        object(),
        session_key="s",
        turn_id="e",
        generation=1,
        message="hello",
        is_current=lambda: True,
    ):
        begin_turn(a, [])
        assert effective_request_overrides(a) == before


@pytest.mark.parametrize("state", ["{broken", '{"enabled":true}', "missing"])
def test_malformed_state_retains_existing_off_contract(overlay, state):
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn

    if state == "missing":
        overlay.state_path.unlink()
    else:
        overlay.state_path.write_text(state)
    a = agent()
    a.request_overrides["service_tier"] = "priority"
    with turn_route_scope(
        object(),
        session_key="s",
        turn_id="e",
        generation=1,
        message="hello",
        is_current=lambda: True,
    ):
        begin_turn(a, [])
        assert terminal(a, wire(a))[0].get("service_tier") is None


def test_child_cannot_borrow_unconfigured_profile(overlay, tmp_path):
    from tools.delegate_tool import _build_child_agent
    from agent.fast_mode import begin_turn
    from agent.service_tier_policy import ServiceTierPolicyError
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override

    parent = real_agent()
    child = _build_child_agent(
        task_index=0,
        goal="goal",
        context=None,
        toolsets=[],
        model="gpt-6-luna",
        max_iterations=1,
        task_count=1,
        parent_agent=parent,
    )
    token = set_hermes_home_override(tmp_path / "unconfigured")
    try:
        with pytest.raises(ServiceTierPolicyError):
            begin_turn(child, [])
    finally:
        reset_hermes_home_override(token)
        child.close()
        parent.close()


@pytest.mark.parametrize(
    "message", ["hello", "example trigger-a.", "example trigger-b.", "example-action-a.", "example-action-b."]
)
def test_native_background_execution_and_durable_replay(
    overlay, monkeypatch, tmp_path, message
):
    from tests.gateway.test_gateway_background_route_replay import BackgroundRuntime
    from run_agent import AIAgent as RealAgent
    import run_agent
    import hermes_cli.runtime_provider as providers

    seen = []
    constructed = []
    with BackgroundRuntime(tmp_path / "sessions") as r:
        r.manager.discover_and_load()
        r.pick = {
            "provider": "openai-codex",
            "model": "gpt-6-astra",
            "tier": "priority",
        }
        r.prompt = message
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

        class OfflineAgent(RealAgent):
            def __init__(self, **kwargs):
                constructed.append(kwargs)
                from tests.fast_consumer_receipts import record

                record(
                    "native-background-constructor",
                    {
                        k: v
                        for k, v in kwargs.items()
                        if k not in ("api_key", "credential_pool", "session_db")
                    },
                )
                super().__init__(**kwargs)
                response_sink(self, seen)

        r.stack.enter_context(patch.object(run_agent, "AIAgent", OfflineAgent))
        overlay.toggle()
        asyncio.run(r.attempt())
        assert seen and seen[-1].get("service_tier") == "priority"
        overlay.toggle()
        r.handle.release()
        r.handle = None
        asyncio.run(r.attempt())
        assert len(seen) == 2, r.adapter.send.await_args_list
        assert seen[-1].get("service_tier") == (
            "priority" if message == "example trigger-b." else None
        )
        assert len(r.decisions) == 1
        assert [kw["service_tier"] for kw in constructed] == ["priority", "priority"]
        assert all(kw["fallback_model"] == [] for kw in constructed)


def test_stale_generation_denies_tier_only_policy(overlay):
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn
    from agent.service_tier_policy import ServiceTierPolicyError

    a = agent()
    current = [True]
    overlay.toggle()
    with turn_route_scope(
        object(),
        session_key="s",
        turn_id="e",
        generation=1,
        message="hello",
        is_current=lambda: current[0],
    ):
        begin_turn(a, [])
        current[0] = False
        with pytest.raises(ServiceTierPolicyError):
            terminal(a, wire(a))


def test_concurrent_profiles_keep_independent_snapshots(overlay, tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override
    from tests.fixtures.builtin_policy import fast_state
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn

    other = tmp_path / "other"
    other.mkdir()
    shutil.copyfile(overlay.home / "config.yaml", other / "config.yaml")
    fast_state.initialize_fast_mode_state(state_path=other / "fast-mode.json")
    overlay.toggle()
    barrier = Barrier(2)

    def run(home):
        token = set_hermes_home_override(home)
        try:
            a = agent()
            with turn_route_scope(
                object(),
                session_key="same",
                turn_id="same",
                generation=1,
                message="hello",
                is_current=lambda: True,
            ):
                begin_turn(a, [])
                barrier.wait(timeout=10)
                return terminal(a, wire(a))[0].get("service_tier")
        finally:
            reset_hermes_home_override(token)

    with ThreadPoolExecutor(max_workers=2) as pool:
        a = pool.submit(run, overlay.home)
        b = pool.submit(run, other)
        assert (a.result(timeout=15), b.result(timeout=15)) == ("priority", None)


def test_nested_child_owns_tier_without_parent_exemption(overlay):
    from tools.delegate_tool import _build_child_agent, _run_single_child

    parent = real_agent(request_overrides={"service_tier": "priority"})
    child = _build_child_agent(
        task_index=0,
        goal="first",
        context=None,
        toolsets=[],
        model="gpt-6-astra",
        max_iterations=2,
        task_count=1,
        parent_agent=parent,
    )
    grandchild = _build_child_agent(
        task_index=0,
        goal="nested",
        context=None,
        toolsets=[],
        model="gpt-6-luna",
        max_iterations=2,
        task_count=1,
        parent_agent=child,
    )
    seen = []
    response_sink(grandchild, seen)
    try:
        result = _run_single_child(0, "nested", grandchild, child)
        assert result["status"] == "completed", result
        assert seen and all(request.get("service_tier") is None for request in seen)
        assert parent.request_overrides["service_tier"] == "priority"
    finally:
        child.close()
        parent.close()


@pytest.mark.parametrize(
    "message,expected",
    [
        ("example trigger-a. example trigger-b.", None),
        ("example trigger-b. example trigger-c.", "priority"),
        ("reexample trigger-b.", None),
        ("example trigger-b", None),
        ("example-action-a. example trigger-b.", "priority"),
    ],
)
def test_explicit_override_precedence(overlay, message, expected):
    from gateway.route_policy import turn_route_scope
    from agent.fast_mode import begin_turn

    a = agent()
    with turn_route_scope(
        object(),
        session_key="s",
        turn_id="e",
        generation=1,
        message=message,
        is_current=lambda: True,
    ):
        begin_turn(a, [])
        assert terminal(a, wire(a))[0].get("service_tier") == expected


def test_process_death_restart_duplicate_replay(overlay, tmp_path):
    import json, os, subprocess, sys

    driver = (
        Path(__file__).parents[1] / "fixtures" / "fast_consumers" / "replay_driver.py"
    )
    overlay.toggle()
    records = []
    for index, phase in enumerate(("capture", "replay", "replay")):
        if index == 1:
            overlay.toggle()
        record = tmp_path / f"replay-{index}.json"
        run = subprocess.run(
            [
                sys.executable,
                str(driver),
                str(tmp_path / "durable"),
                str(record),
                phase,
            ],
            capture_output=True,
            text=True,
            env=dict(os.environ, PYTHONPATH=str(Path.cwd())),
        )
        assert run.returncode == (73 if index == 0 else 0), (run.stdout, run.stderr)
        records.append(json.loads(record.read_text()))
    from tests.fast_consumer_receipts import record

    record("process-replay", records)
    assert len({r["pid"] for r in records}) == 3
    assert [r["built"][0]["service_tier"] for r in records] == ["priority"] * 3
    assert [r["wire"][0].get("service_tier") for r in records] == [
        "priority",
        None,
        None,
    ]
    assert [len(r["decisions"]) for r in records] == [1, 0, 0]


def test_auxiliary_fork_does_not_inherit_foreground_overlay(overlay):
    from agent.background_review import build_cache_parity_fork
    from agent.fast_mode import begin_turn
    from gateway.route_policy import turn_route_scope

    parent = real_agent()
    fork = None
    overlay.toggle()
    try:
        with turn_route_scope(
            object(),
            session_key="s",
            turn_id="e",
            generation=1,
            message="hello",
            is_current=lambda: True,
        ):
            fork, _, _ = build_cache_parity_fork(
                parent, max_iterations=1, write_origin="side_question"
            )
            begin_turn(fork, [])
            assert terminal(fork, wire(fork))[0].get("service_tier") is None
    finally:
        if fork is not None:
            fork.close()
        parent.close()


def test_unconfigured_profiles_keep_native_consumption(overlay, tmp_path):
    from agent.fast_mode import begin_turn
    from agent.service_tier_policy import capture_request_owner
    from hermes_constants import set_hermes_home_override, reset_hermes_home_override

    (overlay.home / "config.yaml").write_text("{}\n")
    a = agent()
    capture_request_owner(a, kind="delegation")
    token = set_hermes_home_override(tmp_path / "native-profile")
    try:
        begin_turn(a, [])
        assert wire(a).get("service_tier") is None
    finally:
        reset_hermes_home_override(token)


@pytest.mark.asyncio
async def test_background_cancellation_then_live_replay(overlay, monkeypatch, tmp_path):
    from tests.gateway.test_gateway_background_route_replay import BackgroundRuntime
    from run_agent import AIAgent as RealAgent
    import run_agent
    import hermes_cli.runtime_provider as providers

    seen = []
    with BackgroundRuntime(tmp_path / "sessions") as r:
        r.manager.discover_and_load()
        r.pick = {
            "provider": "openai-codex",
            "model": "gpt-6-astra",
            "tier": "priority",
        }
        r.prompt = "hello"
        r.park = True
        r.started = asyncio.Event()
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

        class OfflineAgent(RealAgent):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                response_sink(self, seen)

        r.stack.enter_context(patch.object(run_agent, "AIAgent", OfflineAgent))
        overlay.toggle()
        task = asyncio.create_task(r.attempt())
        await asyncio.wait_for(r.started.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not seen
        overlay.toggle()
        r.park = False
        r.handle.release()
        r.handle = None
        await r.attempt()
        assert len(seen) == 1 and seen[0].get("service_tier") is None
        assert len(r.decisions) == 1
