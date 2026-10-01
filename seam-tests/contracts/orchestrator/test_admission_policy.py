"""Host controls on real registered/native paths; only HTTP transport is synthetic."""

from contextlib import contextmanager
from threading import Event
import json, time, types
import pytest
from test_native_worker_adapter import (
    fixture,
    ROUTE,
    _client_factory,
    _real_parent,
    _close_parent,
    _sse_response,
)
import external_orchestrator as plugin


@contextmanager
def harness(tmp_path, monkeypatch, response=None):
    import hermes_cli.config as config
    import external_orchestrator.capacity as capacity
    monkeypatch.setattr(capacity, "host_worker_capacity", lambda: 1)
    import agent.subagent_lifecycle as lifecycle
    from tools.delegate_tool_registry import is_spawn_paused, set_spawn_paused

    cfg = {
        "delegation": {
            "scheduler": {"enabled": True},
            "worker": {
                **{k: ROUTE[k] for k in ["provider", "model", "base_url", "api_mode"]},
                "allow_fallbacks": False,
                "default_reasoning_effort": ROUTE["effort"],
                "background_service_tier": "normal",
            },
        }
    }
    monkeypatch.setattr(config, "load_config_readonly", lambda: cfg)
    monkeypatch.setenv("EXTERNAL_ORCHESTRATOR_DATA", str(tmp_path / "store"))
    monkeypatch.setattr(plugin, "_SCHEDULERS", {})
    handlers = {}
    ctx = types.SimpleNamespace(
        plugin_id="external_orchestrator",
        register_tool=lambda **kw: handlers.update({kw["name"]: kw["handler"]}),
        register_cli_command=lambda **kw: None,
        register_hook=lambda *a, **kw: None,
    )
    requests = []

    def transport(req):
        requests.append(req)
        if response:
            response(req, len(requests))
        return _sse_response(req, model=ROUTE["model"])

    factory, cleanup = _client_factory(transport, [])
    parent = _real_parent(factory)
    monkeypatch.setattr(lifecycle, "get_active_subagent_parent", lambda: parent)
    old = is_spawn_paused()
    set_spawn_paused(False)
    plugin.register(ctx)

    def control(kind, blocked):
        if kind == "disabled":
            cfg["delegation"]["scheduler"]["enabled"] = not blocked
        else:
            set_spawn_paused(blocked)

    def call(name, **args):
        return json.loads(handlers["orchestration_" + name](args))

    try:
        yield call, plugin._scheduler(ctx), control, requests, cfg, parent
    finally:
        set_spawn_paused(old)
        for s in plugin._SCHEDULERS.values():
            s.close()
        cleanup.close()
        _close_parent(parent)


def task(name):
    return {
        "task_id": name,
        "goal": "inspect only",
        "max_attempts": 1,
        "timeout_seconds": 30,
    }


def states(result):
    return {v["task_id"]: v for v in result["task_states"]}


def until(fn):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if fn():
            return
        time.sleep(0.01)
    assert fn()


@pytest.mark.parametrize("kind", ["disabled", "paused"])
@pytest.mark.parametrize("cancel", [False, True])
def test_queued_resume_and_running_worker_unchanged(
    fixture, tmp_path, monkeypatch, kind, cancel
):
    started = Event()
    release = Event()

    def response(req, n):
        if n == 1:
            started.set()
            assert release.wait(8)

    with harness(tmp_path, monkeypatch, response) as (
        call,
        s,
        control,
        requests,
        cfg,
        parent,
    ):
        try:
            created = call(
                "create", run_id="run", tasks=[task("first"), task("queued")]
            )
            assert created["ok"], created
            assert started.wait(5)
            control(kind, True)
            assert not call("enqueue", run_id="run", task_id="extra", goal="blocked")[
                "ok"
            ]
            assert not call("supersede", run_id="run", task_id="first", goal="blocked")[
                "ok"
            ]
            assert call("status", run_id="run")["ok"]
            assert call("collect", run_id="run")["ok"]
            release.set()
            until(
                lambda: (
                    states(call("status", run_id="run"))["first"]["state"]
                    == "SUCCEEDED"
                )
            )
            s.pump()
            q = states(call("status", run_id="run"))["queued"]
            assert q["state"] == "PENDING" and q["attempt"] == 0 and len(requests) == 1
            if cancel:
                assert call("cancel", run_id="run", task_id="queued")["ok"]
            control(kind, False)
            result = call("join", run_id="run", timeout_seconds=5)
            assert states(result)["queued"]["state"] == (
                "CANCELLED" if cancel else "SUCCEEDED"
            )
            assert len(requests) == (1 if cancel else 2)
        finally:
            release.set()


@pytest.mark.parametrize("kind", ["disabled", "paused"])
@pytest.mark.parametrize("stage", ["executor", "construction", "quota"])
def test_toggle_after_reservation_defers_without_consuming_attempt(
    fixture, tmp_path, monkeypatch, kind, stage
):
    import tools.delegate_tool_results as children
    import external_orchestrator.quota_gate as quota

    with harness(tmp_path, monkeypatch) as (call, s, control, requests, cfg, parent):
        futures = []
        gate = Event()
        once = [False]
        if stage == "executor":
            original = s._executor.submit

            def submit(fn, *args, **kw):
                def gated():
                    assert gate.wait(8)
                    return fn(*args, **kw)

                f = original(gated)
                futures.append(f)
                return f

            monkeypatch.setattr(s._executor, "submit", submit)
        elif stage == "construction":
            original = children._build_child_preserving_parent_tools

            def construct(*args, **kw):
                child = original(*args, **kw)
                if not once[0]:
                    once[0] = True
                    control(kind, True)
                return child

            monkeypatch.setattr(
                children, "_build_child_preserving_parent_tools", construct
            )
        else:
            original = quota.enforce_quota
            calls = [0]

            def check(*args, **kw):
                result = original(*args, **kw)
                calls[0] += 1
                if calls[0] == 2:
                    control(kind, True)
                return result

            monkeypatch.setattr(quota, "enforce_quota", check)
        try:
            assert call("create", run_id="run", tasks=[task("work")])["ok"]
            if stage == "executor":
                assert futures
                control(kind, True)
                gate.set()
                futures[0].result(timeout=5)
            until(
                lambda: (
                    states(call("status", run_id="run"))["work"]["state"] != "RUNNING"
                )
            )
            q = states(call("status", run_id="run"))["work"]
            assert q["state"] == "PENDING" and q["attempt"] == 0 and not requests
            control(kind, False)
            result = call("join", run_id="run", timeout_seconds=5)
            assert states(result)["work"]["state"] == "SUCCEEDED" and len(requests) == 1
        finally:
            gate.set()
