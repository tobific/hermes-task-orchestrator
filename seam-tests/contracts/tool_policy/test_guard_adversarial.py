"""Deterministic adversarial dispatch and callback-boundary probes."""

import asyncio, json
from threading import Event
import pytest
from test_guard_lifetime import fixture


def context(f):
    from hermes_cli.plugins import PluginContext

    m = f[0]
    return PluginContext(m._plugins["guard_fixture"].manifest, m)


def test_request_revocation_prevents_downstream_observers(fixture):
    m, p, marker, r, mt = fixture
    c = context(fixture)
    seen = []

    def revoke(**kw):
        p.revoke()
        return {"args": kw["args"]}

    c.register_middleware("tool_request", revoke)
    c.register_hook("pre_tool_call", lambda **kw: seen.append("pre"))
    c.register_hook("post_tool_call", lambda **kw: seen.append("post"))
    with p.scope():
        result = mt.handle_function_call("guard_fixture_effect", {"secret": "private"})
    assert not marker.exists()
    assert seen == []
    assert "error" in json.loads(result)


def test_rewritten_deny_prevents_pre_hook(fixture):
    m, p, marker, r, mt = fixture
    c = context(fixture)
    seen = []
    c.register_middleware("tool_request", lambda **kw: {"args": {"deny": True}})
    c.register_hook("pre_tool_call", lambda **kw: seen.append("pre"))
    with p.scope():
        result = mt.handle_function_call("guard_fixture_effect", {})
    assert not marker.exists()
    assert seen == []
    assert "error" in json.loads(result)


def test_execution_revocation_does_not_disclose_to_post_hooks(fixture):
    m, p, marker, r, mt = fixture
    c = context(fixture)
    seen = []

    def revoke(next_call, **kw):
        p.revoke()
        return next_call(kw["args"])

    c.register_middleware("tool_execution", revoke)
    c.register_hook("post_tool_call", lambda **kw: seen.append("post"))
    c.register_hook("transform_tool_result", lambda **kw: seen.append("transform"))
    with p.scope():
        result = mt.handle_function_call("guard_fixture_effect", {"secret": "private"})
    assert not marker.exists()
    assert seen == []
    assert "error" in json.loads(result)


def test_pre_hook_revocation_prevents_execution_middleware(fixture):
    m, p, marker, r, mt = fixture
    c = context(fixture)
    seen = []

    def revoke(**kw):
        p.revoke()

    def execution(next_call, **kw):
        seen.append("execution")
        return next_call(kw["args"])

    c.register_hook("pre_tool_call", revoke)
    c.register_middleware("tool_execution", execution)
    with p.scope():
        result = mt.handle_function_call("guard_fixture_effect", {})
    assert not marker.exists()
    assert seen == []
    assert "error" in json.loads(result)


@pytest.mark.parametrize("result", [None, False, 1, "allow", {}, []])
def test_malformed_decision_cannot_admit(fixture, result):
    m, p, marker, r, mt = fixture
    policy = context(fixture).register_required_tool_policy(lambda name, args: result)
    with policy.scope():
        response = r.dispatch("guard_fixture_effect", {})
    assert not marker.exists() and "error" in json.loads(response)


def test_validator_cannot_rewrite_arguments(fixture):
    m, p, marker, r, mt = fixture
    seen = []

    def mutate(name, args):
        args["nested"].append("changed")
        return True

    policy = context(fixture).register_required_tool_policy(mutate)
    args = {"nested": []}
    with policy.scope():
        result = r.dispatch("guard_fixture_effect", args)
    assert args == {"nested": []}
    assert marker.exists()


def test_later_validator_cannot_revoke_earlier_and_admit(fixture):
    m, p, marker, r, mt = fixture
    q = context(fixture).register_required_tool_policy(
        lambda name, args: (p.revoke(), True)[1]
    )
    with p.scope(), q.scope():
        result = r.dispatch("guard_fixture_effect", {})
    assert not marker.exists() and "error" in json.loads(result)


def test_exception_exit_restores_parent_context(fixture):
    m, p, marker, r, mt = fixture
    with pytest.raises(RuntimeError):
        with p.scope():
            p.revoke()
            raise RuntimeError("stop")
    assert json.loads(r.dispatch("guard_fixture_effect", {}))["effect"]


def test_callback_chain_stops_after_revocation(fixture):
    m, p, marker, r, mt = fixture
    c = context(fixture)
    seen = []
    c.register_hook("pre_tool_call", lambda **kw: p.revoke())
    c.register_hook("pre_tool_call", lambda **kw: seen.append("second callback"))
    with p.scope():
        result = mt.handle_function_call("guard_fixture_effect", {})
    assert not marker.exists() and seen == []


def test_registry_rechecks_after_entry_lookup(fixture, monkeypatch):
    m, p, marker, r, mt = fixture
    real = r.get_entry

    def lookup(*args, **kwargs):
        entry = real(*args, **kwargs)
        p.revoke()
        return entry

    monkeypatch.setattr(r, "get_entry", lookup)
    with p.scope():
        result = r.dispatch("guard_fixture_effect", {})
    assert not marker.exists() and "error" in json.loads(result)


def test_async_bridge_carries_scope_to_nested_dispatch(fixture):
    m, p, marker, r, mt = fixture

    async def async_handler(args, **kw):
        p.revoke()
        return r.dispatch("guard_fixture_effect", {})

    r.register(
        name="guard_async_fixture",
        toolset="guard_fixture",
        schema={"name": "guard_async_fixture", "parameters": {"type": "object"}},
        handler=async_handler,
        is_async=True,
    )
    entry = r.get_entry("guard_async_fixture")
    try:

        async def running_loop():
            with p.scope():
                return r.dispatch("guard_async_fixture", {})

        result = asyncio.run(running_loop())
        assert not marker.exists() and "error" in json.loads(result)
    finally:
        r.restore_registration("guard_async_fixture", entry, None)
