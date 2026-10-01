"""Real native dispatch regressions for an opt-in lifetime-bound policy."""

import asyncio
from contextlib import contextmanager, nullcontext
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pytest

PLUGIN = """from contextlib import nullcontext
policy = None
registration = None
def register(ctx):
    global policy, registration
    def check(name, args):
        if args.get("explode"):
            raise RuntimeError("PRIVATE-POLICY-SENTINEL")
        return not args.get("deny", False)
    if hasattr(ctx, "register_required_tool_policy"):
        policy = ctx.register_required_tool_policy(check)
    else:
        # Without patch 0002, fall back to native hooks, so a failure
        # demonstrates an effect after unload rather than missing-import noise.
        registration = ctx.register_hook("pre_tool_call", lambda tool_name, args, **kw: None if check(tool_name,args) else {"action":"block","message":"policy denied"})
        class Legacy:
            def scope(self): return nullcontext()
            def revoke(self): registration.dispose()
        policy = Legacy()
"""


@pytest.fixture
def fixture(tmp_path, monkeypatch):
    home = tmp_path / "home"
    plugin = home / "plugins" / "guard_fixture"
    plugin.mkdir(parents=True)
    (plugin / "plugin.yaml").write_text(
        "name: guard_fixture\nversion: 1.0.0\ndescription: isolated guard test\n"
    )
    (plugin / "__init__.py").write_text(PLUGIN)
    (home / "config.yaml").write_text("plugins:\n  enabled: [guard_fixture]\n")
    monkeypatch.setenv("HERMES_HOME", str(home))
    import hermes_cli.plugins as pm
    import model_tools
    from tools.registry import registry

    manager = pm.PluginManager()
    manager.discover_and_load()
    monkeypatch.setattr(pm, "get_plugin_manager", lambda: manager)
    marker = tmp_path / "effect"

    def effect(args, **kwargs):
        marker.write_text("effect")
        return json.dumps({"effect": True})

    registry.register(
        name="guard_fixture_effect",
        toolset="guard_fixture",
        schema={
            "name": "guard_fixture_effect",
            "parameters": {"type": "object", "properties": {}},
        },
        handler=effect,
    )
    entry = registry.get_entry("guard_fixture_effect")
    policy = manager._plugins["guard_fixture"].module.policy
    yield manager, policy, marker, registry, model_tools
    registry.restore_registration("guard_fixture_effect", entry, None)
    manager.unload("guard_fixture")


@pytest.mark.parametrize("direct", [False, True])
def test_live_policy_positive(fixture, direct):
    m, p, marker, r, mt = fixture
    with p.scope():
        result = (
            r.dispatch("guard_fixture_effect", {})
            if direct
            else mt.handle_function_call("guard_fixture_effect", {})
        )
    assert json.loads(result)["effect"] and marker.exists()


@pytest.mark.parametrize("direct", [False, True])
def test_unload_does_not_remove_required_scope(fixture, direct):
    m, p, marker, r, mt = fixture
    with p.scope():
        assert m.unload("guard_fixture")
        result = (
            r.dispatch("guard_fixture_effect", {})
            if direct
            else mt.handle_function_call("guard_fixture_effect", {})
        )
    assert not marker.exists(), (
        "required guard disappeared and an actual effect occurred"
    )
    assert "error" in json.loads(result)


@pytest.mark.parametrize("direct", [False, True])
def test_denied_policy_never_executes(fixture, direct):
    m, p, marker, r, mt = fixture
    with p.scope():
        result = (
            r.dispatch("guard_fixture_effect", {"deny": True})
            if direct
            else mt.handle_function_call("guard_fixture_effect", {"deny": True})
        )
    assert not marker.exists()
    assert "error" in json.loads(result)


@pytest.mark.parametrize("direct", [False, True])
def test_policy_errors_fail_closed_without_secret(fixture, direct):
    m, p, marker, r, mt = fixture
    with p.scope():
        result = (
            r.dispatch("guard_fixture_effect", {"explode": True})
            if direct
            else mt.handle_function_call("guard_fixture_effect", {"explode": True})
        )
    assert not marker.exists()
    assert "error" in json.loads(result)
    assert "PRIVATE-POLICY-SENTINEL" not in result


def test_new_registration_cannot_revive_old_scope(fixture):
    m, p, marker, r, mt = fixture
    with p.scope():
        m.unload("guard_fixture")
        m.discover_and_load(force=True)
        successor = m._plugins["guard_fixture"].module.policy
        with successor.scope():
            result = mt.handle_function_call("guard_fixture_effect", {})
        assert not marker.exists()
    with successor.scope():
        assert json.loads(mt.handle_function_call("guard_fixture_effect", {}))["effect"]


def test_native_thread_propagation_keeps_revocation(fixture):
    m, p, marker, r, mt = fixture
    from tools.thread_context import propagate_context_to_thread

    with p.scope():
        wrapped = propagate_context_to_thread(
            lambda: r.dispatch("guard_fixture_effect", {})
        )
        m.unload("guard_fixture")
        with ThreadPoolExecutor(max_workers=1) as pool:
            result = pool.submit(wrapped).result(timeout=5)
    assert not marker.exists()
    assert "error" in json.loads(result)


def test_execution_middleware_revocation_is_rechecked(fixture):
    m, p, marker, r, mt = fixture
    ctx = (
        m._plugins["guard_fixture"].context
        if hasattr(m._plugins["guard_fixture"], "context")
        else None
    )
    from hermes_cli.plugins import PluginContext

    ctx = PluginContext(m._plugins["guard_fixture"].manifest, m)

    def revoke_before_next(next_call, **kw):
        p.revoke()
        return next_call(kw["args"])

    ctx.register_middleware("tool_execution", revoke_before_next)
    with p.scope():
        result = mt.handle_function_call("guard_fixture_effect", {})
    assert not marker.exists()
    assert "error" in json.loads(result)


def test_unbound_native_dispatch_unchanged_after_unload(fixture):
    m, p, marker, r, mt = fixture
    m.unload("guard_fixture")
    assert json.loads(mt.handle_function_call("guard_fixture_effect", {}))["effect"]
