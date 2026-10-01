"""Exercise native registration, bounded hook dispatch and turn collection."""

from copy import deepcopy
from types import SimpleNamespace
import pytest
from test_native_worker_adapter import fixture

PROMPT = "Inspect each file independently and summarize findings for each:\n1. /tmp/a.py\n2. /tmp/b.py\n3. /tmp/c.py"


def setup_host(monkeypatch, **attrs):
    import external_orchestrator as plugin
    import hermes_cli.plugins as plugins
    from agent.subagent_lifecycle import bind_subagent_parent
    from agent.turn_context import _collect_pre_llm_call_context

    manager = plugins.PluginManager()
    manifest = plugins.PluginManifest(name="external-selector-test")
    ctx = plugins.PluginContext(manifest, manager)
    plugin.register(ctx)
    monkeypatch.setattr(plugins, "_delivery_manager", lambda: manager)
    parent = SimpleNamespace(
        session_id="selector-native",
        model="gpt-6-astra",
        provider="openai-codex",
        api_mode="codex_responses",
        platform="telegram",
        valid_tool_names={"orchestration_create", "orchestration_join"},
        _parent_session_id=None,
        _persist_disabled=False,
    )
    for key, value in attrs.items():
        setattr(parent, key, value)
    history = [
        {"role": "user", "content": "old question"},
        {"role": "assistant", "content": "old answer"},
    ]
    messages = [
        {"role": "system", "content": "byte-stable system"},
        *deepcopy(history),
        {"role": "user", "content": PROMPT},
    ]
    before = deepcopy((history, messages))

    def collect():
        with bind_subagent_parent(parent):
            value = _collect_pre_llm_call_context(
                parent,
                effective_task_id=parent.session_id,
                turn_id="one",
                original_user_message=PROMPT,
                messages=messages,
                conversation_history=history,
            )
        assert (history, messages) == before
        return value

    return collect, parent


def test_real_native_dispatch_carries_bound_parent_and_preserves_history(
    fixture, monkeypatch
):
    collect, parent = setup_host(monkeypatch)
    value = collect()
    assert "orchestration_create" in value and "xhigh" in value
    assert "low-effort" not in value and "delegate_task" not in value
    parent.valid_tool_names = set()
    assert not collect(), "must reread this parent's current exposed tools"


def test_child_context_cannot_trigger_selector_with_parent_like_attributes(
    fixture, monkeypatch
):
    from agent.delegation_context import delegated_child_context

    collect, _ = setup_host(monkeypatch)
    with delegated_child_context(session_id="child"):
        assert not collect()


def test_unsupported_parent_model_is_not_silently_admitted(fixture, monkeypatch):
    collect, _ = setup_host(monkeypatch, model="unapproved-model")
    assert not collect()


def test_explicit_scheduler_opt_out_is_honored(fixture, monkeypatch):
    import hermes_cli.config as config

    monkeypatch.setattr(
        config,
        "load_config_readonly",
        lambda: {"delegation": {"scheduler": {"enabled": False}}},
    )
    collect, _ = setup_host(monkeypatch)
    assert not collect()
