from __future__ import annotations

from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from external_orchestrator.automatic_selector import (  # noqa: E402
    DIRECT_LUNA_ROUTE,
    HOST_REASONING_EFFORT,
    assess_external_selector,
    automatic_selector_hook,
    parse_explicit_groups,
)


PROMPT = """
Inspect each file independently and summarize findings for each:
1. /tmp/alpha.py
2. /tmp/beta.py
3. /tmp/gamma.py
Do not modify files.
"""


def parent(*tools: str, **attrs: object) -> SimpleNamespace:
    values = {
        "session_id": "selector-test-parent",
        "model": "gpt-6-astra",
        "provider": "openai-codex",
        "api_mode": "codex_responses",
        "valid_tool_names": set(tools),
        "_parent_session_id": None,
        "_delegate_depth": 0,
    }
    values.update(attrs)
    return SimpleNamespace(**values)


def test_selects_independent_read_only_items_with_actual_external_tools() -> None:
    decision = assess_external_selector(
        PROMPT,
        valid_tool_names={"orchestration_create", "orchestration_join"},
    )

    assert decision.eligible is True
    assert decision.item_count == 3
    assert decision.join_available is True
    assert "orchestration_create" in decision.directive
    assert "orchestration_join" in decision.directive
    assert 'mode="joinable"' in decision.directive
    assert DIRECT_LUNA_ROUTE in decision.directive
    assert HOST_REASONING_EFFORT in decision.directive
    assert "delegate_task" not in decision.directive
    assert "low-effort" not in decision.directive
    assert "gpt-6-astra" not in decision.directive


def test_create_only_host_uses_detached_external_flow() -> None:
    decision = assess_external_selector(
        PROMPT,
        valid_tool_names={"orchestration_create"},
    )

    assert decision.eligible is True
    assert decision.join_available is False
    assert 'mode="detached"' in decision.directive
    assert "orchestration_create" in decision.directive
    assert "orchestration_join is not exposed" in decision.directive
    assert "orchestration_join with" not in decision.directive


def test_host_tool_snapshot_is_authoritative_not_global_registry() -> None:
    missing_create = assess_external_selector(
        PROMPT,
        valid_tool_names={"orchestration_join", "read_file"},
    )
    create_only = assess_external_selector(
        PROMPT,
        valid_tool_names={"orchestration_create"},
    )

    assert missing_create.eligible is False
    assert missing_create.reason == "orchestration_create_unavailable"
    assert create_only.eligible is True


def test_explicit_groups_are_preserved_in_directive() -> None:
    prompt = """
Group 1: inspect each source independently and report risks
- /tmp/alpha.py
- /tmp/beta.py
Group 2: inspect each source independently and report risks
- /tmp/gamma.py
- /tmp/delta.py
Group 3: inspect each source independently and report risks
- /tmp/epsilon.py
- /tmp/zeta.py
Do not modify files.
"""
    groups = parse_explicit_groups(prompt)
    decision = assess_external_selector(
        prompt,
        valid_tool_names={"orchestration_create", "orchestration_join"},
    )

    assert groups is not None and len(groups) == 3
    assert decision.eligible is True
    assert decision.item_count == 3
    assert "Group 1" in decision.directive
    assert "/tmp/alpha.py, /tmp/beta.py" in decision.directive
    assert "keep each group's sources together" in decision.directive


@pytest.mark.parametrize(
    ("prompt", "reason"),
    [
        ("Inspect /tmp/a.py, /tmp/b.py, and /tmp/c.py.", "independence_not_explicit"),
        ("Inspect /tmp/a.py and /tmp/b.py independently.", "fewer_than_three_items"),
        (
            "Inspect /tmp/a.py, /tmp/b.py, and /tmp/c.py yourself. Do not delegate.",
            "user_opt_out",
        ),
        (
            "Inspect /tmp/a.py, /tmp/b.py, and /tmp/c.py independently and edit them.",
            "mutation_or_side_effect_requested",
        ),
    ],
)
def test_negative_selection_gates(prompt: str, reason: str) -> None:
    decision = assess_external_selector(
        prompt,
        valid_tool_names={"orchestration_create", "orchestration_join"},
    )

    assert decision.eligible is False
    assert decision.reason == reason
    assert decision.directive == ""


def test_malformed_grouping_fails_closed_instead_of_path_fanout() -> None:
    prompt = """
Group 1: inspect each source independently
- /tmp/a.py
Group 1: inspect each source independently
- /tmp/b.py
- /tmp/c.py
"""

    assert parse_explicit_groups(prompt) == ()
    decision = assess_external_selector(
        prompt,
        valid_tool_names={"orchestration_create", "orchestration_join"},
    )
    assert decision.eligible is False
    assert decision.reason == "ambiguous_grouping"


def test_child_and_recursive_parents_are_rejected() -> None:
    child = assess_external_selector(
        PROMPT,
        valid_tool_names={"orchestration_create", "orchestration_join"},
        parent_session_id="real-parent-session",
    )
    recursive = assess_external_selector(
        PROMPT,
        valid_tool_names={"orchestration_create", "orchestration_join"},
        parent=parent("orchestration_create", "orchestration_join", _delegate_depth=1),
    )

    assert child.eligible is False and child.reason == "delegated_child"
    assert recursive.eligible is False and recursive.reason == "delegated_child"


def test_hook_uses_bound_native_parent_and_returns_context_only(monkeypatch) -> None:
    active = parent("orchestration_create", "orchestration_join")
    monkeypatch.setattr(
        "agent.subagent_lifecycle.get_active_subagent_parent",
        lambda: active,
    )

    result = automatic_selector_hook(
        user_message=PROMPT,
        model="host-model-input-is-not-route-authority",
        session_id=active.session_id,
        parent_session_id="",
    )

    assert isinstance(result, dict)
    assert set(result) == {"context"}
    assert "orchestration_create" in result["context"]
    assert "orchestration_join" in result["context"]


def test_hook_fails_closed_without_bound_parent(monkeypatch) -> None:
    monkeypatch.setattr(
        "agent.subagent_lifecycle.get_active_subagent_parent",
        lambda: None,
    )
    assert automatic_selector_hook(user_message=PROMPT) is None


def test_plugin_registers_selector_on_pre_llm_call() -> None:
    import external_orchestrator as plugin

    hooks: list[tuple[str, object]] = []

    class Context:
        plugin_id = "selector-test"

        def register_tool(self, **kwargs):
            pass

        def register_cli_command(self, **kwargs):
            pass

        def register_hook(self, event, callback, **kwargs):
            hooks.append((event, callback))

    plugin.register(Context())

    assert any(
        event == "pre_llm_call" and callback is automatic_selector_hook
        for event, callback in hooks
    )
