"""Current-turn-bound optional cleanup for the external orchestration plugin.

The native agent owns the ``on_session_end`` lifecycle signal.  This adapter only
uses that signal after proving it belongs to the same live parent turn that created
or enqueued the scheduler task.  Missing or conflicting identity is deliberately a
no-op: it is safer to leave optional work running than to cancel an older turn.
"""

from __future__ import annotations

import hashlib
from typing import Any, Mapping


_BINDING_FIELDS = ("owner_turn_id", "owner_task_id")


def _text(value: Any, *, maximum: int = 512) -> str | None:
    if not isinstance(value, str) or not value or len(value) > maximum:
        return None
    return value


def current_turn_binding(
    parent: Any,
    *,
    expected_session_id: str,
    runtime_task_id: Any = None,
) -> dict[str, str] | None:
    """Read the native parent's exact current-turn identity.

    ``_current_*`` are populated by the native turn prologue and are not model
    arguments.  The registry dispatch task id, when present, is an additional
    consistency check rather than an identity source.
    """

    session_id = _text(getattr(parent, "session_id", None))
    task_id = _text(getattr(parent, "_current_task_id", None))
    turn_id = _text(getattr(parent, "_current_turn_id", None))
    expected = _text(expected_session_id)
    if not session_id or not task_id or not turn_id or session_id != expected:
        return None
    dispatched = _text(runtime_task_id)
    if runtime_task_id is not None and (not dispatched or dispatched != task_id):
        return None
    return {
        "session_id": session_id,
        "owner_task_id": task_id,
        "owner_turn_id": turn_id,
    }


def bind_task_payload(
    payload: dict[str, Any],
    binding: Mapping[str, str] | None,
    *,
    method: str,
) -> None:
    """Attach trusted current-turn metadata to create/enqueue inputs.

    Reserved fields are removed even when identity is unavailable so callers
    cannot smuggle a model-supplied binding into the durable scheduler state.
    """

    # The top-level fields are also reserved: the scheduler records them on a
    # run for diagnostics, so never preserve a model-supplied value there.
    for field in _BINDING_FIELDS:
        payload.pop(field, None)

    if method == "create_run":
        if binding is not None:
            payload.update(
                owner_task_id=binding["owner_task_id"],
                owner_turn_id=binding["owner_turn_id"],
            )
        tasks = payload.get("tasks")
        if isinstance(tasks, list):
            rebound = []
            for raw in tasks:
                if not isinstance(raw, Mapping):
                    rebound.append(raw)
                    continue
                spec = dict(raw)
                for field in _BINDING_FIELDS:
                    spec.pop(field, None)
                if binding is not None:
                    spec.update(
                        owner_task_id=binding["owner_task_id"],
                        owner_turn_id=binding["owner_turn_id"],
                    )
                rebound.append(spec)
            payload["tasks"] = rebound
        return

    if method == "enqueue":
        if binding is not None:
            payload.update(
                owner_task_id=binding["owner_task_id"],
                owner_turn_id=binding["owner_turn_id"],
            )


def owner_token(profile: str, session_id: str) -> str:
    """Match the existing external handler's host-derived owner token."""

    return hashlib.sha256((profile + "\0" + session_id).encode()).hexdigest()


def auto_cancel_optional_enabled() -> bool:
    """Read the typed policy, failing closed on unavailable/malformed config."""

    try:
        from hermes_cli.config import load_config_readonly

        config = load_config_readonly() or {}
        delegation = config.get("delegation", {})
        policy = delegation.get("policy", {}) if isinstance(delegation, Mapping) else {}
        value = policy.get("auto_cancel_optional_on_finalize", True)
        return value is True
    except Exception:
        return False


def on_session_end_hook(scheduler: Any, **kwargs: Any) -> dict[str, Any] | None:
    """Cancel only optional tasks bound to this exact completed native turn."""

    if not auto_cancel_optional_enabled():
        return {"ok": True, "skipped": "policy_disabled", "cancelled_tasks": []}
    if (
        type(kwargs.get("completed")) is not bool
        or kwargs.get("completed") is not True
        or type(kwargs.get("failed")) is not bool
        or kwargs.get("failed") is not False
        or type(kwargs.get("interrupted")) is not bool
        or kwargs.get("interrupted") is not False
    ):
        return {"ok": True, "skipped": "turn_not_completed", "cancelled_tasks": []}

    try:
        from agent.subagent_lifecycle import get_active_subagent_parent
        from hermes_constants import get_hermes_home

        parent = get_active_subagent_parent()
        if parent is None:
            return {"ok": True, "skipped": "missing_parent", "cancelled_tasks": []}
        profile = str(get_hermes_home().resolve())
        session_id = _text(kwargs.get("session_id"))
        task_id = _text(kwargs.get("task_id"))
        turn_id = _text(kwargs.get("turn_id"))
        if not session_id or not task_id or not turn_id:
            return {
                "ok": True,
                "skipped": "missing_turn_identity",
                "cancelled_tasks": [],
            }
        binding = current_turn_binding(
            parent,
            expected_session_id=session_id,
            runtime_task_id=task_id,
        )
        if not binding or binding["owner_turn_id"] != turn_id:
            return {
                "ok": True,
                "skipped": "ambiguous_turn_identity",
                "cancelled_tasks": [],
            }
        args = {
            "owner_token": owner_token(profile, session_id),
            "parent_session_id": session_id,
            "profile": profile,
        }
        return scheduler.cancel_optional_on_finalize(
            args,
            owner_turn_id=turn_id,
            owner_task_id=task_id,
        )
    except Exception:
        # Lifecycle observers are fail-open for the native turn, but never turn
        # an uncertain identity into a cancellation.
        return {
            "ok": True,
            "skipped": "identity_or_scheduler_error",
            "cancelled_tasks": [],
        }


__all__ = [
    "auto_cancel_optional_enabled",
    "bind_task_payload",
    "current_turn_binding",
    "on_session_end_hook",
    "owner_token",
]
