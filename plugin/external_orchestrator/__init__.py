"""Standalone external orchestration plugin for the upstream PluginContext API."""

from __future__ import annotations

import argparse
import json
import hashlib
import logging
import os
from pathlib import Path
from threading import RLock
from typing import Any, Dict

from .scheduler import (
    ExternalScheduler,
    OrchestrationError,
    scheduler_config_from_env,
    LUNA_MODEL,
)
from .host_binding import HostDispatch
from .admission_policy import enforce_host_admission
from .automatic_selector import automatic_selector_hook
from .optional_finalization import (
    bind_task_payload,
    current_turn_binding,
    on_session_end_hook,
)
from .transcript import TranscriptAccessError

logger = logging.getLogger(__name__)

_TOOL_DESCRIPTIONS = {
    "orchestration_create": "Create an owner-bound orchestration run with immutable task packets.",
    "orchestration_enqueue": "Enqueue one owner-bound packet into an existing run.",
    "orchestration_supersede": "Supersede the current task generation and enqueue a replacement.",
    "orchestration_join": "Wait for all, required, first-result or named tasks; partial readiness never authorizes final delivery.",
    "orchestration_history": "Read bounded owner-authorized diagnostic result history; noncurrent records never authorize delivery or review acceptance.",
    "orchestration_collect": "Collect bounded, cursor-based parent-delivery events.",
    "orchestration_cancel": "Cancel one task or all active tasks in a run.",
    "orchestration_status": "Read owner-bound run/task state and host status.",
}


def _schema(name: str, description: str) -> Dict[str, Any]:
    if name == "orchestration_history":
        return {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "maxLength": 256},
                    "cursor": {"type": "integer", "minimum": 0, "maximum": 2**63 - 1},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 50},
                },
                "required": ["run_id"],
                "additionalProperties": False,
            },
        }
    properties: Dict[str, Any] = {
        "run_id": {"type": "string"},
        "owner_only": {
            "type": "boolean",
            "description": "Status/cancel all runs for the authenticated owner; omit run_id and task_id.",
        },
        "mode": {
            "type": "string",
            "enum": ["joinable", "detached"],
            "description": "Create mode; detached uses native completion delivery.",
        },
        "task_id": {"type": "string"},
        "final_review_task_id": {"type": "string"},
        "capability_profile": {
            "type": "string",
            "description": "Run capability boundary; automatic inspection uses read-only.",
        },
        "goal": {"type": "string"},
        "context": {"type": "string", "maxLength": 16000},
        "acceptance": {"type": "string", "maxLength": 16000},
        "tasks": {
            "type": "array",
            "maxItems": 64,
            "items": {
                "type": "object",
                "properties": {
                    "goal": {"type": "string", "maxLength": 16000},
                    "context": {"type": "string", "maxLength": 16000},
                    "acceptance": {"type": "string", "maxLength": 16000},
                    "priority": {
                        "type": "string",
                        "enum": ["critical", "support", "background"],
                        "default": "support",
                    },
                    "deadline_seconds": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 86400,
                    },
                },
                "additionalProperties": True,
            },
        },
        "dependencies": {"type": "array", "maxItems": 64},
        "worker_mode": {
            "type": "string",
            "enum": [
                "success",
                "fail",
                "retryable",
                "malformed",
                "route_mismatch",
                "slow",
                "timeout",
            ],
        },
        "cursor": {"type": "integer", "minimum": 0},
        "limit": {"type": "integer", "minimum": 1, "maximum": 50},
        "timeout_seconds": {"type": "number", "minimum": 0, "maximum": 600},
        "deadline_seconds": {"type": "integer", "minimum": 1, "maximum": 86400},
    }
    if name == "orchestration_join":
        properties["condition"] = {
            "type": "string",
            "enum": ["all", "required", "first", "task_ids"],
            "default": "all",
            "description": "Required returns partial readiness without cancelling optional work or approving final delivery.",
        }
        properties["task_ids"] = {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 1,
            "maxItems": 64,
        }
    if name == "orchestration_status":
        properties["inspect_transcript"] = {
            "type": "boolean",
            "description": "Include a bounded owner-authorized transcript cursor in each task projection.",
        }
        properties["transcript_max_chars"] = {
            "type": "integer",
            "minimum": 1,
            "maximum": 65536,
        }
        properties["transcript_offset"] = {
            "type": "integer",
            "minimum": 0,
            "maximum": 1000000000,
        }
    return {
        "name": name,
        "description": description,
        "parameters": {"type": "object", "properties": properties},
    }


_SCHEDULERS: Dict[str, ExternalScheduler] = {}
_SCHEDULERS_LOCK = RLock()


def _worker_route(profile):
    """Use the existing host worker policy; model input is never route authority."""
    from hermes_cli.config import load_config_readonly
    from hermes_constants import get_hermes_home

    if profile != str(get_hermes_home().resolve()):
        raise OrchestrationError("worker route profile mismatch")
    cfg = (load_config_readonly() or {}).get("delegation", {}).get("worker", {})
    if not isinstance(cfg, dict) or cfg.get("allow_fallbacks") is not False:
        raise OrchestrationError("strict host worker policy required")
    tier = cfg.get("background_service_tier")
    tier = "default" if tier == "normal" else tier
    if tier not in {"default", "priority"}:
        raise OrchestrationError("host worker tier unavailable")
    route = {key: cfg.get(key) for key in ("provider", "model", "base_url", "api_mode")}
    route.update(
        effort=cfg.get("default_reasoning_effort"), service_tier=tier, fallback=False
    )
    if route.get("model") != LUNA_MODEL:
        raise OrchestrationError("approved Luna worker model required")
    return ExternalScheduler._route(route)


def _scheduler(ctx: Any) -> ExternalScheduler:
    from hermes_constants import get_hermes_home, get_default_hermes_root
    from .capacity import (
        SharedCapacity,
        host_scheduler_limits,
        host_worker_capacity,
        host_worker_limits,
    )

    home = get_hermes_home().resolve()
    key = "|".join((
        str(
            getattr(ctx, "plugin_id", None)
            or getattr(ctx, "plugin_name", None)
            or "default"
        ),
        os.environ.get("EXTERNAL_ORCHESTRATOR_DATA", ""),
        str(home),
    ))
    with _SCHEDULERS_LOCK:
        scheduler = _SCHEDULERS.get(key)
        if scheduler is None or scheduler._closing:
            workers = host_worker_capacity()
            limits = host_scheduler_limits()
            scheduler = ExternalScheduler(
                data_dir=Path(
                    os.environ.get("EXTERNAL_ORCHESTRATOR_DATA")
                    or home / "plugin-data" / "external-orchestrator"
                ),
                max_global=workers,
                per_profile=workers,
                shared_capacity=SharedCapacity(
                    get_default_hermes_root()
                    / "plugin-data"
                    / "external-orchestrator-capacity",
                    workers,
                ),
                worker=HostDispatch(
                    _worker_route,
                    admission_policy=enforce_host_admission,
                    worker_policy=host_worker_limits,
                ),
                worker_policy=host_worker_limits,
                **limits,
            )
            scheduler.route_resolver = _worker_route
            _SCHEDULERS[key] = scheduler
        # The default provider is intentionally one live scheduler object.  It
        # is used only for marked rows; legacy missing-row claims never consult
        # it, so plugin availability cannot change old callers.
        from .async_delivery import ExternalCompletionFence
        from tools import async_delegation as native

        native.set_completion_guard(ExternalCompletionFence(scheduler))
        return scheduler


def _handler(ctx: Any, method: str):
    def handle(args: dict | None = None, **runtime: Any) -> str:
        try:
            payload = dict(args or {})
            from agent.subagent_lifecycle import get_active_subagent_parent
            from hermes_constants import get_hermes_home

            parent = get_active_subagent_parent()
            if parent is None or not getattr(parent, "session_id", None):
                raise OrchestrationError("trusted host caller context required")
            if method in {"create_run", "enqueue", "supersede"} and getattr(parent, "provider", None) != "openai-codex":
                raise OrchestrationError("new Luna work requires an openai-codex parent")
            profile = str(get_hermes_home().resolve())
            session = str(parent.session_id)
            payload.update(
                owner_token=hashlib.sha256(
                    (profile + "\0" + session).encode()
                ).hexdigest(),
                parent_session_id=session,
                origin=str(getattr(parent, "platform", "local")) + ":" + session,
                profile=profile,
            )
            # Bind create/enqueue packets to the native turn identity, when
            # available.  Missing identity is a safe no-op; spoofed reserved
            # fields are stripped by bind_task_payload regardless.
            if method in {"create_run", "enqueue"}:
                mode = payload.get("mode", "joinable")
                binding = None
                if not (method == "create_run" and mode == "detached"):
                    binding = current_turn_binding(
                        parent,
                        expected_session_id=session,
                        runtime_task_id=runtime.get("task_id"),
                    )
                bind_task_payload(payload, binding, method=method)
            if method == "history":
                from .history import read_history
                from .storage import configured_storage_directory

                data_dir = configured_storage_directory(
                    Path(
                        os.environ.get("EXTERNAL_ORCHESTRATOR_DATA")
                        or get_hermes_home().resolve()
                        / "plugin-data"
                        / "external-orchestrator"
                    )
                )
                return json.dumps(read_history(data_dir, payload), sort_keys=True)
            scheduler = _scheduler(ctx)
            # Only a Codex parent may own Luna worker construction. A same-session
            # parent of another model family (e.g. a Claude turn) may still inspect or
            # cancel its runs, but must not replace the Codex worker binding.
            if getattr(parent, "provider", None) == "openai-codex":
                scheduler._worker.bind(parent)
            if method in {"create_run", "enqueue", "supersede"}:
                scheduler._worker.admission_preflight(payload)
            if method == "create_run":
                mode = payload.pop("mode", "joinable")
                if mode not in {"joinable", "detached"}:
                    raise OrchestrationError("unsupported orchestration create mode")
                if mode == "detached":
                    from .async_delivery import dispatch_detached

                    return json.dumps(
                        dispatch_detached(scheduler, payload, parent), sort_keys=True
                    )
            if method == "join":
                # A retry of delivery registration must not repeat a mutation.
                from .async_delivery import safe_rearm_detached

                delivery = safe_rearm_detached(scheduler, payload, parent)
                result = scheduler.join(payload, parent=parent)
                result["detached_completion"] = delivery
            else:
                result = getattr(scheduler, method)(payload)
            if method in {"supersede", "enqueue"}:
                from .async_delivery import safe_rearm_detached

                result["detached_completion"] = safe_rearm_detached(
                    scheduler, payload, parent
                )
            return json.dumps(result, sort_keys=True)
        except (
            OrchestrationError,
            TranscriptAccessError,
            KeyError,
            TypeError,
            ValueError,
        ) as exc:
            return json.dumps(
                {"ok": False, "error": type(exc).__name__, "message": str(exc)[:2000]},
                sort_keys=True,
            )

    return handle


def _cli_setup(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("action", choices=("status", "collect", "cancel"))
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--owner-token", required=True)
    parser.add_argument("--parent-session-id", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--task-id")
    parser.add_argument("--cursor", type=int, default=0)
    parser.add_argument("--inspect-transcript", action="store_true")
    parser.add_argument("--transcript-max-chars", type=int, default=4000)
    parser.add_argument("--transcript-offset", type=int, default=0)


def _cli_handler(args: argparse.Namespace) -> int:
    scheduler = ExternalScheduler(**scheduler_config_from_env())
    payload = vars(args).copy()
    action = payload.pop("action")
    method = {"status": "status", "collect": "collect", "cancel": "cancel"}[action]
    try:
        print(json.dumps(getattr(scheduler, method)(payload), sort_keys=True, indent=2))
        return 0
    except OrchestrationError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}))
        return 2
    finally:
        scheduler.close()


def register(ctx) -> None:
    """Register external orchestration tools and a small operator CLI surface."""
    get_config = getattr(ctx, "get_config", None)
    if callable(get_config) and get_config("gateway_extensions", False) is True:
        from .gateway_extensions import register as register_gateway_extensions

        register_gateway_extensions(ctx)
    if callable(get_config) and get_config("cron_reasoning_effort_tool", False) is True:
        from .cron_effort import register as register_cron_effort

        register_cron_effort(ctx)
    # Install the live validator before startup replay/drains.  A malformed or
    # unavailable external store must not prevent plugin registration; guarded
    # events then remain pending/fail closed until the host can re-register it.
    try:
        _scheduler(ctx)
    except Exception:
        logger.warning(
            "External completion fence provider unavailable during plugin registration",
            exc_info=True,
        )
    methods = {
        "orchestration_create": "create_run",
        "orchestration_enqueue": "enqueue",
        "orchestration_supersede": "supersede",
        "orchestration_join": "join",
        "orchestration_history": "history",
        "orchestration_collect": "collect",
        "orchestration_cancel": "cancel",
        "orchestration_status": "status",
    }
    for name, method in methods.items():
        ctx.register_tool(
            name=name,
            toolset="external_orchestration",
            schema=_schema(name, _TOOL_DESCRIPTIONS[name]),
            handler=_handler(ctx, method),
            description=_TOOL_DESCRIPTIONS[name],
            emoji="🧭",
        )
    ctx.register_cli_command(
        name="orchestration",
        help="Inspect or cancel external plugin-owned orchestration runs",
        setup_fn=_cli_setup,
        handler_fn=_cli_handler,
        description="Offline-safe status/collect/cancel CLI for external orchestration runs.",
    )
    # Selection is observe-only: the hook returns current-turn context and never
    # invokes an orchestration tool itself.
    ctx.register_hook("pre_llm_call", automatic_selector_hook)
    from .boundary_finalization import on_session_finalize

    ctx.register_hook(
        "on_session_finalize",
        lambda **kwargs: on_session_finalize(lambda: _scheduler(ctx), **kwargs),
    )
    # Turn-end optional cleanup remains separate from session-boundary fencing.
    # Both callbacks reject missing or ambiguous host identity.
    ctx.register_hook(
        "on_session_end",
        lambda **kwargs: on_session_end_hook(_scheduler(ctx), **kwargs),
    )
