"""Opt-in profile-scoped effort forwarding through the existing native cron API."""

from copy import deepcopy

from pathlib import Path


def register(ctx):
    from hermes_constants import get_hermes_home
    from tools import cronjob_tools as native

    # The native schema's dynamic path hint becomes an owner-scoped constant here.
    # Never stamp one profile's hint into another profile's override registration.
    if get_hermes_home().resolve() != Path(ctx._manager.scope_key).resolve():
        raise ValueError("Cron effort registration profile mismatch")
    schema = deepcopy(native.CRONJOB_SCHEMA)
    schema.update(native._cronjob_schema_overrides())
    schema["parameters"]["properties"]["reasoning_effort"] = {
        "type": "string",
        "description": "Optional per-job reasoning effort. Native validation applies; empty string clears the override on update.",
    }
    allowed = frozenset(native._HANDLER_FORWARDED_ARGS)
    allowed -= {"model", "provider", "base_url", "task_id", "session_id"}
    upstream = native._cronjob_handler

    owner_scope = ctx._manager.scope_key

    def handler(args, **kwargs):
        import json
        from registration_lifecycle import replacement_coordinator
        from tools.registry import registry

        # Serialize effort writes with supported override disposal/replacement.
        # No inference or synchronous job execution occurs in create/update.
        with replacement_coordinator.transaction():
            current = registry.get_entry(schema["name"], scope=owner_scope)
            if (
                get_hermes_home().resolve() != Path(owner_scope).resolve()
                or current is None
                or current.handler is not handler
            ):
                return json.dumps(
                    {
                        "success": False,
                        "error": "Cron effort registration is inactive or belongs to another profile",
                    }
                )
            if (
                args.get("action") in {"create", "update"}
                and "reasoning_effort" in args
            ):
                public = {key: args.get(key) for key in allowed}
                monitor_script, monitor_url = native._split_monitor_arg(
                    args.get("monitor"), args.get("monitor_script"), args.get("monitor_url")
                )
                public.update(
                    action=args.get("action", ""),
                    include_disabled=args.get("include_disabled", True),
                    paused=args.get("paused", False),
                    monitor_script=monitor_script,
                    monitor_url=monitor_url,
                    reasoning_effort=args.get("reasoning_effort"),
                )
                public["task_id"] = kwargs.get("task_id")
                public["session_id"] = kwargs.get("session_id")
                return native.cronjob(**public)
        return upstream(args, **kwargs)

    handle = ctx.register_tool(
        name=schema["name"],
        toolset="cronjob",
        schema=schema,
        handler=handler,
        check_fn=native.check_cronjob_requirements,
        emoji="⏰",
        override=True,
    )
    if handle is None:
        raise RuntimeError("Cron effort tool override was not installed")
    return handle
