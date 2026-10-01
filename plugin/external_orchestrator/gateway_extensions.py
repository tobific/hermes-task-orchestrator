"""Opt-in owner-scoped gateway projections and exact manual recovery policy."""

from pathlib import Path
import hashlib


def register(ctx):
    from hermes_constants import get_hermes_home

    scope = Path(ctx._manager.scope_key).resolve()
    if get_hermes_home().resolve() != scope:
        raise ValueError("gateway extension registration profile mismatch")

    def active(name, callback, profile):
        return (
            str(scope) == profile
            and get_hermes_home().resolve() == scope
            and callback in ctx._manager._hooks.get(name, [])
            and ctx.get_config("gateway_extensions", False) is True
        )

    def busy(*, text, message_type, has_media, session_key, profile, **kwargs):
        if not active("gateway_busy_control", busy, profile):
            return None
        if (
            text == "continue."
            and message_type == "text"
            and not has_media
            and session_key
        ):
            return {"action": "resume", "reason": "manual_continue"}
        return None

    def agents(*, session_id, session_key, profile, **kwargs):
        if (
            not active("gateway_agents", agents, profile)
            or not session_id
            or not session_key
        ):
            return None
        from . import _scheduler

        scheduler = _scheduler(ctx)
        identity = {
            "profile": profile,
            "parent_session_id": session_id,
            "owner_token": hashlib.sha256(
                (profile + "\0" + session_id).encode()
            ).hexdigest(),
        }

        def project(state):
            runs = [
                r
                for r in state["runs"].values()
                if all(r.get(k) == v for k, v in identity.items())
            ]
            if not runs:
                return None
            lines = [
                "**Owned scheduler runs**",
                f"Configured capacity: {scheduler.max_global} global / {scheduler.per_profile} per profile",
            ]
            for run in sorted(runs, key=lambda r: r["run_id"])[:10]:
                lines.append(
                    f"{run['run_id']} | {run['state']} | generation {run['generation']}"
                )
                tasks = [
                    t for t in state["tasks"].values() if t["run_id"] == run["run_id"]
                ]
                for task in sorted(tasks, key=lambda t: t["task_id"])[:10]:
                    proof = (task.get("result") or {}).get("route_proof") or {}
                    observed = (
                        proof.get("observation_source") == "sdk-transport"
                        and proof.get("observed_at_transport") is True
                    )
                    route = (
                        f"applied effort={proof.get('effort', 'unknown')} tier={proof.get('service_tier', 'unknown')}"
                        if observed
                        else "applied route unobserved"
                    )
                    lines.append(
                        f"  {task['task_id']} | {task['state']} | generation {task['generation']} | {route}"
                    )
                if len(tasks) > 10:
                    lines.append(f"{len(tasks) - 10} further tasks omitted")
            if len(runs) > 10:
                lines.append(f"{len(runs) - 10} further owned runs omitted")
            return {"lines": lines}

        return scheduler._read(project)

    return [
        ctx.register_hook("gateway_busy_control", busy),
        ctx.register_hook("gateway_agents", agents),
    ]
