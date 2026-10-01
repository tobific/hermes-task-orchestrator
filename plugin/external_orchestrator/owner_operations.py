"""Explicit owner-scoped durable discovery and atomic bulk cancellation."""

import time
from .scheduler import OrchestrationError, TERMINAL


def owner_operation(scheduler, args, *, cancel=False, joinable_only=False):
    if args.get("owner_only") is not True or args.get("run_id") or args.get("task_id"):
        raise OrchestrationError("owner_only requires no run_id or task_id")
    identity = {
        k: scheduler._required_str(args.get(k), k)
        for k in ("owner_token", "parent_session_id", "profile")
    }
    cursor = args.get("cursor", 0)
    limit = args.get("limit", 50)
    if (
        type(cursor) is not int
        or cursor < 0
        or type(limit) is not int
        or not 1 <= limit <= 50
    ):
        raise OrchestrationError("invalid owner discovery cursor or limit")

    def operation(state):
        ids = sorted(
            r["run_id"]
            for r in state["runs"].values()
            if all(r.get(k) == v for k, v in identity.items())
            and (
                not joinable_only
                or ("delivery_context" in r and r["delivery_context"] is None)
            )
        )
        if not cancel:
            page = ids[cursor : cursor + limit]
            return {
                "ok": True,
                "owner_only": True,
                "runs": [scheduler._run_view(state, k) for k in page],
                "next_cursor": cursor + len(page),
                "has_more": cursor + len(page) < len(ids),
                "bounded": True,
            }
        changed = []
        count = 0
        for run_id in ids:
            targets = [
                t
                for t in state["tasks"].values()
                if t["run_id"] == run_id
                and (
                    t["state"] not in TERMINAL
                    or (
                        joinable_only
                        and t["state"] == "BLOCKED"
                        and t.get("result") is None
                    )
                )
            ]
            if not targets and (
                joinable_only or state["runs"][run_id].get("cancel_requested")
            ):
                continue
            # Preserve cancellation intent even when all work already settled.
            # Cancellation cannot undo completed effects, but must fence mutation.
            state["runs"][run_id]["cancel_requested"] = True
            for task in targets:
                task.update(
                    state="CANCELLED",
                    updated_at=time.time(),
                    host_status="cancel_requested",
                )
                scheduler._append_delivery(state, task)
                count += 1
            scheduler._refresh_run(state, run_id)
            changed.append(run_id)
        return {
            "ok": True,
            "owner_only": True,
            "cancelled_runs": changed,
            "cancelled_task_count": count,
            "bounded": True,
        }

    result = (scheduler._mutate if cancel else scheduler._read)(operation)
    if cancel:
        scheduler._dispatch_revocations()
    return result
