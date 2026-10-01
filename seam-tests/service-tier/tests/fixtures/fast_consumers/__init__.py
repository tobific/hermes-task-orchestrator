"""Synthetic example allowlist; the host only supplies a generic tier consumption boundary."""

import re
from . import fast_state as state

_HARD = tuple(
    (name, re.compile(r"(?<!\w)example\s+" + name + r"\.(?!\w)", re.I))
    for name in ("trigger-a", "trigger-b", "trigger-c")
)
ACTIONS = {
    "example-action-a.": state.FAST_MODE_SCOPE_EXAMPLE_JOB,
    "example-action-b.": state.FAST_MODE_SCOPE_APPROVAL,
}
EXAMPLE_JOB = "example-weekly-job"


def decide(request):
    origin = request.origin
    scope = None
    if origin.kind == "delegation":
        scope = {
            "gpt-6-luna": state.FAST_MODE_SCOPE_WORKER_ADAPTIVE,
            "gpt-6-astra": state.FAST_MODE_SCOPE_WORKER_DEFAULT,
        }.get(request.model)
    elif origin.kind == "cron":
        if origin.job_name == EXAMPLE_JOB:
            scope = state.FAST_MODE_SCOPE_EXAMPLE_JOB
    elif origin.kind == "gateway" and request.platform not in (
        "",
        "local",
        "cli",
        "desktop",
        "tui",
    ):
        scope = ACTIONS.get(origin.message.strip().lower())
        if scope is None:
            hard = next(
                (name for name, pattern in _HARD if pattern.search(origin.message)),
                None,
            )
            if hard == "trigger-c":
                return None
            if hard == "trigger-b":
                return (
                    "priority"
                    if state._resolve_fast_mode_service_tier_for_state(
                        enabled=True,
                        scope=state.FAST_MODE_SCOPE_PRO,
                        provider=request.provider,
                        model=request.model,
                        api_mode=request.api_mode,
                        base_url=request.base_url,
                    )
                    else None
                )
            scope = (
                state.FAST_MODE_SCOPE_PRO
                if hard == "trigger-a"
                else state.FAST_MODE_SCOPE_FOREGROUND_SOL
            )
    if scope is None:
        return None
    return state.resolve_fast_mode_service_tier(
        scope=scope,
        provider=request.provider,
        model=request.model,
        api_mode=request.api_mode,
        base_url=request.base_url,
        state_path=origin.profile_home / state.STATE_FILENAME,
    )


def register(ctx):
    ctx.register_service_tier_policy("overlay", decide)
