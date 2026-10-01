"""Fail-closed, external-only account quota admission.

The gate deliberately imports the upstream host account-usage API at call time.
No caller/model supplied snapshot, credentials, provider flags or policy mode is
trusted.  Host bindings perform the profile/lifetime checks around this helper.
"""

from __future__ import annotations

from datetime import datetime, timezone
from contextvars import copy_context
from time import monotonic
import math
from numbers import Real
from threading import BoundedSemaphore, Event, Thread
from typing import Any, Mapping


PROVIDER = "openai-codex"
MAX_SNAPSHOT_AGE_SECONDS = 120.0
MAX_CLOCK_SKEW_SECONDS = 5.0
MAX_WINDOWS = 16
MAX_TEXT_CHARS = 512
_FETCH_SLOTS = BoundedSemaphore(2)
FETCH_TIMEOUT_SECONDS = 10.0


class QuotaAdmissionError(ValueError):
    """A quota check could not prove that admission is safe."""


def _policy_mode() -> str:
    """Read the host policy; malformed policy fails closed to enforce mode."""
    try:
        from hermes_cli.config import load_config_readonly

        raw = load_config_readonly() or {}
        delegation = raw.get("delegation")
        policy = delegation.get("policy") if isinstance(delegation, Mapping) else None
        mode = policy.get("quota_guard_mode") if isinstance(policy, Mapping) else None
    except Exception:
        return "enforce"
    return "observe" if mode == "observe" else "enforce"


def _failure(reason: str) -> QuotaAdmissionError:
    return QuotaAdmissionError(str(reason)[:MAX_TEXT_CHARS])


def quota_block_message(reason: str) -> str:
    return (
        "Delegation quota guard blocked admission: "
        f"{str(reason)[:MAX_TEXT_CHARS]}. "
        "Explicit human check-in is required before retrying."
    )


def _current_profile() -> str:
    try:
        from hermes_constants import get_hermes_home

        return str(get_hermes_home().resolve())
    except Exception as exc:
        raise _failure("quota profile unavailable") from exc


def _text(value: Any, field: str) -> str:
    if type(value) is not str or not value.strip() or len(value) > MAX_TEXT_CHARS:
        raise _failure(f"malformed quota snapshot {field}")
    return value.strip()


def _validate_snapshot(snapshot: Any, expected_profile: str) -> None:
    """Validate only the typed/native snapshot fields needed for admission."""
    from agent.account_usage import AccountUsageSnapshot, AccountUsageWindow
    if not isinstance(snapshot, AccountUsageSnapshot):
        raise _failure("quota status unavailable")

    provider = getattr(snapshot, "provider", None)
    if type(provider) is not str or provider.strip().lower() != PROVIDER:
        raise _failure("quota snapshot provider mismatch")
    _text(getattr(snapshot, "source", None), "source")

    fetched_at = getattr(snapshot, "fetched_at", None)
    if not isinstance(fetched_at, datetime) or fetched_at.tzinfo is None:
        raise _failure("malformed quota snapshot fetched_at")
    try:
        age = (datetime.now(timezone.utc) - fetched_at).total_seconds()
    except Exception as exc:
        raise _failure("malformed quota snapshot fetched_at") from exc
    if (
        not math.isfinite(age)
        or age < -MAX_CLOCK_SKEW_SECONDS
        or age > MAX_SNAPSHOT_AGE_SECONDS
    ):
        raise _failure("stale quota snapshot")

    unavailable = getattr(snapshot, "unavailable_reason", None)
    if unavailable is not None:
        _text(unavailable, "unavailable_reason")
        raise _failure("quota status unavailable")

    # Some controlled native fixtures attach the resolved profile.  The upstream
    # snapshot type does not, but rejecting a present mismatch prevents a
    # cross-profile fixture or future provider extension from being accepted.
    for field in ("profile", "profile_id", "hermes_home"):
        value = getattr(snapshot, field, None)
        if value is not None and str(value).strip() != expected_profile:
            raise _failure("quota snapshot profile mismatch")

    windows = getattr(snapshot, "windows", None)
    if (
        not isinstance(windows, (tuple, list))
        or not windows
        or len(windows) > MAX_WINDOWS
    ):
        raise _failure("malformed quota snapshot windows")
    for window in windows:
        if not isinstance(window, AccountUsageWindow):
            raise _failure("malformed quota snapshot window")
        label = getattr(window, "label", None)
        _text(label, "window label")
        used = getattr(window, "used_percent", None)
        if isinstance(used, bool) or not isinstance(used, Real):
            raise _failure("malformed quota snapshot used_percent")
        used = float(used)
        if not math.isfinite(used) or not 0.0 <= used <= 100.0:
            raise _failure("malformed quota snapshot used_percent")
        if used >= 100.0:
            raise _failure("quota exhausted")


def enforce_quota(expected_profile: str) -> None:
    """Require a fresh, usable Codex snapshot unless host policy is observe."""
    if _policy_mode() == "observe":
        return
    profile = _current_profile()
    if type(expected_profile) is not str or expected_profile != profile:
        raise _failure("quota profile mismatch")

    deadline = monotonic() + FETCH_TIMEOUT_SECONDS
    if not _FETCH_SLOTS.acquire(timeout=max(0.0, deadline-monotonic())):
        raise _failure("quota status unavailable")
    done = Event()
    result = [None]
    context = copy_context()

    def fetch():
        try:
            # This is intentionally the only supported call shape: the native
            # resolver selects credentials for the active Hermes profile.
            from agent.account_usage import fetch_account_usage

            result[0] = context.run(fetch_account_usage, PROVIDER)
        except Exception:
            result[0] = None
        finally:
            _FETCH_SLOTS.release()
            done.set()

    try:
        Thread(target=fetch, name="external-quota-read", daemon=True).start()
    except Exception as exc:
        _FETCH_SLOTS.release()
        raise _failure("quota status unavailable") from exc
    if not done.wait(max(0.0, deadline-monotonic())):
        raise _failure("quota status unavailable: read deadline exceeded")
    if _current_profile() != profile:
        raise _failure("quota profile changed during read")
    _validate_snapshot(result[0], profile)
