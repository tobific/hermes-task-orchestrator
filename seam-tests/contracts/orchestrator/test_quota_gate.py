"""Fail-closed quota validation and bounded-I/O contracts."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from threading import Event, Thread
import pytest
from agent.account_usage import AccountUsageSnapshot, AccountUsageWindow
from hermes_constants import get_hermes_home
from external_orchestrator import quota_gate as gate


def good():
    return AccountUsageSnapshot(
        provider="openai-codex",
        source="fixture",
        fetched_at=datetime.now(timezone.utc),
        windows=(AccountUsageWindow(label="session", used_percent=20.0),),
    )


@pytest.mark.parametrize(
    "kind",
    [
        "none",
        "mapping",
        "duck",
        "provider",
        "source",
        "naive",
        "stale",
        "future",
        "unavailable",
        "empty",
        "bool",
        "nan",
        "negative",
        "over",
        "exhausted",
        "window-mapping",
    ],
)
def test_invalid_quota_fails_closed(monkeypatch, kind):
    import agent.account_usage as api

    snap = good()
    if kind == "none":
        snap = None
    elif kind == "mapping":
        snap = snap.__dict__
    elif kind == "duck":
        snap = SimpleNamespace(**snap.__dict__)
    elif kind == "provider":
        snap = replace(snap, provider="openrouter")
    elif kind == "source":
        snap = replace(snap, source="")
    elif kind == "naive":
        snap = replace(snap, fetched_at=datetime.now())
    elif kind == "stale":
        snap = replace(snap, fetched_at=datetime.now(timezone.utc) - timedelta(hours=1))
    elif kind == "future":
        snap = replace(snap, fetched_at=datetime.now(timezone.utc) + timedelta(hours=1))
    elif kind == "unavailable":
        snap = replace(snap, unavailable_reason="unavailable")
    elif kind == "empty":
        snap = replace(snap, windows=())
    elif kind == "window-mapping":
        snap = replace(snap, windows=({"label": "session", "used_percent": 0},))
    else:
        value = {
            "bool": False,
            "nan": float("nan"),
            "negative": -1,
            "over": 101,
            "exhausted": 100,
        }[kind]
        snap = replace(
            snap, windows=(AccountUsageWindow(label="session", used_percent=value),)
        )
    monkeypatch.setattr(api, "fetch_account_usage", lambda *a, **kw: snap)
    with pytest.raises(gate.QuotaAdmissionError):
        gate.enforce_quota(str(get_hermes_home().resolve()))


def test_available_quota_uses_only_native_provider(monkeypatch):
    import agent.account_usage as api

    seen = []

    def fetch(*a, **kw):
        seen.append((a, kw))
        return good()

    monkeypatch.setattr(api, "fetch_account_usage", fetch)
    gate.enforce_quota(str(get_hermes_home().resolve()))
    assert seen == [(("openai-codex",), {})]


def test_stalled_native_fetch_is_bounded(monkeypatch):
    import agent.account_usage as api

    entered = Event()
    release = Event()
    done = Event()
    errors = []
    monkeypatch.setattr(gate, "FETCH_TIMEOUT_SECONDS", 0.1, raising=False)

    def fetch(*a, **kw):
        entered.set()
        release.wait(8)
        return good()

    monkeypatch.setattr(api, "fetch_account_usage", fetch)
    profile = str(get_hermes_home().resolve())

    def call():
        try:
            gate.enforce_quota(profile)
        except Exception as exc:
            errors.append(exc)
        finally:
            done.set()

    thread = Thread(target=call, daemon=True)
    thread.start()
    try:
        assert entered.wait(2)
        assert done.wait(2), "quota fetch exceeded its host deadline"
        assert errors and isinstance(errors[0], gate.QuotaAdmissionError)
    finally:
        release.set()
        thread.join(2)


def test_wrong_profile_never_reads_quota(monkeypatch):
    import agent.account_usage as api

    calls = []
    monkeypatch.setattr(api, "fetch_account_usage", lambda *a, **kw: calls.append(True))
    with pytest.raises(gate.QuotaAdmissionError):
        gate.enforce_quota("/wrong/profile")
    assert not calls
