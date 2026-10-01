"""Native completion-fence contracts (patch 0003)."""

from contextlib import contextmanager
from queue import Queue

from test_native_worker_adapter import fixture


class _Provider:
    def __init__(self):
        self.calls = []
        self.locks = 0

    @contextmanager
    def lock(self):
        self.locks += 1
        yield

    def stamp(self, record):
        return {"marker": record["delegation_id"]}

    def validate(self, marker, delegation_id):
        self.calls.append((marker, delegation_id))
        return marker == {"marker": delegation_id}


def test_marker_presence_never_downgrades_to_legacy_missing_row(fixture):
    import tools.async_delegation as native

    provider = _Provider()
    native.set_completion_guard(provider)
    try:
        # No marker key is the only legacy selector, and must not consult the
        # provider even when a host provider is installed.
        assert native.claim_completion_delivery("legacy-opaque-id", "legacy")
        assert provider.calls == []

        assert not native.claim_completion_delivery(
            "guarded-null-id", "consumer", completion_fence=None
        )
        assert not native.claim_completion_delivery(
            "guarded-bad-id", "consumer", completion_fence={"wrong": True}
        )
        assert all(identity != "legacy-opaque-id" for _, identity in provider.calls)
    finally:
        native.set_completion_guard(None)


def test_registered_guard_stamps_event_after_result_fields(fixture, monkeypatch):
    import tools.async_delegation as native
    from tools.process_registry import process_registry

    events = Queue()
    monkeypatch.setattr(process_registry, "completion_queue", events)
    provider = _Provider()
    with native.register_completion_guard(provider):
        handle = native.dispatch_async_delegation(
            goal="guarded result",
            context=None,
            toolsets=None,
            role="leaf",
            model=None,
            session_key="test:fence",
            parent_session_id="fence-parent",
            runner=lambda: {
                "status": "completed",
                "summary": "opaque result",
                "completion_fence": {"model": "must not win"},
            },
        )
    assert handle["status"] == "dispatched"
    event = events.get(timeout=5)
    assert event["completion_fence"] == {"marker": handle["delegation_id"]}
    assert event["summary"] == "opaque result"
    assert provider.calls[-1] == (
        {"marker": handle["delegation_id"]},
        handle["delegation_id"],
    )
