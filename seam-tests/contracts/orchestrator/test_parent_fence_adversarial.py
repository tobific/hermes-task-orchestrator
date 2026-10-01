from contextlib import contextmanager
from queue import Queue
import json
import pytest
from test_native_worker_adapter import fixture
from test_native_completion_fence_contract import _Provider


def completed(native, monkeypatch, provider=None):
    from tools.process_registry import process_registry

    q = Queue()
    monkeypatch.setattr(process_registry, "completion_queue", q)
    native.set_completion_guard(provider)
    context = native.register_completion_guard(provider)
    with context:
        handle = native.dispatch_async_delegation(
            goal="evidence",
            context=None,
            toolsets=None,
            role="leaf",
            model=None,
            session_key="test",
            parent_session_id="parent",
            runner=lambda: {"summary": "exact evidence", "status": "completed"},
        )
    evt = q.get(timeout=8)
    return handle, evt, q


def test_valid_guard_does_not_admit_missing_native_row(fixture):
    import tools.async_delegation as n

    n.set_completion_guard(_Provider())
    assert not n.claim_completion_delivery(
        "missing", "a", completion_fence={"marker": "missing"}
    )


def test_guarded_claim_counts_attempts(fixture, monkeypatch):
    import tools.async_delegation as n

    h, e, q = completed(n, monkeypatch, _Provider())
    for index in range(3):
        claim = n.claim_event_delivery(e, "consumer")
        assert claim
        assert (
            n.get_durable_delegation(h["delegation_id"])["delivery_attempts"]
            == index + 1
        )
        n.release_event_delivery(e, claim)


def test_guarded_drop_is_not_resurrected_by_late_publication(fixture, monkeypatch):
    import tools.async_delegation as n

    h, e, q = completed(n, monkeypatch, _Provider())
    claim = n.claim_event_delivery(e, "consumer")
    assert claim
    assert n.drop_completion_delivery(h["delegation_id"], claim)
    n._push_completion_event(
        n._records[h["delegation_id"]], {"summary": "exact evidence"}, "completed"
    )
    assert n.get_durable_delegation(h["delegation_id"])["delivery_state"] == "dropped"
    assert q.empty()


def test_replay_cannot_reapprove_old_payload(fixture, monkeypatch):
    import tools.async_delegation as n

    class Revision(_Provider):
        revision = 0

        def stamp(self, record):
            return {"marker": record["delegation_id"], "revision": self.revision}

        def prepare(self, marker, delegation_id):
            return {"marker": delegation_id, "revision": self.revision}

        def validate(self, marker, delegation_id):
            return marker == {"marker": delegation_id, "revision": self.revision}

    p = Revision()
    h, e, q = completed(n, monkeypatch, p)
    p.revision = 1
    assert n.restore_undelivered_completions(q) == 0
    assert q.empty()


def test_legacy_malformed_task_payload_keeps_claim_contract(fixture, monkeypatch):
    import tools.async_delegation as n

    h, e, q = completed(n, monkeypatch)
    with n._DB_LOCK, n._transaction() as db:
        db.execute(
            "UPDATE async_delegations SET task_json=? WHERE delegation_id=?",
            ("not-json", h["delegation_id"]),
        )
    assert n.claim_event_delivery(e, "legacy")
