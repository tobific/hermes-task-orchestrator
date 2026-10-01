"""Contract tests for owner-bound, bounded structured generation history.

These tests intentionally exercise real simulated-worker runs before reading history.
They need ExternalScheduler.history and the orchestration_history tool.
"""

from __future__ import annotations

import copy
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from external_orchestrator.scheduler import (  # noqa: E402
    MAX_EVIDENCE_CHARS,
    MAX_EVIDENCE_ITEMS,
    MAX_RESULT_CHARS,
    ExternalScheduler,
    OrchestrationError,
)


_HISTORY_RECORD_KEYS = {
    "cursor",
    "run_id",
    "task_id",
    "generation",
    "attempt",
    "record_type",
    "state",
    "current",
    "answer",
    "evidence",
    "uncertainties",
    "suggested_followups",
    "error_classification",
    "simulated",
}


@pytest.fixture
def scheduler(tmp_path):
    value = ExternalScheduler(
        tmp_path / "store",
        allow_simulated=True,
        quota_preflight=lambda packet: None,
    )
    try:
        yield value
    finally:
        value.close()


def _owner(run_id: str = "history-run") -> dict:
    return {
        "owner_token": "history-owner",
        "parent_session_id": "history-session",
        "profile": "history-profile",
        "run_id": run_id,
    }


def _complete_two_generations(scheduler, owner):
    scheduler.create_run(
        {
            **owner,
            "tasks": [
                {
                    "task_id": "task",
                    "goal": "generation zero result",
                    "max_attempts": 1,
                }
            ],
        }
    )
    first_join = scheduler.join({**owner, "timeout_seconds": 5})
    assert first_join["state"] == "SUCCEEDED"
    first = scheduler.status(owner)["task_states"][0]

    scheduler.supersede({**owner, "task_id": "task", "goal": "generation one result"})
    second_join = scheduler.join({**owner, "timeout_seconds": 5})
    assert second_join["state"] == "SUCCEEDED"
    current = scheduler.status(owner)["task_states"][0]
    assert (first["generation"], current["generation"]) == (0, 1)
    return first, current


def _history(scheduler, owner, **options):
    # This is the public scheduler operation, not a private state projection.
    return scheduler.history({**owner, **options})


def test_history_reads_both_generations_and_keeps_current_delivery_fenced(scheduler):
    owner = _owner()
    _complete_two_generations(scheduler, owner)

    history = _history(scheduler, owner)
    assert history["ok"] is True
    assert history["run_id"] == owner["run_id"]
    assert history["bounded"] is True
    assert history["diagnostic"] is True
    assert history["cursor"] == 0
    records = history["records"]
    assert [record["generation"] for record in records] == [0, 1]
    assert {record["record_type"] for record in records} == {"task_result"}

    by_generation = {record["generation"]: record for record in records}
    assert by_generation[0]["current"] is False
    assert by_generation[1]["current"] is True
    assert all(record["task_id"] == "task" for record in records)
    # Existing retained event rows do not record attempts; the read-only API
    # must disclose that absence, not mutate delivery storage or infer from now.
    assert all(record["attempt"] is None for record in records)
    assert all(record["state"] == "SUCCEEDED" for record in records)
    assert by_generation[0]["cursor"] < by_generation[1]["cursor"]

    # History must not widen the existing delivery/readiness fences.
    collected = scheduler.collect(owner)
    assert {event["generation"] for event in collected["events"]} == {1}
    joined = scheduler.join({**owner, "timeout_seconds": 0})
    assert joined["state"] == "SUCCEEDED"
    assert {task["generation"] for task in joined["task_states"]} == {1}


def test_history_uses_stable_event_cursor_across_appends(scheduler):
    owner = _owner("history-cursor-run")
    _complete_two_generations(scheduler, owner)

    first_page = _history(scheduler, owner, limit=1)
    assert len(first_page["records"]) == 1
    first_cursor = first_page["records"][0]["cursor"]
    assert first_page["next_cursor"] == first_cursor

    # Append a newer generation after page one.  The old generation-one row
    # must remain reachable from the event cursor returned by page one.
    scheduler.supersede({**owner, "task_id": "task", "goal": "generation two result"})
    assert scheduler.join({**owner, "timeout_seconds": 5})["state"] == "SUCCEEDED"

    second_page = _history(scheduler, owner, cursor=first_page["next_cursor"], limit=50)
    assert [record["generation"] for record in second_page["records"]] == [1, 2]
    assert all(record["cursor"] > first_cursor for record in second_page["records"])
    assert second_page["next_cursor"] == second_page["records"][-1]["cursor"]


@pytest.mark.parametrize(
    ("field", "wrong_value", "message"),
    [
        ("owner_token", "wrong-owner", "owner token mismatch"),
        ("parent_session_id", "wrong-session", "parent session mismatch"),
        ("profile", "wrong-profile", "profile mismatch"),
    ],
)
def test_history_owner_authorization_is_fail_closed_before_any_pump(
    scheduler, field, wrong_value, message
):
    owner = _owner("history-auth-run")
    _complete_two_generations(scheduler, owner)
    scheduler.close()
    before = scheduler.state_path.read_bytes()
    pumped = []

    def forbidden_pump():
        pumped.append(True)
        raise AssertionError("history must not pump worker work")

    scheduler.pump = forbidden_pump
    try:
        with pytest.raises(OrchestrationError, match=message):
            _history(scheduler, {**owner, field: wrong_value})
    finally:
        scheduler.pump = ExternalScheduler.pump.__get__(scheduler)

    assert pumped == []
    assert scheduler.state_path.read_bytes() == before


def test_history_is_read_only_and_does_not_consume_collect_cursor(scheduler):
    owner = _owner("history-read-only-run")
    _complete_two_generations(scheduler, owner)
    scheduler.close()
    before_state = scheduler._read(lambda state: copy.deepcopy(state))
    before_bytes = scheduler.state_path.read_bytes()
    original_pump = scheduler.pump

    def forbidden_pump():
        raise AssertionError("history must not invoke pump")

    scheduler.pump = forbidden_pump
    try:
        history = _history(scheduler, owner, limit=1)
    finally:
        scheduler.pump = original_pump

    assert len(history["records"]) == 1
    assert scheduler._read(lambda state: state) == before_state
    assert scheduler.state_path.read_bytes() == before_bytes

    # The separate history cursor cannot mark delivery events acknowledged or
    # change the current collect page.
    collected = scheduler.collect({**owner, "cursor": 0, "limit": 50})
    assert collected["cursor"] == 0
    assert {event["generation"] for event in collected["events"]} == {1}


def test_history_allowlist_redacts_internal_result_and_owner_fields(scheduler):
    owner = _owner("history-redaction-run")
    _complete_two_generations(scheduler, owner)

    history = _history(scheduler, owner, limit=10_000)
    assert len(history["records"]) <= 50
    for record in history["records"]:
        assert set(record) <= _HISTORY_RECORD_KEYS
        assert "owner_token" not in record
        assert "parent_session_id" not in record
        assert "profile" not in record
        assert "event_key" not in record
        assert "delivered" not in record
        assert "route_proof" not in record
        assert "claim_validation" not in record
        assert "artifacts" not in record
        assert "checks" not in record
        assert "error_message" not in record
        assert len(record.get("answer", "")) <= MAX_RESULT_CHARS
        for field in ("evidence", "uncertainties", "suggested_followups"):
            value = record[field]
            assert len(value) <= MAX_EVIDENCE_ITEMS
            assert all(len(item) <= MAX_EVIDENCE_CHARS for item in value)


def test_history_retains_failed_validation_records_without_calling_them_success(
    scheduler,
):
    owner = _owner("history-failure-run")
    scheduler.create_run(
        {
            **owner,
            "tasks": [
                {
                    "task_id": "task",
                    "goal": "malformed generation",
                    "worker_mode": "malformed",
                    "max_attempts": 1,
                }
            ],
        }
    )
    assert scheduler.join({**owner, "timeout_seconds": 5})["state"] == "FAILED"

    scheduler.supersede(
        {
            **owner,
            "task_id": "task",
            "goal": "validated replacement",
            "worker_mode": "success",
        }
    )
    assert scheduler.join({**owner, "timeout_seconds": 5})["state"] == "SUCCEEDED"

    history = _history(scheduler, owner)
    by_generation = {record["generation"]: record for record in history["records"]}
    failed = by_generation[0]
    succeeded = by_generation[1]
    assert failed["record_type"] == "task_result"
    assert failed["state"] == "FAILED"
    assert failed["error_classification"] == "MALFORMED_RESULT"
    assert failed["current"] is False
    assert succeeded["state"] == "SUCCEEDED"
    assert succeeded["current"] is True
    assert {event["generation"] for event in scheduler.collect(owner)["events"]} == {1}


def test_history_does_not_bypass_final_review_or_create_final_delivery(scheduler):
    owner = _owner("history-review-run")
    scheduler.create_run(
        {
            **owner,
            "final_review_task_id": "review",
            "tasks": [
                {"task_id": "work", "goal": "work result", "max_attempts": 1},
                {
                    "task_id": "review",
                    "goal": "strict final review",
                    "dependencies": ["work"],
                    "max_attempts": 1,
                },
            ],
        }
    )
    joined = scheduler.join({**owner, "timeout_seconds": 5})
    assert joined["state"] == "FAILED"
    # Terminal result publication precedes the worker's final settlement.
    # Drain that independent writer before measuring read-only behavior.
    scheduler.close()

    before = scheduler._read(lambda state: copy.deepcopy(state))
    history = _history(scheduler, owner)
    after = scheduler._read(lambda state: copy.deepcopy(state))
    assert after == before
    assert any(
        record["task_id"] == "review"
        and record["state"] == "FAILED"
        and record["error_classification"] == "REVIEW_NOT_APPROVED"
        for record in history["records"]
    )
    assert scheduler.collect(owner)["events"] == []
    assert not any(event.get("final_delivery") for event in after["delivery_events"])


def test_orchestration_history_is_a_public_bounded_tool_contract(monkeypatch):
    import external_orchestrator as plugin

    class Context:
        plugin_id = "history-contract"

        def __init__(self):
            self.tools = {}

        def get_config(self, name, default=False):
            return default

        def register_tool(self, **kwargs):
            self.tools[kwargs["name"]] = kwargs

        def register_cli_command(self, **kwargs):
            return None

        def register_hook(self, *args, **kwargs):
            return None

    # Avoid host startup/configuration while still exercising the real public
    # registration table and handler construction.
    monkeypatch.setattr(plugin, "_scheduler", lambda ctx: object())
    ctx = Context()
    plugin.register(ctx)
    assert "orchestration_history" in ctx.tools

    tool = ctx.tools["orchestration_history"]
    assert callable(tool["handler"])
    schema = tool["schema"]
    properties = schema["parameters"]["properties"]
    assert schema["name"] == "orchestration_history"
    assert properties["cursor"]["minimum"] == 0
    assert properties["limit"]["minimum"] == 1
    assert properties["limit"]["maximum"] <= 50
    assert "owner_only" not in properties
