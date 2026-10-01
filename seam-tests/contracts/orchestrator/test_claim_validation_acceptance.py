"""Acceptance tests for the external result claim-validation boundary.

These tests deliberately keep the native child, SDK parser, scheduler and
required reviewer real.  Only the HTTP transport is a fixture, matching the
native adapter tests.  The worker summary is treated as an opaque string: a
strict, top-level JSON result is the only supported structured-claim contract.
Arbitrary prose is not semantically scanned for claims.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy

import pytest

from test_native_worker_adapter import (
    ROUTE,
    _client_factory,
    _close_parent,
    _host_fields,
    _real_parent,
    _sse_response,
    fixture,
)
from external_orchestrator.native_worker import NativeWorkerAdapter
from external_orchestrator.scheduler import ExternalScheduler


def _structured(*, artifacts=None, checks=None, answer="claimed completion"):
    return {
        "status": "pass",
        "answer": answer,
        "evidence": ["controlled acceptance fixture"],
        "artifacts": [] if artifacts is None else artifacts,
        "checks": [] if checks is None else checks,
        "uncertainties": [],
        "suggested_followups": [],
    }


def _run_claim(
    fixture, tmp_path, raw, *, mutate_after_review=False, run_id="claim-acceptance"
):
    packets = []

    def handler(request):
        packet = packets[-1]
        text = (
            raw
            if packet["task_id"] == "work"
            else json.dumps(
                {
                    "verdict": "approve",
                    "evidence_digest": packet["review_evidence_digest"],
                    "rationale": "controlled acceptance approval",
                },
                separators=(",", ":"),
            )
        )
        return _sse_response(request, model=ROUTE["model"], text=text)

    factory, cleanup = _client_factory(handler, [])
    parent = _real_parent(factory)

    class Measured(NativeWorkerAdapter):
        def __call__(self, packet):
            packets.append(deepcopy(packet))
            return super().__call__(packet)

    class MutatingReviewScheduler(ExternalScheduler):
        def __init__(self, *args, mutation_target, **kwargs):
            self._mutation_target = mutation_target
            self._mutated = False
            super().__init__(*args, **kwargs)

        def _review_approval_record(self, state, run):
            approval = super()._review_approval_record(state, run)
            if approval and not self._mutated:
                self._mutation_target.write_text(
                    "changed after review", encoding="utf-8"
                )
                self._mutated = True
            return approval

    scheduler_cls = (
        MutatingReviewScheduler if mutate_after_review else ExternalScheduler
    )
    scheduler_kwargs = {}
    if mutate_after_review:
        scheduler_kwargs["mutation_target"] = tmp_path / "artifact.txt"
    scheduler = scheduler_cls(
        data_dir=tmp_path / "state",
        worker=Measured(parent, lambda _: dict(ROUTE)),
        max_global=1,
        per_profile=1,
        **scheduler_kwargs,
    )
    scheduler.route_resolver = lambda _: dict(ROUTE)
    fields = _host_fields()
    args = {
        **fields,
        "run_id": run_id,
        "capability_profile": "read-only",
        "write_scope": [str(tmp_path)],
        "route": dict(ROUTE),
        "final_review_task_id": "review",
        "tasks": [
            {
                "task_id": "work",
                "goal": "return the controlled structured result",
                "timeout_seconds": 60,
                "max_attempts": 1,
            },
            {
                "task_id": "review",
                "goal": "review the controlled result",
                "required": True,
                "dependencies": ["work"],
                "timeout_seconds": 60,
                "max_attempts": 1,
            },
        ],
    }
    owner = {
        key: args[key]
        for key in ("run_id", "owner_token", "parent_session_id", "profile")
    }
    try:
        scheduler.create_run(args)
        state = scheduler.join({**owner, "timeout_seconds": 20})
        collected = scheduler.collect(owner)
        durable = json.loads(scheduler.state_path.read_text(encoding="utf-8"))
        return state, collected, durable
    finally:
        scheduler.close()
        cleanup.close()
        _close_parent(parent)


def test_missing_file_artifact_claim_fails_real_native_delivery(fixture, tmp_path):
    missing = tmp_path / "never-created.txt"
    raw = json.dumps(_structured(artifacts=[{"path": str(missing)}]))
    state, collected, durable = _run_claim(fixture, tmp_path, raw)
    assert state["state"] == "FAILED"
    assert collected["events"] == []
    assert durable["tasks"]["work"]["state"] == "FAILED"
    assert "artifact" in durable["tasks"]["work"]["result"]["error_message"].lower()
    assert durable["tasks"]["work"]["result"]["answer"] == raw
    assert durable["delivery_events"][0]["answer"] == raw
    assert not missing.exists()


def test_explicit_failed_check_claim_fails_real_native_delivery(fixture, tmp_path):
    raw = json.dumps(
        _structured(
            checks=[
                {
                    "name": "required check",
                    "status": "fail",
                    "detail": "controlled failure",
                }
            ]
        )
    )
    state, collected, durable = _run_claim(fixture, tmp_path, raw)
    assert state["state"] == "FAILED"
    assert collected["events"] == []
    assert durable["tasks"]["work"]["state"] == "FAILED"
    assert "failed" in durable["tasks"]["work"]["result"]["error_message"].lower()


def test_false_pass_check_without_host_verification_fails_real_native_delivery(
    fixture, tmp_path
):
    raw = json.dumps(
        _structured(
            checks=[
                {
                    "name": "required check",
                    "status": "pass",
                    "detail": "worker says passed",
                }
            ]
        )
    )
    state, collected, durable = _run_claim(fixture, tmp_path, raw)
    assert state["state"] == "FAILED"
    assert collected["events"] == []
    assert durable["tasks"]["work"]["state"] == "FAILED"
    assert "host" in durable["tasks"]["work"]["result"]["error_message"].lower()


def test_positive_local_artifact_is_host_verified_and_summary_bytes_are_unchanged(
    fixture, tmp_path
):
    artifact = tmp_path / "artifact.txt"
    content = b"host-created acceptance artifact\n"
    artifact.write_bytes(content)
    digest = hashlib.sha256(content).hexdigest()
    raw = json.dumps(
        _structured(artifacts=[{"path": str(artifact), "sha256": digest}]),
        ensure_ascii=False,
        separators=(",", ":"),
    )
    state, collected, durable = _run_claim(fixture, tmp_path, raw)
    assert state["state"] == "SUCCEEDED"
    event = collected["events"][0]
    assert event["final_delivery"] is True
    assert event["work_results"][0]["result"]["answer"] == raw
    assert durable["tasks"]["work"]["result"]["answer"] == raw
    assert durable["tasks"]["work"]["result"]["artifacts"][0]["sha256"] == digest


def test_native_observer_verifies_only_its_narrow_positive_check(fixture, tmp_path):
    raw = json.dumps(
        _structured(
            checks=[
                {
                    "name": "native-conversation-completed",
                    "status": "pass",
                    "detail": "worker detail is not used as proof",
                }
            ]
        ),
        separators=(",", ":"),
    )
    state, collected, durable = _run_claim(fixture, tmp_path, raw)
    assert state["state"] == "SUCCEEDED"
    checks = durable["tasks"]["work"]["result"]["checks"]
    assert checks == [
        {
            "name": "native-conversation-completed",
            "status": "pass",
            "detail": "worker detail is not used as proof",
            "host_verified": True,
        }
    ]
    assert collected["events"][0]["work_results"][0]["result"]["answer"] == raw


def test_plain_native_summary_is_opaque_and_preserved(fixture, tmp_path):
    raw = "native summary with no declared structured claims — exact bytes"
    state, collected, durable = _run_claim(fixture, tmp_path, raw)
    assert state["state"] == "SUCCEEDED"
    assert collected["events"][0]["work_results"][0]["result"]["answer"] == raw
    assert durable["tasks"]["work"]["result"]["answer"] == raw


@pytest.mark.parametrize(
    "raw_factory",
    [
        lambda tmp: json.dumps({"status": "pass", "artifacts": "not-an-array"}),
        lambda tmp: json.dumps({"status": "pass", "checks": [{"name": "x"}]}),
        lambda tmp: json.dumps({"result": _structured(artifacts=[])}),
        lambda tmp: "```json\n" + json.dumps(_structured(artifacts=[])) + "\n```",
        lambda tmp: json.dumps(
            _structured(
                artifacts=[{"path": str(tmp / "artifact.txt"), "sha256": "0" * 64}]
            )
        ),
    ],
)
def test_malformed_wrapped_fenced_and_forged_claims_fail(
    fixture, tmp_path, raw_factory
):
    artifact = tmp_path / "artifact.txt"
    artifact.write_text("real content", encoding="utf-8")
    raw = raw_factory(tmp_path)
    state, collected, durable = _run_claim(fixture, tmp_path, raw)
    assert state["state"] == "FAILED"
    assert collected["events"] == []
    assert durable["tasks"]["work"]["state"] == "FAILED"


def test_outside_root_and_symlink_artifacts_fail(fixture, tmp_path):
    outside = tmp_path.parent / "outside-claim.txt"
    outside.write_text("outside", encoding="utf-8")
    outside_raw = json.dumps(_structured(artifacts=[{"path": str(outside)}]))
    state, collected, durable = _run_claim(fixture, tmp_path, outside_raw)
    assert state["state"] == "FAILED"
    assert collected["events"] == []
    assert durable["tasks"]["work"]["state"] == "FAILED"

    second = tmp_path / "symlink-case"
    second.mkdir()
    target = second / "target.txt"
    target.write_text("target", encoding="utf-8")
    link = second / "link.txt"
    link.symlink_to(target)
    symlink_raw = json.dumps(_structured(artifacts=[{"path": str(link)}]))
    state, collected, durable = _run_claim(
        fixture, second, symlink_raw, run_id="claim-acceptance-symlink"
    )
    assert state["state"] == "FAILED"
    assert collected["events"] == []
    assert durable["tasks"]["work"]["state"] == "FAILED"


def test_artifact_mutation_after_review_blocks_final_delivery(fixture, tmp_path):
    artifact = tmp_path / "artifact.txt"
    artifact.write_text("before review", encoding="utf-8")
    digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
    raw = json.dumps(
        _structured(artifacts=[{"path": str(artifact), "sha256": digest}]),
        separators=(",", ":"),
    )
    state, collected, durable = _run_claim(
        fixture, tmp_path, raw, mutate_after_review=True
    )
    assert state["state"] == "FAILED"
    assert collected["events"] == []
    assert durable["tasks"]["work"]["state"] == "SUCCEEDED"
    assert durable["tasks"]["review"]["state"] == "SUCCEEDED"
