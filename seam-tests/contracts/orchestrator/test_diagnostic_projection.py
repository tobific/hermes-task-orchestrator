"""Focused contract tests for the bounded owner diagnostic projection."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from external_orchestrator.scheduler import ExternalScheduler  # noqa: E402
from test_admission_policy import fixture, harness, task  # noqa: E402,F401


MAX_HISTORY_BYTES = 65536


def _edit_diagnostic(scheduler, callback):
    from external_orchestrator.diagnostic_store import load_diagnostic, save_diagnostic

    state = scheduler._read(lambda s: s)
    event = state["delivery_events"][0]
    run = state["runs"][event["run_id"]]
    row = load_diagnostic(scheduler.data_dir, run, event)
    assert row is not None
    callback({"diagnostic_events": [row]})
    assert save_diagnostic(scheduler.data_dir, run, event, row)


def _diagnostic_by_generation(history):
    return {item["generation"]: item for item in history["diagnostics"]}


def test_native_sdk_observation_projects_route_validation_and_result(
    fixture, tmp_path, monkeypatch
):
    with harness(tmp_path, monkeypatch) as (
        call,
        _scheduler,
        _control,
        requests,
        _cfg,
        _parent,
    ):
        assert call("create", run_id="diagnostic-native", tasks=[task("work")])["ok"]
        assert (
            call(
                "join", run_id="diagnostic-native", condition="all", timeout_seconds=10
            )["state"]
            == "SUCCEEDED"
        )
        history = call("history", run_id="diagnostic-native")
        diagnostic = history["diagnostics"][0]

        assert len(requests) == 1
        assert diagnostic["bounded"] is True
        assert diagnostic["current"] is True
        assert diagnostic["provenance"] == {
            "available": True,
            "observation_source": "sdk-transport",
            "observed_at_transport": True,
            "authority": "host",
        }
        assert diagnostic["requested_route"]["available"] is True
        assert diagnostic["requested_route"]["source"] == "host-packet"
        assert diagnostic["requested_route"]["route"]["model"] == "gpt-6-luna"
        observed = diagnostic["transport_observed_route"]
        assert observed["available"] is True
        assert observed["authority"] == "host"
        assert observed["route"]["observation_source"] == "sdk-transport"
        assert observed["route"]["observed_at_transport"] is True
        validation = diagnostic["validation"]
        assert validation["available"] is True
        assert validation["source"] == "host-claim-validator"
        assert validation["mode"] == "plain_summary"
        assert validation["outcome"] == "plain_summary_no_claims"
        assert diagnostic["result_evidence"]["available"] is True
        assert diagnostic["result_evidence"]["answer_available"] is True
        assert diagnostic["result_evidence"]["native_summary"]["available"] is False
        assert diagnostic["artifact_evidence"]["available"] is False
        assert "owner_token" not in json.dumps(diagnostic)
        assert "parent_session_id" not in json.dumps(diagnostic)


def test_simulated_route_remains_worker_claim_not_transport_observation(tmp_path):
    owner = {
        "owner_token": "sim-owner",
        "parent_session_id": "sim-session",
        "profile": "sim-profile",
        "run_id": "diagnostic-simulated",
    }
    scheduler = ExternalScheduler(
        tmp_path / "store", allow_simulated=True, quota_preflight=lambda packet: None
    )
    try:
        assert scheduler.create_run(
            {**owner, "tasks": [{"task_id": "work", "goal": "offline"}]}
        )["ok"]
        assert scheduler.join({**owner, "timeout_seconds": 10})["state"] == "SUCCEEDED"
        diagnostic = scheduler.history(owner)["diagnostics"][0]
        assert diagnostic["provenance"]["observation_source"] == "simulated-worker"
        assert diagnostic["provenance"]["authority"] == "worker"
        assert diagnostic["transport_observed_route"]["available"] is False
        assert diagnostic["worker_reported_route"]["available"] is True
    finally:
        scheduler.close()


def test_legacy_generation_is_explicitly_unavailable_and_new_generation_is_retained(
    fixture, tmp_path, monkeypatch
):
    with harness(tmp_path, monkeypatch) as (
        call,
        scheduler,
        _control,
        _requests,
        _cfg,
        _parent,
    ):
        assert call("create", run_id="diagnostic-generations", tasks=[task("work")])[
            "ok"
        ]
        assert (
            call(
                "join",
                run_id="diagnostic-generations",
                condition="all",
                timeout_seconds=10,
            )["state"]
            == "SUCCEEDED"
        )
        for sidecar in (scheduler.data_dir / "diagnostics").glob("*.json"):
            sidecar.unlink()
        assert call("supersede", run_id="diagnostic-generations", task_id="work")["ok"]
        assert (
            call(
                "join",
                run_id="diagnostic-generations",
                condition="all",
                timeout_seconds=10,
            )["state"]
            == "SUCCEEDED"
        )

        by_generation = _diagnostic_by_generation(
            call("history", run_id="diagnostic-generations")
        )
        assert set(by_generation) == {0, 1}
        old = by_generation[0]
        assert old["current"] is False
        assert old["transport_observed_route"]["available"] is False
        assert old["validation"]["available"] is False
        assert old["result_evidence"]["available"] is False
        current = by_generation[1]
        assert current["current"] is True
        assert current["transport_observed_route"]["available"] is True
        assert current["validation"]["available"] is True


def test_diagnostic_projection_is_owner_isolated_and_does_not_change_collection(
    fixture, tmp_path, monkeypatch
):
    with harness(tmp_path, monkeypatch) as (
        call,
        scheduler,
        _control,
        _requests,
        _cfg,
        parent,
    ):
        assert call("create", run_id="diagnostic-owner", tasks=[task("work")])["ok"]
        assert (
            call(
                "join", run_id="diagnostic-owner", condition="all", timeout_seconds=10
            )["state"]
            == "SUCCEEDED"
        )
        before = call("collect", run_id="diagnostic-owner")
        before_state = scheduler._read(lambda state: state["delivery_events"][:])
        assert call("history", run_id="diagnostic-owner")["ok"] is True
        after = call("collect", run_id="diagnostic-owner")
        after_state = scheduler._read(lambda state: state["delivery_events"][:])
        assert before["events"] == after["events"]
        assert before["cursor"] == after["cursor"] == 0
        assert before_state == after_state

        parent.session_id = "different-owner"
        denied = call("history", run_id="diagnostic-owner")
        assert denied["ok"] is False


def test_diagnostic_projection_redacts_and_bounds_retained_evidence_references(
    fixture, tmp_path, monkeypatch
):
    with harness(tmp_path, monkeypatch) as (
        call,
        scheduler,
        _control,
        _requests,
        _cfg,
        _parent,
    ):
        assert call("create", run_id="diagnostic-bounds", tasks=[task("work")])["ok"]
        assert (
            call(
                "join", run_id="diagnostic-bounds", condition="all", timeout_seconds=10
            )["state"]
            == "SUCCEEDED"
        )
        secret = "sk-" + ("A" * 80)
        _edit_diagnostic(
            scheduler,
            lambda state: state["diagnostic_events"][0].update(
                {
                    "route_proof": {
                        "provider": "openai-codex",
                        "model": secret,
                        "base_url": "https://example.test/" + ("B" * 5000),
                        "observation_source": "sdk-transport",
                        "observed_at_transport": True,
                    },
                    "claim_validation": {
                        "schema": 1,
                        "mode": "structured_claims",
                        "declared": True,
                        "artifacts": [
                            {
                                "type": "file",
                                "path": "/not-read/" + secret,
                                "size": 12,
                                "sha256": "c" * 64,
                                "verified": True,
                            }
                        ],
                        "checks": [],
                        "limitations": ["L" * 5000],
                    },
                    "result": {
                        "available": True,
                        "worker_status": "succeeded",
                        "answer_available": True,
                        "native_summary": secret + ("N" * 5000),
                        "evidence": [secret + ("E" * 5000)],
                        "simulated": False,
                        "error_classification": None,
                    },
                }
            ),
        )
        history = call("history", run_id="diagnostic-bounds", limit=50)
        encoded = json.dumps(history, ensure_ascii=False).encode("utf-8")
        assert len(encoded) <= MAX_HISTORY_BYTES
        diagnostic = history["diagnostics"][0]
        assert secret not in encoded.decode("utf-8")
        assert len(diagnostic["result_evidence"]["native_summary"]["reference"]) <= 4000
        assert (
            secret not in diagnostic["result_evidence"]["native_summary"]["reference"]
        )
        assert len(diagnostic["result_evidence"]["evidence"]["items"][0]) <= 500
        artifact = diagnostic["artifact_evidence"]["items"][0]
        assert artifact["reference"].startswith("/not-read/")
        assert secret not in artifact["reference"]
        assert artifact["verified"] is True
        assert artifact["sha256"] == "c" * 64


def test_revalidation_failure_is_not_projected_as_prior_acceptance(
    fixture, tmp_path, monkeypatch
):
    with harness(tmp_path, monkeypatch) as (
        call,
        scheduler,
        _control,
        _requests,
        _cfg,
        _parent,
    ):
        assert call("create", run_id="diagnostic-revalidate", tasks=[task("work")])[
            "ok"
        ]
        assert (
            call(
                "join",
                run_id="diagnostic-revalidate",
                condition="all",
                timeout_seconds=10,
            )["state"]
            == "SUCCEEDED"
        )
        _edit_diagnostic(
            scheduler,
            lambda state: state["diagnostic_events"][0].update(
                {
                    "validation_outcome": "delivery_claim_revalidation_failed",
                    "claim_validation": {
                        "schema": 1,
                        "mode": "structured_claims",
                        "declared": True,
                        "artifacts": [],
                        "checks": [
                            {
                                "name": "worker-asserted",
                                "status": "pass",
                                "detail": "not host proof",
                                "host_verified": False,
                            }
                        ],
                        "limitations": [],
                    },
                    "result": {
                        "available": True,
                        "worker_status": "failed",
                        "answer_available": True,
                        "native_summary": None,
                        "evidence": [],
                        "simulated": False,
                        "error_classification": "DELIVERY_CLAIM_REVALIDATION_FAILED",
                    },
                }
            ),
        )
        diagnostic = call("history", run_id="diagnostic-revalidate")["diagnostics"][0]
        validation = diagnostic["validation"]
        assert validation["outcome"] == "delivery_claim_revalidation_failed"
        assert validation["prior_outcome"] == "structured_claims_unverified"
        assert validation["accepted_before_delivery"] is False
        assert diagnostic["check_evidence"]["available"] is False
        assert diagnostic["result_evidence"]["error_classification"] == (
            "DELIVERY_CLAIM_REVALIDATION_FAILED"
        )
