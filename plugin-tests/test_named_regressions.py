"""Two named regression counterexamples (each failed before its fix)."""

from __future__ import annotations

import json
import threading

import pytest

from external_orchestrator.planner import plan_packets
from external_orchestrator.scheduler import (
    ExternalScheduler,
    HostOutcome,
    packet_identity,
)


def test_planner_rejects_heterogeneous_profiles_before_grouping():
    units = [
        {
            "unit_id": profile,
            "goal": f"goal for {profile}",
            "evidence_scope": "same-source",
            "capability_profile": "read-only",
            "profile": profile,
        }
        for profile in ("profile-a", "profile-b")
    ]

    with pytest.raises(ValueError, match="profile"):
        plan_packets(units)


def test_cancelled_replacement_does_not_deliver_prior_generation_result(tmp_path):
    release = threading.Event()
    replacement_entered = threading.Event()

    def worker(packet):
        # This is a host-observation test double, not production transport.
        if packet["generation"] == 1:
            replacement_entered.set()
            release.wait(5)
        observation = dict(packet["route"])
        observation.update(
            {
                "fallback": False,
                "physical_attempts": 1,
                "observed_at_transport": True,
                "observation_source": "sdk-transport",
            }
        )
        return HostOutcome(
            payload={
                "worker_status": "succeeded",
                "answer": f"answer-generation-{packet['generation']}",
                "evidence": [f"evidence-generation-{packet['generation']}"],
                "artifacts": [],
                "checks": [],
                "uncertainties": [],
                "suggested_followups": [],
            },
            observation=observation,
            identity=packet_identity(packet),
        )

    # Offline test double: the real quota gate (quota_gate.py) needs a live account snapshot and
    # fails closed without one. Quota admission is tested in seam-tests (test_quota_gate.py).
    scheduler = ExternalScheduler(
        tmp_path, worker=worker, max_global=1, quota_preflight=lambda packet: None
    )
    owner = {
        "owner_token": "owner",
        "parent_session_id": "session",
        "origin": "local",
        "profile": "profile",
    }
    try:
        run = scheduler.create_run(
            {
                **owner,
                "tasks": [{"task_id": "a", "goal": "original answer"}],
            }
        )
        run_owner = {**owner, "run_id": run["run_id"]}
        assert (
            scheduler.join({**run_owner, "timeout_seconds": 3})["state"] == "SUCCEEDED"
        )
        original = next(
            event
            for event in scheduler.collect(run_owner)["events"]
            if event["state"] == "SUCCEEDED" and event["generation"] == 0
        )

        scheduler.supersede({**run_owner, "task_id": "a"})
        assert replacement_entered.wait(2)
        scheduler.cancel({**run_owner, "task_id": "a"})

        cancelled = next(
            event
            for event in scheduler.collect(run_owner)["events"]
            if event["state"] == "CANCELLED"
        )
        assert cancelled["generation"] == 1
        assert cancelled["answer"] == ""
        assert cancelled["evidence"] == []
        assert cancelled["answer"] != original["answer"]
        assert cancelled["evidence"] != original["evidence"]

        # The replacement must not retain the prior host observation either.
        state = json.loads((tmp_path / "state.json").read_text(encoding="utf-8"))
        assert state["tasks"]["a"].get("result") in (None, {})
    finally:
        release.set()
        scheduler.close()
