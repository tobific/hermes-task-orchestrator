"""Sidecar authorization, failure isolation and cold retention proofs."""

import json
import pytest
from test_admission_policy import fixture, harness, task
from external_orchestrator import diagnostic_store as ds


def _finish(call, run):
    assert call("create", run_id=run, tasks=[task("work")])["ok"]
    assert (
        call("join", run_id=run, condition="all", timeout_seconds=10)["state"]
        == "SUCCEEDED"
    )


def test_sidecar_io_failure_cannot_fail_work_or_collection(
    fixture, tmp_path, monkeypatch
):
    with harness(tmp_path, monkeypatch) as (call, scheduler, *_):
        monkeypatch.setattr(
            ds,
            "atomic_replace_bytes",
            lambda *a, **k: (_ for _ in ()).throw(OSError("disk full")),
        )
        _finish(call, "io-failure")
        assert call("collect", run_id="io-failure")["events"]
        assert "diagnostic_events" not in scheduler._read(lambda s: s)
        assert call("supersede", run_id="io-failure", task_id="work")["ok"]
        assert (
            call("join", run_id="io-failure", condition="all", timeout_seconds=10)[
                "state"
            ]
            == "SUCCEEDED"
        )
        old = call("history", run_id="io-failure")["diagnostics"][0]
        assert old["current"] is False
        assert old["transport_observed_route"]["available"] is False


def test_sidecar_retains_old_generation_without_state_or_file_mutation(
    fixture, tmp_path, monkeypatch
):
    from external_orchestrator.history import read_history

    with harness(tmp_path, monkeypatch) as (call, scheduler, *_):
        _finish(call, "retention")
        assert call("supersede", run_id="retention", task_id="work")["ok"]
        assert (
            call("join", run_id="retention", condition="all", timeout_seconds=10)[
                "state"
            ]
            == "SUCCEEDED"
        )
        state = scheduler._read(lambda s: s)
        run = state["runs"]["retention"]
        before = {
            p.name: p.read_bytes()
            for p in (scheduler.data_dir / "diagnostics").glob("*.json")
        }
        state_before = scheduler.state_path.read_bytes()
        view = read_history(
            scheduler.data_dir,
            {
                k: run[k]
                for k in ("run_id", "owner_token", "parent_session_id", "profile")
            },
        )
        assert [r["current"] for r in view["diagnostics"]] == [False, True]
        assert all(
            r["transport_observed_route"]["available"] for r in view["diagnostics"]
        )
        assert scheduler.state_path.read_bytes() == state_before
        assert before == {
            p.name: p.read_bytes()
            for p in (scheduler.data_dir / "diagnostics").glob("*.json")
        }


@pytest.mark.parametrize(
    "attack", ["binding", "symlink", "hardlink", "mode", "oversize"]
)
def test_invalid_sidecar_is_unavailable_not_trusted(
    fixture, tmp_path, monkeypatch, attack
):
    with harness(tmp_path, monkeypatch) as (call, scheduler, *_):
        _finish(call, "tamper")
        state = scheduler._read(lambda s: s)
        run = state["runs"]["tamper"]
        event = state["delivery_events"][0]
        p = ds.diagnostic_path(scheduler.data_dir, run, event)
        assert ds.load_diagnostic(scheduler.data_dir, run, event)
        if attack == "binding":
            j = json.loads(p.read_text())
            j["binding"] = "0" * 64
            p.write_text(json.dumps(j))
        elif attack in ("symlink", "hardlink"):
            import os

            other = tmp_path / "outside"
            other.write_bytes(p.read_bytes())
            other.chmod(0o600)
            p.unlink()
            if attack == "symlink":
                p.symlink_to(other)
            else:
                os.link(other, p)
        elif attack == "mode":
            p.chmod(0o644)
        else:
            p.write_bytes(b"x" * (ds.MAX_DIAGNOSTIC_BYTES + 1))
        assert ds.load_diagnostic(scheduler.data_dir, run, event) is None
        assert call("collect", run_id="tamper")["events"]


def test_unauthorized_history_does_not_read_sidecar(fixture, tmp_path, monkeypatch):
    with harness(tmp_path, monkeypatch) as (
        call,
        scheduler,
        control,
        requests,
        cfg,
        parent,
    ):
        _finish(call, "authorize-first")

        def forbidden(*a, **k):
            raise AssertionError("read before authorization")

        monkeypatch.setattr(ds, "load_diagnostic", forbidden)
        parent.session_id = "other-session"
        assert call("history", run_id="authorize-first")["ok"] is False
