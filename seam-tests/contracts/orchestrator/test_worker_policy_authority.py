"""Task input cannot widen host budgets or an admitted generation's budget."""

import copy
import test_admission_policy as support
from test_admission_policy import fixture, harness, task
from test_native_worker_adapter import _sse_response


def test_task_packet_limit_override_is_ignored_for_valid_small_task(
    fixture, tmp_path, monkeypatch
):
    with harness(tmp_path, monkeypatch) as (call, s, control, requests, cfg, parent):
        cfg["delegation"]["worker"]["max_packet_tokens"] = 256
        with monkeypatch.context() as hold:
            hold.setattr(s._worker, "can_admit", lambda p: False)
            result = call(
                "create", run_id="r", tasks=[{**task("w"), "max_packet_tokens": 272000}]
            )
            assert result["ok"], result
            assert s._read(lambda st: st["tasks"]["w"]["max_packet_tokens"]) == 256
        result = call("join", run_id="r", timeout_seconds=5)
        assert result["state"] == "SUCCEEDED", result
        assert len(requests) == 1


def test_supersession_cannot_relax_already_admitted_summary_budget(
    fixture, tmp_path, monkeypatch
):
    raw = "x" * 300
    monkeypatch.setattr(
        support, "_sse_response", lambda req, **kw: _sse_response(req, text=raw, **kw)
    )
    with harness(tmp_path, monkeypatch) as (call, s, control, requests, cfg, parent):
        cfg["delegation"]["worker"]["max_result_chars"] = 256
        with monkeypatch.context() as hold:
            hold.setattr(s._worker, "can_admit", lambda p: False)
            assert call("create", run_id="r", tasks=[task("w")])["ok"]
            cfg["delegation"]["worker"]["max_result_chars"] = 1024
            assert call(
                "supersede",
                run_id="r",
                task_id="w",
                goal="replacement",
                max_result_chars=5000,
            )["ok"]
        result = call("join", run_id="r", timeout_seconds=5)
        assert result["state"] == "FAILED", result
        stored = s._read(lambda st: copy.deepcopy(st["tasks"]["w"]))
        assert stored["host_max_result_chars"] == 256
        assert stored["result"]["answer"] == raw
        assert all(
            e["state"] == "FAILED" and e["answer"] == ""
            for e in call("collect", run_id="r")["events"]
        )
        assert len(requests) == 1
