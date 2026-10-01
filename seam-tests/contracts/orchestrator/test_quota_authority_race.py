"""Slow quota I/O must not make worker admission authority stale."""

from threading import Event
import pytest
from test_native_worker_adapter import (
    fixture,
    _host_fields,
    ROUTE,
    _client_factory,
    _real_parent,
    _close_parent,
    _sse_response,
)
from test_quota_gate import good
from external_orchestrator.scheduler import ExternalScheduler
from external_orchestrator.native_worker import NativeWorkerAdapter


@pytest.mark.parametrize("change", ["cancel", "policy", "route"])
def test_authority_rechecked_after_slow_quota(fixture, tmp_path, monkeypatch, change):
    import agent.account_usage as api
    import tools.delegate_tool_results as native
    from agent.required_tool_policy import RequiredToolPolicy

    entered = Event()
    release = Event()
    reads = []
    builds = []
    requests = []
    route = dict(ROUTE)

    def fetch(*a, **kw):
        reads.append(True)
        if len(reads) == 2:
            entered.set()
            assert release.wait(15)
        return good()

    monkeypatch.setattr(api, "fetch_account_usage", fetch)
    original = native._build_child_preserving_parent_tools

    def build(*a, **kw):
        builds.append(True)
        return original(*a, **kw)

    monkeypatch.setattr(native, "_build_child_preserving_parent_tools", build)

    def response(request):
        requests.append(request)
        return _sse_response(request, model=ROUTE["model"], text="fixture")

    factory, cleanup = _client_factory(response, [])
    parent = _real_parent(factory)
    policy = RequiredToolPolicy(lambda name, args: True)
    with policy.scope():
        worker = NativeWorkerAdapter(parent, lambda profile: dict(route))
    s = ExternalScheduler(tmp_path / "store", worker=worker)
    owner = {**_host_fields(), "run_id": "slow-quota"}
    try:
        s.create_run(
            {
                **owner,
                "route": dict(ROUTE),
                "tasks": [
                    {
                        "task_id": "work",
                        "goal": "Read fixture",
                        "max_attempts": 1,
                        "timeout_seconds": 30,
                    }
                ],
            }
        )
        assert entered.wait(5)
        if change == "cancel":
            s.cancel({**owner, "task_ids": ["work"]})
        elif change == "policy":
            policy.revoke()
        else:
            route["model"] = "changed-host-route"
        release.set()
        s.join({**owner, "timeout_seconds": 10})
        s.close()
        assert not builds and not requests, (
            "stale authority crossed quota wait into child construction"
        )
    finally:
        release.set()
        s.close()
        cleanup.close()
        _close_parent(parent)
