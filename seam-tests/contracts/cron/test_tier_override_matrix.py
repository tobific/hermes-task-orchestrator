"""Explicit job pins override premium settings; omission preserves inheritance."""

from copy import deepcopy
from types import SimpleNamespace
import pytest


@pytest.mark.parametrize("tier", [None, "", "normal", "priority"])
@pytest.mark.parametrize("inherited", [False, True])
def test_real_agent_override_matrix(tier, inherited):
    from run_agent import AIAgent
    from cron.scheduler_agent import construct_cron_agent
    from agent.fast_mode import effective_request_overrides

    overrides = {"extra_body": {"fixture_marker": "preserve"}}
    if inherited:
        overrides.update(service_tier="priority", speed="fast")
        overrides["extra_body"].update(service_tier="priority", speed="fast")
    runtime = {
        "provider": "openai",
        "api_mode": "chat_completions",
        "api_key": "offline-fixture",
        "base_url": "https://api.openai.com/v1",
        "request_overrides": overrides,
    }
    opening = deepcopy(runtime)
    setup = SimpleNamespace(
        model="gpt-5",
        runtime=runtime,
        max_iterations=1,
        reasoning_config=None,
        prefill_messages=None,
        fallback_model=[],
        credential_pool=None,
    )
    job = {"id": "fixture", "enabled_toolsets": [], "allow_fallbacks": False}
    if tier is not None:
        job["service_tier"] = tier
    agent = construct_cron_agent(
        AIAgent, job, {}, setup, workdir=None, session_id="fixture", session_db=None
    )
    try:
        result = effective_request_overrides(agent)
        assert runtime == opening
        assert result["extra_body"]["fixture_marker"] == "preserve"
        if tier == "priority":
            assert result.get("service_tier") == "priority"
        elif tier == "normal":
            assert not {"service_tier", "speed"} & result.keys()
            assert not {"service_tier", "speed"} & result["extra_body"].keys()
        elif inherited:
            assert result.get("service_tier") == "priority"
        else:
            assert result.get("service_tier") != "priority"
    finally:
        agent.close()


def test_unsupported_priority_fails_before_agent_construction():
    from cron.scheduler_agent import construct_cron_agent

    calls = []
    setup = SimpleNamespace(
        model="unsupported-fixture", runtime={"provider": "unsupported-fixture"}
    )
    with pytest.raises(ValueError, match="unsupported"):
        construct_cron_agent(
            lambda **kw: calls.append(kw),
            {"id": "fixture", "service_tier": "priority"},
            {},
            setup,
            workdir=None,
            session_id="fixture",
            session_db=None,
        )
    assert calls == []


def test_formatted_job_preserves_operator_policy_visibility():
    from tools.cronjob_tools import _format_job

    result = _format_job(
        {"id": "fixture", "service_tier": "priority", "allow_fallbacks": False}
    )
    assert result["service_tier"] == "priority"
    assert result["allow_fallbacks"] is False
