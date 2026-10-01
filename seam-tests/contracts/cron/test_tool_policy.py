"""Exercise policy fields through actual tool dispatch and temporary job storage."""

import json
import pytest


@pytest.fixture
def invoke(monkeypatch):
    import cron.jobs as jobs
    import cron.scheduler as scheduler
    import tools.cronjob_tools as tool

    # Keep actual jobs.json storage; replace only live scheduler registration.
    monkeypatch.setattr(
        scheduler, "create_job_with_scheduler_registration", jobs.create_job
    )
    monkeypatch.setattr(tool, "_gateway_liveness_notice", lambda: {})

    def call(**args):
        return json.loads(tool._cronjob_handler(args))

    return call


def create(invoke, **fields):
    result = invoke(
        action="create",
        prompt="offline policy fixture",
        schedule="every 1h",
        deliver="local",
        paused=True,
        **fields,
    )
    assert result["success"], result
    return result["job_id"]


def test_model_tool_create_persists_explicit_policy(invoke):
    from cron.jobs import get_job

    identity = create(invoke, service_tier="priority", allow_fallbacks=False)
    job = get_job(identity)
    assert job.get("service_tier") == "priority"
    assert job.get("allow_fallbacks") is False


def test_model_tool_update_clear_and_omission(invoke):
    from cron.jobs import get_job

    identity = create(invoke, service_tier="priority", allow_fallbacks=False)
    assert invoke(
        action="update", job_id=identity, service_tier="normal", allow_fallbacks=True
    )["success"]
    assert get_job(identity).get("service_tier") == "normal"
    assert get_job(identity).get("allow_fallbacks") is True
    assert invoke(action="update", job_id=identity, name="unchanged policy")["success"]
    assert get_job(identity).get("service_tier") == "normal"
    assert invoke(action="update", job_id=identity, service_tier="")["success"]
    assert get_job(identity).get("service_tier") is None


@pytest.mark.parametrize(
    "fields",
    [{"service_tier": "urgent"}, {"allow_fallbacks": "false"}, {"allow_fallbacks": 0}],
)
def test_invalid_policy_cannot_mutate_job(invoke, fields):
    from cron.jobs import get_job

    identity = create(invoke)
    before = get_job(identity)
    result = invoke(action="update", job_id=identity, name="must not commit", **fields)
    assert result["success"] is False, result
    assert get_job(identity) == before


def test_schema_and_dispatch_keep_inference_pins_user_owned(invoke):
    from cron.jobs import get_job
    from tools.cronjob_tools import CRONJOB_SCHEMA

    props = CRONJOB_SCHEMA["parameters"]["properties"]
    assert props["service_tier"]["type"] == "string"
    assert props["allow_fallbacks"]["type"] == "boolean"
    assert not {"model", "provider", "base_url"} & props.keys()
    identity = create(
        invoke,
        model="untrusted-model",
        provider="untrusted-provider",
        base_url="https://example.invalid",
        service_tier="normal",
    )
    job = get_job(identity)
    assert not job.get("model") and not job.get("provider") and not job.get("base_url")
    assert job.get("service_tier") == "normal"
