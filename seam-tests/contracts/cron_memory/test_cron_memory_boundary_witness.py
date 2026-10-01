"""Cron memory boundary witness through the real constructor."""

from types import SimpleNamespace
import pytest


@pytest.fixture
def observed():
    from cron.scheduler_agent import construct_cron_agent

    calls = []

    def factory(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace()

    setup = SimpleNamespace(
        model="fixture-model",
        runtime={},
        max_iterations=1,
        reasoning_config=None,
        prefill_messages=None,
        fallback_model=[],
        credential_pool=None,
    )
    construct_cron_agent(
        factory,
        {"id": "offline-memory-witness"},
        {},
        setup,
        workdir=None,
        session_id="offline-memory-witness",
        session_db=None,
    )
    assert len(calls) == 1
    return calls[0]


def test_constructor_control_retains_cron_identity(observed):
    assert observed["platform"] == "cron"
    assert observed["session_id"] == "offline-memory-witness"


def test_cron_skips_personal_memory(observed):
    assert observed["skip_memory"] is True


def test_cron_disables_memory_toolset(observed):
    assert "memory" in observed["disabled_toolsets"]
