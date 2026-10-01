"""Missing capability probe, not an upstream defect or live provider test.

EXPECTED TO FAIL (marked xfail, strict): it asks the native Codex stream for an
``agent._last_transport_observation`` attribute that neither upstream nor patch 0004
provides. Patch 0004 exposes request observation differently (an attached
``_codex_request_observer``; see ``observation.py``), and that path is covered by
``seam-tests/.../test_native_worker_adapter.py``. Kept as a record of the gap.

Real native streaming code calls a deterministic SDK-boundary test double.
The retained worker contract needs host proof of that request, not agent config.
"""

from types import SimpleNamespace

import pytest


@pytest.mark.xfail(strict=True, reason="probe for an attribute no patch provides; see module docstring")
def test_native_terminal_sdk_call_exposes_host_route_observation():
    from agent.codex_runtime import run_codex_stream

    calls = []
    response = SimpleNamespace(
        id="offline-response",
        status="completed",
        output=[],
        model="gpt-6-luna",
        usage=None,
        error=None,
        incomplete_details=None,
    )

    class Stream:
        def __iter__(self):
            yield SimpleNamespace(type="response.completed", response=response)

        def close(self):
            pass

    class Responses:
        def create(self, **kwargs):
            calls.append(kwargs)
            return Stream()

    client = SimpleNamespace(
        base_url="https://chatgpt.com/backend-api/codex", responses=Responses()
    )
    agent = SimpleNamespace(
        model="gpt-6-astra",
        provider="openai-codex",
        api_mode="codex_responses",
        base_url="https://chatgpt.com/backend-api/codex",
        session_id="",
        _interrupt_requested=False,
        _current_api_request_id="offline-request",
        _fallback_index=0,
        _fallback_activated=False,
        is_subagent=True,
        _fire_stream_delta=lambda _: None,
        _fire_reasoning_delta=lambda _: None,
        _touch_activity=lambda _: None,
        _client_log_context=lambda: "offline fixture",
    )
    result = run_codex_stream(
        agent,
        {
            "model": "gpt-6-luna",
            "reasoning": {"effort": "xhigh"},
            "service_tier": "normal",
        },
        client=client,
    )
    assert result.status == "completed"
    assert len(calls) == 1 and calls[0]["model"] == "gpt-6-luna"
    observed = getattr(agent, "_last_transport_observation", None)
    assert isinstance(observed, dict), (
        "Native SDK call finished but no host route proof is exposed"
    )
    assert observed["model"] == calls[0]["model"] != agent.model
    assert observed["physical_attempts"] == 1
    assert observed["observation_source"] == "codex_responses.create"
