"""Contract tests for the narrow Codex request-observation boundary.

These tests deliberately use the native ``agent.codex_runtime.run_codex_stream``
with a real OpenAI client and an in-process httpx.MockTransport.  They need
the external observer and the opted-in core hook wiring (patch 0004).
"""

from __future__ import annotations

import json
import threading
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import httpx
import pytest
from openai import OpenAI, APIConnectionError

from agent.codex_runtime import run_codex_stream
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))


def RequestObservation(expected):
    from external_orchestrator.observation import RequestObservation as Collector
    return Collector(expected)


BASE_URL = "https://api.example.test/v1"


def _response_object(model: str, *, status: str) -> dict:
    return {
        "id": "resp_observation_test",
        "object": "response",
        "created_at": 1,
        "status": status,
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "max_output_tokens": None,
        "model": model,
        "output": ([{"id": "msg_observation_test", "type": "message", "role": "assistant",
                     "status": "completed", "content": [{"type": "output_text", "text": "ok",
                     "annotations": [], "logprobs": []}]}] if status == "completed" else []),
        "parallel_tool_calls": True,
        "previous_response_id": None,
        "prompt_cache_key": None,
        "reasoning": {"effort": None, "generate_summary": None, "summary": None},
        "safety_identifier": None,
        "service_tier": "default",
        "store": True,
        "temperature": 1.0,
        "text": {"format": {"type": "text"}},
        "tool_choice": "auto",
        "tools": [],
        "top_logprobs": 0,
        "top_p": 1.0,
        "truncation": "disabled",
        "usage": None,
        "user": None,
        "metadata": {},
    }


def _sse_response(
    request: httpx.Request, *, model: str = "wire-model"
) -> httpx.Response:
    created = _response_object(model, status="in_progress")
    completed = _response_object(model, status="completed")
    events = [
        {"type": "response.created", "response": created, "sequence_number": 0},
        {
            "type": "response.output_text.delta",
            "item_id": "msg_observation_test",
            "output_index": 0,
            "content_index": 0,
            "delta": "ok",
            "logprobs": [],
            "sequence_number": 1,
        },
        {"type": "response.completed", "response": completed, "sequence_number": 2},
    ]
    payload = "".join(
        f"event: {event['type']}\ndata: {json.dumps(event, separators=(',', ':'))}\n\n"
        for event in events
    )
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        content=payload.encode(),
        request=request,
    )


def _client(handler, *, max_retries: int = 0, base_url: str = BASE_URL):
    transport = httpx.MockTransport(handler)
    http_client = httpx.Client(transport=transport)
    client = OpenAI(
        api_key="test-key",
        base_url=base_url,
        max_retries=max_retries,
        http_client=http_client,
    )
    return client, http_client


def _agent(*, model: str = "agent-model", observer=None) -> SimpleNamespace:
    agent = SimpleNamespace(
        model=model,
        provider="openai-codex",
        base_url=BASE_URL,
        session_id="",
        is_subagent=False,
        _fallback_index=0,
        _current_api_request_id="observation-test-request",
        _interrupt_requested=False,
        _active_codex_stream_request_token=None,
        _session_db=None,
        show_commentary=False,
        interim_assistant_callback=None,
        _codex_streamed_text_parts=[],
    )
    agent._fire_stream_delta = lambda _text: None
    agent._fire_reasoning_delta = lambda _text: None
    agent._fire_streamed_codex_commentary = lambda _text: None
    agent._touch_activity = lambda _reason: None
    agent._client_log_context = lambda: "observation-test"
    agent._abort_request_openai_client = lambda *_args, **_kwargs: None
    agent._is_codex_backend = lambda: False
    agent._claim_stream_writer = lambda: 1
    agent._stream_writer_is_current = lambda _token: True
    if observer is not None:
        agent._codex_request_observer = observer
    return agent


def _run(client, agent, *, model="wire-model", **request):
    api_kwargs = {"model": model, "input": "safe test prompt", **request}
    return run_codex_stream(agent, api_kwargs, client=client)


def _assert_completed(result):
    assert result.status == "completed"
    assert result.output_text == "ok"


def test_real_sdk_wire_request_produces_route_proof():
    requests = []

    def handler(request):
        requests.append(request)
        return _sse_response(request, model="wire-model")

    collector = RequestObservation({"model": "wire-model"})
    agent = _agent(model="agent-model", observer=collector)
    client, http_client = _client(handler)
    try:
        _assert_completed(_run(client, agent))
        proof = collector.proof()
    finally:
        http_client.close()

    assert len(requests) == 1
    assert proof["route"]["model"] == "wire-model"
    assert proof["counts"] == {"sdk_invocations": 1, "http_exchanges": 1}


def test_wire_model_effective_effort_and_tier_use_sdk_extra_body_precedence():
    bodies = []
    collector = RequestObservation(
        {"model": "wire-model", "effort": "high", "tier": "flex"}
    )
    agent = _agent(model="agent-model", observer=collector)

    def handler(request):
        body = json.loads(request.content)
        bodies.append(body)
        return _sse_response(request, model=body["model"])

    client, http_client = _client(handler)
    try:
        _assert_completed(
            _run(
                client,
                agent,
                reasoning={"effort": "low"},
                service_tier="default",
                extra_body={"reasoning": {"effort": "high"}, "service_tier": "flex"},
            )
        )
        proof = collector.proof()
    finally:
        http_client.close()

    assert bodies[0]["model"] == "wire-model"
    assert bodies[0]["reasoning"]["effort"] == "high"
    assert bodies[0]["service_tier"] == "flex"
    assert proof["route"] == {"model": "wire-model", "effort": "high", "tier": "flex"}


def test_privacy_excludes_poisoned_url_prompt_and_unknown_model_from_receipts():
    poisoned = "route-user:route-password@api.example.test"
    query_secret = "query-secret-should-not-leak"
    prompt_secret = "prompt-secret-should-not-leak"
    collector = RequestObservation({"model": "wire-model"})
    agent = _agent(observer=collector)

    def handler(request):
        assert prompt_secret in request.content.decode()
        return _sse_response(request, model="wire-model")

    client, http_client = _client(
        handler,
        base_url=f"https://{poisoned}/v1?poison={query_secret}",
    )
    try:
        _assert_completed(
            run_codex_stream(
                agent,
                {"model": "wire-model", "input": prompt_secret},
                client=client,
            )
        )
        with pytest.raises(ValueError, match="^request observation proof unavailable$"):
            collector.proof()
        serialized = json.dumps(collector.snapshot(), sort_keys=True)
    finally:
        http_client.close()

    assert poisoned not in serialized
    assert query_secret not in serialized
    assert prompt_secret not in serialized

    unknown_model = "unknown-model-secret-should-not-leak"
    mismatch = RequestObservation({"model": "wire-model"})
    mismatch_agent = _agent(observer=mismatch)
    client, http_client = _client(
        lambda request: _sse_response(request, model=unknown_model)
    )
    try:
        _assert_completed(_run(client, mismatch_agent, model=unknown_model))
        with pytest.raises(ValueError) as exc_info:
            mismatch.proof()
    finally:
        http_client.close()

    assert str(exc_info.value) == "request observation proof unavailable"
    assert unknown_model not in repr(mismatch)


def test_mismatch_followed_by_good_request_cannot_be_masked():
    collector = RequestObservation({"model": "good-model"})
    agent = _agent(observer=collector)

    def handler(request):
        model = json.loads(request.content)["model"]
        return _sse_response(request, model=model)

    client, http_client = _client(handler)
    try:
        _assert_completed(_run(client, agent, model="bad-model"))
        _assert_completed(_run(client, agent, model="good-model"))
        with pytest.raises(ValueError, match="^request observation proof unavailable$"):
            collector.proof()
    finally:
        http_client.close()


def test_sdk_retry_counts_distinct_http_exchanges_without_claiming_two_invocations():
    attempts = []
    collector = RequestObservation({"model": "wire-model"})
    agent = _agent(observer=collector)

    def handler(request):
        attempts.append(request)
        if len(attempts) == 1:
            return httpx.Response(
                500,
                json={"error": {"message": "temporary", "type": "server_error"}},
                request=request,
            )
        return _sse_response(request, model="wire-model")

    client, http_client = _client(handler, max_retries=1)
    try:
        _assert_completed(_run(client, agent))
        proof = collector.proof()
    finally:
        http_client.close()

    assert len(attempts) == 2
    assert proof["counts"] == {"sdk_invocations": 1, "http_exchanges": 2}


def test_without_observer_native_stream_and_wire_body_are_unchanged():
    bodies = []

    def handler(request):
        bodies.append(json.loads(request.content))
        return _sse_response(request, model="wire-model")

    agent = _agent()
    client, http_client = _client(handler)
    try:
        result = _run(client, agent)
    finally:
        http_client.close()

    _assert_completed(result)
    assert not hasattr(agent, "_codex_request_observer")
    assert bodies[0]["model"] == "wire-model"
    assert bodies[0]["input"] == "safe test prompt"


def test_preexisting_hooks_survive_and_shared_client_observers_are_isolated():
    hook_lock = threading.Lock()
    preexisting = {"request": 0, "response": 0}
    seen_models = []

    def old_request_hook(_request):
        with hook_lock:
            preexisting["request"] += 1

    def old_response_hook(_response):
        with hook_lock:
            preexisting["response"] += 1

    def handler(request):
        model = json.loads(request.content)["model"]
        with hook_lock:
            seen_models.append(model)
        return _sse_response(request, model=model)

    http_client = httpx.Client(
        transport=httpx.MockTransport(handler),
        event_hooks={"request": [old_request_hook], "response": [old_response_hook]},
    )
    client = OpenAI(api_key="test-key", base_url=BASE_URL, http_client=http_client)
    collectors = {
        "left-model": RequestObservation({"model": "left-model"}),
        "right-model": RequestObservation({"model": "right-model"}),
    }
    agents = {
        model: _agent(observer=collector) for model, collector in collectors.items()
    }

    def invoke(model):
        result = _run(client, agents[model], model=model)
        _assert_completed(result)
        return collectors[model].proof()

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            proofs = list(pool.map(invoke, ("left-model", "right-model")))
    finally:
        http_client.close()

    assert sorted(seen_models) == ["left-model", "right-model"]
    assert preexisting == {"request": 2, "response": 2}
    assert {proof["route"]["model"] for proof in proofs} == {
        "left-model",
        "right-model",
    }
    assert all(
        proof["counts"] == {"sdk_invocations": 1, "http_exchanges": 1}
        for proof in proofs
    )


def test_short_circuit_without_sdk_call_fails_closed():
    requests = []
    collector = RequestObservation({"model": "wire-model"})
    agent = _agent(observer=collector)
    agent._interrupt_requested = True

    def handler(request):
        requests.append(request)
        return _sse_response(request)

    client, http_client = _client(handler)
    try:
        with pytest.raises(InterruptedError):
            _run(client, agent)
        with pytest.raises(ValueError, match="^request observation proof unavailable$"):
            collector.proof()
    finally:
        http_client.close()

    assert requests == []


def test_observer_callback_failure_does_not_change_native_result():
    calls = []

    def broken_observer(event):
        calls.append(event)
        raise RuntimeError("observer failure must be contained")

    agent = _agent(observer=broken_observer)
    requests = []

    def handler(request):
        requests.append(request)
        return _sse_response(request)

    client, http_client = _client(handler)
    try:
        _assert_completed(_run(client, agent))
    finally:
        http_client.close()

    assert len(requests) == 1
    assert calls


def test_inflight_interrupt_preserves_native_sdk_error_and_fails_closed():
    collector = RequestObservation({"model": "wire-model"})
    agent = _agent(observer=collector)
    requests = []

    def interrupt_after_request(_request):
        if not requests:
            agent._interrupt_requested = True

    def handler(request):
        requests.append(request)
        raise httpx.RemoteProtocolError(
            "fixture transport interruption", request=request
        )

    http_client = httpx.Client(
        transport=httpx.MockTransport(handler),
        event_hooks={"request": [interrupt_after_request]},
    )
    client = OpenAI(api_key="test-key", base_url=BASE_URL, http_client=http_client, max_retries=0)
    try:
        with pytest.raises(APIConnectionError):
            _run(client, agent)
        with pytest.raises(ValueError, match="^request observation proof unavailable$"):
            collector.proof()
    finally:
        http_client.close()

    assert len(requests) == 1
