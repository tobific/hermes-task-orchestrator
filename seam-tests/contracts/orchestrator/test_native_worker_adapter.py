"""Contract tests for the external native worker adapter.

The adapter is intentionally exercised through the real native child builder and
runner.  The only transport substitute is an OpenAI client backed by an
in-process httpx.MockTransport; no credential or network is permitted.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import pytest
from openai import OpenAI

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tool_policy"))
from test_guard_lifetime import fixture

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from external_orchestrator.scheduler import (  # noqa: E402
    ExternalScheduler,
    HostOutcome,
    OrchestrationError,
    packet_identity,
)
from external_orchestrator.observation import RequestObservation  # noqa: E402


BASE_URL = "https://api.example.test/v1"
ROUTE = {
    "provider": "openai-codex",
    "model": "gpt-6-luna",
    "base_url": BASE_URL,
    "api_mode": "codex_responses",
    "fallback": False,
    "effort": "high",
    "service_tier": "default",
}


def _adapter_or_skip(parent, route_resolver):
    """Missing implementation must fail the gate, never silently skip it."""
    from external_orchestrator.native_worker import NativeWorkerAdapter

    return NativeWorkerAdapter(parent, route_resolver)


def _response_object(model: str, *, status: str, output=None) -> dict:
    return {
        "id": "native_worker_adapter_response",
        "object": "response",
        "created_at": 1,
        "status": status,
        "error": None,
        "incomplete_details": None,
        "instructions": None,
        "max_output_tokens": None,
        "model": model,
        "output": output or [],
        "parallel_tool_calls": True,
        "previous_response_id": None,
        "prompt_cache_key": None,
        "reasoning": {"effort": "high", "generate_summary": None, "summary": None},
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
    request: httpx.Request, *, model: str, text: str = "native child complete"
) -> httpx.Response:
    message = {
        "id": "native_worker_adapter_message",
        "type": "message",
        "role": "assistant",
        "status": "completed",
        "content": [
            {
                "type": "output_text",
                "text": text,
                "annotations": [],
                "logprobs": [],
            }
        ],
    }
    created = _response_object(model, status="in_progress")
    completed = _response_object(model, status="completed", output=[message])
    events = [
        {"type": "response.created", "response": created, "sequence_number": 0},
        {
            "type": "response.output_text.delta",
            "item_id": "native_worker_adapter_message",
            "output_index": 0,
            "content_index": 0,
            "delta": text,
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


def _mutate_outbound_model(model):
    """Mutate the actual HTTPX request before the native observer sees it."""

    def mutate(request):
        body = json.loads(request.content)
        body["model"] = model
        payload = json.dumps(body, separators=(",", ":")).encode()
        request._content = payload
        request.headers["content-length"] = str(len(payload))

    return mutate


def _client_factory(handler, calls, *, request_mutator=None):
    """Build one SDK client per native request, preserving native close ownership."""
    clients = []

    def factory(*_args, **_kwargs):
        client = OpenAI(
            api_key="test-key",
            base_url=BASE_URL,
            max_retries=0,
            http_client=httpx.Client(
                transport=httpx.MockTransport(handler),
                event_hooks={
                    "request": [request_mutator] if request_mutator else [],
                    "response": [],
                },
            ),
        )
        clients.append(client)
        calls.append(client)
        return client

    class Cleanup:
        def close(self):
            for client in clients:
                client.close()

    return factory, Cleanup()


def _real_parent(client_factory):
    """Construct a real AIAgent using the same constructor path as the child."""
    from run_agent import AIAgent

    parent = AIAgent(
        api_key="test-key",
        provider=ROUTE["provider"],
        base_url=ROUTE["base_url"],
        api_mode=ROUTE["api_mode"],
        model="native-parent-model",
        session_id="native-parent-session",
        max_iterations=4,
        prefill_messages=None,
        enabled_toolsets=None,
        disabled_toolsets=[],
        quiet_mode=True,
        ephemeral_system_prompt="native adapter test parent",
        log_prefix="[native-adapter-parent]",
        platform="subagent",
        skip_context_files=True,
        skip_memory=True,
        clarify_callback=None,
        thinking_callback=None,
        session_db=None,
        parent_session_id=None,
        request_overrides={},
        tool_progress_callback=None,
        iteration_budget=None,
    )
    # The adapter must consume this external seam when constructing the native
    # child's OpenAI client.  It is deliberately not a core AIAgent hook.
    parent._client_factory = client_factory
    parent._native_test_identity = ("not", "from", "the", "packet")
    return parent


def _host_fields():
    import hashlib
    from hermes_constants import get_hermes_home
    profile = str(get_hermes_home().resolve())
    session = "native-parent-session"
    return {"profile": profile, "parent_session_id": session,
            "owner_token": hashlib.sha256((profile + "\0" + session).encode()).hexdigest(),
            "origin": "subagent:" + session}


def _packet(*, task_id="native-task", route=None):
    return {
        "owner_token": "owner-token",
        "run_id": "packet-run",
        "task_id": task_id,
        "generation": 0,
        "attempt": 1,
        "lease_id": "lease-native",
        "profile": "native-profile",
        **_host_fields(),
        "parent_session_id": "native-parent-session",
        "capability_profile": "read-only",
        "write_scope": [],
        "route": dict(route or ROUTE),
        "goal": "complete the native adapter fixture",
        "max_iterations": 4,
        "max_result_chars": 14000,
        "unit_ids": [],
    }


def _close_parent(parent):
    close = getattr(parent, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def test_valid_measured_route_succeeds_through_scheduler_and_native_child(
    fixture, tmp_path
):
    """A real child plus transport observation is accepted as HostOutcome."""
    calls = []
    requests = []

    def handler(request):
        requests.append(request)
        body = json.loads(request.content)
        return _sse_response(request, model=body["model"])

    client_factory, http_client = _client_factory(handler, calls)
    parent = _real_parent(client_factory)
    resolved_profiles = []

    def route_resolver(profile):
        resolved_profiles.append(profile)
        return dict(ROUTE)

    adapter = _adapter_or_skip(parent, route_resolver)
    scheduler = ExternalScheduler(
        data_dir=tmp_path,
        worker=adapter,
        max_global=1,
        per_profile=1,
    )
    scheduler.route_resolver = route_resolver
    args = {
        "owner_token": "owner-token",
        "parent_session_id": "native-parent-session",
        "profile": "native-profile",
        **_host_fields(),
        "capability_profile": "read-only",
        "write_scope": [str(tmp_path)],
        "route": dict(ROUTE),
        "tasks": [
            {"task_id": "native-task", "goal": "complete the native adapter fixture"}
        ],
    }
    try:
        created = scheduler.create_run(args)
        result = scheduler.join(
            {**args, "run_id": created["run_id"], "timeout_seconds": 20}
        )
        assert result["state"] == "SUCCEEDED"
        assert resolved_profiles
        assert len(requests) >= 1
        assert calls

        state = json.loads(scheduler.state_path.read_text(encoding="utf-8"))
        stored = state["tasks"]["native-task"]["result"]
        assert stored["simulated"] is False
        assert stored["route_proof"]["observation_source"] == "sdk-transport"
        assert stored["route_proof"]["observed_at_transport"] is True
        assert stored["route_proof"]["physical_attempts"] >= 1
        for key, value in ROUTE.items():
            assert stored["route_proof"][key] == value
    finally:
        scheduler.close()
        http_client.close()
        _close_parent(parent)


def test_response_label_alone_does_not_prove_outbound_mismatch():
    """A response model label is not evidence that the request used that model."""
    from test_wire_observation import (
        _agent,
        _assert_completed,
        _client,
        _run,
        _sse_response,
    )

    requests = []
    collector = RequestObservation({"model": ROUTE["model"]})
    agent = _agent(model="parent-label", observer=collector)

    def handler(request):
        requests.append(request)
        assert json.loads(request.content)["model"] == ROUTE["model"]
        return _sse_response(request, model="response-label-only")

    client, http_client = _client(handler)
    try:
        _assert_completed(_run(client, agent, model=ROUTE["model"]))
        proof = collector.proof()
    finally:
        http_client.close()

    assert len(requests) == 1
    assert proof["route"]["model"] == ROUTE["model"]


def test_actual_mismatched_route_refuses_instead_of_model_self_attestation(
    fixture, tmp_path
):
    """A changed outbound request is rejected; response labels are not wire proof."""
    requests = []
    outbound_model = "actual-wire-model-not-requested"

    def handler(request):
        requests.append(request)
        body = json.loads(request.content)
        assert body["model"] == outbound_model
        return _sse_response(request, model=body["model"])

    client_factory, http_client = _client_factory(
        handler, [], request_mutator=_mutate_outbound_model(outbound_model)
    )
    parent = _real_parent(client_factory)
    adapter = _adapter_or_skip(parent, lambda _profile: dict(ROUTE))
    scheduler = ExternalScheduler(
        data_dir=tmp_path,
        worker=adapter,
        max_global=1,
        per_profile=1,
    )
    scheduler.route_resolver = lambda _profile: dict(ROUTE)
    args = {
        "owner_token": "owner-token",
        "parent_session_id": "native-parent-session",
        "profile": "native-profile",
        **_host_fields(),
        "capability_profile": "read-only",
        "write_scope": [],
        "route": dict(ROUTE),
        "tasks": [
            {"task_id": "mismatch-task", "goal": "must refuse mismatched transport"}
        ],
    }
    try:
        created = scheduler.create_run(args)
        result = scheduler.join(
            {**args, "run_id": created["run_id"], "timeout_seconds": 20}
        )
        assert requests
        assert result["state"] == "FAILED"
        state = json.loads(scheduler.state_path.read_text(encoding="utf-8"))
        stored = state["tasks"]["mismatch-task"]["result"]
        assert stored["error_classification"] == "POLICY_DENIED"
        assert "observation" in stored["error_message"].lower()
        assert "actual-wire-model-not-requested" not in json.dumps(stored)
    finally:
        scheduler.close()
        http_client.close()
        _close_parent(parent)
        state_path = scheduler.state_path
        if state_path.exists():
            state_path.unlink()
        if scheduler.lock_path.exists():
            scheduler.lock_path.unlink()


def test_caller_identity_is_derived_from_packet_not_parent_state(fixture):
    calls = []

    def handler(request):
        body = json.loads(request.content)
        return _sse_response(request, model=body["model"])

    client_factory, http_client = _client_factory(handler, calls)
    parent = _real_parent(client_factory)
    packet = _packet(task_id="identity-from-packet")
    adapter = _adapter_or_skip(parent, lambda _profile: dict(ROUTE))
    try:
        outcome = adapter(packet)
        assert isinstance(outcome, HostOutcome)
        assert outcome.identity == packet_identity(packet)
        assert outcome.identity != parent._native_test_identity
        assert outcome.observation["observation_source"] == "sdk-transport"
    finally:
        http_client.close()
        _close_parent(parent)


def _function_call_sse_response(
    request: httpx.Request, *, model: str
) -> httpx.Response:
    item = {
        "id": "native_worker_adapter_function_call",
        "type": "function_call",
        "status": "completed",
        "call_id": "native-worker-call",
        "name": "terminal",
        "arguments": "{}",
    }
    created = _response_object(model, status="in_progress")
    completed = _response_object(model, status="completed", output=[item])
    events = [
        {"type": "response.created", "response": created, "sequence_number": 0},
        {"type": "response.output_item.added", "output_index": 0, "item": item},
        {
            "type": "response.function_call_arguments.delta",
            "item_id": item["id"],
            "output_index": 0,
            "delta": "{}",
            "sequence_number": 1,
        },
        {"type": "response.output_item.done", "output_index": 0, "item": item},
        {"type": "response.completed", "response": completed, "sequence_number": 2},
    ]
    payload = "".join(
        f"event: {event['type']}\\ndata: {json.dumps(event, separators=(',', ':'))}\\n\\n"
        for event in events
    )
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        content=payload.encode(),
        request=request,
    )


def test_adapter_counts_all_native_sdk_calls_in_a_real_child(fixture, monkeypatch):
    """The adapter must aggregate both real child calls, not just its last one."""
    import model_tools
    from test_native_child_sdk_control import _function_call_sse as control_sse

    requests = []

    def handler(request):
        requests.append(request)
        model = json.loads(request.content)["model"]
        if len(requests) == 1:
            return control_sse(request, model=model)
        return _sse_response(request, model=model)

    monkeypatch.setattr(
        model_tools,
        "handle_function_call",
        lambda *_a, **_kw: json.dumps({"output": "local control"}),
    )
    factory, cleanup = _client_factory(handler, [])
    parent = _real_parent(factory)
    adapter = _adapter_or_skip(parent, lambda _profile: dict(ROUTE))
    try:
        outcome = adapter(_packet())
        assert isinstance(outcome, HostOutcome)
        assert len(requests) == 2
        assert outcome.observation["physical_attempts"] == 2
        assert outcome.observation["model"] == ROUTE["model"]
    finally:
        cleanup.close()
        _close_parent(parent)


def test_host_validation_still_rejects_unobserved_success_envelopes(fixture, tmp_path):
    """Scheduler authority remains HostOutcome, never worker JSON."""
    packet = _packet()
    scheduler = ExternalScheduler(data_dir=tmp_path, worker=lambda _packet: {})
    try:
        with pytest.raises(
            OrchestrationError,
            match="host-typed transport observation required",
        ):
            scheduler._validate_result(
                packet,
                {
                    "worker_status": "succeeded",
                    "answer": "model self-attested success",
                    "evidence": [],
                    "artifacts": [],
                    "checks": [],
                    "uncertainties": [],
                    "suggested_followups": [],
                    "route_proof": dict(ROUTE),
                },
            )
    finally:
        scheduler.close()
