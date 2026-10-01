"""Offline real native child + OpenAI SDK transport control.

This control intentionally does not import NativeWorkerAdapter.  It constructs a
real AIAgent child through the accepted native seams, runs the real child loop
through ``_run_single_child``, and uses only an in-process HTTPX transport.
The first native response asks for one local tool step; the second is the final
response.  Thus the two requests are produced by one logical child, not by two
direct ``run_codex_stream`` calls.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
from openai import OpenAI

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "plugin"))

from external_orchestrator.observation import RequestObservation  # noqa: E402


BASE_URL = "https://api.example.test/v1"
ROUTE = {
    "provider": "openai-codex",
    "model": "native-control-model",
    "base_url": BASE_URL,
    "api_mode": "codex_responses",
}


def _response(model: str, status: str, output=None) -> dict:
    return {
        "id": "native_control_response",
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


def _sse(
    request: httpx.Request, *, model: str, output, delta: str = ""
) -> httpx.Response:
    created = _response(model, "in_progress")
    completed = _response(model, "completed", output=output)
    events = [
        {"type": "response.created", "response": created, "sequence_number": 0},
    ]
    if delta:
        events.append(
            {
                "type": "response.output_text.delta",
                "item_id": "native_control_message",
                "output_index": 0,
                "content_index": 0,
                "delta": delta,
                "logprobs": [],
                "sequence_number": 1,
            }
        )
    events.append(
        {"type": "response.completed", "response": completed, "sequence_number": 2}
    )
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


def _function_call_sse(request: httpx.Request, *, model: str) -> httpx.Response:
    item = {
        "id": "native_control_function_call",
        "type": "function_call",
        "status": "completed",
        "call_id": "native-control-call",
        "name": "terminal",
        "arguments": "{}",
    }
    created = _response(model, "in_progress")
    completed = _response(model, "completed", output=[item])
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
        f"event: {event['type']}\ndata: {json.dumps(event, separators=(',', ':'))}\n\n"
        for event in events
    )
    return httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        content=payload.encode(),
        request=request,
    )


def _parent():
    from run_agent import AIAgent

    return AIAgent(
        api_key="test-key",
        provider=ROUTE["provider"],
        base_url=ROUTE["base_url"],
        api_mode=ROUTE["api_mode"],
        model="native-control-parent",
        max_iterations=4,
        prefill_messages=None,
        enabled_toolsets=None,
        disabled_toolsets=[],
        quiet_mode=True,
        ephemeral_system_prompt="offline native child control",
        log_prefix="[native-control-parent]",
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


def test_real_native_child_sdk_transport_without_adapter(tmp_path, monkeypatch):
    """One real child accounts for both logical SDK steps."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    import model_tools
    from tools.delegate_tool import _run_single_child
    from tools.delegate_tool_results import _build_child_preserving_parent_tools

    monkeypatch.setattr(
        model_tools,
        "handle_function_call",
        lambda *_args, **_kwargs: json.dumps({"output": "local tool result"}),
    )

    requests = []

    def handler(request):
        requests.append(request)
        body = json.loads(request.content)
        if len(requests) == 1:
            return _function_call_sse(request, model=body["model"])
        input_items = body.get("input", [])
        assert any(
            isinstance(item, dict) and item.get("type") == "function_call_output"
            for item in input_items
        )
        message = {
            "id": "native_control_message",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [
                {
                    "type": "output_text",
                    "text": "native child complete",
                    "annotations": [],
                    "logprobs": [],
                }
            ],
        }
        return _sse(
            request,
            model=body["model"],
            output=[message],
            delta="native child complete",
        )

    http_client = httpx.Client(
        transport=httpx.MockTransport(handler),
        event_hooks={"request": [], "response": []},
    )
    client = OpenAI(
        api_key="test-key",
        base_url=BASE_URL,
        max_retries=0,
        http_client=http_client,
    )
    parent = _parent()
    child = None
    try:
        call_proofs = []

        class ControlObservation(RequestObservation):
            def _codex_observation_finish(self, state):
                super()._codex_observation_finish(state)
                call_proofs.append(self.proof())

        collector = ControlObservation(
            {"model": ROUTE["model"], "effort": "high", "tier": "default"}
        )
        child = _build_child_preserving_parent_tools(
            task_index=0,
            goal="use one local tool and then finish",
            context="This is a two-step native child control.",
            toolsets=["terminal"],
            model=ROUTE["model"],
            max_iterations=4,
            task_count=1,
            parent_agent=parent,
            override_provider=ROUTE["provider"],
            override_base_url=ROUTE["base_url"],
            override_api_key="test-key",
            override_api_mode=ROUTE["api_mode"],
            override_request_overrides={
                "reasoning": {"effort": "high"},
                "service_tier": "default",
            },
            routing_cfg={**ROUTE, "effort": "high", "service_tier": "default"},
        )
        child._codex_request_observer = collector

        def request_client(**_kwargs):
            return OpenAI(
                api_key="test-key",
                base_url=BASE_URL,
                max_retries=0,
                http_client=httpx.Client(transport=httpx.MockTransport(handler)),
            )

        child._create_request_openai_client = request_client
        result = _run_single_child(
            0,
            "use one local tool and then finish",
            child=child,
            parent_agent=parent,
        )
        observed_routes = [
            {
                "model": body.get("model"),
                "effort": (body.get("reasoning") or {}).get("effort"),
                "tier": body.get("service_tier"),
            }
            for body in (json.loads(request.content) for request in requests)
        ]
        assert (
            observed_routes
            == [{"model": ROUTE["model"], "effort": "high", "tier": "default"}] * 2
        ), observed_routes
        proof = collector.proof()
    finally:
        if child is not None:
            close = getattr(child, "close", None)
            if callable(close):
                close()
        http_client.close()
        close_parent = getattr(parent, "close", None)
        if callable(close_parent):
            close_parent()

    assert result["status"] == "completed"
    assert len(requests) == 2
    assert len(call_proofs) == 2
    assert all(
        p["counts"] == {"sdk_invocations": 1, "http_exchanges": 1} for p in call_proofs
    )
    assert {
        key: sum(p["counts"][key] for p in call_proofs) for key in proof["counts"]
    } == {"sdk_invocations": 2, "http_exchanges": 2}
    assert proof["route"]["model"] == ROUTE["model"]
