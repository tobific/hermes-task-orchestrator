"""Fail-closed, privacy-preserving observation of native Codex HTTP exchanges.

This module is deliberately an evidence gate rather than a request recorder.  The
native runtime supplies already-transformed HTTPX ``Request``/``Response`` event
objects; this collector retains only matched expected route values, opaque field
names, and numeric diagnostics.
"""

from __future__ import annotations

from collections.abc import Mapping
from threading import RLock
from typing import Any
from urllib.parse import urlsplit

_ALLOWED_FIELDS = ("model", "effort", "tier", "endpoint")
_FAILURE = "request observation proof unavailable"
_MISSING = object()


def _safe_endpoint(value: Any) -> str | None:
    """Return a query/userinfo/fragment-free URL, without retaining unsafe input."""
    try:
        text = str(value)
        parsed = urlsplit(text)
        # Accessing username/password can itself reject malformed netlocs.
        if (
            not parsed.scheme
            or not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
            or "?" in text
            or "#" in text
            or parsed.path not in {"/responses", "/v1/responses", "/backend-api/codex/responses"}
        ):
            return None
        return text
    except Exception:
        return None


def _wire_route(request: Any) -> tuple[dict[str, Any], set[str], bool]:
    """Extract route scalars from a transformed request without retaining its body."""
    route: dict[str, Any] = {}
    mismatches: set[str] = set()
    try:
        endpoint = _safe_endpoint(getattr(request, "url", _MISSING))
        if endpoint is None:
            mismatches.add("endpoint")
        else:
            route["endpoint"] = endpoint
    except Exception:
        mismatches.add("endpoint")

    try:
        content = getattr(request, "content")
        if isinstance(content, str):
            import json

            body = json.loads(content)
        elif isinstance(content, (bytes, bytearray, memoryview)):
            import json

            body = json.loads(bytes(content))
        else:
            body = None
        if not isinstance(body, dict):
            mismatches.update(("model", "effort", "tier"))
            return route, mismatches, False
    except Exception:
        mismatches.update(("model", "effort", "tier"))
        return route, mismatches, False

    route["model"] = body.get("model") if isinstance(body.get("model"), str) else None
    reasoning = body.get("reasoning")
    route["effort"] = reasoning.get("effort") if isinstance(reasoning, dict) else None
    route["tier"] = body.get("service_tier")
    return route, mismatches, True


class RequestObservation:
    """Collect safe route evidence from native runtime observation events.

    ``RequestObservation(expected_route)`` is assigned to
    ``agent._codex_request_observer`` for one native call.  The runtime invokes
    the callable with the DESIGN.md ``response``/``error`` event protocol and
    privately brackets the call with lifecycle metadata.  ``proof()`` is useful
    diagnostics; a scheduler adapter must additionally call the external adapter's
    strict ``get_codex_route_evidence(agent, full_expected_route)`` helper, which
    checks observer ownership and completion before accepting host route evidence.
    """

    def __init__(self, expected_route: Mapping[str, Any] | None):
        expected_route = expected_route if isinstance(expected_route, Mapping) else {}
        self._expected: dict[str, Any] = {}
        self._invalid_expected = False
        for key, value in expected_route.items():
            if key not in _ALLOWED_FIELDS:
                self._invalid_expected = True
                continue
            if key == "endpoint":
                safe = _safe_endpoint(value)
                if safe is None:
                    self._invalid_expected = True
                else:
                    self._expected[key] = safe
            elif value is None or isinstance(value, (str, int, float, bool)):
                self._expected[key] = value
            else:
                self._invalid_expected = True

        self._lock = RLock()
        self._generation = 0
        self._core_lifecycle = False
        self._active_started = False
        self._active_finished = False
        self._active_native_completed = False
        self._active_state_id: int | None = None
        self._active_owner_token = None
        self._active_callback_failed = False
        self._active_missing_callbacks = 0
        self._active_sdk_invocations = 0
        self._active_http_exchanges = 0
        self._active_response_events = 0
        self._active_failed_exchanges = 0
        self._active_exchange_keys: set[tuple[int, int]] = set()
        self._active_sdk_ids: set[int] = set()
        self._matched_route: dict[str, Any] = {}
        self._mismatch_fields: set[str] = set()
        self._callback_incomplete = False

    def __repr__(self) -> str:
        # Do not let the default repr expose expected route values.
        return "<RequestObservation>"

    def _start_direct_if_needed(self) -> None:
        if not self._active_started:
            self._generation += 1
            self._active_started = True
            self._active_finished = False
            self._active_native_completed = False
            self._active_state_id = None
            self._active_callback_failed = False
            self._active_missing_callbacks = 0
            self._active_sdk_invocations = 0
            self._active_http_exchanges = 0
            self._active_response_events = 0
            self._active_failed_exchanges = 0
            self._active_exchange_keys = set()
            self._active_sdk_ids = set()
            self._matched_route = {}

    def _codex_observation_begin(self, state: Any) -> None:
        """Private runtime lifecycle hook; invalidates evidence from an older call."""
        with self._lock:
            self._core_lifecycle = True
            self._generation += 1
            self._active_started = True
            self._active_finished = False
            self._active_native_completed = False
            self._active_state_id = id(state)
            self._active_owner_token = getattr(state, "owner_token", None)
            self._active_callback_failed = False
            self._active_missing_callbacks = 0
            self._active_sdk_invocations = 0
            self._active_http_exchanges = 0
            self._active_response_events = 0
            self._active_failed_exchanges = 0
            self._active_exchange_keys = set()
            self._active_sdk_ids = set()
            self._matched_route = {}

    def _codex_observation_finish(self, state: Any) -> None:
        """Private runtime lifecycle hook; records only safe completion metadata."""
        with self._lock:
            self._start_direct_if_needed()
            if self._active_state_id is not None and self._active_state_id != id(state):
                self._callback_incomplete = True
                return
            self._active_state_id = id(state)
            self._active_finished = True
            self._active_native_completed = bool(
                getattr(state, "native_completed", False)
            )
            self._active_callback_failed = bool(
                getattr(state, "callback_failed", False)
            )
            self._active_missing_callbacks = max(
                self._active_missing_callbacks,
                int(getattr(state, "missing_callbacks", 0) or 0),
            )
            self._active_sdk_invocations = max(
                self._active_sdk_invocations,
                int(getattr(state, "sdk_invocations", 0) or 0),
            )
            self._active_http_exchanges = max(
                self._active_http_exchanges,
                int(getattr(state, "response_events", 0) or 0),
            )
            if bool(getattr(state, "callback_failed", False)):
                self._callback_incomplete = True

    def _compare_route(self, route: dict[str, Any], unsafe: set[str]) -> None:
        self._mismatch_fields.update(unsafe)
        for key, expected in self._expected.items():
            observed = route.get(key, _MISSING)
            if observed is not _MISSING and observed == expected and key not in unsafe:
                self._matched_route[key] = expected
            else:
                self._mismatch_fields.add(key)
                self._matched_route.pop(key, None)

    def __call__(self, event: Mapping[str, Any]) -> None:
        """Consume one safe response/error event; observer exceptions stay outside native flow."""
        with self._lock:
            if (not self._active_started or self._active_finished
                or not isinstance(event, Mapping)
                or event.get("owner_token") is not self._active_owner_token
                or self._active_owner_token is None):
                self._callback_incomplete = True
                return
            phase = event.get("phase")
            if phase not in {"request", "response", "error"}:
                self._callback_incomplete = True
                return

            sdk_id = event.get("sdk_invocation")
            exchange = event.get("http_exchange")
            attempt = event.get("attempt")
            if (
                isinstance(sdk_id, bool)
                or not isinstance(sdk_id, int)
                or sdk_id < 1
                or isinstance(exchange, bool)
                or not isinstance(exchange, int)
                or exchange < 1
                or isinstance(attempt, bool)
                or not isinstance(attempt, int)
                or attempt < 1
            ):
                self._callback_incomplete = True
                return

            request = event.get("request")
            if request is None:
                self._callback_incomplete = True
                return
            if phase == "request":
                route, unsafe, parsed = _wire_route(request)
                self._compare_route(route, unsafe)
                if not parsed:
                    self._callback_incomplete = True
                return
            response = event.get("response", _MISSING)
            if phase == "response":
                if (
                    response is None
                    or getattr(response, "request", _MISSING) is not request
                ):
                    self._callback_incomplete = True
                    self._mismatch_fields.add("response_request")
                    return
            elif response is not None:
                self._callback_incomplete = True
                return

            exchange_key = (sdk_id, exchange)
            if exchange_key in self._active_exchange_keys:
                self._callback_incomplete = True
                return
            self._active_exchange_keys.add(exchange_key)
            self._active_sdk_ids.add(sdk_id)
            self._active_sdk_invocations = max(
                self._active_sdk_invocations, len(self._active_sdk_ids)
            )


            route, unsafe, parsed = _wire_route(request)
            self._compare_route(route, unsafe)
            if not parsed:
                self._callback_incomplete = True
            if phase == "response":
                self._active_response_events += 1
                self._active_http_exchanges = self._active_response_events
            else:
                self._active_failed_exchanges += 1

    def _failure(self) -> ValueError:
        return ValueError(_FAILURE)

    def proof(self) -> dict[str, Any]:
        """Return matched route proof, or one generic error on any incomplete evidence."""
        with self._lock:
            if self._invalid_expected:
                raise self._failure()
            if not self._active_started:
                raise self._failure()
            if self._core_lifecycle and (
                not self._active_finished or not self._active_native_completed
            ):
                raise self._failure()
            if (
                self._callback_incomplete
                or self._active_callback_failed
                or self._active_missing_callbacks
                or not self._active_sdk_invocations
                or not self._active_http_exchanges
                or not self._active_response_events
                or self._mismatch_fields
            ):
                raise self._failure()
            if set(self._matched_route) != set(self._expected):
                raise self._failure()
            return {
                "route": {key: self._matched_route[key] for key in self._expected},
                "counts": {
                    "sdk_invocations": self._active_sdk_invocations,
                    "http_exchanges": self._active_http_exchanges,
                },
            }

    def snapshot(self) -> dict[str, Any]:
        """Return safe diagnostics; no request, body, header, URL, or exception text is exposed."""
        with self._lock:
            return {
                "route": dict(self._matched_route),
                "counts": {
                    "sdk_invocations": self._active_sdk_invocations,
                    "http_exchanges": self._active_http_exchanges,
                },
                "completed": self._active_native_completed,
                "response_events": self._active_response_events,
                "failed_exchanges": self._active_failed_exchanges,
                "callback_failed": self._active_callback_failed,
                "missing_callbacks": self._active_missing_callbacks,
                "mismatch_fields": sorted(self._mismatch_fields),
                "proof_available": self._proof_available_unlocked(),
            }

    def _proof_available_unlocked(self) -> bool:
        return bool(
            self._active_started
            and (
                not self._core_lifecycle
                or (self._active_finished and self._active_native_completed)
            )
            and not self._invalid_expected
            and not self._callback_incomplete
            and not self._active_callback_failed
            and not self._active_missing_callbacks
            and self._active_sdk_invocations
            and self._active_http_exchanges
            and self._active_response_events
            and not self._mismatch_fields
            and set(self._matched_route) == set(self._expected)
        )


def _observation_expected_route(expected_route: Any) -> dict[str, Any] | None:
    if not isinstance(expected_route, Mapping) or set(
        expected_route
    ) != set(_ALLOWED_FIELDS):
        return None
    route: dict[str, Any] = {}
    for key in _ALLOWED_FIELDS:
        value = expected_route[key]
        if key == "endpoint":
            if _safe_endpoint(value) is None:
                return None
            route[key] = str(value)
        elif value is None or isinstance(value, (str, int, float, bool)):
            route[key] = value
        else:
            return None
    return route


def get_codex_route_evidence(
    agent: Any, expected_route: Mapping[str, Any]
) -> dict[str, Any]:
    """Strict host integration API for a complete four-field route proof.

    The adapter must pass exactly ``model``, ``effort``, ``tier`` and
    ``endpoint``.  This checks that the current agent still owns the exact
    observer state, the native call completed, every HTTP callback was
    delivered, and a response was observed.  The returned data proves only
    SDK invocation/HTTP exchange observation; it does not prove remote model
    execution or server-side tier processing.
    """
    expected = _observation_expected_route(expected_route)
    state = getattr(agent, "_codex_request_observation_state", None)
    observer = getattr(agent, "_codex_request_observer", None)
    if (
        expected is None
        or state is None
        or getattr(state, "observer", _MISSING) is not observer
        or not getattr(state, "native_completed", False)
        or getattr(state, "native_failed", True)
        or getattr(state, "callback_failed", True)
        or getattr(state, "missing_callbacks", 1)
        or not getattr(state, "sdk_invocations", 0)
        or not getattr(state, "http_exchanges", 0)
        or not getattr(state, "response_events", 0)
        or getattr(state, "http_exchanges", 0) != getattr(state, "response_events", 0)
        or getattr(state, "pending", ())
        or any(type(getattr(state, key, None)) is not int for key in
               ("sdk_invocations", "http_exchanges", "response_events", "missing_callbacks"))
    ):
        raise ValueError(_FAILURE)
    proof_fn = getattr(observer, "proof", None)
    if not callable(proof_fn):
        raise ValueError(_FAILURE)
    try:
        proof = proof_fn()
    except BaseException:
        raise ValueError(_FAILURE) from None
    if not isinstance(proof, dict):
        raise ValueError(_FAILURE)
    route = proof.get("route")
    counts = proof.get("counts")
    if not isinstance(route, dict) or set(route) != set(_ALLOWED_FIELDS):
        raise ValueError(_FAILURE)
    if any(route[key] != expected[key] for key in _ALLOWED_FIELDS):
        raise ValueError(_FAILURE)
    if (
        not isinstance(counts, dict)
        or set(counts) != {"sdk_invocations", "http_exchanges"}
        or isinstance(counts.get("sdk_invocations"), bool)
        or not isinstance(counts.get("sdk_invocations"), int)
        or isinstance(counts.get("http_exchanges"), bool)
        or not isinstance(counts.get("http_exchanges"), int)
        or counts["sdk_invocations"] < 1
        or counts["http_exchanges"] < 1
        or counts["sdk_invocations"] != state.sdk_invocations
        or counts["http_exchanges"] != state.response_events
    ):
        raise ValueError(_FAILURE)
    return {
        "route": {key: expected[key] for key in _ALLOWED_FIELDS},
        "counts": {
            "sdk_invocations": counts["sdk_invocations"],
            "http_exchanges": counts["http_exchanges"],
        },
    }



__all__ = ["RequestObservation", "get_codex_route_evidence"]
