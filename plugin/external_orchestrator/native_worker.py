"""External native child adapter. Host metadata never comes from model output."""

from __future__ import annotations

from contextvars import copy_context
from copy import deepcopy
from threading import RLock
from urllib.parse import urlsplit

from .observation import RequestObservation, get_codex_route_evidence
from .scheduler import (
    ExternalScheduler,
    HostFailure,
    HostOutcome,
    LUNA_MODEL,
    OrchestrationError,
    _validate_worker_packet,
    packet_identity,
)


class _CallObservation:
    """Accumulate completed logical calls without changing single-call semantics."""

    def __init__(self, expected, packet=None):
        self.packet = deepcopy(packet)
        self.tier_decision = None
        self._tier_captured = False
        self.expected = dict(expected)
        self.agent = None
        self.lock = RLock()
        self.current = None
        self.state = None
        self.proofs = []
        self.failed = False
        self.sealed = False

    def _codex_observation_begin(self, state):
        with self.lock:
            if self.sealed or self.current is not None:
                self.failed = True
                return
            if self.packet is not None:
                from .tier_authority import authorized_tier

                decision = getattr(self.agent, "_service_tier_decision", None)
                if self._tier_captured and decision is not self.tier_decision:
                    self.failed = True
                    return
                tier = authorized_tier(self.packet, decision)
                self.tier_decision = decision
                self._tier_captured = True
                self.expected["tier"] = (
                    None if decision is not None and decision.tier == "normal" else tier
                )
            self.state = state
            self.current = RequestObservation(self.expected)
            self.current._codex_observation_begin(state)

    def __call__(self, event):
        with self.lock:
            if self.sealed or self.current is None:
                self.failed = True
                return
            self.current(event)

    def _codex_observation_finish(self, state):
        with self.lock:
            if self.sealed or self.current is None or state is not self.state:
                self.failed = True
                return
            try:
                self.current._codex_observation_finish(state)
                self.proofs.append(get_codex_route_evidence(self.agent, self.expected))
            except Exception:
                self.failed = True
            finally:
                self.current = None
                self.state = None

    def proof(self):
        with self.lock:
            if self.current is None or self.failed or self.sealed:
                raise ValueError("request observation proof unavailable")
            return self.current.proof()

    def finish(self):
        with self.lock:
            if (
                self.sealed
                or self.failed
                or self.current is not None
                or not self.proofs
            ):
                self.sealed = True
                raise ValueError("request observation proof unavailable")
            self.sealed = True
            return {
                "logical_calls": len(self.proofs),
                "sdk_invocations": sum(
                    p["counts"]["sdk_invocations"] for p in self.proofs
                ),
                "http_exchanges": sum(
                    p["counts"]["http_exchanges"] for p in self.proofs
                ),
            }


class NativeWorkerAdapter:
    """Bind one host parent, route resolver and its captured policy lifetime."""

    def __init__(
        self, parent, route_resolver, admission_policy=None, worker_policy=None
    ):
        if not callable(route_resolver):
            raise TypeError("host route resolver required")
        from hashlib import sha256
        from hermes_constants import get_hermes_home

        session = getattr(parent, "session_id", None)
        if not isinstance(session, str) or not session:
            raise ValueError("trusted host session required")
        profile = str(get_hermes_home().resolve())
        self._binding = (
            profile,
            session,
            sha256((profile + "\0" + session).encode()).hexdigest(),
        )
        self.parent = parent
        self.route_resolver = route_resolver
        self.admission_policy = admission_policy
        self.worker_policy = worker_policy
        self.context = copy_context()
        self._revoked = set()
        self._claimed = set()
        self._children = {}
        self._authorities = {}
        self._revocation_lock = RLock()
        register = getattr(parent, "register_close_callback", None)
        if callable(register):
            from weakref import ref

            adapter_ref = ref(self)

            def parent_closed():
                adapter = adapter_ref()
                if adapter is not None:
                    with adapter._revocation_lock:
                        identities = tuple(adapter._claimed)
                    for identity in identities:
                        adapter.revoke(identity, "parent hard closed")

            register(parent_closed)

    def _assert_codex_parent(self):
        """Fail closed unless the bound live parent is still genuinely on Codex.

        The adapter holds the live parent object, which can change route in place
        (automatic fallback, /model switch). Luna children take the parent's
        CURRENT credential, so no Luna work may be admitted or built while the
        parent is on another provider. This is a reversible hold: the task stays
        queued and proceeds once the same parent is back on Codex.
        """
        if (
            getattr(self.parent, "provider", None) != "openai-codex"
            or getattr(self.parent, "api_mode", None) != "codex_responses"
        ):
            from .admission_policy import AdmissionBlocked

            raise AdmissionBlocked("bound parent is not currently on openai-codex")

    def _assert_current_host(self, packet):
        """Re-read captured host authority around slow quota I/O."""
        self._assert_codex_parent()
        if (
            tuple(
                packet.get(k) for k in ("profile", "parent_session_id", "owner_token")
            )
            != self._binding
        ):
            raise ValueError("host identity mismatch")
        if getattr(self.parent, "is_closed", False):
            raise ValueError("host hard closed")
        if getattr(self.parent, "session_id", None) != self._binding[1]:
            raise ValueError("host session changed")
        from hermes_constants import get_hermes_home

        if str(get_hermes_home().resolve()) != self._binding[0]:
            raise ValueError("host profile changed")

    def _check_worker_policy(self, packet):
        if self.worker_policy is None:
            return
        _validate_worker_packet(packet, self.worker_policy())

    def admission_preflight(self, packet):
        """Read host controls freshly in the captured profile context."""
        if self.admission_policy is None:
            return

        def check():
            self._assert_current_host(packet)
            import inspect

            if self.admission_policy is not None:
                if "effort" in inspect.signature(self.admission_policy).parameters:
                    self.admission_policy(
                        self._binding[0],
                        effort=(packet.get("route") or {}).get("effort"),
                    )
                else:
                    self.admission_policy(self._binding[0])

        self.context.copy().run(check)

    def quota_preflight(self, packet):
        """Fetch quota in the captured parent context without holding scheduler locks."""
        packet = deepcopy(packet)

        def check():
            self._assert_current_host(packet)
            from .quota_gate import QuotaAdmissionError, enforce_quota

            try:
                enforce_quota(self._binding[0])
            except QuotaAdmissionError:
                raise
            except Exception as exc:
                raise QuotaAdmissionError("quota status unavailable") from exc
            self._assert_current_host(packet)

        try:
            return self.context.copy().run(check)
        except OrchestrationError:
            raise
        except ValueError as exc:
            from .quota_gate import QuotaAdmissionError

            if isinstance(exc, QuotaAdmissionError):
                raise
            raise OrchestrationError(
                "captured host context unavailable after quota check"
            ) from exc

    def revoke(self, identity, reason):
        with self._revocation_lock:
            self._revoked.add(identity)
            child = self._children.get(identity)
            authority = self._authorities.get(identity)
        if authority is not None:
            authority.revoke(str(reason))
        if child is not None:
            from tools.delegate_tool_child_run import _signal_child_stop

            _signal_child_stop(child, str(reason))

    def __call__(self, packet):
        # Each invocation gets a distinct context copy; captured policy objects
        # retain revocation across threads. Never reuse an entered Context.
        packet = deepcopy(packet)
        identity = packet_identity(packet)
        with self._revocation_lock:
            if identity in self._claimed:
                return HostFailure(
                    identity, "POLICY_DENIED", "attempt already admitted"
                )
            self._claimed.add(identity)
        from .host_usage import get_usage_sink

        # Carry only the new host-owned observation callback. Admission policy
        # still executes in the original captured context, without rebinding it.
        return self.context.copy().run(self._run, packet, get_usage_sink())

    def _run(self, packet, usage_sink=None):
        from .admission_policy import AdmissionBlocked
        from .scheduler import HostDeferred

        identity = packet_identity(packet)
        try:
            with self._revocation_lock:
                if identity in self._revoked:
                    raise ValueError("attempt revoked")
            if ExternalScheduler._deadline_expired(packet):
                return HostFailure(
                    identity,
                    "DEADLINE_EXPIRED",
                    "queue deadline expired before native execution",
                    retryable=False,
                )
            self.admission_preflight(packet)
            from agent.required_tool_policy import required_policy_error

            if required_policy_error("delegate_task", packet) is not None:
                raise ValueError("captured host policy denied worker admission")
            self._assert_current_host(packet)
            route = dict(self.route_resolver(self._binding[0]))
            keys = (
                "provider",
                "model",
                "base_url",
                "api_mode",
                "effort",
                "service_tier",
                "fallback",
            )
            if any(route.get(k) != packet["route"].get(k) for k in keys):
                raise ValueError("host route mismatch")
            if (
                route.get("provider") != "openai-codex"
                or route.get("api_mode") != "codex_responses"
                or route.get("fallback") is not False
            ):
                raise ValueError("unsupported route")
            endpoint = urlsplit(route["base_url"])
            if (
                endpoint.scheme != "https"
                or not endpoint.hostname
                or endpoint.username
                or endpoint.password
                or endpoint.query
                or endpoint.fragment
            ):
                raise ValueError("unsafe endpoint")
            if route.get("model") != LUNA_MODEL:
                raise ValueError("unsupported worker model")
            if route.get("effort") not in {
                "low",
                "medium",
                "high",
                "xhigh",
            } or route.get("service_tier") not in {"default", "priority"}:
                raise ValueError("unsupported request policy")
            if packet.get("unit_ids"):
                raise ValueError("grouped units require structured result integration")
            iterations = packet.get("max_iterations", 4)
            if type(iterations) is not int or not 1 <= iterations <= 500:
                raise ValueError("invalid iteration bound")
            self._check_worker_policy(packet)
            goal = packet["goal"]
            if not isinstance(goal, str) or not goal:
                raise ValueError("missing goal")
            context = packet.get("context", "")
            acceptance = packet.get("acceptance", "")
            if any(
                not isinstance(v, str) or len(v) > 16_000 for v in (context, acceptance)
            ):
                raise ValueError("invalid task context or acceptance")
            from .scheduler import worker_request_fields

            goal, context = worker_request_fields(packet)
            from .quota_gate import (
                QuotaAdmissionError,
                enforce_quota,
                quota_block_message,
            )

            try:
                # This is a second check in the captured Context, immediately
                # before native child construction and any SDK request.
                enforce_quota(self._binding[0])
            except QuotaAdmissionError as exc:
                return HostFailure(
                    identity,
                    "QUOTA_DENIED",
                    quota_block_message(str(exc)),
                    retryable=False,
                )
            self._assert_current_host(packet)
            with self._revocation_lock:
                if identity in self._revoked:
                    raise ValueError("attempt revoked during quota read")
            if required_policy_error("delegate_task", packet) is not None:
                raise ValueError("captured policy revoked during quota read")
            refreshed = dict(self.route_resolver(self._binding[0]))
            self._check_worker_policy(packet)
            if any(refreshed.get(key) != route.get(key) for key in keys):
                raise ValueError("host route changed during quota read")
            if ExternalScheduler._deadline_expired(packet):
                return HostFailure(
                    identity,
                    "DEADLINE_EXPIRED",
                    "queue deadline expired during native admission",
                    retryable=False,
                )
        except AdmissionBlocked as exc:
            return HostDeferred(identity, str(exc))
        except Exception:
            return HostFailure(
                identity, "POLICY_DENIED", "native worker host policy denied"
            )

        from .task_authority import TaskAuthority, parent_tool_ceiling

        from tools.delegate_tool import _run_single_child
        from tools.delegate_tool_results import _build_child_preserving_parent_tools

        observation = _CallObservation(
            {
                "model": route["model"],
                "effort": route["effort"],
                "tier": route["service_tier"],
                "endpoint": route["base_url"].rstrip("/") + "/responses",
            },
            packet=packet,
        )
        child = None
        handed_to_runner = False
        from .host_usage import capture_usage

        usage = [None]
        usage_lock = RLock()
        running = [0]
        runner_done = [False]
        completion_ok = []
        transcript_writer = None
        transcript_entry = {"status": "failed", "exit_reason": "worker_failed"}

        if ExternalScheduler._deadline_expired(packet):
            return HostFailure(
                identity,
                "DEADLINE_EXPIRED",
                "queue deadline expired immediately before native execution",
                retryable=False,
            )

        def finish_usage():
            with usage_lock:
                runner_done[0] = True
                if child is not None:
                    usage[0] = capture_usage(
                        child, complete=bool(completion_ok) and running[0] == 0
                    )

        try:
            authority = TaskAuthority.from_packet(
                packet,
                self._binding,
                parent_tool_ceiling(self.parent),
            )
            with self._revocation_lock:
                if identity in self._revoked:
                    raise ValueError("attempt revoked before authority binding")
                self._authorities[identity] = authority
        except Exception:
            return HostFailure(
                identity, "POLICY_DENIED", "native task authority denied"
            )

        try:
            self.admission_preflight(packet)
            # Unconditional (admission_preflight is a no-op without a policy):
            # the child copies the parent's live credential at construction.
            self._assert_codex_parent()
            with authority.scope():
                child = _build_child_preserving_parent_tools(
                    task_index=0,
                    goal=goal,
                    context=context or None,
                    toolsets=None,
                    model=route["model"],
                    max_iterations=iterations,
                    task_count=1,
                    parent_agent=self.parent,
                    override_provider=route["provider"],
                    override_base_url=route["base_url"],
                    override_api_mode=route["api_mode"],
                    override_request_overrides={
                        "reasoning": {"effort": route["effort"]},
                        "service_tier": route["service_tier"],
                    },
                    routing_cfg={**route, "fallback_model": []},
                )
            from agent.service_tier_policy import capture_request_owner
            from .tier_authority import tier_owner_identity

            capture_request_owner(
                child, kind="delegation", identity=tier_owner_identity(packet)
            )
            with self._revocation_lock:
                if identity in self._revoked:
                    raise ValueError("attempt revoked during construction")
                self._children[identity] = child
            observation.agent = child
            child._codex_request_observer = observation
            try:
                from .transcript import attach_transcript

                _, transcript_writer = attach_transcript(child, packet)
            except Exception:
                # Transcript persistence is observational and never changes the
                # native worker's admission or result classification.
                transcript_writer = None
            # Optional host-owned injection for offline transport tests. The
            # packet and model output cannot provide this callable.
            factory = getattr(self.parent, "_client_factory", None)
            if factory is not None:
                if not callable(factory):
                    raise ValueError("invalid host client factory")
                child._create_request_openai_client = factory
            # Native child summaries can replace an empty response with a
            # diagnostic. Retain only structured completion flags from the
            # real conversation, before the runner reduces its result.
            from agent.turn_explainers import (
                _EXIT_REASON_EXPLANATIONS,
                _EXIT_REASON_PREFIX_EXPLANATIONS,
            )

            native_conversation = child.run_conversation

            def measured_conversation(*args, **kwargs):
                with usage_lock:
                    running[0] += 1
                try:
                    return observe_conversation(*args, **kwargs)
                finally:
                    with usage_lock:
                        running[0] -= 1
                        snapshot = capture_usage(
                            child, complete=runner_done[0] and running[0] == 0
                        )
                        usage[0] = snapshot
                    if usage_sink is not None and snapshot is not None:
                        usage_sink(snapshot)

            def observe_conversation(*args, **kwargs):
                raw = native_conversation(*args, **kwargs)
                reason = raw.get("turn_exit_reason")
                completion_ok.append(
                    raw.get("completed") is True
                    and raw.get("failed") is False
                    and isinstance(reason, str)
                    and reason not in _EXIT_REASON_EXPLANATIONS
                    and not any(
                        reason.startswith(prefix)
                        for prefix, _ in _EXIT_REASON_PREFIX_EXPLANATIONS
                    )
                )
                return raw

            child.run_conversation = measured_conversation
            if ExternalScheduler._deadline_expired(packet):
                return HostFailure(
                    identity,
                    "DEADLINE_EXPIRED",
                    "queue deadline expired during child construction",
                    retryable=False,
                )
            # Linearization point for starting a worker. Later pause toggles
            # do not revoke an already-started native conversation.
            self.admission_preflight(packet)
            # Re-check after construction: a parent that changed route while the
            # child was being built must not start it (child is closed below).
            self._assert_codex_parent()
            handed_to_runner = True
            with authority.scope(session_id=getattr(child, "session_id", None) or None):
                result = _run_single_child(
                    0, goal, child=child, parent_agent=self.parent
                )
            transcript_entry = result
            finish_usage()
            counts = observation.finish()
            with self._revocation_lock:
                if identity in self._revoked:
                    return HostFailure(
                        identity, "POLICY_DENIED", "attempt revoked", usage=usage[0]
                    )
            if (
                not completion_ok
                or not all(completion_ok)
                or result.get("status") != "completed"
                or result.get("exit_reason") != "completed"
                or result.get("truncated") is not False
            ):
                return HostFailure(
                    identity,
                    "WORKER_FAILED",
                    "native child did not complete",
                    usage=usage[0],
                )
            answer = result.get("summary")
            if not isinstance(answer, str) or not answer.strip():
                return HostFailure(
                    identity,
                    "WORKER_FAILED",
                    "native child result is not text",
                    usage=usage[0],
                )
            payload = {
                "worker_status": "succeeded",
                "answer": answer,
                "evidence": [],
                "artifacts": [],
                "checks": [],
                "uncertainties": [],
                "suggested_followups": [],
            }
            proof = {k: route[k] for k in keys}
            proof.update(
                observation_source="sdk-transport",
                observed_at_transport=True,
                physical_attempts=counts["http_exchanges"],
                logical_calls=counts["logical_calls"],
                sdk_invocations=counts["sdk_invocations"],
                host_check_evidence={
                    "source": "native-conversation-observer",
                    "native_conversation_completed": bool(completion_ok)
                    and all(completion_ok),
                    "observed_calls": len(completion_ok),
                },
            )
            from .tier_authority import authorized_tier

            proof["service_tier"] = authorized_tier(packet, observation.tier_decision)
            return HostOutcome(
                payload, proof, identity, observation.tier_decision, usage[0]
            )
        except AdmissionBlocked as exc:
            transcript_entry = {"status": "deferred", "error": "admission blocked"}
            return HostDeferred(identity, str(exc))
        except Exception:
            transcript_entry = {
                "status": "failed",
                "error": "native request observation unavailable",
            }
            finish_usage()
            return HostFailure(
                identity,
                "POLICY_DENIED",
                "native request observation unavailable",
                usage=usage[0],
            )
        finally:
            try:
                from .transcript import finalize_transcript

                finalize_transcript(transcript_writer, transcript_entry)
            except Exception:
                pass
            authority.revoke("native attempt finished")
            with self._revocation_lock:
                self._children.pop(identity, None)
                self._authorities.pop(identity, None)
            # Native runner owns close/deferred-close after invocation. Avoid
            # racing its still-running child on cancellation or timeout.
            if child is not None and not handed_to_runner:
                child.close()
