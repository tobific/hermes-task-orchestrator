"""External final-result delivery through the existing native async dispatcher."""

from contextvars import copy_context
import copy
import hashlib
import json
import math
import threading
import time
import uuid
from contextlib import contextmanager
from .scheduler import OrchestrationError


_FENCE_VERSION = 1
_FENCE_PROVIDER = "external_orchestrator"


class ExternalCompletionFence:
    """Host-owned native completion fence backed by the scheduler state file.

    The marker is an event-carried, bounded descriptor.  No native identifier
    history is retained: supersession is represented by the scheduler's
    delivery epoch and checked under its existing file lock.
    """

    def __init__(
        self, scheduler, payload=None, *, delivery_epoch=0, registration_nonce=None
    ):
        self.scheduler = scheduler
        self.payload = dict(payload or {})
        self.delivery_epoch = int(delivery_epoch)
        self.registration_nonce = registration_nonce or self.payload.get(
            "delivery_registration_nonce"
        )
        self._local = threading.local()
        self._captured = None

    @staticmethod
    def _text(value, name):
        if not isinstance(value, str) or not value or len(value) > 512:
            raise OrchestrationError(f"invalid completion fence {name}")
        return value

    def stamp(self, record):
        """Stamp only native dispatch metadata, never model result content."""
        state = self._state()
        if state is None:
            with self.lock():
                return self.stamp(record)
        run = state["runs"].get(self.payload.get("run_id"))
        review = self._review_snapshot(state, run) if run else None
        now = time.time()
        return {
            "version": _FENCE_VERSION,
            "provider": _FENCE_PROVIDER,
            "delegation_id": self._text(record.get("delegation_id"), "delegation_id"),
            "run_id": self._text(self.payload.get("run_id"), "run_id"),
            "owner_token": self._text(self.payload.get("owner_token"), "owner_token"),
            "parent_session_id": self._text(
                self.payload.get("parent_session_id"), "parent_session_id"
            ),
            "profile": self._text(self.payload.get("profile"), "profile"),
            "delivery_epoch": self.delivery_epoch,
            "nonce": self.registration_nonce,
            "issued_at": now,
            "review": review,
            "outcome": None,
        }

    @contextmanager
    def lock(self):
        """Hold the scheduler's durable lock before native state.db access."""
        depth = getattr(self._local, "depth", 0)
        if depth:
            self._local.depth = depth + 1
            try:
                yield
            finally:
                self._local.depth -= 1
            return
        with self.scheduler._thread_lock, self.scheduler._file_transaction() as state:
            self._local.depth = 1
            self._local.state = state
            try:
                yield
            finally:
                self._local.depth = 0
                self._local.state = None

    def _state(self):
        return getattr(self._local, "state", None)

    def _review_snapshot(self, state, run):
        review = self.scheduler._review_task(state, run)
        if not review:
            return None
        approval = run.get("final_review_approval")
        approval_digest = None
        if approval is not None:
            approval_digest = hashlib.sha256(
                self.scheduler._canonical_bytes(approval)
            ).hexdigest()
        return {
            "task_id": review.get("task_id"),
            "generation": review.get("generation"),
            "attempt": review.get("attempt"),
            "state": review.get("state"),
            "review_evidence_digest": review.get("review_evidence_digest"),
            "approval_digest": approval_digest,
        }

    def _validate_identity(self, marker, delegation_id, state, *, check_review):
        keys = {
            "version",
            "provider",
            "delegation_id",
            "run_id",
            "owner_token",
            "parent_session_id",
            "profile",
            "delivery_epoch",
            "nonce",
            "issued_at",
            "review",
            "outcome",
        }
        if not isinstance(marker, dict) or set(marker) != keys:
            return False
        if (
            marker.get("version") != _FENCE_VERSION
            or marker.get("provider") != _FENCE_PROVIDER
            or marker.get("delegation_id") != delegation_id
            or type(marker.get("delivery_epoch")) is not int
            or not isinstance(marker.get("nonce"), str)
            or not marker["nonce"]
            or type(marker.get("issued_at")) not in (int, float)
            or not math.isfinite(marker["issued_at"])
        ):
            return False
        try:
            for key in (
                "delegation_id",
                "run_id",
                "owner_token",
                "parent_session_id",
                "profile",
            ):
                self._text(marker.get(key), key)
        except OrchestrationError:
            return False
        run = state.get("runs", {}).get(marker["run_id"])
        if not isinstance(run, dict):
            return False
        if (
            (self.scheduler._closing and run.get("state") == "SUCCEEDED")
            or run.get("owner_token") != marker["owner_token"]
            or run.get("parent_session_id") != marker["parent_session_id"]
            or run.get("profile") != marker["profile"]
            or int(run.get("delivery_epoch", -1)) != marker["delivery_epoch"]
            or run.get("delivery_registered_epoch") != marker["delivery_epoch"]
            or run.get("delivery_registration_nonce") != marker.get("nonce")
            or not run.get("delivery_context")
            or (run.get("state") == "SUCCEEDED" and run.get("cancel_requested"))
            or run.get("state")
            not in {
                "SUCCEEDED",
                "FAILED",
                "SUPERSEDED",
                "CANCELLED",
                "TIMED_OUT",
                "BLOCKED",
            }
        ):
            return False
        if not check_review:
            return True
        current_review = self._review_snapshot(state, run)
        if run.get("final_review_task_id") and current_review is None:
            return False
        return (marker.get("review") == current_review
                and marker.get("outcome") == self._outcome(run))

    @staticmethod
    def _outcome(run):
        return {"state": run.get("state"), "cancel_requested": bool(run.get("cancel_requested"))}

    def capture(self, state, run):
        # Invoked by collect under its existing durable transaction, never by a
        # later publication/replay that could bless an obsolete payload.
        if run.get("delivery_registration_nonce") != self.registration_nonce:
            raise OrchestrationError("superseded completion registration")
        if run.get("state") == "SUCCEEDED" and run.get("cancel_requested"):
            raise OrchestrationError("cancelled completion")
        snapshot = {"review": self._review_snapshot(state, run), "outcome": self._outcome(run)}
        if self._captured is not None and self._captured != snapshot:
            raise OrchestrationError("completion changed during collection")
        self._captured = copy.deepcopy(snapshot)

    def prepare(self, marker, delegation_id):
        """Attach the current host review only after stable epoch validation."""
        state = self._state()
        if state is None:
            with self.lock():
                return self.prepare(marker, delegation_id)
        if not self._validate_identity(
            marker, delegation_id, state, check_review=False
        ):
            return None
        if self._captured is None:
            return None
        prepared = copy.deepcopy(marker)
        prepared.update(copy.deepcopy(self._captured))
        return prepared

    def validate(self, marker, delegation_id):
        state = self._state()
        if state is None:
            with self.lock():
                return self.validate(marker, delegation_id)
        return self._validate_identity(marker, delegation_id, state, check_review=True)


def safe_rearm_detached(scheduler, payload, parent):
    try:
        return rearm_detached(scheduler, payload, parent)
    except Exception as exc:
        # The mutation already committed. Never pretend it rolled back.
        return {"ok": False, "completion_delivery": "rearm_failed",
                "error": type(exc).__name__, "retry": "orchestration_join"}


def rearm_detached(scheduler, payload, parent):
    from .operational_controls import detached_delivery_enabled

    def has_delivery(state):
        run = state["runs"].get(payload.get("run_id"))
        if not run:
            raise OrchestrationError("unknown run")
        scheduler._owner(run, payload)
        return bool(run.get("delivery_context"))
    if not scheduler._read(has_delivery):
        return {"ok": True, "completion_delivery": "unchanged"}
    if not detached_delivery_enabled(payload["profile"]):
        return {"ok": True, "completion_delivery": "disabled"}

    def claim(state):
        run = state["runs"].get(payload.get("run_id"))
        if not run:
            raise OrchestrationError("unknown run")
        scheduler._owner(run, payload)
        epoch = run.get("delivery_epoch", 0)
        if (
            not run.get("delivery_context")
            or run.get("delivery_registered_epoch") == epoch
        ):
            return None
        nonce = uuid.uuid4().hex
        run.update(delivery_registered_epoch=epoch, delivery_registration_nonce=nonce)
        return {
            "delivery_context": dict(run["delivery_context"]),
            "delivery_epoch": epoch,
            "nonce": nonce,
        }

    registration = scheduler._mutate(claim)
    if registration is None:
        return {"ok": True, "completion_delivery": "unchanged"}

    def release(state):
        run = state["runs"][payload["run_id"]]
        scheduler._owner(run, payload)
        if run.get("delivery_registration_nonce") == registration["nonce"]:
            run["delivery_registered_epoch"] = None

    try:
        handle = dispatch_detached(scheduler, payload, parent, existing=registration)
    except Exception:
        scheduler._mutate(release)
        raise
    if not handle.get("ok") or handle.get("completion_delivery") == "disabled":
        scheduler._mutate(release)
    return handle


def dispatch_detached(scheduler, payload, parent, *, existing=None):
    from tools.async_delegation import dispatch_async_delegation_batch
    from tools.approval_context import get_current_session_key
    from gateway.session_context import get_session_env

    args = (
        dict(payload)
        if existing is None
        else {
            k: payload[k]
            for k in ("run_id", "owner_token", "parent_session_id", "profile")
        }
    )
    args.setdefault("run_id", uuid.uuid4().hex)
    session = str(parent.session_id)
    session_key = get_current_session_key(default="") or session
    ui = str(get_session_env("HERMES_UI_SESSION_ID", "") or "")
    origin = str(get_session_env("HERMES_SESSION_ID", "") or session)
    routing = {
        "session": session,
        "session_key": session_key,
        "ui": ui,
        "origin": origin,
    }
    if existing is not None:
        routing = existing["delivery_context"]
        session, session_key, ui, origin = (
            routing[k] for k in ("session", "session_key", "ui", "origin")
        )
    from .operational_controls import detached_delivery_enabled

    if not detached_delivery_enabled(args["profile"]):
        if existing is None:
            result = scheduler.create_run(
                args, delivery_context=routing, delivery_registered=False
            )
            return {**result, "mode": "detached", "completion_delivery": "disabled"}
        return {"ok": True, "completion_delivery": "disabled"}
    epoch = existing["delivery_epoch"] if existing is not None else 0
    stopped = threading.Event()
    context = copy_context()
    # Keep one live validator for claims/replay; the per-dispatch context
    # carries the exact native delegation id and delivery epoch to stamp.
    from tools import async_delegation as native

    native.set_completion_guard(ExternalCompletionFence(scheduler))
    registration_nonce = existing["nonce"] if existing is not None else uuid.uuid4().hex
    dispatch_fence = ExternalCompletionFence(
        scheduler,
        args,
        delivery_epoch=epoch,
        registration_nonce=registration_nonce,
    )

    def interrupt():
        stopped.set()
        try:
            scheduler.cancel(args, delivery_epoch=epoch)

            # Cancellation is an invalidation boundary for the already-admitted
            # native publisher too.  Bump the durable epoch only if this exact
            # run still owns the expected epoch; the next join/rearm can carry
            # the new epoch if a completion is still wanted.
            def invalidate(state):
                run = state["runs"].get(args["run_id"])
                if not run:
                    return
                scheduler._owner(run, args)
                if run.get("delivery_epoch", 0) == epoch:
                    run["delivery_epoch"] = epoch + 1
                    run["delivery_registered_epoch"] = None
                    run["updated_at"] = time.time()

            scheduler._mutate(invalidate)
        except OrchestrationError:
            pass  # admitted native unit may not have created its run yet

    def work(_parent=parent):
        # The captured parent and context stay alive for this admitted unit only.
        if stopped.is_set():
            return {"results": [], "error": "cancelled before external admission"}
        if existing is None:
            scheduler.create_run(args, delivery_context=routing,
                                 delivery_registration_nonce=registration_nonce)
        if stopped.is_set():
            scheduler.cancel(args, delivery_epoch=epoch)
        while True:
            state = scheduler.join({**args, "timeout_seconds": 1})
            if state.get("delivery_epoch", 0) != epoch:
                return {"results": [], "error": "superseded detached completion epoch"}
            if state["state"] in {"SUCCEEDED", "FAILED", "CANCELLED", "TIMED_OUT"}:
                break
        events = []
        cursor = 0
        while True:
            page = scheduler.collect({**args, "cursor": cursor}, delivery_epoch=epoch,
                                     completion_observer=dispatch_fence.capture)
            events.extend(page["events"])
            if page["next_cursor"] == cursor:
                break
            cursor = page["next_cursor"]
        state["state"] = dispatch_fence._captured["outcome"]["state"]
        return {
            "results": [
                {
                    "task_index": 0,
                    "goal": "External orchestration " + args["run_id"],
                    "status": "success" if state["state"] == "SUCCEEDED" else "error",
                    "summary": json.dumps(
                        {
                            "run_id": args["run_id"],
                            "state": state["state"],
                            "events": events,
                        },
                        ensure_ascii=False,
                    ),
                }
            ]
        }

    with native.register_completion_guard(dispatch_fence):
        handle = dispatch_async_delegation_batch(
            goals=["External orchestration " + args["run_id"]],
            context=None,
            toolsets=None,
            role="leaf",
            model=None,
            session_key=session_key,
            parent_session_id=session,
            origin_ui_session_id=ui,
            origin_session_id=origin,
            runner=lambda: context.run(work),
            interrupt_fn=interrupt,
        )
    return {
        **handle,
        "ok": handle.get("status") == "dispatched",
        "run_id": args["run_id"],
    }
