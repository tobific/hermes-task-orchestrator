"""Bounded read-only projection of retained diagnostic result events.

No scheduler startup, reconciliation, worker pump, delivery ack or analytics.
"""

from __future__ import annotations

import fcntl
import json
import os
import stat
from contextlib import contextmanager
from pathlib import Path

from .storage import StorageError, _directory_fd, _flags

MAX_HISTORY_BYTES = 65536


def _error(message):
    from .scheduler import OrchestrationError

    return OrchestrationError(message)


@contextmanager
def _existing_file(directory, name):
    fd = os.open(name, os.O_RDONLY | _flags() | os.O_NONBLOCK, dir_fd=directory)
    try:
        st = os.fstat(fd)
        if (
            not stat.S_ISREG(st.st_mode)
            or st.st_uid != os.getuid()
            or st.st_nlink != 1
            or st.st_mode & 0o077
        ):
            raise _error("unsafe history storage file")
        with os.fdopen(fd, "rb", closefd=False) as handle:
            yield handle
    finally:
        os.close(fd)


def _text(value, maximum):
    from tools.delegation_live_log import _redact

    if not isinstance(value, str):
        return ""
    return _redact(value)[:maximum]


def _safe_route(value, *, proof=False):
    """Project route metadata without exposing arbitrary persisted fields."""
    if not isinstance(value, dict):
        return {}
    fields = (
        "provider",
        "model",
        "base_url",
        "api_mode",
        "fallback",
        "effort",
        "service_tier",
    )
    if proof:
        fields += (
            "physical_attempts",
            "logical_calls",
            "sdk_invocations",
            "observed_at_transport",
            "observation_source",
        )
    result = {}
    for key in fields:
        item = value.get(key)
        if isinstance(item, str):
            result[key] = _text(item, 2048)
        elif type(item) in (bool, int):
            result[key] = item
    evidence = value.get("host_check_evidence")
    if proof and isinstance(evidence, dict):
        result["host_check_evidence"] = {
            key: (_text(item, 256) if isinstance(item, str) else item)
            for key, item in evidence.items()
            if key
            in {
                "source",
                "native_conversation_completed",
                "observed_calls",
            }
            and (isinstance(item, str) or type(item) in (bool, int))
        }
    return result


def _safe_validation(value):
    """Expose only the host-normalized claim-validation record."""
    if not isinstance(value, dict) or type(value.get("schema")) is not int:
        return None
    if value.get("schema") != 1 or value.get("mode") not in {
        "plain_summary",
        "structured_claims",
    }:
        return None
    artifacts = []
    for item in (
        value.get("artifacts", [])[:32]
        if isinstance(value.get("artifacts"), list)
        else []
    ):
        if not isinstance(item, dict):
            continue
        entry = {}
        if isinstance(item.get("type"), str):
            entry["type"] = _text(item["type"], 64)
        if isinstance(item.get("reference"), str):
            entry["reference"] = _text(item["reference"], 1024)
        elif isinstance(item.get("path"), str):
            # Legacy rows may retain a path; expose only a redacted reference.
            entry["reference"] = _text(item["path"], 1024)
        if type(item.get("size")) is int and item["size"] >= 0:
            entry["size"] = item["size"]
        if isinstance(item.get("sha256"), str):
            entry["sha256"] = _text(item["sha256"], 128)
        if type(item.get("verified")) is bool:
            entry["verified"] = item["verified"]
        if entry:
            artifacts.append(entry)
    checks = []
    for item in (
        value.get("checks", [])[:32] if isinstance(value.get("checks"), list) else []
    ):
        if not isinstance(item, dict):
            continue
        entry = {}
        if isinstance(item.get("name"), str):
            entry["name"] = _text(item["name"], 500)
        if isinstance(item.get("status"), str):
            entry["status"] = _text(item["status"], 32)
        if isinstance(item.get("detail"), str):
            entry["detail"] = _text(item["detail"], 500)
        if type(item.get("host_verified")) is bool:
            entry["host_verified"] = item["host_verified"]
        if entry:
            checks.append(entry)
    limitations = value.get("limitations")
    return {
        "schema": 1,
        "mode": value["mode"],
        "declared": value.get("declared") is True,
        "artifacts": artifacts,
        "checks": checks,
        "limitations": (
            [_text(item, 500) for item in limitations[:4] if isinstance(item, str)]
            if isinstance(limitations, list)
            else []
        ),
    }


def _unavailable(reason):
    return {"available": False, "reason": reason}


def _project_diagnostic(event, task, retained):
    """Build a host-labelled, read-only diagnostic for one event."""
    current = task.get("run_id") == event.get("run_id") and task.get(
        "generation"
    ) == event.get("generation")
    task_result = task.get("result") if current else None
    if retained is not None and isinstance(retained.get("result"), dict):
        result_snapshot = retained["result"]
        result_available = result_snapshot.get("available") is True
        worker_status = result_snapshot.get("worker_status")
        answer_available = result_snapshot.get("answer_available") is True
        evidence_values = result_snapshot.get("evidence", [])
        native_summary = result_snapshot.get("native_summary")
        simulated = result_snapshot.get("simulated") is True
        error_classification = result_snapshot.get("error_classification")
    else:
        result = task_result if isinstance(task_result, dict) else {}
        result_available = isinstance(task_result, dict)
        worker_status = result.get("worker_status")
        answer_available = isinstance(result.get("answer"), str)
        evidence_values = result.get("evidence", event.get("evidence", []))
        native_summary = result.get("native_summary")
        simulated = result.get("simulated") is True
        error_classification = result.get("error_classification")

    if (
        current
        and not isinstance(native_summary, str)
        and isinstance(task_result, dict)
    ):
        current_native_summary = task_result.get("native_summary")
        if isinstance(current_native_summary, str):
            native_summary = current_native_summary

    if retained is not None:
        requested_raw = retained.get("requested_route")
        proof_raw = retained.get("route_proof")
        validation_raw = retained.get("claim_validation")
    else:
        result = task_result if isinstance(task_result, dict) else {}
        requested_raw = task.get("route") if current else None
        proof_raw = result.get("route_proof")
        validation_raw = result.get("claim_validation")

    requested = _safe_route(requested_raw)
    proof = _safe_route(proof_raw, proof=True)
    source = proof.get("observation_source") if proof else None
    observed = (
        proof.get("observed_at_transport")
        if type(proof.get("observed_at_transport")) is bool
        else None
    )
    host_observed = source == "sdk-transport" and observed is True
    authority = (
        "host" if source == "sdk-transport" else ("worker" if proof else "unavailable")
    )
    provenance = {
        "available": bool(proof),
        "observation_source": source or "unavailable",
        "observed_at_transport": observed,
        "authority": authority,
    }
    requested_route = (
        {
            "available": True,
            "source": "host-packet",
            "route": requested,
        }
        if requested
        else _unavailable("requested route not retained")
    )
    transport_route = (
        {
            "available": True,
            "source": "sdk-transport",
            "authority": "host",
            "observation_source": "sdk-transport",
            "observed_at_transport": True,
            "route": proof,
        }
        if host_observed
        else _unavailable("host transport observation unavailable")
    )
    worker_route = (
        {
            "available": True,
            "source": source or "worker-result",
            "authority": "worker",
            "observation_source": source or "worker-result",
            "observed_at_transport": observed,
            "route": proof,
        }
        if proof and source != "sdk-transport"
        else _unavailable("worker route proof unavailable")
    )

    validation = _safe_validation(validation_raw)
    if validation is None:
        validation_view = {
            "available": False,
            "outcome": "unavailable",
            "reason": "host validation record not retained",
        }
    else:
        host_checks = all(
            item.get("status") == "pass" and item.get("host_verified") is True
            for item in validation["checks"]
        )
        stored_outcome = (
            retained.get("validation_outcome") if isinstance(retained, dict) else None
        )
        if not isinstance(stored_outcome, str):
            if validation["declared"] and host_checks:
                stored_outcome = "structured_claims_accepted"
            elif validation["declared"]:
                stored_outcome = "structured_claims_unverified"
            else:
                stored_outcome = "plain_summary_no_claims"
        if stored_outcome == "structured_claims_accepted" and not host_checks:
            stored_outcome = "structured_claims_unverified"
        validation_view = {
            "available": True,
            "source": "host-claim-validator",
            "outcome": stored_outcome,
            **validation,
        }
        if stored_outcome == "delivery_claim_revalidation_failed":
            validation_view["prior_outcome"] = (
                "structured_claims_accepted"
                if host_checks
                else "structured_claims_unverified"
            )
            validation_view["accepted_before_delivery"] = host_checks

    evidence = (
        [_text(item, 500) for item in evidence_values[:4] if isinstance(item, str)]
        if isinstance(evidence_values, list)
        else []
    )
    result_view = {
        "available": result_available,
        "source": "host-retained-result" if result_available else "unavailable",
        "worker_status": _text(worker_status, 128) or None,
        "answer_available": answer_available,
        "native_summary": (
            {
                "available": True,
                "source": "host-retained-result",
                "reference": _text(native_summary, 4000),
            }
            if isinstance(native_summary, str) and native_summary
            else _unavailable("raw native_summary not retained")
        ),
        "evidence": (
            {"available": True, "source": "worker-result", "items": evidence}
            if evidence
            else _unavailable("result evidence not retained")
        ),
        "simulated": simulated,
        "error_classification": _text(error_classification, 128) or None,
    }
    artifacts = validation["artifacts"] if validation is not None else []
    checks = (
        [
            item
            for item in validation["checks"]
            if item.get("status") == "pass" and item.get("host_verified") is True
        ]
        if validation is not None
        else []
    )
    artifact_view = (
        {
            "available": True,
            "source": "host-claim-validator",
            "items": artifacts,
        }
        if artifacts
        else _unavailable(
            "no host-validated artifact evidence"
            if validation is not None
            else "host validation unavailable"
        )
    )
    check_view = (
        {
            "available": True,
            "source": "host-claim-validator",
            "items": checks,
        }
        if checks
        else _unavailable(
            "no host-validated check evidence"
            if validation is not None
            else "host validation unavailable"
        )
    )
    return {
        "record_type": "diagnostic",
        "cursor": event.get("cursor"),
        "run_id": _text(event.get("run_id"), 256),
        "task_id": _text(event.get("task_id"), 256),
        "generation": event.get("generation"),
        "current": current,
        "bounded": True,
        "provenance": provenance,
        "requested_route": requested_route,
        "transport_observed_route": transport_route,
        "worker_reported_route": worker_route,
        "validation": validation_view,
        "result_evidence": result_view,
        "artifact_evidence": artifact_view,
        "check_evidence": check_view,
    }


def _bounded_diagnostic(value):
    """Compact oversized diagnostics without hiding or blocking the task row."""
    if len(json.dumps(value, ensure_ascii=False).encode()) <= 16384:
        return value

    def trim(item, chars):
        if isinstance(item, str):
            return item[:chars]
        if isinstance(item, list):
            return [trim(child, chars) for child in item[:4]]
        if isinstance(item, dict):
            return {key: trim(child, chars) for key, child in item.items()}
        return item

    for chars in (256, 128, 64, 32):
        compact = trim(value, chars)
        for key in ("run_id", "task_id", "generation", "cursor", "current"):
            compact[key] = value[key]
        compact["truncated"] = True
        if len(json.dumps(compact, ensure_ascii=False).encode()) <= 16384:
            return compact
    raise _error("diagnostic projection bound exceeded")


def project_history(state, args, data_dir=None):
    """Project one locked state snapshot; returned data never aliases storage."""
    from .scheduler import ExternalScheduler

    run_id = args.get("run_id")
    if not isinstance(run_id, str) or not run_id or len(run_id) > 256:
        raise _error("run_id is required")
    run = state.get("runs", {}).get(run_id)
    if not isinstance(run, dict):
        raise _error("unknown run")
    for field in ("owner_token", "parent_session_id", "profile"):
        if not isinstance(args.get(field), str) or not args[field]:
            raise _error("missing owner identity")
    ExternalScheduler._owner(None, run, args)
    cursor, limit = args.get("cursor", 0), args.get("limit", 20)
    if type(cursor) is not int or not 0 <= cursor <= 2**63 - 1:
        raise _error("invalid history cursor")
    if type(limit) is not int or limit < 1:
        raise _error("invalid history limit")
    limit = min(limit, 50)
    result = dict(
        ok=True,
        run_id=run_id,
        cursor=cursor,
        next_cursor=cursor,
        records=[],
        diagnostics=[],
        bounded=True,
        diagnostic=True,
    )
    from .diagnostic_store import load_diagnostic

    for event in state.get("delivery_events", []):
        if event.get("run_id") != run_id or event.get("cursor", 0) <= cursor:
            continue
        # Final-review aggregates are not task-result records. Their position is
        # scanned but their nested claims must not escape through history.
        if event.get("final_delivery"):
            result["next_cursor"] = event["cursor"]
            continue
        task = state.get("tasks", {}).get(event.get("task_id"), {})
        record = {key: event.get(key) for key in ("cursor", "generation", "attempt")}
        record.update(
            run_id=run_id,
            task_id=_text(event.get("task_id"), 256),
            record_type="task_result",
            state=_text(event.get("state"), 32),
            current=(
                task.get("run_id") == run_id
                and task.get("generation") == event.get("generation")
            ),
            answer=_text(event.get("answer"), 4000),
            error_classification=_text(event.get("error_classification"), 256) or None,
            simulated=event.get("simulated") is True,
        )
        for field in ("evidence", "uncertainties", "suggested_followups"):
            values = event.get(field)
            record[field] = (
                [_text(item, 500) for item in values[:4] if isinstance(item, str)]
                if isinstance(values, list)
                else []
            )
        proposed = {
            **result,
            "records": [*result["records"], record],
            "diagnostics": [
                *result["diagnostics"],
                _bounded_diagnostic(
                    _project_diagnostic(
                        event,
                        task,
                        load_diagnostic(data_dir, run, event)
                        if data_dir is not None
                        else None,
                    )
                ),
            ],
            "next_cursor": event["cursor"],
        }
        if (
            len(json.dumps(proposed, ensure_ascii=False).encode("utf-8"))
            > MAX_HISTORY_BYTES
        ):
            break
        result = proposed
        if len(result["records"]) >= limit:
            break
    return result


def read_history(data_dir, args):
    """Cold safe read: never create/repair state or instantiate a scheduler."""
    from .scheduler import MAX_STATE_BYTES, OrchestrationError

    directory = None
    try:
        directory = _directory_fd(Path(data_dir))
        st = os.fstat(directory)
        if st.st_uid != os.getuid() or st.st_mode & 0o077:
            raise _error("unsafe history storage directory")
        with _existing_file(directory, "state.lock") as lock:
            fcntl.flock(lock.fileno(), fcntl.LOCK_SH)
            try:
                with _existing_file(directory, "state.json") as handle:
                    raw = handle.read(MAX_STATE_BYTES + 1)
                if len(raw) > MAX_STATE_BYTES:
                    raise _error("history state byte budget exceeded")
                state = json.loads(raw)
                if not isinstance(state, dict):
                    raise _error("invalid history state")
                return project_history(state, args, data_dir=data_dir)
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    except OrchestrationError:
        raise
    except (OSError, StorageError, ValueError, TypeError, AttributeError) as exc:
        raise _error("history storage unavailable or invalid") from exc
    finally:
        if directory is not None:
            os.close(directory)
