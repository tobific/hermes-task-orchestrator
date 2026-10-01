"""Optional diagnostic snapshots isolated from scheduler state/capacity.

References derive only from an authorized stored run and committed event.
No artifact dereference, scheduler mutation, or repair occurs on reads.
"""

from __future__ import annotations
import hashlib
import json
import os
import stat
from pathlib import Path
from .storage import atomic_replace_bytes, _directory_fd, _flags

MAX_DIAGNOSTIC_BYTES = 262144


def binding(run, event):
    # Delivery acknowledgement is the only mutable event field. It cannot
    # invalidate the observation or lend one event another event's evidence.
    identity = {
        "owner": {
            k: run.get(k) for k in ("owner_token", "parent_session_id", "profile")
        },
        "event": {k: v for k, v in event.items() if k != "delivered"},
    }
    return hashlib.sha256(
        json.dumps(
            identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()


def diagnostic_path(data_dir, run, event):
    return Path(data_dir) / "diagnostics" / (binding(run, event) + ".json")


def save_diagnostic(data_dir, run, event, diagnostic):
    """Best effort evidence only; never changes authoritative settlement."""
    try:
        payload = json.dumps(
            {"binding": binding(run, event), "diagnostic": diagnostic},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        if len(payload) > MAX_DIAGNOSTIC_BYTES:
            return False
        atomic_replace_bytes(diagnostic_path(data_dir, run, event), payload)
        return True
    except Exception:
        return False


def load_diagnostic(data_dir, run, event):
    """Owner authorization must happen before calling; fail unavailable."""
    directory = None
    fd = None
    try:
        path = diagnostic_path(data_dir, run, event)
        directory = _directory_fd(path.parent)
        st = os.fstat(directory)
        if st.st_uid != os.getuid() or st.st_mode & 0o077:
            return None
        fd = os.open(
            path.name, os.O_RDONLY | _flags() | os.O_NONBLOCK, dir_fd=directory
        )
        st = os.fstat(fd)
        if (
            not stat.S_ISREG(st.st_mode)
            or st.st_uid != os.getuid()
            or st.st_nlink != 1
            or st.st_mode & 0o077
        ):
            return None
        with os.fdopen(fd, "rb", closefd=False) as handle:
            raw = handle.read(MAX_DIAGNOSTIC_BYTES + 1)
        if len(raw) > MAX_DIAGNOSTIC_BYTES:
            return None
        value = json.loads(raw)
        if not isinstance(value, dict) or value.get("binding") != binding(run, event):
            return None
        row = value.get("diagnostic")
        if not isinstance(row, dict):
            return None
        if any(
            row.get(k) != event.get(k)
            for k in ("run_id", "task_id", "generation", "state", "cursor")
        ):
            return None
        return row
    except Exception:
        return None
    finally:
        if fd is not None:
            os.close(fd)
        if directory is not None:
            os.close(directory)
