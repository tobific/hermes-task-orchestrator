"""CLI session-boundary cleanup through the native lifecycle hook."""

import hashlib
import logging
from pathlib import Path

from .owner_operations import owner_operation

log = logging.getLogger(__name__)


def on_session_finalize(scheduler_factory, **kwargs):
    """Fence unfinished joinable work without changing detached or settled runs."""
    session_id = kwargs.get("session_id")
    if (
        kwargs.get("platform") != "cli"
        or kwargs.get("reason") != "session_boundary"
        or not isinstance(session_id, str)
        or not session_id.strip()
        or session_id != session_id.strip()
    ):
        return {"ok": True, "skipped": "not_an_identified_cli_boundary"}
    try:
        from hermes_constants import get_hermes_home

        profile = str(Path(get_hermes_home()).resolve())
        owner = hashlib.sha256((profile + "\0" + session_id).encode()).hexdigest()
        return owner_operation(
            scheduler_factory(),
            {
                "owner_only": True,
                "owner_token": owner,
                "parent_session_id": session_id,
                "profile": profile,
            },
            cancel=True,
            joinable_only=True,
        )
    except Exception:
        log.warning("External CLI boundary cleanup failed", exc_info=True)
        return {"ok": False, "error": "cli_boundary_cleanup_failed"}
