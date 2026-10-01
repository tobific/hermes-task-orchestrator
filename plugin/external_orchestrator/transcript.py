"""External owner-bound adapter for the upstream live transcript writer."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Mapping

from .storage import (
    StorageError,
    atomic_replace_bytes,
    ensure_private_directory,
    open_private_file,
)


class TranscriptAccessError(PermissionError):
    """A transcript locator or owner binding failed closed validation."""


_MAX_HANDLE_CHARS = 4096
_MAX_READ_CHARS = 65536
_MAX_OFFSET = 1_000_000_000
_LOG_NAME = "task-0.log"
_METADATA_NAME = "metadata.json"
_DELEGATION_RE = re.compile(r"^deleg_([0-9a-f]{64})_([0-9a-f]{64})$")


def _root(root: Path | None = None) -> Path:
    """Resolve the trusted profile root; packet fields are never consulted."""
    if root is None:
        try:
            from hermes_constants import get_hermes_dir

            root = get_hermes_dir("cache/delegation", "delegation_cache") / "live"
        except Exception as exc:
            raise TranscriptAccessError("trusted profile root unavailable") from exc
    try:
        value = Path(root).expanduser()
        if ".." in value.parts:
            raise ValueError("transcript root traversal is not permitted")
        return Path(os.path.abspath(os.fspath(value)))
    except (OSError, TypeError, ValueError) as exc:
        raise TranscriptAccessError("invalid transcript root") from exc


def _text_field(packet: Mapping[str, Any], name: str) -> str:
    value = packet.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"transcript {name} is required")
    return value


def _counter_field(packet: Mapping[str, Any], name: str) -> int:
    value = packet.get(name)
    if type(value) is not int or value < 0:
        raise ValueError(f"transcript {name} is invalid")
    return value


def _owner_digest(owner_token: str) -> str:
    return hashlib.sha256(owner_token.encode("utf-8")).hexdigest()


def _identity_digest(owner_digest: str, session: str, profile: str) -> str:
    payload = [owner_digest, session, profile]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _binding_digest(
    owner_digest: str,
    run_id: str,
    task_id: str,
    generation: int,
    attempt: int,
    profile: str,
    session: str,
) -> str:
    payload = [
        owner_digest,
        run_id,
        task_id,
        generation,
        attempt,
        profile,
        session,
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _packet_binding(packet: Mapping[str, Any]) -> dict[str, Any]:
    owner = _text_field(packet, "owner_token")
    session = _text_field(packet, "parent_session_id")
    profile = _text_field(packet, "profile")
    run_id = _text_field(packet, "run_id")
    task_id = _text_field(packet, "task_id")
    generation = _counter_field(packet, "generation")
    attempt = _counter_field(packet, "attempt")
    owner_digest = _owner_digest(owner)
    return {
        "owner_token_sha256": owner_digest,
        "parent_session_id": session,
        "profile": profile,
        "run_id": run_id,
        "task_id": task_id,
        "generation": generation,
        "attempt": attempt,
        "identity_sha256": _identity_digest(owner_digest, session, profile),
        "binding_sha256": _binding_digest(
            owner_digest, run_id, task_id, generation, attempt, profile, session
        ),
    }


def _handle_for(root: Path, binding: Mapping[str, Any]) -> str:
    directory = f"deleg_{binding['identity_sha256']}_{binding['binding_sha256']}"
    return str(root / directory / _LOG_NAME)


def transcript_handle(packet: Mapping[str, Any], *, root: Path | None = None) -> str:
    """Return the deterministic host-owned locator for one task attempt."""
    if not isinstance(packet, Mapping):
        raise TypeError("transcript packet must be a mapping")
    return _handle_for(_root(root), _packet_binding(packet))


def optional_transcript_handle(packet: Mapping[str, Any]) -> str | None:
    """Unavailable diagnostics cannot prevent scheduling or task completion."""
    try:
        return transcript_handle(packet)
    except Exception:
        return None


def _metadata(binding: Mapping[str, Any], root: Path) -> bytes:
    record = {
        "version": 1,
        **dict(binding),
        "root": str(root),
    }
    return json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8")


def attach_transcript(
    child: Any, packet: Mapping[str, Any], *, root: Path | None = None
) -> tuple[str, Any | None]:
    """Best-effort attach of one upstream writer and callback tee."""
    handle = transcript_handle(packet, root=root)
    try:
        child._live_transcript_path = handle
    except Exception:
        pass

    try:
        configured_root = _root(root)
        log_path = Path(handle)
        ensure_private_directory(configured_root)
        ensure_private_directory(log_path.parent)
        _assert_no_symlink_components(
            configured_root, log_path.relative_to(configured_root)
        )
        atomic_replace_bytes(
            log_path.parent / _METADATA_NAME,
            _metadata(_packet_binding(packet), configured_root),
        )
    except Exception:
        # A storage failure must not affect native worker setup.  Do not invoke
        # the upstream writer on an unvalidated directory after this failure.
        return handle, None

    try:
        from tools.delegation_live_log import (
            LiveTranscriptWriter,
            wrap_progress_callback,
        )

        writer = LiveTranscriptWriter(
            log_path.parent.name,
            0,
            str(packet.get("goal", "")),
            context=packet.get("context") or None,
            root=configured_root,
        )
        if writer.path is None:
            return handle, None
        inner_callback = getattr(child, "tool_progress_callback", None)
        try:
            child.tool_progress_callback = wrap_progress_callback(
                inner_callback, writer
            )
        except Exception:
            # Keep the writer usable for finalization, but preserve the child
            # callback if the host child disallows replacement.
            pass
        return handle, writer
    except Exception:
        return handle, None


def finalize_transcript(writer: Any | None, entry: Mapping[str, Any]) -> None:
    """Best-effort upstream finalization; never alters native result handling."""
    if writer is None:
        return
    payload = dict(entry) if isinstance(entry, Mapping) else {}
    try:
        writer.finalize(payload)
    except Exception:
        pass
    try:
        writer.flush_stream()
    except Exception:
        pass


def _bounded_read_args(max_chars: int, offset: int) -> tuple[int, int]:
    if type(max_chars) is not int or max_chars <= 0:
        raise TranscriptAccessError("max_chars must be a positive integer")
    if type(offset) is not int or offset < 0:
        raise TranscriptAccessError("offset must be a non-negative integer")
    return min(max_chars, _MAX_READ_CHARS), min(offset, _MAX_OFFSET)


def _assert_no_symlink_components(root: Path, relative: Path) -> None:
    current = Path(root.anchor)
    for component in root.parts[1:]:
        current /= component
        try:
            if stat.S_ISLNK(os.lstat(current).st_mode):
                raise TranscriptAccessError("transcript path component is a symlink")
        except FileNotFoundError:
            return
    current = root
    for component in relative.parts:
        current /= component
        try:
            if stat.S_ISLNK(os.lstat(current).st_mode):
                raise TranscriptAccessError("transcript path component is a symlink")
        except FileNotFoundError:
            return


def _unavailable(offset: int) -> dict[str, Any]:
    return {
        "available": False,
        "text": "",
        "offset": offset,
        "next_offset": offset,
        "bounded": True,
    }


def _validate_locator(
    handle: str,
    root: Path,
    *,
    owner_token: str,
    parent_session_id: str,
    profile: str,
) -> tuple[Path, Path, str]:
    if not isinstance(handle, str) or not handle or len(handle) > _MAX_HANDLE_CHARS:
        raise TranscriptAccessError(
            "transcript handle must be a bounded non-empty string"
        )
    if "\x00" in handle:
        raise TranscriptAccessError("transcript handle contains NUL")
    try:
        path = Path(handle)
    except (OSError, TypeError, ValueError) as exc:
        raise TranscriptAccessError("invalid transcript handle") from exc
    if not path.is_absolute() or ".." in path.parts:
        raise TranscriptAccessError("transcript handle traversal is not permitted")
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise TranscriptAccessError(
            "transcript handle is outside the configured root"
        ) from exc
    if relative.parts[-1:] != (_LOG_NAME,) or len(relative.parts) != 2:
        raise TranscriptAccessError("invalid transcript handle")
    match = _DELEGATION_RE.fullmatch(relative.parts[0])
    if match is None:
        raise TranscriptAccessError("invalid transcript handle")
    if not all(
        isinstance(value, str) and value
        for value in (owner_token, parent_session_id, profile)
    ):
        raise TranscriptAccessError("owner/session/profile identity is required")
    expected_identity = _identity_digest(
        _owner_digest(owner_token), parent_session_id, profile
    )
    if match.group(1) != expected_identity:
        raise TranscriptAccessError("owner/session/profile identity mismatch")
    _assert_no_symlink_components(root, relative)
    return path, path.parent / _METADATA_NAME, match.group(2)


def _load_metadata(path: Path) -> dict[str, Any] | None:
    try:
        with open_private_file(path, "r", create=False) as handle:
            value = json.load(handle)
    except FileNotFoundError:
        return None
    except (StorageError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        raise TranscriptAccessError("transcript metadata is unreadable") from exc
    if not isinstance(value, dict):
        raise TranscriptAccessError("transcript metadata is invalid")
    return value


def _validate_metadata(
    metadata: Mapping[str, Any],
    root: Path,
    binding_digest: str,
    *,
    owner_token: str,
    parent_session_id: str,
    profile: str,
) -> None:
    if metadata.get("version") != 1 or metadata.get("root") != str(root):
        raise TranscriptAccessError("transcript metadata root mismatch")
    owner_digest = _owner_digest(owner_token)
    if metadata.get("owner_token_sha256") != owner_digest:
        raise TranscriptAccessError("owner token mismatch")
    if metadata.get("parent_session_id") != parent_session_id:
        raise TranscriptAccessError("parent session mismatch")
    if metadata.get("profile") != profile:
        raise TranscriptAccessError("profile mismatch")
    if metadata.get("identity_sha256") != _identity_digest(
        owner_digest, parent_session_id, profile
    ):
        raise TranscriptAccessError("transcript metadata identity mismatch")
    for name in ("run_id", "task_id"):
        if not isinstance(metadata.get(name), str) or not metadata[name]:
            raise TranscriptAccessError("transcript metadata identity mismatch")
    for name in ("generation", "attempt"):
        if type(metadata.get(name)) is not int or metadata[name] < 0:
            raise TranscriptAccessError("transcript metadata identity mismatch")
    expected_binding = _binding_digest(
        owner_digest,
        metadata["run_id"],
        metadata["task_id"],
        metadata["generation"],
        metadata["attempt"],
        metadata["profile"],
        metadata["parent_session_id"],
    )
    if (
        metadata.get("binding_sha256") != binding_digest
        or expected_binding != binding_digest
    ):
        raise TranscriptAccessError("transcript metadata identity mismatch")


def read_transcript(
    handle: str,
    *,
    owner_token: str,
    parent_session_id: str,
    profile: str,
    root: Path | None = None,
    max_chars: int = 4000,
    offset: int = 0,
) -> dict[str, Any]:
    """Read a bounded byte cursor from an owner-authorized transcript."""
    max_chars, offset = _bounded_read_args(max_chars, offset)
    configured_root = _root(root)
    path, metadata_path, binding_digest = _validate_locator(
        handle,
        configured_root,
        owner_token=owner_token,
        parent_session_id=parent_session_id,
        profile=profile,
    )
    metadata = _load_metadata(metadata_path)
    if metadata is None:
        return _unavailable(offset)
    _validate_metadata(
        metadata,
        configured_root,
        binding_digest,
        owner_token=owner_token,
        parent_session_id=parent_session_id,
        profile=profile,
    )
    _assert_no_symlink_components(configured_root, path.relative_to(configured_root))
    try:
        with open_private_file(path, "r+b", create=False) as handle_file:
            handle_file.seek(offset)
            data = handle_file.read(max_chars)
    except FileNotFoundError:
        return _unavailable(offset)
    except StorageError as exc:
        raise TranscriptAccessError(str(exc)) from exc
    except (OSError, ValueError) as exc:
        raise TranscriptAccessError("transcript read failed") from exc
    if not isinstance(data, bytes):
        raise TranscriptAccessError("transcript read returned invalid data")
    return {
        "available": True,
        "text": data.decode("utf-8", errors="replace"),
        "offset": offset,
        "next_offset": offset + len(data),
        "bounded": True,
    }
