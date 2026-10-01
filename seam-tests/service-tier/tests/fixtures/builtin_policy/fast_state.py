"""Persistent two-state storage and guarded tier policy for Hermes Fast mode."""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import re
import threading
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Mapping
from urllib.parse import urlsplit

from hermes_constants import get_hermes_home

SCHEMA_VERSION = 1
MAX_RECENT_COMMANDS = 32
STATE_FILENAME = "fast-mode.json"
LOCK_FILENAME = "fast-mode.lock"
STATE_MODE = 0o600
FAST_MODE_SCOPE_FOREGROUND_SOL = "foreground_default"
FAST_MODE_SCOPE_PRO = "explicit_override"
FAST_MODE_SCOPE_EXAMPLE_JOB = "example_job"
FAST_MODE_SCOPE_APPROVAL = "approval_replay"
FAST_MODE_SCOPE_WORKER_ADAPTIVE = "worker_adaptive"
FAST_MODE_SCOPE_WORKER_DEFAULT = "worker_default"
DIRECT_CODEX_BASE_URL = "https://chatgpt.com/backend-api/codex"
FAST_MODE_CONFIRMATION_ON = "Fast mode on all included Codex-direct routes is now ON."
FAST_MODE_CONFIRMATION_OFF = "Fast mode on all included Codex-direct routes is now OFF."
FAST_MODE_USAGE_MESSAGE = "Usage: /fast"
FAST_MODE_UNAUTHORIZED_MESSAGE = "You are not authorized to use /fast."
_FAST_MODE_SCOPE_MODELS = {
    FAST_MODE_SCOPE_FOREGROUND_SOL: "gpt-6-astra",
    FAST_MODE_SCOPE_PRO: "gpt-6-astra",
    FAST_MODE_SCOPE_EXAMPLE_JOB: "gpt-6-astra",
    FAST_MODE_SCOPE_APPROVAL: "gpt-6-astra",
    FAST_MODE_SCOPE_WORKER_ADAPTIVE: "gpt-6-luna",
    FAST_MODE_SCOPE_WORKER_DEFAULT: "gpt-6-astra",
}
_OFF_TIMESTAMP = "1970-01-01T00:00:00Z"
_KEY_HASH_RE = re.compile(r"^[0-9a-f]{64}$")
_STATE_KEYS = {
    "schema_version",
    "enabled",
    "generation",
    "updated_at",
    "recent_commands",
}


class FastModeStateError(RuntimeError):
    """Raised when persisted Fast-mode state is unavailable or invalid."""


class _FastModeAtomicCommitError(FastModeStateError):
    """Raised only when the old state is known to remain intact."""


class _FastModeStateMissingError(FastModeStateError):
    """Raised when initialization is safe because no state file exists."""


class FastModeAuthorizationError(PermissionError):
    """Raised before parsing when a caller cannot mutate profile-wide Fast mode."""


class FastModeUsageError(ValueError):
    """Raised when anything follows the bare /fast command."""


@dataclass(frozen=True, slots=True)
class RecentCommand:
    key_hash: str
    generation: int
    enabled: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "key_hash": self.key_hash,
            "generation": self.generation,
            "enabled": self.enabled,
        }


@dataclass(frozen=True, slots=True)
class FastModeState:
    schema_version: int
    enabled: bool
    generation: int
    updated_at: str
    recent_commands: tuple[RecentCommand, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "enabled": self.enabled,
            "generation": self.generation,
            "updated_at": self.updated_at,
            "recent_commands": [item.to_dict() for item in self.recent_commands],
        }


@dataclass(frozen=True, slots=True)
class ToggleResult:
    enabled: bool
    generation: int
    duplicate: bool
    state: FastModeState


@dataclass(frozen=True, slots=True)
class FastModeCommandResult:
    message: str
    toggle: ToggleResult


@dataclass(frozen=True, slots=True)
class _CacheEntry:
    fingerprint: tuple[int, int, int]
    state: FastModeState


_CACHE: dict[Path, _CacheEntry] = {}
_CACHE_LOCK = threading.RLock()


def _normalize_state_path(state_path: Path | str | None) -> Path:
    path = (
        Path(state_path)
        if state_path is not None
        else Path(get_hermes_home()) / STATE_FILENAME
    )
    return path.expanduser().absolute()


def _lock_path_for(state_path: Path) -> Path:
    return state_path.with_name(LOCK_FILENAME)


def _fingerprint(stat_result: os.stat_result) -> tuple[int, int, int]:
    return (stat_result.st_ino, stat_result.st_mtime_ns, stat_result.st_size)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _off_state() -> FastModeState:
    return FastModeState(
        schema_version=SCHEMA_VERSION,
        enabled=False,
        generation=0,
        updated_at=_OFF_TIMESTAMP,
        recent_commands=(),
    )


def _parse_timestamp(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise FastModeStateError(
            "updated_at must be a non-empty timezone-aware timestamp"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FastModeStateError("updated_at is not valid ISO 8601") from exc
    if parsed.tzinfo is None:
        raise FastModeStateError("updated_at must include a timezone")
    return value


def _parse_recent_command(value: Any, *, state_generation: int) -> RecentCommand:
    if not isinstance(value, Mapping):
        raise FastModeStateError("recent_commands entries must be objects")
    if set(value) != {"key_hash", "generation", "enabled"}:
        raise FastModeStateError("recent_commands entry has an invalid schema")

    key_hash = value.get("key_hash")
    generation = value.get("generation")
    enabled = value.get("enabled")
    if not isinstance(key_hash, str) or not _KEY_HASH_RE.fullmatch(key_hash):
        raise FastModeStateError(
            "recent command key_hash must be lowercase SHA-256 hex"
        )
    if isinstance(generation, bool) or not isinstance(generation, int):
        raise FastModeStateError("recent command generation must be an integer")
    if generation < 0 or generation > state_generation:
        raise FastModeStateError(
            "recent command generation is outside the state history"
        )
    if not isinstance(enabled, bool):
        raise FastModeStateError("recent command enabled must be a boolean")
    return RecentCommand(key_hash=key_hash, generation=generation, enabled=enabled)


def _parse_state(value: Any) -> FastModeState:
    if not isinstance(value, Mapping):
        raise FastModeStateError("Fast-mode state must be a JSON object")
    if set(value) != _STATE_KEYS:
        raise FastModeStateError("Fast-mode state has an invalid schema")

    schema_version = value.get("schema_version")
    enabled = value.get("enabled")
    generation = value.get("generation")
    recent_raw = value.get("recent_commands")

    if schema_version != SCHEMA_VERSION:
        raise FastModeStateError(
            f"unsupported Fast-mode state schema_version {schema_version!r}"
        )
    if not isinstance(enabled, bool):
        raise FastModeStateError("enabled must be a boolean")
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
    ):
        raise FastModeStateError("generation must be a non-negative integer")
    if not isinstance(recent_raw, list):
        raise FastModeStateError("recent_commands must be a list")
    if len(recent_raw) > MAX_RECENT_COMMANDS:
        raise FastModeStateError(
            f"recent_commands exceeds the {MAX_RECENT_COMMANDS}-entry limit"
        )

    recent_commands = tuple(
        _parse_recent_command(item, state_generation=generation) for item in recent_raw
    )
    hashes = [item.key_hash for item in recent_commands]
    if len(hashes) != len(set(hashes)):
        raise FastModeStateError("recent_commands contains duplicate key hashes")

    return FastModeState(
        schema_version=SCHEMA_VERSION,
        enabled=enabled,
        generation=generation,
        updated_at=_parse_timestamp(value.get("updated_at")),
        recent_commands=recent_commands,
    )


def _read_state_uncached(path: Path) -> FastModeState:
    try:
        with path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except FileNotFoundError as exc:
        raise _FastModeStateMissingError(
            f"Fast-mode state file does not exist: {path}"
        ) from exc
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FastModeStateError(f"cannot read Fast-mode state file: {path}") from exc
    return _parse_state(raw)


def clear_fast_mode_state_cache(state_path: Path | str | None = None) -> None:
    """Drop one cached state snapshot, or every snapshot when no path is given."""

    with _CACHE_LOCK:
        if state_path is None:
            _CACHE.clear()
        else:
            _CACHE.pop(_normalize_state_path(state_path), None)


def read_fast_mode_state(
    *,
    state_path: Path | str | None = None,
    strict: bool = True,
) -> FastModeState:
    """Read a stable state snapshot, failing closed to OFF when requested."""

    path = _normalize_state_path(state_path)
    try:
        before = path.stat()
        before_fingerprint = _fingerprint(before)
        with _CACHE_LOCK:
            cached = _CACHE.get(path)
        if cached is not None and cached.fingerprint == before_fingerprint:
            # Recheck after the cache lookup so an atomic replacement between
            # the first stat and the lookup cannot return the stale snapshot.
            confirmed_fingerprint = _fingerprint(path.stat())
            if confirmed_fingerprint == before_fingerprint:
                return cached.state

        state: FastModeState | None = None
        for _attempt in range(3):
            before = path.stat()
            before_fingerprint = _fingerprint(before)
            state = _read_state_uncached(path)
            after = path.stat()
            after_fingerprint = _fingerprint(after)
            if before_fingerprint == after_fingerprint:
                with _CACHE_LOCK:
                    _CACHE[path] = _CacheEntry(after_fingerprint, state)
                return state

        raise FastModeStateError(f"could not obtain a stable state snapshot: {path}")
    except FileNotFoundError as exc:
        if strict:
            raise _FastModeStateMissingError(
                f"Fast-mode state file does not exist: {path}"
            ) from exc
        return _off_state()
    except (FastModeStateError, OSError):
        if strict:
            raise
        return _off_state()


def _is_direct_codex_runtime(
    *,
    provider: str | None,
    api_mode: str | None,
    base_url: str | None,
) -> bool:
    if str(provider or "").strip().lower() != "openai-codex":
        return False
    if str(api_mode or "").strip().lower() != "codex_responses":
        return False
    try:
        actual = urlsplit(str(base_url or ""))
        required = urlsplit(DIRECT_CODEX_BASE_URL)
        actual_port = actual.port
    except (TypeError, ValueError):
        return False
    return bool(
        actual.scheme.lower() == required.scheme
        and (actual.hostname or "").lower() == (required.hostname or "").lower()
        and actual_port in {None, 443}
        and actual.path.rstrip("/") == required.path.rstrip("/")
        and actual.username is None
        and actual.password is None
        and not actual.query
        and not actual.fragment
    )


def _resolve_fast_mode_service_tier_for_state(
    *,
    enabled: bool,
    scope: str | None,
    provider: str | None,
    model: str | None,
    api_mode: str | None,
    base_url: str | None,
) -> str | None:
    expected_model = _FAST_MODE_SCOPE_MODELS.get(str(scope or ""))
    if expected_model is None or str(model or "").strip() != expected_model:
        return None
    if not _is_direct_codex_runtime(
        provider=provider,
        api_mode=api_mode,
        base_url=base_url,
    ):
        return None
    return "priority" if enabled else "normal"


def resolve_fast_mode_service_tier(
    *,
    scope: str,
    provider: str | None,
    model: str | None,
    api_mode: str | None,
    base_url: str | None,
    state_path: Path | str | None = None,
) -> str | None:
    """Resolve Standard or Priority for one explicitly included direct route.

    ``None`` means the route is outside the allowlist or failed the
    direct Codex provider guard. Callers must leave such routes unchanged.
    """

    state = read_fast_mode_state(state_path=state_path, strict=False)
    return _resolve_fast_mode_service_tier_for_state(
        enabled=state.enabled,
        scope=scope,
        provider=provider,
        model=model,
        api_mode=api_mode,
        base_url=base_url,
    )


def validate_fast_mode_route_matrix(enabled: bool) -> None:
    """Validate all five included routes and every permanent exclusion."""

    expected_tier = "priority" if enabled else "normal"
    failures: list[str] = []
    for scope, model in _FAST_MODE_SCOPE_MODELS.items():
        actual = _resolve_fast_mode_service_tier_for_state(
            enabled=enabled,
            scope=scope,
            provider="openai-codex",
            model=model,
            api_mode="codex_responses",
            base_url=DIRECT_CODEX_BASE_URL,
        )
        if actual != expected_tier:
            failures.append(f"{scope}: expected {expected_tier}, found {actual}")

    exclusions = {
        "fast_terra": (
            FAST_MODE_SCOPE_FOREGROUND_SOL,
            "openai-codex",
            "gpt-5.6-terra",
            "codex_responses",
            DIRECT_CODEX_BASE_URL,
        ),
        "opus_claude": (
            FAST_MODE_SCOPE_PRO,
            "claude-code",
            "claude-opus-5",
            "chat_completions",
            "acp://claude-code",
        ),
        "desktop_sticky": (
            None,
            "openai-codex",
            "gpt-5.6-sol",
            "codex_responses",
            DIRECT_CODEX_BASE_URL,
        ),
        "openrouter": (
            FAST_MODE_SCOPE_FOREGROUND_SOL,
            "openrouter",
            "openai/gpt-5.6-sol",
            "chat_completions",
            "https://openrouter.ai/api/v1",
        ),
        "other_provider": (
            FAST_MODE_SCOPE_FOREGROUND_SOL,
            "xai-oauth",
            "grok-4.6",
            "chat_completions",
            "https://api.x.ai/v1",
        ),
    }
    for name, (scope, provider, model, api_mode, base_url) in exclusions.items():
        actual = _resolve_fast_mode_service_tier_for_state(
            enabled=enabled,
            scope=scope,
            provider=provider,
            model=model,
            api_mode=api_mode,
            base_url=base_url,
        )
        if actual is not None:
            failures.append(f"{name}: exclusion resolved to {actual}")

    if failures:
        detail = "; ".join(failures)
        raise FastModeStateError(f"Fast-mode route matrix validation failed: {detail}")


def build_platform_command_key(
    platform: str,
    *,
    chat_id: str | int | None,
    thread_id: str | int | None = None,
    message_id: str | int | None = None,
    source_message_id: str | int | None = None,
    signal_sender_id: str | int | None = None,
    signal_timestamp: str | int | None = None,
    update_id: str | int | None = None,
) -> str:
    """Build a stable persisted replay key for a gateway delivery."""

    normalized_platform = str(platform or "").strip().lower()
    if not normalized_platform:
        raise ValueError("platform is required for replay protection")
    if normalized_platform == "telegram":
        normalized_update_id = str(update_id).strip() if update_id is not None else ""
        if normalized_update_id:
            # Preserve the existing direct-Telegram key shape so persisted
            # update-id deduplication remains stable across this fallback.
            identity = [normalized_platform, normalized_update_id]
        else:
            normalized_source_message_id = (
                str(source_message_id).strip() if source_message_id is not None else ""
            )
            if not normalized_source_message_id:
                raise ValueError(
                    "Telegram replay protection requires update_id or source_message_id"
                )
            normalized_chat_id = str(chat_id).strip() if chat_id is not None else ""
            if not normalized_chat_id:
                raise ValueError(
                    "Telegram source_message_id replay protection requires chat_id"
                )
            # Telegram message IDs are scoped to a chat. Namespace the relay
            # fallback separately from direct update IDs and include the chat
            # to prevent same-number collisions across chats.
            identity = [
                normalized_platform,
                "source_message_id",
                normalized_chat_id,
                normalized_source_message_id,
            ]
    else:
        stable_message_id = message_id
        if stable_message_id is None:
            stable_message_id = source_message_id
        normalized_message_id = (
            str(stable_message_id).strip() if stable_message_id is not None else ""
        )
        if normalized_message_id:
            delivery_identity = normalized_message_id
        elif normalized_platform == "signal":
            normalized_sender_id = (
                str(signal_sender_id).strip() if signal_sender_id is not None else ""
            )
            normalized_timestamp = (
                str(signal_timestamp).strip() if signal_timestamp is not None else ""
            )
            if not normalized_sender_id:
                raise ValueError(
                    "signal replay protection requires message_id or sender identity"
                )
            if not normalized_timestamp or normalized_timestamp == "0":
                raise ValueError(
                    "signal replay protection requires message_id or timestamp"
                )
            delivery_identity = (
                f"sender:{normalized_sender_id}:timestamp:{normalized_timestamp}"
            )
        else:
            raise ValueError(
                f"{normalized_platform} replay protection requires message_id"
            )
        identity = [
            normalized_platform,
            str(chat_id) if chat_id is not None else "",
            str(thread_id) if thread_id is not None else "",
            delivery_identity,
        ]

    return json.dumps(identity, ensure_ascii=False, separators=(",", ":"))


def _command_hash(command_key: str) -> str:
    if not isinstance(command_key, str) or not command_key:
        raise ValueError("command_key must be a non-empty string")
    return hashlib.sha256(command_key.encode("utf-8")).hexdigest()


@contextlib.contextmanager
def _state_lock(state_path: Path | str | None = None) -> Iterator[None]:
    """Serialize state writers across gateway, CLI, and TUI processes."""

    path = _normalize_state_path(state_path)
    lock_path = _lock_path_for(path)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, STATE_MODE)
    handle = os.fdopen(fd, "r+b", buffering=0)
    try:
        if os.name == "nt":
            import msvcrt

            if os.fstat(handle.fileno()).st_size == 0:
                handle.write(b"\0")
                handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _stage_state_file(target: Path, state: FastModeState) -> Path:
    """Write and fsync a private state file beside its eventual target."""

    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    tmp_path = Path(tmp_name)
    handle_open = True
    try:
        if hasattr(os, "fchmod"):
            os.fchmod(fd, STATE_MODE)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle_open = False
            json.dump(state.to_dict(), handle, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if not hasattr(os, "fchmod"):
            os.chmod(tmp_path, STATE_MODE)
        return tmp_path
    except BaseException:
        if handle_open:
            os.close(fd)
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _fsync_parent_directory(target: Path) -> None:
    """Persist a published directory entry on platforms that support it."""

    if os.name != "posix":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    directory_fd = os.open(target.parent, flags)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def _atomic_commit_state(path: Path, state: FastModeState) -> None:
    """Commit with rename-only atomicity; never fall back to an in-place copy."""

    target = Path(os.path.realpath(path)) if path.is_symlink() else path
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _stage_state_file(target, state)
    replaced = False
    try:
        os.replace(tmp_path, target)
        replaced = True
        _fsync_parent_directory(target)
    except OSError as exc:
        if replaced:
            raise FastModeStateError(
                f"Fast-mode state directory fsync failed after atomic replacement: {target}"
            ) from exc
        raise _FastModeAtomicCommitError(
            f"atomic Fast-mode state commit failed: {target}"
        ) from exc
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _atomic_create_state(path: Path, state: FastModeState) -> None:
    """Publish initial state only if the destination is still absent."""

    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = _stage_state_file(path, state)
    published = False
    try:
        os.link(tmp_path, path)
        published = True
        tmp_path.unlink()
        _fsync_parent_directory(path)
    except FileExistsError:
        raise
    except OSError as exc:
        if published:
            raise FastModeStateError(
                f"Fast-mode state directory fsync failed after atomic initialization: {path}"
            ) from exc
        raise _FastModeAtomicCommitError(
            f"atomic Fast-mode state initialization failed: {path}"
        ) from exc
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _write_state(path: Path, state: FastModeState) -> FastModeState:
    _atomic_commit_state(path, state)
    clear_fast_mode_state_cache(path)
    committed = read_fast_mode_state(state_path=path, strict=True)
    if committed != state:
        raise FastModeStateError("Fast-mode state read-back did not match the write")
    return committed


def _initialize_fast_mode_state_locked(path: Path) -> FastModeState:
    """Read existing state or create OFF without ever replacing a raced file."""

    try:
        return read_fast_mode_state(state_path=path, strict=True)
    except _FastModeStateMissingError:
        pass

    state = FastModeState(
        schema_version=SCHEMA_VERSION,
        enabled=False,
        generation=0,
        updated_at=_utc_now(),
        recent_commands=(),
    )
    try:
        _atomic_create_state(path, state)
    except FileExistsError:
        # A writer that does not share our lock won the no-clobber publish race.
        # Accept it only if its complete state passes the strict schema read.
        return read_fast_mode_state(state_path=path, strict=True)

    clear_fast_mode_state_cache(path)
    committed = read_fast_mode_state(state_path=path, strict=True)
    if committed != state:
        raise FastModeStateError(
            "Fast-mode initial state read-back did not match the write"
        )
    return committed


def initialize_fast_mode_state(
    *,
    state_path: Path | str | None = None,
) -> FastModeState:
    """Create the durable OFF state without resetting an existing valid state."""

    path = _normalize_state_path(state_path)
    with _state_lock(path):
        state = _initialize_fast_mode_state_locked(path)
        try:
            os.chmod(path, STATE_MODE)
        except OSError as exc:
            raise FastModeStateError(
                f"cannot secure Fast-mode state file: {path}"
            ) from exc
        return state


def toggle_fast_mode(
    *,
    command_key: str | None = None,
    state_path: Path | str | None = None,
) -> ToggleResult:
    """Atomically invert persisted state, deduplicating replayed platform commands."""

    path = _normalize_state_path(state_path)
    key_hash = _command_hash(command_key) if command_key is not None else None

    with _state_lock(path):
        previous = _initialize_fast_mode_state_locked(path)
        if key_hash is not None:
            for recent in reversed(previous.recent_commands):
                if recent.key_hash == key_hash:
                    return ToggleResult(
                        enabled=recent.enabled,
                        generation=recent.generation,
                        duplicate=True,
                        state=previous,
                    )

        next_generation = previous.generation + 1
        next_enabled = not previous.enabled
        recent_commands = list(previous.recent_commands)
        if key_hash is not None:
            recent_commands.append(
                RecentCommand(
                    key_hash=key_hash,
                    generation=next_generation,
                    enabled=next_enabled,
                )
            )
            recent_commands = recent_commands[-MAX_RECENT_COMMANDS:]

        proposed = FastModeState(
            schema_version=SCHEMA_VERSION,
            enabled=next_enabled,
            generation=next_generation,
            updated_at=_utc_now(),
            recent_commands=tuple(recent_commands),
        )
        validate_fast_mode_route_matrix(proposed.enabled)
        try:
            committed = _write_state(path, proposed)
        except _FastModeAtomicCommitError:
            raise
        except BaseException:
            try:
                _write_state(path, previous)
            except BaseException as rollback_exc:
                raise FastModeStateError(
                    "Fast-mode toggle failed and the previous state could not be restored"
                ) from rollback_exc
            raise

        return ToggleResult(
            enabled=committed.enabled,
            generation=committed.generation,
            duplicate=False,
            state=committed,
        )


def format_fast_mode_confirmation(enabled: bool) -> str:
    return FAST_MODE_CONFIRMATION_ON if enabled else FAST_MODE_CONFIRMATION_OFF


def execute_fast_mode_command(
    *,
    arguments: str | None,
    authorized: bool,
    replay_key: str,
    state_path: Path | str | None = None,
) -> FastModeCommandResult:
    """Run the bare /fast transaction shared by every interactive surface."""

    if authorized is not True:
        raise FastModeAuthorizationError(FAST_MODE_UNAUTHORIZED_MESSAGE)
    if str(arguments or "").strip():
        raise FastModeUsageError(FAST_MODE_USAGE_MESSAGE)
    toggle = toggle_fast_mode(command_key=replay_key, state_path=state_path)
    return FastModeCommandResult(
        message=format_fast_mode_confirmation(toggle.enabled),
        toggle=toggle,
    )


__all__ = [
    "DIRECT_CODEX_BASE_URL",
    "FAST_MODE_CONFIRMATION_OFF",
    "FAST_MODE_CONFIRMATION_ON",
    "FAST_MODE_SCOPE_WORKER_ADAPTIVE",
    "FAST_MODE_SCOPE_APPROVAL",
    "FAST_MODE_SCOPE_FOREGROUND_SOL",
    "FAST_MODE_SCOPE_PRO",
    "FAST_MODE_SCOPE_EXAMPLE_JOB",
    "FAST_MODE_UNAUTHORIZED_MESSAGE",
    "FAST_MODE_USAGE_MESSAGE",
    "FastModeAuthorizationError",
    "FastModeCommandResult",
    "FastModeState",
    "FastModeStateError",
    "FastModeUsageError",
    "RecentCommand",
    "ToggleResult",
    "build_platform_command_key",
    "clear_fast_mode_state_cache",
    "execute_fast_mode_command",
    "format_fast_mode_confirmation",
    "initialize_fast_mode_state",
    "read_fast_mode_state",
    "resolve_fast_mode_service_tier",
    "toggle_fast_mode",
    "validate_fast_mode_route_matrix",
]
