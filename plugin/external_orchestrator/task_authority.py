"""Host-owned, task-scoped authority for native child dispatch.

The native adapter is a host boundary: packet text is untrusted input, while the
parent binding and the authority object below are created by the host.  The
actual native tool path already exposes two ContextVar-scoped enforcement
interfaces:

* ``agent.delegation_context`` checks the capability and concrete file target;
* ``agent.required_tool_policy`` checks every tool at dispatch and composes
  nested policy generations.

This module only binds those existing interfaces to one immutable task identity.
It deliberately does not patch registries, monkeypatch tool handlers, or use
process-global mutable permission state.
"""

from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
import json
import os
from pathlib import Path
from threading import Event
from typing import Iterator, Mapping, Iterable


class TaskAuthorityError(PermissionError):
    """A native task packet cannot be admitted or its effect is not authorized."""


class TaskAuthority:
    """One host-owned authority lease for exactly one native task attempt.

    ``host_binding`` is the adapter's already captured ``(profile, session,
    owner_token)`` tuple.  It is intentionally not accepted from model output.
    The object is immutable from the caller's perspective except for the
    host-owned revocation event, which is shared by all copied execution
    contexts and therefore remains effective across worker threads.
    """

    __slots__ = (
        "run_id",
        "task_id",
        "generation",
        "capability_profile",
        "write_scope",
        "write_owner_token",
        "host_binding",
        "parent_tool_ceiling",
        "_allowed_tools",
        "_revoked",
        "_policy",
        "_sealed",
    )

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_sealed", False):
            raise AttributeError("task authority is immutable")
        object.__setattr__(self, name, value)

    def __init__(
        self,
        *,
        run_id: str,
        task_id: str,
        generation: int,
        capability_profile: str,
        write_scope: Iterable[str],
        write_owner_token: str,
        host_binding: tuple[str, str, str],
        parent_tool_ceiling: Iterable[str] | None = None,
    ) -> None:
        self.run_id = run_id
        self.task_id = task_id
        self.generation = generation
        self.capability_profile = capability_profile
        self.write_scope = tuple(write_scope)
        self.write_owner_token = write_owner_token
        self.host_binding = tuple(host_binding)
        self.parent_tool_ceiling = (
            None
            if parent_tool_ceiling is None
            else frozenset(str(name) for name in parent_tool_ceiling)
        )

        # The packet is admitted only after the profile has been validated in
        # ``from_packet``.  Keep this lookup fail-closed as a second line of
        # defense if the object is constructed directly.
        self._allowed_tools = self._capability_tools(capability_profile)
        self._revoked = Event()

        # RequiredToolPolicy is the native dispatch guard.  Its scope composes
        # with the parent's captured policy, and its revoke event is checked at
        # ingress and immediately before handler admission.
        from agent.required_tool_policy import RequiredToolPolicy

        self._policy = RequiredToolPolicy(self.allows_tool)
        self._sealed = True

    @classmethod
    def from_packet(
        cls,
        packet: Mapping[str, object],
        host_binding: tuple[str, str, str],
        parent_tool_ceiling: Iterable[str] | None = None,
    ) -> "TaskAuthority":
        """Create an authority only for a packet owned by the captured host.

        The external scheduler currently carries the host ``owner_token`` but
        not a separate write token.  Consequently the write token is derived
        here from host ownership plus the full task identity and canonical
        scope list; a packet field cannot forge it.
        """
        if not isinstance(packet, Mapping):
            raise TaskAuthorityError("native task packet must be a mapping")
        binding = cls._binding(host_binding)
        packet_binding = tuple(
            packet.get(key) for key in ("profile", "parent_session_id", "owner_token")
        )
        if packet_binding != binding:
            raise TaskAuthorityError("native task host authority mismatch")

        run_id = cls._required_text(packet.get("run_id"), "run_id")
        task_id = cls._required_text(packet.get("task_id"), "task_id")
        generation = packet.get("generation")
        if type(generation) is not int or generation < 0:
            raise TaskAuthorityError("generation must be a non-negative integer")

        capability = str(packet.get("capability_profile") or "").strip().lower()
        try:
            from tools.delegation_contracts import CapabilityProfile

            CapabilityProfile(capability)
        except (ImportError, TypeError, ValueError) as exc:
            raise TaskAuthorityError(
                f"unsupported capability_profile {capability!r}"
            ) from exc

        raw_scope = packet.get("write_scope", ())
        if not isinstance(raw_scope, (list, tuple)):
            raise TaskAuthorityError("write_scope must be a list or tuple of paths")
        if len(raw_scope) > 64:
            raise TaskAuthorityError("write_scope contains too many paths")
        scope = []
        for item in raw_scope:
            if not isinstance(item, str) or not item.strip():
                raise TaskAuthorityError(
                    "write_scope entries must be non-empty strings"
                )
            scope.append(cls._canonical_path(item))

        # A recognized workspace-write packet must carry a real scope.  Other
        # profiles may contain stale metadata, but that metadata never grants a
        # write because their capability allowlist excludes write_file/patch.
        if capability == "workspace-write" and not scope:
            raise TaskAuthorityError("workspace-write requires a non-empty write_scope")

        write_owner_token = derive_write_owner_token(
            binding[2], run_id, task_id, generation, tuple(scope)
        )
        return cls(
            run_id=run_id,
            task_id=task_id,
            generation=generation,
            capability_profile=capability,
            write_scope=tuple(scope),
            write_owner_token=write_owner_token,
            host_binding=binding,
            parent_tool_ceiling=parent_tool_ceiling,
        )

    @staticmethod
    def _binding(value: object) -> tuple[str, str, str]:
        if not isinstance(value, (tuple, list)) or len(value) != 3:
            raise TaskAuthorityError(
                "host binding must be (profile, session, owner_token)"
            )
        result = tuple(value)
        if any(not isinstance(item, str) or not item for item in result):
            raise TaskAuthorityError("host binding fields must be non-empty strings")
        return result  # type: ignore[return-value]

    @staticmethod
    def _required_text(value: object, name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise TaskAuthorityError(f"{name} must be non-empty text")
        return value.strip()

    @staticmethod
    def _canonical_path(value: str) -> str:
        raw = os.path.expanduser(value.strip())
        return os.path.normcase(os.path.realpath(os.path.abspath(raw)))

    @staticmethod
    def _capability_tools(capability: str) -> frozenset[str]:
        try:
            from tools.delegation_contracts import (
                CAPABILITY_TOOL_ALLOWLISTS,
                CapabilityProfile,
            )

            return frozenset(CAPABILITY_TOOL_ALLOWLISTS[CapabilityProfile(capability)])
        except (ImportError, KeyError, TypeError, ValueError):
            return frozenset()

    @property
    def revoked(self) -> bool:
        return self._revoked.is_set()

    def revoke(self, reason: str = "host revoked") -> None:
        """Revoke this exact attempt for every active/copied context."""
        self._revoked.set()
        self._policy.revoke()

    def allows_tool(
        self, tool_name: str, args: Mapping[str, object] | None = None
    ) -> bool:
        """Return whether dispatch may admit one tool call, fail-closed."""
        if self.revoked or not isinstance(tool_name, str):
            return False
        if tool_name not in self._allowed_tools:
            return False
        if (
            self.parent_tool_ceiling is not None
            and tool_name not in self.parent_tool_ceiling
        ):
            return False
        return True

    def authorize_tool(
        self, tool_name: str, args: Mapping[str, object] | None = None
    ) -> None:
        if not self.allows_tool(tool_name, args):
            raise TaskAuthorityError(
                f"tool {tool_name!r} is outside the task authority"
            )

    def authorize_write(self, path: str | Path) -> None:
        """Delegate concrete path checking to the existing native guard."""
        self.authorize_tool("write_file")
        from agent.delegation_context import authorize_delegated_write

        authorize_delegated_write(path)

    @contextmanager
    def scope(self, *, session_id: str | None = None) -> Iterator[None]:
        """Bind identity and dispatch policy only for this execution scope."""
        if self.revoked:
            raise TaskAuthorityError("native task authority is revoked")
        from agent.delegation_context import delegated_child_context

        with delegated_child_context(
            session_id=session_id,
            run_id=self.run_id,
            task_id=self.task_id,
            generation=self.generation,
            write_owner_token=self.write_owner_token,
            write_scope=self.write_scope,
            capability_profile=self.capability_profile,
        ):
            with self._policy.scope():
                yield


def derive_write_owner_token(
    host_owner_token: str,
    run_id: str,
    task_id: str,
    generation: int,
    write_scope: Iterable[str],
) -> str:
    """Derive a deterministic, host-owned token for one task attempt."""
    payload = json.dumps(
        [host_owner_token, run_id, task_id, generation, list(write_scope)],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return sha256(b"hermes-native-task-authority\0" + payload).hexdigest()


def parent_tool_ceiling(parent: object) -> frozenset[str] | None:
    """Snapshot the parent's effective tool names without mutating globals.

    A populated ``valid_tool_names`` is the strongest existing native ceiling.
    For a test double or an agent before first tool assembly, derive names from
    its explicit enabled/disabled toolsets.  ``None`` means the parent itself
    has not declared a narrower ceiling; it does not widen an active required
    policy, which remains conjunctive in ``RequiredToolPolicy``.
    """
    valid = getattr(parent, "valid_tool_names", None)
    if isinstance(valid, (set, frozenset, list, tuple)):
        return frozenset(str(name) for name in valid)

    enabled = getattr(parent, "enabled_toolsets", None)
    if enabled is None:
        return None
    if not isinstance(enabled, (set, frozenset, list, tuple)):
        return frozenset()

    try:
        from toolsets import TOOLSETS

        names = {
            str(tool)
            for toolset in enabled
            for tool in (TOOLSETS.get(str(toolset), {}) or {}).get("tools", ())
        }
        disabled = getattr(parent, "disabled_toolsets", ())
        for toolset in (
            disabled if isinstance(disabled, (set, frozenset, list, tuple)) else ()
        ):
            names.difference_update(
                str(tool)
                for tool in (TOOLSETS.get(str(toolset), {}) or {}).get("tools", ())
            )
        return frozenset(names)
    except Exception:
        # An explicit parent ceiling that cannot be inspected must not widen.
        return frozenset()


# Descriptive alias for callers that prefer verb-oriented naming.
bind_task_authority = TaskAuthority.from_packet


__all__ = [
    "TaskAuthority",
    "TaskAuthorityError",
    "bind_task_authority",
    "derive_write_owner_token",
    "parent_tool_ceiling",
]
