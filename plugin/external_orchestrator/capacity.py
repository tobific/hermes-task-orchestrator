"""Kernel-held capacity slots shared across profile-local scheduler stores.

Slot files are never removed: unlinking a locked inode would split the lock.
Descriptors remain held until result settlement; process death releases them.
Write-scope metadata lives in the same locked host-slot inode, so ownership ends
with the slot's actual kernel lock rather than a PID or timestamp heuristic.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
from .scope_identity import scope_identity, valid_identities, identities_overlap

from .storage import (
    ensure_private_directory,
    open_private_file,
    repair_private_tree,
    configured_storage_directory,
)

HARD_CAP = 16
_METADATA_VERSION = 3
_MAX_SCOPE_ITEMS = 16
_MAX_SCOPE_CHARS = 4096
# Sixteen supported paths at the input bound.  Six bytes per character covers
# worst-case JSON escaping (which is larger than UTF-8 for control characters),
# plus the fixed JSON envelope.  This is bounded without rejecting any scope
# declaration accepted by _canonical_scopes.
_MAX_METADATA_BYTES = 2 * 1024 * 1024


class SharedCapacity:
    def __init__(self, root, per_profile):
        if type(per_profile) is not int or not 1 <= per_profile <= HARD_CAP:
            raise ValueError("invalid host worker capacity")
        self.root = ensure_private_directory(configured_storage_directory(root))
        repair_private_tree(self.root)
        self.per_profile = per_profile

    @staticmethod
    def _slot(directory, count):
        directory = ensure_private_directory(directory)
        for i in range(count):
            handle = open_private_file(directory / str(i), "a+b")
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                handle.close()
                continue
            except BaseException:
                handle.close()
                raise
            return handle
        return None

    @staticmethod
    def _canonical_scopes(write_scope):
        if write_scope is None:
            return ()
        if not isinstance(write_scope, (list, tuple)):
            raise ValueError("write_scope must be a list of strings")
        if len(write_scope) > _MAX_SCOPE_ITEMS:
            raise ValueError("write_scope contains too many paths")
        scopes = []
        for value in write_scope:
            if not isinstance(value, str) or not value or "\x00" in value:
                raise ValueError("write_scope contains an invalid path")
            if len(value) > _MAX_SCOPE_CHARS:
                raise ValueError("write_scope path is too long")
            try:
                canonical = Path(os.path.abspath(os.path.expanduser(value))).resolve(
                    strict=False
                )
            except (OSError, RuntimeError, ValueError) as exc:
                raise ValueError("write_scope path cannot be canonicalized") from exc
            encoded = os.fspath(canonical)
            if len(encoded) > _MAX_SCOPE_CHARS:
                raise ValueError("canonical write_scope path is too long")
            if encoded not in scopes:
                scopes.append(encoded)
        return tuple(scopes)

    @staticmethod
    def _scopes_overlap(left, right):
        for a in left:
            pa = Path(a)
            for b in right:
                pb = Path(b)
                if pa == pb or pa in pb.parents or pb in pa.parents:
                    return True
        return False

    @classmethod
    def _metadata_bytes(cls, scopes, identities=None):
        if identities is None:
            identities = [scope_identity(p) for p in scopes]
        payload = json.dumps(
            {
                "version": _METADATA_VERSION,
                "scopes": list(scopes),
                "identities": identities,
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        if len(payload) > _MAX_METADATA_BYTES:
            raise ValueError("write-scope metadata exceeds its bound")
        return payload

    @classmethod
    def _publish_metadata(cls, handle, scopes):
        payload = cls._metadata_bytes(scopes)
        handle.seek(0)
        handle.truncate()
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
        handle.seek(0)

    @classmethod
    def _read_metadata(cls, handle):
        try:
            handle.seek(0)
            raw = handle.read(_MAX_METADATA_BYTES + 1)
            if len(raw) > _MAX_METADATA_BYTES:
                return None
            value = json.loads(raw.decode("utf-8"))
            if not isinstance(value, dict) or value.get("version") != _METADATA_VERSION:
                return None
            raw_scopes = value.get("scopes")
            if not isinstance(raw_scopes, list):
                return None
            scopes = cls._canonical_scopes(raw_scopes)
            if list(scopes) != raw_scopes:
                return None
            # Re-encoding also rejects unbounded/ambiguous object shapes.
            identities = value.get("identities")
            if not valid_identities(identities, len(scopes)):
                return None
            if cls._metadata_bytes(scopes, identities) != raw:
                return None
            return value
        except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
            return None

    def _active_scope_conflict(self, requested):
        """Return true for an active overlapping slot or malformed active metadata.

        A successful non-blocking probe means the slot is unlocked, so any
        metadata left by a dead/stale owner is ignored.  Active metadata is read
        through a separate descriptor while the owner's lock remains held.
        """
        if not requested:
            return False
        requested_identities = [scope_identity(p) for p in requested]
        host = self.root / "host"
        for index in range(HARD_CAP):
            try:
                probe = open_private_file(host / str(index), "r+b", create=False)
            except FileNotFoundError:
                continue
            try:
                try:
                    fcntl.flock(probe.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    active = True
                else:
                    active = False
                    fcntl.flock(probe.fileno(), fcntl.LOCK_UN)
                if not active:
                    continue
                scopes = self._read_metadata(probe)
                if (
                    scopes is None
                    or self._scopes_overlap(requested, scopes["scopes"])
                    or identities_overlap(requested_identities, scopes["identities"])
                ):
                    return True
            finally:
                probe.close()
        return False

    def acquire(self, profile, write_scope=None):
        """Acquire profile and host slots, atomically excluding live scopes."""
        scopes = self._canonical_scopes(write_scope)
        try:
            admission = open_private_file(self.root / "admission.lock", "a+b")
        except FileNotFoundError:
            # Concurrent first creation can transiently miss on Darwin. No
            # slot was acquired: let the bounded caller pump retry admission.
            return None
        admission_locked = False
        try:
            # This lock is deliberately short-lived.  Slot locks, not this
            # mutex, carry ownership through worker execution and process death.
            try:
                fcntl.flock(admission.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                # A contending scheduler must return to its pump.  Blocking
                # here would bypass queue deadlines and cancellation.
                return None
            admission_locked = True
            key = hashlib.sha256(str(profile).encode()).hexdigest()
            profile_slot = self._slot(self.root / "profiles" / key, self.per_profile)
            if profile_slot is None:
                return None
            try:
                host_slot = self._slot(self.root / "host", HARD_CAP)
                if host_slot is None:
                    profile_slot.close()
                    return None
                try:
                    # A newly acquired slot may contain stale metadata.  Clear
                    # it while locked before probing the other host slots.
                    self._publish_metadata(host_slot, ())
                    if self._active_scope_conflict(scopes):
                        host_slot.close()
                        profile_slot.close()
                        return None
                    self._publish_metadata(host_slot, scopes)
                    return (profile_slot, host_slot)
                except BaseException:
                    host_slot.close()
                    raise
            except BaseException:
                profile_slot.close()
                raise
        finally:
            try:
                if admission_locked:
                    fcntl.flock(admission.fileno(), fcntl.LOCK_UN)
            finally:
                admission.close()

    @staticmethod
    def release(slots):
        for handle in slots:
            handle.close()


def host_worker_capacity():
    """Host config is authoritative; unset retains existing default of three."""
    from hermes_cli.config import load_config_readonly

    cfg = load_config_readonly() or {}
    delegation = cfg.get("delegation", {})
    if not isinstance(delegation, dict):
        raise ValueError("invalid delegation configuration")
    scheduler = delegation.get("scheduler", {})
    if not isinstance(scheduler, dict):
        raise ValueError("invalid scheduler configuration")
    workers = scheduler.get("max_workers", 3)
    if type(workers) is not int or not 1 <= workers <= HARD_CAP:
        raise ValueError("max_workers must be an integer from 1 to 16")
    return workers


WORKER_MIN_PACKET_TOKENS = 256
WORKER_MAX_PACKET_TOKENS = 272_000
WORKER_DEFAULT_PACKET_TOKENS = 12_000
WORKER_MIN_RESULT_CHARS = 256
WORKER_MAX_RESULT_CHARS = 100_000
WORKER_DEFAULT_RESULT_CHARS = 14_000


def host_worker_limits():
    """Read host-owned worker ceilings; malformed present values fail closed."""
    from hermes_cli.config import load_config_readonly

    cfg = load_config_readonly() or {}
    if not isinstance(cfg, dict):
        raise ValueError("invalid host configuration")
    delegation = cfg.get("delegation", {})
    if not isinstance(delegation, dict):
        raise ValueError("invalid delegation configuration")
    worker = delegation.get("worker", {})
    if not isinstance(worker, dict):
        raise ValueError("invalid worker configuration")

    def bounded(name, default, minimum, maximum):
        value = worker.get(name, default)
        if type(value) is not int or not minimum <= value <= maximum:
            raise ValueError(f"{name} must be an integer from {minimum} to {maximum}")
        return value

    return {
        "max_packet_tokens": bounded(
            "max_packet_tokens",
            WORKER_DEFAULT_PACKET_TOKENS,
            WORKER_MIN_PACKET_TOKENS,
            WORKER_MAX_PACKET_TOKENS,
        ),
        "max_result_chars": bounded(
            "max_result_chars",
            WORKER_DEFAULT_RESULT_CHARS,
            WORKER_MIN_RESULT_CHARS,
            WORKER_MAX_RESULT_CHARS,
        ),
    }


def host_scheduler_limits():
    """Read queue policy from host config; invalid present values fail closed."""
    from hermes_cli.config import load_config_readonly

    cfg = load_config_readonly() or {}
    if not isinstance(cfg, dict):
        raise ValueError("invalid host configuration")
    delegation = cfg.get("delegation", {})
    if not isinstance(delegation, dict):
        raise ValueError("invalid delegation configuration")
    scheduler = delegation.get("scheduler", {})
    if not isinstance(scheduler, dict):
        raise ValueError("invalid scheduler configuration")

    def bounded(name, default, maximum):
        value = scheduler.get(name, default)
        if type(value) is not int or not 1 <= value <= maximum:
            raise ValueError(f"{name} must be an integer from 1 to {maximum}")
        return value

    fairness = scheduler.get("fairness", "weighted_deficit_round_robin")
    if fairness != "weighted_deficit_round_robin":
        raise ValueError("unsupported scheduler fairness policy")
    return {
        "max_tasks_per_run": bounded("max_tasks_per_run", 64, 64),
        "max_queued_tasks": bounded("max_queued_tasks", 128, 128),
        "fairness": fairness,
    }
