"""Host-owned parent bindings and pinned admission for shared plugin runs."""

from collections import OrderedDict
from threading import RLock
from .native_worker import NativeWorkerAdapter
from .scheduler import HostFailure, OrchestrationError, packet_identity


class HostDispatch:
    def __init__(self, route_resolver, admission_policy=None, worker_policy=None):
        self.route_resolver = route_resolver
        self.admission_policy = admission_policy
        self.worker_policy = worker_policy
        self._lock = RLock()
        self._owners = OrderedDict()
        self._attempts = {}

    @staticmethod
    def key(packet):
        return tuple(
            packet.get(k) for k in ("profile", "parent_session_id", "owner_token")
        )

    def bind(self, parent):
        options = {"admission_policy": self.admission_policy} if self.admission_policy is not None else {}
        if self.worker_policy is not None:
            options["worker_policy"] = self.worker_policy
        adapter = NativeWorkerAdapter(parent, self.route_resolver, **options)
        with self._lock:
            self._owners[adapter._binding] = adapter
            self._owners.move_to_end(adapter._binding)
            while len(self._owners) > 128:
                self._owners.popitem(last=False)

    def admission_preflight(self, packet):
        with self._lock:
            adapter = self._owners.get(self.key(packet))
        if adapter is None:
            # Durable queue recovery may precede parent re-binding. Check only
            # the exact current host profile here; can_admit still denies execution.
            if self.admission_policy is not None:
                self.admission_policy(packet.get("profile"))
            return
        adapter.admission_preflight(packet)

    def can_admit(self, packet):
        try:
            self.admission_preflight(packet)
        except Exception:
            return False
        with self._lock:
            adapter = self._owners.get(self.key(packet))
            return (
                adapter is not None
                and not getattr(adapter.parent, "is_closed", False)
                and getattr(adapter.parent, "session_id", None) == adapter._binding[1]
                # Live parent must still be on Codex; otherwise the task waits.
                and getattr(adapter.parent, "provider", None) == "openai-codex"
                and getattr(adapter.parent, "api_mode", None) == "codex_responses"
            )

    def quota_preflight(self, packet):
        """Check quota in the exact captured host context, outside this lock."""
        with self._lock:
            adapter = self._owners.get(self.key(packet))
        if adapter is None:
            raise OrchestrationError("no host admission binding")
        return adapter.quota_preflight(packet)

    def reserve(self, packet):
        """Pin the exact captured host context before durable admission."""
        if not self.can_admit(packet):
            return False
        with self._lock:
            adapter = self._owners.get(self.key(packet))
            if (
                adapter is None
                or getattr(adapter.parent, "is_closed", False)
                or getattr(adapter.parent, "session_id", None) != adapter._binding[1]
            ):
                return False
            identity = packet_identity(packet)
            if identity in self._attempts:
                return False
            self._attempts[identity] = adapter
            return True

    def release(self, identity):
        with self._lock:
            self._attempts.pop(identity, None)

    def revoke(self, identity, reason):
        with self._lock:
            adapter = self._attempts.get(identity)
        if adapter is not None:
            adapter.revoke(identity, reason)

    def __call__(self, packet):
        identity = packet_identity(packet)
        with self._lock:
            adapter = self._attempts.get(identity)
        if adapter is None:
            return HostFailure(identity, "POLICY_DENIED", "no host admission binding")
        return adapter(packet)
