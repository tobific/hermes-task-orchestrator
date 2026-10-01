"""Host dispatch pins exact parent context at admission, not worker start."""

from types import SimpleNamespace
from test_native_worker_adapter import fixture, ROUTE, _packet
from external_orchestrator.host_binding import HostDispatch
from external_orchestrator.scheduler import packet_identity


def test_rebinding_same_session_does_not_replace_reserved_parent(fixture):
    first = SimpleNamespace(provider="openai-codex", api_mode="codex_responses", session_id="native-parent-session")
    second = SimpleNamespace(provider="openai-codex", api_mode="codex_responses", session_id="native-parent-session")
    dispatch = HostDispatch(lambda profile: dict(ROUTE))
    packet = _packet(task_id="pinned-parent")
    identity = packet_identity(packet)
    dispatch.bind(first)
    assert dispatch.can_admit(packet)
    assert dispatch.reserve(packet)
    assert not dispatch.reserve(packet)
    pinned = dispatch._attempts[identity]
    dispatch.bind(second)
    assert pinned.parent is first
    assert dispatch._owners[dispatch.key(packet)].parent is second
    dispatch.revoke(identity, "cancel before worker entry")
    assert identity in pinned._revoked
    assert identity not in dispatch._owners[dispatch.key(packet)]._revoked
    dispatch.release(identity)
    assert identity not in dispatch._attempts


def test_missing_or_changed_parent_is_not_admitted(fixture):
    dispatch = HostDispatch(lambda profile: dict(ROUTE))
    packet = _packet(task_id="unbound-parent")
    assert not dispatch.can_admit(packet)
    assert not dispatch.reserve(packet)
    parent = SimpleNamespace(provider="openai-codex", api_mode="codex_responses", session_id="native-parent-session")
    dispatch.bind(parent)
    assert dispatch.can_admit(packet)
    parent.session_id = "another-session"
    assert not dispatch.can_admit(packet)
    assert not dispatch.reserve(packet)
    assert not dispatch._attempts
