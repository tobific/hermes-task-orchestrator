"""Existing host compatibility controls, never supplied by model packets."""

from .scheduler import OrchestrationError


def detached_delivery_enabled(profile):
    from hermes_cli.config import load_config_readonly
    from hermes_constants import get_hermes_home

    if profile != str(get_hermes_home().resolve()):
        raise OrchestrationError("delivery profile mismatch")
    cfg = load_config_readonly()
    if not isinstance(cfg, dict):
        raise OrchestrationError("invalid host configuration")
    delegation = cfg.get("delegation", {})
    if not isinstance(delegation, dict):
        raise OrchestrationError("invalid host delegation policy")
    compatibility = delegation.get("compatibility", {})
    if not isinstance(compatibility, dict):
        raise OrchestrationError("invalid host compatibility policy")
    enabled = compatibility.get("detached_completion_delivery", True)
    if type(enabled) is not bool:
        raise OrchestrationError("invalid detached completion control")
    return enabled


def touch_join_parent(parent, *, heartbeat=True):
    if parent is None:
        return
    if getattr(parent, "_interrupt_requested", False) or getattr(
        parent, "is_closed", False
    ):
        raise InterruptedError("external orchestration join interrupted by parent")
    touch = getattr(parent, "_touch_activity", None)
    if heartbeat and callable(touch):
        try:
            touch('external orchestration: waiting for joinable results')
        except Exception:
            import logging
            logging.getLogger(__name__).debug('parent activity heartbeat failed', exc_info=True)
