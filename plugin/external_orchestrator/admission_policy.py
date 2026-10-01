"""Read existing host controls; packets never supply policy authority."""

from .scheduler import OrchestrationError


class AdmissionBlocked(OrchestrationError):
    """A reversible host admission hold, not a failure of an active worker."""


def enforce_host_admission(profile, *, effort=None):
    try:
        from hermes_cli.config import load_config_readonly
        from hermes_constants import get_hermes_home
        from tools.delegate_tool_registry import is_spawn_paused

        if profile != str(get_hermes_home().resolve()):
            raise AdmissionBlocked("host admission profile mismatch")
        cfg = load_config_readonly()
        if not isinstance(cfg, dict):
            raise AdmissionBlocked("host admission configuration unavailable")
        delegation = cfg.get("delegation", {})
        if not isinstance(delegation, dict):
            raise AdmissionBlocked("invalid host delegation configuration")
        policy = delegation.get("policy", {})
        worker = delegation.get("worker", {})
        if not isinstance(policy, dict) or not isinstance(worker, dict):
            raise AdmissionBlocked("invalid host effort policy")
        allow_xhigh = policy.get("allow_xhigh", True)
        if type(allow_xhigh) is not bool:
            raise AdmissionBlocked("invalid xhigh policy")
        if not allow_xhigh and (effort or worker.get("default_reasoning_effort")) == "xhigh":
            raise AdmissionBlocked("xhigh delegation effort is disabled by policy")
        scheduler = delegation.get("scheduler", {})
        if not isinstance(scheduler, dict):
            raise AdmissionBlocked("invalid host scheduler configuration")
        # The existing scheduler defaults to enabled when the field is absent.
        enabled = scheduler.get("enabled", True)
        if type(enabled) is not bool or not enabled:
            raise AdmissionBlocked("delegation.scheduler.enabled denies admission")
        paused = is_spawn_paused()
        if type(paused) is not bool or paused:
            raise AdmissionBlocked("global spawn-pause denies admission")
    except AdmissionBlocked:
        raise
    except Exception as exc:
        raise AdmissionBlocked("host admission controls unavailable") from exc
