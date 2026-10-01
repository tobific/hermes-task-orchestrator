"""Bind an existing native tier decision to one external worker attempt."""

import json
from pathlib import Path
from .scheduler import packet_identity, OrchestrationError


def tier_owner_identity(packet):
    return json.dumps(packet_identity(packet), separators=(",", ":"))


def authorized_tier(packet, decision):
    """Only native, live, exact-attempt policy authority may alter planned tier."""
    if decision is None:
        return packet["route"]["service_tier"]
    from agent.service_tier_policy import _Decision

    if type(decision) is not _Decision:
        raise OrchestrationError("native tier decision required")
    try:
        decision.check()
        request = decision.request
        origin = request.origin
        route = packet["route"]
        if (
            origin.kind != "delegation"
            or origin.message != ""
            or origin.identity != tier_owner_identity(packet)
            or origin.profile_home.resolve() != Path(packet["profile"]).resolve()
        ):
            raise ValueError("tier owner mismatch")
        for key in ["provider", "model", "base_url", "api_mode"]:
            if getattr(request, key) != route[key]:
                raise ValueError("tier route mismatch")
        if decision.tier not in (None, "normal", "priority"):
            raise ValueError("invalid tier")
        return (
            route["service_tier"]
            if decision.tier is None
            else ("priority" if decision.tier == "priority" else "default")
        )
    except Exception as exc:
        raise OrchestrationError("tier policy authority is invalid or revoked") from exc
