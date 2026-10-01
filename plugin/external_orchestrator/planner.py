"""Bounded host-side packet grouping. Never mix source or permission boundaries."""

import hashlib, json, uuid
from pathlib import Path


def plan_packets(units, max_packet_bytes=12000):
    if not isinstance(units, list) or not 1 <= len(units) <= 64:
        raise ValueError("1..64 units required")
    profiles = {u.get("profile") for u in units}
    if len(profiles) != 1:
        raise ValueError("mixed profile units are not permitted")
    profile = next(iter(profiles))
    if profile is not None and (not isinstance(profile, str) or not profile.strip()):
        raise ValueError("profile must be a non-empty string when supplied")
    by_id = {u["unit_id"]: u for u in units}
    if len(by_id) != len(units):
        raise ValueError("duplicate unit ids")
    graph = {k: set(v.get("dependencies", [])) for k, v in by_id.items()}
    if any(dep not in by_id for deps in graph.values() for dep in deps):
        raise ValueError("unknown dependency")
    pending = {k: set(v) for k, v in graph.items()}
    while pending:
        ready = {k for k, v in pending.items() if not v}
        if not ready:
            raise ValueError("dependency cycle")
        pending = {k: v - ready for k, v in pending.items() if k not in ready}
    prefix = uuid.uuid4().hex[:12]
    packets = []
    latest = {}
    unit_packet = {}

    def build(items, index):
        first = items[0]
        scope = first.get("evidence_scope")
        if not isinstance(scope, str) or not scope:
            raise ValueError("explicit evidence scope required for grouping")
        goals = [{"unit_id": u["unit_id"], "goal": u["goal"]} for u in items]
        return {
            "task_id": f"{prefix}-{index}",
            **({"profile": profile} if profile is not None else {}),
            "group_key": hashlib.sha256(signature.encode()).hexdigest(),
            "goal": "Complete every independent unit and return unit_results:\n"
            + json.dumps(goals, ensure_ascii=False),
            "unit_ids": [u["unit_id"] for u in items],
            "evidence_scope": scope,
            "capability_profile": first["capability_profile"],
            "write_scope": [
                str(Path(p).resolve()) for p in first.get("write_scope", [])
            ],
            "required": first.get("required", True),
            "dependencies": list(first.get("dependencies", [])),
        }

    for u in units:
        if not isinstance(u.get("goal"), str) or not u["goal"]:
            raise ValueError("unit goal required")
        signature = json.dumps(
            [
                u.get("evidence_scope"),
                profile,
                u["capability_profile"],
                sorted(str(Path(p).resolve()) for p in u.get("write_scope", [])),
                sorted(u.get("dependencies", [])),
                u.get("required", True),
            ],
            sort_keys=True,
        )
        idx = latest.get(signature)
        items = packets[idx][0] + [u] if idx is not None else [u]
        proposed = build(items, idx if idx is not None else len(packets))
        # Reserve enough room for generated dependency ids before their remap.
        size = len(json.dumps(proposed, ensure_ascii=False).encode()) + 64 * len(
            proposed["dependencies"]
        )
        if size > max_packet_bytes and idx is not None:
            idx = None
            items = [u]
            proposed = build(items, len(packets))
            size = len(json.dumps(proposed, ensure_ascii=False).encode()) + 64 * len(
                proposed["dependencies"]
            )
        if size > max_packet_bytes:
            raise ValueError("unit exceeds packet budget; split explicitly")
        if idx is None:
            idx = len(packets)
            packets.append((items, proposed))
            latest[signature] = idx
        else:
            packets[idx] = (items, proposed)
        unit_packet[u["unit_id"]] = proposed["task_id"]
    result = []
    for items, packet in packets:
        packet["dependencies"] = sorted(
            {unit_packet[d] for d in packet["dependencies"]}
        )
        packet["group_key"] = hashlib.sha256(
            json.dumps(
                [
                    packet["evidence_scope"],
                    packet.get("profile"),
                    packet["capability_profile"],
                    packet["write_scope"],
                ],
                sort_keys=True,
            ).encode()
        ).hexdigest()
        if len(json.dumps(packet, ensure_ascii=False).encode()) > max_packet_bytes:
            raise ValueError("final packet exceeds budget")
        result.append(packet)
    assert sum(len(p["unit_ids"]) for p in result) == len(units)
    return result
