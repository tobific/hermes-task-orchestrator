"""Bounded, host-gated automatic selection for external orchestration.

This module deliberately stays outside the native agent.  It only recognizes a
small class of independent, read-only requests and returns user-turn context;
it never creates a run, chooses a worker, or changes the system prompt.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Iterable


DIRECT_LUNA_ROUTE = "openai-codex/gpt-6-luna"
HOST_REASONING_EFFORT = "xhigh"
MIN_ITEMS = 3
MAX_ITEMS = 16
MAX_DIRECTIVE_CHARS = 8192

_OPT_OUT_RE = re.compile(
    r"\b(?:do not|don't|dont|never)\s+delegat(?:e|ion)\b"
    r"|\bno\s+delegation\b"
    r"|\b(?:handle|do)\s+(?:all of\s+)?this\s+(?:yourself|locally)\b"
    r"|\bsol[- ]only\b",
    re.IGNORECASE,
)
_READ_ONLY_RE = re.compile(
    r"\b(?:analy[sz]e|audit|check|classify|compare|extract|identify|inspect|"
    r"read|review|summari[sz]e|triage|verify)\b",
    re.IGNORECASE,
)
_PER_ITEM_RE = re.compile(
    r"\b(?:for each|each (?:file|item|packet|report|document|log)|"
    r"independent(?:ly)?|separate(?:ly)?|one (?:task|worker) per)\b",
    re.IGNORECASE,
)
_MUTATING_RE = re.compile(
    r"\b(?:commit|delete|deploy|edit|implement|merge|modify|patch|publish|"
    r"restart|send|ship|write)\b",
    re.IGNORECASE,
)
_NEGATED_MUTATION_RE = re.compile(
    r"\b(?:do not|don't|dont|never|without)\s+"
    r"(?:commit|delete|deploy|edit|implement|merge|modify|patch|publish|"
    r"restart|send|ship|write)\b",
    re.IGNORECASE,
)
_RANGE_RE = re.compile(
    r"\b(?:packet|file|document|item|report|log|fixture)s?[-_ ]?(\d+)"
    r"(?:\.[a-z0-9]+)?\s+(?:through|to)\s+"
    r"(?:packet|file|document|item|report|log|fixture)?s?[-_ ]?(\d+)"
    r"(?:\.[a-z0-9]+)?\b",
    re.IGNORECASE,
)
_PATH_RE = re.compile(
    r"(?<![\w])(?:~?/|\.?\.?/)?[\w@+.-]+(?:/[\w@+.-]+)*"
    r"\.(?:c|cc|cpp|csv|go|h|hpp|java|js|json|jsx|log|md|py|rs|toml|ts|tsx|txt|yaml|yml)\b",
    re.IGNORECASE,
)
_URL_RE = re.compile(r"https?://[^\s)>\]}]+", re.IGNORECASE)
_LIST_ITEM_RE = re.compile(r"(?m)^\s*(?:[-*]|\d+[.)])\s+\S")
_GROUP_HEADING_RE = re.compile(
    r"^\s*(?:[-*]\s+)?(?:#{1,6}\s+)?"
    r"(?P<label>(?:packet|group)\s*[-_ ]?\s*[A-Za-z0-9]+)"
    r"\s*(?::(?P<inline>.*))?\s*$",
    re.IGNORECASE,
)
_GROUPISH_LINE_RE = re.compile(
    r"^\s*(?:[-*]\s+)?(?:#{1,6}\s+)?(?:packet|group)(?:[ \t]+|:)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ExternalSelectorDecision:
    """Pure selection output; no external orchestration action is performed."""

    eligible: bool
    item_count: int = 0
    reason: str = ""
    directive: str = ""
    grouped: tuple[tuple[str, tuple[str, ...]], ...] | None = None
    join_available: bool = False
    policy_mode: str = "observe"
    route: str = DIRECT_LUNA_ROUTE
    reasoning_effort: str = HOST_REASONING_EFFORT


# Short alias for callers that do not need the implementation-specific name.
SelectorDecision = ExternalSelectorDecision


def _canonical_scope(value: str) -> str:
    return re.sub(r"/+", "/", value.strip().casefold()).rstrip("/")


def _scopes_overlap(left: str, right: str) -> bool:
    left_norm, right_norm = _canonical_scope(left), _canonical_scope(right)
    if not left_norm or not right_norm:
        return False
    return (
        left_norm == right_norm
        or left_norm.startswith(right_norm + "/")
        or right_norm.startswith(left_norm + "/")
    )


def _range_item_count(text: str) -> int:
    counts = []
    for match in _RANGE_RE.finditer(text):
        start, end = int(match.group(1)), int(match.group(2))
        count = end - start + 1
        if 0 < count <= MAX_ITEMS:
            counts.append(count)
    return max(counts, default=0)


def _source_items(text: str) -> tuple[str, ...]:
    """Return concrete sources, preserving their first-seen order."""

    urls = list(_URL_RE.finditer(text))
    # A URL containing a file suffix is one source, not a URL plus an
    # invented local path. Retain textual order for grouped assignments.
    matches = urls + [
        match
        for match in _PATH_RE.finditer(text)
        if not any(url.start() <= match.start() < url.end() for url in urls)
    ]
    values = [match.group(0) for match in sorted(matches, key=lambda m: m.start())]
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        key = value.casefold() if not value.startswith("http") else value
        if key not in seen:
            seen.add(key)
            result.append(value)
    return tuple(result)


def _independent_item_count(text: str) -> int:
    sources = _source_items(text)
    urls = sum(bool(_URL_RE.fullmatch(source)) for source in sources)
    # Preserve the original conservative maximum-of-source-kinds rule and
    # use list markers only when there are no concrete sources at all.
    concrete = max(_range_item_count(text), urls, len(sources) - urls)
    return min(concrete or len(_LIST_ITEM_RE.findall(text)), MAX_ITEMS)


def _group_label_key(label: str) -> str:
    return re.sub(r"[\s_-]+", " ", label.strip()).casefold()


def parse_explicit_groups(
    text: str,
) -> tuple[tuple[str, tuple[str, ...]], ...] | None:
    """Parse explicit Packet/Group headings, failing closed on ambiguity.

    ``None`` means no grouping was requested.  ``()`` means a group-shaped
    request was malformed, duplicated, empty, or had unassigned sources.
    """

    normalized = re.sub(
        r"(?<=\s)(?=(?:packet|group)\s+\d+\s*:)",
        "\n",
        text,
        flags=re.IGNORECASE,
    )
    lines = normalized.splitlines()
    headings: list[tuple[int, str, str, str]] = []
    for line_index, line in enumerate(lines):
        match = _GROUP_HEADING_RE.fullmatch(line)
        if match:
            display = re.sub(r"\s+", " ", match.group("label").strip())
            headings.append(
                (
                    line_index,
                    _group_label_key(display),
                    display,
                    match.group("inline") or "",
                )
            )
        elif _GROUPISH_LINE_RE.match(line):
            return ()
    if not headings:
        return None
    if len(headings) > MAX_ITEMS:
        return ()
    labels = [heading[1] for heading in headings]
    if len(labels) != len(set(labels)):
        return ()

    all_sources = _source_items(text)
    groups: list[tuple[str, tuple[str, ...]]] = []
    assigned = 0
    for index, (line_index, _key, display, inline) in enumerate(headings):
        end = headings[index + 1][0] if index + 1 < len(headings) else len(lines)
        section = "\n".join((inline, *lines[line_index + 1 : end]))
        raw_sources = _source_items(section)
        if not raw_sources or len(raw_sources) > MAX_ITEMS:
            return ()
        normalized_sources = tuple(_canonical_scope(source) for source in raw_sources)
        if len(normalized_sources) != len(set(normalized_sources)):
            return ()
        assigned += len(raw_sources)
        groups.append((display, raw_sources))

    if assigned != len(all_sources):
        return ()
    flattened = [source for _label, sources in groups for source in sources]
    if any(
        _scopes_overlap(left, right)
        for index, left in enumerate(flattened)
        for right in flattened[index + 1 :]
    ):
        return ()
    return tuple(groups)


def _format_groups(groups: tuple[tuple[str, tuple[str, ...]], ...]) -> str:
    lines = [
        "Preserve this explicit group-to-source mapping; keep each group's sources together:",
        *(f"- {label}: {', '.join(sources)}" for label, sources in groups),
    ]
    result = "\n".join(lines)
    return result if len(result) <= MAX_DIRECTIVE_CHARS else ""


def _is_child_or_recursive(parent_session_id: Any, parent: Any = None) -> bool:
    if str(parent_session_id or "").strip():
        return True
    if parent is None:
        return False
    for name in ("_parent_session_id", "parent_session_id"):
        value = getattr(parent, name, None)
        if value and str(value).strip():
            return True
    for name in ("_delegated_child", "_is_delegated_child", "delegated_child"):
        if getattr(parent, name, False) is True:
            return True
    for name in ("_delegate_depth", "delegate_depth", "delegation_depth"):
        try:
            if int(getattr(parent, name, 0) or 0) > 0:
                return True
        except (TypeError, ValueError):
            return True
    return False


def _decision(
    eligible: bool,
    *,
    reason: str,
    item_count: int = 0,
    directive: str = "",
    grouped: tuple[tuple[str, tuple[str, ...]], ...] | None = None,
    join_available: bool = False,
) -> ExternalSelectorDecision:
    return ExternalSelectorDecision(
        eligible=eligible,
        item_count=item_count,
        reason=reason,
        directive=directive,
        grouped=grouped,
        join_available=join_available,
    )


def _directive(
    *,
    item_count: int,
    grouped: tuple[tuple[str, tuple[str, ...]], ...] | None,
    join_available: bool,
) -> str:
    lines = [
        "[EXTERNAL AUTOMATIC ORCHESTRATION DECISION]",
        f"The host rule matched {item_count} independent read-only work items.",
        "Use the external plugin's actual orchestration interface; do not perform the work serially first.",
    ]
    if grouped is not None:
        mapping = _format_groups(grouped)
        if not mapping:
            return ""
        lines.append(mapping)
    mode = "joinable" if join_available else "detached"
    lines.extend(
        [
            "Call orchestration_create once. Start with these exact create options and add tasks:",
            "```json\n"
            + '{"mode":"'
            + mode
            + '","capability_profile":"read-only","final_review_task_id":"final-review"}'
            + "\n```",
            "Give each inspection task a unique task_id, required=true, goal containing all requested per-item steps, evidence_scope naming its exact assigned sources, and timeout_seconds=180 (never exceed the user's deadline).",
            "Add a required task_id=final-review with dependencies containing every inspection task_id and timeout_seconds=180. Its goal is to independently review all results against the request. The scheduler supplies the trusted review contract and actual evidence; do not invent result-envelope instructions.",
            "Keep each explicit group's sources together. If independent execution is unsafe, do not delegate and explain why.",
            f"The host owns the fixed {DIRECT_LUNA_ROUTE} route with {HOST_REASONING_EFFORT} reasoning; do not select another model or route.",
        ]
    )
    if join_available:
        lines.extend(
            [
                'Use mode="joinable" and then call orchestration_join with the returned run_id and a bounded timeout.',
                "Validate the joined results before synthesizing the final answer.",
            ]
        )
    else:
        lines.extend(
            [
                'Use mode="detached" because orchestration_join is not exposed in this host turn.',
                "Do not call an unavailable tool; use the plugin's native completion delivery before synthesizing.",
            ]
        )
    return "\n".join(lines)


def assess_external_selector(
    user_message: Any,
    *,
    valid_tool_names: Iterable[str] = (),
    delegated_child: bool = False,
    parent_session_id: Any = "",
    parent: Any = None,
) -> ExternalSelectorDecision:
    """Purely assess one request using only the supplied host tool snapshot."""

    text = user_message if isinstance(user_message, str) else ""
    if not text.strip():
        return _decision(False, reason="non_text_request")
    if delegated_child or _is_child_or_recursive(parent_session_id, parent):
        return _decision(False, reason="delegated_child")

    tools = {str(name) for name in (valid_tool_names or ())}
    # Creation is the minimum external capability.  Join is required only for
    # the joinable form; a create-only host can use native detached delivery.
    if "orchestration_create" not in tools:
        return _decision(False, reason="orchestration_create_unavailable")

    if _OPT_OUT_RE.search(text):
        return _decision(False, reason="user_opt_out")
    if not _READ_ONLY_RE.search(text):
        return _decision(False, reason="not_read_only_work")
    mutation_text = _NEGATED_MUTATION_RE.sub("", text)
    if _MUTATING_RE.search(mutation_text):
        return _decision(False, reason="mutation_or_side_effect_requested")
    if not _PER_ITEM_RE.search(text):
        return _decision(False, reason="independence_not_explicit")

    grouped = parse_explicit_groups(text)
    if grouped == ():
        return _decision(False, reason="ambiguous_grouping")
    item_count = len(grouped) if grouped is not None else _independent_item_count(text)
    if item_count < MIN_ITEMS:
        return _decision(
            False,
            item_count=item_count,
            reason="fewer_than_three_items",
            grouped=grouped,
        )

    join_available = "orchestration_join" in tools
    directive = _directive(
        item_count=item_count,
        grouped=grouped,
        join_available=join_available,
    )
    if not directive:
        return _decision(
            False, item_count=item_count, reason="ambiguous_grouping", grouped=grouped
        )
    return _decision(
        True,
        item_count=item_count,
        reason="independent_read_only_items",
        directive=directive,
        grouped=grouped,
        join_available=join_available,
    )


def select_external_orchestration(
    user_message: Any, parent: Any, **kwargs: Any
) -> ExternalSelectorDecision:
    """Assess against the active host parent, never a process-wide registry."""

    return assess_external_selector(
        user_message,
        valid_tool_names=getattr(parent, "valid_tool_names", ()) or (),
        parent=parent,
        **kwargs,
    )


def automatic_selector_hook(
    *,
    user_message: Any = None,
    parent_session_id: Any = "",
    **_: Any,
) -> dict[str, str] | None:
    """Return observe-only context for the native ``pre_llm_call`` channel."""

    try:
        from agent.subagent_lifecycle import get_active_subagent_parent
        from agent.delegation_context import is_delegated_child_context
        from hermes_cli.config import load_config_readonly

        parent = get_active_subagent_parent()
        if parent is None or not getattr(parent, "session_id", None):
            return None
        # Read native host identity, never model-supplied hook metadata.
        if is_delegated_child_context():
            return None
        if getattr(parent, "provider", None) != "openai-codex":
            return None
        if str(getattr(parent, "model", "")).lower().split("/")[-1] != "gpt-6-astra":
            return None
        delegation = (load_config_readonly() or {}).get("delegation", {})
        # With no legacy scheduler setting the exposed external plugin is the
        # capability gate. An explicit operator disable still wins.
        if delegation.get("scheduler", {}).get("enabled", True) is not True:
            return None
        if (
            delegation.get("routing_policy", {}).get("luna_policy_mode", "observe")
            != "observe"
        ):
            return None
        decision = select_external_orchestration(
            user_message,
            parent,
            parent_session_id=parent_session_id,
        )
    except Exception:
        # Hook failures must never affect the host turn.
        return None
    if not decision.eligible:
        return None
    return {"context": decision.directive}


# Friendly hook name for plugin integrations and focused tests.
pre_llm_call = automatic_selector_hook


__all__ = [
    "DIRECT_LUNA_ROUTE",
    "HOST_REASONING_EFFORT",
    "MIN_ITEMS",
    "ExternalSelectorDecision",
    "SelectorDecision",
    "assess_external_selector",
    "automatic_selector_hook",
    "parse_explicit_groups",
    "pre_llm_call",
    "select_external_orchestration",
]
