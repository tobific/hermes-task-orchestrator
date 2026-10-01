"""Host-owned validation for structured claims in external worker summaries.

The native adapter deliberately preserves the child's summary as an opaque
string.  This helper recognizes only one explicit contract: the entire summary
must be a top-level JSON object with the typed worker-result keys. Nested and
fenced claim objects, and malformed JSON-looking summaries, fail closed.
Arbitrary English and ordinary code/log prose are not semantically scanned.

Only local regular files under the packet's already-confined write scope are
supported as artifacts.  URLs, commands, and worker-supplied execution receipts
are not accepted or used.  Passing checks require an optional host-owned
callback; worker assertions alone are never host verification.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from pathlib import Path
from typing import Any, Callable, Mapping


MAX_CLAIMS = 32
MAX_CLAIM_NAME_CHARS = 512
MAX_CLAIM_DETAIL_CHARS = 2_000
MAX_ARTIFACT_PATH_CHARS = 4_096
MAX_ARTIFACT_BYTES = 16 * 1024 * 1024
_SHA256_RE = re.compile(r"[0-9a-fA-F]{64}\Z")
_STRUCTURED_KEYS = frozenset(
    {
        "status",
        "answer",
        "evidence",
        "artifacts",
        "checks",
        "uncertainties",
        "suggested_followups",
    }
)
_CLAIM_KEYS = frozenset({"artifacts", "checks"})
_LIMITATION = (
    "Only top-level JSON summaries declaring artifacts/checks are validated; "
    "nested or fenced structured claims are rejected. Arbitrary prose and logs "
    "are not semantically scanned."
)


class ClaimValidationError(ValueError):
    """A structured claim was malformed, untrusted, or not host-verifiable."""


def _unique_pairs(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ClaimValidationError("structured summary contains duplicate keys")
        value[key] = item
    return value


def _contains_claim_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            key in _CLAIM_KEYS or _contains_claim_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_claim_key(item) for item in value)
    return False


def _has_declared_claim_key(text: str) -> bool:
    """Conservatively detect possible claim keys in malformed JSON-like text.

    Valid JSON is parsed first. Ambiguous quoting in a failed parse must not
    hide a claim. This fallback does not classify arbitrary free prose.
    """
    text = text.lstrip("\ufeff \r\n\t")
    if not text.startswith(("{", "[")):
        return False
    for match in re.finditer(r'"((?:\\.|[^"\\])*)"\s*:', text):
        try:
            key = json.loads('"' + match.group(1) + '"')
        except ValueError:
            continue
        if key in _CLAIM_KEYS:
            return True
    return False


def _fenced_claim_object(answer: str) -> Any:
    lines = answer.strip().splitlines()
    if not lines or not lines[0].startswith(("```", "~~~")):
        return None
    marker = lines[0][:3]
    closed = len(lines) > 1 and lines[-1].startswith(marker)
    body = "\n".join(lines[1:-1] if closed else lines[1:])
    try:
        return json.loads(body, object_pairs_hook=_unique_pairs)
    except (TypeError, ValueError):
        if _has_declared_claim_key(body):
            raise ClaimValidationError("malformed structured claim summary")
        return None


def _declared_object(answer: str) -> Mapping[str, Any] | None:
    """Return a strict top-level claim object, or None for opaque prose."""
    # Parse a BOM-free view only; the original native answer is never rewritten.
    answer = answer.lstrip("\ufeff \r\n\t")
    try:
        parsed = json.loads(answer, object_pairs_hook=_unique_pairs)
    except ClaimValidationError:
        raise
    except (TypeError, ValueError, json.JSONDecodeError):
        fenced = _fenced_claim_object(answer)
        if fenced is not None and _contains_claim_key(fenced):
            raise ClaimValidationError(
                "fenced structured claims are not supported; return top-level JSON"
            )
        if _has_declared_claim_key(answer):
            raise ClaimValidationError("malformed structured summary")
        return None

    if isinstance(parsed, Mapping) and _CLAIM_KEYS.intersection(parsed):
        return parsed
    if _contains_claim_key(parsed):
        raise ClaimValidationError(
            "artifact/check claims must be top-level, not nested or wrapped"
        )
    return None


def _string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or len(value) > MAX_CLAIMS:
        raise ClaimValidationError(f"structured {field} must be a bounded array")
    if any(
        not isinstance(item, str) or len(item) > MAX_CLAIM_DETAIL_CHARS
        for item in value
    ):
        raise ClaimValidationError(f"structured {field} must contain bounded strings")
    return list(value)


def _structured_claims(answer: str) -> Mapping[str, Any] | None:
    value = _declared_object(answer)
    if value is None:
        return None
    if set(value) != _STRUCTURED_KEYS:
        raise ClaimValidationError(
            "structured summary must contain exactly the typed worker-result keys"
        )
    if value.get("status") != "pass":
        raise ClaimValidationError("structured success summary status must be pass")
    if not isinstance(value.get("answer"), str) or not value["answer"].strip():
        raise ClaimValidationError("structured summary answer must be non-empty text")
    for field in ("evidence", "uncertainties", "suggested_followups"):
        _string_list(value.get(field), field)
    return value


def _scope_roots(write_scope: Any) -> list[Path]:
    if not isinstance(write_scope, list) or len(write_scope) > 16:
        raise ClaimValidationError(
            "artifact verification requires a bounded write scope"
        )
    roots = []
    for root in write_scope:
        if not isinstance(root, str) or not root or "\x00" in root:
            raise ClaimValidationError("write scope contains an invalid root")
        roots.append(Path(os.path.abspath(os.path.expanduser(root))))
    # A redundant nested root must not let a worker-writable intermediate
    # component become part of the trusted anchor opened as an absolute path.
    roots = list(dict.fromkeys(roots))
    return [root for root in roots if not any(other in root.parents for other in roots)]


def _confined(path: Path, roots: list[Path]) -> bool:
    return any(root == path or root in path.parents for root in roots)


def _reject_symlink_components(path: Path, roots: list[Path]) -> None:
    """Reject a symlink in the lexical artifact path, including its leaf."""
    lexical = Path(os.path.abspath(os.fspath(path)))
    for root in roots:
        try:
            relative = lexical.relative_to(root)
        except ValueError:
            continue
        current = root
        for component in relative.parts:
            current = current / component
            try:
                if stat.S_ISLNK(os.lstat(current).st_mode):
                    raise ClaimValidationError(
                        f"artifact path contains a symlink: {path}"
                    )
            except FileNotFoundError:
                break
        return


def _read_regular_file(path: Path, roots: list[Path]) -> tuple[str, int, int, int]:
    """Open every path component through directory descriptors, never symlinks."""
    directory = (
        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    )
    regular = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | getattr(os, "O_CLOEXEC", 0)
    lexical = Path(os.path.abspath(os.fspath(path)))
    scope = max(
        (root for root in roots if root == lexical or root in lexical.parents),
        key=lambda root: len(root.parts),
    )
    # Ancestors above the host-granted scope are outside worker write authority.
    # Anchor directly in that scope; opening / would exceed the sandbox grant.
    anchor = scope.parent if scope == lexical else scope
    parts = lexical.relative_to(anchor).parts
    dir_fd = fd = None

    def identity(st):
        return (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)

    try:
        dir_fd = os.open(os.fspath(anchor), directory)
        for component in parts[:-1]:
            child_fd = os.open(component, directory, dir_fd=dir_fd)
            os.close(dir_fd)
            dir_fd = child_fd
        fd = os.open(parts[-1], regular, dir_fd=dir_fd)
        before = os.fstat(fd)
        if not stat.S_ISREG(before.st_mode):
            raise ClaimValidationError(f"artifact is not a regular file: {path}")
        if before.st_size > MAX_ARTIFACT_BYTES:
            raise ClaimValidationError(f"artifact exceeds the bounded size: {path}")
        digest = hashlib.sha256()
        total = 0
        while total <= MAX_ARTIFACT_BYTES:
            chunk = os.read(fd, min(1024 * 1024, MAX_ARTIFACT_BYTES + 1 - total))
            if not chunk:
                break
            digest.update(chunk)
            total += len(chunk)
        after = os.fstat(fd)
        current = os.stat(parts[-1], dir_fd=dir_fd, follow_symlinks=False)
        if total > MAX_ARTIFACT_BYTES:
            raise ClaimValidationError(f"artifact exceeds the bounded size: {path}")
        if (
            identity(before) != identity(after)
            or identity(after) != identity(current)
            or total != after.st_size
        ):
            raise ClaimValidationError(f"artifact changed while being verified: {path}")
        return digest.hexdigest(), total, after.st_dev, after.st_ino
    except OSError as exc:
        raise ClaimValidationError(
            f"artifact cannot be verified safely: {path}"
        ) from exc
    finally:
        if fd is not None:
            os.close(fd)
        if dir_fd is not None:
            os.close(dir_fd)


def _verify_artifact(
    claim: Mapping[str, Any],
    roots: list[Path],
    *,
    expected: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    path_value = claim.get("path")
    if (
        not isinstance(path_value, str)
        or not path_value
        or len(path_value) > MAX_ARTIFACT_PATH_CHARS
        or "\x00" in path_value
        or not os.path.isabs(path_value)
    ):
        raise ClaimValidationError("artifact path must be an absolute bounded path")
    candidate = Path(path_value)
    if ".." in candidate.parts:
        raise ClaimValidationError("artifact path must not contain parent traversal")
    canonical = Path(os.path.abspath(os.fspath(candidate)))
    if not _confined(canonical, roots):
        raise ClaimValidationError(
            f"artifact path is outside the declared write scope: {path_value}"
        )
    _reject_symlink_components(candidate, roots)
    actual_sha256, size, device, inode = _read_regular_file(candidate, roots)
    if "sha256" in claim:
        supplied = claim["sha256"]
        if not isinstance(supplied, str) or not _SHA256_RE.fullmatch(supplied):
            raise ClaimValidationError("artifact sha256 is malformed")
    else:
        supplied = ""
    if supplied and supplied.lower() != actual_sha256:
        raise ClaimValidationError(
            f"artifact sha256 mismatch for {path_value}: got {actual_sha256}"
        )
    if expected is not None:
        if expected.get("sha256") != actual_sha256:
            raise ClaimValidationError(f"artifact changed after review: {path_value}")
        if expected.get("size") != size:
            raise ClaimValidationError(
                f"artifact size changed after review: {path_value}"
            )
        if expected.get("device") != device or expected.get("inode") != inode:
            raise ClaimValidationError(
                f"artifact identity changed after review: {path_value}"
            )
    return {
        "type": "file",
        "path": str(canonical),
        "size": size,
        "sha256": actual_sha256,
        "device": device,
        "inode": inode,
        "verified": True,
    }


def validate_claims(
    answer: str,
    write_scope: Any,
    *,
    check_verifier: Callable[[Mapping[str, Any]], Any] | None = None,
) -> dict[str, Any]:
    """Validate declared claims and return host-owned normalized metadata."""
    structured = _structured_claims(answer)
    if structured is None:
        return {
            "schema": 1,
            "mode": "plain_summary",
            "declared": False,
            "artifacts": [],
            "checks": [],
            "limitations": [_LIMITATION],
        }

    roots = _scope_roots(write_scope)
    raw_artifacts = structured["artifacts"]
    if not isinstance(raw_artifacts, list) or len(raw_artifacts) > MAX_CLAIMS:
        raise ClaimValidationError("structured artifacts must be a bounded array")
    artifacts = []
    for raw in raw_artifacts:
        if not isinstance(raw, Mapping):
            raise ClaimValidationError("structured artifacts must contain objects")
        if set(raw) - {"path", "sha256"} or "path" not in raw:
            raise ClaimValidationError("artifact claim supports only path and sha256")
        artifacts.append(_verify_artifact(raw, roots))

    raw_checks = structured["checks"]
    if not isinstance(raw_checks, list) or len(raw_checks) > MAX_CLAIMS:
        raise ClaimValidationError("structured checks must be a bounded array")
    checks = []
    for raw in raw_checks:
        if not isinstance(raw, Mapping):
            raise ClaimValidationError("structured checks must contain objects")
        if (
            set(raw) - {"name", "status", "detail"}
            or "name" not in raw
            or "status" not in raw
        ):
            raise ClaimValidationError(
                "check claim requires only name, status and detail"
            )
        name = raw["name"]
        status = raw["status"]
        detail = raw.get("detail", "")
        if (
            not isinstance(name, str)
            or not name.strip()
            or len(name) > MAX_CLAIM_NAME_CHARS
        ):
            raise ClaimValidationError("check name is malformed")
        if status not in {"pass", "fail", "skipped"}:
            raise ClaimValidationError("check status must be pass, fail or skipped")
        if not isinstance(detail, str) or len(detail) > MAX_CLAIM_DETAIL_CHARS:
            raise ClaimValidationError("check detail is malformed")
        normalized = {"name": name, "status": status, "detail": detail}
        if status == "fail":
            raise ClaimValidationError(f"reported check failed: {name}: {detail}")
        if status == "skipped":
            raise ClaimValidationError(f"reported check was not completed: {name}")
        if status == "pass":
            if check_verifier is None:
                raise ClaimValidationError(
                    f"reported passing check lacks host verification: {name}"
                )
            try:
                host_verified = check_verifier(normalized) is True
            except Exception as exc:
                raise ClaimValidationError(
                    f"host check verification failed: {name}"
                ) from exc
            if not host_verified:
                raise ClaimValidationError(
                    f"reported passing check lacks host verification: {name}"
                )
            normalized["host_verified"] = True
        checks.append(normalized)

    return {
        "schema": 1,
        "mode": "structured_claims",
        "declared": True,
        "artifacts": artifacts,
        "checks": checks,
        "limitations": [_LIMITATION],
    }


def revalidate_claims(validation: Any, write_scope: Any) -> None:
    """Re-check host-owned artifact identity/content immediately before delivery."""
    if not isinstance(validation, Mapping) or validation.get("schema") != 1:
        raise ClaimValidationError("missing or unsupported claim-validation record")
    mode = validation.get("mode")
    if mode == "plain_summary":
        return
    if mode != "structured_claims":
        raise ClaimValidationError("invalid claim-validation mode")
    roots = _scope_roots(write_scope)
    artifacts = validation.get("artifacts")
    if not isinstance(artifacts, list) or len(artifacts) > MAX_CLAIMS:
        raise ClaimValidationError("stored artifact validation is malformed")
    for artifact in artifacts:
        if not isinstance(artifact, Mapping):
            raise ClaimValidationError("stored artifact validation is malformed")
        _verify_artifact(artifact, roots, expected=artifact)
    checks = validation.get("checks")
    if not isinstance(checks, list) or len(checks) > MAX_CLAIMS:
        raise ClaimValidationError("stored check validation is malformed")
    for check in checks:
        if not isinstance(check, Mapping) or check.get("status") == "fail":
            raise ClaimValidationError("stored check validation is malformed")
        if check.get("status") == "pass" and check.get("host_verified") is not True:
            raise ClaimValidationError("stored passing check lacks host verification")
