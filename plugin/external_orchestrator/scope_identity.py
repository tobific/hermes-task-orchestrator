"""Physical scope identity with compact paths relative to existing ancestors."""

import os
from pathlib import Path
import sys
import unicodedata


def _component(name, parent):
    if sys.platform != "darwin":
        return name
    # _PC_CASE_SENSITIVE from Darwin sys/unistd.h (not Python's name table).
    sensitive = os.pathconf(parent, 11)
    if sensitive not in (0, 1):
        raise ValueError("unknown filesystem case semantics")
    name = unicodedata.normalize("NFC", name)
    return name if sensitive else name.casefold()


def scope_identity(value):
    anchor = Path(value)
    tail = []
    while True:
        try:
            info = anchor.stat()
            break
        except FileNotFoundError:
            if anchor.parent == anchor:
                raise
            tail.insert(0, anchor.name)
            anchor = anchor.parent
    tail = [_component(x, anchor) for x in tail]
    chain = []
    names = []
    for parent in (anchor, *anchor.parents):
        s = parent.stat()
        chain.append([s.st_dev, s.st_ino])
        if parent.parent != parent:
            names.append(_component(parent.name, parent.parent))
    return {
        "node": [info.st_dev, info.st_ino],
        "ancestors": chain,
        "names": names,
        "tail": tail,
    }


def valid_identities(items, count):
    def node(x):
        return (
            isinstance(x, list)
            and len(x) == 2
            and all(type(v) is int and 0 <= v < 2**64 for v in x)
        )

    def components(x):
        return (
            isinstance(x, list)
            and len(x) <= 4096
            and all(
                isinstance(v, str)
                and v
                and "/" not in v
                and "\x00" not in v
                and v not in (".", "..")
                for v in x
            )
        )

    if not isinstance(items, list) or len(items) != count:
        return False
    for x in items:
        if not isinstance(x, dict) or set(x) != {"node", "ancestors", "names", "tail"}:
            return False
        if not node(x["node"]):
            return False
        ancestors = x["ancestors"]
        tail = x["tail"]
        names = x["names"]
        if (
            not isinstance(ancestors, list)
            or not 1 <= len(ancestors) <= 4097
            or ancestors[0] != x["node"]
            or not all(node(v) for v in ancestors)
        ):
            return False
        if (
            not components(tail)
            or not components(names)
            or len(names) != len(ancestors) - 1
        ):
            return False
        if sum(map(len, tail + names)) > 16384:
            return False
    return True


def identities_overlap(left, right):
    for a in left:
        for b in right:
            # Existing objects have authoritative physical ancestry: do not
            # case-fold two genuinely distinct objects on a sensitive volume.
            if not a["tail"] and not b["tail"]:
                if a["node"] in b["ancestors"] or b["node"] in a["ancestors"]:
                    return True
                continue
            index = {tuple(node): i for i, node in enumerate(b["ancestors"])}
            for i, node in enumerate(a["ancestors"]):
                j = index.get(tuple(node))
                if j is None:
                    continue
                pa = list(reversed(a["names"][:i])) + a["tail"]
                pb = list(reversed(b["names"][:j])) + b["tail"]
                n = min(len(pa), len(pb))
                if pa[:n] == pb[:n]:
                    return True
                break
    return False
