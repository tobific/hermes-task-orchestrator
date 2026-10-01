"""Optional audit observations; assertions never depend on this output channel."""

import json
import os
import sys
from pathlib import Path
from threading import Lock

_LOCK = Lock()


def record(kind, payload):
    report = next(
        (arg.split("=", 1)[1] for arg in sys.argv if arg.startswith("--junitxml=")),
        None,
    )
    destination = (
        str(Path(report).parent) if report else os.environ.get("SEAM_RECEIPT_DIR")
    )
    if destination:
        entry = {
            "case": os.environ.get("PYTEST_CURRENT_TEST", "subprocess"),
            "kind": kind,
            "payload": payload,
            "pid": os.getpid(),
        }
        with (
            _LOCK,
            (Path(destination) / f"observations-{os.getpid()}.jsonl").open(
                "a"
            ) as stream,
        ):
            stream.write(json.dumps(entry, default=str, sort_keys=True) + "\n")
