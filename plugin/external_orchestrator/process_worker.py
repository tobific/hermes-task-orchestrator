"""Trusted host foreground-process adapter, not an arbitrary-code sandbox.

The command is host-selected code, never a model-supplied shell program. Its
inherited lifetime descriptors must remain private to that trusted process
family. Advisory coordination cannot defend its state from hostile same-UID
host code; model tool authority is enforced separately by NativeWorkerAdapter.
Subprocess output remains untrusted result data, not model authority.
"""

import json, subprocess, sys
from pathlib import Path
from .scheduler import HostOutcome, HostFailure, packet_identity
from .storage import configured_storage_directory


class ProcessWorker:
    def __init__(self, command, data_dir, *, context=None, decode=None):
        self.command = tuple(command)
        self.data_dir = configured_storage_directory(data_dir)
        self.context = context or {}
        self.decode = decode

    def __call__(self, packet):
        return self.run_with_capacity(packet, ())

    def run_with_capacity(self, packet, capacity_fds):
        # Host-only ephemeral descriptors, never caller/model packet fields.
        capacity_fds = tuple(capacity_fds)
        helper = Path(__file__).with_name("process_watchdog.py")
        with subprocess.Popen(
            [sys.executable, "-B", str(helper)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            pass_fds=capacity_fds,
        ) as process:
            control = {
                "command": self.command,
                "capacity_fds": capacity_fds,
                "data_dir": str(self.data_dir),
                "packet": packet,
                "context": self.context,
            }
            process.stdin.write(json.dumps(control).encode() + b"\n")
            process.stdin.flush()
            # stdin deliberately remains open as a parent-death signal.
            raw = process.stdout.read(262145)
            process.wait(timeout=10)
            if process.returncode or len(raw) > 262144:
                raise RuntimeError(
                    "worker supervisor failed: "
                    + process.stderr.read(2000).decode(errors="replace")
                )
            reply = json.loads(raw)
            self.last_reply = reply
        if not reply.get("ok"):
            return HostFailure(
                packet_identity(packet),
                reply.get("reason", "PROCESS_FAILED"),
                "owned process failed or was revoked",
            )
        body = reply["result"]
        if self.decode:
            return self.decode(packet, body)
        return HostOutcome(
            body["payload"], body["observation"], packet_identity(packet)
        )
