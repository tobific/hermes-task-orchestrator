"""Owned worker supervision. A shared owner lock lives until the child is drained."""

import fcntl, json, os, selectors, signal, subprocess, sys, threading, time
from pathlib import Path
import psutil
# Keep the owned leader unreaped, even if the caller ignored SIGCHLD.
signal.signal(signal.SIGCHLD, signal.SIG_DFL)

# This module is launched by absolute path, so make package imports explicit.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from external_orchestrator.storage import (
    atomic_replace_bytes,
    ensure_private_directory,
    open_private_file,
)

from external_orchestrator.process_drain import drain_owned_group

LIMIT = 262144
request = json.loads(sys.stdin.buffer.readline(LIMIT))
root = ensure_private_directory(Path(request["data_dir"]))
packet = request["packet"]
capacity_fds=tuple(request.get("capacity_fds", ()))
if any(type(fd) is not int or fd <= 2 for fd in capacity_fds):
    raise ValueError("invalid inherited capacity descriptors")
for fd in capacity_fds:
    os.fstat(fd)  # Require the real inherited descriptor, not a numeric claim.
owner = packet["executor_owner"]
owners = ensure_private_directory(root / "owners")
gone = threading.Event()
threading.Thread(
    target=lambda: (os.read(sys.stdin.fileno(), 1), gone.set()), daemon=True
).start()
owner_file = open_private_file(owners / owner, "a+b")
fcntl.flock(owner_file.fileno(), fcntl.LOCK_SH)


def authorized():
    with open_private_file(root / "state.lock", "a+b") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_SH)
        try:
            with open_private_file(
                root / "state.json", "r", create=False
            ) as state_file:
                state = json.load(state_file)
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
        t = state["tasks"].get(packet["task_id"])
        lease = state.get("leases", {}).get(packet["lease_id"])
        return bool(
            t
            and lease
            and t["state"] == "RUNNING"
            and all(
                t.get(k) == packet.get(k)
                for k in (
                    "run_id",
                    "task_id",
                    "generation",
                    "attempt",
                    "lease_id",
                    "executor_owner",
                )
            )
            and time.time() < t["started_at"] + t["timeout_seconds"]
        )


p = None
children = {}
out = bytearray()
err = bytearray()
reason = None
try:
    if gone.is_set() or not authorized():
        raise RuntimeError("revoked before spawn")
    p = subprocess.Popen(
        request["command"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
        pass_fds=(owner_file.fileno(), *capacity_fds),
    )
    native = psutil.Process(p.pid)
    birth = native.create_time()
    sel = selectors.DefaultSelector()
    pending = memoryview(
        json.dumps({"packet": packet, "context": request.get("context", {})}).encode()
    )
    for pipe, events in (
        (p.stdin, selectors.EVENT_WRITE),
        (p.stdout, selectors.EVENT_READ),
        (p.stderr, selectors.EVENT_READ),
    ):
        os.set_blocking(pipe.fileno(), False)
        sel.register(pipe, events)
    while sel.get_map():
        if native.is_running() and native.status() != psutil.STATUS_ZOMBIE:
            for child in native.children(recursive=True):
                children[(child.pid, child.create_time())] = child
        if gone.is_set() or not authorized():
            reason = "REVOKED_OR_DEADLINE"
            break
        for key, mask in sel.select(0.02):
            pipe = key.fileobj
            if pipe is p.stdin:
                count = os.write(pipe.fileno(), pending[:8192])
                pending = pending[count:]
                if not pending:
                    sel.unregister(pipe)
                    pipe.close()
            else:
                chunk = os.read(pipe.fileno(), 8192)
                if not chunk:
                    sel.unregister(pipe)
                    pipe.close()
                else:
                    (out if pipe is p.stdout else err).extend(chunk)
        if len(out) + len(err) > LIMIT:
            reason = "OUTPUT_BUDGET"
            break
    sel.close()
except Exception as exc:
    reason = type(exc).__name__ + ": " + str(exc)[:500]
finally:
    if p is not None:
        drained = drain_owned_group(p, native, birth, children)
        p.wait(timeout=5)
        if not drained:
            reason = "DESCENDANT_DRAIN_UNCONFIRMED"
        else:
            directory = ensure_private_directory(root / "drained")
            receipt = {
                "lease_id": packet["lease_id"],
                "owner": owner,
                "drained": True,
                "drain_scope": "owned_group_and_observed_descendants",
                "unobserved_detached_descendants_contained": False,
                "worker_pid": p.pid,
                "worker_returncode": p.returncode,
            }
            target = directory / (packet["lease_id"] + ".json")
            atomic_replace_bytes(
                target, json.dumps(receipt, separators=(",", ":")).encode()
            )
    if reason:
        reply = {"ok": False, "reason": reason}
    elif p is None or p.returncode:
        reply = {"ok": False, "reason": "WORKER_EXIT"}
    else:
        try:
            reply = {"ok": True, "result": json.loads(out)}
        except Exception:
            reply = {
                "ok": False,
                "reason": "MALFORMED_PROCESS_OUTPUT",
                "diagnostic": out.decode(errors="replace")[:4000],
            }
    print(json.dumps(reply), flush=True)
    owner_file.close()
    for fd in capacity_fds:
        os.close(fd)
