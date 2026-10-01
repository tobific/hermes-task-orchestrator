"""Drain an unreaped owned process group, plus observed descendants.

Not recursive containment: an unobserved descendant that changes sessions can
escape on macOS. Never reap the group leader before this function returns;
its retained child PID is the kernel anchor preventing process-group ID reuse.
"""

import os
import signal
import time
import psutil


def _live(process):
    try:
        return process.is_running() and process.status() not in {
            psutil.STATUS_ZOMBIE,
            psutil.STATUS_DEAD,
        }
    except psutil.NoSuchProcess:
        return False


def drain_owned_group(process, native, birth, children, timeout=5):
    end = time.monotonic() + timeout
    while True:
        try:
            # is_running checks reuse; the original leader remains unreaped,
            # including when zombie. Never signal an unanchored numeric group.
            if not native.is_running() or native.create_time() != birth:
                return False
            # Darwin getpgid excludes zombies, though the unreaped child PID
            # still pins the group number. Validate live leaders only.
            if (
                native.status() != psutil.STATUS_ZOMBIE
                and os.getpgid(process.pid) != process.pid
            ):
                return False
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass  # The anchored group is already empty.
            group_alive = []
            for pid in psutil.pids():
                try:
                    if os.getpgid(pid) == process.pid:
                        child = psutil.Process(pid)
                        if _live(child):
                            group_alive.append(child)
                except (ProcessLookupError, psutil.NoSuchProcess):
                    pass
            observed_alive = []
            for (_, born), child in children.items():
                if _live(child) and child.create_time() == born:
                    try:
                        child.kill()
                        observed_alive.append(child)
                    except psutil.NoSuchProcess:
                        pass
            if not group_alive and not observed_alive:
                return True
        except (OSError, psutil.Error):
            return False
        if time.monotonic() >= end:
            return False
        time.sleep(0.01)
