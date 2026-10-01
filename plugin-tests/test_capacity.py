"""Real spawned schedulers; events synchronize admission, not sleeps."""

import importlib, json, multiprocessing as mp, queue, sys, tempfile, unittest
from pathlib import Path

R = Path(__file__).resolve().parent


def load():
    # The plugin is a package (scheduler.py uses relative imports): import it as one,
    # also inside the spawned child processes.
    if str(R) not in sys.path:
        sys.path.insert(0, str(R))
    return importlib.import_module("external_orchestrator.scheduler")


def child(home, name, ready, starts, release, stop):
    m = load()

    def work(packet):
        starts.put({"profile": packet["profile"], "pid": __import__("os").getpid()})
        if not release.wait(15):
            raise TimeoutError("fixture not released")
        return s._simulated_worker(packet)

    s = m.ExternalScheduler(
        home, max_global=1, per_profile=1, worker=work, allow_simulated=True,
        quota_preflight=lambda packet: None,  # offline: no live account snapshot
    )
    try:
        view = s.create_run(
            {
                "owner_token": name,
                "parent_session_id": name,
                "profile": name,
                "tasks": [
                    {
                        "task_id": name,
                        "goal": "offline capacity fixture",
                        "timeout_seconds": 60,
                    }
                ],
            }
        )
        ready.put(view)
        stop.wait(20)
    finally:
        s.close()


class Capacity(unittest.TestCase):
    def scenario(self, kill_owner=False):
        c = mp.get_context("spawn")
        ready = c.Queue()
        starts = c.Queue()
        release = c.Event()
        stop = c.Event()
        with tempfile.TemporaryDirectory(prefix="gate-capacity-") as home:
            # A killed process can leave a multiprocessing Event's lock poisoned.
            # Its events must never be shared with the surviving fixture.
            ar, astop = (c.Event(), c.Event()) if kill_owner else (release, stop)
            a = c.Process(target=child, args=(home, "A", ready, starts, ar, astop))
            b = c.Process(target=child, args=(home, "B", ready, starts, release, stop))
            a.start()
            try:
                ready.get(timeout=10)
                self.assertEqual(starts.get(timeout=10)["profile"], "A")
                if kill_owner:
                    a.kill()
                    a.join(5)
                b.start()
                view = ready.get(timeout=10)
                state = json.loads((Path(home) / "state.json").read_text())
                if kill_owner:
                    self.assertEqual(starts.get(timeout=10)["profile"], "B")
                    self.assertEqual(state["tasks"]["A"]["state"], "FAILED")
                else:
                    # Both processes are live, A's worker remains held by an event.
                    self.assertTrue(a.is_alive() and b.is_alive())
                    self.assertEqual(
                        state["tasks"]["A"]["state"], "RUNNING", "live owner reclaimed"
                    )
                    self.assertEqual(
                        state["tasks"]["B"]["state"],
                        "PENDING",
                        "capacity one admitted second worker",
                    )
                    release.set()
                    self.assertEqual(
                        starts.get(timeout=10)["profile"],
                        "B",
                        "queued worker never admitted after release",
                    )
            finally:
                release.set()
                stop.set()
                for p in (a, b):
                    if p.pid is not None:
                        p.join(10)
                        if p.is_alive():
                            p.kill()
                            p.join(5)
            self.assertEqual(b.exitcode, 0)
            if not kill_owner:
                self.assertEqual(a.exitcode, 0)

    def test_second_scheduler_preserves_live_owner(self):
        self.scenario()

    def test_dead_owner_is_reconciled(self):
        self.scenario(True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
