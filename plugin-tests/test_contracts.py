import importlib, sys, tempfile, threading, time, unittest
from pathlib import Path

# The plugin is a package (scheduler.py uses relative imports): import it as one.
R = Path(__file__).resolve().parent
sys.path.insert(0, str(R))
m = importlib.import_module("external_orchestrator.scheduler")


def args(name="one", tasks=None, **extra):
    return {
        "owner_token": "host",
        "parent_session_id": "parent",
        "profile": "p",
        "tasks": tasks or [{"task_id": name, "goal": "fixture"}],
        **extra,
    }


def owned(run):
    return {
        "run_id": run["run_id"],
        "owner_token": "host",
        "parent_session_id": "parent",
        "profile": "p",
    }


class Contracts(unittest.TestCase):
    def setUp(self):
        self.home = tempfile.TemporaryDirectory()
        self.schedulers = []
        self.release = threading.Event()

    def tearDown(self):
        self.release.set()
        for s in self.schedulers:
            s.close()
        self.home.cleanup()

    def scheduler(self, **kw):
        # Offline: the real quota gate needs a live account snapshot and fails closed without one.
        kw.setdefault("quota_preflight", lambda packet: None)
        s = m.ExternalScheduler(self.home.name, allow_simulated=True, **kw)
        self.schedulers.append(s)
        return s

    def test_profile_round_robin_does_not_drain_one_backlog_first(self):
        seen = []
        first = threading.Event()

        def worker(p):
            seen.append(p["task_id"])
            if p["task_id"] == "a":
                first.set()
                self.release.wait(5)
            return s._simulated_worker(p)

        s = self.scheduler(max_global=1, worker=worker)
        a = s.create_run(
            args(tasks=[{"task_id": "a", "goal": "a"}, {"task_id": "a2", "goal": "a2"}])
        )
        self.assertTrue(first.wait(2))
        b = s.create_run(args("b", profile="other"))
        self.release.set()
        s.join({**owned(a), "timeout_seconds": 3})
        self.assertEqual(seen[:3], ["a", "b", "a2"])

    def test_task_cannot_override_parent_session(self):
        s = self.scheduler()
        with self.assertRaises(m.OrchestrationError):
            s.create_run(
                args(
                    tasks=[{"task_id": "a", "goal": "a", "parent_session_id": "forged"}]
                )
            )

    def test_task_cannot_override_profile_or_route(self):
        s = self.scheduler()
        for override in ({"profile": "other"}, {"route": {"provider": "wrong"}}):
            with self.assertRaises(m.OrchestrationError):
                s.create_run(args(tasks=[{"task_id": "a", "goal": "a", **override}]))

    def test_serialized_envelope_budget_not_just_answer(self):
        def worker(p):
            result = s._simulated_worker(p)
            result["evidence"] = ["x" * 2000] * 32
            return result

        s = self.scheduler(worker=worker)
        run = s.create_run(args())
        v = s.join({**owned(run), "timeout_seconds": 3})
        self.assertEqual(v["state"], "FAILED")

    def test_optional_failure_does_not_fail_required_success(self):
        s = self.scheduler()
        run = s.create_run(
            args(
                tasks=[
                    {"task_id": "a", "goal": "a"},
                    {
                        "task_id": "b",
                        "goal": "b",
                        "worker_mode": "fail",
                        "required": False,
                    },
                ]
            )
        )
        self.assertEqual(
            s.join({**owned(run), "timeout_seconds": 3})["state"], "SUCCEEDED"
        )

    def test_admission_route_is_refreshed_for_queued_work(self):
        first = threading.Event()
        seen = []
        policy = {
            "provider": "openai-codex",
            "model": "gpt-6-luna",
            "reasoning_effort": "xhigh",
            "service_tier": "normal",
        }

        def worker(p):
            seen.append((p["task_id"], p["route"]["service_tier"]))
            if p["task_id"] == "a":
                first.set()
                self.release.wait(5)
            return s._simulated_worker(p)

        s = self.scheduler(max_global=1, worker=worker)
        s.route_resolver = lambda profile: dict(policy)
        a = s.create_run(args("a"))
        self.assertTrue(first.wait(2))
        b = s.create_run(args("b"))
        policy["service_tier"] = "priority"
        self.release.set()
        s.join({**owned(b), "timeout_seconds": 3})
        self.assertEqual(seen, [("a", "normal"), ("b", "priority")])

    def test_admission_policy_failure_never_starts_worker(self):
        started = []
        s = self.scheduler(worker=lambda p: started.append(p))

        def unavailable(profile):
            raise m.OrchestrationError("route policy unavailable")

        s.route_resolver = unavailable
        with self.assertRaises(m.OrchestrationError):
            s.create_run(args())
        self.assertEqual(started, [])

    def test_task_source_grouping_and_narrowed_write_scope(self):
        s = self.scheduler()
        root = self.home.name
        run = s.create_run(
            args(
                tasks=[
                    {
                        "task_id": "a",
                        "goal": "a",
                        "evidence_scope": "file:a",
                        "write_scope": [str(Path(root) / "a")],
                    },
                    {
                        "task_id": "b",
                        "goal": "b",
                        "evidence_scope": "file:b",
                        "write_scope": [str(Path(root) / "b")],
                    },
                ],
                write_scope=[root],
            )
        )
        packets = s._mutate(lambda state: list(state["tasks"].values()))
        self.assertEqual({p["evidence_scope"] for p in packets}, {"file:a", "file:b"})
        self.assertTrue(all(p.get("group_key") for p in packets))

    def test_runtime_requires_host_typed_transport_observation(self):
        s = m.ExternalScheduler(self.home.name, worker=lambda p: s._simulated_worker(p), quota_preflight=lambda packet: None)
        self.schedulers.append(s)
        run = s.create_run(args())
        v = s.join({**owned(run), "timeout_seconds": 3})
        self.assertEqual(v["state"], "FAILED")

    def test_grouped_work_requires_every_unit_result(self):
        from external_orchestrator.planner import plan_packets

        tasks = plan_packets(
            [
                {
                    "unit_id": x,
                    "goal": x,
                    "evidence_scope": "file:a",
                    "capability_profile": "read-only",
                    "write_scope": [],
                }
                for x in ("a", "b")
            ]
        )
        s = self.scheduler()
        run = s.create_run(args(tasks=tasks, capability_profile="read-only"))
        self.assertEqual(
            s.join({**owned(run), "timeout_seconds": 3})["state"], "FAILED"
        )

    def test_grouped_work_executes_through_scheduler(self):
        from external_orchestrator.planner import plan_packets

        tasks = plan_packets(
            [
                {
                    "unit_id": x,
                    "goal": x,
                    "evidence_scope": "file:a",
                    "capability_profile": "read-only",
                    "write_scope": [],
                }
                for x in ("a", "b")
            ]
        )
        seen = []

        def worker(p):
            seen.extend(p["unit_ids"])
            result = s._simulated_worker(p)
            result["unit_results"] = [
                {"unit_id": x, "status": "succeeded", "answer": "checked"}
                for x in p["unit_ids"]
            ]
            return result

        s = self.scheduler(worker=worker)
        run = s.create_run(args(tasks=tasks, capability_profile="read-only"))
        self.assertEqual(
            s.join({**owned(run), "timeout_seconds": 3})["state"], "SUCCEEDED"
        )
        self.assertEqual(seen, ["a", "b"])

    def test_superseded_blocked_task_keeps_dependencies(self):
        started = threading.Event()

        def worker(p):
            if p["task_id"] == "a":
                started.set()
                self.release.wait(5)
            return s._simulated_worker(p)

        s = self.scheduler(max_global=2, per_profile=2, worker=worker)
        run = s.create_run(
            args(
                tasks=[
                    {"task_id": "a", "goal": "a"},
                    {"task_id": "b", "goal": "b", "dependencies": ["a"]},
                ]
            )
        )
        self.assertTrue(started.wait(2))
        s.supersede({**owned(run), "task_id": "b"})
        states = s.status(owned(run))["task_states"]
        self.assertEqual(
            next(x["state"] for x in states if x["task_id"] == "b"), "BLOCKED"
        )

    def test_late_completion_without_pump_is_not_success(self):
        done = threading.Event()

        def worker(p):
            time.sleep(0.04)
            done.set()
            return s._simulated_worker(p)

        s = self.scheduler(worker=worker)
        run = s.create_run(
            args(
                tasks=[
                    {
                        "task_id": "a",
                        "goal": "a",
                        "timeout_seconds": 0.01,
                        "max_attempts": 1,
                    }
                ]
            )
        )
        self.assertTrue(done.wait(2))
        s.close()
        state = s._mutate(lambda st: st["tasks"]["a"]["state"])
        self.assertEqual(state, "TIMED_OUT")

    def test_enqueue_obeys_task_budget(self):
        s = self.scheduler()
        run = s.create_run(
            args(tasks=[{"task_id": str(i), "goal": "x"} for i in range(64)])
        )
        with self.assertRaises(m.OrchestrationError):
            s.enqueue({**owned(run), "task_id": "overflow", "goal": "x"})

    def test_shared_queue_budget_is_enforced(self):
        old = m.MAX_QUEUED_TASKS
        m.MAX_QUEUED_TASKS = 1
        try:

            def worker(p):
                self.release.wait(5)
                return s._simulated_worker(p)

            s = self.scheduler(max_global=1, worker=worker, max_queued_tasks=1)
            run = s.create_run(args(tasks=[{"task_id": "a", "goal": "a"}]))
            s.enqueue({**owned(run), "task_id": "b", "goal": "b"})
            with self.assertRaises(m.OrchestrationError):
                s.enqueue({**owned(run), "task_id": "c", "goal": "c"})
        finally:
            m.MAX_QUEUED_TASKS = old

    def test_generation_history_has_a_bound(self):
        old = m.MAX_GENERATIONS
        m.MAX_GENERATIONS = 1
        try:
            s = self.scheduler()
            run = s.create_run(args(tasks=[{"task_id": "a", "goal": "a"}]))
            with self.assertRaises(m.OrchestrationError):
                s.supersede({**owned(run), "task_id": "a"})
        finally:
            m.MAX_GENERATIONS = old

    def test_result_space_is_reserved_before_execution(self):
        old = m.MAX_STATE_BYTES
        m.MAX_STATE_BYTES = 1024
        try:
            s = self.scheduler()
            with self.assertRaises(m.OrchestrationError):
                s.create_run(args(tasks=[{"task_id": "a", "goal": "a"}]))
        finally:
            m.MAX_STATE_BYTES = old

    def test_dynamic_route_failure_is_terminal_without_execution(self):
        first = threading.Event()

        def worker(p):
            first.set()
            self.release.wait(5)
            return s._simulated_worker(p)

        s = self.scheduler(max_global=1, worker=worker)
        run = s.create_run(
            args(tasks=[{"task_id": "a", "goal": "a"}, {"task_id": "b", "goal": "b"}])
        )
        self.assertTrue(first.wait(2))

        def broken(profile):
            raise RuntimeError("policy unavailable")

        s.route_resolver = broken
        self.release.set()
        s._executor.shutdown(wait=True)
        s.pump()
        state = s.status(owned(run))
        self.assertEqual(state["state"], "FAILED")
        self.assertEqual(
            next(t for t in state["task_states"] if t["task_id"] == "b")["host_status"],
            "route_policy_unavailable",
        )

    def test_runtime_retries_host_timeouts_but_not_model_claims(self):
        def worker(p):
            raise TimeoutError("transport timeout")

        s = m.ExternalScheduler(self.home.name, worker=worker, quota_preflight=lambda packet: None)
        self.schedulers.append(s)
        run = s.create_run(
            args(tasks=[{"task_id": "a", "goal": "a", "max_attempts": 2}])
        )
        state = s.join({**owned(run), "timeout_seconds": 3})
        self.assertEqual(state["task_states"][0]["attempt"], 2)

    def test_reasoning_alias_is_bound_or_rejected(self):
        self.assertEqual(
            m.ExternalScheduler._route({"reasoning_effort": "low"})["effort"], "low"
        )
        with self.assertRaises(m.OrchestrationError):
            m.ExternalScheduler._route({"effort": "xhigh", "reasoning_effort": "low"})

    def test_configuration_cannot_expand_shared_capacity(self):
        self.scheduler(max_global=1)
        with self.assertRaises(m.OrchestrationError):
            self.scheduler(max_global=2)

    def test_cross_run_task_collision_rejected(self):
        s = self.scheduler()
        s.create_run(args())
        with self.assertRaises(m.OrchestrationError):
            s.create_run(args())

    def test_dependency_cycle_rejected(self):
        s = self.scheduler()
        with self.assertRaises(m.OrchestrationError):
            s.create_run(
                args(
                    tasks=[
                        {"task_id": "a", "goal": "a", "dependencies": ["b"]},
                        {"task_id": "b", "goal": "b", "dependencies": ["a"]},
                    ]
                )
            )

    def test_cross_run_dependency_rejected(self):
        s = self.scheduler()
        s.create_run(args("a"))
        b = s.create_run(args("b"))
        with self.assertRaises(m.OrchestrationError):
            s.enqueue({**owned(b), "task_id": "c", "goal": "c", "dependencies": ["a"]})

    def test_cancellation_retains_capacity_until_worker_exits(self):
        started = threading.Event()

        def worker(p):
            started.set()
            self.release.wait(5)
            return s._simulated_worker(p)

        s = self.scheduler(max_global=1, worker=worker)
        run = s.create_run(args())
        self.assertTrue(started.wait(2))
        s.cancel(owned(run))
        other = s.create_run(args("other"))
        self.assertEqual(s.status(owned(other))["task_states"][0]["state"], "PENDING")

    def test_timeout_retains_capacity_until_worker_exits(self):
        started = threading.Event()

        def worker(p):
            started.set()
            self.release.wait(5)
            return s._simulated_worker(p)

        s = self.scheduler(max_global=1, worker=worker)
        run = s.create_run(
            args(
                tasks=[
                    {
                        "task_id": "a",
                        "goal": "a",
                        "timeout_seconds": 0.01,
                        "max_attempts": 1,
                    }
                ]
            )
        )
        self.assertTrue(started.wait(2))
        time.sleep(0.03)
        s.pump()
        other = s.create_run(args("other"))
        self.assertEqual(s.status(owned(other))["task_states"][0]["state"], "PENDING")

    def test_overlapping_write_scopes_do_not_overlap_workers(self):
        started = threading.Event()

        def worker(p):
            started.set()
            self.release.wait(5)
            return s._simulated_worker(p)

        s = self.scheduler(max_global=2, per_profile=2, worker=worker)
        run = s.create_run(args("a", write_scope=[self.home.name]))
        self.assertTrue(started.wait(2))
        other = s.create_run(
            args("b", write_scope=[str(Path(self.home.name) / "child")])
        )
        self.assertEqual(s.status(owned(other))["task_states"][0]["state"], "PENDING")

    def test_nonretryable_policy_exception(self):
        def worker(p):
            raise m.OrchestrationError("DENIED")

        s = self.scheduler(worker=worker)
        run = s.create_run(args())
        v = s.join({**owned(run), "timeout_seconds": 3})
        self.assertEqual(v["task_states"][0]["attempt"], 1)

    def test_cancelled_generation_not_delivered_after_supersede(self):
        s = self.scheduler()
        run = s.create_run(args())
        s.join({**owned(run), "timeout_seconds": 3})
        s.supersede({**owned(run), "task_id": "one"})
        s.join({**owned(run), "timeout_seconds": 3})
        events = s.collect(owned(run))["events"]
        self.assertEqual({e["generation"] for e in events}, {1})


if __name__ == "__main__":
    unittest.main(verbosity=2)
