import unittest
from external_orchestrator.planner import plan_packets


class Planner(unittest.TestCase):
    def unit(self, name, source="file:a", deps=None):
        return {
            "unit_id": name,
            "goal": "inspect " + name,
            "evidence_scope": source,
            "capability_profile": "read-only",
            "write_scope": [],
            "dependencies": deps or [],
        }

    def test_same_source_groups_and_dependencies_remap(self):
        p = plan_packets(
            [self.unit("a"), self.unit("b"), self.unit("c", "file:c", ["a"])]
        )
        self.assertEqual(len(p), 2)
        self.assertEqual(p[0]["unit_ids"], ["a", "b"])
        self.assertEqual(p[1]["dependencies"], [p[0]["task_id"]])

    def test_different_sources_and_write_owners_stay_separate(self):
        a = self.unit("a")
        b = self.unit("b", "file:b")
        c = {**self.unit("c"), "write_scope": ["/tmp/write-c"]}
        self.assertEqual(len(plan_packets([a, b, c])), 3)

    def test_oversize_never_silently_truncates(self):
        with self.assertRaises(ValueError):
            plan_packets([{**self.unit("a"), "goal": "x" * 1000}], max_packet_bytes=100)

    def test_cycles_rejected(self):
        with self.assertRaises(ValueError):
            plan_packets([self.unit("a", deps=["b"]), self.unit("b", deps=["a"])])

    def test_duplicate_units_rejected(self):
        with self.assertRaises(ValueError):
            plan_packets([self.unit("a"), self.unit("a")])

    def test_split_preserves_all_units_exactly_once(self):
        units = [self.unit(str(i)) for i in range(20)]
        p = plan_packets(units, max_packet_bytes=800)
        self.assertGreater(len(p), 1)
        self.assertEqual(
            [x for row in p for x in row["unit_ids"]], [str(i) for i in range(20)]
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
