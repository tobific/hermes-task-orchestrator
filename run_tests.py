#!/usr/bin/env python3
"""Run this package's tests against a Hermes checkout.

Usage:
    python run_tests.py --hermes /path/to/hermes-checkout [--suite plugin|seam|all] [--python PYTHON] [--log FILE]

The checkout must be a DISPOSABLE copy of Hermes 8a3ede1b, with or without the patches in
``patches/``: this script copies test files into it. PYTHON must have Hermes' test
dependencies (see TESTING.md); default: the interpreter running this script.

What it copies:
  plugin suite  plugin/external_orchestrator + plugin-tests/   ->  <hermes>/orch_tests/
  seam suite    seam-tests/service-tier/tests/*                 ->  <hermes>/tests/  (new files only; a file
                                                                    that already exists must be identical,
                                                                    else it stops)
                seam-tests/contracts/*                          ->  <hermes>/seam_contracts/
                plugin/external_orchestrator                    ->  <hermes>/seam_contracts/plugin/
Each test file runs in its own pytest process, with HOME, HERMES_HOME and TMPDIR in a fresh
temporary folder and a minimal environment. This is NOT a sandbox: it does not block network
access or writes elsewhere. The tests themselves make no network calls.
Exit code 0 only if no test failed or errored, no expected failure passed, and every pytest
process exited cleanly. Expected results are in TESTING.md.
"""
import argparse
import filecmp
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
UPSTREAM = "8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96"

PLUGIN_TESTS = [
    "test_planner.py",
    "test_contracts.py",
    "test_named_regressions.py",
    "test_capacity.py",
    "test_native_transport_observation_gap.py",
]

C = "seam_contracts/"
# seam label -> test files (see README "Seam labels")
SEAM_TESTS = {
    "GW-03": ["tests/agent/test_fast_consumer_wire.py", "tests/hermes_cli/test_service_tier_consumption.py"],
    "DC-01": [C + "orchestrator/test_admission_policy.py", C + "orchestrator/test_structured_history.py"],
    "DC-02": [C + "tool_policy/test_guard_adversarial.py", C + "tool_policy/test_guard_lifetime.py"],
    "DC-03": [C + "orchestrator/test_native_completion_fence_contract.py",
              C + "orchestrator/test_parent_fence_adversarial.py"],
    "DC-04": [C + "orchestrator/test_automatic_selector.py", C + "orchestrator/test_selector_native_dispatch.py"],
    "DC-05": [C + "orchestrator/test_claim_validation_acceptance.py",
              C + "orchestrator/test_native_worker_adapter.py"],
    "DC-06": [C + "orchestrator/test_host_dispatch_pinning.py", C + "orchestrator/test_worker_policy_authority.py"],
    "DC-07": [C + "orchestrator/test_quota_authority_race.py", C + "orchestrator/test_quota_gate.py"],
    "DC-08": [C + "cron/test_tier_override_matrix.py", C + "cron/test_tool_policy.py"],
    "DC-09": [C + "cron_memory/test_cron_memory_boundary_witness.py"],
    "DC-10": [C + "orchestrator/test_diagnostic_projection.py", C + "orchestrator/test_diagnostic_sidecar.py"],
    "TM-01": [C + "task_authority/test_execution_authority_carry.py", C + "task_authority/test_file_authority.py",
              C + "task_authority/test_local_authority_forwarding.py"],
}

COUNT = re.compile(r"(\d+) (passed|failed|skipped|xfailed|xpassed|errors?)")


def copy_tree_new_only(src: Path, dst: Path) -> None:
    for f in sorted(p for p in src.rglob("*") if p.is_file() and "__pycache__" not in p.parts):
        target = dst / f.relative_to(src)
        if target.exists():
            if not filecmp.cmp(f, target, shallow=False):
                sys.exit(f"refusing to overwrite a different existing file: {target}")
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(f, target)


def check_checkout(hermes: Path) -> None:
    if not (hermes / "run_agent.py").is_file():
        sys.exit("--hermes must be a Hermes checkout (run_agent.py not found)")
    if not (hermes / ".git").exists():
        print("note: not a git checkout; cannot verify that it is based on " + UPSTREAM[:10], flush=True)
        return
    r = subprocess.run(["git", "-C", str(hermes), "merge-base", "--is-ancestor", UPSTREAM, "HEAD"],
                       capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(f"the checkout's HEAD is not based on Hermes {UPSTREAM}")


def run_file(python, hermes, rel, pythonpath, scratch):
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": str(scratch / "home"),
        "HERMES_HOME": str(scratch / "home" / ".hermes"),
        "TMPDIR": str(scratch / "tmp"),
        "PYTHONPATH": os.pathsep.join(pythonpath),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONHASHSEED": "0",
        "TZ": "UTC",
        "LANG": "C.UTF-8",
    }
    for d in ("home/.hermes", "tmp"):
        (scratch / d).mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        [python, "-B", "-m", "pytest", "-p", "no:cacheprovider", "-q", "-rA", rel],
        cwd=hermes, env=env, capture_output=True, text=True, timeout=600,
    )
    out = proc.stdout + proc.stderr
    counts = {}
    for line in out.splitlines()[::-1]:
        found = COUNT.findall(line)
        if found and (" in " in line):
            for n, kind in found:
                counts["errors" if kind.startswith("error") else kind] = int(n)
            break
    # Never lose a non-zero exit: if pytest failed but no failure/error was counted, count one error.
    if proc.returncode != 0 and not (counts.get("failed") or counts.get("errors")):
        counts["errors"] = counts.get("errors", 0) + 1
    return counts, out, proc.returncode


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hermes", required=True, type=Path)
    ap.add_argument("--suite", choices=["plugin", "seam", "all"], default="all")
    ap.add_argument("--python", default=sys.executable)
    ap.add_argument("--log", type=Path, help="write full pytest output here")
    a = ap.parse_args()
    hermes = a.hermes.resolve()
    check_checkout(hermes)
    jobs = []  # (label, relative test path, pythonpath)
    if a.suite in ("plugin", "all"):
        copy_tree_new_only(HERE / "plugin", hermes / "orch_tests")
        copy_tree_new_only(HERE / "plugin-tests", hermes / "orch_tests")
        for t in PLUGIN_TESTS:
            jobs.append(("plugin", "orch_tests/" + t, [str(hermes), str(hermes / "orch_tests")]))
    if a.suite in ("seam", "all"):
        copy_tree_new_only(HERE / "seam-tests" / "service-tier" / "tests", hermes / "tests")
        sc = hermes / "seam_contracts"
        copy_tree_new_only(HERE / "seam-tests" / "contracts", sc)
        copy_tree_new_only(HERE / "plugin", sc / "plugin")
        path = [str(hermes), str(sc)] + [str(sc / d) for d in
                ("plugin", "orchestrator", "cron", "task_authority", "tool_policy")]
        for label, files in SEAM_TESTS.items():
            for t in files:
                jobs.append((label, t, path))
    totals, by_label, logs, bad_exit = {}, {}, [], 0
    with tempfile.TemporaryDirectory(prefix="orch-tests-") as tmp:
        for i, (label, rel, path) in enumerate(jobs):
            counts, out, rc = run_file(a.python, hermes, rel, path, Path(tmp) / str(i))
            bad_exit += rc != 0
            logs.append(f"===== {label} {rel} (exit {rc})\n{out}")
            line = ", ".join(f"{v} {k}" for k, v in sorted(counts.items())) or "no result"
            print(f"{label:7} {rel}: {line}", flush=True)
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
                by_label.setdefault(label, {}).setdefault(k, 0)
                by_label[label][k] += v
    if a.log:
        a.log.write_text("\n".join(logs))
    print("\nper label:")
    for label, c in by_label.items():
        bad = c.get("failed", 0) + c.get("errors", 0) + c.get("xpassed", 0)
        print(f"  {label:7} {'PASS' if not bad and c.get('passed') else 'FAIL'}  "
              + ", ".join(f"{v} {k}" for k, v in sorted(c.items())))
    print("TOTAL: " + ", ".join(f"{v} {k}" for k, v in sorted(totals.items()))
          + f"; pytest processes with non-zero exit: {bad_exit}")
    sys.exit(1 if totals.get("failed") or totals.get("errors") or totals.get("xpassed") or bad_exit else 0)


if __name__ == "__main__":
    main()
