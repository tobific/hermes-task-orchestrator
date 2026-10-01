# Testing

The tests themselves are offline: no network, no credentials, no provider calls (fake providers and mocked
transports). **Preparing them needs network:** cloning Hermes and building Hermes' Python environment download code
and packages. The tests need a DISPOSABLE checkout of Hermes at upstream commit
8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96 (the test script copies test files into it) and a Python environment with
Hermes' test dependencies.

## How the blocks fit together

The blocks below are meant to be pasted **in order, in one shell session**. They depend on each other:

| Block | Needs | Network |
|---|---|---|
| 0. Settings and guard | nothing | no |
| 1. Test checkout and environment | block 0 | **yes** (clone, environment build) |
| 2. Run with the patches | blocks 0 and 1 | no |
| 3. Control run without the patches | block 0, and the environment from block 1 | **yes** (clone) |

Each block is one `&&` chain: it stops at the first failing step and prints nothing after that step's error, so a
failed clone, checkout, environment build or patch is never followed by tests on the wrong state. A block that
succeeds ends with a line starting with `OK:` or with the test total.

## 0. Settings and guard

Choose ONE new folder for everything below; it may exist (empty) or not exist yet, but its parent folder must exist.
Set `WORK` to its absolute path and `ORCH_PKG` to this repository, for example:

```sh
export WORK="$HOME/orchestrator-test"
export ORCH_PKG=/path/to/hermes-task-orchestrator
```

Then paste the guard. Every block below starts with it. It resolves the physical path of `WORK` (symlinks, `.` and
`..` resolved) into `W`, and refuses when:

- `WORK` is empty, unset or not absolute, or `HOME` is unset;
- `WORK` itself, after removing any trailing slashes (so `link` and `link/` are treated alike), is `/`, a symlink or
  a file. Symlinks in the folders above `WORK` are allowed; they are resolved, and the checks below apply to the
  resolved path;
- the parent folder of a not-yet-existing `WORK` does not exist, or its last part is `.` or `..`;
- the resolved folder is `/`, your home folder, or any folder that contains your home folder (also when reached
  through an alias such as `/.` or `/usr/..`);
- a checkout or environment folder the block is about to create already exists (use a fresh `WORK` then).

The guard checks the folders at the moment it runs. It lowers the risk of writing to the wrong place; it is not a
guarantee (for example against folders changed by another program after the check). Use a folder only you control.

```sh
work_ok() {
  w="${WORK:-}"
  [ -n "$w" ] || { echo "refused: WORK is empty or unset" >&2; return 1; }
  case "$w" in /*) ;; *) echo "refused: WORK must be an absolute path" >&2; return 1;; esac
  [ -n "${HOME:-}" ] || { echo "refused: HOME is unset" >&2; return 1; }
  w=$(printf '%s' "$w" | sed 's#/*$##')
  [ -n "$w" ] || { echo "refused: WORK is /" >&2; return 1; }
  [ ! -L "$w" ] || { echo "refused: WORK is a symlink" >&2; return 1; }
  if [ -e "$w" ]; then
    [ -d "$w" ] || { echo "refused: WORK exists and is not a folder" >&2; return 1; }
    W=$(cd -P -- "$w" && pwd -P) || { echo "refused: cannot resolve WORK" >&2; return 1; }
  else
    b=$(basename -- "$w")
    case "$b" in .|..) echo "refused: WORK must not end in . or .." >&2; return 1;; esac
    p=$(dirname -- "$w")
    [ -d "$p" ] || { echo "refused: the parent folder of WORK does not exist: $p" >&2; return 1; }
    W=$(cd -P -- "$p" && pwd -P) || { echo "refused: cannot resolve the parent of WORK" >&2; return 1; }
    W="${W%/}/$b"
  fi
  W=$(printf '%s' "$W" | sed 's#/*$##')
  hp=$(cd -P -- "$HOME" && pwd -P) || { echo "refused: cannot resolve HOME" >&2; return 1; }
  hp=$(printf '%s' "$hp" | sed 's#/*$##')
  case "$hp/" in "$W"/*) echo "refused: WORK resolves to /, your home folder or a folder above it: ${W:-/}" >&2; return 1;; esac
  for t in "$@"; do
    if [ -e "$W/$t" ] || [ -L "$W/$t" ]; then echo "refused: $W/$t already exists (use a fresh WORK)" >&2; return 1; fi
  done
}
```

`work_ok hermes-test` means: the guard passes, and `$W/hermes-test` does not exist yet.

The test commands never use your Hermes home: `run_tests.py` gives every test process its own temporary `HOME` and
`HERMES_HOME`. Installing the plugin into a real Hermes home is covered by the guarded commands in the README.

## 1. Test checkout and environment (needs network)

Builds Hermes' own test environment as its CONTRIBUTING.md describes (Python 3.11 to 3.14; `python` must be one of
those):

```sh
work_ok hermes-test hermes-test-env &&
  mkdir -p "$W" &&
  git clone https://github.com/NousResearch/hermes-agent.git "$W/hermes-test" &&
  git -C "$W/hermes-test" checkout --detach 8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96 &&
  (cd "$W/hermes-test" && python -m pm.build_env --source . --out "$W/hermes-test-env" --group dev --group test --extra messaging) &&
  echo "OK: test checkout and environment ready in $W"
```

The tests need, from that environment: pytest 9.1.1, pytest-asyncio 1.3.0, httpx 0.28.1, openai 2.24.0,
psutil 7.2.2 (all core or `dev` dependencies of Hermes) and discord.py 2.7.1 (Hermes' `messaging` extra; the
gateway tests build a Discord adapter offline). Those are the versions the expected results below were measured
with.

## 2. Run with the patches (offline; needs block 1)

The chain first checks that the checkout is still exactly at 8a3ede1b with no changes (so the patches are applied
only once, to a fresh checkout), then applies the 8 patches and runs the tests:

```sh
work_ok && [ -f "${ORCH_PKG:-}/run_tests.py" ] && [ -x "$W/hermes-test-env/bin/python" ] &&
  [ "$(git -C "$W/hermes-test" rev-parse HEAD)" = 8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96 ] &&
  [ -z "$(git -C "$W/hermes-test" status --porcelain)" ] &&
  git -C "$W/hermes-test" apply --index "$ORCH_PKG"/patches/*.patch &&
  "$W/hermes-test-env/bin/python" "$ORCH_PKG/run_tests.py" \
       --hermes "$W/hermes-test" --suite all \
       --python "$W/hermes-test-env/bin/python" --log "$W/orch-tests.log"
```

## 3. Control run without the patches (needs network for the clone, and the environment from block 1)

A second fresh checkout, no `git apply`:

```sh
work_ok hermes-control && [ -f "${ORCH_PKG:-}/run_tests.py" ] && [ -x "$W/hermes-test-env/bin/python" ] &&
  git clone https://github.com/NousResearch/hermes-agent.git "$W/hermes-control" &&
  git -C "$W/hermes-control" checkout --detach 8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96 &&
  "$W/hermes-test-env/bin/python" "$ORCH_PKG/run_tests.py" \
       --hermes "$W/hermes-control" --suite all \
       --python "$W/hermes-test-env/bin/python" --log "$W/orch-tests-control.log"
```

`run_tests.py` checks that the checkout is based on 8a3ede1b, copies the plugin and the tests into it (it refuses
to overwrite a different existing file), runs every test file in its own pytest process with HOME, HERMES_HOME
and TMPDIR in a temporary folder, and prints one line per file, one line per seam label and a total.
Exit code 0 means: no failure, no error, no unexpected pass of an expected failure, and every pytest process
exited cleanly. It is not a sandbox (it does not block network or file access); the tests make no network
calls. Use a fresh checkout for every run.

Suites: `--suite plugin` (plugin unit tests), `--suite seam` (seam contract tests), `--suite all` (both).

## 4. Expected results (Python 3.14.6, macOS, offline, in an OS sandbox without network)

| Label | Tests | WITH the 8 patches | WITHOUT the patches |
|---|---|---|---|
| plugin | plugin-tests/ (5 files) | 39 passed, 1 xfail | 39 passed, 1 xfail |
| GW-03 | service-tier policy (0001) | 9 passed, 8 xfail | 9 failed, 8 xfail |
| DC-01 | task queue and admission | 20 passed | 10 failed, 10 passed |
| DC-02 | required tool policy (0002) | 28 passed | 23 failed, 5 passed |
| DC-03 | completion fence (0003) | 7 passed | 7 failed |
| DC-04 | read-only selector | 17 passed | 17 passed |
| DC-05 | request observation / worker binding (0004, 0005) | 19 passed | 11 failed, 8 passed |
| DC-06 | worker route policy | 4 passed | 2 failed, 2 passed |
| DC-07 | quota admission | 22 passed | 3 failed, 19 passed |
| DC-08 | cron per-job tier (0008) | 16 passed | 16 failed |
| DC-09 | cron memory isolation (0008) | 3 passed | 3 errors |
| DC-10 | diagnostic hooks (0006) | 14 passed | 13 failed, 1 passed |
| TM-01 | per-task capability authority (0007) | 16 passed | 14 failed, 1 error, 1 passed |
| **TOTAL** | | **214 passed, 9 xfail, 0 failed; exit 0** | 102 passed, 108 failed, 4 errors, 9 xfail; exit 1 |

What this shows:
- The plugin unit tests test the plugin on its own, so they give the same result on both trees.
- The seam tests show that the patches are needed: without them 11 of the 12 seam groups fail.
  DC-04 passes on both because the read-only selector only uses a hook upstream already has.
- The results are from offline tests with simulated workers. Nothing was run against a live model, account,
  quota or rate limit, on Linux, or on a later Hermes release.

## 5. The 9 expected failures (marked `xfail(strict=True)`) and why

1. `plugin-tests/test_native_transport_observation_gap.py::test_native_terminal_sdk_call_exposes_host_route_observation`
   A diagnostic probe for an `agent._last_transport_observation` attribute on the native Codex stream. Neither
   upstream nor patch 0004 provides that attribute (patch 0004 uses an attached `_codex_request_observer`
   instead, covered by the DC-05 tests). Kept as a record of that gap; it fails on purpose.
2-9. Eight tests in `seam-tests/service-tier/tests/agent/test_fast_consumer_wire.py`:
   `test_foreground_real_loop_reaches_sdk_wire`,
   `test_foreground_route_replay_constructor_and_sdk[hello|example trigger-a.|example trigger-b.|example-action-a.|example-action-b.]`,
   `test_native_completion_admission_to_sdk`, `test_native_completion_with_required_route_policy`.
   They drive the gateway through `gateway/route_policy.py`, a separate "gateway route policy" seam that is NOT
   part of this series. They are expected to fail only while that module is missing; with it they run normally.

## 6. What the tests use

- `seam-tests/service-tier/tests/fixtures/` is a SYNTHETIC example tier policy (invented trigger words
  "example trigger-a." etc., an invented cron job "example-weekly-job", and invented scope names such as
  `worker_default` and `worker_adaptive`). It exists only to exercise patch 0001.
- `seam-tests/contracts/orchestrator/conftest.py` supplies offline quota data (fake account usage), so quota
  admission does not need a live account; the quota tests themselves replace that data.
- Fake API keys in the tests are literal placeholders such as `offline-fixture`; URLs with user names or query
  strings in `test_fast_consumer_execution.py` are deliberate rejection cases.
