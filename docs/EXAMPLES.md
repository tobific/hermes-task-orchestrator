# Examples

Two kinds of examples:

- **A. Tool calls** a parent model makes in a Hermes session with the plugin enabled. The plugin fills in the owner,
  session and profile itself; a model never supplies them. Values in `<angle brackets>` are placeholders for values
  from earlier replies.
- **B. Offline scripts** that drive the scheduler or a core seam directly, so you can see the behaviour without any
  account. They are demonstrations that **write files**: B1 and B3 create temporary folders (they are not removed
  afterwards), B2 creates a file in the folder you give it. The outputs shown are what the scripts printed.

## A. Tool calls

### A1. A small fan-out

```json
{"tool": "orchestration_create",
 "arguments": {"tasks": [
   {"task_id": "summary-a", "goal": "Summarize docs/a.md in five bullet points.", "timeout_seconds": 180},
   {"task_id": "summary-b", "goal": "Summarize docs/b.md in five bullet points.", "timeout_seconds": 180},
   {"task_id": "summary-c", "goal": "Summarize docs/c.md in five bullet points.", "timeout_seconds": 180}]}}
```

The reply contains a `run_id` and the state of each task. Then wait for all of them:

```json
{"tool": "orchestration_join", "arguments": {"run_id": "<run_id>", "timeout_seconds": 300}}
```

Set `timeout_seconds` on each task for real work: the default is 5 seconds per attempt.

### A2. A dependency chain

Dependencies set the **order**. They do not hand one task's answer to the next: each worker receives only its own
`goal`, `context` and `acceptance` text. So each step must be able to get the data it needs. There are two ways.

**Each step reads the source itself.** The goal says what to read; the dependency only makes sure the steps run in
this order:

```json
{"tool": "orchestration_create",
 "arguments": {"tasks": [
   {"task_id": "collect", "goal": "List every config file under conf/ with its size.", "timeout_seconds": 180},
   {"task_id": "analyse", "goal": "Read every config file under conf/ and point out settings that disagree between them.",
    "dependencies": ["collect"], "timeout_seconds": 180}]}}
```

`analyse` waits in state `BLOCKED` until `collect` succeeded, then becomes `PENDING`.

**The parent passes the earlier answer on.** Create the first step, wait for it, read its answer with
`orchestration_collect`, then add the next step to the same run with `orchestration_enqueue`, putting the answer into
its `context`:

```json
{"tool": "orchestration_create",
 "arguments": {"tasks": [{"task_id": "collect", "goal": "List every config file under conf/ with its size.",
                          "timeout_seconds": 180}]}}
{"tool": "orchestration_join", "arguments": {"run_id": "<run_id>", "timeout_seconds": 300}}
{"tool": "orchestration_collect", "arguments": {"run_id": "<run_id>"}}
{"tool": "orchestration_enqueue",
 "arguments": {"run_id": "<run_id>", "task_id": "analyse", "dependencies": ["collect"], "timeout_seconds": 180,
               "goal": "Point out settings that disagree between the files listed in the context.",
               "context": "Output of collect:\n<answer of the collect event>"}}
```

Script B3 below shows that the second worker really receives that context.

### A3. Collect later

Create a run, keep working, and fetch results whenever convenient:

```json
{"tool": "orchestration_collect", "arguments": {"run_id": "<run_id>", "limit": 2}}
```

The reply has `events` (at most 2 here) and `next_cursor`. Continue with **the `next_cursor` value from that reply**,
not with a number you compute yourself; a page can hold fewer events than `limit`:

```json
{"tool": "orchestration_collect", "arguments": {"run_id": "<run_id>", "cursor": "<next_cursor from the previous reply>"}}
```

(`cursor` is an integer; write the number without quotes.)

To wait only for the required tasks and leave optional work running:

```json
{"tool": "orchestration_join", "arguments": {"run_id": "<run_id>", "condition": "required", "timeout_seconds": 120}}
```

### A4. A scoped-permission task

Only the `src/` folder may be changed; the task cannot use the terminal:

```json
{"tool": "orchestration_create",
 "arguments": {"capability_profile": "workspace-write",
               "write_scope": ["/abs/path/project/src"],
               "tasks": [{"task_id": "fix-typos", "goal": "Fix spelling mistakes in comments under src/.",
                          "timeout_seconds": 300}]}}
```

The capability profile belongs to the whole run. A task may give a narrower `write_scope` (for example
`["/abs/path/project/src/docs"]`), but it keeps the run's profile. If other tasks must only read, put them in a
separate run with `"capability_profile": "read-only"`.

What the core seam does with such a task (output of the check on a patched Hermes, see B2 below):

```text
write inside src/   -> written
write in docs/      -> write target is outside the delegated scope: /abs/path/project/docs/notes.txt
terminal            -> adaptive terminal is disabled because no verified OS execution sandbox is installed
```

### A5. A read-only review task

Several read-only inspections plus a declared final review. These are the create options the read-only selector
suggests:

```json
{"tool": "orchestration_create",
 "arguments": {"mode": "joinable", "capability_profile": "read-only", "final_review_task_id": "final-review",
   "tasks": [
     {"task_id": "check-a", "goal": "Check report-a.md for unsupported claims.", "timeout_seconds": 180},
     {"task_id": "check-b", "goal": "Check report-b.md for unsupported claims.", "timeout_seconds": 180},
     {"task_id": "check-c", "goal": "Check report-c.md for unsupported claims.", "timeout_seconds": 180},
     {"task_id": "final-review", "goal": "Independently review all three checks against the request.",
      "dependencies": ["check-a", "check-b", "check-c"], "timeout_seconds": 180}]}}
```

Because this run declares `final_review_task_id`, the host builds the reviewer's prompt from the actual results, and
`orchestration_collect` returns nothing until the reviewer answered with the exact approval object
`{"verdict": "approve", "evidence_digest": "<host digest>", "rationale": "..."}`. Runs without
`final_review_task_id` (A1 to A4) have no such gate.

### A6. A quota refusal

When the account usage cannot be read, is stale, or a window is exhausted, `orchestration_create` returns:

```json
{"ok": false, "error": "OrchestrationError",
 "message": "Delegation quota guard blocked admission: quota exhausted. Explicit human check-in is required before retrying."}
```

No worker is started. Retry after checking the account.

### A7. Cancel

```json
{"tool": "orchestration_cancel", "arguments": {"run_id": "<run_id>"}}
{"tool": "orchestration_cancel", "arguments": {"run_id": "<run_id>", "task_id": "report"}}
{"tool": "orchestration_cancel", "arguments": {"owner_only": true}}
```

From a terminal, the operator CLI can do the same for one run; see section 22 of [FEATURES.md](FEATURES.md) for a
complete `hermes orchestration status` command.

## B. Offline scripts

**How to run them.** Use the Python of a Hermes environment (for example the test environment from
[TESTING.md](../TESTING.md), `$W/hermes-test-env/bin/python`), because the scripts import Hermes modules. Save each
script to a file, for example `b1.py`, and pass the arguments shown. `ORCH_PKG` is the path of this repository.
The commands use `python -B` and `PYTHONDONTWRITEBYTECODE=1` so that importing the plugin or Hermes does not write
`__pycache__` folders next to their source files.

### B1. The scheduler with a simulated worker

```sh
PYTHONDONTWRITEBYTECODE=1 "$W/hermes-test-env/bin/python" -B b1.py "$ORCH_PKG/plugin"
```

The argument is the folder that contains `external_orchestrator/`. Run as shown, the script writes only into new
temporary folders (it does not delete them). Without `-B`, Python may also write `__pycache__` folders next to the
imported source files.

```python
import json, os, sys, tempfile
os.environ["HERMES_HOME"] = tempfile.mkdtemp()          # throwaway home for this offline demo
sys.path.insert(0, sys.argv[1])                          # the folder that contains external_orchestrator/
from external_orchestrator.scheduler import ExternalScheduler, OrchestrationError

me = {"owner_token": "demo-owner", "parent_session_id": "demo-session", "profile": "demo-profile"}

def worker(packet):
    # offline stand-in for a model worker; the final reviewer approves with the host's evidence digest
    if packet["task_id"] == "final-review":
        verdict = {"verdict": "approve", "evidence_digest": packet["review_evidence_digest"],
                   "rationale": "both summaries answer the request"}
        out = s._simulated_worker(packet); out["answer"] = json.dumps(verdict); return out
    return s._simulated_worker(packet)

s = ExternalScheduler(tempfile.mkdtemp(), allow_simulated=True, quota_preflight=lambda p: None, worker=worker)

# 1. fan-out
r = s.create_run({**me, "tasks": [{"task_id": f"t{i}", "goal": f"summarize file {i}"} for i in (1, 2, 3)]})
j = s.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
print("1 fan-out:", j["state"], [(t["task_id"], t["state"]) for t in j["task_states"]])

# 2. dependency chain
r = s.create_run({**me, "tasks": [
    {"task_id": "collect", "goal": "list the files"},
    {"task_id": "analyse", "goal": "analyse the list", "dependencies": ["collect"]},
    {"task_id": "report", "goal": "write the report text", "dependencies": ["analyse"]}]})
j = s.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
print("2 chain:", j["state"], [(t["task_id"], t["state"]) for t in j["task_states"]])

# 3. collect later, page by page
page1 = s.collect({**me, "run_id": r["run_id"], "limit": 2})
page2 = s.collect({**me, "run_id": r["run_id"], "cursor": page1["next_cursor"]})
print("3 collect:", [(e["task_id"], e["answer"]) for e in page1["events"]], "next_cursor", page1["next_cursor"],
      "| then", [e["task_id"] for e in page2["events"]])

# 4. enforced final review
r = s.create_run({**me, "final_review_task_id": "final-review", "tasks": [
    {"task_id": "w1", "goal": "summarize part 1"}, {"task_id": "w2", "goal": "summarize part 2"},
    {"task_id": "final-review", "goal": "check both summaries", "dependencies": ["w1", "w2"]}]})
j = s.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
print("4 review approved:", j["state"], "events delivered:", len(s.collect({**me, "run_id": r["run_id"]})["events"]))

# 5. a review that does not approve blocks delivery
s2 = ExternalScheduler(tempfile.mkdtemp(), allow_simulated=True, quota_preflight=lambda p: None)
r = s2.create_run({**me, "final_review_task_id": "final-review", "tasks": [
    {"task_id": "w1", "goal": "summarize part 1"},
    {"task_id": "final-review", "goal": "check the summary", "dependencies": ["w1"]}]})
j = s2.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
print("5 review not approved:", j["state"], "events delivered:", len(s2.collect({**me, "run_id": r["run_id"]})["events"]))
s2.close()

# 6. refusals
for name, tasks in [("cycle", [{"task_id": "a", "goal": "a", "dependencies": ["b"]}, {"task_id": "b", "goal": "b", "dependencies": ["a"]}]),
                    ("override", [{"task_id": "a", "goal": "a", "route": {"model": "another-model"}}])]:
    try: s.create_run({**me, "tasks": tasks})
    except OrchestrationError as e: print("6", name, "refused:", e)
def no_quota(packet): raise RuntimeError("quota exhausted")
s3 = ExternalScheduler(tempfile.mkdtemp(), allow_simulated=True, quota_preflight=no_quota)
try: s3.create_run({**me, "tasks": [{"task_id": "q", "goal": "q"}]})
except OrchestrationError as e: print("6 quota refused:", e)
s3.close()

# 7. supersede and cancel
r = s.create_run({**me, "tasks": [{"task_id": "one", "goal": "first try"}]})
s.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
s.supersede({**me, "run_id": r["run_id"], "task_id": "one", "goal": "second try"})
s.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
print("7 supersede: delivered generations", sorted({e["generation"] for e in s.collect({**me, "run_id": r["run_id"]})["events"]}))
r = s.create_run({**me, "tasks": [{"task_id": "slow", "goal": "slow job", "sleep_seconds": 2, "timeout_seconds": 30}]})
c = s.cancel({**me, "run_id": r["run_id"]})
print("7 cancel:", c["state"], [(t["task_id"], t["state"]) for t in c["task_states"]])
s.close()
```

Output:

```text
1 fan-out: SUCCEEDED [('t1', 'SUCCEEDED'), ('t2', 'SUCCEEDED'), ('t3', 'SUCCEEDED')]
2 chain: SUCCEEDED [('analyse', 'SUCCEEDED'), ('collect', 'SUCCEEDED'), ('report', 'SUCCEEDED')]
3 collect: [('collect', 'SIMULATED: list the files'), ('analyse', 'SIMULATED: analyse the list')] next_cursor 2 | then ['report']
4 review approved: SUCCEEDED events delivered: 1
5 review not approved: FAILED events delivered: 0
6 cycle refused: dependency cycle
6 override refused: task cannot override host route
6 quota refused: Delegation quota guard blocked admission: quota exhausted. Explicit human check-in is required before retrying.
7 supersede: delivered generations [1]
7 cancel: CANCELLED [('slow', 'CANCELLED')]
```

Notes:

- Step 3 continues with the `next_cursor` the first page returned.
- In step 4 one event is delivered: the final delivery that follows the approval.
- In step 5 the simulated reviewer answers with plain text, which is not an approval, so the run fails and nothing
  is delivered.
- Step 7 shows that only the current generation (1) is delivered after a supersede.
- `allow_simulated=True` and `quota_preflight` are test settings. The plugin never uses them: it always runs real
  workers with the real quota check.

### B2. The permission check on a patched Hermes

This one needs a Hermes checkout at 8a3ede1b **with** the 8 patches (for example `$W/hermes-test` after block 2 of
[TESTING.md](../TESTING.md)) and an empty folder of your own that contains `src/` and `docs/`. Run it from the root
of that checkout, with the checkout first on the import path:

```sh
mkdir -p "$HOME/orch-demo/src" "$HOME/orch-demo/docs" &&
  cd "$W/hermes-test" &&
  PYTHONPATH="$PWD" PYTHONDONTWRITEBYTECODE=1 "$W/hermes-test-env/bin/python" -B /path/to/b2.py "$HOME/orch-demo"
```

```python
import json, os, sys, tempfile
os.environ["HERMES_HOME"] = tempfile.mkdtemp()
work = sys.argv[1]                                       # absolute path of a folder with src/ and docs/
from agent.delegation_context import delegated_child_context
from tools.file_tools import write_file_tool
from tools.terminal_tool import terminal_tool
with delegated_child_context(session_id="demo", run_id="run-demo", task_id="edit-src", generation=0,
                             write_owner_token="demo-token", write_scope=(work + "/src",),
                             capability_profile="workspace-write"):
    print("inside :", json.loads(write_file_tool(work + "/src/notes.txt", "hello\n")).get("error", "written"))
    print("outside:", json.loads(write_file_tool(work + "/docs/notes.txt", "hello\n")).get("error", "written"))
    print("terminal:", json.loads(terminal_tool("echo hi"))["error"])
with delegated_child_context(session_id="demo", run_id="run-demo", task_id="read-only", generation=0,
                             write_owner_token="", write_scope=(), capability_profile="read-only"):
    print("read-only write:", json.loads(write_file_tool(work + "/src/other.txt", "x\n")).get("error", "written"))
```

Output (paths shortened):

```text
inside : written
outside: write target is outside the delegated scope: <work>/docs/notes.txt
terminal: Failed to execute command: adaptive terminal is disabled because no verified OS execution sandbox is installed
read-only write: Tool 'write_file' is not authorized for 'read-only'. Do not retry or use GUI/browser/MCP alternatives. Return needs_parent_decision with completed work and the required parent-run action.
```

Run as shown, only `src/notes.txt` is created (and a temporary Hermes home); without `-B`, Python may also write
`__pycache__` folders inside the checkout. The terminal refusal is also logged by Hermes, with a
traceback, on standard error. In the plugin, the same context is set by the host for each worker; the model cannot
set it.

### B3. Passing an earlier answer on with `enqueue`

This shows the second way of A2: the parent reads the first answer and gives it to the next task as `context`. The
worker records what it received. Run it like B1:

```sh
PYTHONDONTWRITEBYTECODE=1 "$W/hermes-test-env/bin/python" -B b3.py "$ORCH_PKG/plugin"
```

```python
import os, sys, tempfile
os.environ["HERMES_HOME"] = tempfile.mkdtemp()          # throwaway home for this offline demo
sys.path.insert(0, sys.argv[1])                          # the folder that contains external_orchestrator/
from external_orchestrator.scheduler import ExternalScheduler

me = {"owner_token": "demo-owner", "parent_session_id": "demo-session", "profile": "demo-profile"}
received = []

def worker(packet):
    received.append((packet["task_id"], packet.get("context", "")))
    return s._simulated_worker(packet)

s = ExternalScheduler(tempfile.mkdtemp(), allow_simulated=True, quota_preflight=lambda p: None, worker=worker)
r = s.create_run({**me, "tasks": [{"task_id": "collect", "goal": "list the files"}]})
s.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
answer = s.collect({**me, "run_id": r["run_id"]})["events"][0]["answer"]
s.enqueue({**me, "run_id": r["run_id"], "task_id": "analyse", "goal": "analyse the list",
           "context": "Output of collect:\n" + answer, "dependencies": ["collect"]})
j = s.join({**me, "run_id": r["run_id"], "timeout_seconds": 10})
print(j["state"], [(t["task_id"], t["state"]) for t in j["task_states"]])
for task_id, context in received:
    print(task_id, "received context:", repr(context))
s.close()
```

Output:

```text
SUCCEEDED [('analyse', 'SUCCEEDED'), ('collect', 'SUCCEEDED')]
collect received context: ''
analyse received context: 'Output of collect:\nSIMULATED: list the files'
```

The first task received no context; the second received exactly what the parent put into `context`. Without that
step it would have received nothing from `collect`.

The test suite proves the same behaviour in more depth: see the file and test names in [TESTING.md](../TESTING.md).
