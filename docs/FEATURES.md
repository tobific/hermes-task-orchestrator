# Features

Every feature the plugin and the core seams provide. Each entry says what it does, why it helps, how to use it and
its limits. Configuration keys are explained in [CONFIGURATION.md](CONFIGURATION.md); runnable examples are in
[EXAMPLES.md](EXAMPLES.md).

The tools are registered in the `external_orchestration` toolset:
`orchestration_create`, `orchestration_enqueue`, `orchestration_supersede`, `orchestration_join`,
`orchestration_collect`, `orchestration_cancel`, `orchestration_status`, `orchestration_history`.

---

## 1. Runs and tasks

**What it does.** `orchestration_create` creates a *run*: a set of 1 to 64 tasks. Each task has a `goal` and
optional `context` and `acceptance` text (each up to 16,000 characters), an optional `task_id`, `priority`,
`dependencies`, `required` flag and limits. The run is stored on disk and survives a restart of the Hermes process.

**Why it helps.** A larger job becomes one tracked object you can wait for, page through, cancel or inspect,
instead of separate helper calls.

**How to use it.** Call `orchestration_create` with `tasks: [...]`. `orchestration_enqueue` adds one task to an
existing run.

**Limits.** At most 64 tasks per run and 128 queued tasks in total (both lower if configured). Task ids must be
unique across runs. A task cannot set its own owner, session, profile, capability or route: those come from the host
("task cannot override host route").

## 2. Dependencies

**What it does.** A task may list `dependencies` (task ids in the same run). It waits in state `BLOCKED` until all of
them succeeded, then becomes `PENDING`. If a dependency fails, is cancelled or times out, the dependent task stays
blocked with status `dependency_failed`.

**Why it helps.** "Collect, then analyse, then report" runs in order without the parent model babysitting it.

**How to use it.** `{"task_id": "analyse", "goal": "...", "dependencies": ["collect"]}`.

**Limits.** Dependencies set the **order only**. A worker receives its own task's `goal`, `context` and `acceptance`
text; the answer of an earlier task is **not** passed to the next task automatically. A later task can read the same
source data itself (its goal says what to read), or the parent can collect the earlier answer and add it to the next
task's `context` with `orchestration_enqueue` (shown in [EXAMPLES.md](EXAMPLES.md), A2 and B3). Dependencies must be
in the same run; unknown ids and cycles are refused when the run is created ("dependency cycle"). There is no
conditional branching.

## 3. Priorities, aging and fairness

**What it does.** Each task has a priority `critical`, `support` (default) or `background`, weighted 4, 2 and 1.
Waiting tasks age: a `background` task is treated as `support` after 60 seconds and as `critical` after 300 seconds.
When one scheduler picks the next task, it alternates between the profiles that have ready work in its own state and,
within a profile, shares turns between runs by weighted deficit round robin.

**Why it helps.** Urgent work goes first, and one large run does not hold back the other runs of the same scheduler.

**Limits.** The fairness bookkeeping lives inside one scheduler's state. Each Hermes profile normally has its own
scheduler and store (see 4), so fairness **between** profiles or Hermes processes is not established by this code.
There is no guarantee of progress: a spawn pause, an unavailable quota reading, a failed dependency or a capacity slot
still held by a worker that has not exited can keep a task from starting. A queued task whose `deadline_seconds`
passes ends with `deadline_expired_while_queued` when the scheduler next checks deadlines. Only the
`weighted_deficit_round_robin` fairness policy exists.

## 4. Capacity and caps

**What it does.** Each Hermes profile (each Hermes home) gets its own scheduler and its own state store. The host
setting `delegation.scheduler.max_workers` (1 to 16, default 3) limits how many workers that scheduler runs at once,
and how many slots that profile may hold in a small file-lock slot pool. Slots are kept as file locks under the
default Hermes root, so they also count across Hermes processes:

- **per-profile slots**: `max_workers` slots per profile, shared by every process that uses that profile;
- **host slots**: one pool of **16** slots shared by all profiles of the installation. This number is fixed in the
  code; `max_workers` does not change it.

So two different profiles each configured with `max_workers: 3` can together run 6 workers. The installation-wide
total is at most 16. A slot held by a process that died is reclaimed; a slot of a live process is kept. Two tasks
whose write scopes overlap are not admitted at the same time (checked against the running tasks of the scheduler and
against the scopes published in the shared slots).

**Why it helps.** Predictable load per profile and an upper bound for the whole installation, also with several
sessions or processes.

**Limits.** There is no configurable installation-wide cap below 16; to limit several profiles together, lower each
profile's `max_workers`. POSIX file locks only (Linux, macOS). This was read from the code and tested with unit tests;
concurrent load across several profiles was not measured.

## 5. Join

**What it does.** `orchestration_join` waits up to `timeout_seconds` (0 to 600, default 30) for a condition:

- `all` (default): the whole run is finished;
- `required`: every task marked `required` (the default) is settled; optional work keeps running;
- `first`: any task has settled;
- `task_ids`: the listed tasks have settled.

It returns the run view (state of every task) and `join_timed_out: true` when the wait ended first. While waiting it
keeps the parent's activity heartbeat alive and stops when the parent is interrupted.

**Why it helps.** The parent can wait exactly as long as it needs, and for exactly what it needs.

**Limits.** "Partial readiness never authorizes final delivery": a `required` or `first` join does not approve or
deliver anything by itself.

## 6. Collect later

**What it does.** `orchestration_collect` returns result *events* for a run, after a `cursor`, at most `limit`
(1 to 50, default 20) per call, with `next_cursor`. Each event carries the task id, generation, state, the bounded
answer, evidence, artifacts, checks, uncertainties and suggested follow-ups.

**Why it helps.** Create a run, keep working, and pick up results whenever convenient, page by page, without
re-reading old ones.

**How to use it.** Always continue with the `next_cursor` the previous reply returned. A page can hold fewer events
than `limit`, because events of superseded generations (and, with a final review, events before the approved final
delivery) are skipped while the cursor still moves past them.

**Limits.** Results are bounded (default 14,000 characters per result). When the run declares a final review task,
`collect` returns nothing until the review approved (see 12). Like status, `collect` also lets the scheduler do its
regular work first (see 17).

## 7. Detached mode

**What it does.** `orchestration_create` with `mode: "detached"` hands the run to Hermes' native background
completion delivery: the result arrives later as a normal completion message, like a background `delegate_task`.
The completion fence (patch 0003) re-checks it before delivery, also after a restart.

**How to use it.** `"mode": "detached"`. It can be switched off on the host with
`delegation.compatibility.detached_completion_delivery: false`.

**Limits.** Needs patch 0003. The default is `joinable`.

## 8. Required and optional tasks

**What it does.** Tasks are required unless `"required": false`. The run succeeds when all required tasks succeed;
an optional task that fails does not fail the run.

**Why it helps.** Nice-to-have work (an extra check, a second opinion) can run without risking the main result.

**Limits.** An optional task bound to the current turn may be cancelled automatically when that turn completes
(see 16).

## 9. Timeouts, deadlines and retries

**What it does.** Per task:

- `timeout_seconds` (above 0, up to 600, **default 5**): running time per attempt. When it is exceeded the attempt is
  marked as timed out and retried while attempts remain, otherwise the task ends `TIMED_OUT`;
- `max_attempts` (1 to 5, default 2);
- `deadline_seconds` (1 to 86,400, default 300): a queue deadline; a task still waiting when it passes ends with
  `deadline_expired_while_queued`.

Retries happen only for host-side failures and timeouts, never because a worker claims it should be retried.

**How to use it.** Set `timeout_seconds` for real work; the 5-second default suits tests. The read-only selector
suggests 180 seconds.

**Limits.** A timed-out worker thread is not killed: the task is settled, but its capacity slot stays reserved until
the worker really exits, and a late result is ignored.

## 10. Supersede (generations)

**What it does.** `orchestration_supersede` replaces a task with a new *generation* (new goal or limits). The
replacement starts with no inherited result, and the old generation's result is not delivered, even if it arrives
late. Up to 16 generations per task.

**Why it helps.** Correct a task mid-run without confusion about which answer is current.

**Limits.** Superseding a task that a final review depends on invalidates that review; the review runs again on the
new evidence.

## 11. Scoped permissions (one capability profile per run)

**What it does.** Each run has one capability profile, and a `workspace-write` run a list of folders
(`write_scope`). The profiles come from the core seam (patch 0007):

| Profile | Tools a task may use |
|---|---|
| `read-only`, `code-read`, `code-read-test` | `read_file`, `search_files` |
| `workspace-write` | `read_file`, `search_files`, `write_file`, `patch` (inside `write_scope` only) |
| `web-read` | `web_search`, `web_extract` |
| `computer-use` | `computer_use` |
| `none` | no tools |

The check runs at every tool dispatch (patch 0002) and again at the actual file effect (patch 0007): a write outside
the scope is refused even if a tool was allowed. `terminal` and `execute_code` are refused for these tasks. A task
also cannot use a tool its parent could not use.

**Why it helps.** "This run may edit `src/`, that one may only read" is enforced in code, not only asked for in the
prompt.

**How to use it.** `"capability_profile": "workspace-write", "write_scope": ["/abs/path/src"]` on
`orchestration_create`. Without it, runs are `read-only`. A task in a `workspace-write` run may give its own,
narrower `write_scope` (for example one sub-folder).

**Limits.** The profile belongs to the run: narrowing a task's `write_scope` makes its writable folders smaller but
does **not** change its capability profile. For work that needs different profiles (one task that writes, others
that only read with a read-only profile), create separate runs. This is cooperative enforcement inside the Hermes
process, **not an operating-system sandbox**; see [SECURITY.md](SECURITY.md). `code-read-test` currently grants the
same tools as `read-only` (no test runner is admitted, because terminal use is refused).

## 12. Final review (optional)

**What it does.** A run **may** name `final_review_task_id`; this is optional. Without it, results are delivered as
the tasks finish, with no review gate. With it: the review task must be required and must depend on every other
task, and only on them. When the work tasks have succeeded, the host builds the reviewer's prompt itself: the
original request plus the actual results as JSON data, and a SHA-256 digest of that evidence. The run succeeds only
if the reviewer answers with exactly one JSON object `{"verdict": "approve", "evidence_digest": "<the digest>",
"rationale": "<text>"}`. Anything else (prose, a missing key, another digest, `block`) fails the run, and `collect`
then delivers nothing. Results are re-checked right before delivery; an artifact changed after the review blocks
delivery.

**Why it helps.** When you declare a review, nothing reaches the user before a check of the real results, and a
reviewer cannot "approve" with vague text or approve different evidence.

**Limits.** Only runs that declare a final review are gated. The reviewer is a model; the gate checks the form and
the evidence binding of the approval, not its quality. The reviewer prompt is bounded (16,000 characters); larger
evidence fails the review with `REVIEW_EVIDENCE_TOO_LARGE`.

## 13. Structured results and claim checks

**What it does.** A worker's summary is kept as opaque text. **Only** if the whole summary is one JSON object with the
keys `answer`, `evidence`, `artifacts`, `checks`, `uncertainties`, `suggested_followups`, the host validates the
structured claims in it: an artifact must be a regular local file inside the task's write scope (no URLs, no symlinks
out of it), and a check reported as passed counts only if a host verifier confirmed it. Failed or forged structured
claims fail the task.

**Why it helps.** A worker that reports in the structured format cannot list a file as written or a check as passed
unless the host can confirm it.

**Limits.** Plain prose summaries are not scanned; a sentence such as "I wrote the file and the tests pass" is
delivered as text and is not verified.

## 14. Quota admission

**What it does.** Before new work is admitted, the plugin reads the account usage for `openai-codex` through Hermes'
own account-usage API (`agent.account_usage.fetch_account_usage`) and refuses when the reading is missing, from
another provider or profile, older than 120 seconds, malformed, or any usage window is at 100 percent. The read is
limited to 10 seconds. The refusal says: "Delegation quota guard blocked admission: <reason>. Explicit human check-in
is required before retrying."

**Why it helps.** No work is started that is likely to fail for lack of allowance, and nothing silently retries
against an exhausted account.

**How to use it.** On by default (`quota_guard_mode: enforce`). `observe` skips the check; any other value means
`enforce`.

**Limits.** It refuses only at an exhausted window (100 percent); there is no configurable threshold. It depends on
what the provider reports.

## 15. Host admission controls

**What it does.** Before work starts the plugin also honours host controls: `delegation.scheduler.enabled: false`
refuses new work; Hermes' global spawn pause (the same pause native delegation uses) refuses new work;
`delegation.policy.allow_xhigh: false` refuses work whose effort would be `xhigh`. These checks repeat just before a
queued task starts, so a pause also holds work that is already queued.

## 16. Stop, cancel and cleanup

**What it does.**

- `orchestration_cancel` cancels one task (`task_id`) or every active task of a run; with `owner_only: true` and no
  run id, every run of the calling session.
- When the parent agent is hard-closed, its workers' permissions are revoked (patch 0005).
- At the end of a completed turn, optional tasks bound to that turn are cancelled
  (`delegation.policy.auto_cancel_optional_on_finalize`, default true).
- At a CLI session boundary, unfinished `joinable` runs of that session are cancelled. Detached and finished runs are
  left alone.

**Limits.** A cancelled worker is revoked, not killed: it can finish an effect that was already admitted, and its
capacity slot is held until it exits.

## 17. Status, history and transcripts

**What it does.**

- `orchestration_status` with a `run_id` shows the run with the state, generation, attempt and host status of each
  task. With `inspect_transcript: true` it adds a bounded excerpt of each worker's live transcript (up to 65,536
  characters, with an offset).
- `orchestration_status` with `owner_only: true` lists all runs of the calling session, page by page.
- `orchestration_history` reads retained result records, including older generations, page by page (up to 50).

**Side effects.** A status call with a `run_id`, and `orchestration_collect`, first let the scheduler do its regular
work in a live Hermes session: it settles finished workers, checks timeouts and deadlines, and may **start queued
tasks** if capacity is free. They are not pure reads. The diagnostic views do not do this: `orchestration_history`,
the `owner_only` status list and the gateway `/agents` lines only read the stored state.

**Why it helps.** You can see what is happening and why.

**Limits.** Only the owning session can read its runs. History is diagnostic: it never authorizes delivery or a
review.

## 18. Worker binding and route proof

**What it does.** Workers run as native Hermes child agents on the host-configured route. With patch 0004 the host
observes the actual provider request of each worker call and accepts a result only if the provider, model, effort
and service tier really used match the configured route. A worker's own statement about its model is not trusted.

**Why it helps.** You know which model actually produced each result.

**Limits.** Implemented for the `openai-codex` Codex Responses path only.

## 19. Read-only selector

**What it does.** A `pre_llm_call` hook looks at each user message of a top-level `openai-codex` / `gpt-6-astra`
session. If the message asks for at least 3 (at most 16) independent, read-only inspections ("review each of these
files independently: ...") and contains no request to change anything, it adds a short instruction to the turn
suggesting one `orchestration_create` run with a final review. It never starts work itself. Phrases such as "do not
delegate" switch it off for that message.

**Limits.** English wording only; fixed parent model; it is only a suggestion the model may ignore. It stays off
unless `delegation.routing_policy.luna_policy_mode` is `observe` (the default) and `delegation.scheduler.enabled` is
`true`.

## 20. Gateway views (optional)

**What it does.** With `gateway_extensions: true` in the plugin settings and patch 0006, the gateway's `/agents`
command appends up to 10 owned runs with up to 10 tasks each, including the observed effort and tier. These lines
only read the stored state.

**Limits.** Patch 0006 also declares a second hook, `gateway_busy_control`, and the plugin registers a handler for
it, but no code in the patched Hermes calls that hook yet, so it has no effect in this build.

## 21. Cron: per-job tier, no fallback, memory isolation

**What it does.** Patch 0008 adds per-job `service_tier` (`normal` or `priority`) and `allow_fallbacks` (`false`
switches off every fallback chain for that job; `true` or unset keeps Hermes' normal fallback behaviour).

**Memory isolation applies whenever patch 0008 is installed**, with or without this plugin and without any setting:
cron agents then run without memory (`skip_memory`) and without the `memory` toolset.

Optionally, with `cron_reasoning_effort_tool: true` in the plugin settings, the `cronjob` tool schema gains an
optional `reasoning_effort` field, passed on to Hermes' own cron validation. This plugin setting is not the switch for
the memory isolation.

## 22. Operator CLI

**What it does.** `hermes orchestration status`, `hermes orchestration collect` and `hermes orchestration cancel`
read or cancel a run from a terminal. The CLI has no worker: it **never starts workers**, also not for queued tasks.
It reads the same store as the plugin (`EXTERNAL_ORCHESTRATOR_DATA`, or `$HERMES_HOME/plugin-data/external-orchestrator`).

**How to use it.** You need the run id, the session id of the Hermes session that created the run, and the profile
(the physical path of the Hermes home). The owner token is the SHA-256 of profile, a zero byte and session id:

```sh
PROFILE=$(cd -P -- "${HERMES_HOME:-$HOME/.hermes}" && pwd -P) &&
  SESSION="<session id of the creating session>" &&
  TOKEN=$(printf '%s\0%s' "$PROFILE" "$SESSION" | shasum -a 256 | cut -d' ' -f1) &&
  hermes orchestration status --run-id "<run_id>" --owner-token "$TOKEN" --parent-session-id "$SESSION" --profile "$PROFILE"
```

Replace `status` by `collect` (optionally with `--cursor N`) or `cancel` (optionally with `--task-id ID`). On Linux,
use `sha256sum` instead of `shasum -a 256`.

## 23. Storage

Runs are kept in `$HERMES_HOME/plugin-data/external-orchestrator` (or `EXTERNAL_ORCHESTRATOR_DATA`), in folders
with owner-only permissions (0700, files 0600), with file locking for every change. The state file is bounded
(32 MiB). Capacity slots are kept separately under `plugin-data/external-orchestrator-capacity` in the default Hermes
root.
