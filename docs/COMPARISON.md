# Native `delegate_task` and this orchestration, side by side

Hermes' built-in `delegate_task` and this plugin solve related but different problems. Both can be installed
together; the plugin does not replace or change `delegate_task`. The native column describes Hermes upstream
8a3ede1b.

| Topic | Native `delegate_task` | This orchestration |
|---|---|---|
| Installation | built in | plugin plus 8 core patches |
| Unit of work | one call with one goal or a `tasks` array | a stored *run* of 1 to 64 tasks |
| Parallel width | up to `delegation.max_concurrent_children` (default 10) per call; larger batches are refused | `delegation.scheduler.max_workers` (1 to 16, default 3) per profile; all profiles of the installation together at most 16 |
| Ordering | none: tasks in one call are independent | dependencies between tasks (order only; answers are not passed on automatically), cycles refused |
| Priorities | none | `critical` / `support` / `background`, with aging; turns shared between runs inside one profile's scheduler |
| Getting results | top-level calls run in the background; the batch result arrives later as one message | `orchestration_join` (all, required, first, named tasks) and `orchestration_collect` with a cursor; or `detached` delivery |
| Live control | `action`: `list` running children, `steer` a child with a correction, `stop` one child | cancel a task or a run, supersede a task with a new generation, status with transcript excerpts |
| Retries | none by the tool | `max_attempts` (1 to 5) after host-side failures and timeouts |
| Timeouts | no configured cap by default (`child_timeout_seconds: 0`); a stale-child monitor ends a child after about 450 s without progress between turns or about 1200 s in one tool | per-attempt `timeout_seconds` (default 5, up to 600) and a queue `deadline_seconds` |
| Model of the helpers | `delegation.model` / `provider`, or inherit the parent | fixed worker route in `delegation.worker` (in this build `openai-codex` / `gpt-6-luna`) |
| Proof of the model used | not recorded | the actual provider request is observed and must match the route |
| Permissions | children get the parent's toolsets minus blocked tools, and native approvals (`subagent_auto_approve: false` denies dangerous commands) | one capability profile per run, a write scope, checks at every dispatch and at the file effect; terminal refused |
| Quota | not checked | account usage read before admission; refused when missing, stale or exhausted |
| Final review | ask for it in the prompt | optional; when a run declares one, nothing is delivered without a strict, evidence-bound approval |
| Memory and context files in helpers | not loaded (children start with `skip_memory` and `skip_context_files`) | not loaded (workers are native child agents) |
| Persistence | background results survive a restart through native durable delivery | runs and results are stored on disk; the completion fence re-checks replayed results |
| Providers | any | `openai-codex` only |
| Platforms | all Hermes platforms | Linux and macOS |

## What each is good for

**Use `delegate_task` when** you want a few independent helpers right now, on any provider, with the least setup:
quick research in parallel, a handful of independent file reads, a one-off batch. You can still list, steer or stop
the helpers while they run. It is simple, well tested and part of Hermes.

**Consider this orchestration when** the job has structure or needs extra checks: steps that must run in order,
results you want to pick up later or page through, a run that may write next to runs that must only read, a stop
when the account allowance cannot be confirmed, or a declared review that must approve the actual results before
anything is delivered.

**Limits of this comparison.** The orchestration figures come from the code and from offline tests with simulated
workers; it has not been measured against live accounts, rate limits, or later Hermes releases.
