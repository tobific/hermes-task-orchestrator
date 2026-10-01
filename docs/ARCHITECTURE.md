# Architecture

## Overview

```text
  user message
       |
       v
 +--------------------------- Hermes process (one profile) -----------------------------+
 |                                                                                      |
 |  parent agent (openai-codex session)                                                 |
 |    |  pre_llm_call hook: read-only selector (suggests a run; never starts work)       |
 |    |                                                                                 |
 |    |  tool calls: orchestration_create / enqueue / supersede / join / collect /      |
 |    |              cancel / status / history                                          |
 |    v                                                                                 |
 |  plugin handler  -- adds owner, session, profile from the live host context          |
 |    |                                                                                 |
 |    v                                                                                 |
 |  scheduler (one per profile) <--> state file (owner-only folder, file lock, bounded) |
 |    |   admission: host route + limits, spawn pause, quota gate, capacity slots       |
 |    |   queue: dependencies, priorities + aging, turn sharing, deadlines, retries     |
 |    v                                                                                 |
 |  host dispatch / worker adapter                                                      |
 |    |   task authority: capability profile + write scope + parent tool ceiling        |
 |    |   builds a native child agent on the configured worker route                    |
 |    v                                                                                 |
 |  native child agent  ---- tool calls ---->  [seam 0002] required tool policy         |
 |    |                                         [seam 0007] check at the file effect,   |
 |    |                                                     terminal refused            |
 |    |---- provider request -----------------> [seam 0004] request observation         |
 |    v                                                                                 |
 |  result  ->  validation (route proof, structured claims, size)  ->  delivery event   |
 |                                                            |                         |
 |              final review gate (only if declared) <--------+                         |
 |                                                            |                         |
 |  join / collect  <-----------------------------------------+                         |
 |  detached delivery ---> [seam 0003] completion fence ---> native background delivery |
 |                                                                                      |
 |  [seam 0005] parent hard-close -> revoke workers                                     |
 |  [seam 0006] /agents lines (optional)      [seam 0001] tier policy (if configured)   |
 |  [seam 0008] cron tier / no fallback; cron memory isolation (always, once installed) |
 +--------------------------------------------------------------------------------------+
     capacity slots (file locks): max_workers per profile + one host pool of 16
```

## Data flow of one run

1. **Create.** The model calls `orchestration_create`. The plugin handler refuses the call unless there is a live
   parent with a session; new work also needs an `openai-codex` parent. It adds the owner token (a SHA-256 of
   profile and session), the session and the profile. Model-supplied values for these are not trusted.
2. **Validate.** The scheduler checks sizes, ids, the dependency graph (no cycles, no unknown ids), the final review
   declaration if there is one, and the per-task limits. Tasks may not override the host's owner, profile,
   capability or route.
3. **Admit.** Host controls are checked (scheduler enabled, spawn pause, `allow_xhigh`), then the quota gate reads
   the account usage outside the state lock, then the controls are checked again. The run is written to the state
   file in one locked transaction.
4. **Schedule.** Whenever the scheduler runs its admission step (after create, enqueue, supersede, during join, at a
   status call with a run id, at collect, and when a worker finishes), it settles finished work, checks timeouts and
   deadlines, and picks pending tasks under the limits: this profile's `max_workers`, a free per-profile slot and a
   free host slot (16 for the installation), no two tasks with overlapping write scopes, turn sharing between runs
   and profiles inside this scheduler, weighted priorities with aging. A scheduler without a worker (the operator
   CLI) skips this step.
5. **Run.** For each task the host builds a task authority (capability profile, write scope, a write token derived
   from the host owner and the task identity, the parent's tool ceiling) and starts a native child agent on the
   configured worker route inside that authority's scope. The worker receives its task's goal, context and
   acceptance text, not the answers of earlier tasks. The authority is enforced at tool dispatch (0002) and at the
   effect (0007). The provider request is observed (0004).
6. **Validate the result.** The observed route must match the configured route. Structured claims (artifacts,
   checks) must be host-verifiable; plain prose is not scanned. The result is bounded.
7. **Deliver.** A delivery event is appended with a cursor. Dependent tasks are released. Only if the run declared a
   final review: the host builds the reviewer's evidence from the actual results; delivery waits for a strict
   approval that names the evidence digest, and results are re-checked just before delivery.
8. **Read.** `orchestration_join` waits for a condition; `orchestration_collect` pages through events; detached runs
   use the native background delivery through the completion fence.
9. **Stop.** Cancel, parent hard-close, turn end (optional tasks) and the CLI session boundary (joinable runs) revoke
   or cancel work. A revoked worker may finish an effect that was already admitted; its slot is held until it exits.

## Contracts

The invariants are listed in [DESIGN-CONTRACTS.md](DESIGN-CONTRACTS.md) and checked by the seam tests. In short:

- Model-written task packets describe work; they never supply authority, route, capacity, quota or policy.
- One capability profile per run; a task's write scope can only narrow the run's.
- Superseded generations are not delivered.
- Quota admission refuses when the reading is missing, stale or exhausted.
- A declared final review gates every delivery of its run; runs without one are not gated.
- A worker result is accepted only with an observed route that matches the configuration.
- Status, history and diagnostics are visible only to the owning session. History, the owner-wide status list and
  the gateway lines only read; a status call with a run id and `collect` may also let the live scheduler admit
  queued work.
- Seven seams leave Hermes unchanged when nothing registers for them; patch 0008 changes cron memory behaviour as
  soon as it is installed.

## Files of the plugin

| File | Role |
|---|---|
| `__init__.py` | registration: tools, hooks, CLI command; handler that binds owner, session and profile; one scheduler per profile |
| `scheduler.py` | runs, tasks, queue, admission, generations, review gate, delivery events, persistence |
| `capacity.py` | capacity slots (file locks: per profile and one host pool of 16), host limits from `config.yaml` |
| `admission_policy.py`, `operational_controls.py` | host controls: enabled, spawn pause, `allow_xhigh`, detached delivery |
| `quota_gate.py` | account-usage check before admission |
| `host_binding.py`, `native_worker.py` | binding to the live parent; building and measuring the native child agent |
| `task_authority.py` | per-task capability, write scope, write token, parent tool ceiling |
| `observation.py`, `host_usage.py`, `tier_authority.py` | route proof from the observed request; token counters; tier binding |
| `claim_validation.py` | host checks for structured artifact and check claims |
| `async_delivery.py` | detached runs through native background delivery and the completion fence |
| `optional_finalization.py`, `boundary_finalization.py`, `owner_operations.py` | turn-end and session-boundary cleanup; owner-wide status and cancel |
| `automatic_selector.py` | the read-only selector hook |
| `history.py`, `diagnostic_store.py`, `transcript.py` | bounded history, diagnostics and transcript excerpts |
| `gateway_extensions.py`, `cron_effort.py` | optional `/agents` lines and cron effort field |
| `storage.py`, `scope_identity.py` | owner-only storage; path identity for scope overlap |
| `planner.py`, `process_worker.py`, `process_watchdog.py`, `process_drain.py` | helpers for grouping and supervised host processes, covered by the unit tests; the tool path above does not start host processes |

## Glossary

| Term | Meaning |
|---|---|
| **run** | a stored set of tasks created by one `orchestration_create` call |
| **task** (packet) | one unit of work with a goal; the stored record of it is the packet |
| **dependency** | "start this task only after those succeeded"; it orders tasks and passes no data |
| **generation** | the version of a task; a supersede creates generation n+1 and makes older results undeliverable |
| **attempt** | one execution of a generation; retries add attempts |
| **owner** | the profile and session that created a run; only the owner can read or change it |
| **capability profile** | the named set of tools the tasks of a run may use (for example `read-only`, `workspace-write`) |
| **write scope** | the folders a `workspace-write` task may change |
| **seam** | an extension point in Hermes core that the plugin uses (one patch each) |
| **joinable / detached** | results read with join and collect, or delivered through native background delivery |
| **final review** | an optional declared task whose strict approval is required before any delivery of its run |
| **route proof** | the observed provider, model, effort and tier of a worker's actual request |
| **completion fence** | the check that a background completion is still valid before it is delivered |
| **quota gate** | the account-usage check before admission |
| **selector** | the hook that suggests an orchestration run for independent read-only work |
| **slot** | a file lock that a running worker holds; per profile and in the installation-wide host pool |
