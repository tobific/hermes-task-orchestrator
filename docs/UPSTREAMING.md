# Upstreaming: proposals for the Hermes maintainers

This page is written for the Hermes maintainers. The plugin depends on 8 core seams. Below is one proposal card per
seam, ordered from the easiest to review to the largest. Seven of them leave Hermes unchanged when nothing registers
for them or no new setting is used. **Patch 0008 is the exception:** once installed, it changes cron jobs with or
without this plugin (cron agents run without memory and without the memory toolset); see card 5.

The seams differ a lot in size. Cards 1 to 4 are small, self-contained hooks. Cards 5 and 6 change behaviour that
needs a decision on defaults. **Cards 7 and 8 (patches 0003 and 0007, 546 / 61 and 960 / 4 lines) are not small
hooks** as they are packaged here; each should be split into smaller pull requests before review (suggestions in
the cards).

You are welcome to reuse and adapt this under MIT, retaining its required copyright and permission notices. No
additional acknowledgement is requested.

## If you read only one thing

We would open **one** public feature request, focused on the first small lifecycle and status hooks: **card 1**
(parent hard-close callbacks) and **card 2** (a plugin hook for the gateway `/agents` view). Both are small, change
nothing when unused, and are useful to any plugin that runs background work. Everything else on this page is
optional background for later, if the first hooks fit.

## Summary

| Order | Seam | Patch | Size (lines added / removed) | Files in Hermes |
|---|---|---|---|---|
| 1 | Parent hard-close callbacks | 0005 | 33 / 0 | `run_agent.py` |
| 2 | Gateway `/agents` plugin hook | 0006 | 44 / 3 | `gateway/slash_commands_status.py`, `hermes_cli/plugins.py` |
| 3 | Opt-in request observation for Codex streams | 0004 | 321 / 1 | `agent/codex_runtime.py` |
| 4 | Required tool policy that survives plugin unload | 0002 | 149 / 1 | `agent/required_tool_policy.py` (new), `hermes_cli/plugins.py`, `hermes_cli/plugins_dispatch.py`, `model_tools.py`, `tools/registry.py` |
| 5 | Cron per-job tier, no-fallback and memory isolation | 0008 | 174 / 42 | `cron/jobs.py`, `cron/scheduler.py`, `cron/scheduler_agent.py` (new), `tools/cronjob_tools.py` |
| 6 | Owner-bound service-tier policy | 0001 | 268 / 2 | `agent/service_tier_policy.py` (new), `hermes_cli/plugins_service_tier.py` (new), `agent/fast_mode.py`, `agent/turn_api_call.py`, `agent/background_review.py`, `tools/delegate_tool.py`, `hermes_cli/plugins.py` |
| 7 | Trusted completion fence for background delegation | 0003 | 546 / 61 (**not small; split**) | `tools/async_delegation.py`, `gateway/run_notifications.py` |
| 8 | Per-task capability authority at the effect | 0007 | 960 / 4 (**not small; split**) | `agent/delegation_context.py`, `tools/delegation_contracts.py` (new), `tools/file_tools.py`, `tools/terminal_tool.py`, `tools/code_execution_tool.py`, `tools/environments/local.py`, `tools/process_registry.py` |

Sizes are from the patch headers against upstream 8a3ede1b.

---

## 1. Parent hard-close callbacks (patch 0005)

**Problem.** Code that starts work on behalf of an agent (helpers, background jobs, plugins) cannot learn that the
agent was hard-closed, so it cannot stop that work at the right moment.

**Minimal core change.** In `run_agent.py`, class `AIAgent`: a read-only property `is_closed`, a method
`register_close_callback(callback)` and an internal `_notify_hard_close()` called at the start of the hard close.
Each callback runs once, outside the lock, before resource cleanup; a registration after close runs immediately;
exceptions are swallowed and never block cleanup.

**Tests.** No dedicated test file in this repository. Suggested tests: one callback runs exactly once; a late
registration runs immediately; a raising callback does not stop the close.

**Risks and compatibility.** Nothing changes for agents with no callbacks. Callbacks must be short; a slow callback
delays the close.

**Benefit without this plugin.** Any plugin or integration that owns resources per agent (a browser, a subprocess, a
remote session) gets a clean, documented release point.

## 2. Gateway `/agents` plugin hook (patch 0006)

**Problem.** Plugins that run work for a gateway session cannot show it in `/agents`.

**Minimal core change.** In `hermes_cli/plugins.py`, add `gateway_agents` to `VALID_HOOKS`. In
`gateway/slash_commands_status.py`, class `GatewayStatusCommandsMixin`, a helper `_agents_plugin_lines(session_key)`
calls the hook with the session's own `session_key`, `session_id` and profile, accepts only
`{"lines": [str, ...]}`, bounds the result, and appends it to the native `/agents` output. No plugin, no change.

**Note.** The patch also declares a second hook name, `gateway_busy_control`, which nothing calls yet. We would drop
it from a first proposal.

**Tests.** No dedicated test file in this repository. Suggested tests: no hook registered gives the native output
byte for byte; a malformed hook result is ignored; long output is bounded.

**Risks and compatibility.** A plugin could show misleading lines; they are clearly separated from the native output.

**Benefit without this plugin.** Any plugin with background work (schedulers, watchers, long tools) can show its
state where users already look.

## 3. Opt-in request observation for Codex streams (patch 0004)

**Problem.** Hermes cannot prove which provider route actually served a call (model, effort, tier on the wire), for
example after a fallback or a request override.

**Minimal core change.** In `agent/codex_runtime.py`, `run_codex_stream` is wrapped so that, **only when an agent has
a `_codex_request_observer` attached**, per-call HTTPX request and response hooks report the route of each actual
exchange to that observer. Without an observer the native path, the client and the wire body are unchanged.
Existing hooks are preserved; observers on a shared client are isolated; an observer error never changes the result.

**Tests.** `seam-tests/contracts/orchestrator/test_wire_observation.py` (10 tests: real SDK wire request, effective
effort and tier, privacy of recorded values, retries, unchanged path without observer, observer failure),
`test_native_child_sdk_control.py`, `test_native_worker_adapter.py`.

**Risks and compatibility.** It touches a hot path; the observer branch must stay strictly opt-in. Codex Responses only.

**Benefit without this plugin.** Debugging and auditing: confirm the model and effort really used, detect silent
fallbacks, record tier usage for cost tracking.

## 4. Required tool policy that survives plugin unload (patch 0002)

**Problem.** A `pre_tool_call` hook can block tools, but it is optional by nature: if the plugin is unloaded or the
hook is skipped on some dispatch path, the restriction disappears. A restriction that a task depends on should not.

**Minimal core change.** A new `agent/required_tool_policy.py` with `RequiredToolPolicy` (a task-local lease held in
a `ContextVar`) and `required_policy_error(name, args)`. `PluginContext.register_required_tool_policy(callback)` in
`hermes_cli/plugins.py`. The check runs at every dispatch point after argument rewrites: `model_tools.py`
(`_pre_dispatch_guards`, `_execute_tool`, `handle_function_call`), `tools/registry.py` and
`hermes_cli/plugins_dispatch.py`. A denial is reported once; nested policies combine (all must allow); a policy
stays binding until revoked, also after plugin unload, and a reload cannot revive an old scope.

**Tests.** `seam-tests/contracts/tool_policy/test_guard_adversarial.py`, `test_guard_lifetime.py`.

**Risks and compatibility.** No policy registered: no change. It is cooperative dispatch policy, not a sandbox, and
says so.

**Benefit without this plugin.** Safe "read-only mode" or "no network tools" for a session or a helper that cannot be
lost through a plugin reload or a missed dispatch path.

## 5. Cron per-job tier, no-fallback and memory isolation (patch 0008)

**Problem.** Cron jobs cannot pin a service tier or forbid fallback per job, and cron agents can load and write
personal memory although they run unattended.

**Minimal core change.** `cron/jobs.py`: optional job fields `service_tier` (`normal` or `priority`) and
`allow_fallbacks` (a boolean; `false` switches off every fallback chain, `true` or unset keeps today's behaviour),
validated in `create_job`. A new `cron/scheduler_agent.py` (`construct_cron_agent`) builds cron agents with
`skip_memory=True` and the `memory` toolset disabled; `cron/scheduler.py` uses it. `tools/cronjob_tools.py` accepts
and shows the new fields.

**Not opt-in as packaged.** The tier and fallback fields change nothing until a job sets them, but the memory
isolation applies to **every** cron job as soon as the patch is installed, with or without this plugin. The plugin's
optional `cron_reasoning_effort_tool` setting does not control it.

**Tests.** `seam-tests/contracts/cron/test_tier_override_matrix.py`, `test_tool_policy.py`,
`seam-tests/contracts/cron_memory/test_cron_memory_boundary_witness.py`.

**Risks and compatibility.** Memory isolation for cron is a behaviour change for existing users; for upstream it
should be a config switch with the current behaviour as the default. The new job fields are optional.

**Benefit without this plugin.** Predictable cost and model for scheduled jobs, and unattended jobs that cannot
change the user's memory.

## 6. Owner-bound service-tier policy (patch 0001)

**Problem.** Whether a request uses a normal or priority tier is decided in several places (`/fast`, background
review, delegation), with no single policy point and no binding to who made the request.

**Minimal core change.** A new `agent/service_tier_policy.py` and a `PluginContext` mixin
(`register_service_tier_policy(name, callback)` in `hermes_cli/plugins_service_tier.py`). A top-level config key
`service_tier_policy` selects one registered callback. The decision is bound to the request owner (gateway turn,
delegation child, cron job, background review), re-evaluated when the route changes, and the wire model may not
change after the decision. `agent/fast_mode.py`, `agent/turn_api_call.py`, `agent/background_review.py` and
`tools/delegate_tool.py` consult it. If a policy is selected but missing or failing, the request stops instead of
running with an unchecked tier. With no `service_tier_policy` key, nothing changes.

**Tests.** `seam-tests/service-tier/tests/` (wire, consumption and gateway tests). Eight gateway tests also need a
separate "gateway route policy" module that is not part of this series; they are expected failures here (see
[TESTING.md](../TESTING.md)).

**Risks and compatibility.** The largest behaviour surface of the medium seams; the gateway half is coupled to a
module that is not included. A first proposal could cover only the agent-side policy point.

**Benefit without this plugin.** One place to decide priority tier for cost control, per user or per channel.

## 7. Trusted completion fence for background delegation (patch 0003)

**Size.** 546 lines added, 61 removed. This is **not a small hook**; we recommend splitting it (see below).

**Problem.** Background delegation results are stored and replayed after a restart. An integration that must check a
result before delivery (still current? reviewed? not superseded?) has no hook on the replay path.

**Minimal core change.** In `tools/async_delegation.py`: `register_completion_guard(provider)` /
`set_completion_guard(provider)`. A completion that carries a `completion_fence` marker is delivered only after the
registered guard validates it, on live delivery and on orphan replay after a restart
(`restore_undelivered_completions`, `sweep_orphaned_completions`). A marked completion with no matching row never
falls back to legacy delivery. `gateway/run_notifications.py` passes the fence when it claims a completion.
Unmarked completions behave exactly as today.

**Suggested split.** (a) the guard registration and the check on live delivery; (b) the check on restart replay and
orphan sweep; (c) the gateway claim change.

**Tests.** `seam-tests/contracts/orchestrator/test_native_completion_fence_contract.py`,
`test_parent_fence_adversarial.py`.

**Risks and compatibility.** It touches durable delivery; review needs care around restart and replay. Unmarked
records keep the current path.

**Benefit without this plugin.** Any plugin or workflow that hands work to background delegation can stop stale or
unapproved results from reaching users, including after a crash.

## 8. Per-task capability authority at the effect (patch 0007)

**Size.** 960 lines added, 4 removed. This is **not a small hook**; we recommend splitting it (see below).

**Problem.** Helper agents get whole toolsets. There is no way to say "this helper may write only inside `src/`" and
have it enforced at the actual file write rather than in the prompt.

**Minimal core change.** `agent/delegation_context.py` carries a `DelegationIdentity` (run, task, generation, write
scope, capability profile), set through `delegated_child_context(...)`. A new `tools/delegation_contracts.py` holds
the capability profiles and their tool allowlists. `authorize_delegated_tool`, `authorize_delegated_write` and
`authorize_delegated_terminal` are called at the effect in `tools/file_tools.py` (`write_file_tool`, `patch_tool`),
`tools/code_execution_tool.py` (`execute_code`), `tools/terminal_tool.py`, `tools/environments/local.py` and
`tools/process_registry.py`. With no identity set, every function returns immediately and nothing changes.

**Suggested split.** (a) the identity and the write-scope check in `write_file_tool` / `patch_tool` only; (b) the
profiles and allowlists, without the task-graph and state-machine types that `tools/delegation_contracts.py` also
contains; (c) terminal, code execution and process handling, as a separate design discussion.

**Tests.** `seam-tests/contracts/task_authority/test_execution_authority_carry.py`, `test_file_authority.py`,
`test_local_authority_forwarding.py`.

**Risks and compatibility.** The largest patch. Terminal is refused outright for scoped tasks, which is safe but
strict.

**Benefit without this plugin.** Native `delegate_task` could offer per-child write scopes ("this child edits docs/,
that one only reads") with real enforcement.

---

## What could live as a plugin today

Most of the orchestration needs no core change and could stay a plugin: the queue, dependencies, priorities,
join and collect, supersede, quota admission (it uses Hermes' existing account-usage API), the final review gate,
history and status, and the read-only selector (it uses the existing `pre_llm_call` hook). Detached delivery uses the
existing background delegation; it only needs seam 7 to be safe after a restart.

## Ideas for native `delegate_task`, longer term

**Optional background, not part of the first request.** These are features of the plugin that could one day fit
native `delegate_task` itself, if the maintainers find them useful. We list them only as ideas; any shape that fits
Hermes is welcome.

- **Dependency-aware task graph.** Tasks that name the tasks they wait for, with cycles refused, so "collect, then
  analyse" runs in order without the parent waiting in between. Plugin-only today; no card.
- **Collect-later results with a cursor.** Results stored per run and read page by page with a cursor, instead of one
  delivered message. Plugin-only today; no card.
- **Per-task write scopes enforced at the effect.** "This helper may write only inside `src/`", checked at the actual
  file write rather than in the prompt. Card 8 (with card 4 for the dispatch-time check).
- **Quota admission before starting work.** Read the account usage first and refuse to start helpers when it is
  missing, stale or exhausted. Uses Hermes' existing account-usage API; no card.
- **An optional final review gate.** When a run declares a reviewer task, nothing is delivered until it approves the
  actual results with an evidence-bound answer. Plugin-only today; card 7 keeps it safe for background delivery after
  a restart.
- **A read-only selector hint.** A hook that notices requests for several independent read-only checks and suggests
  parallel helpers; it only adds a hint and never starts work. Uses the existing `pre_llm_call` hook; no card.

## What we would NOT propose

- The plugin as packaged, taken as is. It is an experiment and not a drop-in, so we do not ask Hermes to take it in
  this form. You are warmly invited to adopt, reimplement or integrate any of its ideas in whatever shape fits Hermes
  (see the section above), and we would be glad to help with design, tests and review.
- The fixed model names (`gpt-6-astra`, `gpt-6-luna`) and the `openai-codex`-only restriction; they belong to this
  experiment, not to Hermes.
- The unused `gateway_busy_control` hook name.
- The task-graph and state-machine parts of `tools/delegation_contracts.py`.
- Cron memory isolation as an unconditional change; upstream it should be a switch.
- Refusing the terminal for every scoped helper as a default for everyone; that should stay a choice.

## Suggested way to review

1. **One public feature request** for the first small lifecycle and status hooks: seam 1 (`register_close_callback`,
   33 lines) and seam 2 (`/agents` hook, 44 lines), each as its own small pull request.
2. Optional, only if the style fits: seam 3 (request observation) and seam 4 (required tool policy), as separate pull
   requests with their tests moved into Hermes' own test layout.
3. Optional: seams 5 and 6 after a decision on the defaults (memory isolation for cron as a switch; scope of the tier
   policy).
4. Optional, last: seams 7 and 8 as design discussions first, then in the smaller pieces suggested in their cards;
   they touch durable delivery and file effects.

Each seam is one patch in `patches/`, made with `git format-patch` against 8a3ede1b: each of the 8 patches applies
cleanly by itself on 8a3ede1b (`git apply --check`).

## Offer to help

If any seam is of interest, open an issue here. We are glad to rebase a seam onto current Hermes, split it further,
rename it to your conventions, adapt the tests to your layout, or answer questions in a review.
