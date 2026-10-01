# Configuration

Every setting the plugin and the core seams read, where it lives, its type, default and valid values. All settings
are read from the Hermes profile's `config.yaml` (or the environment, where noted) on the host side. Values written
by a model in a tool call never set policy, route or limits.

## How values are checked

Not every setting is checked the same way. There are four kinds:

| Kind | Settings | Absent | Wrong type or out of range |
|---|---|---|---|
| **Strict host limits and security settings** | `delegation.worker.*`, `delegation.scheduler.*`, `delegation.policy.allow_xhigh`, `delegation.compatibility.detached_completion_delivery` | the default shown below (the worker route has no defaults: it is required) | **refused**: new work is not admitted (or the worker is not started) and an error is returned; no default is substituted |
| **Permissive switches** | `delegation.policy.quota_guard_mode`, `delegation.policy.auto_cancel_optional_on_finalize`, `delegation.routing_policy.luna_policy_mode` | the default | no error; the value falls back to the **safer** side: an unknown quota mode means `enforce`; anything other than literal `true` turns turn-end cleanup **off**; anything other than `observe` turns the selector's suggestions **off** |
| **Plugin switches** | `plugins.entries.external-orchestrator.settings.*` | off | no error; only the literal boolean `true` turns a feature on, every other value leaves it off |
| **Per-task values and CLI capacities** | tool arguments (section 10), `EXTERNAL_ORCHESTRATOR_MAX_GLOBAL`, `EXTERNAL_ORCHESTRATOR_PER_PROFILE` | the default | mixed, see section 10 and 9: some are refused, some are converted (`"3"` becomes 3) or clamped into range |

## 1. The worker route: `delegation.worker` (required)

The model, provider and endpoint every worker uses. The plugin refuses to start workers without this block.

| Key | Type | Default | Valid values | Meaning |
|---|---|---|---|---|
| `provider` | string | none (required) | `openai-codex` | provider of every worker |
| `model` | string | none (required) | `gpt-6-luna` | worker model; any other value is refused ("approved Luna worker model required") |
| `api_mode` | string | none (required) | `codex_responses` | wire API |
| `base_url` | string | none (required) | an `https` URL with a host and no user name, password, query or fragment | provider endpoint |
| `allow_fallbacks` | boolean | none (required) | exactly `false` | workers never fall back to another provider or model |
| `default_reasoning_effort` | string | none (required) | `low`, `medium`, `high`, `xhigh` | reasoning effort of every worker |
| `background_service_tier` | string | none (required) | `normal`, `priority` | service tier of every worker |
| `max_result_chars` | integer | 14000 | 256 to 100000 | largest result a worker may return |
| `max_packet_tokens` | integer | 12000 | 256 to 272000 | largest task packet (goal, context, acceptance) sent to a worker, estimated in tokens |

**Public spellings and internal aliases.** Write `normal` or `priority` for `background_service_tier`. Internally,
`normal` is translated to Hermes' tier name `default`, and the plugin also accepts `default` written directly. That
alias is an implementation detail; use `normal`.

## 2. The queue: `delegation.scheduler` (optional)

| Key | Type | Default | Valid values | Meaning |
|---|---|---|---|---|
| `enabled` | boolean | `true` | `true`, `false` | `false` refuses all new work and switches the read-only selector off |
| `max_workers` | integer | 3 | 1 to 16 | workers this profile's scheduler runs at once, and its per-profile slot count (see below) |
| `max_tasks_per_run` | integer | 64 | 1 to 64 | tasks in one run |
| `max_queued_tasks` | integer | 128 | 1 to 128 | tasks waiting in this profile's store |
| `fairness` | string | `weighted_deficit_round_robin` | only that value | how one scheduler shares turns between runs and between the profiles in its state |

`max_workers` is per profile. Each profile has its own scheduler and store; capacity slots are file locks with a
per-profile pool of `max_workers` slots and one installation-wide host pool of 16 slots fixed in the code. Two
profiles configured with `max_workers: 3` can therefore run 6 workers together; the whole installation runs at most
16. Fairness between different profiles' schedulers or processes is not established. Details:
[FEATURES.md](FEATURES.md) sections 3 and 4.

## 3. Policy: `delegation.policy` (optional)

| Key | Type | Default | Valid values | Meaning |
|---|---|---|---|---|
| `allow_xhigh` | boolean | `true` | `true`, `false` | `false` refuses work whose effort would be `xhigh`; any other type is refused |
| `quota_guard_mode` | string | `enforce` | `enforce`, `observe` | `enforce` checks the account allowance before admitting work; `observe` skips the check; any other value (or an unreadable configuration) means `enforce` |
| `auto_cancel_optional_on_finalize` | boolean | `true` | `true`, `false` | cancel optional tasks bound to a turn when that turn completes; any value other than literal `true` (or an unreadable configuration) switches it **off** |

## 4. Delivery: `delegation.compatibility` (optional)

| Key | Type | Default | Valid values | Meaning |
|---|---|---|---|---|
| `detached_completion_delivery` | boolean | `true` | `true`, `false` | allow `mode: "detached"` runs (results through Hermes' background completion delivery); any other type is refused |

## 5. The read-only selector: `delegation.routing_policy` (optional)

| Key | Type | Default | Valid values | Meaning |
|---|---|---|---|---|
| `luna_policy_mode` | string | `observe` | `observe`, or anything else to switch off | the selector suggests runs only while this is `observe`; any other value switches the suggestions off, with no error |

The selector also requires `delegation.scheduler.enabled` to be `true`, a top-level session on provider
`openai-codex` with model `gpt-6-astra`, and `orchestration_create` among the session's tools.

## 6. Plugin settings: `plugins.entries.external-orchestrator.settings` (optional)

Read through Hermes' standard plugin settings (`PluginContext.get_config`) when the plugin is registered. Only the
literal boolean `true` turns a feature on; `"true"`, `1` or any other value leaves it off, without an error.

| Key | Type | Default | Meaning |
|---|---|---|---|
| `gateway_extensions` | boolean | `false` | `true` adds owned runs to the gateway `/agents` output (needs patch 0006) |
| `cron_reasoning_effort_tool` | boolean | `false` | `true` adds an optional `reasoning_effort` field to the `cronjob` tool. It does not switch the cron memory isolation of patch 0008 on or off |

## 7. Service-tier policy: `service_tier_policy` (top level; patch 0001; optional)

| Key | Type | Default | Meaning |
|---|---|---|---|
| `service_tier_policy` | string | unset | name of a plugin-registered tier policy that decides `normal` or `priority` per request. Unset: no policy, Hermes behaves as before. Set but not registered or failing: the request **stops** instead of running with an unchecked tier. |

This plugin does not register a tier policy itself; it only respects one when present.

## 8. Cron jobs (patch 0008; per job)

| Field | Type | Default | Valid values | Meaning |
|---|---|---|---|---|
| `service_tier` | string | unset | `normal`, `priority` | tier for this job's agent; other values are refused when the job is created |
| `allow_fallbacks` | boolean | unset | `true`, `false` | `false` switches off every fallback chain for this job; `true` or unset keeps Hermes' normal fallback behaviour; a non-boolean is refused |
| `reasoning_effort` | string | unset | Hermes' own cron validation | settable through the `cronjob` tool when `cron_reasoning_effort_tool` is on |

**Not a setting:** with patch 0008 installed, cron agents always run without memory and without the memory toolset,
whether or not this plugin is installed or registered.

## 9. Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `EXTERNAL_ORCHESTRATOR_DATA` | `$HERMES_HOME/plugin-data/external-orchestrator` | where runs are stored |
| `EXTERNAL_ORCHESTRATOR_MAX_GLOBAL` | 2 | capacity value used only by the `hermes orchestration` CLI; converted to an integer and clamped to 1 to 32; a non-number gives the default |
| `EXTERNAL_ORCHESTRATOR_PER_PROFILE` | 1 | per-profile value used only by the CLI; same rules |

The CLI never starts workers, so these two values have no practical effect on running work.

## 10. Per-task settings (tool arguments)

These are set per run or task in `orchestration_create`, `orchestration_enqueue` or `orchestration_supersede`. The
tool schema already rejects many wrong values before the plugin sees them; the column "If wrong" describes what the
plugin itself then does.

| Field | Type | Default | Valid values | If wrong | Meaning |
|---|---|---|---|---|---|
| `mode` (run) | string | `joinable` | `joinable`, `detached` | refused | how results are delivered |
| `capability_profile` (run) | string | `read-only` | `none`, `read-only`, `web-read`, `code-read`, `code-read-test`, `workspace-write`, `computer-use` | refused | tools every task of the run may use ([FEATURES.md](FEATURES.md) 11) |
| `write_scope` (run or task) | list of paths | `[]` | absolute folders; a task may only narrow the run's list | refused | where `workspace-write` tasks may write; narrowing does not change the profile |
| `final_review_task_id` (run) | string | unset (no review) | id of a required task that depends on every other task | refused | optional final review gate |
| `task_id` | string | generated | up to 512 characters, unique | refused | task identity |
| `goal`, `context`, `acceptance` | string | `goal` required | up to 16000 characters each | refused | the work; this text is all a worker receives |
| `dependencies` | list of task ids | `[]` | ids in the same run, no cycles | refused | ordering only; no data is passed |
| `priority` | string | `support` | `critical`, `support`, `background` | refused | admission weight |
| `required` | boolean | `true` | | | an optional task's failure does not fail the run |
| `timeout_seconds` | number | **5** | above 0, up to 600 | converted to a number; out of range refused | running time per attempt; set it for real work |
| `max_attempts` | integer | 2 | 1 to 5 | converted to an integer; out of range refused | attempts after host-side failures or timeouts |
| `deadline_seconds` | integer | 300 | 1 to 86400 | refused (must be a real integer) | how long a task may wait in the queue |
| `max_iterations` | integer | 8 | 1 to 500 | converted and clamped to 1 to 1000 when stored; a value above 500 is refused when the worker starts | model-loop steps of the worker |
| `max_result_chars` | integer | 14000 | 256 up to the host's `max_result_chars` | converted and clamped into range | result size for this task |

`orchestration_join` takes `condition` (`all`, `required`, `first`, `task_ids`), `task_ids` and `timeout_seconds`
(clamped to 0 to 600, default 30). `orchestration_collect` and `orchestration_history` take `cursor` and `limit`
(1 to 50).

## Parent and worker models

The orchestrator has two roles:

- **The parent** is your normal Hermes session. It must run on provider `openai-codex` to create work (choose the
  provider and model as usual, for example with `hermes model`). For the read-only selector the parent model must be
  `gpt-6-astra`.
- **The workers** always use the route in `delegation.worker`. In this build the worker model is fixed to
  `gpt-6-luna`; the parent cannot pick another one per task.

Workers are separate from Hermes' native `delegate_task` settings (`delegation.model`, `delegation.provider`,
`delegation.max_concurrent_children` and so on). Those keep controlling `delegate_task`, which stays available next
to this plugin.

| Concern | Setting |
|---|---|
| Worker model and effort | `delegation.worker.model`, `delegation.worker.default_reasoning_effort` |
| Concurrency | `delegation.scheduler.max_workers` per profile; at most 16 for the installation |
| Step limit | per-task `max_iterations` (default 8, at most 500) |
| Timeouts | per-task `timeout_seconds` (attempt), `deadline_seconds` (queue); `orchestration_join` `timeout_seconds` (wait) |
| Reasoning effort | `delegation.worker.default_reasoning_effort`; `delegation.policy.allow_xhigh` |

## Minimal example

```yaml
plugins:
  enabled:
    - external-orchestrator
delegation:
  worker:
    provider: openai-codex
    model: gpt-6-luna
    api_mode: codex_responses
    base_url: https://chatgpt.com/backend-api/codex
    allow_fallbacks: false
    default_reasoning_effort: high
    background_service_tier: normal
```

## A fuller example

This example writes every optional key out with its default value, except the reasoning effort. It uses `xhigh`
where the minimal example uses `high`. That is a **policy choice** (more reasoning per worker, at a higher cost and
time per call), not a measured or benchmarked recommendation; `high` is a reasonable choice as well.

```yaml
plugins:
  enabled:
    - external-orchestrator
  entries:
    external-orchestrator:
      settings:
        gateway_extensions: false      # true needs patch 0006
        cron_reasoning_effort_tool: false
delegation:
  worker:
    provider: openai-codex
    model: gpt-6-luna
    api_mode: codex_responses
    base_url: https://chatgpt.com/backend-api/codex
    allow_fallbacks: false
    default_reasoning_effort: xhigh    # policy choice; see above
    background_service_tier: normal
    max_result_chars: 14000
    max_packet_tokens: 12000
  scheduler:
    enabled: true
    max_workers: 3                     # per profile; all workers may share one account
    max_tasks_per_run: 64
    max_queued_tasks: 128
  policy:
    allow_xhigh: true
    quota_guard_mode: enforce
    auto_cancel_optional_on_finalize: true
  compatibility:
    detached_completion_delivery: true
  routing_policy:
    luna_policy_mode: observe          # read-only selector on
```
