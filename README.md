# hermes-task-orchestrator

Experimental task orchestration for [Hermes Agent](https://github.com/NousResearch/hermes-agent): a task queue with
dependencies, joins and collect-later results, per-run scoped permissions, quota admission and an optional final
review gate. It is a Hermes plugin plus a series of core patches ("seams").

> **Status: experimental, built against Hermes upstream 8a3ede1b.**
> Built and tested only against upstream commit `8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96` with the 8 patches in
> `patches/` applied. It is not a drop-in plugin for unmodified Hermes, and it has not been run against a later
> Hermes release, a live model account or Windows.

## Who is this for

**It may suit you if** you run Hermes on Linux or macOS with an `openai-codex` login, you want dependency-aware task
graphs, results you can collect later, per-run scoped permissions, quota admission before work starts and an optional
review gate, and you are willing to run a Hermes checkout with the 8 patches applied. There are two ways to use it:
try it as is by following the steps below, or take the ideas and build them your own way.

**It is not for** unmodified Hermes installs that want a drop-in plugin, Windows, providers other than
`openai-codex`, or anyone who needs a stable release. It is experimental, built against one Hermes commit, and comes
with no guarantees. [TESTING.md](TESTING.md) shows exactly what was verified and how.

## This is a NON-VANILLA add-on

Hermes already ships `delegate_task`, which runs helper agents in parallel and is the right tool for most jobs. This
project explores what an orchestration layer *on top* of it could add. It changes Hermes in two ways, and both are
clearly separated:

| Part | Where | What it needs |
|---|---|---|
| **The plugin** | `plugin/external_orchestrator/` | a normal user plugin: 8 tools, 3 hooks, a CLI command |
| **The core seams** | `patches/0001` to `patches/0008` | extension points in Hermes core that the plugin calls |

Seven of the eight seams change nothing until something registers for them or a new setting is used. **One
exception:** patch 0008 changes cron jobs as soon as it is installed, with or without this plugin: cron agents then
run without memory and without the memory toolset. See card 5 in [docs/UPSTREAMING.md](docs/UPSTREAMING.md).

Without the seams the plugin's own unit tests still pass, but the plugin cannot enforce what it promises (11 of the
12 seam test groups fail on unpatched Hermes; see [TESTING.md](TESTING.md)). Each seam is described as an upstream
proposal in [docs/UPSTREAMING.md](docs/UPSTREAMING.md).

## Why

`delegate_task` runs helpers in parallel. Top-level calls run in the background, and while they run the parent can
list them, steer a child with a correction or stop one. Longer agent jobs sometimes also need:

- **ordering** ("B starts after A"), not only parallel width;
- **results you can pick up later**, page by page, instead of one delivered message;
- **different permissions per run** ("this run may edit `src/`, that one may only read"), enforced where the effect
  happens, not only described in the prompt;
- **a stop before spending**: do not start workers when the account allowance cannot be confirmed;
- **an optional gate before delivery**: when a final review is declared, nothing is delivered until the reviewer
  task approved the actual results.

## Headline features

- **Queue with dependencies.** Up to 64 tasks per run, a dependency graph (cycles refused), priorities with aging,
  per-task queue deadlines, retries and supersession with generations. Dependencies set the *order*; they do not
  pass one task's answer to the next automatically.
- **Join and collect later.** `orchestration_join` waits for all, the required, the first or named tasks;
  `orchestration_collect` pages through results with a cursor.
- **Scoped permissions.** One capability profile per run (for example `read-only` or `workspace-write`) and, for
  writes, the folders a task may change. Checked at every tool dispatch and at the file effect. Terminal use is
  refused for these tasks. This is in-process enforcement, not an operating-system sandbox.
- **Quota admission.** Before work starts, the plugin reads the account usage through Hermes' own account-usage API
  and refuses when it is missing, stale or exhausted.
- **Final review, when declared.** A run may name a final review task (this is optional). If it does, nothing is
  delivered until that task returns a strict, evidence-bound approval.
- **Read-only selector.** A `pre_llm_call` hook notices requests for several independent read-only inspections and
  suggests an orchestration run. It only adds context; it never starts work.
- **Caps and observability.** Host-configured worker width per profile (see section 4 of
  [docs/FEATURES.md](docs/FEATURES.md) for what is and is not shared), queue size and result-size limits; owner-only
  status, bounded history and transcript cursors; optional `/agents` lines in the gateway.
- **Stop and cancel.** Cancel a task or a run; workers are revoked when their parent closes; unfinished work is
  cleaned up at a CLI session boundary.

Every feature, with its limits: [docs/FEATURES.md](docs/FEATURES.md).

## Try it

You need Linux or macOS, git, network access for the clone, a Python environment for Hermes, and an `openai-codex`
login in the Hermes profile you use (see [Requirements](#requirements)).

First tell your shell where this repository is (replace the path):

```sh
export ORCH_PKG=/path/to/hermes-task-orchestrator
```

**1. Hermes with the seams.** Run this in a folder where `hermes` does not exist yet. The chain stops at the first
failing step, so a failed clone or checkout is never followed by `git apply`.

```sh
[ -f "${ORCH_PKG:-}/run_tests.py" ] &&
  [ ! -e hermes ] &&
  git clone https://github.com/NousResearch/hermes-agent.git hermes &&
  git -C hermes checkout --detach 8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96 &&
  git -C hermes apply --index "$ORCH_PKG"/patches/*.patch &&
  echo "OK: Hermes at 8a3ede1b with the 8 patches in ./hermes"
```

`git apply --index` changes the files and the index; it creates no commits. Then install Hermes from that folder as
Hermes' own documentation describes.

**2. The plugin.** It goes into the `plugins/` folder of your Hermes home. Set `HERMES_HOME` to that home first (for
the default profile: `export HERMES_HOME="$HOME/.hermes"`). Paste the guard below, then the copy line.

The guard resolves the physical path of `HERMES_HOME` (symlinks, `.` and `..` resolved) and refuses when:

- `HERMES_HOME` is empty or unset, or not an existing directory, or `HOME` is unset;
- the resolved folder is `/`, your home folder, or any folder that contains your home folder (also when it is
  reached through a symlink or an alias such as `/.` or `/usr/..`);
- the folder has no `config.yaml`. Hermes keeps its settings in `config.yaml` in the Hermes home, and `config.yaml`
  is one of the files Hermes itself uses to recognise a Hermes home;
- `plugins/` is a symlink, or exists but is not a folder;
- `plugins/external-orchestrator` already exists. Remove or move the old copy first: copying again into an existing
  folder would nest the files instead of replacing them.

The guard checks the folders at the moment it runs. It reduces the risk of copying to the wrong place, but it is not
a guarantee: for example, folders changed by another program between the check and the copy are not detected. Use a
Hermes home that only you control.

```sh
hermes_home_ok() {
  h="${HERMES_HOME:-}"
  [ -n "$h" ] || { echo "refused: HERMES_HOME is empty or unset" >&2; return 1; }
  [ -d "$h" ] || { echo "refused: HERMES_HOME is not an existing directory: $h" >&2; return 1; }
  [ -n "${HOME:-}" ] || { echo "refused: HOME is unset" >&2; return 1; }
  ORCH_HOME=$(cd -P -- "$h" && pwd -P) || { echo "refused: cannot resolve HERMES_HOME" >&2; return 1; }
  ORCH_HOME=$(printf '%s' "$ORCH_HOME" | sed 's#/*$##')
  hp=$(cd -P -- "$HOME" && pwd -P) || { echo "refused: cannot resolve HOME" >&2; return 1; }
  hp=$(printf '%s' "$hp" | sed 's#/*$##')
  case "$hp/" in "$ORCH_HOME"/*) echo "refused: HERMES_HOME resolves to /, your home folder or a folder above it: ${ORCH_HOME:-/}" >&2; return 1;; esac
  [ -f "$ORCH_HOME/config.yaml" ] || { echo "refused: no config.yaml in $ORCH_HOME (not a Hermes home)" >&2; return 1; }
  [ ! -L "$ORCH_HOME/plugins" ] || { echo "refused: $ORCH_HOME/plugins is a symlink" >&2; return 1; }
  [ ! -e "$ORCH_HOME/plugins" ] || [ -d "$ORCH_HOME/plugins" ] || { echo "refused: $ORCH_HOME/plugins is not a folder" >&2; return 1; }
  ORCH_DEST="$ORCH_HOME/plugins/external-orchestrator"
  if [ -e "$ORCH_DEST" ] || [ -L "$ORCH_DEST" ]; then echo "refused: $ORCH_DEST already exists" >&2; return 1; fi
}
```

```sh
hermes_home_ok && [ -f "${ORCH_PKG:-}/plugin/external_orchestrator/plugin.yaml" ] && mkdir -p "$ORCH_HOME/plugins" && cp -R "$ORCH_PKG/plugin/external_orchestrator" "$ORCH_DEST" && echo "OK: copied to $ORCH_DEST"
```

**3. Enable it.** Add the plugin to the `plugins.enabled` list in your existing `config.yaml` (merge it into what is
there; do not replace other enabled plugins), or run `hermes plugins enable external-orchestrator`. The `enable`
command may need network access the first time; in an offline test it tried to download a Python runtime and
stopped, while the `config.yaml` route worked.

```yaml
plugins:
  enabled:
    - external-orchestrator
```

**4. Configure the worker route** (required; see below), then start Hermes on the `openai-codex` provider. The
tools `orchestration_create`, `orchestration_join` and friends appear in the `external_orchestration` toolset.

To see the scheduler without any account first, run the offline example scripts in
[docs/EXAMPLES.md](docs/EXAMPLES.md). They drive the scheduler with a simulated worker; they create temporary
folders and files.

## Required configuration

The plugin takes its route and limits from the host `config.yaml`, never from model output. Without this block it
refuses to start workers ("strict host worker policy required"). This is the minimal block; every key is explained in
[docs/CONFIGURATION.md](docs/CONFIGURATION.md).

```yaml
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

The parent must be an `openai-codex` session; the model names `gpt-6-astra` (parent, for the selector) and
`gpt-6-luna` (worker) are fixed in the code. Data is stored under `$HERMES_HOME/plugin-data/external-orchestrator`.

## Requirements

- Linux or macOS. Windows is not supported: the plugin uses POSIX file locks (`fcntl`) and POSIX process handling.
- Hermes at `8a3ede1be0618462e3e5e15e9ab4bdb8ae82af96` with the 8 patches, in its own Python environment
  (Python 3.11 to 3.14). The plugin uses only the standard library and packages Hermes already depends on.
- An `openai-codex` provider login in that Hermes profile. Other providers are refused by design.

## Documentation

| File | What it covers |
|---|---|
| [docs/FEATURES.md](docs/FEATURES.md) | every feature: what it does, why it helps, how to use it, its limits |
| [docs/COMPARISON.md](docs/COMPARISON.md) | native `delegate_task` and this orchestration, side by side |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | every configuration key, with types, defaults, validation and examples |
| [docs/EXAMPLES.md](docs/EXAMPLES.md) | copy-paste examples with the outputs the code produces |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | diagram, data flow, contracts, glossary |
| [docs/SECURITY.md](docs/SECURITY.md) | what the permission guards protect, and what they do not |
| [docs/UPSTREAMING.md](docs/UPSTREAMING.md) | one proposal card per core seam, for the Hermes maintainers |
| [docs/DESIGN-CONTRACTS.md](docs/DESIGN-CONTRACTS.md) | the invariants the seam tests check |
| [docs/CONTRIBUTING.md](docs/CONTRIBUTING.md) | issues and small pull requests welcome |
| [TESTING.md](TESTING.md) | exact test commands and expected numbers |

## Testing in one line

On upstream 8a3ede1b **with** the 8 patches: 214 passed, 0 failed, 9 expected failures. **Without** the patches:
102 passed, 108 failed, 4 errors, 9 expected failures, which shows that the seams are needed. The tests themselves
are offline, with fake providers; cloning Hermes and building its environment need network. Details:
[TESTING.md](TESTING.md).

## For the Hermes maintainers

You are welcome to reuse and adapt this under MIT, retaining its required copyright and permission notices. No
additional acknowledgement is requested.

Seven of the seams change nothing when nothing registers for them; patch 0008 changes cron memory behaviour as soon
as it is installed (see above). [docs/UPSTREAMING.md](docs/UPSTREAMING.md) proposes the seams one at a time,
easiest first, and offers help with tests and review.

The plugin as packaged is an experiment, not a drop-in, so we do not ask Hermes to take it as is. But you are warmly
invited to adopt, reimplement or integrate any of its ideas in whatever shape fits Hermes; UPSTREAMING.md also lists
longer-term ideas for native `delegate_task`. We would be glad to help with design, tests and review.

## Seam labels

Test names and a few code comments carry short labels. They only name a seam or a plugin area:

| Label | Meaning |
|---|---|
| GW-03 | owner-bound service-tier policy (patch 0001) |
| DC-02 | required tool policy (0002) |
| DC-03 | completion fence (0003) |
| DC-05 | request observation (0004) and parent close callbacks (0005) |
| DC-10 | gateway diagnostic hooks (0006) |
| TM-01 | per-task capability authority (0007) |
| DC-08 / DC-09 | cron per-job tier / cron memory isolation (0008) |
| DC-01, DC-04, DC-06, DC-07 | plugin-only areas: task queue, read-only selector, worker route policy, quota |

## License and affiliation

MIT, see [LICENSE](LICENSE); the upstream Hermes notice is kept. See [NOTICE.md](NOTICE.md) for what comes from
where. This project is **not affiliated with or endorsed by Nous Research**. "Hermes" refers to Nous Research's
Hermes Agent, which this add-on extends.
