# Security

This project is experimental. Please read this page before you let workers change files.

## What the permission guards protect

For every worker task, the host builds a *task authority* from host data only: the run's capability profile, its
write scope, a write token derived from the host owner and the exact task identity, and the parent's own tool set.
A model cannot set or widen any of these.

The authority is checked in two places:

1. **At every tool dispatch** (core seam 0002). A tool outside the capability profile, or outside what the parent
   itself may use, is refused. The check runs after argument rewrites, on every dispatch path, and it stays binding
   even if the plugin is unloaded, until the task is revoked.
2. **At the effect** (core seam 0007). `write_file` and `patch` must target a path inside the write scope (paths are
   resolved, so `..` and symlinks out of the scope do not help). `terminal` and `execute_code` are refused for
   worker tasks, because a shell command cannot be checked reliably before it runs.

Further protections:

- Workers use only the route configured by the host; the actual provider request is observed and must match it.
- Status, history and transcripts are visible only to the owning session.
- Storage folders are owner-only (0700, files 0600) and are opened without following symlinks.
- Two tasks with overlapping write scopes are not admitted at the same time.
- Results in the structured format that list artifacts or passed checks are accepted only if the host can verify
  them.
- When a run declares a final review, that review must approve the actual evidence before anything of the run is
  delivered.

## What they do NOT protect

- **This is not an operating-system sandbox.** The checks run inside the Hermes process, in the same user account.
  They are cooperative: they stop the model's tool calls, not code that is already running. Do not use this to run
  hostile code.
- **Revocation is not termination.** Cancelling a task or closing its parent revokes the authority; a tool call that
  was already admitted may still finish. Worker threads are not killed.
- **Reads are not confined to the write scope.** `read-only` and `workspace-write` tasks can read any file the Hermes
  process can read.
- **Web and computer-use profiles** (`web-read`, `computer-use`) are only as safe as the corresponding Hermes tools.
- **Prompt content is not filtered.** Task text and worker results are passed as data, and the reviewer prompt says
  so, but a model can still be misled by what it reads.
- **Prose claims are not verified.** A worker that writes "tests passed" in plain text is not checked; only the
  structured result format is.
- **Runs without a declared final review are not reviewed.** The review gate is optional.
- **The final review is a model.** The gate checks that an approval is explicit and bound to the actual evidence,
  not that the reviewer is right.
- **Quota protection depends on the provider's report.** It refuses at an exhausted window, not before.
- **The install guards are checks, not guarantees.** The guards in the README and TESTING refuse common mistakes
  (empty values, `/`, your home folder and aliases of them, symlinked or existing destinations), but they check the
  folders only at the moment they run.
- **Native `delegate_task` is unchanged.** Its children follow Hermes' normal rules, not these guards.

## Safe defaults

- Runs are `read-only` unless you ask for `workspace-write`, and `workspace-write` requires a non-empty write scope.
- Workers cannot use the terminal or run code.
- `allow_fallbacks` must be `false`: workers never silently switch provider or model.
- The worker route, the queue limits and `allow_xhigh` must be valid: a wrong value refuses new work instead of
  using a guess. The softer switches fall back to the safer side (see "How values are checked" in
  [CONFIGURATION.md](CONFIGURATION.md)).
- The quota check is on (`enforce`); an unknown mode also means `enforce`.
- The optional gateway and cron plugin features are off by default.
- The install commands in the README refuse an unset or unsafe `HERMES_HOME`.

## Recommendations

- Give `workspace-write` tasks the narrowest folder that works, and keep version control on it.
- Declare a final review for anything that is delivered to people or changes files.
- Keep `max_workers` low on a single account; remember that it applies per profile.
- Run Hermes itself in a container or a separate user account if you need real isolation.

## Reporting a problem

Please open an issue that describes the problem without including any secrets, keys or private data. If the issue
is sensitive, open an issue asking for a private contact first.
