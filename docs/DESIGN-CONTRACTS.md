# Design contracts (what each part promises)

These are the invariants the seam tests (`seam-tests/`) check. Wording is from
the code's own docstrings and the contract tests.

## Plugin
- **Packets never supply authority.** Policy, capacity, quota and route come
  from host config and host state; model-written task packets only describe work
  (`admission_policy.py`, `operational_controls.py`).
- **One profile per run.** The planner rejects tasks with different profiles
  before grouping; source or permission boundaries are never mixed
  (`planner.py`).
- **Generations.** A superseded task gets a new generation; the replacement
  starts with no inherited result, and a cancelled replacement delivers an empty
  envelope, never the old result (`scheduler.py`).
- **Capacity is kernel-held.** Slots are shared across profile-local stores via
  owner locks; a dead owner's slot is reconciled, a live owner's is preserved
  (`capacity.py`).
- **Quota fails closed.** Missing or mismatched quota evidence blocks admission
  and requires a human check-in (`quota_gate.py`).
- **Final review.** When a run names a final review task, delivery waits for its
  pass; artifacts changed after review block delivery (`claim_validation.py`).
- **Worker binding.** A worker's result is accepted only if the host observed the
  actual provider request (route, model) for that attempt; host metadata never
  comes from model output (`native_worker.py`, `observation.py`).
- **Owner isolation.** Status, history and diagnostics are visible only to the
  owning caller; diagnostic reads must not change scheduler state
  (`gateway_extensions.py`, `history.py`, `diagnostic_store.py`).
- **Storage** is owner-private with descriptor-anchored path traversal
  (`storage.py`).

## Core seams (patches)
- **0001 tier policy:** the decision is bound to the request owner and the exact
  route; a route change re-evaluates it; the wire model may not change after the
  decision; an unavailable or failing required policy STOPS execution.
- **0002 required tool policy:** checked at every dispatch path after argument
  rewrites; a denial is observed exactly once; it stays binding after plugin
  unload until revoked; reload cannot revive old scopes. Cooperative
  enforcement, not an OS sandbox: an effect admitted before revocation may
  finish.
- **0003 completion fence:** a fenced completion is delivered only after the
  registered guard validates it, on live delivery and on orphan replay after a
  restart; a missing row never downgrades to legacy delivery.
- **0004 request observation:** without an observer the native code path is
  unchanged; with one, per-call hooks report request and response routes.
- **0005 close callbacks:** each callback runs once, outside the lock, before
  cleanup; a registration after close runs immediately; exceptions do not veto
  cleanup.
- **0006 diagnostic hooks:** projections are bounded and use only the gateway's
  current route identity; a missing plugin falls back to native output.
- **0007 task authority:** with no delegation identity set, nothing changes;
  with one, tools outside the capability allowlist are refused, writes must lie
  inside the owned write scope, and terminal use is refused (no OS sandbox is
  assumed).
- **0008 cron:** per-job `service_tier` is `normal|priority` only;
  `allow_fallbacks=false` disables every fallback chain; cron agents never load
  memory.
