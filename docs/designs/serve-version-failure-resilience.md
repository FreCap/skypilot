# SkyServe version failure resilience

## Problem

The `boltz-l4-fleet` rolling update exposed three independent availability
failures:

1. Version 39 was durably committed and elected, but every launch failed in
   `_get_resources_ports` because the authoritative persisted service spec had
   no `service.ports`. The same submission form had passed validation because
   every resource declared the same single port.
2. While that version was pending, the controller kept retrying the same
   deterministic configuration failure and kept the old version behind the
   pending-version scale-up fence. The old fleet remained healthy but could
   not respond to a capped queue.
3. A controller restart reconstructed demand-owned capacity from the latest
   version only. During a rolling update with no latest-version replicas, the
   aggregate target restarted at the minimum and climbed through configured
   scale-up waves even though the old fleet already proved a much larger live
   demand target.

The result was a growing queue, a stationary healthy fleet, and avoidable
request rejection. The controller must distinguish a bad candidate version
from transient infrastructure failure, retain a safe old runtime, and recover
its demand target from the whole active fleet.

## Behavior contract

### Deterministic preflight and quarantine

Every classified candidate-spec calculation required by the replica manager to
launch a replica runs before the manager's pending or applied-version state is
mutated. A failure that depends only on the immutable committed spec is a
deterministic preflight failure. The initial classified set includes:

- resolving the ingress port from `service.ports` or all resource ports;
- loading the authoritative task and validating logical replica capacity;
- constructing the candidate placement policy and exact accelerator catalog.

Network, database, cloud-capacity, and controller-ownership failures are not
deterministic and retain the existing retry behavior.

Autoscaler and load-balancer routing transitions are not part of the initial
quarantine classifier. They retain the existing retry behavior. Extending the
classifier to them requires a separate pure candidate-artifact boundary so an
error can be proven to occur before any replica-manager mutation.

When deterministic preflight fails, the controller atomically marks that
committed version quarantined with the reason and timestamp. The committed
spec and submitted YAML remain immutable. The controller then clears the
pending-version launch fence and keeps the prior applied runtime. It does not
retry the quarantined version. A later committed version supersedes it and is
evaluated independently.

Quarantine is durable in `version_specs` so controller replacement does not
retry the same bad version. Recovery selects the newest committed version that
is not quarantined. Provenance and status still report the highest committed
version, the applied version, the quarantined version, its reason, and its
timestamp. A quarantined version never becomes launch authority.

### Fail-open old-version capacity

The prior applied version is the only fail-open target. Quarantine converts the
failed candidate from pending rollout authority into durable rejected history,
so the healthy applied runtime may continue its ordinary autoscaling policy.
This is safe under queue pressure because:

- no stale version is chosen dynamically or reconstructed from replica rows;
- only the already applied runtime and its committed policy may launch;
- its existing `max_replicas`, exact-card, ownership, and capacity fences
  remain active;
- the quarantined candidate has no manager, autoscaler, or routing mutation to
  roll back;
- a new version can replace it without editing or deleting history.

This covers a capped queue without making queue saturation itself an authority
switch. Queue saturation is an observed reason to scale the active runtime,
not permission to select arbitrary historical configuration.

### Restart adoption from total ready demand-owned capacity

On the first fresh concurrency recompute after controller construction, the
aggregate target is seeded from ready demand-owned logical capacity across
every version, not only the latest version. Reserved-fill capacity and rows
already marked for logical retirement remain excluded. Provisioning rows are
also excluded from this restart seed: during a rolling update they overlap the
old ready fleet and summing both versions would turn replacement work into new
demand on every controller deploy. The manager still counts those rows for
duplicate-launch suppression.

The recovered total is bounded by `max_replicas`. It is a one-shot upward
safety floor, not a fresh demand sample and not permission to downscale. The
normal queue/concurrency signal, scale-up wave policy, and downscale hysteresis
take over after adoption. This prevents a live old fleet of 50 or 156 logical
slots from restarting at 1 or 10 while still avoiding paid-demand inference
from opportunistic fill.

This total-capacity adoption flag is armed only by autoscaler construction. An
ordinary in-process version update still resets the replacement target to its
cold baseline and enters through the configured scale-up wave.

### Exact-card actuation continuity and bounded replacement launches

Once an aggregate target is adopted, exact-card transition shaping must not
return an empty actuation map merely because the latest-version committed
capacity plus the current wave budget is below that already-held target. The
complete active fleet may provide transitional exact-card evidence, and the
actuation map is completed up to the adopted aggregate target. It remains
bounded by that target and `max_replicas`; it cannot create additional demand.

The complete exact-card map is a safety fence, not launch authority. A logical
scale-up decision carries a separate latest-version launch-capacity ceiling.
That ceiling is the latest version's already committed capacity plus the
current configured rollout wave. The replica manager checks the full aggregate
and exact-card generation fence, but stops launching when the separate ceiling
is reached. Subsequent ticks inside the wave cooldown retain the same ceiling;
the next elapsed wave advances it. Whole multi-GPU backends may round one wave
up by at most one backend width.

Configured waves therefore continue to limit replacement launches even when
restart adoption already holds a much larger aggregate target. They do not
revoke old-version capacity or weaken the aggregate retirement fence.

### Ingress-port compatibility

There is one shared ingress-port resolver. Pools return `-`. Services prefer an
explicit `service.ports`; otherwise every resource must declare exactly one
identical port. The server validation boundary writes that inferred port into
the committed service spec for new versions. The launch preflight also resolves
the resource form so older authoritative versions are repaired on read.

Ambiguous, missing, or inconsistent resource ports are deterministic preflight
errors and quarantine the candidate before runtime mutation.

## Data model

Add nullable `quarantined_at` and `quarantine_reason` columns to
`version_specs` in PostgreSQL migration 024. Null means the version remains
eligible for application. Setting quarantine is conditional on the exact
service and version and is idempotent. The migration does not rewrite existing
rows.

Read paths have distinct meanings:

- latest committed includes quarantined rows for provenance and monotonically
  increasing version allocation;
- latest applicable excludes quarantined rows for controller boot and respawn;
- version history includes quarantine metadata for the dashboard and API;
- an explicit version fetch remains available for immutable audit history.

SQLite receives the additive columns through the normal migration compatibility
path for local/controller tests, but the production central database contract
and verification target are PostgreSQL.

## Failure classification and atomicity

Only an explicit deterministic preflight exception triggers quarantine. Broad
exceptions continue retrying. Preflight is pure with respect to live runtime:
it may parse persisted data and construct candidate objects, but may not mutate
manager state, autoscaler state, replicas, routing publication, or applied
version.

Post-preflight provision and setup failures also remain retryable. Replica rows
do not yet persist a typed failure reason or immutable failure fingerprint, so
classifying those failures solely from a terminal replica status could
quarantine a healthy version during a transient provider or cluster outage.
Durable typed launch-failure evidence is a prerequisite for extending
quarantine into that phase.

The controller writes quarantine before clearing the pending version signal.
If that database write fails, it keeps the pending fence and retries, preserving
fail-closed behavior because the rejection is not yet durable. If a newer
version commits concurrently, the older quarantine remains valid history and
the reconciler immediately proceeds to the newer pending version.

## Alternatives rejected

- Retrying every exception forever cannot distinguish malformed immutable
  input from recoverable infrastructure and recreates the incident.
- Rolling back after partial manager mutation requires a second fallible state
  transition. Pure preflight makes rollback unnecessary for classified errors.
- Selecting any historical version when the queue is full makes queue pressure
  an unsafe deployment-authority mechanism. Keeping the already applied runtime
  is sufficient and auditable.
- Counting all logical rows on restart would convert opportunistic reserved fill
  into paid demand. Only demand-owned nonterminal capacity is adopted.
- Seeding from latest-version capacity alone repeats the rollout crawl whenever
  a controller restarts before the new version has replicas.

## Tests

Unit regressions cover:

- validation persists a common resource-level port;
- recovery resolves that port from an older authoritative spec;
- inconsistent resource ports fail before manager mutation;
- a deterministic preflight failure durably quarantines the exact version,
  clears its pending signal, retains the old applied version, and is not retried;
- a transient apply failure is not quarantined and still retries;
- a newer commit supersedes a quarantined version;
- controller recovery selects the newest non-quarantined committed version;
- version history and update status surface quarantine metadata;
- restart adoption uses total ready demand-owned capacity across old and latest
  versions while excluding reserved fill, retirement rows, and overlapping
  provisioning replacements;
- production wave settings do not reduce the recovered target from 50 to 10;
- exact-card shaping completes an already adopted target instead of returning
  an empty map, while the manager launches only the separately authorized
  latest-version wave;
- the rolling-update regression enables the production wave limiter.

Run:

```bash
pytest -q tests/unit_tests/test_serve_state.py
pytest -q tests/unit_tests/test_serve_controller.py
pytest -q tests/unit_tests/test_serve_controller_respawn.py
pytest -q tests/unit_tests/test_serve_replica_managers.py
pytest -q tests/unit_tests/test_spot_placer_hybrid.py
pytest -q tests/unit_tests/test_concurrency_autoscaler.py
pytest -q tests/unit_tests/test_serve_version_election.py
bash format.sh --files sky/serve/autoscalers.py \
  sky/serve/controller.py \
  sky/serve/replica_managers.py \
  sky/serve/serve_state.py \
  sky/serve/serve_utils.py \
  sky/serve/server/server.py \
  sky/utils/db/migration_utils.py \
  sky/schemas/db/serve_state/024_version_quarantine.py \
  tests/unit_tests/test_concurrency_autoscaler.py \
  tests/unit_tests/test_serve_controller.py \
  tests/unit_tests/test_serve_controller_respawn.py \
  tests/unit_tests/test_serve_replica_managers.py \
  tests/unit_tests/test_serve_state.py \
  tests/unit_tests/test_serve_version_election.py \
  tests/unit_tests/test_spot_placer_hybrid.py
git diff --check
```

## Rollout and manual verification

1. Apply migration 024 and verify both quarantine columns exist on the
   production PostgreSQL `version_specs` table.
2. Deploy the API/controller image using the existing Helm values.
3. Confirm the live malformed version 39 launches through the read-repair port
   path and reaches provisioning, rather than requiring quarantine.
4. In a test service, commit a deterministically invalid candidate and confirm
   status shows committed greater than applied plus quarantine metadata, the
   controller does not retry it, and the old service scales under queued load.
5. Restart a controller with old-version demand-owned capacity above one wave.
   Confirm its first logical target starts at total demand-owned capacity and
   does not descend to the minimum or first wave. Confirm the replacement
   version launches only one configured wave, rather than the full recovered
   target, and a second controller restart does not count pending replacements
   again as demand.
6. Confirm request queue depth and rejections fall as ready capacity arrives.
7. Confirm a valid later version applies normally after a quarantine.

Rollback the image if needed. The additive columns are harmless to the old
binary, committed specs are unchanged, and an old binary will ignore quarantine
metadata. If the old binary would retry a quarantined version after rollback,
deploy a valid superseding version before rolling back the controller image.
