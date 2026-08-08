# Durable SkyServe Replica Actions

Last updated: 2026-08-08

Status: the dedicated resource-action authority proposal is retired before
activation. PRs #1112, #1239, #1240, #1336, #1338, and #1343 are closed. PR
#1335 merged, but its dark V2 preflight and qualification layer is retired by
this cleanup. PR #1342 also merged after the retirement review began; its dark,
uncalled renderer and representability evidence was removed atomically rather
than left import-broken by the retirement of its authority dependencies. PR
#1333's forward-only Serve038/039 migrations are retained inert while its
uncalled runtime state layer is removed. The unexercised V2 authority contracts
merged by PR #1332 and the disabled PR #1232 activation surface were also
removed. PR #1340 merged the compatibility cleanup. PR #1346 merged as
`0b77ca77ae8b099c2de07566670743651744bbe2` and deletes the temporary disabled
Helm value, private-handler quarantine, private result codecs, and authority
routing. Its `boltz-test` deployment gates passed on 2026-08-08: the exact
compatibility artifact passed readiness, +10, and +30, and the sole `skypilot`
chart release in that cluster had its stored value scrubbed. The exact
final-removal artifact then passed `boltz-test` readiness, +10, and +30 at
04:04:08, 04:14:25, and 04:35:25 UTC, with every private-handler request absent
across all statuses.
Production reached compatibility-artifact readiness at 03:38:22 UTC and passed
its +10-minute gate at 03:49:03 UTC and +30-minute gate at 04:08:49 UTC. The
released plugin `claim_scope` API remains as an inert `GENERAL`-only
compatibility shim; its retired authority value is rejected and does not affect
queue selection.
No service was promoted through the proposed authority path, no authority
worker claimed a request, and no provider effect ran through that path. Source
cleanup is not operationally complete until the exact merged compatibility
artifact and this final-removal artifact are each deployed and pass their
monitoring gates below.

The combined HA latency fix through PRs #1367, #1369, and #1370 shipped as
release `1.1.1176` and passed the exact `boltz-test` readiness/+10/+30 window.
Its production +10 gate failed with one client timeout on each
`boltz-l4-fleet` slot while the service controller was under provisioning
pressure. Safety held, but completion is now blocked on the executor-isolation
correction and a fresh exact-artifact production readiness/+10/+30/+60 window
defined below.

The review found one smaller, independent correctness gap in the existing
Serve controller: an ordinary replica launch records its exact API request ID
only in process memory. A controller restart can therefore lose the association
and submit another launch request. The system-OOM recovery path already has a
bounded durable binding and adoption mechanism, but ordinary launches do not.
That gap does not justify a dedicated authority deployment, a native provider
renderer, a second execution topology, or a universal physical-capacity
kernel. A bounded fix may proceed only under the contract and evidence gates
in this document. Issue #1352 owns that telemetry-first follow-up; it is not an
R0 authority-retirement blocker.

## Decision record

The original 2026-07-30 request was an evaluation of whether a unified
physical-capacity convergence kernel had enough long-term payoff to justify
its migration risk. The evaluation concluded that the payoff was conditional
and required 30--60 days of production evidence across at least two domains.
The subsequent disabled deployment found no Serve services or replicas, no
independent provider-call audit source, empty capacity tables, and zero
projector database connections. The large payoff therefore remained a
hypothesis.

Subsequent user directions explicitly authorized implementation, testing,
deployment, removal of the old path, and later phases. The defect was not a
lack of authorization: implementation outran the original 30--60-day,
two-domain evidence gate and expanded into a dedicated authority stack before
it proved a complete admission-to-effect path. The stack had already reached
roughly 37,000 changed lines across 113 files at the original review, and later
dark merges expanded it further. It introduced separate authority workers,
cohort and lease protocols, private transport, native rendering, V2
representability inventories, and Serve038/039 state while the named legacy
mutation owners remained authoritative. The deployment evidence does not
justify that architecture, so it is rejected.

The accepted decision is:

1. retain the already shared and independently useful API request/action
   substrate and forward-only additive schemas;
2. remove uncalled V2 authority contracts and the authority-worker deployment,
   preflight, transport, packaging, and claim paths;
3. retain Serve038/039 as forward-only empty schema, but never activate or
   write its authority state and do not add another claimant/executor topology;
4. treat exact ordinary-launch request binding as a localized Serve recovery
   issue; and
5. require measured demand or an explicit correctness decision before that
   localized issue becomes an implementation project.

## Goals

- Preserve the exact request identity for an ordinary Serve launch across a
  controller restart.
- Adopt the same request after restart rather than submit an untracked second
  request.
- Keep launch intent, request binding, service identity, replica incarnation,
  and terminal projection mutually consistent.
- Preserve current launch/down ordering, provider-work limits, retry behavior,
  and public Serve semantics.
- Reuse the ordinary API request executor and existing internal launch path.
- Make ambiguity explicit and operator-visible instead of guessing success or
  absence.

## Non-goals

- A universal resource or physical-capacity state machine.
- A dedicated resource-action authority worker, private HTTPS control plane,
  cohort, policy rotation, or special execution lease.
- Reimplementing SkyPilot launch/down through a native Kubernetes renderer.
- Moving provider credentials or provider clients into a new component.
- Replacing the existing API request queue or ordinary executor.
- Changing pools, managed jobs, paid capacity, reserved fill, spot fallback,
  cost rebalance, placement failover, or logical replica accounting.
- Removing process-local fields that remain useful as caches before their
  durable replacements are proven in production.

## Current behavior and bounded gap

The current controller already persists replica-record identity and cleanup
intent. `replica_record_id` fences the ordinary replica row and prevents a stale
controller from deleting or routing a successor record. The Serve033
action-owned incarnation and generation columns remain null on the ordinary
legacy write path; they are not a current recovery fence and this design does
not pretend otherwise. Failed cleanup remains durable and is redriven after
restart; the process-local cleanup retry maps only preserve exponential-backoff
timing, so a restart may retry sooner but does not forget the cleanup.

Launch behavior is asymmetric:

- system-OOM recovery persists `launch_request_id` and `service_job_id`, then
  adopts that exact request after restart;
- ordinary launches publish the request ID only in
  `_LegacyReplicaMutationRuntime.replica_to_request_id`; and
- restart reconstruction of an ordinary `PENDING` or `PROVISIONING` replica
  can call `_launch_replica()` again without proving what happened to the old
  request.

Cluster-name idempotency may prevent a second provider resource in common
cases, but it is not a durable proof that the old request was adopted, canceled,
or terminal. The controller must not rely on that incidental behavior as its
recovery contract.

## Public contract

There is no new CLI, SDK, configuration, or provider interface. Existing
Serve behavior remains backward compatible.

If the bounded fix is authorized, an ordinary non-pool replica launch will have
the following internal contract:

1. A replica row has a stable record identity. The bounded implementation adds
   a neutral ordinary-launch generation or association identity; it does not
   silently reinterpret nullable Serve033 action columns.
2. Before execution can escape the ordinary API request boundary, the row is
   durably bound to one exact API request ID for that generation.
3. A restarted controller with the same row identity and generation adopts
   that request ID.
4. A controller may create a successor request only after exact request
   terminal/quiescence evidence for the predecessor is persisted.
5. Ordinary controller replacement transfers the association to the new
   controller by compare-and-swap and adopts the exact request. Cancellation
   targets that request only for supersession, teardown, or a failed handoff;
   losing an in-memory cache is never permission to cancel or replace it.
6. A same-name replica created later has a different record identity and cannot
   inherit the predecessor's request, result, absence proof, or cancellation.
7. Unclear request state becomes a durable operator condition and blocks
   another launch request until reconciled.

The API request remains the execution record. A second generic action DAG is
not introduced merely to wrap it.

## Architecture and invariants

### Ownership

- `ReplicaManager` owns the desired replica transition and stable replica
  record identity.
- A separate central PostgreSQL association record owns the neutral ordinary-
  launch identity and exact request binding.
- The existing API request queue and ordinary executor own request claim,
  execution generation, cancellation, and terminal result.
- Existing SkyPilot launch/down internals own provider selection and effects.
- The Serve reducer validates the row/request association before projecting a
  result.

No component may own both an unfenced stale replica snapshot and permission to
start provider I/O.

### Commit-before-effect

The bounded implementation adds a small internal reserve-bind-activate seam.
One PostgreSQL transaction inserts both the complete API request row and the
association without a queue row. After commit, queue activation is idempotent.
A controller or recovery sweep may repeat activation for a committed request;
it may never activate an absent/uncommitted request or leave a committed
nonterminal request permanently unqueued. The transaction must commit before
the request is eligible to execute. The seam must not recursively call the
public SDK from a new worker or render provider-native objects. A controller
uses it only after every eligible API target advertises the exact capability;
an old `/launch` endpoint would ignore unknown context and execute unbound, so
it is not a fallback.

The transaction compares at least:

- service name and service version;
- replica ID and immutable replica record ID;
- the new ordinary-launch association identity and generation;
- desired resource/configuration digest;
- initial controller owner and association-owner revision; and
- expected absence of a conflicting nonterminal binding.

Existing launch requests embed controller PID/IP preconditions that deliberately
fail after owner replacement. A bound launch cannot use that immutable owner
pair as its restart fence. The feature adds an association-ID precondition:
the new controller may compare-and-swap the association owner only while the
same service version, `replica_record_id`, desired launch generation, request
ID, and input digest remain current. The ordinary-bound executor resolves that
association and validates its current owner. The old controller cannot publish
or cancel after the owner revision changes.

### Pre-I/O fence

Immediately before provider I/O, the ordinary executor or internal handler
must revalidate:

- its live request claim and execution generation;
- the service and exact replica row still exist;
- the row still wants this launch generation;
- the row still points to this exact request ID; and
- the submitted input digest matches the durable binding; and
- the association owner/revision matches the current durable service-controller
  owner/revision.

A failed check terminates without provider I/O. A lost claim never becomes
permission for another effect.

### Result and retry

Success is projected only from the exact request result associated with the
same row and generation. Failure and retry policy use the database clock. An
unclear, nonterminal, or succeeded-but-unreduced request blocks automatic
resubmission and emits an operator-visible condition. This bounded design does
not add a cross-provider effect/absence model.

### Cleanup

The existing durable cleanup intent remains authoritative. This project does
not need a second cleanup action graph. Any later change to persist retry
deadlines must preserve current immediate restart redrive and be independently
justified by observed retry storms or provider throttling.

## Implementation phases

### R0: retire the unproven authority stack

- Revert PR #1332's V2 authority-only code and generated artifacts.
- Retain PR #1333's forward-only Serve038/039 migrations and schema catalog,
  but remove its uncalled authority/identity runtime state modules and tests.
- Remove design and removal-ledger claims for nonexistent transitional router
  symbols.
- Keep API005--008, global-user-state 028, Serve033, and other generic or
  forward-only foundations that have independent consumers.
- Remove PR #1232's dormant authority Deployment, bootstrap, claimant,
  preflight, network, native-renderer, provider-artifact, packaging, and Helm
  activation surfaces. Remove PR #1335's V2 preflight and qualification-policy
  additions and PR #1342's dark V2 renderer/representability island atomically
  with them. Do not reverse forward-only migrations.
- In the compatibility artifact, retain the four private handler names as a
  fail-closed quarantine. Ordinary executor and compatibility `all` queues must
  neither advertise nor claim them. This stacked final-removal change deletes
  the handlers, authority routing, queue exclusion, and codecs only after the
  all-status zero-request gate below is recorded. Retain the released plugin
  `claim_scope` parameter and enum as a `GENERAL`-only inert shim; explicitly
  reject the retired authority scope.
- In the compatibility artifact, retain the legacy
  `resourceActions.authorityWorker` Helm value shape so `--reuse-values`
  upgrades with `enabled: false` remain valid and reject `enabled: true` with a
  clear retired-feature error. Before this stacked change may merge, scrub the
  stored value from every release using the exact compatibility chart and
  image. The final chart omits the key from defaults and schema and keeps a
  narrow tombstone that rejects any stored `resourceActions` value instead of
  silently ignoring it.
- Deploy the exact merged cleanup artifact with existing Helm values and verify
  that no authority workload or provider effect is introduced. Preserve the HA
  rollout strategy; temporary CPU surge capacity follows the explicit capacity
  approval gate below.
- Keep this already-authored stacked final-removal PR in draft and blocked
  until stored Helm values are scrubbed and the compatibility artifact records
  zero matching private requests across all statuses at readiness, +10 minutes,
  and +30 minutes. Then merge and deploy the final artifact and repeat the same
  checkpoints before closing R0.

### R1: evidence gate

First ship a telemetry-only PR. It adds an append-only central-PostgreSQL
`serve_ordinary_launch_handoff_events` table with at least 60 days of retention;
the existing API requests are normally collected after 24 hours and cannot
support this gate alone. Each event uses the database clock and records a
closed event kind, service/version, `replica_record_id`, controller route epoch,
ordinary request ID when known, service job ID when known, and a redacted input
digest. It stores no provider payload or credential.

The closed event kinds cover request publication, controller-start observation
of a nonterminal replica, restart redrive, owner-loss cancellation, API
terminal result, Serve result projection, service-job observation, and cleanup
retry after a route-epoch change. Queries report:

- total eligible ordinary launches and controller restarts during nonterminal
  launch windows;
- replica records associated with more than one ordinary request ID before
  terminal projection;
- restart redrives whose predecessor request was still active or terminal but
  unreduced;
- duplicate service-job submissions for one replica record;
- owner-loss cancellations; and
- cleanup retries whose process-local backoff reset after controller restart.

Observe those queries for 30--60 days of eligible production traffic, or record
an explicit product correctness decision that the restart gap must close
regardless of volume. The telemetry writer is diagnostic only: it cannot delay,
cancel, authorize, retry, or project a launch.

If there are no eligible launches and no correctness mandate, stop. The design
remains a documented limitation and no runtime is added.

### R2: bounded binding and adoption

If R1 authorizes work:

- generalize the proven request-binding/adoption seam from system-OOM recovery
  to ordinary launches;
- add one neutral central-PostgreSQL association table keyed by service,
  `replica_record_id`, and ordinary-launch generation; do not reuse the system-
  recovery `launch_request_id` or action-only Serve033 columns;
- add an API-instance capability bit for atomic reserve-bind-activate and keep
  the legacy path until every eligible API target advertises it;
- replace immutable PID/IP validation only for bound launches with the
  association-ID/current-owner fence and a compare-and-swap controller handoff;
- add the pre-I/O association check to the ordinary executor path;
- persist explicit ambiguity instead of resubmitting;
- retain the in-memory request map only as an optimization; and
- add crash tests at intent commit, request binding, claim, pre-I/O, result,
  and projection boundaries.

The crash matrix includes timeout before transaction commit, committed request
and association before queue activation, repeated activation, and recovery of
a committed nonterminal request with no queue row. Every case proves no orphan
nonterminal request and at most one queue entry.

This phase must be one focused feature PR. If it temporarily preserves an old
fallback, the removal PR is created at the same time as a blocked stacked PR.

### R3: rollout and removal

Deploy dark/read-only validation first, then one eligible non-pool service.
Remove the old resubmission inference only after the exact merged artifact has
completed the monitoring gate. Pools and excluded profiles remain unchanged.

## Deployment and rollback

R0 is a code cleanup over retained additive forward-only schemas. Before the
compatibility upgrade, prove no request in any status uses any of the four
private handlers
`serve_shadow_candidate_launch`, `serve_shadow_candidate_down`,
`serve_resource_action_launch`, or `serve_resource_action_down`. Prove all
generic action/attempt, shadow, cohort/reference/coverage, release, and
authority-history relations are empty. The six Serve038 relations
`serve_resource_action_authority_policy_epochs`,
`serve_resource_action_worker_registration_leases`,
`serve_resource_action_worker_registration_handoffs`,
`serve_resource_action_worker_registration_cold_recoveries`,
`serve_resource_action_crash_canary_runs`, and
`serve_resource_action_attempt_exhaustions` must be empty. The nine Serve039
relations `serve_resource_action_execution_authority_lineage`,
`serve_resource_action_attempt_terminal_authority`,
`serve_resource_action_shadow_request_terminal_history`,
`serve_resource_action_shadow_admission_fallback_history`,
`serve_resource_action_shadow_admission_fallback_progress_log`,
`serve_resource_action_shadow_settlement_history`,
`serve_resource_action_shadow_execution_history`,
`serve_resource_action_worker_process_supersessions`, and
`serve_resource_action_api_instance_gc_cursors` must also be empty.

All nullable Serve038 candidate/identity columns on services, version specs,
replicas, worker cohorts, and worker-cohort references must be null. The shadow
coverage table must itself be empty because its Serve038 candidate columns are
non-nullable. Run the same assertions after rollout. This stacked cleanup must
remain draft if any private-handler row exists in any status; deleting its
decoder or queue quarantine is forbidden if any assertion fails.

Deploy the exact merged image as one compatible Helm rollout. The `boltz-test`
release explicitly pins `apiService.image`, `controllerService.image`, and
`executorService.image`, so its `helm upgrade --reuse-values` must set all three
to the same immutable digest; updating only the API value would leave mixed old
controller/executor images. Production stores only `apiService.image`; its
controller and executor image values are null and inherit the API value. Its
ordinary upgrades must use `--reuse-values`, override only the API image, and
require the exact digest in all three client-rendered chart positions. A stored
`resourceActions.authorityWorker.enabled: false` value remains schema-valid and
renders no authority resources. A stored `enabled: true` value fails the
upgrade with `resourceActions.authorityWorker.enabled=true is no longer
supported; the dedicated resource-action authority worker has been retired`.
Verify no authority Deployment, Service, ServiceAccount, ConfigMap, or Pod
exists. Rollback is an application-image rollback of all three roles only. Do
not downgrade PostgreSQL migrations.

After the compatibility artifact passes readiness, +10-minute, and +30-minute
checks, scrub the retired value from every Helm release before merging this
stacked cleanup. Export each release's complete user values as JSON, remove
`.resourceActions.authorityWorker`, remove `.resourceActions` too when it is
then empty, and compare complete client-side renders of the original and
sanitized values. Upgrade the same compatibility chart and image with
`--reset-values` and the complete sanitized values file. Preserve each
release's existing image-inheritance topology, set its explicit image value or
values to the immutable digest, and require that digest in all three rendered
positions. Do not use server-side Helm dry-run, combine `--reset-values` with
`--reuse-values`, use a null override, or use `--atomic`; the migrations remain
forward-only.
Verify `helm get values` contains no retired key.

Only then may this final-removal change merge. PR #1346 instead merged through a
concurrent workspace action at 03:49:52 UTC, after production +10 but before
production +30 and before the two adversarial-review design corrections were
committed. This is a process-contract departure, not evidence that the gate was
waived. No final-removal artifact had been deployed at merge time. A second
concurrent action later deployed it to `boltz-test` before the canonical
corrections landed, as recorded below; at that point, production promotion still
required the corrected design and completed test monitoring.

PR #1350 merged at 04:35:28 UTC, but its exact-capacity-approval gate remained
open and it did not yet contain the zero-capacity exception or the reproducible
60-minute comparison below. A third concurrent action started production
revision 369 at 04:36:18 UTC. The zero-capacity record was authored only at
04:38:37 UTC in an unmerged local follow-up; later commit and rebase timestamps
are not rollout gates. This is a third process-contract departure: the later
empirical zero-capacity proof and monitoring contract do not retroactively
satisfy design-first ordering. Kubernetes had already accepted the rollout
when detected, so no competing rollback or replay was issued; every
post-deployment gate remains binding.

The final chart has no
`resourceActions.authorityWorker` schema or enabled-value guard, and its
request registry intentionally has no private handler, authority claim routing,
codec, or ordinary-queue exclusion. A narrow value tombstone rejects an
unscrubbed release, and the released plugin API retains only an inert
`GENERAL` claim scope. Deploy its exact immutable image to all three roles,
run the retained migrations forward, and repeat readiness, +10-minute, and
+30-minute checks. Production additionally requires the issue-#1349-aware
+60-minute comparison below. A retired-state write, schema/head mismatch,
split-brain, unintended failover, health loss, restart, or source-attributed
regression stops the rollout and restores production revision 368's exact
`1.1.1159` compatibility chart
`sha256:07ed313fc8f7e80ea1aaa82f0a2eb0163b0cc9827e46ed0e5c72cb7d4048d6c6`
and image
`sha256:d4237ec47a2e74d58b93a312157b58cf9066ec134bcce262681ac356087dd4b5`.
Rollback changes application images/chart only and never downgrades the
database. Exceeding only a pre-existing #1349 comparison limit holds R0 open
for attribution; it does not trigger an automatic rollback to an artifact that
already exhibited that signal. An unexplained warning or error also holds R0
open, and requires rollback if investigation connects it to the new artifact.

R2, if authorized, starts with binding writes disabled or validation-only.
Rollback disables new admission and waits for every bound request to become
terminal and projected before restoring an API or controller image without the
capability. A rollback must not clear associations, change replica record IDs,
or allow a predecessor request to race a successor.

No canary that creates provider capacity is authorized by this design alone.
Before such a canary, record the logical GPU slots, physical instance shape and
count, region, duration, market/reservation class, and incremental cost, and
obtain explicit management approval.

## Verification and monitoring

R0 completion requires both the compatibility and final-removal deployments:

- focused unit tests for the retained generic action substrate and removal
  checker;
- no registered private handler, private return codec, authority claim routing,
  or queue exclusion in the final artifact; the released `GENERAL`-only plugin
  shim remains inert;
- exact merged SHA and immutable image digest for each rollout;
- staged `boltz-test` rollout with preserved Helm values;
- production promotion only after the corresponding `boltz-test` evidence
  closes;
- all ordinary control-plane Pods ready with zero new crash loops;
- authority worker disabled and absent;
- no unexpected action, policy, cohort, handoff, or authority rows created;
- ordinary API request and Serve reconciliation health unchanged; and
- start, 10-minute, and 30-minute post-readiness checkpoints recorded in the
  relevant PR with identical empty authority state and no new error/restart
  trend.

Because production issue #1349 overlaps the ordinary Serve-health signal, the
final-removal production rollout also requires an exact 60-minute window from
the first instant that all 17 API/load-balancer workloads are Ready on the
final digest. All 16 slots must remain Ready, `STABLE`, synced, non-draining,
and converged, with exactly one ACTIVE and one STANDBY slot per service. The
window permits zero restart, split-brain, unintended-failover, or health-loss
events, and no role-sync failure interval may reach 60 seconds.

For the exact `boltz-l4-fleet` service pair, use the persisted controller logs
for its current service incarnation, including rotated `controller.log*`
segments. Deduplicate identical access lines and restrict them to the exact
window. Sort the completion timestamps of
`POST /controller/load_balancer_role`; the gap numerator is the number of
adjacent completion intervals at least eight seconds, and the rate is that
count divided by window hours. It must not exceed 24.23/hour (therefore at most
24 in the 60-minute window), which is 125% of the revision-366 baseline of 21
gaps in 65 minutes. For `POST /controller/load_balancer_sync`, divide access
lines with status 503 by all access lines for that path. That controller-side
rate must not exceed 3.44%, which is 125% of the revision-366 baseline of five
503s in 182 attempts. Do not add a load-balancer proxy log to either the
controller numerator or denominator. Enumerate it separately; a proxy-side 503
without a controller-side 503 in the same sync cycle is unexplained and blocks
closure, while a correlated line is classified once and disclosed.

This exact method reproduces the revision-366 baseline as 21 gaps in 3,900
seconds with 2,190 role completions, all HTTP 200, and five sync 503s in 182
attempts. The validated final-artifact +10 window had six gaps in 600 seconds,
328 role completions all HTTP 200, and zero sync 503s in 24 attempts. The
interim rate is above the threshold, but the contractual decision is the exact
60-minute numerator and denominator.

At +60, query `/_lb/capacity` on both exact `boltz-l4-fleet` Pods. On each slot,
`ha_observability.role.total_seconds.p99_recent` must be at most 10.32 seconds,
controller `total_seconds.p99_recent` at most 9.75 seconds, lock-wait maximum at
most 8.74 seconds, lock-hold p99 at most 9.64 seconds, pod-authority maximum at
most 9.39 seconds, and Service-routing-read maximum at most 8.75 seconds. These
are 125% of the worst recorded revision-368 values of respectively 8.25, 7.80,
6.99, 7.71, 7.51, and 7.00 seconds. Each `p99_recent` is the last at most 256
observations at the snapshot; each `max` spans the current process lifetime and
therefore includes startup. These process-local measures supplement rather
than replace the exact access-log window. The eight-second gap is also the
configured client deadline.

Enumerate every application WARN/WARNING, ERROR, CRITICAL, traceback, FATAL,
and PANIC line and every Kubernetes Warning event in the exact window. The only
pre-classified application signature is #1349's recovered
`HA role heartbeat failed; retaining role ... TimeoutError`; it is acceptable
for this cleanup-only comparison only when all safety and numeric limits above
pass. Any other signature is unexplained and blocks R0 until attributed.

This cleanup-only attribution gate is intentionally less strict than the
existing real-cluster HA `observe` qualification, which permits zero
`client_timeout` outcomes and caps recovered role-channel failure at 15
seconds. Passing it establishes that the source cleanup did not worsen the
pre-existing signal; it does not satisfy the HA qualification, change its SLO,
or close #1349. That issue remains the owner of eliminating the timeouts and
qualifying the large-fleet topology under the stricter contract.

### `boltz-test` compatibility deployment evidence (2026-08-08)

PR #1340 merged as `66de423064d01b7e0fbeaf552804bd55236d00f6`.
Its exact chart is `1.1.1159` with OCI digest
`sha256:07ed313fc8f7e80ea1aaa82f0a2eb0163b0cc9827e46ed0e5c72cb7d4048d6c6`;
all three roles use image digest
`sha256:900c539a4c70264bd6f978bc463be665a57a08d6029552c70dac5b6ba56beb2f`.
The monitored workload rollout was Helm revision 93; revision 94 later applied
the stored-value scrub without changing the workload.

The attempted `helm upgrade --dry-run=server` was not read-only in this
environment: it persisted a release revision, executed its migration hook,
patched Deployments, and requested surge capacity. Do not use server-side Helm
dry-run for this release. Interrupted revisions were stopped without a schema
downgrade; their pending release records were checksum-backed up before
removal. All forward heads remain API008, Serve039, state028, and capacity001.

Readiness at 03:00:25 UTC, +10 at 03:11:31 UTC, and +30 at 03:31:38 UTC passed
with all six role Pods on the exact commit and image, zero restarts, no
post-readiness Warning events, no authority objects, every private-handler/all
retained authority table count zero, and every candidate nullable-column count
zero. Recovery and convergence launched nine transient CPU-only Spot
instances, eight of which terminated; the remaining one replaced a consolidated
baseline node. No on-demand instance ran, and the NodePool returned to its
captured 10-node / 80-vCPU baseline at 03:02:23 UTC.

The `boltz-test` cluster has one `skypilot` chart release. Its original and
sanitized values rendered byte-identically with the exact compatibility chart.
Revision 94 applied the sanitized complete values at 03:32 UTC with
`--reset-values`, the same three image digests, and no `--reuse-values` or
`--atomic`. Migration job 94 succeeded, all six Pod names and creation
timestamps remained unchanged, all Deployment generations remained observed
and 2/2, the database zero-state remained unchanged, and `helm get values` now
contains no `resourceActions` key.

### Production compatibility deployment evidence (2026-08-08)

Production Helm revision 368 deployed the exact `1.1.1159` compatibility chart
and central image digest
`sha256:d4237ec47a2e74d58b93a312157b58cf9066ec134bcce262681ac356087dd4b5`.
Readiness at 03:38:22 UTC, +10 at 03:49:03 UTC, and +30 at 04:08:49 UTC passed.
The combined-role API Pod remained Ready with zero restarts, all 16 warm-standby
load-balancer Pods were Ready on the same digest with zero total restarts, and
the full 31-minute log scan found no ERROR, CRITICAL, or traceback signature.
The drained old-role heartbeat aged out by +10; exactly one current `all`
heartbeat was ready and authority heartbeats remained zero.

Production stored values contain no `resourceActions` key, no authority object
exists, and all heads remain API008, Serve039, state028, and capacity001. Every
private-handler request across all statuses, gated relation, and gated nullable
column remained zero or null.

The severe-signature scan through +30 was empty, but the interval was not
warning-free. Across the two `boltz-l4-fleet` slots, 22 HA role-heartbeat
attempts logged an asyncio `TimeoutError`: 10 while retaining ACTIVE and 12
while retaining STANDBY. They occurred in clusters at 03:41:15--03:43:15,
03:54:36--03:56:52, 03:59:41--04:01:27, and 04:04:36--04:04:38 UTC. Both slots
remained Ready with zero restarts, retained safe roles, and continued returning
healthy liveness responses; the other 14 load balancers and API logged no
application warning.

The extended audit through 04:25:04 UTC counted 26 matching warnings, 12 while
retaining ACTIVE and 14 while retaining STANDBY. Post-+30 recurrences were
ACTIVE at 04:14:46 and 04:19:30 and STANDBY at 04:16:15 and 04:19:32. The ACTIVE
slot also logged controller-sync HTTP 503 failures at 04:14:20, 04:19:39, and
04:23:39, each with one ERROR and one traceback line. Readiness, roles, and zero
restarts were unchanged. These signatures are disclosed explicitly; the
post-+30 interval was not a zero-severity quiet window.

Independent pre-change evidence closes attribution to this cleanup. The
persisted controller access log on production revision 366 had 21 role-response
gaps of at least eight seconds from 02:25--03:30 UTC, compared with 19 from
03:40--04:25 on revision 368. The corresponding windows had respectively five
controller-sync 503s in 182 attempts and two controller-side 503s in 155
attempts, plus the one post-change proxy-side 503 above. The exact diff from
revision 366 commit `5eb15b544e6fdb5bf43853b5e753d6e24cf4515e` to compatibility
merge `66de423064d01b7e0fbeaf552804bd55236d00f6` is a broad authority
cleanup spanning 122 files; attribution does not depend on characterizing that
whole diff as small. The executable heartbeat-path comparison leaves
`load_balancer.py`, `lb_k8s.py`, `controller_proxy.py`, `lb_ha.py`, and
`lb_ha_observability.py` unchanged. The diff changes 33 other
`sky/serve` files, but inspection of those deltas shows deletion or
disconnection of retired `resource_action*` modules, arguments, state helpers,
and preflight token functions. In the adjacent `constants.py` and
`controller.py`, it deletes retired-authority constants and a startup-only
token-isolation check; it does not change the functions serving role, proxy,
Kubernetes-authority, routing, or sync traffic. Bounded runtime observations
locate the latency in pre-existing serialized Kubernetes reads: pod-authority
and Service-routing reads reached 6.25--7.51 seconds and role-lock wait reached
6.99 seconds against an eight-second client budget. Issue #1349 owns that
separate performance defect. The evidence does not support treating it as a
#1340 or #1346 regression, so it no longer blocks the authority-cleanup
production promotion.

### HA role latency fix-forward contract (2026-08-08)

The first two bounded #1349 fixes preserved safety but did not meet the exact
latency gate. Production revision 370 shared the Pod and Service reads, yet the
two large-fleet slots each added 14 clean-window `client_timeout` outcomes.
Revision 371 parallelized the independent Pod, Service, and first Deployment
reads. After clean T0 at 07:48:28 UTC, both slots timed out again at 07:52:20
UTC. One validated Kubernetes snapshot took 5.53 seconds while its peer waited
7.40 seconds on the per-service role lock; end-to-end role time reached 8.999
seconds. All eight pairs retained one ACTIVE and one STANDBY, stayed synced and
non-draining, and all 17 Pods remained Ready with zero restarts. The source is
therefore serialized read-side head-of-line blocking, not provider capacity or
an authority-fence failure.

The next fix-forward removes that blocking only for a validated STABLE
read-only snapshot:

- Read the controller fence, durable cutover state, and fail-closed Kubernetes
  role snapshot before acquiring the per-service transition lock. Concurrent
  slot heartbeats may overlap these independent reads.
- Enter the existing role lock and re-read both the controller fence and the
  complete frozen cutover state. Use the prefetched snapshot only when both are
  byte-for-byte equal and the phase remains STABLE; otherwise return the
  existing fail-closed `cutover_state_unavailable` outcome and retry from a new
  snapshot.
- Keep every MIGRATING, ROLLING_BACK, PREPARING, DRAINING, planned promotion,
  selector patch, database transition, session-ledger update, and drain-view
  publication under the existing lock. No prefetched observation can cross a
  durable transition.
- For the read-only snapshot, use the Service's exact API Deployment
  ownerReference as the expected identity, then perform one live Deployment UID
  read after the Service and require equality. This preserves the prior final
  replacement linearization point while removing the redundant earlier GET.
  Mutation callers retain their existing two-read owner fence.

Focused tests must prove that two 143-backend STABLE slot heartbeats overlap
their snapshot reads, then serialize only the short exact fence/state
revalidation and decision tail. They must also prove that any fence or state
change rejects the prefetch, that non-STABLE phases never prefetch, that
malformed/replaced owners fail closed, and that transition mutation ordering is
unchanged. Completion still requires one immutable exact-merge artifact, direct
Helm staging and production rollouts on the existing fixed capacity, and a fresh
readiness/+10/+30/+60 production window with zero `client_timeout` delta,
recovered failures at most 15 seconds, no role/controller/phase observation in
the eight-second bucket, and every safety, health, schema, state, event, and log
gate passing.

PR #1362 implemented the overlap and under-lock revalidation portions of that
contract, while retaining the historical two-read Deployment owner helper. It
merged as
`0d6bd802bb32e2c35a3af7469e8968f4d39ea4b0`. Release `1.1.1171` and source
image digest
`sha256:830a2e317fcb9a9b80d39bc74046ca00b79925169dbf611db173999db8390343`
point exactly to the merge. Its fresh `boltz-test` readiness/+10/+30 window
passed with six exact Ready workloads, zero restarts, and no On-Demand node.
Production direct-Helm revision 372 reached a clean exact readiness baseline at
09:09:50 UTC with all 17 workloads Ready, zero restarts, all eight pairs
`STABLE` and converged, and the same three fixed `m6i.8xlarge` nodes.

The production +10 exact-behavior gate nevertheless failed. Each
`boltz-l4-fleet` slot added 21 `client_timeout` outcomes and ended with an
active failure streak; each `boltz-l4-fleet-test` slot added one recovered
timeout. The change did remove the targeted cross-slot contention: large-fleet
role-lock wait p99 fell to 0.39/0.58 seconds and lock-hold p99 to 0.45/1.40
seconds. The remaining path is one slow individual Kubernetes snapshot plus
duplicated SQL and proxy fences. Kubernetes snapshot p99 remained 6.43/6.44
seconds, controller p99 7.51/7.71 seconds, and end-to-end role p99 entered the
8.998-second bucket. Safety, state, health, fixed capacity, and zero-restart
invariants stayed intact, so this is a fix-forward latency failure rather than
a rollback trigger.

PR #1368 adds the complementary bounded read collapse without changing the
eight-second client budget or six-second independent report-freshness gate:

- One PostgreSQL query returns the exact controller owner/incarnation tuple and
  the complete durable cutover state, including drain start. The pre-lock read
  supplies the fence, frozen state, and resource scope used by the Kubernetes
  snapshot, replacing the prior fence query, cutover-state query, and snapshot
  owner query. No authority value is cached.
- Under the existing role lock, repeat that same complete query and require the
  entire owner/fence/state record to equal the pre-lock record before the
  Kubernetes result may affect the session ledger or role decision. Non-STABLE
  and mutation paths use the record read under the lock and retain all existing
  transition serialization.
- On both reads, derive the owner fingerprint from the live service hash,
  controller PID, normalized IP, and controller port and require it to equal
  the immutable fingerprint with which this controller child booted. Matching
  only PID/IP is insufficient: a controller restart may reuse them while
  changing its port, and a service incarnation change must also fence the old
  child.
- After that exact under-lock owner proof, the controller may attach its
  existing owner fingerprint to the role response. The stable API proxy accepts
  the attestation only when it exactly equals the owner fingerprint read before
  routing the request; it then omits its redundant post-response owner query.
  A missing attestation, as during a mixed-version rollout, retains the current
  post-response owner read. A mismatched attestation fails closed. Every
  non-role controller route retains both proxy owner reads.
- The attestation moves the successful role response's last owner
  linearization point from the proxy to the controller's immediately preceding
  complete under-lock row read. It does not extend a TTL, trust a client value,
  weaken controller-request authentication, accept a stale cutover state, or
  permit a transition outside the lock.

The first merged #1368 implementation compared only PID/IP to the controller's
bootstrap owner before returning the old bootstrap fingerprint. Pre-production
adversarial review rejected that implementation: if the database hash or port
changed after the proxy's first read but before controller prefetch while
PID/IP were reused, the old child could attest the stale proxy fingerprint and
incorrectly suppress the final proxy read. Release `1.1.1174` reached only
`boltz-test`; its interrupted qualification is not promotion evidence. The
fix-forward must validate the complete live fingerprint on both controller
database reads and fail closed before any snapshot or attestation when hash,
PID, IP, or port differs.

Focused tests must prove one SQL read before and one after the Kubernetes
snapshot, byte-for-byte owner/state mismatch rejection, no snapshot-side SQL
owner read, pre-prefetch rejection for every bootstrap fingerprint component,
exact controller attestation, mixed-version proxy fallback,
mismatched-attestation rejection, and unchanged two-read behavior for every
other proxy route. The 143-backend overlap, owner-replacement, transition, and
full external-load-balancer suites remain mandatory. The immutable follow-up
must repeat the same direct-Helm staging and production qualification; revisions
372 and the interrupted `1.1.1174` staging window do not satisfy completion.
PR #1367 subsequently addressed the cross-slot provider amplification exposed
by the same production evidence. It merged as
`34822adbbd56d946cd21c70eebf4aa11cb8dc8ac` and release `1.1.1173`, but was not
deployed independently before the complete read-collapse change was ready.

Revision 372's remaining amplification is between the two slot heartbeats.
After #1362 they independently execute the same fail-closed Pod, Service, and
final live Deployment-owner reads for the same immutable PostgreSQL fence and
cutover state. Under provider pressure that doubles identical Kubernetes API
traffic. Moving the final live Deployment UID read earlier is not acceptable:
it would widen the replacement race by making the owner check cease to be the
last Kubernetes snapshot observation.

PR #1367 coalesces only an identical snapshot while it is actively running:

- key the in-flight task by the complete immutable controller owner/fence row
  and frozen cutover state, and share it only for validated STABLE requests
  with the exact same key;
- remove the task immediately when it completes, with an identity-checked
  callback so an older task cannot clear a newer different-key task. There is
  no TTL, completed-result reuse, cache, or stale authority window;
- shield the shared task from individual request cancellation so one timed-out
  slot cannot cancel work already awaited by its peer. Snapshot success,
  bounded failure, and subphase timings are deterministic for every waiter;
- keep the Service-then-live-Deployment ordering and final owner
  linearization inside `get_lb_role_snapshot` unchanged; and
- independently re-read and compare the exact complete owner/fence/cutover row
  under the role lock for every request before its own session-ledger update or
  role decision. Different keys start independent tasks, and non-STABLE states
  keep the existing locked path.

Focused tests must prove that two concurrent exact-key STABLE heartbeats make
one provider snapshot call but retain two independent fenced decisions, that a
different fence or state is never shared, that cancelling one waiter does not
poison its peer, that shared errors retain their deterministic fail-closed
outcome, and that non-STABLE behavior is unchanged. The same immutable staging
and fresh production readiness/+10/+30/+60 gates remain mandatory.

PR #1369 completes the remaining read-only Kubernetes owner collapse. For HA
Pod authority, the already-read Service supplies the exact API Deployment
ownerReference. The helper validates its kind, name, non-empty UID, service
incarnation label, and resource version, then performs one live Deployment UID
read after the Service and requires equality. This retains the prior final
replacement linearization point while removing the redundant pre-read.
Mutation helpers keep both reads because they must construct a new desired
ownerReference. Focused tests require exactly one live Deployment read on the
HA authority path and fail-closed behavior when the Deployment is replaced.
The immutable rollout artifact must include #1369 together with #1367, #1368,
and the full controller-owner attestation correction above.

### HA role executor-isolation correction (2026-08-08)

Release `1.1.1176` points exactly to merge
`54184f7c7046d1113077f61232045d5e8fe4d6d7`. Its source image digest is
`sha256:68b9869f4fcc7ae8fa752443b98ed779d827c5a6d1e734bc849b58bd49617cbc`;
its chart digest is
`sha256:04288f5d76edaf4658a6d0204667f27cba6f6ba61c3b6a0ef9f526d62600259b`.
Direct-Helm `boltz-test` revision 104 passed readiness, +10, and +30 with the
same six Pod UIDs, zero restarts, no new Warning or high-signal log event,
healthy exact-version endpoints, empty retired state, and Spot-only placement.

Production direct-Helm revision 373 deployed the same exact chart and image on
the unchanged three fixed `m6i.8xlarge` nodes. A concurrent operator then
created revision 374 with the same chart, image, values, and capacity; no API
or LB Pod UID changed and no Pod restarted. The qualification clock was
therefore restarted from a clean revision-374 T0 at 11:57:52 UTC.

The production +10 checkpoint at 12:08:06 UTC failed the exact behavior gate.
Each `boltz-l4-fleet` slot added one `client_timeout` outcome. The clients
timed out at 12:06:15/12:06:16 UTC and recovered in 4.974/4.738 seconds. The
controller returned those requests with HTTP 200 only at
12:06:20.746/12:06:20.818 UTC, after the previous pair had completed at
12:06:05.350/12:06:05.411 UTC. Other services' role requests continued to
complete throughout that interval. All 17 exact-digest API/LB Pods remained
Ready with zero restarts, all eight pairs retained the correct ACTIVE/STANDBY
roles and complete owner/cutover state, the schema and retired-state gates
remained clean, no Warning event was added, and fixed capacity was unchanged.
This is another fix-forward latency failure, not a rollback trigger.

The failing Kubernetes selector names one service incarnation and returned
exactly its two LB Pods, so neither an unbounded Pod payload nor cross-service
API-server failure explains this service-local stall. The live controller log
shows fleet-wide sync and readiness-probe rounds over roughly 89 backends in
the same process around the failure. Unrelated service controllers remained
responsive. The role path currently submits both PostgreSQL authority reads
and the shared Kubernetes snapshot to the controller event loop's default
executor; fleet sync uses that same executor, while readiness probes use a
separate high-fan-out pool in the same process. The cancelled requests did not
publish their phase traces, so this evidence does not distinguish default-pool
queue wait from PostgreSQL, Kubernetes, or process-wide scheduling delay. It
does prove that the safety-critical role path retains an avoidable shared
executor dependency and needs queue-delay telemetry. It does not justify
weakening an owner fence, increasing the eight-second deadline, or reusing a
completed authority snapshot.

The bounded correction is:

- Every HA controller owns one two-worker role executor. All blocking work
  entered by `_handle_load_balancer_role`, including the pre-lock and
  under-lock PostgreSQL reads, the shared STABLE Kubernetes snapshot, and
  serialized transition operations, runs on this executor. Ordinary
  provisioning, autoscaling, sync, and unrelated controller work remain on the
  default executor and cannot occupy or queue ahead on the role executor. This
  does not claim to isolate PostgreSQL capacity, Kubernetes latency, the GIL,
  or the readiness-probe pool; the exact rollout gate remains authoritative.
- The executor changes only scheduling isolation. The complete owner/fence and
  cutover record is still read before the Kubernetes snapshot and compared
  byte-for-byte with a second read under the role lock. The exact Service then
  live-Deployment owner ordering, in-flight-only snapshot sharing, session
  ledger, state machine, demand lock, and every mutation fence remain
  unchanged. There is no TTL, completed-result cache, fallback authority, or
  added capacity.
- Two workers are sufficient for the two independently arriving slot
  pre-reads while the identical STABLE provider snapshot remains coalesced.
  The pool is fixed-size, created only for HA controllers, and shut down by the
  controller lifespan. Executor queue delay is exposed separately from the
  blocking operation's total latency so a future scheduling regression is
  attributable.

Focused tests must saturate a one-worker default executor, prove it is still
blocked, and require a real HA role request to return the exact intended role
through the dedicated executor. Existing exact-key sharing, cancellation,
owner replacement, complete-row mismatch, non-STABLE transition, and all
external-load-balancer suites remain mandatory. Completion requires a new
immutable exact-merge artifact, direct-Helm staging readiness/+10/+30, and a
fresh direct-Helm production readiness/+10/+30/+60 window with zero incremental
`client_timeout`, no role/controller/phase observation in the eight-second
bucket, recovery at most 15 seconds, unchanged fixed capacity, and every
safety, state, schema, event, log, health, and restart gate passing.

Local pre-PR verification passed on 2026-08-08. The selected 21-file HA,
external-load-balancer, controller-proxy, controller-event-loop, and controller
hard-exit suite passed with exit code zero (493 tests before the final
lifecycle test was added). A focused rerun then passed both the default-pool
saturation behavior test and the executor-lifespan shutdown test, bringing the
distinct selected coverage to 494 tests. The repository's configured mypy set
passed for 884 source files, Pylint rated all three changed Python files
10.00/10, and `git diff --check` passed. CI and both exact-artifact deployment
windows remain open gates.

### Final-removal artifact evidence (2026-08-08)

Although it merged before the production compatibility gate closed, PR #1346's
exact code head `7a5315d577b54c1ba970991d3ca974b5fbee797c` passed all 32 CI
checks, and the adversarial code and migration review found no implementation
blocker. Merge `0b77ca77ae8b099c2de07566670743651744bbe2` published release
`1.1.1161`. The source image digest is
`sha256:310effb333ad0808b4289f05ee46ac89ea21b156b6e54e5df5e47bbe8198e002`;
the chart OCI digest is
`sha256:4bc611db6048419dfd296bf4d82d9542f9a0bb599e54febb5520bcc79b2bf799`.
The chart metadata records the exact merge and version.

The same image was mirrored into the existing `boltz-test` registry as
`sha256:b780e6b7c7fcc2606baed83ce06dc2f12a6913db13e01d615d2fcdce48d15eb6`.
Registry normalization changed compressed-layer and manifest digests, while the
image configuration digest remained exactly
`sha256:b31d9b0414aa61fa7b0183d58e5155ddc079838e312c701215e59a059c94543f`.
The local image identity is linux/amd64, version `1.1.1161`, and exact merge
`0b77ca77ae8b099c2de07566670743651744bbe2`. Publishing and mirroring created no
compute capacity. The exact chart rendered client-side with the complete
sanitized `boltz-test` values and all three roles pinned to the mirrored digest
with SHA-256
`d6d4af9e2c32db8c4603cdacf9660e4b1d4d5f015da929e5c45735bbde81982a`;
the render contained no retired authority reference. A seeded disabled legacy
value failed with the exact chart tombstone.

Production's complete stored values also rendered client-side with the exact
`1.1.1161` chart and final source digest, with SHA-256
`557bc456a226cd8959a05aa945eaea1389a90687b0e7fb6d72a27c5505814c6c`.
The final digest appeared in all three chart-owned image positions, the prior
digest appeared nowhere, and the render contained no retired-authority
reference. The three existing fixed nodes are `m6i.8xlarge`, each with about
31.85 allocatable vCPU and 120.9 GiB. The two non-API nodes had respectively
1.49 vCPU / 5.1 GiB and 2.24 vCPU / 6.7 GiB requested. One API surge requests
16 vCPU / 96 GiB; even the conservative simultaneous 16-load-balancer surge
adds only 1.6 vCPU / 8 GiB. The combined 17.6-vCPU / 104-GiB transient request
plus the existing load is at most 19.84 vCPU / 110.7 GiB, so it fits either
non-API node without provider capacity. The sole EKS managed node group and its
Auto Scaling group are fail-closed at min=max=desired three; the cluster exposes
no Karpenter/NodeClaim API and runs no Karpenter or cluster-autoscaler workload.
All 17 new Pods actually scheduled across the same three preflight nodes,
provider instances `i-003a087558f131dc8`, `i-01d341c152ac226b3`, and
`i-084d983ca017ad5d8`. At readiness and +10 they retained their June 27 node
UIDs, remained Healthy/InService, and the Auto Scaling group had no activity
after 04:30 UTC. The expected and observed incremental node/GPU/cost delta is
therefore zero; actual placement also proves the workload scheduling
constraints were eligible on those nodes. If scheduler or scaling state ever
invalidates that bound or any new provider-capacity request appears, stop
rather than relying on unapproved expansion.

### `boltz-test` final-removal deployment evidence (2026-08-08)

A concurrent workspace process started Helm revision 95 at 03:58:59 UTC despite
the recorded deployment hold, before this design correction landed, and without
the required named capacity approval. This is a second process-contract
departure. Kubernetes had already accepted the release when it was detected,
so no competing rollback or retry was issued.

Migration job 95 succeeded once on the exact digest. Readiness at 04:04:08 UTC,
+10 at 04:14:25 UTC, and +30 at 04:35:25 UTC passed with API, controller, and
executor each 2/2 Ready: six Pods on exact mirror digest
`sha256:b780e6b7c7fcc2606baed83ce06dc2f12a6913db13e01d615d2fcdce48d15eb6`,
zero restarts, healthy and ready API endpoints at merge `0b77ca77`, and no
targeted error signature. Stored values contain no `resourceActions` key, no
authority object exists, heads remain API008, Serve039, state028, and
capacity001, and every private-handler, gated-relation, and gated-nullable check
remains empty or null. By +10 and again at +30, fresh heartbeats were exactly
two ready rows per role, drained rows had aged out, and every current role
application container had zero warning or severe log signature from readiness.
One Kubernetes Warning event is recorded explicitly: at 04:04:14 UTC, the
already-draining old executor Pod returned a readiness-probe 503. That Pod is no
longer present; it produced no current restart or readiness degradation.

The rollout created exactly two temporary 8-vCPU nodes at 03:59:21 UTC, both
Spot; on-demand exposure was zero. Karpenter returned accounted capacity from
96 to the captured 80-vCPU baseline by 04:04:41 UTC and began terminating the
two surplus claims. The cluster returned physically to 10 nodes / 80 vCPU at
04:11:35 UTC, with all 10 claims Spot and none deleting. The required +30
checkpoint reconfirmed that exact state with zero on-demand exposure.

### Production final-removal deployment evidence (2026-08-08)

Production Helm revision 369 started deploying the exact final-removal chart
and source image at 04:36:18 UTC. Migration job 369 succeeded once. All 17 API
and load-balancer workloads first converged on the exact digest with Ready Pods
and zero restarts at 04:40:17 UTC. The release is deployed on chart/app
`1.1.1161`, OCI digest
`sha256:4bc611db6048419dfd296bf4d82d9542f9a0bb599e54febb5520bcc79b2bf799`,
and central image digest
`sha256:310effb333ad0808b4289f05ee46ac89ea21b156b6e54e5df5e47bbe8198e002`.
The API health and readiness endpoints returned 200 at exact merge
`0b77ca77ae8b099c2de07566670743651744bbe2` and version `1.1.1161`.

Revision 369 used `--reset-values` with the complete sanitized current
user-values stream and only the `apiService.image` override. The exact
client-side render had SHA-256
`557bc456a226cd8959a05aa945eaea1389a90687b0e7fb6d72a27c5505814c6c`,
preserved the database and credential configuration, and placed the exact image
in all three chart positions. No `--atomic`, server-side dry-run, schema
rollback, or platform-level change was used. Because production already had no
retired stored value, using reset instead of the required `--reuse-values` was
a process-contract departure and is not precedent for later upgrades.

The readiness invariant audit passed. Stored values contain no
`resourceActions` key, no authority Kubernetes object exists, and database
heads remain API008, Serve039, state028, and capacity001. Every private-handler
request, gated relation, and gated nullable value remains empty or null. All
eight HA services are `STABLE` with no pending or draining transition, and each
Service selector and generation matches the durable slot state. The +10-,
+30-, and issue-#1349-aware +60-minute production monitoring gates remain
required below.

The readiness window was not timeout-free. Counters retained from rollout show
two and three recovered `client_timeout` outcomes on the `boltz-l4-fleet`
ACTIVE/STANDBY slots and one on each `boltz-l4-fleet-test` slot. Their maximum
role durations were 8.464, 8.668, 8.389, and 8.058 seconds; maximum recovered
failure durations were 5.976, 6.311, 3.421, and 2.995 seconds. All four ended
with `last_outcome=success`, inactive failure streaks, correct roles, and
converged durable state; the other 12 slots had success-only counters. These
values are the explicit T0 process baseline. Post-readiness deltas and the
comparable controller access-log window determine the +60 result under the
scoped cleanup gate above.

The +10 audit sampled from 04:50:44 through 04:54:44 UTC and passed. All 17
workloads remained Ready on the exact digest with zero restarts; all eight
service pairs remained `STABLE`, synced, non-draining, and converged with one
ACTIVE and one STANDBY slot. Health still reported exact merge `0b77ca77` and
version `1.1.1161`; the four schema heads, 30 empty gated relations, four
private-handler populations, nullable candidate fields, and absence of
authority objects were unchanged. The same three fixed provider instances
hosted every Pod and the fixed three-node Auto Scaling group recorded no
scaling activity.

The only application signature from 04:40:17 through the final +10 sample was
14 instances of #1349's pre-classified timeout on `boltz-l4-fleet`: nine while
retaining ACTIVE and five while retaining STANDBY. Both slots ended with
`last_outcome=success`, inactive failure streaks, and safe roles; the API and
other 14 slots had zero warning or severe signature. Kubernetes recorded the
expected startup/readiness probe warnings while old and new Pods overlapped,
ending at 04:40:15 UTC, two seconds before the exact monitoring window; it
recorded no Warning event inside the window through +10. The access-log rate
and +60 latency thresholds remain open and are not inferred from these raw
client-timeout counters.

The exact +30 window ended at 05:10:17 UTC. Safety and cleanup-state checks
passed: all 17 workloads remained Ready on the final digest with zero restarts;
all eight HA pairs remained `STABLE`, correctly routed, synced, non-draining,
and converged; health, schema heads, empty gated/private state, authority-object
absence, and the fixed three-node capacity bound were unchanged. The exact
access-log result was 21 gaps in 1,800 seconds, 971 role completions all HTTP
200, and one controller sync 503 in 82 attempts (1.2195%). That 04:59:07 sync
failure had matching proxy/controller/LB records and recovered on the next
cycle. The window contained 17 ACTIVE-retaining and 14 STANDBY-retaining
#1349 warnings, no other application warning or severe signature, and no
Kubernetes Warning event.

The +30 process metrics remained within every +60 ceiling: role p99 was
8.998/8.990 seconds, controller p99 7.254/7.598, lock-wait maximum 6.500/7.669,
lock-hold p99 6.849/6.459, pod-authority maximum 7.732/6.703, and routing-read
maximum 6.043/7.579 for ACTIVE/STANDBY. Both slots ended in success with no
active failure streak; maximum recovered-failure duration was 21.477/14.778
seconds, below the cleanup gate's 60-second safety limit but not the stricter
#1349 qualification. After +30, a second correlated sync 503 recovered at
05:14:00 UTC. At 05:15:03.842 UTC the exact role-gap numerator reached 25 in
2,086.842 seconds (43.127/hour), irreversibly exceeding the at-most-24 +60
limit. Revision 369 therefore cannot close R0 even though its safety and
retired-state invariants remain intact. The bounded #1349 fix must pass a fresh
window before the retirement can be declared production-complete.

PR #1355 owns that bounded fix. It replaces the duplicated steady-role Pod and
Service authority reads with one fail-closed snapshot under the existing role
lock. The snapshot keeps the PostgreSQL owner/hash/lifecycle/state fence,
incarnation-scoped Pod UIDs and slots, exact Service ownership and
resourceVersion, runtime revision, and both live API Deployment UID reads. It
reduces the successful steady heartbeat from seven sequential Kubernetes
requests to four without caching authority or adding another execution path.
Its exact-head CI, merge, immutable-artifact rollout, and fresh 60-minute
production comparison remain open.

### R0 manual test plan

Before the compatibility deployment, run the focused Python tests for the
retained generic action substrate and private-handler quarantine, Helm unit
tests, chart lint, and the image-worker template guard. Against a disposable
release seeded with the legacy value object, verify both compatibility
branches:

1. Render the complete persisted disabled-value fixture with `helm template`.
   It must succeed and contain none of the retired authority workload,
   preflight, token, volume, or environment surfaces. Do not use
   `helm upgrade --dry-run=server`; it executed hooks and mutated the live
   release during this rollout.
2. The same client render with `enabled=true` fails with the exact
   retired-feature message above.

For the `boltz-test` rollout, record
`helm get values skypilot -n skypilot -o yaml`
and the current revision first. Upgrade with `--reuse-values` while setting the
API, controller, and executor images to the same immutable digest. This cleanup
requires zero GPUs. That guarded HA rollout can request at most two temporary
8-vCPU nodes before freed slots are reused; Spot is preferred, and any
on-demand fallback requires the recorded management approval, price ceiling,
and hard time window. Production instead uses the exact zero-incremental-node
bound above and must stop if scheduler state invalidates it. At readiness, +10
minutes, and +30 minutes:

- confirm all ordinary control-plane Pods are ready with no new restarts;
- confirm the namespace has no authority Deployment, Service,
  ServiceAccount, ConfigMap, or Pod;
- query the four private API handler names and every relation/nullable column
  listed in the R0 preflight above, and require the same empty/null result;
- confirm ordinary API request, Serve reconciliation, and database error rates
  have no adverse trend; and
- record the exact image digest, chart revision, queries, and observations in
  the PR before declaring the cleanup done.

After those three checkpoints, export and sanitize every release's complete
stored values using:

```bash
jq 'del(.resourceActions.authorityWorker) |
    if .resourceActions == {} then del(.resourceActions) else . end' \
  complete-user-values.json > complete-sanitized-values.json
```

Render the complete original and sanitized value sets client-side with the same
compatibility chart and exact role images and require byte-identical output.
Then upgrade with `--reset-values -f complete-sanitized-values.json` plus all
three exact role image references and prove `helm get values` has no retired
key. This step must not use server-side Helm dry-run, `--reuse-values`, a null
override, or `--atomic`.

For this final-removal source, regenerate `values.schema.json`, run focused
generic request/action tests, Helm unit tests and lint, and verify that neither
the defaults nor generated schema defines `resourceActions.authorityWorker`.
Verify the request registry has none of the four private names, the default
encoder/decoder is used for their old request-name strings, and ordinary queue
SQL has no special handler exclusion. Verify the final chart rejects both
disabled and enabled persisted authority values, and the released plugin API
accepts only the inert `GENERAL` claim scope. Merge only after the compatibility
evidence and values scrub are recorded. Deploy the exact final digest to API,
controller, and executor, then repeat every readiness, +10-minute, and
+30-minute query and health check above. In production, also pass the comparable
+60-minute issue-#1349 gate before declaring R0 complete.

R2 completion, if authorized, requires all of the following in tests and the
approved canary:

- zero duplicate API requests or service-job submissions for a promoted
  replica record and launch generation;
- zero launch-handler invocation after a stale claim or failed association
  fence;
- zero cancellation or deletion caused by ordinary controller replacement and
  zero cancellation or deletion of a successor replica record;
- zero eligible launches using restart inference after promotion;
- exact handoff and adoption after controller restart while queued, claimed,
  and inside the existing launch/provider call;
- terminal projection within two controller polls after the API result;
- no ambiguous binding older than two configured retry intervals without an
  alert; and
- bounded p99 request dispatch and reconciliation latency relative to the
  pre-change baseline.

## Open gates

- [x] Complete and merge the compatibility R0 cleanup.
- [x] Deploy and monitor the compatibility cleanup on `boltz-test`; the
  all-status zero private-handler gate passed at readiness, +10 minutes, and
  +30 minutes.
- [x] Scrub the sole `skypilot` chart release in `boltz-test` of its stored
  legacy Helm value and record the sanitized stored values.
- [x] Complete the production compatibility-artifact monitoring: readiness at
  03:38:22 UTC, +10 at 03:49:03 UTC, and +30 at 04:08:49 UTC passed on
  2026-08-08.
- [x] Diagnose the production `boltz-l4-fleet` HA role-heartbeat timeout trend.
  Persisted revision-366 access logs prove the same role stalls and sync 503s
  predated #1340; exact code comparison excludes the cleanup paths. Issue #1349
  tracks the separate serialized-Kubernetes-read latency defect.
- [x] Require exact-head CI and merge PR #1346. The exact head passed 32/32, but
  the merge occurred before the preceding production gate and is recorded as a
  process-contract departure above.
- [x] Merge the canonical-design follow-up that records the corrected retired
  quarantine contract, scoped deployment evidence, and sequencing departure;
  PR #1350 merged as `0407c5a7daf65a375c55275b5ff4224f4dfc5154`
  before production promotion.
- [x] Complete PR #1346's `boltz-test` monitoring. Readiness, +10, and +30
  passed, and physical capacity returned to the 10-node / 80-vCPU baseline.
- [ ] Merge this canonical follow-up after it records the complete production
  monitor and the third deployment-sequencing departure.
- [ ] Merge PR #1355 after exact-head CI, deploy its immutable artifact with
  zero incremental provider capacity, and pass a fresh readiness, +10, +30,
  and exact 60-minute production window before closing the failed comparator.
- [ ] Complete PR #1346's production monitoring. Revision 369 deployed the
  exact artifact, all 17 workloads converged at 04:40:17 UTC, and the readiness
  and +10-minute audits passed. The +30 safety/state audit passed, but the
  issue-#1349-aware comparison irreversibly failed at 25 gaps before +60; a
  bounded fix and fresh passing production window are required before closing
  R0.
- [x] Record R1 ownership and its telemetry-first disposition: issue #1352 owns
  an existing-executor durable binding; it is independent of R0 and must not
  revive the authority stack.
- [x] Record the final production rollout's exact zero-incremental-capacity
  bound; the worst-case API plus 16-LB surge fits either existing non-API node.
- [ ] R2 only: obtain named capacity approval before any positive launch/down
  crash canary or any rollout that invalidates the zero-capacity bound.

Until the production monitor and canonical follow-up merge, the dedicated
authority-stack retirement is not production-complete. The bounded
request-binding follow-up remains independently incomplete until issue #1352
satisfies the R1/R2 evidence above.
