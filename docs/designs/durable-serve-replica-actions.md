# Durable SkyServe Replica Actions

Last updated: 2026-08-08

Status: the dedicated resource-action authority proposal is retired before
activation. PRs #1112, #1239, #1240, #1336, #1338, and #1343 are closed. PR
#1335 merged, but its dark V2 preflight and qualification layer is retired by
this cleanup. PR #1342 also merged after the retirement review began; its dark,
uncalled renderer and representability evidence is removed atomically rather
than left import-broken by the retirement of its authority dependencies. PR
#1333's forward-only Serve038/039 migrations are retained inert while its
uncalled runtime state layer is removed. The unexercised V2 authority contracts
merged by PR #1332 and the disabled PR #1232 activation surface are also being
removed. PR #1340 merged the compatibility cleanup. Draft PR #1346 deletes the
temporary disabled Helm value, private-handler quarantine, private result
codecs, and authority routing. Its deployment gates passed on 2026-08-08: the
exact compatibility artifact was deployed, every private-handler request was
absent across all statuses at readiness, +10 minutes, and +30 minutes, and the
only SkyPilot Helm release's stored value was scrubbed. The released plugin
`claim_scope` API remains as an inert `GENERAL`-only compatibility shim; its
retired authority value is rejected and does not affect queue selection.
No service was promoted, no authority worker claimed a request, and no provider
effect ran through the proposed path. Source cleanup is not operationally
complete until the exact merged compatibility artifact and this final-removal
artifact are each deployed and pass their monitoring gates below.

The review found one smaller, independent correctness gap in the existing
Serve controller: an ordinary replica launch records its exact API request ID
only in process memory. A controller restart can therefore lose the association
and submit another launch request. The system-OOM recovery path already has a
bounded durable binding and adoption mechanism, but ordinary launches do not.
That gap does not justify a dedicated authority deployment, a native provider
renderer, a second execution topology, or a universal physical-capacity
kernel. A bounded fix may proceed only under the contract and evidence gates
in this document.

## Decision record

The original 2026-07-30 request was an evaluation of whether a unified
physical-capacity convergence kernel had enough long-term payoff to justify
its migration risk. The evaluation concluded that the payoff was conditional
and required 30--60 days of production evidence across at least two domains.
The subsequent disabled deployment found no Serve services or replicas, no
independent provider-call audit source, empty capacity tables, and zero
projector database connections. The large payoff therefore remained a
hypothesis.

Implementation later drifted beyond that decision into a dedicated authority
stack. The stack had already reached roughly 37,000 changed lines across 113
files at the original review, and later dark merges expanded it further while
still leaving no complete admission-to-effect path. It introduced separate
authority workers, cohort and lease protocols, private transport, native
rendering, V2 representability inventories, and Serve038/039 state while the
named legacy mutation owners remained authoritative. That architecture is
rejected.

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

Deploy the exact merged image as one compatible Helm rollout. The live release
explicitly pins `apiService.image`, `controllerService.image`, and
`executorService.image`, so `helm upgrade --reuse-values` must set all three to
the same immutable digest; updating only the API value would leave mixed old
controller/executor images. A stored
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
`--reset-values` and the complete sanitized values file while explicitly
pinning API, controller, and executor to the same immutable image digest. Do
not use server-side Helm dry-run, combine `--reset-values` with
`--reuse-values`, use a null override, or use `--atomic`; the migrations remain
forward-only.
Verify `helm get values` contains no retired key.

Only then may this final-removal change merge. Its chart intentionally has no
`resourceActions.authorityWorker` schema or enabled-value guard, and its
request registry intentionally has no private handler, authority claim routing,
codec, or ordinary-queue exclusion. A narrow value tombstone rejects an
unscrubbed release, and the released plugin API retains only an inert
`GENERAL` claim scope. Deploy its exact immutable image to all three roles,
run the retained migrations forward, and repeat readiness, +10-minute, and
+30-minute checks. Rollback changes application images/chart only and never
downgrades the database.

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
- all ordinary control-plane Pods ready with zero new crash loops;
- authority worker disabled and absent;
- no unexpected action, policy, cohort, handoff, or authority rows created;
- ordinary API request and Serve reconciliation health unchanged; and
- start, 10-minute, and 30-minute post-readiness checkpoints recorded in the
  relevant PR with identical empty authority state and no new error/restart
  trend.

### Compatibility deployment evidence (2026-08-08)

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

The cluster has one SkyPilot Helm release. Its original and sanitized values
rendered byte-identically with the exact compatibility chart. Revision 94
applied the sanitized complete values at 03:32 UTC with `--reset-values`, the
same three image digests, and no `--reuse-values` or `--atomic`. Migration job
94 succeeded, all six Pod names and creation timestamps remained unchanged,
all Deployment generations remained observed and 2/2, the database zero-state
remained unchanged, and `helm get values` now contains no `resourceActions`
key.

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

For the real rollout, record `helm get values skypilot -n skypilot -o yaml`
and the current revision first. Upgrade with `--reuse-values` while setting the
API, controller, and executor images to the same immutable digest. This cleanup
requires zero GPUs. The guarded HA rollout can request at most two temporary
8-vCPU nodes before freed slots are reused; Spot is preferred, and any
on-demand fallback requires the recorded management approval, price ceiling,
and hard time window. At readiness, +10 minutes, and +30 minutes:

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
+30-minute query and health check above before declaring R0 complete.

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
- [x] Scrub the only SkyPilot release's stored legacy Helm value and record the
  sanitized stored values.
- [ ] Move PR #1346 out of draft, require exact-head CI, then merge, deploy its
  exact artifact, and repeat readiness, +10-minute, and +30-minute monitoring
  before closing R0.
- [ ] Establish R1 production telemetry or record the explicit correctness
  decision.
- [ ] Obtain exact capacity approval before the final HA rollout or any
  positive launch/down crash canary.

Until those gates close, the dedicated authority stack is retired and the
bounded request-binding change is not production-complete.
