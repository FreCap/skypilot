# SkyServe System OOM Recovery

_Status: rewritten #1182 implementation and canonical design are complete;
repository-wide unit integration corrections have passed local non-PostgreSQL
verification and exact paid-capacity retry review, with required real-
PostgreSQL CI pending; the corrected route-lease/runtime design passed exact
adversarial re-review; the stacked draft #1183 steady-state removal is authored
and remains blocked on all seven removal gates; production activation is
blocked_

_Last updated: 2026-08-03_

_Design baseline: `origin/improvements` at
`02cc7ceb722b8ef15a0766b6bdf83416ad0603bd`_

## Context and decision

A single-node SkyServe VM runs the service command as one long-lived Ray task.
Ray 2.9.3 protects the node when whole-host memory crosses its configured
threshold by killing a Ray worker. Its last-task policy deliberately does not
retry the only task owned by a caller. The service process therefore exits,
while the VM, raylet, and Ray driver commonly remain alive. SkyServe eventually
interprets failed readiness as `FAILED_PROBING`, tears down the VM, and
provisions a replacement.

This fallback is safe but unnecessarily coarse when terminating and reaping
the workload returns host memory and leaves Ray healthy. The merged runtime
foundation can prove cleanup and let the still-live driver submit the same Ray
task exactly once more on the same machine.

Two broader lifecycle architectures were considered and rejected for this
initiative:

- The first #1182 implementation created an OOM-specific protected API request,
  private header, API migration, claim/GC protocol, protected cluster handle,
  and AWS cleanup receipt. That duplicated cloud/request ownership for a
  feature whose replay never needs cloud authority.
- The durable Serve resource-action initiative has the right long-term
  launch/down model, but authority is currently monotonic per service. The live
  `boltz-l4-fleet` service deliberately mixes AWS, GCP, and Kubernetes
  candidates, while only fresh 16-GB AWS jobs have sufficient immutable
  provider identity for this recovery contract. Making the
  whole service action-authoritative would either make the other providers
  unrepresentable or introduce a new hybrid fallback contract.

The bounded decision is therefore orthogonal to lifecycle authority:

- SkyServe launch and teardown stay on their existing legacy paths. The
  service remains `resource_action_mode=legacy`; this initiative creates no
  resource-action row, provider profile, action worker, or action reducer.
- The already-merged one-shot `FreshProvisionEvidenceLease` can arm recovery
  only inside the exact service job produced by one fresh, dedicated,
  single-node AWS provision result with at most 16 GB of RAM.
- The Ray driver owns the only replay. It performs no API request, setup,
  provisioning, provider mutation, or second SkyPilot job. Before replay it
  also observes a fixed local quiescence interval longer than the external
  load balancer's recovery-route lease.
- #1182 adds only durable launch intent, ordinary request/job association,
  admission disposition, and controller reduction. On exhaustion, preemption,
  evidence loss, or teardown, it schedules or adopts the existing legacy
  replica cleanup/replacement path.

There is **no API008 migration**, no OOM-specific API-request column, and no
`X-SkyPilot-System-Recovery-Operation-ID` header. The unshipped
protected-request implementation is deleted rather than carried as
compatibility. #1182 does add one internal, authenticated, low-cardinality LB
route-lease heartbeat: it is a data-plane freshness fence, not a per-request
header, application contract, cloud authority, or public API.

## Goals

- Recover one eligible Ray host OOM on the same 16-GB-or-smaller AWS VM
  without provisioning, rerunning setup, or relying on application completion
  markers.
- Preserve Ray's node-protection behavior and configured memory threshold.
  No rollout raises/disables the threshold or uses a machine above 16 GB.
- Prove that the owned workload/container scope is gone and host memory has
  reclaimed enough headroom before replay.
- Keep the recovering replica off-route until SkyServe observes the exact
  replacement attempt and a later fresh readiness probe succeeds.
- Persist candidate/ordinary/capable disposition, the exact ordinary launch
  request and service-job associations, event, attempt, boot, reason,
  occurrence, deadline, and controller state across controller replacement.
- Keep the current legacy launch/down request, cleanup thread, request-ID map,
  and provider behavior as the sole VM lifecycle authority.
- Make first-OOM same-machine recovery and second-OOM ordinary VM replacement
  explicit, bounded, and observable for AWS on-demand and Spot instances.
- Ship temporary authorization-document-v1/v2, runtime-marker-v1, and
  status-only compatibility with an immediately rewritten, gh-stack-linked
  draft #1183 and objective seven-day removal gates.

## Non-goals

- Recovering request-level CUDA OOMs or interpreting application SQS, Temporal,
  completion-marker, callback, or workload-success messages.
- Recovering AWS Spot interruption or another provider preemption. Those keep
  ordinary teardown/replacement and take precedence over OOM recovery.
- Changing instance selection, fallback order, market type, capacity policy,
  autoscaling, placement, or lifecycle ownership.
- Increasing RAM beyond 16 GB or changing Ray's memory-monitor threshold.
- Retrying arbitrary user jobs, setup failures, process exit codes, pools,
  multi-node services, reused/stopped clusters, GCP/Kubernetes/Slurm replicas,
  or another provider.
- Adding a public recovery option, public status, public API, or generic
  provider profile.
- Finishing, activating, or removing any part of the separate resource-action
  architecture. A future action-authoritative service needs a separately
  reviewed recovery-to-down adapter.
- Improving every ambiguity in the legacy launch/down path. Existing
  `FAILED_CLEANUP`/retry behavior remains the conservative lifecycle fallback.

## Behavior contract

### Per-job eligibility in the mixed fleet

The service itself remains on legacy lifecycle authority. Before launch, the
controller may resolve one authorization-v3 entry against the service
incarnation and pre-policy task and persist it as a **candidate** intent. That
step happens before optimizer selection and conveys no claim about the eventual
provider, market, shape, or machine. Admin policy, optimization, fallback, and
provisioning proceed unchanged. The backend consumes the one-shot provision
lease and makes the final actual-result decision after the post-policy task and
successful handle exist. Recovery is armed only when all of the following are
true:

An exact pre-policy resource override that excludes AWS (for example, an
explicit GCP/Kubernetes/Slurm-only launch) never creates a candidate intent.
It follows ordinary lifecycle immediately. Mixed-provider resources that still
permit AWS remain candidates because their final provider is not yet known;
backend rejection is resolved by the bounded protocol below.

- the workload is a non-pool SkyServe service launched through the existing
  `CloudVmRayBackend` path on one dedicated Linux VM;
- that service version's ordinary readiness-probe interval is a finite positive
  value at most 10 seconds and its timeout is at most 15 seconds. A larger or
  malformed value cannot create `CANDIDATE`. Recovery-capable routes also use
  the separate fixed-cadence lease prober below; the caps keep the ordinary
  reducer's detection/re-entry contract bounded rather than changing user
  configuration;
- the controller and `/launch` endpoint share the same central PostgreSQL Serve
  database and the existing durable service-owner launch fence is enforced.
  A non-consolidated controller, `enforce_launch_fence=False`, a controller-
  local database, unavailable shared state, or any non-PostgreSQL central path
  cannot create `CANDIDATE`; it omits every recovery key and launches ordinary;
- the effective provider is AWS and the exact successful provision created one
  fresh EC2 instance. Evidence binds the immutable EC2 instance ID plus AWS
  account, region, and availability zone; a reusable name or partial locator is
  never sufficient. Resumed, reused, stopped, partial, duplicate, skipped,
  dry-run, orphan-adopted, or non-proof-carrying results are ineligible;
- both the resolved handle/catalog resource record and the remote
  cgroup-aware total-memory observation independently prove at most 16 GiB;
  disagreement, unknown memory, or either value above the limit disables
  recovery and leaves the job on ordinary legacy behavior;
- effective market type is either on-demand or Spot and matches the server-only
  authorization entry. The feature does not change `use_spot`,
  `InstanceMarketOptions`, optimizer candidates, or fallback behavior;
- the job has one node, one run task/future, no managed-secret reference, no
  task-level persistent outer `container_image`, and no external process
  manager outside the typed local-Docker ownership contract;
- an owner-fenced controller launch intent carries contract version 2,
  authorization version 3, and runtime-profile version 2, and
  matches the exact service incarnation, replica ID/generation, workspace,
  safety-relevant task digest, immutable runtime-image digest, and typed
  `OwnedContainerSpec` digest;
- the backend consumes the exact fresh-provision evidence lease once while
  generating that service job; and
- the generated driver exposes `JOB_SYSTEM_RECOVERY_API_VERSION == 1`,
  independently validates its cgroup-aware total against the authorization-v3
  resource envelope before persisting local `ARMED`, and uses runtime profile
  2, which publishes `subreaper-v2+owned-local-docker-v1` for the exact
  original attempt.

The live `boltz-l4-fleet` task remains representable without a service split:

- a fresh AWS 16-GiB Spot replica may be recovery-capable after the Spot gates;
- a dedicated fresh AWS 16-GiB on-demand canary may be recovery-capable for the
  deterministic initial safety smoke;
- GCP, Kubernetes, a larger VM, an unsupported provider, or any mismatched actual
  result receives an unarmed/ordinary job and current replacement behavior;
  and
- a failed candidate followed by another provider/result cannot reuse the
  first attempt's consumed fresh-provision proof. The final actual result must
  independently match the authorization-v3 envelope.

Production uses authorization document v3. Shipped authorization documents v1
and v2 remain transition readers only until draft #1183; runtime profile 2,
supervisor marker/capability v2, and `OwnedContainerSpec` remain the
steady-state workload proof. A missing, malformed, stale, or mismatched v3
document fails closed to ordinary job behavior. It does not fail an otherwise
valid replica launch.

The server-only document remains
`SKYPILOT_INTERNAL_SERVE_SYSTEM_OOM_RECOVERY_PROFILES`. V1 and v2 canonical
bytes are never reinterpreted. V3 is an additive closed document with an
explicit profiles list:

```text
SystemOomRecoveryAuthorizationDocumentV3 = {
  version: 3,
  profiles: [SystemOomRecoveryAuthorizationV3, ...]
}

SystemOomRecoveryAuthorizationV3 = {
  authorization_version: 3,
  profile_id: NonemptyText,
  workspace: Text,
  service_name: NonemptyText,
  service_hash: NonemptyText,
  task_sha256: Sha256,
  runtime_image_digest: "sha256:" + 64LowerHex,
  runtime_profile_version: 2,
  required_runtime_capability:
      "subreaper-v2+owned-local-docker-v1",
  owned_container_spec: OwnedContainerSpec,
  owned_container_spec_sha256: Sha256,
  execution_envelope_sha256: Sha256,
  resource_envelope: {
    provider: "aws",
    allowed_aws_account_ids: [12DigitText],
    allowed_locations: [{
      region: NonemptyText,
      availability_zones: [NonemptyText]
    }],
    allowed_market_types: NonemptySubset["on_demand", "spot"],
    allowed_instance_types: [Text],
    max_host_memory_gib: 16,
    num_nodes: 1,
    dedicated: true,
    require_new_create: true,
    required_identity: ["aws_account_id", "region",
                        "availability_zone", "ec2_instance_id"]
  }
}
```

Authorization version 3 and runtime profile version 2 are different version
domains. They are never compared for equality. The trusted matcher explicitly
maps an accepted `SystemOomRecoveryAuthorizationV3` to runtime profile 2 and
the exact capability string above; the controller reducer validates the same
mapping when it first observes any valid recovery phase. Runtime profile/marker
version 2 is not renamed, reinterpreted, or replaced by authorization v3.

The document is not a user environment variable or service-name-only switch.
Changing the task or allowed market/shape envelope invalidates the static
authorization until a new exact v3 entry is reviewed. The literal AWS provider
allowance conveys no cloud lifecycle authority; it only lets the backend
consider generating one runtime-profile-2 plan. GCP cannot be added without a
new authorization-document version or an exact v3-compatible
immutable-identity extension reviewed before rollout.

The document cannot name an EC2 instance before provisioning. Its AWS account
list, location list, each location's availability-zone list, and instance-type
list are nonempty, duplicate-free, and canonical-sorted. Region/AZ authorization
is by an exact pair from one location entry, never an independent cross-product.
After the actual result exists, the backend verifies immutable EC2 instance ID,
AWS account, region, availability zone, market, instance type, and resolved
catalog memory against those exact allowlists and the authorization. The
generated driver separately verifies the
cgroup-aware total before it calls the unchanged API-v1 operation that persists
`ARMED`. Neither dynamic proof is added to `JobSystemRecoveryInfo`, its API-v1
protobuf, or the closed marker-v2 schema. Backend and driver each emit one
bounded internal structured admission log with only closed decision, reason,
provider, market, and profile-version values. Service/job/request/session,
account, instance, location, and free-form evidence values remain in their
existing owner-fenced state or provider records; they are not copied into this
new log, metric labels, or user-visible output.

The task digest is the SHA-256 of canonical sorted-key compact JSON produced
from the effective task's redacted YAML form after removing only `name`,
`service`, and `_user_specified_yaml`, and replacing a valid numeric
server-assigned replica-ID environment value with the fixed
`<server-replica-id>` token. Setup, run, event callbacks, environment names and
non-secret values, mounts/storage/volumes, hooks, and every other task field
remain byte-significant after canonicalization. The submitted `resources`
object is replaced by a recursive runtime-resource identity: it preserves
`any_of`/`ordered` shape (canonical-sorting list members) and only `image_id`,
`container_image`, `volumes`, `_cluster_config_overrides`,
`_docker_login_config`, and the digest member of
`_resolved_container_image`. Thus placement-only cloud, region, zone,
instance-type, accelerators, CPU, memory, disk, ports, labels, market, and
ordering spelling do not bind this digest. Authorization v3's closed resource
envelope and the actual provision-result checks bind the selected placement,
market, shape, and memory separately. Any admin-policy mutation outside that
exact normalization changes the digest and fails closed. Managed-secret
references are ineligible because their resolved environment and temporary-
file provenance cannot yet be normalized without hiding such a mutation. The
replica-ID key and numeric shape remain bound even though its numeric value is
normalized.

### Existing legacy launch plus exact controller handoff

Before scheduling the ordinary SkyServe launch request, the controller
owner-fenced compare-and-sets one historical intent in `ReplicaInfo`:

```text
SystemRecoveryLaunchIntentV1 = {
  version: 1,
  controller_contract_version: 2,
  recovery_authorization_version: 3,
  recovery_authorization_profile_id: NonemptyText,
  recovery_authorization_sha256: Sha256,
  runtime_profile_version: 2,
  expected_runtime_capability:
      "subreaper-v2+owned-local-docker-v1",
  service_hash: NonemptyText,
  replica_id: PositiveInteger,
  launch_generation: PositiveInteger,
  launch_nonce: 64LowerHex,
  workspace: Text,
  resource_envelope_sha256: Sha256,
  task_sha256: Sha256,
  runtime_image_digest: "sha256:" + 64LowerHex,
  owned_container_spec_sha256: Sha256,
  execution_envelope_sha256: Sha256
}
```

The internal launch context has exactly two closed forms. The controller-to-
endpoint `SystemRecoveryLaunchContextV2Unbound` contains exactly:

```text
{
  controller_contract_version: 2,
  recovery_authorization_version: 3,
  recovery_authorization_profile_id: NonemptyText,
  recovery_authorization_sha256: Sha256,
  runtime_profile_version: 2,
  expected_runtime_capability:
      "subreaper-v2+owned-local-docker-v1",
  sky_serve_service_name: NonemptyText,
  sky_serve_service_hash: NonemptyText,
  sky_serve_service_version: PositiveInteger,
  sky_serve_controller_pid: IntegerOrNull,
  sky_serve_controller_ip: TextOrNull,
  replica_id: PositiveInteger,
  launch_generation: PositiveInteger,
  launch_nonce: 64LowerHex,
  workspace: Text,
  resource_envelope_sha256: Sha256,
  task_sha256: Sha256,
  runtime_image_digest: "sha256:" + 64LowerHex,
  owned_container_spec_sha256: Sha256,
  execution_envelope_sha256: Sha256
}
```

The service fence and controller owner tuple must equal current server state.
The endpoint-to-backend `SystemRecoveryLaunchContextV2Bound` contains exactly
the same fields except that `launch_nonce` is removed and one
`bound_request_id: RequestIdText` is added. Unknown, missing, wrong-typed, or
wrong-form fields reject the recovery-bearing request before executor
scheduling rather than being ignored. The random 256-bit nonce authorizes only
one atomic endpoint bind: it never names a lifecycle operation, provider
mutation, cleanup, replay, or queue message, is never accepted by the backend,
and is not a public API field.

Neither context carries the shipped
`SYSTEM_OOM_RECOVERY_PROFILE_VERSION_KEY` used by controller contract 1.
#1182 does not overload or reinterpret that v1/v2 key. A PR1/old server
requires controller contract 1 plus the legacy key, so it rejects a v3 context
even if a v2 authorization document is accidentally still installed. The new
server maps controller-contract-2/authorization-v3 explicitly to runtime
profile 2; numeric equality is never the mapping rule.

The same owner-fenced write sets the recovery launch disposition to
`CANDIDATE`. Failure to persist it means the controller omits the recovery
contract from the existing internal Serve launch context and the generated job
is ordinary. The controller then calls the same legacy `sdk.launch`. Only the
first outer launch request for that replica generation may carry the recovery
contract. In the existing `/launch` handler, before executor scheduling, the
API server uses its already-created ordinary request ID plus the closed launch
context to consume the exact nonce and owner/generation-fenced bind that ID as
optional `ReplicaInfo.launch_request_id`. It then overwrites the in-memory
context's bound-request field with that server-known value. That server-created
bound context is the backend's durable-bind witness: it is constructed only
after the row-locked association commits, and the endpoint never accepts a
caller-supplied bound form. The backend deliberately does not import or query
Serve state; it requires the current request ID, server-bound witness ID, and
fresh-provision evidence request ID to agree. A caller cannot mint a bound
context, and the legitimate backend cannot run before association is durable.
This uses the existing request ID and `extra_launch_context` envelope; it adds
no public payload field, request marker/column, claim generation, retention/GC
rule, header, advisory lock, handle format, or cleanup receipt. `sdk.launch`'s
returned ID must equal the already-bound ID. Bind/return mismatch makes
recovery unadoptable and selects ordinary legacy teardown; it never triggers
request discovery.

For only this bound recovery-bearing request, the `/launch` endpoint schedules
the existing request with executor-level `retryable=False` even when the
unchanged launch body has `retry_until_up=True`. This specifically disables
the executor's blind unknown-stage `BrokenProcessPool` replay. The existing
typed `ExecutionRetryableError` path remains allowed only when provisioning
proves no service job/future/driver was submitted; it may re-enter the same
bound request and consume a new attempt's fresh-provision lease before one
eventual job. The body flag's ordinary pre-job capacity retry semantics are
otherwise unchanged. If a worker dies at an unknown stage or after starting a
job, the exact request becomes failed, the candidate stays off-route, and the
controller schedules/adopts legacy teardown without submitting or discovering
another job. Ordinary and already-demoted requests keep their current executor
retryability.

The existing outer launch loop can issue another ordinary request after a
failed request and confirmed legacy cluster cleanup. It must never overwrite
the recovery-bearing request association or reuse its context. Before any such
retry, the controller atomically and irreversibly changes this generation's
disposition from `CANDIDATE` to `ORDINARY`; the retry omits every recovery key
and follows the existing launch behavior. If `sdk.launch` raises before
returning, the controller first checks only the exact row association: a
server-bound ID is adopted and streamed, while an unbound intent takes the
demotion-and-cleanup path. It never searches request history. If legacy cleanup
does not confirm success, no retry occurs. Thus a replica generation has at
most one recovery-bearing request/job association even though the ordinary
legacy retry loop remains available after demotion.

At the backend submission boundary, the API/server revalidates the internal
Serve owner/generation context and exact post-policy task. The one-shot lease
then proves that the selected backend handle came from this call's exact fresh
AWS provision result and actual dedicated one-node shape. It exact-matches the
handle and proof's EC2 instance ID, AWS account, region, availability zone,
market, instance type, and catalog memory against the v3 resource envelope.
Eligibility consumes the lease whether it succeeds or fails; it cannot survive
fallback, be rebound to another handle, or be reused by a later job. It either
generates the exact runtime-profile-2 recovery plan or generates an ordinary
job. The driver, not the supervisor, then reads its cgroup-aware total before
creating/persisting the unchanged API-v1 `ARMED` row. Unknown, mismatched, or
greater-than-16-GiB cgroup evidence leaves no recovery row. The typed workload
still executes once as an ordinary service job, but the driver cannot replay it
and the controller cannot adopt recovery. The marker-v2 schema remains only a
process/container ownership proof and is unchanged.

The exact ordinary request result must return the service job ID. The
controller persists it with an owner-fenced compare-and-set before a capable
replica can become ready. It never uses a latest-job query. On controller
restart it may fetch the result only by the exact durable
`ReplicaInfo.launch_request_id` already bound to that replica. A missing
request ID, missing/malformed exact result, failed owner CAS, or job mismatch
keeps a potentially capable replica off-route and schedules/adopts ordinary
legacy teardown. A failed exact request is first cleaned up by the legacy path
and then, if retryable, re-driven only as `ORDINARY` as described above.

Launch-result callbacks never acquire the replica-manager lock: current
teardown may hold that lock while joining the launch thread. Instead, one
Serve-state primitive begins one PostgreSQL transaction, locks and validates
the service owner row first, then locks the exact replica row, revalidates its
replica/generation and recovery revision, patches the recovery subdocument,
increments the revision once, and rewrites both versioned JSON and
compatibility pickle from that locked value. A caller-computed transition
carries its expected revision: mismatch returns a typed conflict without a
write, after which the caller refreshes and reruns the pure reducer against the
latest state. It never reapplies a stale output. Endpoint nonce binding and
other self-contained compare-and-sets instead compute their transition inside
the row-locked transaction from the latest value. Terminal teardown,
`EXHAUSTED`, and quarantine are absorbing, so no request/job/probe callback can
revive them. Every overlapping mutation locks an already-required lifecycle-
fence row first and then takes the service owner row as its exclusive common
mutex before any replica row. Existing writer-specific locks may retain their
internal order only after that service lock because recovery callbacks never
acquire them; transactions that directly lock multiple replica rows use
ascending replica-ID order. No v13 writer or recovery path may acquire a
service/lifecycle lock after a replica lock. Generic whole-row replica writers
take that common order and preserve the latest stored recovery subdocument/
revision rather than overwriting it from a stale in-memory snapshot. Controller
status/probe reductions use the same explicit patch primitive and then refresh
their local object. Bookkeeping writes for an already-persisted replica are
update-only: each v13 row carries an immutable random `replica_record_id`, and
the writer carries that expected row identity. The transaction aborts the whole
batch if any locked row is absent or has a different identity. A v12/rollback-
shaped row deterministically derives its transition identity from its immutable
replica ID, cluster name, and creation timestamp, and the first v13 rewrite
persists it; a newly created row always receives a new random identity. Only
the explicit initial-create, new paid-capacity claim, and reserved-capacity
fill paths may insert, and those are insert-only rather than conflict-upserts.
Consequently a stale
callback or manager snapshot serialized after terminal row deletion cannot
recreate the replica or overwrite a later same-ID row, even while the service
owner incarnation is still valid. Replica-lifecycle single and batch deletes
likewise carry the expected `replica_record_id`, lock rows in the common order,
and abort or leave a mismatched recreated row untouched; service-incarnation
teardown remains independently fenced by the service hash/owner protocol. A
PostgreSQL deadlock/
serialization abort rolls back the whole transition; the next reconciliation
refreshes and re-reduces instead of retrying individual statements. This
protocol prevents database/manager lock inversion, lost request/job/reducer
updates, post-delete resurrection, and stale same-key aliasing; the identity
lives in versioned JSON/pickle and adds no table column or migration.

An exact durable paid-capacity claim re-delivery is bookkeeping rather than a
new admission. It may update the existing replica only after the immutable
`replica_record_id` matches; that update preserves the stored recovery bundle
and the claim's original `claimed_at`. A missing or replaced row rolls back
every pool/claim mutation in the transaction and reports ownership loss. A live
row without a valid claim remains on the insert-only admission path and
conflicts instead of being adopted.

The Ray job can already be live when job-ID persistence fails. That cannot
retroactively disarm its bounded driver. The driver may complete one replay,
but without exact controller evidence the replica never returns to route and
legacy teardown wins the machine outcome. This is a deliberate conservative
race, not a new cleanup protocol.

### Ray-driver one-replay contract

Immediately before submitting the original eligible Ray task, the generated
driver validates the exact remote recovery API version and typed
phase/state/lock surface and captures the monotonic arm-window start. Immediately
after `.remote()` and before waiting on the task, it creates one
`RecoverySession`, which owns the replayable submission closure,
original/replacement ObjectRefs and contexts, event/deadline/phase, and the
only exhaustion path. The private task uses `max_retries=0`; the driver is the
sole retry authority.

The session fixes one monotonic deadline at the captured start plus
`SYSTEM_RECOVERY_ARM_WINDOW_SECONDS = 35` and a one-way arm latch with states
`PENDING`, `ARMED`, or `DISABLED`. `PENDING` may wait for the unchanged
marker-v2 capability, but it cannot persist `ARMED` until that exact marker is
positive and the driver independently reads a cgroup-aware total at most
16 GiB. Under the existing per-job lock, the driver checks the monotonic
deadline again immediately before the unchanged API-v1 `ARMED` insert and
atomically latches `ARMED` or `DISABLED`. Deadline expiry, malformed/fatal
marker proof, unknown/larger cgroup total, DB failure, task termination, or any
other definitive admission failure latches `DISABLED`. That state is final:
later marker creation, lower memory, retry, polling, or controller restart can
never arm the session. Marker absence before the deadline remains `PENDING`;
it is not itself a fatal proof.

When `ray.get()` raises `ray.exceptions.OutOfMemoryError`, the driver records a
structured `RAY_NODE_OOM` event in the local jobs database. It does not change
Ray's memory monitor or ask Ray's implicit retry policy to override the
last-task safeguard. Setup and every other exception retain existing behavior.

The driver may manually submit exactly one replacement attempt for the same
service job. It reuses the existing placement group, command, task environment,
resource request, log path, and Ray driver. It does not execute setup, sync
mounts, call the API server, provision, change market/provider state, or create
a second SkyPilot job.

Before replay, the session verifies that Ray remains connected, the original
placement group exists, and instance boot/Ray-session identities are unchanged.
It also waits until at least
`SYSTEM_RECOVERY_REPLAY_QUIESCENCE_SECONDS = 83` monotonic seconds have elapsed
since the exact positive process/container cleanup proof completed. Memory
admission may proceed during that interval, but `.remote()` cannot run early.
Every successful readiness probe against the old process necessarily started
before that proof. The 83-second minimum exceeds its 60-second recovery-route
lease plus the marked route's fixed 10-second HTTP connection-pool acquisition
timeout, fixed 10-second backend-connect timeout, and one 2-second heartbeat
period, with one second of strict margin. Every correctly upgraded external LB
has expired the old route, and every request that passed a final pre-transport
lease check has exhausted both its pool wait and possible connect attempt,
before a same-IP/same-port replacement can bind. A fresh `.remote()` then
produces a new ObjectRef in the same placement-group bundle; the failed
ObjectRef is never reused. Placement-group removal occurs exactly once in an
outer `finally`. Every replacement ref is either durably adopted after
`RETRY_SUBMITTED` or best-effort cancelled and fenced by that removal.

A second Ray OOM, nonzero retry result, failed proof/transition, identity
change, or deadline atomically marks the existing job `FAILED` with recovery
`EXHAUSTED`. `FAILED_DRIVER` remains reserved for an unexpectedly dead driver.
The controller then uses the existing replica teardown/replacement path.

### Positive supervisor and container cleanup proof

For an eligible attempt, `run_with_log` launches the typed recovery supervisor
as the actual parent of the service command. It starts a Linux session, enables
`PR_SET_CHILD_SUBREAPER`, installs `PR_SET_PDEATHSIG` before a workload child
can exist, and records the Ray-worker PID and boot identity. Orphaned
descendants are adopted by the supervisor instead of PID 1. The existing
detached cleanup daemon observes worker death and signals the command session;
the supervisor enumerates, terminates, waits for, and reaps its complete
descendant scope before acknowledging cleanup.

The termination latch is one-way. Failed parent-death setup, an already-dead or
changed parent, changed boot ID, or a set latch suppresses workload start. V2
never falls back to the ordinary shell command.

`OwnedContainerSpec` contains a digest-pinned image, tokenized and validated
container-create options, argv, and inherited environment names. The effective
post-policy `task.run` must be exactly its canonical foreground/attached
renderer. Shell wrappers, pipelines, substitutions, redirections, detached or
interactive behavior, caller-controlled lifecycle/name/label/CID/restart
options, mutable images, remote Docker contexts, Docker-socket mounts, and
unknown options are ineligible.

The supervisor, not user text, executes `docker create`,
`docker start --attach`, and exact-ID removal. It records the full container
ID. Parent, latch, boot, Docker daemon, and stable-empty-inventory preflight
finishes before create. Immediately before start, a second gate requires the
same identities and inventory containing exactly that new ID. Failure
suppresses start, removes only that ID, proves stable empty inventory, and
emits no positive marker. A supervisor-marker-v2 result is positive only when
the exact container and every descendant are gone and complete inventory is
stably empty.

The typed `RecoveryExecutionEnvelope` preserves the generated environment
removals/additions, log stream, signals, rclone-flush postlude, and original
workload exit code. Atomic attempt-scoped capability/cleanup records bind the
attempt UUID, job/task/attempt, boot ID, supervisor process identity, Docker
daemon identity, exact container ID, and timestamps. Stale/crossed records are
never evidence.

The driver resubmits only after all of the following are true:

1. the exact attempt's typed cleanup record exists and every identity matches;
2. cleanup was graceful, without SIGKILL, timeout, or surviving descendant;
3. the original Docker daemon remains and full inventory is stably empty;
4. cgroup-aware host memory is below
   `min(90%, configured Ray threshold - 5 percentage points)` for three
   consecutive one-second samples; and
5. cleanup plus memory admission stays within the fixed 120-second deadline.

If the configured threshold cannot be read, admission uses 90%, five points
below Ray 2.9.3's default. This is hysteresis rather than proof memory cannot
rise again; the one-replay bound is final containment. Neither threshold nor
RAM size changes.

### Durable remote/job detail transport

The replica-local jobs database has a companion `job_system_recovery` table,
leaving the positional `jobs` layout unchanged. Its shipped API-v1
`JobSystemRecoveryInfo` remains unchanged: capability, exact job/event/boot
identity, stable original and optional replacement attempt IDs, reason,
occurrence count, timestamps, deadline, and monotonic phase: `ARMED`,
`WAITING_CLEANUP`, `WAITING_MEMORY`, `RESUBMITTING`, `RETRY_SUBMITTED`, or
`EXHAUSTED`. The driver persists the initial `ARMED` row only after its local
cgroup cap passes. Authorization/provider/cgroup evidence is deliberately not
added to this closed schema.

The OOM handler updates it under the existing per-job lock. A failed write
forbids replay. `RESUBMITTING` and the nonblocking `.remote()` call occur under
that lock after confirming the job is `RUNNING`, serializing submission with
ordinary job cancellation. The deadline is checked before lock, after lock,
and after submission. A late future or one whose `RETRY_SUBMITTED` state cannot
commit is never adopted and is cancelled/fenced.

The shipped status gRPC response and SSH fallback carry the existing
structured detail and per-job detail-status maps without new fields. The
closed status is `UNSPECIFIED`, `ABSENT`, `PRESENT`, or `MALFORMED`. `ABSENT`
is positive new-runtime evidence that this exact job has no recovery row at
that read, but is not by itself proof the backend declined recovery;
`PRESENT` requires one valid API-v1 detail, `MALFORMED` preserves
query/conversion failure, and `UNSPECIFIED` denotes an old status-only runtime.
New readers never interpret missing detail as recovery authority.

SkyServe fetches ordinary status and recovery detail in the same remote round
trip. It never parses logs or polls an application queue.

### Durable Serve state and pure controller reduction

`ReplicaInfo` version 13 stores an immutable `replica_record_id`, the historical
launch intent, launch
disposition (`CANDIDATE`, `ORDINARY`, or `CAPABLE`), optional ordinary
`launch_request_id`, exact service job ID, one-shot candidate-ready/
arm-release anchors, one monotonically increasing recovery-subdocument
revision, and one nested `ReplicaSystemRecovery`. Version 12 and older rows
default the new
fields to ordinary/no recovery; the versioned JSON extension needs no
PostgreSQL schema migration. During the supported rollback transition, a
v13-labelled row with the **entire** recovery bundle absent is also decoded as
`ORDINARY`: that exact shape is what an old v12 writer produces after all
candidate/capable rows have been drained. A partial or internally inconsistent
bundle is never granted that exception and is quarantined off-route. Replica
enumeration decodes each row independently: one malformed recovery bundle
cannot abort the fleet read. Its row is returned as a typed quarantined
replica, is never routed or reduced as ordinary/capable, emits one bounded
reason-only audit record without the raw payload, and is handed to the same
owner-fenced legacy teardown scheduler. Failure to acquire cleanup ownership
leaves it visibly quarantined for the next reconciliation rather than deleting
or guessing its state.

The first exact `PRESENT` observation initializes the validated nested object;
the controller never invents remote event/attempt identity. It stores
capability, controller state, job/event/boot identities, stable
original/replacement attempts, reason/count, latest phase,
adoption/deadline/completion times, and one-shot detection/status-barrier
anchors. It is the sole controller recovery state; no parallel flat recovery
strings or booleans are introduced.

Candidate authorization alone never activates the recovery startup barrier.
Before first routing, a `CANDIDATE` row resolves only against the exact bound
job. Any valid `PRESENT` phase with authorization v3 explicitly mapped to
runtime profile 2 and capability
`subreaper-v2+owned-local-docker-v1` proves the job previously passed through
`ARMED`: `ARMED` atomically persists `CAPABLE/ARMED`; `WAITING_CLEANUP`,
`WAITING_MEMORY`, or `RESUBMITTING` persists `CAPABLE/RECOVERING`;
`RETRY_SUBMITTED` persists `CAPABLE/RETRY_SUBMITTED` with exact adoption and
still requires a later fresh probe; and `EXHAUSTED` persists terminal state
and schedules legacy teardown. The controller need not have sampled the
intermediate `ARMED` row. `ABSENT` before the arm window closes is only “no row
now”; it cannot release the candidate because a valid driver may still arm
later.

The candidate's first successful readiness probe persists one immutable
`candidate_ready_observed_at` and
`ordinary_release_not_before = candidate_ready_observed_at + 35 seconds`. In
the same process it starts a 35-second monotonic guard. On controller process
replacement, boot change, or any unprovable clock continuity, every unresolved
candidate with a ready anchor starts a fresh full monotonic guard. Release
requires **both** the durable wall deadline and that process-local monotonic
guard; a forward/backward wall-clock jump or restart can delay release but
cannot make it early. At or after both gates, one new readiness probe begun
after the persisted deadline and monotonic guard must succeed, and the same
reconciliation cycle must re-read the exact job as nonterminal plus recovery
detail `ABSENT`. Only that conjunction atomically persists `ORDINARY` and
releases a mixed-fleet GCP, Kubernetes, larger AWS, or other backend rejection
to current behavior. Because the driver's captured arm-window start precedes
original task submission and successful application readiness, the combined
controller hold cannot finish before the driver's fixed arm deadline. A prior
`ABSENT` followed by any late valid `PRESENT` phase therefore becomes
`CAPABLE` (or terminal if `EXHAUSTED`), never an uncoordinated ordinary driver.

Before the first success, the service's existing initial-readiness deadline
still wins and is never reset. A success inside that budget satisfies the
application-readiness condition, but the candidate remains off-route for the
bounded system-admission hold above; a low configured initial delay therefore
does not tear down an application that already proved ready. Ordinary
post-ready consecutive-failure handling continues during the hold. A
`MALFORMED` or `UNSPECIFIED` candidate remains off-route and schedules/adopts
legacy teardown; neither status can release to ordinary. Missing request/job
association does the same. Exact pre-launch non-AWS overrides skipped
candidacy entirely, so they incur no arm-resolution hold. No latest-job lookup
is permitted.

At controller startup, only a previously persisted `CAPABLE` row enters the
recovery-specific forced-off-route 35-second exact-status barrier. An unresolved
`CANDIDATE` is withheld from the initial routing snapshot only until the
admission resolution above and remains governed by the service's existing
initial-readiness deadline; it never inherits the 35-second recovery barrier.
`ORDINARY` rows follow current startup/routing behavior immediately. For a
`CAPABLE` row, `PRESENT` reconciles typed state; `MALFORMED`, a missing exact
job, runtime-profile/capability mismatch, event identity mismatch, or barrier
expiry exhausts and schedules legacy teardown.

When the controller first observes any valid phase, it persists capability and
the corresponding reduced state as one patch. A ready `CAPABLE/ARMED`
replica's first failed probe sets a one-way 35-second event-detection latch.
Later success from the old attempt cannot clear it or route the replica. If the
exact event is not adopted before expiry, recovery exhausts.

A newer matching `RAY_NODE_OOM` nonterminal phase reduces to `RECOVERING`,
clears the ordinary consecutive-probe window, and keeps the replica off-route.
At first adoption it fixes one absolute deadline equal to the remaining
120-second local budget plus the service version's initial-readiness delay,
capped at 900 seconds. Polls, relabeling, and controller replacement cannot
extend it. The driver keeps `WAITING_CLEANUP` visible for 35 seconds within,
not in addition to, the local deadline so an alive controller can observe it.

Only exact `RETRY_SUBMITTED` may persist replacement adoption. A probe reduces
to `RECOVERED -> READY` only when it started after that adoption and no
down/purge/preemption/scale-down intent exists. A 200 from the old attempt is
stale. A second OOM, terminal job, mismatch, malformed detail, deadline, or
teardown reduces to `EXHAUSTED` and emits `SCHEDULE_OR_ADOPT_LEGACY_TEARDOWN`.

### Recovery-capable external-LB route lease

The controller's ordinary 20-second routing sync is intentionally
availability-biased: after a failed or slow sync an external LB retains its
last ready URL. That is safe for ordinary replicas, but is insufficient for a
recovery-capable process because its replacement can bind the same IP and port.
The data plane therefore adds one narrow freshness fence only for `CAPABLE`
routes; ordinary routes keep the existing controller-outage behavior.

For each `READY` `CAPABLE` replica that a normal readiness reduction allows to
route, the manager registers one process-local route target containing the
decimal replica ID, immutable `replica_record_id`, exact normalized route and
probe URLs, closed readiness request, and a new opaque random route token. A
route generation binds the controller-process epoch, record identity, exact
remote OOM event identity, and the exact routable attempt identity: the
original attempt for `ARMED`, or the adopted replacement attempt for
`RECOVERED`. A token may be issued only once for that exact route generation,
and only by the normal owner-fenced reduction after a qualifying readiness
success **and** a fresh exact remote job-status/detail read whose request starts
after that readiness response completes. Persisted/cached controller state is
never issuance evidence. The post-probe read must prove the exact job remains
nonterminal, detail is `PRESENT`, and its event/original/replacement attempt
identity and phase exactly match the generation that would route. If it instead
observes `WAITING_*`, `RESUBMITTING`, `RETRY_SUBMITTED`, `EXHAUSTED`, absence,
malformation, or any mismatch, that observation is reduced first and the probe
cannot issue the old generation. In particular, a pre-first-readiness OOM whose
controller status loop stalls cannot let a later readiness success from the
replacement mint an original-`ARMED` token: the ordered read must adopt
`RETRY_SUBMITTED` before any replacement probe can qualify, and the existing
post-adoption probe-start fence then requires another probe. A controller
replacement creates a new process epoch, but its startup status barrier must
first re-prove the exact durable generation. Exact `RETRY_SUBMITTED` adoption
plus a later successful probe creates the replacement generation. Row
replacement creates a new record identity. A URL change, off-route/re-entry,
or lease retirement within the same generation never rotates or reissues a
token; it queues conservative exhaustion and legacy replacement. The token is
never derived from or accepted by the application. The controller's
heavyweight routing snapshot includes
`system_recovery_route_lease=v1`, the decimal replica ID, and that exact token;
if its resolved URL does not exact-match the manager target, it emits no usable
marked route. Every resolved URL is transport-canonicalized before comparison:
scheme and DNS case are lowercase, IP literals use their canonical compressed
form, IPv6 remains bracketed, default HTTP/HTTPS ports are omitted, and
credentials, paths, queries, fragments, malformed authorities, or invalid
ports are rejected. Ordinary and capable rows use the same rule; an invalid
ordinary spelling is withheld rather than allowed to bypass a capable marker.
If multiple READY rows resolve to the same canonical transport URL, or a
capable row has no exact marker, the snapshot emits an explicit fail-closed
route fence rather than an ordinary route; row order, trailing slashes, DNS
case, default ports, and equivalent IP spellings therefore cannot downgrade a
capable route to unmarked behavior. The logical READY count remains unchanged.
The emitted sentinel is the mutually exclusive closed object
`{system_recovery_route_fence: v1}` for that URL. Any presence of the fence
field, malformed value, or mixed fence/marker fields is immediately
unselectable, removes the associated client/lease, and can never be interpreted
as ordinary. A collision permanently retires every capable source generation,
so removing the duplicate later cannot rehabilitate the same token. Fence
parsing and collision retirement are part of the new-LB activation gate. Thus
a probe against newer URL B can never renew a stale LB marker for URL A even
when heavyweight sync is stalled.
If endpoint resolution for a READY capable row instead becomes absent or
invalid, the controller cannot safely name a fence transport key, so it
immediately retires that exact row generation. The resulting heartbeat
omission revokes any URL retained by the LB's transient-empty snapshot path;
the malformed or missing endpoint is never projected as ordinary.

The normal readiness worker captures the exact monotonic start immediately
before its HTTP request. When first issuance may be needed, that same worker
starts the exact remote status/detail request only after its own readiness
response has completed. An exact matching result registers the still-inactive
token under the owner-held manager round before the worker returns; unrelated
queued or slow fleet probes therefore cannot age issuance evidence or create a
fleet-wide first-readiness barrier. This registration is process-local and
does not make stale durable state routable: heavyweight sync must still read
the exact READY row/generation, and heartbeat publication still requires the
dedicated prober's later success. A mismatching observation is returned to the
normal reducer and cannot register a token. For a durable `RETRY_SUBMITTED`
snapshot, the worker may validate and register only the captured predicted
`RECOVERED` generation, and only when its readiness probe's wall-clock start is
strictly after the already-persisted adoption fence. The parent still must
reduce and persist `RECOVERED -> READY` before heavyweight sync can expose the
inactive target. A probe at or before adoption cannot register it and requires
a later round. This keeps replacement issuance independent of unrelated slow
fleet futures without weakening the post-adoption fence.

A live capable row is never relabeled in place to another service version,
even for an otherwise config-only update. Its route target permanently binds
the old version's closed readiness path, body, headers, and timeout; comparing
only the URL or attempting to infer semantic equivalence would let that old
request renew after the update. The row therefore remains on its prior version
and exact prior request only while the existing rolling-update lifecycle still
admits that version, then follows the existing legacy teardown/replacement
path; the new-version row and attempt bind the new request. It is never
projected as the new version under the old token. Ordinary rows retain the
existing in-place update optimization.

Issuance also fixes an initial absolute retirement deadline equal to the
qualifying normal readiness request's monotonic start plus 60 seconds. It is
not anchored to target registration, result processing, first dedicated probe,
or first heartbeat. The token remains unavailable to heartbeat snapshots until
one dedicated probe succeeds, but this issuance deadline runs even while the
token is inactive. A normal probe result processed after a controller stall
therefore creates an already-retired token rather than extending old evidence.

A dedicated supervised route-lease prober runs independently of the ordinary
fleet probe/manager lock. It starts a nonoverlapping async round on a fixed
five-second monotonic cadence, probes every current marked target concurrently,
and gives each request a hard total timeout of
`SYSTEM_RECOVERY_ROUTE_PROBE_TIMEOUT_SECONDS = 15`, matching the maximum
readiness timeout admitted by recovery eligibility. A qualifying success is
published immediately when that target's coroutine completes; it never waits
for another replica, candidate job-status work, persistence, or the ordinary
probe round. Its deadline is exactly its monotonic request start plus
`SYSTEM_RECOVERY_ROUTE_LEASE_SECONDS = 60`, never result-processing time. The
prober's persistent aiohttp connector has an explicit 1,000-connection total
limit matching the service and registry ceiling. The library's default
100-connection queue must not consume a request's 15-second total timeout or
make later insertion-ordered targets accumulate false failures. This bounded
concurrency is controller startup capacity, not a service-configurable knob.
The first qualifying success may activate the token and replace the still-live
issuance deadline with its request-start-based deadline; later successes may
likewise extend only a still-live token. The first failure stops renewal; two
consecutive failures latch that exact token revoked, and a later success cannot
clear the latch. Independently, once any issued token's current deadline is
reached, whether or not it was ever activated, that token and exact route
generation are permanently retired before any later probe result can publish;
expiry is not a renewable idle state. A probe completion takes the dedicated
lock, retires an already-expired prior deadline first, then may activate or
extend only a still-live token. Consequently a controller/prober pause cannot
resume after replay, probe the replacement at the same URL, and first-activate
or resurrect the old marker.
Retirement is immediately omitted from heartbeat snapshots and is queued as a
retired-generation observation for normal manager reconciliation; the
independent prober never takes the manager lock or writes the replica row. The
manager immediately suspends the exact record/generation's process-local route
before a capable row is made off-route, terminal, or deleted. Suspension omits
the marker and heartbeat and ignores in-flight probe completions without yet
tombstoning the generation. A successful fenced database mutation commits
permanent retirement. A pre-write local abort may restore only that exact
unchanged token if its prior deadline is still live. Once a database call was
attempted, a raised connection or transaction exception is outcome-ambiguous:
PostgreSQL may have committed before the client lost the result. The manager
therefore performs an owner/record/generation-fenced readback before
restoration. Only the exact same durable row, still READY and route-eligible,
without teardown, preemption, or scale-down intent, and still owned by this
controller may restore the suspended token. An absent, changed, off-route,
owner-mismatched, or unreadable result permanently retires it. Explicit
owner/row `False` results also retire; a revision conflict refreshes and
re-reduces after the same exact readback rule. A suspension advances a
per-target probe epoch, so a dedicated result captured before suspension stays
ignored even after a proved rollback restores the prior deadline. Explicit
teardown intent retires immediately because rollback to a routable row is no
longer an allowed outcome. Successful insertion of a recreated numeric replica
ID retires any target belonging to the prior record identity. This closes
in-flight probe, ambiguous-commit, heavyweight-sync/delete/recreate races while
immutable record and generation fencing prevent stale or aborted work from
revoking the replacement generation. Row-admission observations and complete
fleet snapshots enter the registry only in their authoritative order under the
replica-manager lock; random UUID row identities deliberately do not pretend to
order arbitrary previously unseen observations. The registry additionally
retains a globally capped set of known-old row identities, rejects their
delayed replay, and fail-closes only the affected numeric replica ID if that
bounded history is exhausted. An authoritative snapshot that omits an ID
releases all process-local history for that deleted row before a later ordered
admission begins a new history.
The manager first reduces a fresh exact remote observation. Exact
`RETRY_SUBMITTED` adoption may advance to the replacement generation, but a row
that still presents or tries to re-route the retired generation is persisted
`EXHAUSTED` and sent through legacy replacement. Thus the expected expiry of
the old route while a proved recovery is off-route does not itself abort that
recovery, while stale `ARMED` state can never reissue. This keeps a transient
single miss conservative without allowing a replacement at the same URL to
rehabilitate the old route. Endpoint/user
timeout values cannot extend the fixed 15-second request. Target, failure,
deadline, generation-tombstone, and pending-exhaustion maps use a dedicated
lock, are bounded by the 1,000-replica service ceiling, and prune terminal,
deleted, replaced, and non-capable rows. An off-route live generation retains
its tombstone until it is exhausted or exact attempt evidence advances it.

Every external LB independently POSTs an authenticated internal lease heartbeat
on a fixed-start two-second monotonic cadence with at most one request in flight
and a ten-second total timeout, independent of HA slot mode and heavyweight
routing sync. Its bounded in-memory control path performs no endpoint
resolution, provider/cloud call, or fleet database scan: the controller owner
snapshots the manager map and returns a closed v1 object whose entries contain
decimal replica ID, exact opaque route token, and positive remaining seconds.
The existing LB sync credential and service-owner dependency protect the
controller child and stable API proxy routes. The response contains no URL,
service job, request, account, instance, region, or application data.

The LB validates the version, canonical decimal ID, closed token, finite
non-boolean remaining duration, and `0 < remaining <= 60`. It anchors each
accepted deadline at the heartbeat request's monotonic **start** plus the
returned remaining duration, so network latency consumes rather than extends
the lease. A response applies only to the current heavy-sync `(URL, replica ID,
route token)` marker, and only if its request-start/sequence is newer than the
last applied response for that marker. A reordered old positive therefore
cannot reinstall a lease after a newer omission, revocation, token rotation, or
URL change. A well-formed v1 omission revokes immediately; malformed responses,
authentication failures, timeouts, controller replacement, and the first
heartbeat after LB startup cannot refresh a lease. Maps are intersected with
the current marked routing snapshot.

All external-LB replica-bound data-plane selection, including occupancy probes,
excludes a marked URL unless its exact marker has an unexpired local lease.
Every retry also rechecks the established admission-eligible HA roles (`ARMED`
or `ACTIVE`) and non-draining state. `ARMED` remains eligible to close the
selector-patch/role-heartbeat window during warm-standby promotion; `STANDBY`,
`DRAINING`, and process-local drain remain ineligible. Thus a handler admitted
before an LB begins draining cannot select another replica afterward.
Selection is not the final fence: after any await that can buffer a client body
or delay scheduling, one `_client_pool_lock` critical section revalidates role,
drain, ready membership, URL, replica ID, route token, deadline, and exact client
generation; increments that exact client's reference; and returns the checked
client object to the transport. No second URL lookup occurs. Marked routes use
at most 10 seconds to acquire that client's pooled connection and at most 10
additional seconds to open a backend connection; ordinary routes retain their
existing pool-waiting behavior.

A newly synced marked URL is unavailable until its first valid heartbeat. A
coherent sync that removes or changes its marker removes the associated lease;
ordinary unmarked URLs retain existing controller-outage semantics. With a
stalled heavy sync and no traffic, at most 60 seconds after the last successful
old-process lease-probe start every LB stops selection, and at most 20 seconds
later every pool wait/connect admitted by a final atomic checkout has completed
or failed against the old process. The driver's 83-second minimum after exact
cleanup proof leaves a further three-second strict margin before a replacement
can bind the same URL.

This is an internal per-route freshness lease, not application authority, a
cloud lifecycle lease, public header, or per-request token. It cannot authorize
replay, adoption, readiness, teardown, or provisioning; it can only remove a
marked route from LB selection. Recovery authorization remains disabled until
every live LB process capable of a replica-bound selection—including DRAINING/
terminating processes and already-admitted retry loops—is proven to enforce the
v1 marker, heartbeat, per-attempt drain/role fence, and atomic checkout.

`sky/serve/system_recovery_state.py` owns validated observations, the nested
tagged states (`ARMED`, `RECOVERING`, `RETRY_SUBMITTED`, `RECOVERED`, or
`EXHAUSTED`), and pure `reduce_remote_observation()` /
`reduce_probe_result()` functions. Results contain new state plus explicit
off-route, clear-probe-window, readiness, and legacy-teardown actions. The
replica manager owns writes/routing and invokes only existing cleanup helpers.

### Existing legacy teardown and replacement

This initiative adds no cloud/request cleanup authority. On exhaustion or any
terminal controller decision, the replica manager:

1. owner-fenced persists terminal recovery and the existing teardown intent;
2. under the existing manager lock, finds and adopts an already-scheduled
   launch cleanup/down thread or exact legacy request association when one
   exists, rather than creating a parallel owner;
3. otherwise invokes the current replica teardown scheduling path once; and
4. lets existing request cancellation, `sky.down`, provider cleanup,
   `FAILED_CLEANUP` retry, state removal, and autoscaling replacement semantics
   proceed unchanged.

Legacy down wins every race. Once teardown, purge, preemption, scale-down, or
service-owner loss is durable, the reducer cannot route a later successful
probe. A still-live driver may finish its one replay briefly, but it has no
cloud authority and the existing teardown cancels/terminates the job and
machine as it does today. If job-ID/intent evidence is missing, the controller
does not wait for or infer recovery; it selects ordinary replacement.

For AWS Spot, `RAY_NODE_OOM` and interruption/preemption are distinct causes.
Only preemption/termination already observed and durably recorded by the
existing legacy liveness/down path takes precedence; this initiative adds no
notice receiver or same-cycle provider fence. The controller keeps such a
replica off-route and follows ordinary replacement. Before that observation,
a typed OOM may replay while the Spot VM and driver remain live. The driver
does not query a cloud API, interpret a generic exit as OOM, or recover
preemption. Actual provider termination kills the driver/VM, and the later
legacy liveness observation becomes terminal. The existing small stale-probe
window between provider termination and durable observation remains ordinary
Spot behavior; no claim is made that notice time itself instantly fences
routing.

The service remains `resource_action_mode=legacy` throughout. API005-007,
Serve033 action links/modes/cohorts, and resource-action provider progress are
neither read nor written by this feature. Future action-authoritative services
may design an adapter that maps the reducer's terminal teardown action to a
durable down action, but that adapter must not coexist as dual authority with
this legacy one and is outside this stack.

## Architecture

```text
existing SkyServe legacy launch request
  | owner-fenced candidate authorization intent already persisted
  | /launch binds its server-known ordinary request ID before scheduling
  v
existing provision/backend path
  | exact one-shot FreshProvisionEvidenceLease
  | exact EC2 ID/account/region/AZ + fresh dedicated single node
  | resolved handle/catalog <=16 GiB
  v
one existing SkyPilot service job / Ray driver
  | driver cgroup total <=16 GiB + authorization-v3 envelope
  | explicit authorization-v3 -> runtime-profile-2 mapping -> API-v1 ARMED
  | RecoverySession owns max_retries=0 + exactly one replay
  | supervisor owns typed process/container cleanup proof
  v
unchanged API-v1 job_system_recovery row
  | existing gRPC/SSH status + exact service_job_id
  v
pure SkyServe controller reducer
  | ready + arm-window expiry + exact ABSENT: candidate -> ORDINARY
  | any valid capability-v2 phase: candidate -> CAPABLE/reduced phase
  | MALFORMED/UNSPECIFIED/deadline: legacy teardown
  | CAPABLE ready probe -> bounded external-LB route lease
  | recovered: post-adoption fresh probe -> READY + route-lease renewal
  | exhausted/preempted/evidence loss
  v
existing legacy replica teardown/replacement path
```

The responsibility boundary is intentionally small:

- the legacy launch/down path owns every API request, provider mutation,
  cleanup retry, VM lifetime, and replacement;
- the fresh-provision lease owns only one in-process proof that this generated
  driver belongs to this fresh AWS result;
- the Ray driver owns the local cgroup cap and task replay on the same machine;
- the supervisor owns only process/container cleanup evidence;
- the local job table owns remote event/attempt state; and
- SkyServe owns durable routing/reduction, the bounded process-local route
  lease, and the decision to call its existing teardown scheduler; and
- each external LB owns only its independently expiring local copy of that
  route permission.

### One-shot fresh-provision evidence

`RetryingVmProvisioner` constructs immutable `FreshProvisionEvidence` from the
exact successful `ProvisionRecord` plus closed AWS facts captured for its
created EC2 ID. The record supplies the complete created-ID set, head, provider
and region/zone result. One request-scoped `DescribeInstances` result for that
exact created ID supplies `InstanceId`, `InstanceType`,
`Placement.AvailabilityZone`, and actual lifecycle market (`spot` only when
`InstanceLifecycle == "spot"`; absence means on-demand; every other non-null
value is rejected). The EC2 Describe call and STS caller-identity call use the
exact same request-scoped botocore session, resolved credential/profile chain,
workspace, and region context that performed provisioning; resolving a new
ambient/default session is forbidden. STS supplies the AWS account, which must
also agree with the provision/handle account context, and the resolved handle's
exact instance type is looked up through the AWS catalog for memory. Requested
`use_spot` or a task memory hint is never treated as actual-result proof. The
evidence binds those facts with the ordinary request, display/provider cluster
names, cluster hash, node count, and owner-fenced service identity. Creation
rejects an existing cluster, resumed IDs, partial/duplicate creates, a head
outside the created set, dry runs, skipped/non-proof-carrying results, missing
AWS facts, or any disagreement among provision record, Describe result, STS,
handle, and catalog.

The payload lives only in a noncopyable, nonserializable
`FreshProvisionEvidenceLease`. Aliases share one private lock and atomic
`take()` that clears the slot before returning. Under the backend submission
decision lock, the first decision takes it whether the authorization matches
or not,
revalidates against the resulting handle and effective task, and uses it only
inside that call. Raw payloads are not accepted later. A second take, handle
rebind, fallback result, or lease surviving reset is ineligible.

This evidence carries immutable AWS identity facts only; it is not durable
cloud authority. If the controller loses launch/job
association, the lease cannot be reconstructed to excuse recovery; the VM
falls back to legacy teardown.

### Typed workload ownership

`OwnedContainerSpec` and `RecoveryExecutionEnvelope` are the only
production workload representation. The supervisor supplies container
name/labels/lifecycle and owns exact-ID removal. Parent-death, boot, Docker
daemon, empty inventory, create/start, descendant reaping, and marker
identities are closed typed contracts. The driver's cgroup admission is a
separate pre-`ARMED` check. The deprecated v1
direct-shell scanner remains only for transition reading until #1183.

### Typed driver and controller state

`RecoverySession` owns all replay state/ObjectRefs and the only terminal
`exhaust(reason)`. `JobSystemRecoveryInfo` owns stable remote identity.
`ReplicaSystemRecovery` and pure reducers own controller identity/state.
Attempt directories under
`~/.sky/system_oom_recovery/<job>/<attempt>/` retain exact terminal evidence
for 24 hours; pruning validates every path remains beneath that root.
Incomplete directories are never positive evidence.

## State sequence

```text
candidate authorization-v3 intent -> ordinary legacy launch request ID
  -> fresh exact-identity AWS catalog <=16-GiB result
  -> exact service job ID -> driver cgroup total <=16 GiB
  -> runtime-profile-2 capability-v2 ARMED -> CAPABLE -> application READY

mixed candidate -> backend generates ordinary job -> first ready stays off-route
  -> persisted ready+35-second release deadline -> fresh post-deadline ready
  -> exact nonterminal status + exact ABSENT re-read -> ORDINARY -> route

first Ray host OOM (machine/driver still live)
  -> WAITING_CLEANUP (controller keeps replica off-route)
  -> old external-LB route lease expires
  -> graceful descendant/container cleanup proof + >=83s quiet + memory watermark
  -> same driver submits one new ObjectRef
  -> RETRY_SUBMITTED
  -> fresh post-adoption probe
  -> RECOVERED -> READY -> fresh route lease on the same VM/job/driver

second OOM / failed proof / memory mismatch / teardown / Spot interruption
  -> EXHAUSTED or preemption fence
  -> schedule-or-adopt existing legacy teardown
  -> ordinary provider cleanup / VM replacement
```

A controller crash re-reads version-13 replica state and the exact remote job
detail. It cannot reset the deadline or submit replay. An API/launch/cleanup
crash is handled exactly as by the current legacy lifecycle; recovery adds no
claim, request, provider journal, or absence inference.

## Invariants and races

- Ray remains the sole authority deciding when node memory is unsafe; its
  threshold is unchanged.
- No eligible machine exceeds 16 GiB in either resolved catalog/handle evidence
  or remote cgroup total. Unknown, disagreeing, or larger evidence is ordinary.
- At most one replacement ObjectRef exists per service job. The original
  driver owns it and Ray implicit retries are disabled.
- Replay never provisions, reruns setup, invokes an API, changes market/cloud,
  creates another SkyPilot job, or reserves a port.
- The exact historical launch intent, ordinary launch request ID, and service
  job ID are required for controller adoption. Missing evidence selects VM
  replacement, not discovery.
- The API endpoint binds its own ordinary request ID by consuming the exact
  random launch nonce before executor scheduling. A client-supplied unbound
  context cannot reach backend admission.
- A recovery-bearing launch request is non-replayable at the executor boundary;
  `retry_until_up` retains only the existing typed, proven-pre-job capacity
  requeue. Unknown-stage worker death selects exact-request failure plus legacy
  teardown, never blind whole-entrypoint re-execution or a second job.
- Every overlapping PostgreSQL mutation locks the service owner before replica
  rows and locks multiple replica IDs in ascending order. Recovery callbacks
  never take the replica-manager lock or reverse that database order.
- Whole-row bookkeeping for an existing replica is update-only. Once terminal
  teardown deletes its row, no stale manager snapshot or callback can insert it
  again or match a later row that reuses the numeric ID; immutable record
  identity must also match.
- Exact paid-capacity claim replay requires that same immutable record identity,
  preserves the original claim-selection timestamp and recovery-owned fields,
  and rolls back rather than adopting a live row with a missing or stale claim.
- Authorization v3 maps explicitly to runtime profile/capability v2. No reducer
  compares those different version domains for equality.
- `JobSystemRecoveryInfo` API v1 and supervisor marker/capability v2 remain
  closed and unchanged. Backend/driver admission evidence is operational
  logging, not remote recovery authority.
- Driver arming is one-way and bounded to 35 seconds from the captured
  pre-submission monotonic start. Once disabled, the same job cannot later arm
  or replay.
- `ABSENT` never releases a candidate before a fresh post-arm-window readiness
  success, both durable-wall and process-monotonic 35-second guards, and an
  exact same-cycle status/detail re-read. `MALFORMED` and `UNSPECIFIED`
  candidates tear down rather than route.
- The fresh-provision lease is consumed once and cannot cross fallback,
  controller restart, handle rebind, or a later submission.
- The current legacy path remains the only launch/down/cloud authority. No
  resource-action row, protected request, cleanup release, or second scheduler
  is created.
- A durable teardown/preemption intent always wins routing and VM outcome. A
  bounded replay may race briefly but cannot reverse it.
- AWS Spot interruption and Ray OOM remain separate typed causes; only typed
  Ray OOM can replay, and observed/durable legacy preemption always wins.
- Readiness routes a recovered replica only after a probe begun after exact
  `RETRY_SUBMITTED` adoption.
- A marked recovery-capable URL is selectable only under an exact local route
  token and lease of at most 60 seconds from the qualifying dedicated-probe
  start, plus an atomic final checkout before its bounded 10-second pool wait
  and bounded 10-second backend connect. The driver cannot submit its
  replacement before 83 monotonic seconds from exact positive cleanup proof.
  Because every old-process success started before that proof, no same-IP/same-
  port replacement exists while any conforming old-route lease or lease-
  admitted transport attempt can remain live.
- Ambiguous supervisor/container cleanup is failure, not permission to replay.
- GCP/Kubernetes/Slurm, unsupported providers, and nonmatching AWS jobs remain
  ordinary within the same mixed-provider service.
- Controllers that do not share the API server's central PostgreSQL Serve state
  or do not enforce the existing durable launch fence remain ordinary and never
  send a recovery context.
- The feature creates no listener and has no dependency on port 4517.
- There is no API008, recovery-specific API-request column, private operation
  header, public/per-request routing token, provider action profile, or action
  worker. The bounded authenticated recovery-route lease is the only added L7
  correctness fence.
- SQS/Temporal completion events cannot arm, recover, exhaust, clean up, or
  delete a system replica.

## Known conservative limitations

- A controller may first see a phase after `ARMED`; valid downstream phases are
  adopted directly, but missing/malformed exact detail still selects teardown.
- Ray `.remote()` has no synchronous cancellation point. A late future is not
  adopted and is cancelled/fenced, but a stuck GCS call may delay local
  cancellation while legacy teardown remains authoritative.
- A controller that loses exact job-ID/intent evidence tears down a potentially
  healthy or successfully replayed VM. Safety is preferred over adoption.
- The existing legacy launch/down path retains its current ambiguous-request
  and failed-cleanup behavior. This feature does not claim durable cloud
  operation recovery.
- An AWS Spot interruption may destroy the driver before it writes final
  detail. That is ordinary preemption and may make recovery telemetry
  incomplete. Before the legacy liveness path observes it, one stale
  successful probe can briefly race provider termination exactly as it can
  today; after durable observation it never authorizes replay or routing.

## Alternatives considered

### Resource-action launch/down authority

Rejected as a dependency for the current mixed service. Existing authority is
per service and intentionally forbids an authoritative-to-legacy fallback.
Using it only for AWS while GCP/Kubernetes stay legacy would require a new
hybrid contract; promoting the whole service would make those decisions
unrepresentable. The replay itself needs no cloud action. A future uniformly
action-authoritative service may add a separate adapter later.

### OOM-specific protected API request

Rejected. A private header, request marker, claim generation, GC hold,
operation lock, protected handle/YAML, and cleanup-release protocol duplicate
lifecycle authority. The old #1182 implementation was never shipped and is
deleted without compatibility or migration.

### Controller-side `sky exec`

`sky exec` avoids setup but its request can outlive its controller and it lacks
the exact service-job replay closure. The still-live driver already owns the
minimal bounded retry boundary.

### Reusing `sky launch`

A launch may provision, rerun setup, or choose another provider/machine. The
OOM replay must stay inside the existing job. Whole-machine replacement still
uses the ordinary lifecycle after exhaustion.

### Ray `max_retries`

Ray 2.9.3's last-task policy returns `should_retry=false` for the sole task of a
caller even with retries configured. It cannot prove daemon-owned cleanup or
coordinate Serve routing.

### Blind process, port, or memory checks

A closed port, dead Docker CLI PID, or low memory sample does not prove a
daemon-owned container/descendant is gone. Typed graceful cleanup, exact
container absence, daemon continuity, and repeated memory samples are required.

### More RAM or a relaxed Ray threshold

Those may change frequency but do not provide recovery semantics. Eligibility
stays at 16 GB or less and the threshold is unchanged.

## Stacked implementation and migration

The stack is rebuilt from current `origin/improvements`. #1182 and #1183 remain
one gh-stack pair above the merged foundation. The cleanup is authored at the
same time and stays draft until all numbered gates pass. #1182 links the draft
#1183 cleanup explicitly; #1183 links back and names the seven gates below as
its exact merge condition.

### PR 1: merged inert runtime foundation

[PR #1181](https://github.com/boltz-bio/skypilot/pull/1181), plus the
schema-floor integration fix
[PR #1194](https://github.com/boltz-bio/skypilot/pull/1194), introduced the
server-only authorization-document-v1/v2 matcher, one-shot fresh-provision
evidence, runtime profile and supervisor marker/capability v1/v2,
`OwnedContainerSpec`, `RecoverySession`, local job journal, and additive
gRPC/SSH detail transport. It does not emit the controller contract, so
merging/deploying it cannot activate recovery even if an authorization is
present.

No runtime safety is weakened by this redesign. Its v1 direct-shell reader is
deprecated and remains only for draft #1183.

### PR 2 / #1182: `[Serve] Adopt typed system-OOM recovery in SkyServe`

_[PR #1182](https://github.com/boltz-bio/skypilot/pull/1182), rewritten in
place on current `origin/improvements`_

This persists the owner-fenced candidate intent before the ordinary legacy
launch, then its exact ordinary request ID and the exact job ID from that
request's result. It adds `ReplicaInfo` v13, launch disposition, nested
recovery state, pure reducers, CAPABLE-only startup/detection/deadline
barriers, the additive authorization-document-v3 reader and explicit
authorization-v3-to-runtime-profile/capability-v2 mapping, fresh AWS
eligibility with immutable EC2/account/region/AZ binding, backend catalog plus
driver cgroup admission, observed-Spot-preemption precedence, legacy teardown
schedule/adoption, and bounded compatibility/admission logs. Only this PR
emits controller contract version 2. It does not change
`JobSystemRecoveryInfo` API v1, its protobuf, or marker/capability v2.

The current #1182 protected-request/header/API-migration/protected-AWS cleanup
implementation is replaced entirely. Rewritten #1182 preserves its PR number
and OOM title but contains no API008, request marker/columns, private header,
operation fence, protected handle/YAML, provider receipt/proof, resource-action
integration, or new cloud cleanup logic.

Merge gates include reducer property tables, version-12/13 serialization,
intent/request-ID/job-ID owner fencing, exact request-result recovery with no
latest-job fallback, candidate-to-ordinary/capable reduction, controller
restart, launch-result/teardown races, legacy cleanup adoption, mixed
AWS/GCP/Kubernetes eligibility, fail-closed GCP identity, on-demand/Spot
classification, unchanged API-v1/marker-v2 schemas, old/new gRPC/SSH
combinations, fresh probe fencing, and proof that existing legacy
request/provider semantics are unchanged. It deploys with the server
authorization document absent.

### PR 3 / #1183: `[Serve] Remove deprecated direct-shell OOM recovery`

_[PR #1183](https://github.com/boltz-bio/skypilot/pull/1183), rewritten in
place as a draft directly above #1182_

This accepts only authorization document v3, removes authorization-document
readers v1/v2, removes the direct-shell Docker parser and runtime
marker/capability v1 reader, and removes status-only old-runtime compatibility
after the minimum version makes it unreachable. It also removes the temporary
all-fields-absent-v13 rollback reader after every such row has been rewritten
into complete valid v13 state. It retains runtime profile 2,
the unchanged API-v1 local job table/protobuf fields, `OwnedContainerSpec`,
supervisor marker/capability v2, one driver replay, controller reducer, and
unchanged legacy lifecycle integration.

Current #1183 is based on the superseded #1182 implementation and must be
restacked/re-authored, not merged unchanged. It remains draft throughout the
seven-day observation window.

## Deprecation and removal ledger

| Deprecated/rejected path | Transition behavior | Removal |
| --- | --- | --- |
| Authorization document v1 and direct-shell Docker parser | Merged compatibility; never selected by a new production authorization | #1183 removes after seven gates |
| Authorization document v2 | Typed `OwnedContainerSpec` but lacks the exact authorization-v3 provider/identity/memory envelope; never selected by production after #1182 | #1183 removes the authorization-v2 reader; runtime profile and marker/capability v2 remain |
| Marker schema v1 and `subreaper-v1+local-docker-empty-inventory-v1` | Read-only compatibility for already-generated artifacts | #1183 removes after controller-observed capability audit plus the two-pass remote marker audit |
| Status-only old-runtime recovery decoding | Missing detail can only select ordinary VM behavior/replacement | #1183 removes after image and seven-day gates |
| All-fields-absent v13 recovery bundle written by a v12 rollback controller | Decodes only as `ORDINARY` after candidate/capable drain; partial state quarantines | #1183 removes when rewritten #1182 is the rollback floor |
| Old #1182 protected request/header/API migration/protected AWS cleanup | Never shipped; no transition writer or row exists | Deleted while rewriting #1182; no compatibility/migration |

The legacy launch/down SafeThreads, `_replica_to_request_id`, cleanup retry
maps/clocks, and provider behavior are **not deprecated by this initiative**.
They remain current lifecycle authority because the service remains
resource-action legacy. The separate resource-action M5/#1191 ledger owns any
future removal.

The mutable fresh-cluster boolean, boolean-only codegen selection,
per-transition `getattr()` discovery, duck-typed detail fallback, phase-current
attempt field, flat controller state, controller-owned replay, and cloud
absence inference are rejected before release and never enter the rewritten
stack.

## Deployment and rollback

Deployment is digest-pinned and changes no service resource-action mode. The
production Helm release is owned by Terraform/Terragrunt: rollout and rollback
use only a reviewed infrastructure plan/apply that preserves the release's
existing rendered values and pins API, executor, and controller roles
explicitly. This design does not authorize a direct `helm upgrade`, including
an ad hoc `--reuse-values` mutation outside the owning IaC state.

1. Rewrite, merge, and deploy #1182 with
   `SKYPILOT_INTERNAL_SERVE_SYSTEM_OOM_RECOVERY_PROFILES` absent. Verify health,
   schema heads unchanged, zero recovery intents, no API008 file/head/column,
   no private recovery header, no resource-action row, and the mixed service
   still `resource_action_mode=legacy`. Verify the Terraform/Terragrunt plan
   contains no unrelated Helm-value or infrastructure drift and no local/
   central recovery-schema change.
2. Inventory every live external-LB process that can make a replica-bound
   choice, including ACTIVE, STANDBY, DRAINING, terminating, and processes with
   already-admitted retry handlers. Prove each runs the v1 route-token/lease
   reader, per-attempt role/drain fence, and atomic client checkout, and that the
   authenticated lease heartbeat is healthy through the stable API proxy. With
   authorization absent, inject a synthetic marked route in the test fleet and
   prove no process selects it without a fresh exact token lease. Terminate and
   drain every old or uninventoryable process before proceeding. Only then
   verify all API/controllers and candidate images expose supervisor-marker-v2,
   controller-contract-v2, and job-detail-v1. Existing replicas remain ordinary;
   authorization affects only newly launched jobs.
3. Install an exact authorization-v3 entry for a dedicated fresh AWS on-demand
   16-GB canary through the owning deployment configuration.
   Replace only that canary replica and run first-OOM same-machine recovery,
   second-OOM legacy teardown/replacement, controller restart, and
   authorization-removal rollback with Ray's threshold unchanged.
4. Install a separate exact AWS Spot 16-GB authorization-v3 entry. Verify the
   legacy launch remains Spot with its existing no-on-demand-fallback
   configuration and run first/second OOM. For the terminal-loss race, invoke
   EC2 `TerminateInstances` against only the inventoried canary instance while
   inducing the OOM; after the existing liveness path has durably recorded
   preemption/down, prove no reducer transition or probe can route/recover it
   and legacy replacement wins. This test makes no early-notice claim.
5. Enable the reviewed production authorization only for newly launched AWS
   Spot 16-GB replicas in `boltz-l4-fleet`. GCP, Kubernetes, larger AWS, and
   any fallback/mismatch must persist `ORDINARY` without entering the CAPABLE
   startup barrier. No service split or authority-mode change occurs.
6. Complete one inventoried eligible AWS Spot fleet rollout. Start #1183's
   seven-day clock only after every eligible process/replica is on the approved
   digest and the last authorization-document-v1/v2, runtime-marker-v1, and
   status-only reader is drained. #1183 stays draft.

Rollback requires no public endpoint or security-group change, but the v1 LB
lease reader and controller heartbeat must remain deployed until recovery state
is drained. Remove the server authorization document through Terraform/
Terragrunt first; no newly generated job can arm afterward.
Persist/adopt legacy teardown for every active `CAPABLE` or unresolved
`CANDIDATE` replica and every quarantined or partial-v13 row. A complete
all-row audit must report zero `CAPABLE`, unresolved `CANDIDATE`, quarantined,
or partial-v13 rows before any v12 writer starts; successful cleanup must have
deleted each quarantined row. `ORDINARY` rows with a complete valid v13 bundle
need no recovery teardown. A blocked/ambiguous cleanup or unreadable row blocks
controller rollback. Wait at least 83 seconds after the last capable route lease
renewal, verify every live LB process and admitted retry loop excludes all
marked URLs, and completely drain any process that cannot prove the v1 fences.
Then apply the last
compatible exact digest through the owning infrastructure stack after a clean
reviewed plan. If that old writer
touches an
`ORDINARY` row, it may erase the complete v13 recovery bundle while retaining
the v13 version label; rewritten #1182 recognizes only that all-fields-absent
rollback shape as ordinary on a later re-upgrade. Partial bundles remain
malformed. This compatibility reader is temporary and #1183 removes it once
rewritten #1182 is the rollback floor. After any rollback/re-upgrade exercise,
#1182 owner-fenced rewrites every surviving all-fields-absent ordinary row into
a complete valid v13 bundle; #1183 cannot deploy until an all-row audit reports
zero compatibility-shaped rows.

An already-generated driver remains bounded to one replay after authorization
removal. Therefore rolling below #1182 before active `CAPABLE` and unresolved
`CANDIDATE` replicas are gone is unsupported: an old controller cannot enforce
the fresh-probe fence. The replica-local API-v1 companion table and protobuf
fields remain unchanged across rollback. Central schemas were never changed.
No rollback deletes
action evidence, because this feature creates none.

## Verification plan

### Runtime and driver

- Generated-code tests prove the recovery closure appears only for the exact
  internal intent + consumed fresh AWS evidence, catches only Ray OOM, sets
  `max_retries=0`, creates one new ObjectRef, and makes no API/cloud call.
- Eligibility tests cover actual AWS versus GCP/Kubernetes, <=16 GB versus
  larger shapes, on-demand/Spot authorization binding, fallback, reuse/resume,
  account/region/AZ/type mismatches, single/multi-node,
  owner/generation/task mutations, managed secrets, and
  lease single consumption/rebind/copy/pickle rejection. They also reject a
  readiness interval above 10 seconds or timeout above 15 seconds, a
  non-null AWS `InstanceLifecycle` other than `spot`, a Describe/STS call from a
  different session/profile/workspace/account, and any controller without the
  shared-central-PostgreSQL launch fence.
- Fault injection at every `RecoverySession` transition proves at most one
  `.remote()`, stable event/attempt IDs, adopt-or-cancel, deadline behavior,
  and placement-group removal.
- Monotonic-clock tests prove `.remote()` cannot run before 83 seconds from the
  exact positive cleanup proof even when memory admission finishes immediately,
  while the unchanged local recovery deadline still bounds the attempt.
- Supervisor tests cover subreaper/PDEATHSIG, one-way latch, adopted descendants,
  graceful/forced cleanup, Docker/boot/process identity, empty inventory,
  post-create/pre-start fence, exact-ID removal, atomic markers, pruning, and
  execution-envelope parity.
- Driver memory tests prove the initial cgroup-aware <=16-GiB cap runs before
  API-v1 `ARMED`; unknown/larger totals leave the job ordinary. Separate replay
  tests cover three consecutive samples, threshold-minus-five watermark, and
  zero threshold mutation. Supervisor tests prove marker v2 is unchanged and
  contains no resource-admission fields.
- Arm-gate races cover marker absence before deadline, marker/cgroup checks
  straddling the deadline, DB failure, and a marker or lower-memory observation
  arriving after `DISABLED`; none may produce a late API-v1 `ARMED` row.
- Compatibility tests prove authorization document v3 maps explicitly to
  runtime profile/capability v2, never by numeric equality, while
  `JobSystemRecoveryInfo` API v1, its protobuf, and marker-v2 canonical bytes
  remain unchanged.

### Controller and legacy lifecycle races

- Version-12/13 serialization tests round-trip candidate intent/nonce, launch
  disposition, optional ordinary request ID, exact job ID, monotonic
  subdocument revision, barrier anchors, and nested state. An all-fields-absent
  v13 rollback shape alone defaults ordinary; partial/malformed recovery data
  is isolated per row, forced off-route, logged without raw payload, and fed to
  the existing teardown owner without aborting the fleet read.
- Rollback tests prove a partial-v13 or quarantined row blocks v12 startup until
  legacy cleanup deletes it; only a complete valid `ORDINARY` v13 row may be
  rewritten into the all-fields-absent compatibility shape. Re-upgrade tests
  prove #1182 rewrites that shape into complete valid v13 state before #1183,
  whose reader rejects the removed shape.
- Reducer tables/property tests cover duplicate, stale, skipped, reordered,
  malformed, terminal, teardown, preemption, restart, and fresh-probe events.
- Launch tests crash before/after intent CAS, API-endpoint nonce consumption,
  server request-ID binding, executor scheduling, `sdk.launch` return, backend
  job start, exact result persistence, and job-ID owner CAS. A lost POST
  response adopts only the ID already bound in the exact replica row. Missing
  request/job evidence never invokes latest-job discovery and schedules/adopts
  legacy teardown. A `BrokenProcessPool` after remote job submission marks the
  bound request failed and never requeues the launch entrypoint; the live but
  unassociated candidate remains off-route until legacy teardown. A separate
  typed capacity failure proven to occur before any job/future/driver preserves
  the existing `ExecutionRetryableError` requeue and produces exactly one final
  service job.
- Tests recover job ID only from the exact bound legacy request result and
  reject latest-job, name-only, or mismatched generation results.
- Admission reducer races prove an early exact-job `ABSENT` remains
  `CANDIDATE`; first observations at `ARMED`, every active downstream phase,
  `RETRY_SUBMITTED`, and `EXHAUSTED` reduce to their exact capable/terminal
  states; and only a fresh post-deadline ready probe plus same-cycle
  nonterminal/`ABSENT` re-read persists `ORDINARY`. Forward/backward wall-clock
  jumps and controller restart cannot satisfy the process-monotonic guard
  early. `MALFORMED`/`UNSPECIFIED` schedule teardown. Exact non-AWS overrides
  bypass candidacy, while mixed-fleet GCP/Kubernetes/larger-AWS results use the
  bounded release protocol and never enter the CAPABLE startup barrier. A
  low-initial-delay candidate that succeeds before its application deadline
  remains alive but off-route through the bounded admission hold.
- Topology tests prove a non-consolidated/local-state controller and any
  `enforce_launch_fence=False` controller never persist `CANDIDATE`, never send
  the closed recovery context, and retain ordinary launch behavior.
- Deterministic races start request/job/reducer patches versus teardown in both
  arrival orders, assert every transaction acquires service owner before
  ascending replica rows, prove callbacks never acquire the manager lock even
  while current teardown joins their thread, prove stale whole-row writes
  preserve the latest recovery revision, prove delete-first then stale single/
  batch/paid-capacity completion writes cannot resurrect an absent row, and
  prove write-first then delete leaves no row. Delete, recreate the same numeric
  replica ID with a new record identity, then prove every stale old-identity
  writer and replica-lifecycle deleter aborts without touching the new JSON or
  pickle. Exact paid-capacity claim replay preserves `claimed_at` only for the
  same record identity; a new identity returns ownership loss without changing
  the row, priority, or claim. Insert-only fresh/reserved writers fail rather
  than overwrite a conflict. A stale ready/capable or
  request/job patch cannot land after terminal teardown, exhaustion,
  quarantine, or demotion. A revision conflict must refresh and rerun the pure
  reducer; terminal states remain absorbing. Exactly one existing cleanup
  owner/request is adopted or scheduled. PostgreSQL deadlock detection is not
  the normal serialization mechanism; injected deadlock/serialization aborts
  roll back the whole mutation and the next reconciliation re-reduces from the
  committed revision. No callback retries a partial transition outside its
  transaction.
- Legacy integration tests assert noncandidate/disabled request bodies remain
  byte-compatible. Candidate requests differ only by the enumerated internal
  context, endpoint precondition, and executor-level non-replayability; their
  unchanged body-level `retry_until_up` still retries provisioning inside that
  one execution. After the first request fails, confirmed cleanup and every
  later retry are ordinary and cannot overwrite its association. Cluster
  handles/YAML, provider calls, cancellation, down, failed-cleanup retry, and
  replacement otherwise retain existing behavior.
  No request/action table or column beyond versioned `ReplicaInfo` JSON is
  created.
- Mixed-fleet tests show one service can launch recovery-capable AWS jobs and
  ordinary GCP/Kubernetes jobs while remaining resource-action legacy.
- Route-lease tests cover the fixed five-second independent-probe cadence,
  fixed 15-second hard timeout, immediate per-target publication, two-failure
  token latch, irreversible 60-second expiry, controller/prober pause through
  replay both before and after first activation, normal-result processing
  delayed past its request-start deadline, same-generation token
  non-reissuance, pre-first-readiness OOM with stalled status reconciliation,
  ordered post-probe exact-detail issuance evidence, and exact-attempt
  generation advance. Deterministic composite-worker races block an earlier
  submitted ordinary future while a later `ARMED` or already-adopted
  `RETRY_SUBMITTED` worker completes readiness, exact status, and inactive
  issuance; they prove the actual HTTP callback start rather than submission
  time anchors the deadline. Missing callbacks, malformed/throwing status,
  and probes at or before adoption issue nothing. They cover the closed
  marker/heartbeat schemas,
  authentication/owner fencing, exact record/URL/token matching, token
  issuance, finite/capped
  remaining durations, request-start anchoring, fixed-start heartbeat timeout,
  startup without a heartbeat, expiry, malformed/omitted/timeout/reordered
  responses, controller/LB replacement, bounded pruning, ordinary unmarked
  availability, and occupancy/data-plane filtering. URL A -> B with stalled
  heavyweight sync expires A because only B's token can renew. Same-URL client
  remove/re-add races prove final validation and exact client-generation
  checkout are atomic. `ARMED` remains admission-eligible, while
  `ARMED`/`ACTIVE` -> `DRAINING` during a retry forbids another replica
  selection.
- Route-registry transaction tests cover suspension epochs and late
  pre-suspension completions after rollback; PostgreSQL commit-then-raise
  readback for single/batch writes, patches, and deletes; explicit false
  results; exact proven pre-write restoration; row recreation, known-old UUID
  replay, bounded repeated tombstones, and per-ID overflow isolation. URL
  validation covers scheme/DNS case, default ports, canonical IPv4/IPv6, and
  rejection of credentials, paths, queries, fragments, invalid ports, and
  malformed or ambiguous authorities.
- The adversarial replay regression retains an old ready route, stalls the
  heavyweight controller sync beyond the replay window with no traffic, expires
  its 60-second lease, drains a lease-admitted 10-second pool wait plus
  10-second connect attempt, then binds a replacement on the same URL after the
  83-second driver fence. Every request remains unselectable until exact
  `RETRY_SUBMITTED` adoption creates a new exact attempt generation, a fresh
  later normal readiness reduction issues that generation's one token, the
  independent prober succeeds, and the matching heartbeat lands. A paused old
  generation that resumes after replacement remains retired and instead
  schedules legacy teardown.

### Spot and real 16-GB smoke

For the on-demand smoke, record instance type/RAM, EC2 instance ID, AWS
account/region/AZ, market, both catalog and cgroup memory observations, boot
ID, cluster/job/driver/Ray-session/placement identities, runtime digest, and
Ray threshold. Induce a real Ray memory-monitor kill. Prove the first OOM keeps
the same VM/job/driver and creates one new ObjectRef/attempt; readiness returns
only after `RETRY_SUBMITTED`. Induce a second OOM and prove the existing legacy
teardown replaces the VM. Re-read identity/RAM/threshold unchanged.

Repeat on a fresh AWS Spot 16-GB replica. Prove the existing launch's actual
market is Spot and no on-demand fallback was introduced. First test a pure OOM
with no interruption. Then call EC2 `TerminateInstances` for only the
inventory-verified canary instance while racing an OOM. Capture the existing
controller liveness observation and durable preemption/down transition; all
terminal assertions start at that durable observation, after which no probe or
recovery transition may route the replica and legacy replacement must proceed.
The driver may not classify generic termination as OOM. This deliberately
tests actual provider loss, not an unimplemented early AWS-notice/SQS path.
GCP/Kubernetes jobs remain ordinary controls.

### Negative architecture tests

- Repository guards fail if #1182 adds an API008 migration, OOM-specific
  API-request field, private operation header, another/unbounded L7 dependency,
  protected
  handle/YAML/receipt/proof, resource-action profile/row/worker, direct cloud
  cleanup, another request/queue/lease, a `JobSystemRecoveryInfo` API-v1/
  protobuf field, or a marker-v2 field.
- API/auth tests prove user input or an ordinary service-name match cannot emit
  the controller contract or arm a job. Guessed/replayed contexts, an absent,
  wrong, or already-consumed nonce, request-ID mismatch, unknown context keys,
  and an old server with a v2 authorization document plus a new context all
  fail closed. Executor scheduling cannot begin before the exact endpoint bind
  commits, and the backend rejects an unbound form.
- Telemetry records authorization-document-v1/v2 selection, controller-
  observed runtime-capability-v1 and status-only results, authorization-v3
  candidate/ordinary/capable outcomes, API-v1 recovery/exhaustion, evidence-
  loss fallback, market/provider, and preemption races using bounded nonsecret
  labels. The central controller derives capability-v1 only from an exact
  `PRESENT` job detail; it does not claim direct observation of a remote marker
  read. Structured admission logs are separately bounded and never become
  metric labels.

PR2 adds one low-cardinality counter,
`sky_serve_system_oom_recovery_events_total`, to the existing metrics endpoint.
Its closed `event` label is one of `authorization_v1_selected`,
`authorization_v2_selected`, `runtime_capability_v1_observed`,
`status_only_read`, `authorization_v3_candidate`, `authorization_v3_ordinary`,
`authorization_v3_capable`, `recovery_started`, `recovery_succeeded`,
`recovery_exhausted`, `evidence_lost`, or `preemption_observed`; `provider` is
one of `aws`, `gcp`, `kubernetes`, `other`, or `unknown`, and `market` is one of
`on_demand`, `spot`, `other`, or `unknown`. No service, profile, request, job,
account, region, instance, or reason value is a metric label. Production
monitoring must retain this series for at least eight days before the removal
clock starts. Each of the seven UTC 24-hour gate queries requires both zero
`increase()` for all four deprecated compatibility events and gap-free scrape-
health evidence; counter reset, missing target, or scrape gap resets the clock.
The timestamped query result and eligible-image inventory are retained with
both PRs. Exact per-replica associations remain in current `ReplicaInfo`;
bounded structured logs supply diagnostic correlation but are not lifecycle
authority.

### Local verification evidence (2026-08-03)

- The expanded 33-module changed-test sweep collects 1,782 tests under Python
  3.14. All 1,582 tests in the 29 non-PostgreSQL modules pass. The four real-
  PostgreSQL modules collect 200 tests, including the exact paid-claim replay
  identity regression, but cannot execute because this host has a Docker CLI
  without a Docker socket; they remain a required CI/merge gate rather than
  inferred evidence.
- Repository-wide CI on the preceding tip exposed 36 integration failures in
  legacy mocks/fixtures and the exact paid-claim retry. All 167 affected non-PG
  tests now pass together. The mocks assert the update-only existence fence,
  ordinary fixtures carry explicit recovery state and canonical record IDs,
  fresh-launch tests exercise the insert-only worker seam, and exact-claim
  re-delivery uses identity-fenced UPDATE semantics. The corrected full unit
  lane and real-PostgreSQL result are pending the republished stack.
- Focused architecture, route-lease, route-registry, controller, proxy,
  load-balancer, manager-transaction, backend, driver, AWS-admission, and
  authorization tests pass. They include the composite-worker ordering races,
  commit-then-raise PostgreSQL readback model, probe-epoch suspension fence,
  bounded identity tombstones, strict URL canonicalization, first/second OOM
  production-v2 simulation, and authenticated heartbeat path.
- Mypy reports no issues in the repository's current 868-file configured
  target and pinned Pylint 4.0.4 rates all 20 changed source modules 10.00/10.
  The repository `format.sh` gate passes with YAPF 0.43.0, isort 5.12.0,
  dashboard lint/format, Python compilation, and `git diff --check`. Ruff
  0.15.21, every additional scoped Ruff CI invariant, and the flake8-async
  lifecycle baseline also pass. Cancellation during ambiguous route readback
  retires every suspended route before re-raising and has focused regression
  coverage.
- Two exact adversarial audits accepted the corrected route-lease and runtime
  semantics after the submission-order, stale-route, ambiguous-commit,
  suspension, and identity-history fixes. The negative architecture guard
  confirms the production recovery surface contains no port 4517, SQS,
  EventBridge, Temporal, or application completion-marker authority.
- No production deployment, authorization activation, real AWS OOM injection,
  or provider-termination race has been performed. The real PostgreSQL suite,
  both 16-GB cloud smoke sequences, deployment inventory, rollback exercise,
  and seven continuous UTC days of compatibility telemetry remain blocking
  evidence below.

## PR 3 removal gates

#1183 may merge only after all seven gates are true and exact evidence is
recorded here and in both stacked PR descriptions:

1. A consistent replica-state audit reports zero active unresolved/capable
   authorization-v1/v2 intents and zero ambiguous/unlinked candidate/capable
   replicas, quarantined rows, partial-v13 bundles, or all-fields-absent-v13
   rollback shapes. Every active
   authorization-v3 `CAPABLE` replica has its exact ordinary launch request ID,
   service job ID, runtime profile 2, and matching supervisor-marker/capability
   v2.
2. No authorization document v1 or v2 remains in any rendered deployment,
   secret/config source, or live API/controller environment. Current active
   replica audit plus the retained bounded compatibility-telemetry window—not
   deleted `ReplicaInfo` history—provides removal evidence; deleting the
   current authorization alone cannot satisfy gate 1 or gate 4.
3. Every API/controller and eligible replica image meets the approved
   controller/job-detail/Skylet/library versions and emits only controller
   contract 2 plus unchanged `JOB_SYSTEM_RECOVERY_API_VERSION == 1` and runtime
   profile/marker capability v2. No status-only eligible runtime remains.
4. From completion of one full eligible AWS Spot fleet rollout, compatibility
   telemetry reports zero authorization-document-v1/v2 selection,
   exact runtime-capability-v1 observation, and status-only recovery read for
   seven continuous 24-hour periods. Any hit or eligible image change resets
   the clock. This gate makes no claim about an untransported remote marker
   read; gate 5 independently inventories the marker filesystem.
5. A two-pass remote audit reports zero marker-v1 directories on every active
   eligible VM. The audit first snapshots active eligible replica rows and
   their exact immutable EC2 IDs, exact job IDs, and runtime digests, scans only
   that inventory, then repeats after one full controller probe interval.
   The two inventories must be identical. Any addition, removal, replacement,
   missing row, or unreachable target invalidates both passes and restarts the
   audit from a fresh snapshot; deleted replica history or a nonexistent
   cleanup receipt is never inferred as evidence. Both timestamped inventories
   and every per-target result are attached to the PR evidence. Age pruning
   alone is not evidence.
6. Both real 16-GB authorization-v3/runtime-profile-2/supervisor-v2 smoke
   sequences pass with the Ray threshold unchanged: on-demand first-OOM
   recovery plus second-OOM legacy replacement, and Spot OOM recovery plus the
   exact `TerminateInstances`/OOM race where, from durable preemption/down
   observation onward, legacy replacement wins. GCP and Kubernetes negative
   controls persist `ORDINARY` without the CAPABLE barrier.
7. The supported rollback target is rewritten #1182 on the unchanged legacy
   lifecycle. Terraform/Terragrunt-owned rollback/re-upgrade, authorization
   removal, complete-v13 rewrite, zero-active-capable/unresolved-candidate/
   quarantined/partial-v13/all-fields-absent-v13 all-row audit, legacy teardown
   adoption, and mixed-provider operation have passed without direct-shell
   compatibility.

## Open gates and unresolved decisions

- Freeze the exact on-demand and Spot 16-GB instance types and actual-memory
  observation used by the server authorization. A catalog-only `memory` hint
  is not sufficient if runtime reports more than 16 GB.
- Freeze the exact production service/task/image/authorization digests after the
  current task is digest-pinned. A mutable Docker image remains ineligible.
- Provision the dedicated Spot canary permissions for the exact
  `TerminateInstances` injection described above and record the existing
  liveness observation plus durable preemption/down evidence used by the
  reducer. A generic process exit must not be labeled interruption or OOM, and
  no SQS/EventBridge/early-notice receiver is part of this gate.
- Verify the endpoint's pre-scheduling request-ID bind survives API/controller
  restart and a lost HTTP response, and that only that exact bound request's
  durable result supplies the service job ID. Any uncloseable association gap
  remains ordinary replacement rather than adding a new request protocol or
  latest-job lookup.
- Measure the `ARMED`-before-ready interval on both markets and confirm the
  fixed 35-second visibility/detection budgets against the live 30-second job
  poll.
- Record how the current legacy cleanup reports an already provider-terminated
  Spot VM and its volumes. This design adopts existing behavior; a cleanup gap
  may block rollout but does not authorize new cloud logic inside #1182.
- GCP remains fail-closed and outside authorization document v3. Adding it
  requires immutable numeric instance ID plus project and zone plumbing, an
  exact authorization-versioned identity contract, and a live recovery/
  preemption matrix reviewed before implementation; display names or mutable
  labels are insufficient.
- Keep the separate durable resource-action designs synchronized with the
  explicit non-dependency: this initiative creates no AWS action profile, M4A
  milestone, action row, or action-authoritative service transition.
- No rollout step may add API008, accept the old private header, add a second or
  unbounded L7 fence beyond the v1 recovery-route lease, raise RAM above 16 GB,
  change Ray's threshold, or make an application completion message system
  authority. Any such need reopens this design.
