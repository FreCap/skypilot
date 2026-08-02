# SkyServe System OOM Recovery

_Status: Contract hardening update awaiting exact adversarial re-review; clean
stacked implementation in progress; production activation is blocked_

_Last updated: 2026-08-01_

## Context

A single-node SkyServe VM runs the service command as one long-lived Ray task.
Ray 2.9.3 protects the node when whole-host memory crosses its configured
threshold by killing a Ray worker. Its last-task policy deliberately does not
retry the only task owned by a caller. The service process therefore exits,
the VM and raylet remain alive, and SkyServe eventually interprets failed
readiness as `FAILED_PROBING`, tears down the VM, and provisions a replacement.

This behavior is safe but unnecessarily coarse when terminating the workload
returns host memory and leaves Ray healthy. It also loses the structured Ray
out-of-memory cause between the replica job and the Serve controller.

## Goals

- Recover one eligible service-task host OOM on the same machine without
  provisioning, rerunning setup, or relying on application completion
  markers.
- Preserve Ray's node-protection behavior and fail closed when cleanup, memory
  reclamation, or task resubmission cannot be proven safe.
- Tell SkyServe that a system recovery is in progress so the ordinary
  post-ready probe timeout does not delete the machine during cold restart.
- Persist the system failure reason, occurrence count, and controller recovery
  state across controller replacement.
- Keep the existing teardown/replacement path as the bounded fallback.
- Make process ownership, fresh-machine evidence, remote recovery state, and
  controller reconciliation explicit typed contracts rather than mutable
  booleans, shell inference, or a collection of loosely related fields.
- Ship every temporary compatibility path with its already-authored stacked
  removal PR and objective merge gates.

## Non-goals

- Recovering request-level CUDA OOMs or interpreting application SQS/Temporal
  completion markers.
- Raising or disabling Ray's host-memory threshold, or changing instance RAM.
- Retrying arbitrary user jobs, setup failures, process exit codes, provider
  preemptions, pools, or multi-node services.
- Guaranteeing recovery of unregistered daemon-owned children. An unproven or
  forced cleanup falls back to whole-machine replacement.
- Adding a public recovery configuration or changing the public replica status
  enum in the first implementation.
- Treating a successful application-level completion marker as system cleanup
  evidence. SQS, Temporal, and workload callbacks are outside this recovery
  authority.

## Behavior contract

### Eligibility

The v1 transition rollout is deliberately allowlisted to one trusted
`boltz-l4-fleet` deployment profile. The server-side allowlist entry binds the
active workspace owner, exact service incarnation, and a canonical digest
of the safety-relevant task specification (setup/run commands, environment
names and non-secret values, mount/source identities, and runtime image).
Resource-placement fields that do not change process ownership may vary. The
API server, rather than a user environment variable or service name, derives
the capability from this profile plus its owner-fenced SkyServe launch
context. Recovery is enabled only when all of the following are true:

- the workload is a non-pool SkyServe service launched through
  `CloudVmRayBackend` on a dedicated Linux VM;
- its owner-fenced launch context contains the exact internal controller
  recovery-contract version emitted only by a controller that can persist and
  reconcile the nested state;
- it has one node and one run task/future, and a one-shot typed
  `FreshProvisionEvidence` constructed from this exact provision result proves
  that every requested provider instance ID was newly created and still
  matches the resulting handle;
- the owner, service incarnation, and recomputed safety-profile digest exactly
  match one server-only allowlist entry, including its separately declared
  immutable `sha256:` runtime-image digest; and
- the new remote runtime exposes job-recovery API version 1 and arms the exact
  capability selected by the matched profile version: profile v1 requires
  `subreaper-v1+local-docker-empty-inventory-v1`, while profile v2 requires
  `subreaper-v2+owned-local-docker-v1`.

Only profile v1 may run its initial ordinary shell command safely unarmed after
the fatal parent/boot fence passes; it then has ordinary no-retry behavior. A
v1 replacement and every v2 attempt require a fully armed pre-start handshake.
Any v2 ownership/pre-start failure returns nonzero and selects VM replacement;
v2 never falls back to executing the shell form.

Kubernetes, Slurm, pools, multi-node tasks, managed-secret references, reused
clusters, name-only matches, arbitrary services, and user-supplied opt-ins
retain their current behavior.
An absent or already-consumed provider-created-instance proof (including a
skipped, resumed, orphaned, partial, dry-run, duplicate-submission, or
legacy-provisioner result) is ineligible rather than inferred from cluster
state.

The safety digest binds the effective post-policy `setup`, `run`, and event
callback payloads, environment, file/storage/volume mounts, hooks, and other
task fields, not merely the originally submitted YAML. Inline secret values
are redacted while their names remain bound. Managed-secret references are
ineligible in v1 because their resolved environment and temporary file-mount
provenance is not retained distinctly enough to normalize without also hiding
an admin-policy mutation. The server-assigned numeric Serve replica ID is the
only effective environment value normalized to a stable sentinel; its key and
valid numeric shape remain bound. Generic task-level `container_image` launches
are ineligible: SkyPilot's persistent outer container has different ownership
and cleanup semantics and requires a separate typed plan. For the transitional
direct-Docker v1 profile, every literal `docker run` uses a deliberately closed,
fail-closed CLI form at an actual shell command boundary whose actual image operand is an
`@sha256:<64-hex>` reference. Mutable tags, shell-variable image operands,
unknown/ambiguous Docker options, digest-looking arguments or environment
values, executable names merely ending in `docker`, and multiple runtime image
digests are ineligible. Both attempts use the same captured command closure,
so no post-match transform can swap the image. This arbitrary-shell parser is
deprecated as soon as it lands. The stacked steady-state change replaces it
with a typed, canonical container specification; the stacked removal change
deletes the parser after migration gates pass.

The trusted command contract permits ordinary descendants and one local
Docker engine, but forbids launching work through systemd, containerd, Podman,
a remote Docker context, or another external process manager. This is a
cooperative workload containment boundary, not a sandbox against a malicious
command. Every service update/relaunch recomputes and revalidates the binding;
changing the task invalidates recovery until an operator updates the
server-only profile.

An already-running replica launched before this feature has no generated
recovery path and keeps the current teardown/replacement behavior. This is
intentional rollout isolation rather than a mixed-runtime compatibility
branch.

### Ray-driver retry

Before submitting an eligible task, the generated driver validates the exact
remote job-recovery API version and required typed phase/state/lock operations.
It then creates one `RecoverySession`, which owns the replayable submission
closure, original/replacement ObjectRefs and contexts, event/deadline/phase,
and the only exhaustion path. The private task is submitted with
`max_retries=0`, making the driver the sole retry authority. When `ray.get()`
raises
`ray.exceptions.OutOfMemoryError`, the driver records a structured
`RAY_NODE_OOM` system-failure event in the local job database. It does not
change Ray's memory-monitor configuration and does not ask Ray's implicit
retry policy to override the last-task safeguard. Setup execution and every
other exception retain their existing behavior.

The driver may manually submit exactly one replacement attempt for the service
job. It reuses the existing placement group, command, task environment,
resource request, log path, and Ray driver. It does not execute setup, sync
mounts, call the API server, mutate cloud state, or create a second SkyPilot
job. This makes controller-owner fencing and API-request idempotency irrelevant
to the retry itself: a replacement controller can observe the same remote job,
but cannot submit a duplicate attempt.

Before replay the session verifies that Ray is connected, the original
placement group is still created, and the node boot identity is unchanged. A
new `.remote()` call creates a fresh ObjectRef in the same placement-group
bundle; the failed ObjectRef is never reused. Placement-group removal occurs
exactly once in an outer `finally`, after success or exhaustion. Every
replacement ObjectRef is either durably adopted after `RETRY_SUBMITTED` or
best-effort cancelled and fenced by that placement-group removal.

If the retry attempt receives another Ray OOM, the retry returns a nonzero
code, or any precondition fails, the driver records recovery as exhausted,
atomically sets the existing job status to `FAILED`, and exits. `FAILED_DRIVER`
remains reserved for an unexpectedly dead driver. Existing SkyServe teardown
then owns cleanup and replacement, with structured system-failure detail
taking precedence over user-failure classification.

### Positive cleanup proof

For an eligible attempt, `run_with_log` launches a recovery-only supervisor as
the actual parent of the service command. The supervisor starts a new Linux
session, enables `PR_SET_CHILD_SUBREAPER`, and remains alive while the command
runs. Descendants orphaned by double-forking are therefore adopted by the
supervisor instead of escaping to PID 1. SkyPilot's detached cleanup daemon
still observes Ray-worker death and sends SIGTERM to the command session; the
supervisor continuously enumerates, terminates, waits for, and reaps its
descendants before acknowledging cleanup.

The supervisor installs `PR_SET_PDEATHSIG` before creating any workload child
and records the original Ray-worker PID. Its signal handler sets a one-way
termination latch that can never be cleared. Failure to install the signal, an
already-dead parent, a parent change, or a set latch is fatal. Immediately
before the v1 `Popen`, the supervisor rechecks the latch, live parent PID, and
boot ID; an armed attempt also rechecks its Docker daemon identity and empty
inventory. Any required failure suppresses `Popen` and returns nonzero. This
closes the bug in which a failed handshake could create an orphan service
command. Only a profile-v1 initial attempt that cannot arm for another
capability reason may retain the ordinary non-recovering launch outcome after
that fatal fence passes; its `armed: false` record can never authorize replay.
Profile v2 and every replacement return nonzero without starting when arming
fails.

Before it starts the command, the supervisor proves the local Docker API is
reachable, records Docker's daemon ID plus PID/process-start identity, and
requires the complete local container inventory to be empty. This intentionally
matches the dedicated Boltz replica profile. After host-process cleanup it
requires the same Docker daemon identity and an empty complete inventory over
a short quiescence interval. Thus a foreground `docker run --rm` attempt is
eligible only after Docker itself proves that its container is absent. A
daemon restart, API error, remote context, baseline container, or any remaining
container makes the attempt ineligible or exhausts recovery.

The replacement attempt carries `require_armed_start: true` and the original
Docker identity in its private context. Its new supervisor must complete a
fresh capability handshake, match that original daemon identity, observe the
expected empty baseline, and retain the original boot identity before it
starts any command. A missing marker, an unarmed supervisor, or changed Docker
identity returns nonzero without starting the replacement workload; the driver
then exhausts recovery. Submission of a Ray ObjectRef is therefore not itself
permission for the replacement command to execute.

The supervisor first writes an atomic capability record and later replaces it
with an atomic, attempt-scoped cleanup result. Both bind a random attempt UUID,
exact SkyPilot job ID, task index and attempt number, node boot ID, supervisor
PID/process-start identity, Docker daemon identity, and cleanup timestamps.
The result distinguishes a graceful, completely reaped scope from a timeout
that required SIGKILL or left observable processes alive. The driver accepts
only the UUID and identities it created for that exact ObjectRef; every
attempt uses a new path, so stale files cannot authorize replay.

The driver does not resubmit until all of the following are true:

1. the exact supervisor cleanup result exists and every bound identity matches;
2. cleanup completed without SIGKILL, timeout, or surviving descendants;
3. Docker proves the original daemon is still running and its full inventory
   remains empty for the quiescence interval;
4. Ray's cgroup-aware used/total-memory helpers report host use below
   `min(90%, configured Ray threshold - 5 percentage points)` for three
   consecutive one-second samples; and
5. the combined cleanup/memory wait has not exceeded 120 seconds.

If the configured Ray threshold cannot be read, v1 uses 90 percent, five
percentage points below Ray 2.9.3's default. The watermark is admission
hysteresis, not proof that memory cannot rise again; the single-replay bound is
the final containment mechanism.

The cleanup result does not exist for ordinary jobs and is emitted only when
the backend's internal eligibility decision supplies an attempt-scoped
context. Existing cleanup behavior is otherwise unchanged.

### System-failure transport

The replica-local jobs database adds a companion `job_system_recovery` table,
leaving the positional legacy `jobs` table layout untouched for downgrade
compatibility. Its one row per job stores capability, exact job/event/boot
identity, stable `original_attempt_id`, nullable `replacement_attempt_id`,
reason, occurrence count, timestamps, absolute deadline, and a monotonic phase:
`ARMED`, `WAITING_CLEANUP`, `WAITING_MEMORY`,
`RESUBMITTING`, `RETRY_SUBMITTED`, or `EXHAUSTED`. The OOM handler updates it
under the existing per-job lock. A failed state write forbids replay.

The `RESUBMITTING` transition and fresh `.remote()` call execute while holding
the per-job lock after confirming the job is still `RUNNING`. This serializes
submission with ordinary job cancellation. A controller teardown does not
share this local lock, so an already-live replay may briefly race teardown;
teardown still wins the durable routing state and eventual VM outcome.
Both locked database transitions fail closed on false returns or exceptions.
The deadline is checked before taking the lock, after taking it, and after the
nonblocking Ray submission call. A future returned after the deadline, or one
whose `RETRY_SUBMITTED` state cannot be committed, is never adopted: the job is
atomically exhausted, best-effort cancellation runs after releasing the job
lock, and placement-group removal is the final fence.

The existing job-status gRPC response carries an additive structured detail
map plus an additive per-job detail-status map. The closed detail-status enum
is `UNSPECIFIED`, `ABSENT`, `PRESENT`, or `MALFORMED`: `ABSENT` is a positive
new-runtime observation that no recovery row exists, `PRESENT` requires one
valid typed detail, `MALFORMED` preserves a row/query/conversion failure
without hiding ordinary job status, and `UNSPECIFIED` represents an old
status-only response. The SSH fallback carries the same two maps in its typed
payload. Old readers ignore both additive protobuf fields; new readers retain
ordinary statuses from old responses and classify their missing detail-status
entry as `UNSPECIFIED`. Public job status values do not change.

The feature is born with one explicit runtime capability contract; there is no
per-transition `getattr()` discovery or pre-release backend detail-method
fallback. Mixed old replica runtimes may return `UNSPECIFIED` status without
recovery detail, which can only select ordinary behavior and never authorize
replay. `MALFORMED` is never collapsed into `ABSENT`/`UNSPECIFIED`: an
intent-marked replica is forced off-route and torn down.

SkyServe consumes job status and system-failure details in the same remote
round trip. It never parses log text and does not poll an application queue.

### Durable Serve state

Before scheduling the API launch request, `ReplicaInfo` durably stores a
`system_recovery_launch_intent` containing controller contract version 1, the
exact requested profile version (1 or 2), service hash, and launch generation.
The controller derives this only from the server-owned activation profile and
passes the same owner-fenced values in launch context; the backend still
revalidates the effective post-policy task and may reject recovery. The intent
is retained for the replica's lifetime even if no remote `ARMED` detail is ever
observed.

`ReplicaInfo` also persists an exact `service_job_id` and one nested, validated
`ReplicaSystemRecovery` object in its versioned JSON contract. The tagged
object contains capability, controller state, exact event/boot identity,
stable original/replacement attempt IDs, reason and occurrence count, latest
remote phase, retry-adoption time, recovery/deadline/completion times, and the
one-shot system-event detection deadline. It is the sole controller recovery
state; no parallel flat phase/state strings are introduced.

Old rows default to no launch intent, no service job ID, and no system recovery.
No PostgreSQL schema migration or compatibility projection is required for the
initial feature because the recovery fields have never shipped and replica
state is already stored through the versioned JSON contract.

An intent-marked active row is durably forced off-route during controller
startup before routing/prober threads can run. It remains behind an in-process
controller-generation barrier until the first exact status/detail-status
result for its captured job is reconciled; this includes persisted `ARMED`
and `RECOVERED` rows, not only rows with no nested state. `PRESENT` enters the
typed reducer, `MALFORMED` tears down, and positive `ABSENT` or compatibility
`UNSPECIFIED` releases the barrier to ordinary readiness without authorizing a
replay. A missing exact job is impossible after successful capture and tears
down rather than waiting or using a latest-job lookup.

Strict v13 recovery decoding is isolated per row. If controller timestamps or
tagging are malformed but the exact job/profile/attempt/event/boot identity can
still be validated, the reader sanitizes that identity into typed
`EXHAUSTED` state. If exact identity cannot be reconstructed, it invents no
IDs: the row is returned off-route in the existing failed-cleanup lifecycle so
the VM is torn down and the rest of the fleet continues to reconcile.

When SkyServe observes `ARMED` for the exact current service job, it persists
the capability. A ready capable replica's first failed probe persists a
one-shot detection deadline of 35 seconds, long enough for the 30-second job
status fetcher to consume a just-written system event. This deadline is never
renewed and a successful probe clears it.

When SkyServe observes a newer `RAY_NODE_OOM` nonterminal phase for the exact
job, event, attempt, and boot identity, it persists both that phase and
`RECOVERING`, clears the prior consecutive-probe failure window, and keeps the
replica off-route. `WAITING_CLEANUP`, `WAITING_MEMORY`, and `RESUBMITTING` may
only suppress routing and extend the fixed grace; they cannot authorize
recovery. When the exact replacement attempt advances to `RETRY_SUBMITTED`,
the controller persists its owner-fenced adoption timestamp. At first event
adoption it also persists one absolute deadline equal to controller receipt
time plus the 120-second cleanup budget plus the service version's
initial-readiness delay, with the readiness component capped at 900 seconds.
Repeated polls, controller replacement, and service relabeling cannot extend
it.

The remote driver starts a monotonic visibility timer only after it durably
persists `WAITING_CLEANUP`, the first remotely queryable event phase. It does
not submit the replacement until that phase has been visible for 35 seconds.
The wait uses only the remaining portion of the existing monotonic 120-second
local deadline and never extends it; wall-clock `occurred_at` remains transport
metadata and cannot shorten the fence after a clock jump. This bounded fence
guarantees an alive controller at least one 30-second exact-status poll, while
a restarting controller first installs the durable startup barrier above.
Consequently a replacement cannot become ready entirely between controller
observations and be mistaken for the original `ARMED` attempt.

A successful readiness probe may persist `RECOVERED` and the completion time
only when its probe start timestamp is later than owner-fenced adoption of
`RETRY_SUBMITTED` for the exact replacement attempt. The write is a
compare-and-set from that exact current `RECOVERING/RETRY_SUBMITTED` event and
requires that no down, purge, preemption, or scale-down intent exists; it then
returns the existing replica to `READY`. A 200 response from the old attempt
while cleanup is still in progress remains off-route and cannot advance the
event.
`RAY_NODE_OOM/EXHAUSTED`, a second occurrence in the same service job, a
terminal job while recovery is active, identity mismatch, or expiry of the
absolute deadline persists `EXHAUSTED` and immediately schedules the existing
replica teardown path.

Only the exact current identity and a newer monotonic phase may advance state.
Repeated status polls and controller replacement are idempotent. `EXHAUSTED`
and every teardown intent are terminal; a stale nonterminal detail for an
event already persisted as `RECOVERED` or `EXHAUSTED` cannot reopen recovery.

## Architecture

The hard safety mechanisms are intentionally conservative. The clean feature
stack implements typed evidence, session, identity, and controller state from
the outset; the never-shipped mutable boolean, dynamic API discovery, and flat
controller representation are not ported merely to remove them later. The
only intentional migration debt is direct-shell workload ownership needed for
the first Boltz canary, and its draft removal PR is created with the feature.

### Structured workload ownership

Profile document version 2 replaces inference over an arbitrary `docker run`
shell fragment with an `OwnedContainerSpec`. The spec contains a
digest-pinned image reference, tokenized and validated container-create
options, the container argv, and the exact inherited environment names. The
effective post-policy `task.run` must be exactly the canonical renderer of one
foreground, attached container invocation. Surrounding shell commands,
pipelines, substitutions, redirections, detach/interactive/TTY behavior, and
caller-selected name, label, CID file, restart, auto-remove, signal-proxy, or
attach policy are ineligible. Unknown or ambiguous options, mutable images, a
remote Docker context, a Docker-socket mount, or another external ownership
mechanism are also rejected. Every accepted create option has one documented,
lossless `docker run` to `docker create` mapping; an option without such a
mapping is rejected.

The typed `RecoveryExecutionEnvelope` preserves the code-generated behavior
that normally surrounds `task.run`: the exact Ray environment removals,
SkyPilot task environment, log stream, signal behavior, rclone flush postlude,
and original workload exit code. The v2 codegen path does not call the ordinary
string `build_task_bash_script()` and then discard parts of it. Instead, it
passes the validated spec plus code-owned prelude/postlude metadata to the
supervisor, which applies the environment changes, runs one attached container,
executes the same rclone flush logic after exit, streams both outputs to the
same log, and returns the original container exit code after the postlude.

The supervisor, not the user command, supplies the attempt name/labels and
explicit lifecycle: `docker create`, `docker start --attach`, and exact-ID
removal. User `--rm` is therefore rejected rather than silently reinterpreted.
The supervisor records Docker's returned full container ID; cleanup targets
that ID and still requires the original daemon identity plus stable empty
baseline/final inventory. Profile v2, marker schema v2, and capability
`subreaper-v2+owned-local-docker-v1` coexist with their v1 readers only in the
transition PR. The removal PR accepts only v2 and deletes the direct-shell
scanner.

Parent-death, boot, daemon, and empty-baseline preflight completes before
`docker create`. Once creation returns an ID, the supervisor owns removal of
that ID even if attach/start fails or a termination signal arrives between
create and start. A v2 cleanup marker cannot be positive without proving the
exact ID absent and the full inventory stably empty.

Immediately after `docker create` and immediately before `docker start`, the
supervisor performs a second gate: the termination latch must still be clear;
the original parent must still be live and unchanged; boot and daemon identity
must still match; and the complete Docker inventory must equal exactly the one
new full container ID. On any failure it suppresses start, removes only that
ID, proves stable empty inventory, writes no positive capability/cleanup result,
and returns nonzero. Only after attached `docker start` has successfully
created its child does the supervisor begin atomically publishing `armed:
true`. A signal before start suppresses the workload and cannot publish a
positive capability. A signal after start, including during that atomic write,
may leave a positive capability only when the same attempt subsequently
publishes exact positive cleanup; without both records it takes the VM
fallback. A signal observed at any point is terminal for a not-yet-started
command and cannot be cleared by later successful checks.

Before v2 activation, canonical round-trip tests prove
`parse(render(spec)) == spec`, and differential execution tests compare v1 and
v2 environment, logs, signals, rclone-flush behavior, and exit status for the
exact Boltz command. Any unmodelled difference keeps the v2 profile disabled.

### One-shot provisioning and service-job evidence

`RetryingVmProvisioner` constructs an immutable `FreshProvisionEvidence`
payload directly from the exact successful `ProvisionRecord`. It binds request,
display/provider cluster names, cluster hash, provider, requested node count,
head and complete created-instance IDs, and the owner-fenced service name and
hash. Creation rejects an existing cluster, resumed IDs, partial or duplicate
creates, a head outside the created set, dry runs, and legacy/skipped results.

The payload is held only by a non-copyable, non-serializable
`FreshProvisionEvidenceLease`. The lease owns a private lock and an atomic
`take()` that clears its sole slot before returning the payload; aliases share
the same consumed state, and copy/deepcopy/pickle fail. Under the backend's
submission-decision lock, the first decision takes the lease whether or not the
profile matches, revalidates the payload against the resulting handle, and uses
it only within that call. Raw payloads are not accepted by any later eligibility
API. A second take, a handle rebind, or a lease surviving registration/provision
reset is ineligible. No generic configuration key or mutable backend freshness
boolean is introduced.

Before calling the launch API, the controller owner-fenced compare-and-sets the
exact launch intent described above. Failure to persist it means no recovery
contract is placed in launch context and the request is not recovery-capable.
The persisted marker is historical launch evidence, not proof that the backend
profile matched.

The owner-watched launch path also retains the exact service job ID returned by
the launch request and persists it with an owner-fenced compare-and-set before
declaring launch complete. Because the generated driver may already be running
when that result arrives, a missing, malformed, or failed owner-fenced capture
does not pretend to disable the driver retroactively. It forbids controller
adoption, keeps the replica off-route, and immediately schedules ordinary VM
teardown; the already-generated driver may still complete its bounded replay
before teardown wins. Pools remain outside this feature. The controller never
uses a "latest job" query to discover a recovery-capable service job.

### Typed driver recovery session

The remote runtime exports one exact `JOB_SYSTEM_RECOVERY_API_VERSION`. A
cached validator checks that version, all required phases and data types, and
the callable arm/transition/exhaust/lock surface before an eligible task is
submitted. Production code then calls that surface directly; it does not use
`getattr()` to discover a partially compatible API at each transition.

A `RecoverySession` owns the original and optional replacement attempt
contexts/ObjectRefs, event ID, occurrence count, deadline, phase, transitions,
and the only terminal `exhaust(reason)` path. Every submitted replacement ref
has exactly two outcomes: it is durably adopted by `RETRY_SUBMITTED`, or it is
best-effort cancelled and fenced by placement-group removal. The replacement
supervisor's mandatory pre-start handshake and original Docker-identity check
are part of this adoption protocol.

`JobSystemRecoveryInfo` and its additive protobuf representation are introduced
with stable `original_attempt_id` and nullable `replacement_attempt_id`; no
phase-current attempt field is introduced. Local microphases
(`WAITING_CLEANUP`, `WAITING_MEMORY`, and `RESUBMITTING`) remain useful for
fail-closed persistence but map to one controller `RECOVERING` observation.

The identity transition is exact. `original_attempt_id` is allocated before
the initial ObjectRef is submitted, is the only original ID later persisted,
and never changes.
`event_id` is allocated once on the first typed OOM and never changes.
`replacement_attempt_id` is allocated and durably stored by the
`RESUBMITTING` transition before `.remote()`; it is never cleared or replaced,
including when submission fails or recovery exhausts. `RETRY_SUBMITTED` and
every later observation must match both attempt IDs. A second typed OOM
increments the occurrence count while retaining the same event and attempt
IDs, then exhausts. Any attempted mutation, clearing, or identity widening
fails the local transition and reduces controller state to `EXHAUSTED`.

Attempt directories remain under
`~/.sky/system_oom_recovery/<job>/<attempt>/`. Exact terminal directories are
retained for diagnostics and age-pruned after 24 hours; incomplete directories
are never positive evidence, and pruning validates that every target remains
under the permanent recovery root.

### Typed controller state and pure reconciliation

`sky/serve/system_recovery_state.py` owns validated observations, the nested
`ReplicaSystemRecovery` tagged state (`ARMED`, `RECOVERING`,
`RETRY_SUBMITTED`, `RECOVERED`, or `EXHAUSTED`), and pure
`reduce_remote_observation()` / `reduce_probe_result()` functions. Reducer
results contain the new state plus explicit off-route, clear-probe-window, and
teardown actions; database writes, routing changes, and VM teardown remain in
the replica manager. Invalid identity/state combinations cannot be constructed
and malformed active state reduces to `EXHAUSTED`, never to no recovery.

`ReplicaInfo` version 13 stores the durable launch intent, exact service job ID,
and one nested recovery object with stable original/replacement identities.
Version 12 and older rows default all three fields to `None`. Because no flat
recovery representation has shipped, no dual reader/writer, backfill, or
rollback projection is added.

## State sequence

```text
READY (or initial startup)
  | Ray host OOM; worker killed
  v
driver records RAY_NODE_OOM/WAITING_CLEANUP
  | cleanup proof + safe memory watermark
  v
driver resubmits same Ray task once
  | SkyServe observes event
  v
RECOVERING (off-route, initial-delay readiness grace)
  | readiness succeeds                 | retry/precondition/readiness fails
  v                                    v
RECOVERED -> READY                 EXHAUSTED -> existing sky.down/replacement
```

## Invariants and races

- Ray remains the sole authority that decides when node memory is unsafe.
- At most one replacement attempt exists per service job. The original Ray
  driver owns the counter and submission closure, and Ray implicit retries are
  disabled for this private task, so controller failover cannot duplicate it.
- A retry never provisions, starts, or changes a cluster.
- Readiness never routes a recovering replica until a fresh probe succeeds.
- A recovery grace is durable and cannot restart from zero after controller
  replacement.
- Teardown, purge, preemption, or service-owner loss wins the durable state,
  routing, and eventual VM outcome. An already-live driver replay may race
  teardown briefly; no controller can submit another replay, and terminal
  controller state cannot be reversed.
- Owner-fenced launch-intent/job-ID callbacks may block on the manager lock,
  so teardown never joins a launch thread while retaining that lock. It first
  persists teardown intent, temporarily releases the lock for the bounded
  join, then re-acquires and re-reads before continuing; a deterministic race
  test covers both persistence callbacks.
- An ambiguous supervisor or Docker cleanup is failure, not permission to run
  a second workload.
- A replacement command never starts unless its own supervisor arms and proves
  continuity with the original boot and Docker daemon. Parent-death setup
  failure never starts either attempt's command.
- The feature creates no network listener and reserves no local TCP port. In
  particular, it has no dependency on port 4517.
- Ordinary user job semantics and Ray retry semantics remain unchanged unless
  the backend selects the internal allowlisted Serve profile.

## Known conservative limitations

- The 35-second event-detection hold is installed only after `ARMED` capability
  has been observed. A service that becomes ready and OOMs before the first
  status poll can take the old probe-failure teardown path. Boltz's long startup
  makes pre-ready capability adoption the expected path; the real-VM smoke test
  must confirm it before activation.
- Ray's `.remote()` API has no safe synchronous cancellation point around the
  local submission call. The driver checks the deadline immediately after it
  returns; a late future is never adopted and is cancelled/fenced by placement
  group removal. A GCS call that never returns can still delay local
  cancellation, while external Serve teardown remains authoritative.

Both limitations reduce the recovery rate by falling back to machine
replacement. None widens eligibility, creates a second replay, or routes a
recovery attempt without fresh readiness.

## Alternatives considered

### Controller-side `sky exec`

`sky exec` has the desired no-setup behavior, but it lacks a durable Serve
owner/version/replica fence and its queued API request can outlive the
controller that issued it. Adding a fully fenced internal exec operation also
requires restart generations, exact job IDs, request adoption, and fail-closed
cluster-identity checks. Keeping retry inside the still-live Ray driver is
smaller and removes those duplicate-submission boundaries.

### Reusing `sky launch`

The launch path is owner-fenced, but it is not constrained to an existing
physical machine. A missing or changed cluster may cause provisioning or setup
work, violating same-machine recovery.

### Ray `max_retries`

Ray 2.9.3's last-task worker-killing policy deliberately returns
`should_retry=false` for the sole task of a caller, even with infinite task
retries. It also cannot coordinate SkyServe's readiness grace or verify
external process cleanup.

### Blind process or port checks

A closed port, a dead Docker CLI PID, or a lower memory reading alone does not
prove that a detached daemon-owned workload has exited. Recovery therefore
requires a graceful cleanup-daemon acknowledgment and the memory watermark;
forced or ambiguous cleanup retains VM replacement.

### Raising the memory threshold or adding RAM

Those may reduce event frequency but do not provide recovery semantics. The
memory monitor remains enabled at its configured threshold, and RAM sizing is
outside this change.

## Stacked implementation and migration

The implementation is delivered as a linear stack from current
`origin/improvements`. The cleanup change is authored with the feature instead
of being deferred to a TODO. It remains draft until every objective removal
gate passes. PR URLs are recorded here as soon as the clean branches are
published.

### PR 1: `[Serve] Add typed runtime for bounded system-OOM recovery`

_PR: pending clean stacked-branch publication_

This introduces the server-only v1/v2 profile matcher, typed
`RecoveryLaunchPlan` and one-shot `FreshProvisionEvidence`, v1 direct-shell and
v2 `OwnedContainerSpec` supervisor modes, marker cleanup/pruning,
`RecoverySession`, stable original/replacement identities, companion job state,
and additive gRPC/SSH transport. Parent-death setup is fatal before any
workload/container start, and every replacement supervisor must arm against the
original Docker identity before starting. No mutable freshness boolean,
phase-current attempt field, dynamic runtime API lookup, or flat controller
state is introduced. PR 1 does not emit the required controller-contract value
in launch context, so merging this foundation cannot activate recovery by
itself even if a profile is accidentally present.

Merge gates are an adversarial PASS for the exact design and diff; focused
unit, format, type, lint, and protobuf-regeneration checks; fault injection at
every `RecoverySession` transition proving at most one `.remote()` and
adopt-or-cancel behavior; proof that provision evidence cannot survive,
rebind, or be consumed twice; v1/v2 supervisor identity/cleanup tests; and no
ordinary-job behavior change.

### PR 2: `[Serve] Adopt typed system-OOM recovery in SkyServe`

_PR: pending clean stacked-branch publication; base is PR 1_

This persists the owner-fenced launch intent before the API request and the
exact service job ID after the launch result, introduces `ReplicaInfo` version
13 with one nested recovery object, adds the pure observation/probe reducers,
and integrates their explicit off-route/teardown actions into job status and
readiness processing. Version 12 and older rows default to no recovery. It also
adds compatibility-path counters for the direct-shell profile, v1 markers, and
status-only old runtimes so removal is measurable.
Only this PR begins emitting the exact internal controller-contract value that
allows the backend to generate a recovery-capable driver.

Merge gates include reducer table/property tests over valid, stale, skipped,
reordered, malformed, and terminal sequences; version-12-to-13 storage and
pickle tests; pre-request launch-intent persistence, controller restart, and
owner-fence tests; exact service-job capture with no latest-job fallback;
old/new gRPC and SSH combinations; fresh
post-`RETRY_SUBMITTED` probe fencing; and teardown-precedence tests.
Missing/malformed/owner-CAS-rejected launch results must be tested to stay
off-route and schedule teardown even while a bounded driver replay is live.
Production activation is separate: the real 16-GB smoke must show the same
provider instance, boot ID, SkyPilot job, and Ray driver; a new ObjectRef and
replacement attempt; fresh readiness only after `RETRY_SUBMITTED`; and ordinary
VM replacement after a second OOM. Ray's threshold remains unchanged. Canary
and rollback are respectively an exact service-hash profile and removal of
that profile.

### PR 3: `[Serve] Remove deprecated direct-shell OOM recovery`

_PR: pending clean stacked-branch publication as draft; base is PR 2_

This is the already-authored migration completion. It accepts only profile and
marker v2, removes direct-shell Docker inference and its capability reader, and
removes the status-only old-runtime compatibility path once the minimum remote
version makes it unreachable. It retains stable protobuf fields, the additive
structured status map, and gRPC-to-SSH transport.

It may merge only after all of the following are true:

1. A PostgreSQL audit of the durable launch-intent marker reports zero active
   replicas requested under profile v1 and zero unobserved/ambiguous launch
   intents. Every remaining v2-marked active replica reports v2 capability,
   stable attempt identities, and its exact persisted service job ID.
2. No v1 profile entry remains. Removing the entry cannot satisfy gate 1
   because the per-replica historical launch intent is retained independently
   of the current profile document.
3. All API/Serve controllers and replica images meet `SKYLET_VERSION >= 42`
   and `SKYLET_LIB_VERSION >= 9`, expose
   `JOB_SYSTEM_RECOVERY_API_VERSION == 1`, and emit controller recovery-contract
   version 1.
4. Compatibility telemetry reports zero direct-shell profile, v1-marker, or
   status-only recovery reads for one full fleet rollout plus seven days.
5. A remote audit reports zero v1 marker directories on every active eligible
   VM; age pruning alone is insufficient for this gate.
6. The real 16-GB v2 `OwnedContainerSpec` smoke, including second-OOM VM
   replacement, has passed with the Ray threshold unchanged.
7. The supported rollback target is PR 2, which remains v2-capable; rollback to
   the direct-shell path is no longer required.

## Deprecation and removal ledger

Only genuine rollout compatibility is introduced. Code comments use these
same removal gates rather than open-ended TODOs.

| Deprecated path | Transition behavior | Removal |
| --- | --- | --- |
| Profile document v1 and direct-shell parser (`_tokenize_shell()`, `_is_docker_run_start()`, `_is_closed_docker_command_start()`, option tables, and `_docker_run_image_digest()`) | Exact digest-pinned transitional Boltz command only | Profile v2 canonical `OwnedContainerSpec`; v1 reader and parser removed in PR 3 |
| Marker schema v1 and capability `subreaper-v1+local-docker-empty-inventory-v1` | Positive proof for direct-shell attempts | v2 is preferred from PR 1; v1 reader removed in PR 3 after marker/telemetry gates |
| Status-only recovery decoding for old replica runtimes | Missing detail can only fall back to VM replacement | Removed in PR 3 after the minimum-version and telemetry gates |

The mutable `system_oom_recovery_fresh_cluster` key/boolean, boolean-only
codegen selection, per-transition `getattr()` API discovery,
`get_job_status_with_details` duck-typed fallback, phase-current attempt field,
and flat `ReplicaInfo.system_recovery_*` representation are explicitly rejected
pre-release and never enter this stack.

The following complexity is permanent unless a later independently reviewed
design replaces its safety property: the companion local recovery table;
gRPC-to-SSH transport; server-only exact authorization; driver-owned
`max_retries=0` and one replay; subreaper and parent-death fencing; attempt
markers; Docker daemon/container-absence proof; memory hysteresis; boot,
Ray-session, and placement-group checks; per-job lock and durable
`RESUBMITTING`; fixed deadlines; off-route controller state; fresh
post-submission readiness; and teardown precedence.

## Deployment and rollback

The behavior activates only for newly submitted eligible service jobs whose
generated driver contains the internal recovery path and whose remote
supervisor completes the runtime handshake. Existing replicas continue with
the old behavior until naturally replaced or explicitly updated.

Activation additionally requires a server-only profile document in
`SKYPILOT_INTERNAL_SERVE_SYSTEM_OOM_RECOVERY_PROFILES`. Every entry contains a
nonempty profile ID, workspace, service name, exact service hash, canonical
task SHA-256, and immutable runtime-image `sha256:` digest. Version 2 also
contains the canonical `OwnedContainerSpec`. Missing, malformed, or stale
entries silently fail closed to ordinary replica replacement. The production
service command must be digest-pinned before either version is enabled.

The version-13 replica JSON extension is additive; version-12 readers ignore
the new keys, and version-13 readers default missing keys. The companion job
table remains separate from positional `jobs` inserts. New gRPC fields are
additive and old runtimes return no recovery details.

Rolling back the controller leaves an already-generated driver bounded to one
retry, but an older controller cannot enforce the fresh-probe fence. The
supported rollback procedure therefore first removes every recovery profile,
explicitly tears down every active replica carrying a recovery launch intent
regardless of whether `ARMED`, nested state, or service-job capture was ever
observed, audits that zero such active rows remain, and only then deploys the
older controller. Emergency rollback uses the same launch-intent inventory and
chooses immediate VM replacement before the version change. Rolling back
replica runtime code removes the private retry path for new launches. After PR
3's gates pass, rollback targets PR 2 rather than a direct-shell-capable
release.

## Verification plan

- Generated-code unit tests prove the retry closure is present only for the
  internal allowlisted profile, catches only Ray OOM, sets `max_retries=0`,
  creates a fresh ObjectRef, and permits one replay.
- Driver tests inject Ray OOM and verify cleanup proof plus memory watermark
  gates resubmission; missing, forced, stale, and timed-out cleanup results
  exhaust recovery.
- Supervisor/daemon tests verify subreaper activation, nonce and process
  identity binding, atomic graceful/forced cleanup results, adopted orphan
  reaping, Docker daemon/inventory stability, fatal parent-death setup before
  `Popen`, the latched v1 pre-`Popen` gate, the post-create v2 gate and exact-ID
  cleanup, mandatory replacement arming/original-daemon continuity, and no
  ordinary-job behavior change.
- Job-library tests verify database migration, atomic event updates,
  gRPC/protobuf round trip, SSH fallback, per-job
  `ABSENT/PRESENT/MALFORMED/UNSPECIFIED` parity, and old-runtime status-only
  responses.
- Replica serialization tests verify old-row defaults and round-trip of the
  historical launch intent, exact service job ID, and nested recovery state;
  malformed recovery state quarantines only its row and cannot crash a fleet
  read.
- Replica-manager tests verify capability adoption, the one-shot detection
  hold, exact-identity/phase adoption, duplicate-poll idempotency, fixed
  absolute grace, a stale 200 from the draining old attempt, post-submission
  fresh-probe enforcement, owner-fenced successful recovery, second OOM,
  terminal job, timeout, preemption, purge, and monotonic teardown precedence.
- Steady-state tests validate canonical `OwnedContainerSpec` rendering and
  full execution-envelope differential semantics, exact container-ID cleanup,
  atomic aliased-lease consumption, runtime API version rejection,
  `RecoverySession` transition fault injection, stable attempt IDs, reducer
  properties, v12/v13 storage compatibility, rollback audits that include
  intent-marked replicas never observed as `ARMED`, and marker pruning.
- A single-node VM smoke test records instance identity, induces a Ray
  memory-monitor worker kill, verifies the same instance and job driver remain,
  observes a new task attempt and fresh readiness, and then induces a second
  OOM to verify normal VM replacement.

Pre-port prototype verification evidence on 2026-08-01 (not a substitute for
the clean-stack gates):

- the focused eligibility/backend/runtime recovery suites pass, including
  mutable-image decoys, effective-command mutation, provider-proof handoff,
  effective post-policy environment/mount mutation, non-Docker executable
  rejection, fast-start capability publication, locked database exceptions,
  submission deadline expiry, cancellation preservation, and replacement wait
  failures;
- the combined affected Serve/backend/skylet suite completed with three
  order-sensitive failures that passed immediately in isolated reruns; and
- its remaining unreadable-file test is an existing root-permission artifact
  (root can read a chmod-000 fixture), unrelated to this change.

## Open gates

- Adversarial review of the exact revised behavior and architecture passed on
  2026-08-01. Each PR still requires review against its exact diff and this
  synchronized design.
- The real-VM smoke test on the existing 16-GB shape remains required before
  either v1 activation or the v2 profile flip. It must verify same provider
  instance ID, same boot ID and SkyPilot job, a new Ray ObjectRef/attempt,
  fresh readiness after `RETRY_SUBMITTED`, then ordinary VM replacement on a
  second OOM.
- PR 3 remains draft until every numbered removal gate in this design is
  evidenced and recorded here. Publishing the draft is not permission to merge
  it.
- Replace the pending PR entries above with cross-linked URLs after clean
  stacked branches are published.
- No rollout step may raise instance RAM or relax/disable Ray's memory monitor.
