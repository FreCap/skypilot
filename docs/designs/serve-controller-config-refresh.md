# Atomic Serve controller configuration refresh

Status: implementation and pre-merge validation complete; production rollout
is pending image/chart publication, schema migration, and the restart and
placement canaries below

Last updated: 2026-08-05

## Goals

- Make a consolidation-mode `serve update` admit and launch the new version
  under the exact policy-admitted controller configuration submitted with that
  update. In particular, changes to workspace Kubernetes contexts must update
  the immutable placement catalog instead of remaining frozen at `serve up`.
- Commit the service YAML, placement catalog, and a crash-recoverable controller
  configuration generation in one PostgreSQL transaction.
- Recover the configuration belonging to the version selected by quarantine
  and durable controller-application history, rather than traffic readiness or
  a service-global latest snapshot.
- Keep credential-capable arbitrary configuration out of PostgreSQL, shell
  command arguments, and command logs.

## Non-goals

- Persist arbitrary plugin configuration, Kubernetes pod templates, Docker run
  options, provider `create_instance_kwargs`, or other free-form command and
  metadata fields. Those fields remain available to the initially admitted
  controller process but are intentionally not durable across controller-child
  or API-pod recovery.
- Make old and new binaries accept Serve updates concurrently. The rollout
  deliberately pauses updates until all API pods and existing consolidated
  controllers expose the same protocol capability.
- Change non-consolidated controller update behavior.
- Provide provider-visible fencing or exactly-once cloud APIs after the API
  process or its PostgreSQL advisory-lock session is lost during an already
  accepted cloud request. Such an ambiguous request is detected and reconciled
  from its durable replica row; it is not claimed to have made no external
  side effect.

## Public and operator contract

Consolidated controllers expose config-snapshot protocol version 1. Before
allocating a service version or preparing storage, the API requires that
capability. The API then stages the complete policy-admitted controller YAML in
the owning controller filesystem and submits its SHA-256 plus a random 256-bit
request nonce to a distinct update endpoint. The controller verifies and parses
the exact staged bytes, validates the durable workspace, and builds the
placement catalog under that request-local configuration.

The durable recovery projection is an explicit allowlist of controller routing,
workspace, cloud, pricing, Serve, and jobs settings. It recursively removes
free-form credential- or command-capable fields and membership metadata. The
projection is stored once per immutable `version_specs` row with its SHA-256
and request nonce. The source digest is not persisted because a digest of
stripped low-entropy credentials would be an offline verifier. A retry with no
raw staged bytes is recognized by the unguessable committed nonce and can only
acknowledge the already-committed immutable version.

The durable workspace is an authorization boundary, not just a label. Every
raw, projected, and recovered snapshot must name the service's exact workspace;
for a non-default workspace it must also retain that workspace's mapping.
Deleting a custom workspace therefore rejects the update before commit instead
of falling through to broader top-level policy.

Candidate and recovered snapshots are parsed in a side-effect-free mode.
In particular, parsing cannot publish database-routing environment variables;
only the process's normal authoritative config initialization may do that. A
snapshot rejected before its transaction commits therefore cannot alter global
database selection for later requests.

Raw staged bytes originate in a mode-0600 temporary file. Immediately after
transfer, before update submission, the controller opens the stage without
following symlinks, proves it is one regular file no larger than 1 MiB, tightens
its descriptor to mode 0600, and verifies the exact digest. Admission repeats
that descriptor-based verification. The raw stage is short-lived and is
promoted for the normal, no-crash path only. Every supervised child respawn and
full API-pod recovery deliberately rewrites the live config from the
PostgreSQL-bound safe projection before starting the selected version. This
makes recovery deterministic and prevents a forged pod-local receipt from
changing stripped fields. Operators who require a non-durable field after
recovery must submit a new update once the process is healthy.

After the dead child has been reaped and the elected safe projection has been
installed under the config lock, recovery removes the initial unversioned raw
file, every non-elected live generation, every live receipt, and exactly named
regular-file temporaries left by an interrupted atomic receipt write. It
preserves the elected safe file, unrelated dotfiles, and exact staged
candidates still governed by the commit-aware orphan sweep. Source digests
therefore do not accumulate as offline credential verifiers, and historical
credential-capable bytes do not survive the recovery boundary promised above.

Endpoint-local cleanup is backed by a controller-owned orphan sweep. The first
sweep runs on every child start and repeats every minute under the same lock as
config admission. It considers only exact staged-config filenames in the
service incarnation directory, including the request's 256-bit nonce, waits two
complete update/ambiguity-cleanup budgets, and deletes a stage only when the
matching immutable version row still has NULL YAML. It fingerprints and
rechecks the file before unlinking, so a concurrently refreshed path is not
collected. The sweep also age-gates, fingerprints, and rechecks exactly named
regular-file temporaries left by a hard-killed receipt writer; these are never
protocol inputs and require no database lookup. A database error preserves raw
stages. Thus an API-process or controller-child crash cannot retain
credential-capable raw bytes or source-digest verifiers indefinitely, while a
delayed or already committed update cannot be collected.

## Architecture and invariants

Revision 036 adds nullable `controller_config`, `controller_config_digest`,
`controller_config_snapshot_id`, and `controller_applied_at` columns to
`version_specs`. Null config means the version predates this protocol; null
application time means there is no durable proof that the controller completed
that runtime generation. New `serve up` writes version 1 with a sanitized
snapshot and bootstrap receipt. A recovered controller owner records only the
exact generation it successfully reconstructed. On the first protocol update
of an older service, the controller sanitizes and validates its currently
loaded configuration, and the version commit transaction both records the
exact legacy runtime generation and backfills every earlier committed null
snapshot before writing the new version. This is sound because the historical
behavior never refreshed controller configuration on update: every such
earlier version used that same frozen snapshot. It does not claim that
coalesced intermediate versions were ever applied.

The following invariants are transactional or fail closed:

1. A committed protocol version has exact YAML, submitted YAML, placement
   catalog, sanitized config bytes, digest, and nonce. A protocol retry is
   acknowledgement-only and must match those fields; it never rewrites the
   already-committed authoritative spec or re-applies an older runtime policy.
2. The service-global HA script contains an explicit versioned-recovery marker,
   environment setup, and the launch command. It contains no base64
   configuration payload. Recovery never infers the protocol from incidental
   shell text. The first protocol update strips the known legacy config-restore
   line in the same transaction that backfills old versions.
3. Recovery and launch authorization use the same quarantine-aware election.
   Ordinarily the latest applicable commit wins. If a newer quarantine
   dominates, fallback is the newest committed, non-quarantined row with a
   durable controller-applied receipt, never `active_versions` (which is empty
   at scale-to-zero and has traffic-routing semantics). Recovery obtains the
   election inputs and selected spec from one SQL snapshot, verifies that row's
   config digest, writes it atomically with mode 0600, parses it without logging
   values, and only then launches `_start` or a replacement child. System OOM
   recovery authorization and lifecycle metadata use that same elected row;
   they never pair a fallback generation with `services.current_version`. The
   HA liveness sweep obtains workspace, protocol activation, and the elected
   recovery generation for all services in one statement. Immediately before
   each launch, one incarnation-fenced statement revalidates the controller
   owner and election and returns the exact selected config tuple and recovery
   script. The caller installs that supplied tuple directly, without a second
   config or election read that could tear across transactions. Once a dead
   child is reaped, the same locked recovery pass scrubs every historical live
   raw generation and receipt before spawning its replacement.
4. Config publication and the replica-manager version transition occur while
   holding the same manager launch mutex. Every queued launch worker captures
   immutable config bytes and its versioned config path at construction. Before
   every cloud request it checks its manager generation, and the API scheduler
   plus persisted execution entrypoint atomically check the exact
   quarantine-aware elected launch version and controller owner. An old worker
   can therefore neither inherit a newer policy nor cross a version transition.
   The persisted request repeats that same database check at every outer
   provisioning attempt and before creating its local INIT record. At the
   terminal boundary it acquires a per-service shared PostgreSQL advisory lock,
   rechecks the exact fence, and retains that lock across the complete opaque
   built-in provisioner or legacy `ray up` call, including provider-internal
   waits and retries. Every database mutation that can invalidate the fence --
   committed/elected config, quarantine or application receipt, controller
   owner, launch-blocking status, deletion, or same-name creation -- first
   obtains the exclusive form of that same advisory key on the exact
   transaction that performs the mutation. A dead writer connection therefore
   rolls back its mutation and releases its lock atomically; waiting writers
   use the dedicated advisory NullPool rather than consuming the ordinary Serve
   connection pool. This orders every live guarded provider call before or
   after the invalidating commit and closes the normal check/use race inside a
   provider's own retry loop. Pre- and post-call lock-session probes plus a
   final fence read fail closed on detected session loss.

   PostgreSQL session loss or API-process death during an already accepted
   provider call remains an ambiguous external-side-effect case: the shared
   lock can disappear before the cloud API returns, so a newer generation may
   commit before the old provider operation finishes. The request raises a
   typed terminal fence error when this is detectable, does not retry or clean
   through stale authority inline, and leaves the exact durable replica row for
   manager reconciliation and identity-fenced teardown. This design does not
   claim provider-level exactly-once fencing across that failure. Thus an
   admitted request remains strictly ordered across an ordinary controller
   transition, while crash ambiguity is detected and reconciled rather than
   misclassified as successful current-generation provisioning.
   Legacy persisted launch requests without a version remain compatible only
   before this config protocol is activated; activation fences them so a
   pre-upgrade retry cannot provision across the first config-aware update.
5. Deterministic preflight completes under the request-local candidate config
   before global publication. A preflight failure can durably quarantine the
   candidate and continue the unchanged old runtime. Any failure after global
   publication irreversibly fences queued, in-flight, and future launches,
   target publication, physical/logical scale-up and scale-down, the autoscaler,
   the reserved-capacity poller, replica-manager refresh/probe/status daemons,
   and the update reconciler before arranging supervised child replacement.
   Config transition, autoscaler actuation, and each complete
   reserved-capacity provider/broker/publication cycle share an epoch lock.
   The poller rechecks the irreversible stop fence after acquiring it, so the
   partially transitioned process never retries or resumes actuation even if
   termination is delayed.
6. A failure before commit removes the exact staged raw file and receipt through
   the same controller backend. A lost response after commit preserves the
   committed generation and must not delete it.
7. Publishing an in-memory config is one reference swap. Readers copy a single
   captured global context and cannot combine config bytes with another
   generation's path metadata.

## Implementation and compatibility phases

1. Ship additive schema revision 036 and the read/write helpers while retaining
   null compatibility for historical rows.
2. Ship the capability-gated endpoint, per-version transaction, legacy
   backfill/scrub, remote cleanup, and recovery reconciliation together.
3. Quiesce Serve updates, migrate PostgreSQL, and roll every API pod. Existing
   controllers are respawned on the new image before updates resume. A new
   controller rejects the legacy update endpoint for consolidated services;
   an old controller fails the new API's capability preflight before mutation.
4. Submit a fresh production fleet version and verify both east and PHX
   placement edges, then kill a child and replace an API pod to prove the same
   version/config pair survives both recovery paths.

## Deployment and rollback

Revision 036 is additive and is retained on rollback. Historical services that
have neither been updated nor recreated under this image retain their legacy HA
scripts and can still be read by old binaries. A new `serve up` immediately
stores a version-1 config snapshot and publishes a payload-free recovery script;
the first protocol update does the same conversion for an existing service.
Rolling back to a binary that cannot restore per-version rows is therefore
unsafe for every newly created or protocol-updated service. The rollback gate
is: pause creates and updates, retain the new recovery-capable image until each
converted service is recreated on the old binary or the incident is resolved
forward. A Helm rollback alone is not authorized once any service has been
created or converted under this image.

## Verification plan

- Unit-test schema migration, atomic commit/rollback, immutable retry conflict,
  legacy backfill, corrupt-digest rejection, and quarantine selecting the
  durably applied v1 after a failed v3 when v2 was committed but coalesced.
- Route-test the legacy/new endpoint matrix and malformed capability versions,
  including JSON booleans and floats.
- Fault-test remote stage cleanup before delivery, symlink/FIFO/oversize/digest
  rejection, deleted custom workspaces, a sanitizer-invalid durable projection,
  lost response after commit, install failure, mode 0600, and DB-only child/pod
  recovery.
- Crash the API after raw sync but before submission and crash the controller
  before endpoint cleanup; after the bounded age, prove the serialized sweep
  removes only the NULL-yaml stage and receipt while preserving a committed or
  fresh generation.
- At an empty fleet, quarantine a newer generation and prove system-recovery
  authorization bootstraps the durably applied fallback generation rather than
  the quarantined `current_version`.
- Race a queued v1 worker against v2 publication and prove it keeps v1 config
  while its generation fence rejects provisioning; delay an admitted v1
  request, publish v2, and prove its request is canceled without latching
  manager ownership loss, including retry after one transient cancellation
  failure. Prove a v2 worker sees only v2. Quarantine v2 and prove both API
  precondition and replayed execution authorize elected v1 but reject v2 and a
  stale controller owner. Pause an admitted v1 request immediately before the
  provider call, elect v2 without the old controller child, and prove the fresh
  terminal fence cancels the request without a provider call, cleanup, or
  failover. With real PostgreSQL, hold a v1 provider call inside the shared
  guard and prove v2 commit, quarantine, application-receipt election, owner
  takeover, launch-blocking status, deletion, and same-name recreation wait;
  after the writer commits, prove a queued v1 reader fails its fresh check.
  Prove two readers overlap, a different service does not block, and an
  exclusive waiter precedes a later same-service reader. Terminate a guard
  session and prove the pre/post probes fail closed and durable-row
  reconciliation owns any ambiguous provider result.
- Delay termination after a failure in the middle of manager/autoscaler
  transition and prove target publication, scale-up, scale-down, update
  reconciliation, reserved-capacity polling, job-status reduction, health
  probing, and direct replica termination remain stopped. Delay a
  reserved-capacity provider query and prove an update cannot enter its runtime
  transition until the complete old cycle exits, then prove a poller waiting
  behind a failed update observes the stop fence before any broker write.
- Confirm the persisted HA script and command logs contain neither config
  base64 nor credential sentinels placed in provider labels, external IDs,
  arbitrary metadata, controller resources, or unrelated workspaces. Kill a
  child with credential sentinels in the initial and two historical live files
  and prove recovery retains only the elected safe projection, no live
  receipts, while preserving a fresh uncommitted stage.
- In production, verify the fleet placement catalog and reserved-fill claims
  contain both the east research cluster and PHX H200 cluster, both are priced
  at zero, an H200 replica becomes ready, the model endpoint returns success,
  and the east edge remains launchable.

## Verification evidence

On Python 3.14, the focused Serve regression set passes 1,120 tests and the
adjacent configuration, controller, reserved-capacity, request-precondition,
backend, failover, and lock set passes 666 tests plus 40 subtests. The four
excluded adjacent cases require a host `rsync` binary or a non-root permission
model and are unrelated to this change. The combined PostgreSQL migration,
recovery, schema-contract, and launch-authority set passes 97 tests; the
real-PostgreSQL launch-authority suite accounts for 12 of them, including
shared-reader concurrency, queued-writer ordering, every
fence-invalidating mutation named above, same-name recreation, exact Serve
engine binding, and a terminated advisory-lock backend. YAPF, isort, mypy,
Python compilation, and `git diff --check` are clean; the repository's full-file
pylint invocation continues to report pre-existing warnings in changed legacy
test modules, while the new PostgreSQL suite scores 10.00/10.

An adversarial review of the exact implementation found no production-code
blocker after checking migration behavior, mutation coverage, transaction and
advisory-lock ordering, provider-internal retry boundaries, error
classification, and recovery ownership. Production rollout evidence remains
open and will be recorded after the image/chart, database, controller-respawn,
API-pod replacement, PHX, and east canaries complete.

## Open rollout gates

- All focused and Serve unit tests pass on the production Python version.
- Adversarial crash, security, and mixed-version reviews have no open blocker.
- The SkyPilot image and Helm chart are published and pinned in boltz-platform.
- The production child-respawn, API-pod replacement, PHX H200 canary, and east
  regression checks pass before the rollout is declared complete.
