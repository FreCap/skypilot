# SkyPilot Managed Container Image Distribution

_Created: 2026-07-13_

_Reshaped: 2026-07-14; PostgreSQL-only persistence correction: 2026-07-17;
product, dashboard, lifecycle, and infrastructure reshape: 2026-07-19_

_Status: distribution implementation in progress; builder productization is
evidence-gated; final exact-head acceptance pending_

## Decision

SkyPilot should expose an immutable container artifact abstraction, not a
registry-copy abstraction. A user selects content with a digest, release, or
artifact ID. A workspace distribution policy decides where that content is
materialized. A concrete deployment pins one verified physical location and
keeps a durable reference to it.

This mirrors the useful part of Modal's model:

- the declared image produces immutable content identity;
- content identity is independent from deployment names and infrastructure;
- preparation is content-addressed and idempotent;
- a deployment records a stable version rather than following a mutable tag;
- caches are populated lazily at a real placement boundary;
- build or copy work is isolated from request-serving workers.

Modal's named-image interface sharpens one more boundary that SkyPilot should
copy: image publication is independent from latency-sensitive compute startup.
Publishing a named Modal Image completes before callers adopt it, and an
already deployed Function does not change until it is redeployed. SkyPilot's
portable equivalent separates source registration from release publication.
`register` records immutable content and queues canonical preparation.
`publish` reserves a release, but exposes that release to deployment only when
the canonical digest is verified READY. An independent `prepare` operation
warms selected placement caches. A task resolving an existing release never
discovers source content, initiates a build, or observes a half-published name.

SkyPilot must remain honest about the runtime difference. Modal controls its
container runtime and can use lazy filesystems, memory snapshots, and a global
execution fabric. SkyPilot spans ordinary Docker, containerd, VMs, and many
Kubernetes clusters. The initial implementation therefore publishes normal
OCI manifests and layers to registries near the selected compute. It does not
claim to provide lazy layer loading or memory snapshots.

## Product outcome and release boundaries

This is a permanent control-plane product, so its data-plane components are
independently deployable after additive API/schema expansion and earn
production support separately. One PR may implement more
than one slice, but a passing slice cannot conceal an unfinished one:

1. **Catalog foundation:** immutable identity, PostgreSQL state, placement
   validation, durable references, and provider-neutral copy state.
2. **AWS distribution slice:** one end-to-end ECR adapter, an independently
   deployed copy worker, AWS bootstrap modules, manual lifecycle controls, and
   restart/rollback evidence.
3. **Images product surface:** direct bounded read APIs and the complete
   dashboard catalog/detail/actions experience. This is useful with external
   profiles as well as the managed AWS slice.
4. **Managed-builder prototype:** a separate major feature and design, linked
   below. Its first deliverable is a narrow, non-public prototype that reuses
   catalog identity but creates no central migration, task syntax, durable
   controller, or dashboard Build action. Durable productization follows only
   after the representative-workload worth and executor-feasibility gates pass.
5. **Provider extensions:** GAR, Nebius or generic OCI, and an experimental
   Cloudflare-backed OCI registry qualify independently. Raw R2 is never a
   registry.

The first production claim is deliberately narrower than the complete schema:
an immutable catalog plus externally provisioned profiles and one managed AWS
ECR path. Other provider adapters remain external-only until their own
provisioning, authentication, ownership, deletion, and integration tests pass.

Before converting a large fleet, benchmark three paths with the same digest,
instance types, regions, and cold node caches: direct cross-region pull, an
operator-prewarmed digest, and managed on-demand preparation. At 100, 500, and
1,000 replicas, record image-ready p50/p95/p99, total rollout time, bytes sent
across regions, registry throttles, pull failures, API admission latency, copy
jobs, and operator actions. Every activation mode requires:

- it creates at most one successful materialization per requested physical
  target rather than per replica;
- placement admission adds no provider call and no more than 250 ms p95 over
  the digest-pinned control;
- registry throttles plus terminal pull failures stay below 0.1%; and
- publication plus one prepare command replaces per-region image-copy
  scripting without weakening exact-digest or rollback guarantees.

The **performance mode** additionally requires image-ready p95 at least 40%
below direct cross-region pulls and within 10% of explicit prewarming. It may be
described as a cold-start improvement.

The **operations mode** makes no speed claim. Against a written baseline it
must remove every per-region copy command after one publish/prepare action,
reduce rollout operator steps by at least 75%, complete three injected
copy/verification/restart recoveries without manual registry mutation, cut
median recovery time by at least 50% and below 15 minutes, and send no more than
5% additional cross-region bytes versus explicit prewarming. It may be
described only as safer automation and observability.

If the managed path passes neither mode, the distribution worker is not enabled
for that fleet. These are activation gates, not claims inferred from unit tests.

## PostgreSQL persistence and migration posture

Managed image catalog, queue, lease, and durable-reference state belongs to the
central API-server database and is PostgreSQL-only. Local request, controller,
or skylet databases that still officially use SQLite are separate components
and remain unchanged. Every managed-image state entry point fails closed when
its catalog engine is not PostgreSQL, and managed-image correctness tests use a
real PostgreSQL server.

There is no deployed legacy managed-image schema to correct. At this design
freeze, pull request 368 is open, has no merge commit, migration 023 exists
only on its feature branch, and the base branch contains no
`023_container_images.py`. The branch must not be deployed before this final
schema lands. If that evidence changes, merge is blocked and a separate
evidence-backed corrective migration must be designed; this document does not
pretend an unshipped schema is already immutable.

Migration 023 is therefore rewritten now and frozen at merge. It uses literal
Alembic `op.create_table`, constraint, and index operations rather than
`create_all()` against live SQLAlchemy metadata. It owns exactly these durable
distribution tables:

```text
container_image_catalog
container_image_workspace_quotas
container_image_profile_heads
container_image_profile_revisions
container_image_target_custodies
container_image_realm_generations
container_image_realm_allocations
container_image_registry_limiters
container_image_registry_permit_leases
container_images
container_image_sources
container_image_publications
container_image_releases
container_image_locations
container_image_references
container_image_catalog_summaries
container_image_catalog_facets
```

The literal DDL includes every foreign key, named check, partial uniqueness
constraint, queue/claim index, active-revision join index, and catalog-query
index described below. The catalog singleton includes the nonnegative central
config generation, 64-hex image-config digest, and bounded last-apply
idempotency/request hashes used by the atomic activation protocol. Realm rows
and allocations include the generation-qualified prefix fields and physical
prefix uniqueness described below. The preexisting central `config_yaml` table
is updated in the same transaction but is not recreated or counted as an image
table. Migration 023 creates external-OCI-only artifact provenance and
SOURCE-only canonical origins. It never creates the obsolete single-counter
`container_image_workspace_catalogs` table or artifact-level source columns.
There is no launchability backfill, `LEGACY_REPUBLISH_REQUIRED`, epoch table,
forced-RLS compatibility fence, or zero-session maintenance ceremony.

Migration 024 is not part of the distribution release. It is designed and
tested only after the builder prototype passes its pre-product worth and
executor-feasibility gate. Its complete table and constraint inventory lives in
the builder design. Applying an unused migration or exposing disabled build UI
is not treated as harmless because both create permanent compatibility and
support obligations.

The current pull request deliberately couples the API-62 distribution code,
literal migration 023, task YAML syntax for immutable image selection, AWS
worker, Terraform modules, and Images catalog UI into one merge train. Those
code/schema pieces do not pretend to be independently mergeable. Their
data-plane activation and provider support claims remain independent:

1. deploy API-version-62 code with distribution workers disabled; normal
   startup Alembic applies literal 023 through the existing application database
   URI;
2. verify exact schema, constraints, queue plans, API/UI behavior, and external
   exact-digest adoption;
3. deploy the independently scalable AWS copy worker; and
4. activate each managed realm only after its registry, identity, capacity, and
   fault-injection gates pass.

Rollback before managed content is accepted disables the unused distribution
surface. After managed state exists, rollback is only to another
migration-023-aware API-62 binary. Future image migrations use ordinary expand,
dual-read/write where needed, backfill, and contract phases. They do not inherit
a standing right to revoke the shared application database role or stop
unrelated clusters, requests, jobs, and Serve state.

## The reshape

The original design coupled logical image identity to a registry profile and
stored one release version on the image row. That made an authentication or
endpoint configuration revision look like new content and prevented one
artifact from having multiple useful release names. It also treated
preparation as an explicit side operation instead of a normal consequence of
deploying immutable content.

The model is now eight independent durable concepts:

```text
Artifact                 immutable content
  digest                 runtime identity
  producer metadata      import/build lineage
  lifecycle              active/tombstoned/purging/purged
       |
       +---- Source       exact import alias -> artifact
       |
       +---- Release      immutable human name -> artifact
       |
       +---- Publication  reserved name -> READY release
       |
       +---- Location     artifact bytes at one physical destination
                 |
                 +---- Policy revision  current auth/lifecycle authority
                 |
                 +---- Reference  durable consumer -> READY location

Catalog authority        UUID identifying the exact control-plane database
```

An administrator configures distributions and runtime bindings, but neither
is part of artifact identity:

```text
Distribution profile     placement and materialization policy
Registry target          physical endpoint identity
Kubernetes binding       context-specific pull capability
Runtime pull plan        one secret-free, digest-pinned selected location
```

A managed runtime pull plan also snapshots the distribution name, complete
profile revision, target policy fingerprint, location ID, and runtime auth
strategy. Those fields are launch authority, not advisory diagnostics. They
travel together through serialization and are checked together at restart and
cluster-handle commit boundaries.

The policy fingerprint does not authenticate the other fields by itself. At
the final commit, SkyPilot reloads the complete normalized profile selected by
the serialized task, recomputes the target policy fingerprint and
physical destination fingerprint plus the placement-specific auth strategy,
and compares the active revision's complete fingerprint under the shared
generation lock. This prevents an internal caller from combining a current
location and policy hash with either a different physical destination or a
different otherwise valid auth-strategy name. The same transaction proves that
every supplied artifact ID, source alias, and release selector is bound to that
artifact.

The same final check deterministically renders the expected managed OCI
reference from the current target, realm generation, immutable shard policy,
workspace, and artifact digest. Source
bindings select immutable import provenance but never contribute a repository
path. The configured immutable power-of-two shard count uses the digest's
leading bits to select a workspace repository, while the full digest identifies
the manifest inside that repository. There is no single fixed shard count: the
realm chooses a feasible count from both repository-count and
images-per-repository quotas, within the public v1 ceiling of 256.
This bounds provider resources without placing all artifacts under one
images-per-repository quota. A READY row and pull plan cannot agree on a
different digest-matching registry reference and thereby bypass the physical
destination fingerprint, and rotating a failed import to an equivalent mirror
cannot rename existing canonical or regional content.

Changing a manager identity, pull-auth strategy, or ownership boundary does not
create a new artifact or physical location. It transfers an explicit policy
revision onto the same verified bytes while no operation owns the row. Changing
the registry endpoint, provider account, realm generation, shard policy, or
rendered generation-qualified namespace creates a new physical location so old
and new routes can coexist during migration.

The following is the target module split created by this PR, not a claim about
the branch's current file tree. The foundation currently has `state.py`,
`providers.py`, `worker.py`, and `resolver.py`. During productization,
`models.py` and `config.py` retain pure validated contracts; new read
pagination, publication, and lifecycle SQL move to `catalog_state.py`,
`publication_state.py`, and `lifecycle_state.py`; and `runtime.py` takes the
pull-plan work from `resolver.py`. A small transaction helper accepts the
caller's PostgreSQL connection for the one cluster-handle/reference atomic
commit; `global_user_state.py` does not grow a second image state machine.
Provider-neutral capability interfaces move from `providers.py` to
`provider_adapters/base.py`, AWS behavior lands in `provider_adapters/aws.py`,
and `worker_service.py` wraps the reconciler from `worker.py` as the independent
process. Each extraction preserves behavior under focused tests before the next
boundary moves.
Dashboard response schemas are projections over these services, not direct SQL
or raw configuration. The separately designed builder uses `build_state.py`
and its own controller.

## CLI, SDK, and task interface

Source registration and release publication are explicit and never fan
content out to every region:

```text
sky image register ghcr.io/boltz-bio/boltz@sha256:<digest> \
    --distribution global-gpu

sky image publish ghcr.io/boltz-bio/boltz@sha256:<digest> \
    --release boltz-2.1.0 \
    --distribution global-gpu [--no-wait]

sky image publish --artifact-id 019f5a80-8bc9-7cf2-9fa8-0123456789ab \
    --release boltz-2.1.0 \
    --distribution global-gpu [--no-wait]
```

`register` records artifact identity and source alias, activates the selected
profile generation, creates the canonical materialization intent, and returns
its current state. For a managed profile that intent imports from an anonymous
or worker-authorized source. For an external profile, the same typed operation
is destination adoption: it returns the deterministic digest-pinned canonical
destination and `AWAITING_EXTERNAL_PUSH`, never acquires source credentials or
copies from the source. The operator pushes the exact digest out of band; the
worker only inspects destination bytes and transitions READY after full
digest/platform verification. `publish` does the same work and also creates a durable
release reservation. The public `container_image_releases` row is bound only
in the same transaction that verifies the canonical location READY. Until
then, release lookup returns a closed `PUBLICATION_PENDING` result, never a
source fallback or a launchable half-release. A canonical failure marks the
publication FAILED without exposing the release; an authorized retry reuses
the reservation. Conflicting reservations fail before catalog mutation, while
identical concurrent requests converge through database uniqueness.

The publish request is a typed one-of between `source_ref` and `artifact_id`.
Source publication performs registration as above. Artifact publication never
creates or selects a source alias: it locks an ACTIVE artifact and requires an
exact canonical READY location under the requested active distribution, then
enters the same reservation and READY fast-path finalizer. It is intended for
builder output and previously registered content. A missing route returns
`CANONICAL_NOT_READY` without queuing an import from historical provenance.
`distribution` is required for artifact publication unless exactly one active
READY canonical profile is eligible; zero or multiple eligible profiles fail
closed. Supplying both selectors, neither selector, or a source and artifact
whose digests merely happen to match is invalid rather than ambiguous.

The SDK exposes distinct `publish_source()` and `publish_artifact()` helpers
over the one request schema. CLI parsing and the dashboard Publish dialog use
the same one-of. Reservation quota, RBAC, wait behavior, audit, and idempotency
are identical after selector resolution.

Publishing an additional release for content whose exact canonical location is
already READY does not wait for a nonexistent future completion event. The
reservation transaction takes the same profile, artifact, canonical,
publication, and release locks, runs the shared READY validator, inserts the
release, and returns a READY publication immediately. An existing same-digest
release is idempotent. Worker completion and this fast path call one finalizer,
so neither can bypass revision, platform, digest, lifecycle, or quota checks.

The CLI waits for READY by default by polling a direct bounded publication
resource; `--no-wait` returns the publication ID and current state. Neither the
API request executor nor a deployment worker is held while OCI transfer runs.
SDK callers receive the publication resource and can use the same wait helper.
The asynchronous mutation request is terminal once it has durably created the
intent and reservation. Publication status is the truthful completion surface.

Regional preparation is a separate operator choice. Prepare validates every
requested target before registering a new source or creating any location
intent, so a misspelled later target cannot leave a partial registration or
only an earlier subset of targets. Because this interface has not shipped,
`register` is no longer a compatibility alias for `publish`; it is the explicit
source-only primitive. Both remain API-versioned together.

The equivalent task-level first use is a digest-pinned source plus an optional
release publication request. It uses the same registration and reservation
transaction, then adds only the placement-specific regional intent after
placement:

```yaml
resources:
  container_image:
    ref: ghcr.io/boltz-bio/boltz@sha256:<digest>
    release: boltz-2.1.0
```

That first launch may use only its request-scoped digest-pinned source while
canonical preparation is pending and policy permits fallback. It never treats
the requested release as available. After canonical verification publishes the
release, later tasks may select the release or artifact without restating the
source:

```yaml
resources:
  container_image:
    release: boltz-2.1.0
```

```yaml
resources:
  container_image:
    artifact_id: 019f5a80-8bc9-7cf2-9fa8-0123456789ab
```

The optional `distribution` chooses an allowed administrator-defined profile:

```yaml
resources:
  container_image:
    release: boltz-2.1.0
    distribution: global-gpu
```

`distribution: direct` is a reserved task-level escape hatch. It selects no
registry profile, preserves the existing digest-pinned source pull, is allowed
only by `managed_preferred`, and is rejected by `managed_required`. It cannot be
defined as a profile, selected as a server/workspace default, or used for a
release-only or artifact-only selector. Digest-pinned runtime references are an
unconditional managed-image invariant. The unreleased
`require_digest_at_runtime` profile key is removed rather than exposing a
boolean whose only valid value is `true`; configuration containing it is
rejected with a migration message.

`profile` and `version` remain accepted configuration and Python-constructor
compatibility aliases while this unreleased feature is reshaped. New
serialization emits `distribution` and `release`. The existing
`resources.image_id: docker:...` form remains a
deprecated compatibility input; cloud machine images continue to use
`image_id`. A resource created through that legacy spelling is serialized back
through `image_id` when talking to an old server. An explicitly authored
`container_image` requires API version 62, so a new client cannot silently
send an unknown field to an old server.

Compatibility is explicit: clients below 62 cannot serialize
`resources.container_image` or call image SDK methods; a 62 client down-converts
only legacy `image_id: docker:...` when talking to an older server; servers at
62 keep legacy response fields decodable by old clients; and rolling the server
back below 62 disables managed-image operations without rewriting existing
task, cluster, or catalog rows. A new client receives a version error before
request persistence, not an unknown-field failure from an old server.

`Resources.container_image` is keyword-only and appended after every existing
constructor parameter. This keeps the complete pre-feature positional calling
convention unchanged, including `disk_size` and the internal compatibility
arguments, while preventing the new field from creating another positional
ABI slot.

`Resources.copy()` preserves the old replacement semantics. When the current
container image came from legacy `image_id: docker:...`, an explicit
`image_id` override replaces or clears that derived container identity and
invalidates its resolved pull plan and Docker login. A separately authored
`container_image` remains independent from cloud-machine `image_id` changes.

The current request path accepts only digest-pinned managed sources. Mutable
tag resolution is deliberately rejected until a credential-aware metadata
worker can resolve tags outside the API process. This avoids network and
credential work in request handlers and avoids pretending a mutable tag is an
immutable artifact.

There is deliberately no mutable `latest` release. Modal can safely update a
named-image pointer because an App deployment snapshots a new Function version
and existing deployed Functions keep their previous image. SkyPilot should add
a mutable channel only with one generationed deployment snapshot shared by
clusters, managed jobs, and SkyServe. Resolving a mutable channel independently
for every replica or restart would make one logical service version run mixed
artifacts.

SkyServe applies that snapshot rule to every immutable selector today. After
admin policy and before a service version or controller request is persisted,
all resource candidates resolve to one artifact ID and the durable task YAML is
rewritten to that artifact ID while retaining each candidate's distribution.
Different source or release spellings may converge to the same artifact, but a
managed/direct mixture, a candidate without an image, or two different
artifacts is rejected. Candidate identity resolution is read-only, so a
rejected service leaves no artifact, alias, publication, profile-generation, or
canonical intent rows for a worker to import. After convergence, every
first-use source alias, release reservation, profile generation, and canonical
intent is registered in one ordered database transaction; a later binding
conflict rolls back the whole candidate set. The durable version stores the
artifact ID, not a pending release. Replicas then wait for the managed artifact
route instead of independently falling back to the source. Canonical completion
publishes any reserved release independently. This gives one content snapshot
per service version across heterogeneous `any_of` placements.

Multi-candidate publication uses global transaction phases rather than
locking one candidate end to end. Every batch follows the canonical table below:
quota, profile/custody, digest advisory keys, artifacts, sources, canonical and
regional locations, references, publications, then releases. Keys are sorted
within every phase. Thus
two convergent batches whose source order maps the same two release labels to
opposite digests cannot hold one release while waiting on the other; one batch
commits and the other receives the intended immutable-release conflict.

Release labels use the OCI tag grammar: one to 128 ASCII letters, digits,
underscores, periods, or hyphens, starting with a letter, digit, or underscore.
They cannot be URLs, paths, credentials, whitespace, or terminal control
sequences. The same value-free model validation runs for API requests, task
YAML, pickle restoration, state writes, and catalog responses before a release
can be persisted or rendered by the CLI.

Artifact and location IDs use canonical RFC UUID text. Distribution and target
names use one to 128 ASCII letters, digits, underscores, periods, or hyphens,
starting with a letter or digit. URL syntax, userinfo, paths, whitespace, and
terminal controls therefore cannot masquerade as control-plane identifiers.
The grammar is enforced before request persistence and again at config, task
YAML, pickle, state-write, runtime-plan, and response-decoding boundaries.

Operational commands accept a bare selector only when it resolves to one
artifact across the artifact-ID, release, and source namespaces. Callers can
select the namespace explicitly with `artifact_id=<id>`, `release=<name>`, or
`ref=<oci-reference>`. This avoids silent precedence when, for example, a
release label is textually equal to another artifact's UUID. The dashboard and
SDK always send a typed selector, so adding a later UUID-shaped release cannot
retroactively reinterpret an existing action. The prepare
command's `--release` binding is valid only with a source reference; it rejects
explicit release and artifact selectors instead of replacing their identity.
The status table prints the full artifact ID, so its ARTIFACT value can be
copied directly into status, prepare, or retry. A bare digest-pinned OCI source
remains a source registration request even if its digest already identifies an
artifact; prepare records that additional immutable alias instead of silently
rewriting it to an artifact-only lookup.

Every source reference is validated before persistence as a Docker/OCI image
reference. URL schemes, userinfo, queries, fragments, backslashes,
percent-encoded material, invalid authorities, and invalid repository or tag
grammar are rejected, and repository names obey Docker's 255-character limit.
The worker completion boundary repeats the same validation and canonicalizes
the authority before a destination reference can become READY. Runtime route
and resolved pull-plan models also validate and canonicalize references and
accept only declared auth-strategy names before YAML, pickle, or controller
state can carry them. Credential-bearing URLs can never become catalog state,
status output, or a source-fallback plan.

## Managed builder boundary

Distribution alone does not remove repeated runtime setup, but builder context
custody, private-base authentication, cache retention, and execution isolation
are not registry-copy concerns. The managed builder is therefore specified in
the independent canonical design
[`managed-container-image-builder.md`](managed-container-image-builder.md).
It reuses immutable catalog output and release publication but owns separate
API, PostgreSQL metadata, object-store custody, BuildKit workers, logs, quotas,
retry, and garbage collection. Builder tables live in `build_state.py`, not in
the already large distribution `state.py`.

The distribution slice does not claim builder readiness merely because the
artifact model has producer fields. A task may select a READY build output, but
placement never executes build work. The Boltz L4 fleet uses `linux/amd64`
only; ARM64 is built only after an actual placement or explicit target asks for
it. Clouds and regions cause copies of one compatible OCI artifact, not
different builds.

Immutable releases remain the default human name. A future mutable channel is
accepted only after a generationed `(channel, generation, artifact_id)`
snapshot is stored in the central PostgreSQL record for a cluster, managed-job
controller request, or SkyServe version before any replica starts. Promotion
creates a new generation and never mutates a running deployment. Channels stay
outside both initial designs until every consumer has that shared snapshot
boundary.

## Dashboard product surface

Managed images are a first-class dashboard resource. `Images` appears in the
desktop and mobile navigation beside `Volumes` and opens `/images`. The page is
workspace-scoped and uses the same accessible-workspace resolver as the rest of
the dashboard; it never assumes that every user can access `default`.

The catalog view contains:

- an accessible workspace selector whose value is reflected in the URL;
- a cursor-paginated artifact table, ordered by `(created_at, id)` descending;
- exact selector search for artifact ID, immutable release, or OCI source;
- server-side filters for location state, distribution, and target;
- digest, lifecycle, publications, releases, source count, platforms,
  compressed size, producer,
  location-health summary, and created/updated timestamps;
- explicit loading, empty, permission-denied, unsupported-old-server, and
  retryable-error states; and
- responsive cards on narrow screens rather than a horizontally unusable
  table.

Selecting a row opens `/images/[artifact]`. The detail view shows the complete
immutable identity, all bounded source and release aliases, publication state,
producer provenance, and aggregate location health. A separately cursor-
paginated location table defaults to current active profile revisions and can
include retained historical rows. Location rows expose distribution, target,
canonical status, policy revision, state, attempts, next retry, last
verification, last use, immutable reference, digest, and closed diagnostic
code. Fingerprints are available behind a diagnostic disclosure, not used as
primary labels.

Authorized workspace users can open dialogs for the existing safe operations:

- **Register** accepts a digest-pinned source and optional allowed
  distribution, creating no release; for an external profile it displays the
  deterministic destination, adoption state, and out-of-band push guidance;
- **Publish** accepts exactly one digest-pinned source or artifact ID, a
  required immutable release, and an allowed distribution; READY build rows
  open it prefilled with their artifact and output distribution;
- **Prepare** accepts an existing selector, one or more configured targets,
  and an optional distribution; and
- **Retry** is enabled only for an eligible failed or missing target.

Dialogs validate locally, submit through the existing asynchronous request
protocol, show request progress, retain a failure for correction, close only
on success, and refresh the affected catalog row. Repeated clicks are disabled
while a request is active. The first dashboard deliberately has no delete,
evict, release-rebind, or mutable-tag action. A separate admin-only
tombstone/purge API exists for legal and cost operations, but its preconditions
and consequences are too sharp for a catalog row action.

The page also shows the workspace's effective image policy and a secret-free
registry topology: profile name and revision, managed/external ownership,
namespace template, canonical target, regional targets, provider, region,
registry prefix when known, locality declarations, and runtime pull strategy
name. It excludes manager identities, credential references, tokens, raw
configuration, and every unrelated SkyPilot setting. Administrators also
receive a complete **Settings > Image distribution** panel, not a raw-YAML-only
escape hatch. It lists profiles, realm generation/shards, workspace
defaults/allowlists/quotas, target custody state, capability probes, and
Terraform inventory/output status. Create/edit forms cover every supported
profile, target, Kubernetes/VM binding, and workspace policy field; credential
references are selected by name and secret values are never fetched. The editor
shows the normalized secret-free diff/fingerprint, requires an explicit
monotonic revision on semantic changes, validates endpoint/locality ambiguity
and quota feasibility server-side, and applies through the managed-image
compare-and-swap activation transaction described below. The raw Settings
editor invokes that same transaction whenever its diff touches an image
section. It offers YAML preview/export for GitOps users. It does
not run Terraform or create cloud resources from the browser; it links exact
module/preflight commands and refreshes observed readiness. Read-only users see
only the topology projection above.

### Dashboard API contract

The asynchronous `GET /images` and existing SDK/CLI response shape remain
compatible. Without an exact selector it has a hard 1,000-artifact response
ceiling and fails with `IMAGE_STATUS_REQUIRES_PAGINATION` instead of returning a
partial result; large catalogs use the direct catalog API. The dashboard adds
direct, authenticated bounded reads and one bounded admin compare-and-swap:

```text
GET /images/catalog?workspace=W&limit=50&cursor=C[&selector=S]
GET /images/catalog?workspace=W&limit=50&cursor=C&lifecycle=ACTIVE
GET /images/catalog?workspace=W&limit=50&cursor=C&distribution=P
    [&target=T][&state=READY][&canonical=true]
GET /images/artifacts/{artifact_id}?workspace=W
GET /images/artifacts/{artifact_id}/locations?workspace=W&limit=50&cursor=C
    [&current_only=true]
GET /images/profiles?workspace=W
GET /images/publications/{publication_id}?workspace=W
GET /images/admin/config
POST /images/admin/config/validate
POST /images/admin/config/apply
```

These FastAPI routes are synchronous `def` handlers so blocking PostgreSQL
work runs in the framework threadpool. They return typed responses directly
and never create request rows, logs, or executor work. Catalog lifecycle and
publication mutations remain on the existing asynchronous request protocol;
admin config apply is the one direct, bounded PostgreSQL compare-and-swap. A
dashboard tab therefore performs bounded reads, not a stream of request-table
writes.

The three `/images/admin/config` routes are admin-only typed projections over
the managed-image activation transaction shared with the raw configuration
editor. GET returns only image
distribution sections plus capability/infrastructure status. Validate accepts a
complete proposed image subsection, normalizes it, performs parser/profile/
quota checks, and returns a secret-free diff without mutation. Apply requires
that diff's hash plus the current config generation, rejects a stale generation,
and updates only those sections. It cannot read or replace secret values. These
routes do not create a second source of configuration truth. The ordinary raw
Settings editor detects an image-section diff, produces the same normalized
proposal and hash, and calls the same activation helper rather than the legacy
best-effort reload hook.

`/images/catalog` returns `ContainerImageCatalogPage` with `items`, an opaque
`next_cursor`, and `has_more`. The cursor contains a versioned, value-validated
keyset boundary and is never a raw SQL fragment or offset. `limit` defaults to
50 and is bounded to 200. A selector is resolved by the same ambiguity-safe
artifact/release/source logic as operational commands. Lifecycle alone, or
distribution with optional state, target, and canonical filters, use the
transactionally maintained
catalog-facet indexes described below. They never walk the artifact ordering
index hoping to encounter a selective match and never load the whole workspace
in memory. Each page joins its one-row summaries and batch-loads bounded aliases;
it never materializes every location row for the page. Catalog polling refreshes only the first visible
page every 15 seconds while the tab is visible, backs off after errors, and
never resets the user's cursor or open dialog. Detail polling uses the direct artifact
endpoint, and publish progress uses the direct publication endpoint. State,
target, or canonical without distribution is rejected before database access.
An exact selector is mutually exclusive with every list filter.

`/images/profiles` returns `ContainerImageProfileSummary` for only the resolved
workspace. It is produced from validated profile models and a dedicated
response schema, not by redacting or returning the raw configuration tree.
Every read and mutation resolves and authorizes the requested workspace before
database access or scheduling. SkyPilot currently has exactly three global
roles, so the image contract does not invent a workspace-admin role:

| Operation | viewer | user | admin |
| --- | --- | --- | --- |
| catalog, detail, provenance, attempts, diagnostics | workspace read | workspace read | all accessible |
| secret-free profile topology and pull strategy | workspace read | workspace read | all accessible |
| publication status and redacted build metadata | workspace read | workspace read | all accessible |
| sensitive build inputs and bounded build logs | denied | workspace read | all accessible |
| register, publish, prepare, retry, build | denied | workspace write plus quota | allowed |
| retry a paid transfer | denied | workspace write plus quota | allowed |
| edit raw registry configuration | denied | denied | allowed through Settings |
| tombstone, retry purge, cancel or release a failed publication name | denied | denied | allowed |

Workspace authorization is enforced independently of role. Image mutations
consume the workspace's image-byte, artifact, copy-attempt, and build budgets;
the user role cannot bypass them by calling retry. Profile summaries never
contain manager identities, credential references, raw config, or tokens.
Route-coverage tests enumerate every image endpoint for all three roles and
cross-workspace access.

The viewer allowlist uses explicit route patterns, not an assumed `/images`
prefix grant: `GET /images`, `GET /images/catalog`,
`GET /images/artifacts/:artifact_id`,
`GET /images/artifacts/:artifact_id/locations`, `GET /images/profiles`, and
`GET /images/publications/:publication_id`. Builder read routes are enumerated
separately in its implementation. Every new image route is denied to viewers
until deliberately added and covered.

The detail endpoint is bounded to one typed artifact ID and returns no location
array. Aliases and publications remain bounded by existing per-artifact quotas.
The location endpoint uses `(current_revision, updated_at, id)` keyset ordering,
a default of 50, a hard limit of 200, and indexed current/history predicates.
Historical nonreferenced rows are compacted after 90 days by default, and the
workspace `max_location_records` quota is a hard backstop. If alias quotas later
grow beyond the detail response cap, aliases receive their own cursor endpoints
rather than weakening the bound.

Dashboard tests cover connector query encoding and direct-read polling, stale
response suppression when workspaces change, all dialog success and failure
paths, permission and old-server states, cursor navigation, filters, location
status rendering, responsive behavior, and keyboard-accessible dialogs. The
release gate includes `npm run lint`, targeted Jest tests, and a production
`npm run build`, followed by a manual browser pass against a PostgreSQL API
server with READY, WARMING, FAILED, and empty workspaces.

## Administrator interface

Registry endpoints and credentials stay out of model YAML:

```yaml
container_registries:
  default_profile: global-gpu
  profiles:
    global-gpu:
      revision: 1
      ownership: managed
      realm: boltz-production
      realm_generation: 1
      repository_shards: 16
      namespace: skypilot/{organization}/{workspace}/g{realm_generation}
      canonical:
        provider: aws
        account: "699..."
        region: us-east-1
        manager_identity: registry-manager
        pull_auth: ecr_runtime_identity
      targets:
        - name: aws-us-west-2
          provider: aws
          account: "699..."
          region: us-west-2
          manager_identity: registry-manager
          pull_auth: ecr_runtime_identity
        - name: aws-eu-west-1
          provider: aws
          account: "699..."
          region: eu-west-1
          manager_identity: registry-manager
          pull_auth: ecr_runtime_identity
    gcp-external:
      revision: 1
      ownership: external
      realm: boltz-gcp
      namespace: skypilot/{organization}/{workspace}
      canonical:
        provider: gcp
        project: skypilot-images
        region: us-central1
        registry: us-central1-docker.pkg.dev/skypilot-images
        manager_identity: external-registry-writer
        pull_auth: gar_runtime_identity
```

Workspace policy supplies a default and constrains behavior:

```yaml
workspaces:
  default:
    container_images:
      mode: managed_preferred      # managed_preferred | managed_required
      default_profile: global-gpu
      allowed_profiles: [global-gpu, gcp-external]
      locality: prefer             # prefer | require | canonical
      regional_cache_retention_weeks: 8
      max_artifacts: 1000000
      max_platforms_per_artifact: 1   # Boltz L4 fleet is linux/amd64 only
      max_sources_per_artifact: 128
      max_releases_per_artifact: 128
      max_release_reservations: 2000000
      max_location_records: 5000000
      max_locations_per_artifact: 128
      max_materialized_bytes: null  # null disables the optional byte budget
      max_user_retry_generations_per_location: 3
```

Defaults mean models do not repeat registry choices. A task-level
`distribution` is an override within the workspace allowlist, not a credential
or endpoint declaration. `realm_generation` and `repository_shards` are copied
from the Terraform output, are identical across managed profiles sharing that
realm generation, and are immutable after the first allocation. A managed
namespace must contain both `{workspace}` and `{realm_generation}`; the renderer
appends `/shard-<fixed-width-prefix>` selected from the declared shard count.
The rendered generation prefix, realm generation, and shard count are always
part of destination identity even if another literal field happens to repeat.
The general
parser ceiling remains 128 platforms, but an administrator must deliberately
raise the workspace bound and pass the repository-feasibility proof before a
multi-platform fleet is admitted.

Ownership applies to the complete profile. Mixed managed/external targets are
forbidden because one profile-level lifecycle policy cannot safely authorize
only some destructive operations. Cross-cloud services use separate
distributions that resolve to the same artifact ID, such as `global-gpu` for
managed AWS and `gcp-external` for externally provisioned GAR. A future
target-scoped ownership model would require target-scoped manager generations
and is not implied by this schema.

The workspace quota row tracks artifact, permanent release-reservation, and
location-record counts plus READY materialized bytes. Artifact, publication,
and location-intent creation reserve count quota in their transaction. One
publication consumes both one workspace reservation and one per-artifact
release slot before copy work begins. Finalizing its release does not consume a
second slot, and FAILED or CANCELLED state does not free either permanently
digest-bound reservation. Before registry I/O, the worker inspects exact
manifest size and atomically reserves byte quota when configured; a race that
loses quota does not begin the copy or publish a release. Eviction or confirmed
purge releases READY byte quota, while location-record quota is released only
when an orphaned
historical row is compacted under its audit retention policy. User retries
increment a per-location generation counter and require an admin after the
workspace limit. Quota counters are repaired from source tables by a bounded
operator reconciliation job and never trusted to authorize a negative count.

`revision` is a positive monotonic administrator-controlled generation for the
complete profile. Any endpoint, ownership, identity, auth, namespace, realm
generation, shard count, or target edit must increment it. The complete revision
fingerprint includes all of those fields. `container_image_profile_heads` stores the one active
revision and fingerprint. `container_image_profile_revisions` retains every
accepted secret-free normalized revision, and
`container_image_target_custodies` retains each revision/target's provider,
account/project, endpoint, physical namespace fingerprint, ownership tags,
manager-credential reference, and `ACTIVE|DRAINING|RETIRED` custody state.
Secrets remain in the configured secret provider. A credential reference may
be retired only after every location, delete-unknown row, and audit-retained
manifest owned by that custody is settled. Purge and repair use the location's
historical custody, never current profile credentials.

An older API or worker replica cannot roll the policy back, and two different
configurations cannot claim the same revision. Advancing a revision waits until
the profile has no active data-plane lease. The active-lease check uses one partial
profile-prefixed `LIMIT 1` probe for each of COPYING, EVICTING, and READY
verification. Keeping those states separate lets PostgreSQL use its
state-specific partial indexes instead of scanning an OR-shaped profile
predicate. Activation inserts the immutable revision/custody snapshot and
changes only the head authority row. It never rewrites the dominant profile
location population. Old rows become ineligible immediately through the head
join and are
transferred lazily, one locked physical location at a time, when the new policy
touches them. At that exact-row boundary, uncertain `COPYING` becomes retryable
FAILED, uncertain `EVICTING` conservatively becomes MISSING, and inactive or
semantically impossible ownership is cleared without consulting the canonical
manifest. Expiry is half-open: a lease is owned only while
`expires_at > now`, and is reclaimable at `expires_at <= now`. Only a
structurally complete lease on `COPYING`, `EVICTING`, or a requested READY
verification counts as active, and the database constraint rejects a complete
lease on any other state. A lost canonical manifest or exact-second late worker
therefore cannot deadlock or mutate the repair revision. A broken historical
credential keeps its custody in DRAINING with an operator-visible closed error
and blocks purge completion; it is never silently replaced by unrelated current
authority. There is no
implicit revision or synthetic legacy fingerprint in either the configuration
parser or location API. The final revision compare-and-swap must affect exactly
one row; a concurrent activation is surfaced for retry rather than silently
reported as applied.

### Atomic configuration and profile activation

The central PostgreSQL configuration row and image profile heads are one
authority boundary. Migration 023 extends the singleton
`container_image_catalog` row with `active_config_generation`,
`active_image_config_digest`, and the last apply idempotency key/request hash.
The database-backed API-server config writer is refactored so every whole-config
update locks the existing `config_yaml` row and this catalog singleton first,
compares the submitted generation, and commits through one caller-owned
PostgreSQL session. A file- or PVC-backed API server may edit ordinary local
configuration, but it cannot activate managed images; managed-image activation
requires the central PostgreSQL config row.

Validation and provider capability probes run before the transaction and
produce bounded, secret-free evidence digests. The apply transaction then
rereads and normalizes the complete proposed config, verifies its diff hash and
evidence generations, follows the global lock phases, inserts immutable realm,
profile-revision, and custody rows, advances every changed profile head, writes
the complete YAML value, and increments the singleton generation/digest in one
commit. All changed profile keys are sorted. Removing a profile first moves its
retained custody to DRAINING and is rejected when that would orphan active
authority. No cloud, registry, filesystem, ConfigMap, or secret-provider I/O
occurs under the transaction.

Both the typed image editor and raw Settings editor use this helper. A raw edit
that does not change image configuration still takes the same config-row
compare-and-swap, so it cannot overwrite a concurrent image activation. A
repeated idempotency key with the same request hash returns the committed
generation; reuse with different bytes fails closed. A crash before commit
changes neither config nor heads. A crash or lost response after commit is
recovered from the recorded generation and idempotency result, so config
revision 2 with profile head 1, or the reverse, is not a representable state.

Commit emits PostgreSQL `NOTIFY` only as a wakeup. Every API and worker replica
also polls the singleton generation, reloads the complete DB config, validates
its digest, and records the locally loaded generation. Managed-image request
admission, queue claim, and final transition compare that generation under the
phase-1 catalog lock; a stale or failed replica returns
`IMAGE_CONFIG_RELOAD_PENDING` or skips work until it converges. Ordinary
non-image operations remain available. The in-process reload hook and
Kubernetes ConfigMap mirror are post-commit conveniences, never authority and
never a way to advance a profile head. Activation remains disabled during a
mixed-version rollout until every participating API, controller, and worker
advertises this generation protocol.

A managed namespace must include `{workspace}` and `{realm_generation}`.
Cross-workspace physical deduplication would require a global reference and
eviction authority model; the workspace-scoped catalog deliberately refuses to
pretend it has one. Profile validation rejects a managed namespace that could
render the same generation-qualified repository prefix as another realm
generation on the same provider/account/region/registry authority.

Kubernetes is explicit because a registry being close to a cluster does not
prove that its nodes or service accounts can pull it:

```yaml
container_registries:
  kubernetes_contexts:
    production-eks:
      registry_provider: aws
      registry_region: us-east-1
      registry: 699....dkr.ecr.us-east-1.amazonaws.com
      auth_strategy: node_identity
      runtime_platforms: [linux/amd64]
    research-gke:
      registry_provider: gcp
      registry_region: us-central1
      registry: us-central1-docker.pkg.dev/skypilot-images
      auth_strategy: node_identity
      runtime_platforms: [linux/amd64]
  vm_bindings:
    - provider: nebius
      region: eu-north1
      instance_type: gpu-h100-sxm
      registry: registry.example.internal/skypilot
      auth_strategy: anonymous
      runtime_platforms: [linux/amd64]
```

These bindings are assertions about pre-bootstrapped runtime access to one
exact normalized registry prefix. `node_identity` specifically means the
kubelet or node identity used for image pulls, not a pod service account. Pod
service-account credentials become available only after the image has already
been pulled and therefore cannot authorize this operation. Provider and region
alone are not authority. Binding regions are stripped, lowercased, validated
against the registry provider's bounded region grammar at admission, and
revalidated on the concrete placement. An exact Kubernetes binding takes
precedence over target-level anonymous access because it is the more specific
runtime authority:
two ECR accounts or GAR projects in one region may have different access. The
current implementation does not advertise a pull-secret strategy because it
does not yet carry a secret name into the pod template. A future
secret-reference binding must name an existing secret and inject only that
name, never the credential value.

Runtime platform is an admission fact, not a best-effort hint. SkyPilot first
uses the concrete cloud catalog architecture. When a VM provider or Kubernetes
context cannot prove it, an exact context or `(provider, region,
instance_type)` binding must declare the finite `runtime_platforms` set. The
artifact must cover every declared platform. A missing binding fails closed;
an unknown placement never accepts an AMD64-only or ARM64-only image merely
because either platform is common. Providers such as Nebius should implement
their catalog architecture mapping so ordinary configurations do not need an
override. Bindings are assertions administrators can audit and test, not
architecture guesses in the registry resolver.

VM placement intentionally does not infer an ECR account or GAR project from
API-server credentials because the remote workload identity can differ and may
have cross-account access. Profile admission rejects two targets whose
effective locality overlaps on the same provider and region, even if their
aliases sort deterministically. Administrators split those endpoints into
separate distributions. This moves ambiguity to configuration validation
rather than discovering it during a launch. Exact Kubernetes and VM bindings
still prove pull authority and runtime platform, but they do not make an
ambiguous distribution valid.

Raw R2 object storage is not an OCI registry. An OCI-compatible registry whose
blob store is backed by R2 can be configured as a generic target. R2 can also
remain an explicit rollback archive, but it is not a transparent Docker pull
fallback.

## Restart and policy-rotation semantics

The pre-provision INIT transaction pins the exact pull plan and durable
location reference before that plan is rendered into a VM or Kubernetes
runtime. The READY transition is a continuation of the same launch and must
preserve that execution state. If an administrator activates a new profile
revision while provisioning is in progress, it applies to later launches and
restarts; SkyPilot must not rewrite the durable handle to a route the running
runtime never received. This also keeps the INIT reference as the eviction
fence for the bytes actually in use.

A resolved managed route is reusable only while its distribution, profile
revision, policy fingerprint, physical destination fingerprint, target alias,
placement-specific runtime auth, the derived runtime login instruction, digest,
reference, and READY location all still agree with the current catalog and
complete administrator profile. A
policy-only edit such as an ECR pull-auth rotation may reuse the same verified
bytes and stable location ID, but it still produces a new resolved pull plan.
The old serialized auth or login instruction is never carried forward.
Docker-compatible source references without an explicit registry authority
retain their user-facing shorthand but derive `docker.io` as their runtime
authority. This makes digest-pinned Docker Hub fallback portable across VM and
Kubernetes backends instead of failing with an untyped missing-authority
assertion.
SSH node pools use SSH only as their control transport: workloads run as k3s
pods, so registry selection, pull identity, and restored-pod enforcement treat
them as Kubernetes rather than VM Docker. Kubernetes restoration resolves the
active head node type and rewrites exactly its `ray-node` container; an unused
default node type cannot mask a divergent active runtime image.

Restart resolution drops and recomputes a stale or unavailable plan from the
immutable artifact selector. It may select another verified location under the
current policy. A WARMING source fallback is likewise per-attempt rather than
permanent: a later restart upgrades to a managed READY route when one exists.
The final cluster transaction locks the exact location and active profile
revision, compares every policy-snapshot field, and replaces the durable
location reference only with the cluster-handle update. A direct caller with a
stale handle therefore rolls back without leaving either a cluster row or an
image reference. It also recomputes runtime pull authority and the exact
`DockerLoginConfig` from the concrete VM or Kubernetes placement and current
context binding. Only a non-launch status refresh carrying the exact selector,
resolved plan, provider/backend/region, login instruction, and location
reference already stored in the durable handle may skip revalidation. Naming
the same location with changed placement, auth, login, or selector state is not
a no-op.

A locationless WARMING plan is checked at the same persistence boundary. It is
valid only for the exact digest-pinned source selector durably bound to the
artifact, while the live workspace policy remains `managed_preferred` with
`locality: prefer`. Release-only and artifact-only selectors, unresolved
managed selectors, mismatched login servers, and inline credentials cannot be
persisted as source fallbacks. The same cluster-handle transaction inserts its
artifact/source `SOURCE_FALLBACK` consumer reference; failure of either write
rolls back both. The source reference and concrete placement
also deterministically derive the runtime pull authority: ECR on an AWS VM
pins `ecr_runtime_identity` and its exact value-free ECR login instruction,
GAR on a GCP VM pins the equivalent GAR identity, and Kubernetes requires an
exact configured registry-prefix binding. A recognized private cloud registry
on any other placement fails closed before provisioning instead of attempting
an anonymous pull. Arbitrary OCI authorities retain the existing direct-source
contract because privacy cannot be inferred from their host name.

Node provisioning may last minutes. The READY commit therefore reads the
durable INIT handle and preserves the plan only when the complete execution
state and existing location reference are identical to the handle that was
actually rendered. It does not re-resolve against a revision activated during
provisioning. A later real restart re-resolves before rendering and atomically
moves the handle and reference to the current revision.

## Durable data model and authority

### `container_image_workspace_quotas`

One row per workspace stores `artifact_count`, `release_reservation_count`,
`location_record_count`, and `materialized_bytes` as nonnegative counters plus
update time. PostgreSQL
check constraints reject negative or signed-64-bit-overflow values. All quota
authorizations lock this row before the artifact/location they may create, so
concurrent users cannot oversubscribe a limit. Policy limits remain validated
configuration rather than database state; changing a limit does not rewrite
the counters.

### `container_image_catalog_summaries` and `container_image_catalog_facets`

Dashboard scale uses explicit read projections, not a selective walk over the
artifact index. One summary row per artifact stores revision-independent source,
release, publication-state, and latest-change counters. Current-location counts
are intentionally not stored there because profile activation must remain O(1).
A facet row mirrors each physical location's workspace, artifact ID, artifact
creation key, lifecycle, distribution, profile revision, custody ID, target,
canonical flag, and closed state. A current query joins its revision to the
small `container_image_profile_heads` authority, so one head update immediately
excludes all old-revision facets without rewriting them. Facets contain no
registry credential, endpoint, or source value.

Every location insert/state transition and artifact/publication lifecycle
mutation updates its facet or summary in the same PostgreSQL transaction under
the shared lock order. These projections are read acceleration, not lifecycle
authority. Application transactions and nonnegative checks prevent ordinary
drift, but they do not pretend to prove cross-table equality. A scheduled
integrity check compares bounded artifact pages to source rows, emits drift
metrics, and a fenced operator repair rebuilds one artifact or one keyset page.
Dashboard results may be temporarily stale under detected projection drift;
launch, purge, quota, and worker decisions never read these tables.

The catalog API accepts only these indexed filter shapes: no filter; lifecycle
alone; or distribution with optional target, state, and canonical flag. State,
target, or canonical without distribution is a 400 error. Exact artifact,
release, and source selectors use their authority indexes. Literal migration
023 creates separate indexes for lifecycle and each prefix of the allowed
`(workspace, distribution, active_profile_revision, target, state, canonical,
artifact_created_at DESC, artifact_id DESC)` shape. A distribution query first
loads its single active head, then uses that exact revision in the facet index;
it never scans historical revisions or an unbounded set of profile heads.
Filtered queries deduplicate one bounded page of artifact IDs, then load
artifacts and revision-independent summaries in a fixed number of statements.
An artifact's authoritative `location_count` is locked on insertion and has a
database check of 0 through 128; workspace policy may set a lower limit. That
hard bound, not an unenforced configuration promise, caps duplicate qualifying
facets per artifact. Unfiltered pages use the artifact ordering index directly.

### `container_image_realm_generations` and `container_image_realm_allocations`

One immutable realm-generation row stores the Terraform-declared workspace
capacity, power-of-two shard count, generation-qualified prefix template,
quota/inventory snapshot digest, and ownership identity. One allocation row per
`(realm_generation_id, workspace)` reserves a stable namespace slot and its
fully rendered generation prefix. The first managed-profile activation locks
the realm generation and creates the allocation before any repository intent.
A uniqueness constraint over provider, account/project, region, registry
authority, and rendered generation prefix prevents two generations from
claiming the same repository set. Both rows are retained while any artifact,
location, publication, or audit tombstone uses the realm. This is capacity
authority only, not cloud state; the provider adapter still proves repository
ownership and handles quota drift.

### `container_image_registry_limiters` and
`container_image_registry_permit_leases`

Registry rate authority is global per `(provider, account, region,
operation_class)`, not per pod. One limiter row stores the applied quota
snapshot, conservative token capacity/refill rate, last-refill timestamp,
available weighted tokens, and shared penalty-until time for
`AUTH_TOKEN`, `METADATA_READ`, `REPOSITORY_MUTATION`, `LAYER_CHECK`,
`LAYER_READ`, `LAYER_INITIATE`, `LAYER_PART`, `LAYER_COMPLETE`,
`MANIFEST_READ`, `MANIFEST_PUT`, `VERIFY`, and `DELETE`. Each admitted provider
operation creates its own permit-lease row with owner, random-token hash,
resource ID, weight, expiry, and observed outcome. Acquisition locks the limiter,
refills from elapsed database time, reclaims expired leases, consumes tokens,
and inserts the lease atomically. Completion deletes or closes only the matching
token. A throttle extends the shared penalty and lowers the effective refill
rate under bounded recovery. An aggregate counter is never used as a substitute
for independently expirable owners.

Every ECR API call made by the distribution worker or trusted publisher
acquires its matching permit immediately before that call. A closed adapter
mapping assigns `GetAuthorizationToken` to `AUTH_TOKEN`; repository and tag
describes to `METADATA_READ`; create/tag/policy mutation to
`REPOSITORY_MUTATION`; `BatchCheckLayerAvailability`,
`GetDownloadUrlForLayer`, `InitiateLayerUpload`, `UploadLayerPart`, and
`CompleteLayerUpload` to their corresponding layer classes; `BatchGetImage` to
`MANIFEST_READ`; `PutImage` to `MANIFEST_PUT`; `DescribeImages` and
`ListImages` to `VERIFY`; and manifest/repository deletion to `DELETE`. Adding
an ECR call without registering an operation class fails an adapter test and a
runtime assertion before the SDK invocation. Authorization tokens are cached
only until their bounded refresh margin and every mint is paced. Service
Quotas, CloudWatch, Terraform, and the explicit operator inventory command are
control-plane probes with their own bounded AWS SDK retry policy, not hidden
data-plane calls covered by this invariant.

The v1 copy/publisher adapters traverse a bounded manifest graph and own
the ECR layer and manifest APIs directly, so they can pace actual calls. They do
not hand credentialed pushes to an opaque subprocess. A local token bucket may
reduce database chatter only by leasing a bounded, expiring token tranche that
remains represented in PostgreSQL. Scaling worker replicas therefore increases
parallelism only while shared account/region budget remains. Permit acquisition
is independent worker work and never blocks placement admission. Provider
throttle metrics and CloudWatch usage remain operational feedback; the design
does not claim to control calls made by external clients using the same AWS
account.

### `container_images`

One row per `(workspace, digest)`:

- stable artifact ID;
- digest, bounded unique OCI `os/architecture[/variant]` platform set, and
  nonnegative signed-64-bit compressed size backed by a PostgreSQL `BIGINT`;
- closed initial producer kind (`external_oci` or `managed_build`), optional
  64-hex producer spec hash, and optional bounded builder-version identifier;
  later same-digest build convergence is recorded on the build without
  rewriting this initial provenance;
- lifecycle state (`ACTIVE`, `TOMBSTONED`, `PURGING`, or `PURGED`), tombstone
  actor/reason/time, and purge completion time;
- timestamps and creator hash;
- no profile, endpoint, runtime auth, or credential data.

Literal migration 023 creates no artifact-level `source_ref` or
`resolved_source_ref` columns. Immutable aliases live only in
`container_image_sources`; a managed-build artifact may have none. No artifact
lookup, destination renderer, runtime validator, or fallback infers a source
from the artifact row.

Migration 023 creates the producer columns with an exact named check that
allows only `external_oci` and requires both builder-specific fields NULL.
Migration 024 drops that named check and installs the final one-of check:
`external_oci` still requires both builder fields NULL, while `managed_build`
requires a lowercase 64-hex producer spec hash and nonempty bounded builder
version. Content convergence never rewrites an existing artifact's initial
producer tuple.

### `container_image_sources`

One immutable source alias bound to one artifact. Multiple digest-pinned
sources may resolve to the same content. Fallback is authorized only by the
exact source in the current request; a release or artifact selector never
inherits the first historical source stored on the artifact.

### `container_image_releases`

One immutable workspace-scoped name bound to one artifact. Multiple release
names may point to the same content. Literal migration 023 creates a release
row only through canonical READY finalization and records its authorizing
publication and canonical location. Pending intent exists only in
`container_image_publications`. Rebinding an existing successful release to a
different digest fails. Selectors require an ACTIVE artifact, so pending or
tombstoned content cannot be newly adopted.

### `container_image_publications`

One durable reservation generation per `(workspace, release)`, with at most one
non-RELEASED generation active while a release is pending:

```text
id, workspace, release, image_id, canonical_location_id,
state PENDING|READY|FAILED|CANCELLED|RELEASED,
attempt_count, retry_generation, reservation_generation,
error_code,
created_by, created_at, updated_at, completed_at NULL,
released_by/reason/at NULL
```

The reservation prevents two digests from racing for a name without making the
name launchable. Canonical READY completion follows the global phases through
artifact, canonical location, publication, and release, then verifies the exact
digest and active profile revision, inserts the immutable release, and marks
the publication READY in one transaction. Failure records a closed code but
keeps the reservation for authorized retry. An administrator may cancel only a
terminal FAILED publication with no release row; cancellation is audited and
keeps the digest reservation so a same-digest retry can reactivate it. A
successful READY release is permanently bound.

For a publication that has never created a release row, an admin-only
`release-name` operation is the typo and wrong-digest escape hatch. It resolves
the publication ID without locking, then locks quota, profile, artifact,
canonical location if present, publication, and active-name release key in the
global order; it requires
FAILED or CANCELLED, no durable consumer, no live lease, and no release row;
records actor, reason, time, and terminal `RELEASED`; restores the workspace and
per-artifact reservation counters; and removes only that generation from the
partial unique active-name index. A later publication may reuse the text with a
new `reservation_generation`. Every worker and finalizer is fenced by
publication ID and generation, so a late callback for the released reservation
cannot bind the reused name. Audit history is never deleted. Publication status
is directly readable by ID and workspace with no asynchronous request row.

Creating the reservation locks its workspace quota row and artifact, checks
`max_release_reservations` plus `max_releases_per_artifact`, and consumes both
slots before any copy is queued. They remain charged through FAILED and
CANCELLED and are restored only by the never-READY RELEASED transaction above.
Reactivation increments
`retry_generation` on the same row and consumes retry-attempt budget, but never
consumes another release slot. Consequently the READY finalizer cannot finish a
paid transfer and then fail because another caller exhausted release quota.

Reservation creation invokes that same finalization transaction immediately
when the exact canonical location is already READY under the active profile.
It never depends on a later worker callback to publish an additional name for
existing bytes.

### `container_image_locations`

One row per artifact and materialization identity:

- stable location ID;
- distribution name and target display name;
- lowercase 64-hex physical destination fingerprint, separate lowercase
  64-hex policy fingerprint, profile revision, target-custody ID, realm
  generation ID, immutable shard count, and rendered generation-qualified
  workspace prefix;
- canonical flag, exact expected digest, and for a regional copy the exact
  canonical location ID for the same revision;
- for a canonical location, closed `origin_kind SOURCE|BUILD` plus exactly one
  immutable `source_id` or `build_id`; regional rows inherit this provenance
  through their exact canonical location and carry neither field themselves;
- secret-free digest-pinned destination reference;
- state, closed lease kind, fenced lease, retry, verification, use, and
  eviction metadata. Migration 024 adds `BUILD_RESERVED` plus the
  generation/attempt/token-bound `BUILD_OUTPUT` lease kind.

The physical fingerprint excludes profile and target aliases as well as auth
configuration, but always includes provider authority, account/project,
endpoint, rendered generation-qualified workspace prefix, realm generation,
and shard count. The destination reference is derived from exactly those fields,
the digest-selected shard, and the full artifact digest. One physical
destination cannot be canonical in one profile
and an evictable cache in another. A uniqueness constraint also prevents two
logical locations for one artifact from publishing the same physical manifest
reference. Renaming a target alias in a new profile revision transfers that
same physical row under the revision lock; it does not manufacture a duplicate
location or reject an otherwise unchanged endpoint.

A PostgreSQL check constraint enforces the origin one-of. Migration 023
installs a named
SOURCE-only constraint: canonical rows require one non-null `source_id`, and
regional rows carry no origin field. Additive migration 024 adds the nullable
`build_id` foreign key, drops that exact named
constraint, and installs the complete check: a canonical is either SOURCE with
only `source_id` or BUILD with only `build_id`, while a regional row has
`origin_kind`, `source_id`, and `build_id` all NULL. Managed destination
rendering accepts only workspace, target, and artifact digest, never a source
reference, so both origins produce the same immutable shard identity. Runtime
validation, regional
copy, and purge lock the canonical origin and prove either an immutable source
record resolving to the artifact digest or a READY build whose output artifact
and digest match. Builder completion follows the shared global lock phases. It
revalidates its fenced build and output tokens after acquiring those locks.
Audit and compact purge provenance retain the closed origin kind and a
nonreversible source/build identifier hash.

A failed or pending SOURCE canonical import is not permanently tied to the artifact's
first source alias. Publishing a later digest-equivalent source atomically
rotates the canonical location's source-record binding only while it is
non-READY and has no live lease. Rotation clears expired ownership and returns
the row to PENDING with a fresh retry budget. A live lease rejects and rolls
back the publication; after exact expiry, the old lease token cannot complete.
A later release, artifact, prepare, retry, or launch selector with no explicit
source preserves that established canonical source binding. The earliest
immutable source is selected only while creating a brand-new canonical row;
source-less ensures never rotate an existing A-to-B binding back to A. A BUILD
origin is immutable and never participates in source rotation.
A READY canonical location keeps the provenance that produced its verified
destination. Workers, runtime route validation, and the final atomic cluster
commit validate that origin binding against the artifact digest rather than
the artifact's historical first-source fields. Managed
destination repositories are independently selected by workspace and the
realm-generation's immutable power-of-two digest-prefix shard count, and
manifests remain content-addressed by the full digest.
Rotating a failed canonical import between digest-equivalent source
repositories therefore preserves every canonical and regional destination
reference.

A physical profile revision can coexist with an earlier READY location. A
policy-only revision reuses the location but is accepted by workers and route
resolution only after an explicit lease-free transfer. Old durable consumers
continue to name their pinned location until they terminate.

### `container_image_profile_heads`, `container_image_profile_revisions`, and
`container_image_target_custodies`

One head row per `(workspace, distribution)` stores the active revision and
complete fingerprint. Immutable revision and target-custody rows preserve the
secret-free historical contract described above. Profile activation inserts
the new snapshots and advances the head in one transaction. Queue claims,
READY publication, new managed-location references, retry, verification, and
eviction join a location's revision to the active head. Regional claims also
require their exact bound canonical location to be READY. Purge is the deliberate
exception: it uses the row's retained custody even after that revision is no
longer active.

Availability-creating or ownership-acquiring transactions hold `FOR KEY SHARE`
on the exact head/revision rows. Profile activation takes `FOR UPDATE` on the
head, so it linearizes after earlier work and before later work. A final
transition depending on a regional route takes `FOR SHARE` on its exact READY
canonical before updating the regional row. The canonical lock, not a correlated
statement-snapshot predicate, serializes canonical loss against regional
readiness and durable acquisition. Queue claims use an unlocked index probe,
then reacquire all rows through the global order below with the final candidate
using `SKIP LOCKED` and a conditional primary-key update.

### `container_image_references`

One durable consumer reference per `(workspace, consumer_type, consumer_id)`
always points to an artifact. `MANAGED_LOCATION` references additionally point
to a READY location. `SOURCE_FALLBACK` references instead point to the exact
immutable source alias and retain the value-free runtime authority and concrete
placement fingerprint. The cluster-handle transaction creates either form, so
a locationless WARMING launch is visible to tombstone and purge. Cluster stop
retains it because a stopped cluster can restart. A restart atomically replaces
it when the pull plan changes, and cluster termination removes it. Eviction
checks location references; artifact lifecycle checks both forms.

The API server catalog is authoritative. Its database stores a randomly
generated catalog UUID. Every dedicated SkyServe and managed-jobs controller
receives that UUID and must open the database containing the exact same UUID.
Merely opening some PostgreSQL database is insufficient. A controller-local
database is unsupported for managed image state and fails before artifact or
location mutation; consolidated controllers naturally share the API server
catalog.

### Global PostgreSQL lock order

Every image mutation, helper, callback, repair path, and multi-candidate batch
uses the same phases, with keys sorted lexicographically inside each phase:

| Phase | Rows or locks |
| --- | --- |
| 1 | catalog singleton/config generation and central `config_yaml` row |
| 2 | distribution workspace quota, builder workspace quota, builder UTC-day usage, then realm generation/allocation |
| 3 | profile head, immutable revision, and target custody |
| 4 | workspace/digest advisory keys |
| 5 | artifacts |
| 6 | producer rows: sources, then builds |
| 7 | context uploads and part evidence, context manifests/object custody, then build-cache records |
| 8 | canonical locations, then regional locations |
| 9 | consumer references |
| 10 | build attempts, build-output leases, staging custody, then build-log segment metadata |
| 11 | publications |
| 12 | releases |
| 13 | catalog summaries and facets |
| 14 | audit/outbox append rows |

Within a phase, table rank is the order shown and primary keys are sorted
lexicographically; a multi-workspace operation sorts workspace before table
rank. Object-store keys and staging references are custody fields on their phase
7 or phase 10 owner row, never an independently locked hidden resource.
Registry limiter and permit rows use their own short transaction, lock limiter
then permit key, and are never held with any catalog phase. No provider,
Kubernetes, secret-provider, filesystem, or object-store I/O occurs while
holding these locks.

A path that does not need an earlier phase simply starts at its first required
phase; it may never acquire an earlier phase afterward. All foreign keys and
candidate IDs are discovered without locks before the mutation begins, then
revalidated after acquisition. Release-name recovery, tombstone, builder
finalization, publication fast paths, and batch publication all follow this
order. Batch work locks all keys in one phase before moving to the next. The
shared builder contract maps its mixed paths explicitly: resolve miss uses
1/2/5/6/7, bundle commit uses 1/2/3/5/6/7/8/9, scheduler admission uses
1/2/6/10, and retry/reactivation uses 1/2/3/5/6/7/8/9/10. Intake cancellation
uses 1/2/6/7; active cancellation that has output custody uses
1/2/3/5/6/7/8/9/10/13/14. Output publication uses
1/2/3/4/5/6/8/10/13/14, and artifact tombstone/purge walks
1/2/5/6/7/8/9/10/11/12/13/14. Every managed-image mutation and worker claim
takes `FOR KEY SHARE` on the phase-1 catalog singleton and verifies its locally
loaded config generation; config apply takes `FOR UPDATE` on the singleton and
central config row in sorted key order. A transaction may omit other
unused phases but never reorder the ones it uses. Concurrency tests exercise every adjacent
overlap and the full crossed mixed-path graph. This table is the authority over
prose in both canonical designs.

## Deployment lifecycle

```text
task selector
  -> resolve an existing artifact or atomically register a source-backed one
  -> quota and candidate checks
  -> actual placement chosen
  -> ensure canonical and matching local materialization intent
  -> resolve a secret-free digest-pinned pull plan
  -> atomically persist cluster handle and durable location reference
  -> provision runtime with the pinned plan
```

Optimizer enumeration and dry runs are read-only. They never create
repositories, rows, or copy jobs. Ensure-on-use runs only after quota checks
and with a concrete provider/region or Kubernetes context.

With `managed_preferred` and `locality: prefer`, the first launch can use the
original digest-pinned source while canonical and regional materializations
are `WARMING`. This is the latency-friendly path: useful work starts now and
future launches become local. No database row claims that the source fallback
is a managed READY location. Persisted fallback state uses the closed
`managed_route_warming` reason code, never resolver or provider exception text.

With `managed_required`, `locality: require`, or `locality: canonical`, the
required verified route must be READY before provisioning. This is the
predictable large-fleet path. A Boltz fleet rollout should explicitly prepare
the target regions and gate scale-up on verified readiness.

Every newly selected managed route is row-locked, touched, and referenced in
the same database transaction as its cluster handle. This serializes launch
against eviction. If the route stops being READY or owns a verification lease,
or if a regional route's exact canonical origin is no longer READY, both writes
roll back. Standalone durable-reference acquisition applies the same canonical
row lock. If it linearizes first, canonical loss waits for the reference
transaction to commit; if canonical loss linearizes first, the reference is
rejected. A normal refresh of a cluster already referencing the same location
is a no-op for image state, so transient repair cannot mark a running cluster
UNKNOWN and a 1,000-node fleet does not rewrite two catalog rows per status
cycle. Replacing the handle with a direct image releases the previous reference
in that same transaction. A real launch or restart always revalidates READY,
profile revision, exact canonical readiness, and the pinned reference even when
the prior handle points to the same location; only `is_launch=False` status
refreshes use the no-op path.

## Materialization lifecycle

Canonical and regional copies use the same location state machine:

```text
PENDING -> COPYING -> READY
                 \-> FAILED
READY -> MISSING -> COPYING
READY -> EVICTING -> EVICTED -> PENDING

external adoption:
AWAITING_EXTERNAL_PUSH -> VERIFYING -> READY
                         |            \-> FAILED
                         \-> AWAITING_EXTERNAL_PUSH
FAILED -> AWAITING_EXTERNAL_PUSH      # authorized retry

builder-owned canonical publication (migration 024):
BUILD_RESERVED -> COPYING -> READY
                         \-> FAILED

admin or qualified abandoned-build purge only:
PENDING|BUILD_RESERVED|FAILED|MISSING|READY|EVICTED
  -> PURGE_PENDING -> EVICTING -> EVICTED
                                               \-> DELETE_UNKNOWN
DELETE_UNKNOWN -> inspection -> EVICTED|PURGE_PENDING|DELETE_UNKNOWN

external admin purge only:
PENDING|AWAITING_EXTERNAL_PUSH|VERIFYING|FAILED|MISSING|READY|EVICTED
  -> EXTERNAL_PURGE_PENDING
EXTERNAL_PURGE_PENDING -> EXTERNAL_PURGED
```

External-adoption claims use the ordinary fenced lease fields while VERIFYING.
Exact absence or a transient registry error returns to
`AWAITING_EXTERNAL_PUSH` with bounded backoff; a digest/platform mismatch or
byte-quota denial is FAILED with a closed code. No transition downloads from
the source or writes registry content.

An external location can enter `EXTERNAL_PURGE_PENDING` only with no live
lease or durable reference and while its artifact is PURGING. A VERIFYING row
must first settle or lose its fenced verification lease; an
`AWAITING_EXTERNAL_PUSH` row that was never materialized may be acknowledged
only with exact provider absence evidence or an external-owner attestation that
is explicit about never-pushed content. External regional locations are
acknowledged before their external canonical location. The admin acknowledgement
transaction locks artifact then location, rechecks external ownership, lifecycle,
zero references, and ordering, and stores bounded `acknowledged_by`,
`acknowledged_at`, `evidence_reference`, and its audit hash before setting
`EXTERNAL_PURGED`. Repeating the same acknowledgement is idempotent; changing a
terminal acknowledgement is rejected and requires a separate audited correction
event that does not make content managed. There is no automatic retry or delete
callback for external rows: failed out-of-band work remains
`EXTERNAL_PURGE_PENDING` until an admin truthfully acknowledges it. Schema checks
forbid external purge states on managed locations and managed purge states on
external locations.

Workers claim by stable location ID with an owner, token, and lease expiry.
Heartbeats extend only the matching claim. Completion is fenced by the token.
Migration 024 adds a closed lease kind. The distribution worker never claims a
BUILD_RESERVED row or a `BUILD_OUTPUT` lease; only the builder controller may
reclaim that generation/attempt/token-bound publication. A BUILD_RESERVED or
COPYING builder row must settle to FAILED before lifecycle cleanup can claim
it, and no purge claim can coexist with its live lease.
Copying is content-addressed and idempotent. READY requires exact destination
manifest or image-index digest equality plus a nonempty, bounded OCI platform
set obtained from the index descriptors or the single manifest's image config.
An index carrying a root `artifactType` is not runnable. A descriptor using an
image-manifest media type must have a valid digest, exact fetched-byte size,
platform, schema-v2 runnable child manifest, and matching child config
platform; one malformed image child fails the complete index even when another
child is valid. Explicit artifact descriptors and recognized attached
attestation or signature descriptors are ignored as platform evidence.
Digest-only callbacks cannot publish READY, including through external
adoption, and the catalog completion boundary repeats the nonempty check. A
forced retry leaves a READY reference usable while an independent verification
lease is pending. Only a confirmed mismatch changes it to MISSING; transient
inspection failures retain READY and retry with backoff.
The first canonical completion stores artifact-wide platform and compressed
size evidence under an artifact row lock. Later canonical completions may fill
an unknown size but otherwise must agree exactly with the established platform
set and known size; disagreement fails that location without changing the
artifact. Canonical dependency transitions update only children from that
canonical row's current profile revision, so an old generation cannot be
re-enabled by a new generation's completion.

The OCI copy primitive verifies the digest-pinned destination before every
push. A retry therefore succeeds without rewriting a deterministic tag when a
prior attempt committed it before losing its lease or completing verification,
including registries that enforce tag immutability. Any ambiguous copy failure
is followed by one fresh destination inspection and is accepted only when the
exact expected digest is present; otherwise the copy remains failed and
retryable.

The implementation now provides three data-plane primitives:

- adopt and verify an externally populated materialization;
- copy canonical content to a destination through an injected OCI copy and
  verification operation;
- revalidate a READY reference in place without making healthy content
  unavailable.

A bounded `reconcile_once` loop atomically selects one indexed due row at a
time, orders canonical before regional materializations, reclaims expired
leases, retries FAILED or MISSING content with exponential per-location
jitter, and processes requested READY verification. PENDING, expired COPYING,
FAILED or MISSING retry, and requested READY verification each have a partial
profile-prefixed queue index whose predicate excludes the exhausted automatic
attempt budget. Fresh work and deferred retries use disjoint indexes: fresh
verification is ordered by request time, deferred verification by
`next_retry_at`, fresh eviction by last use, and deferred eviction by
`next_retry_at`. Listing and atomic claiming use the same state-specific queue
probes rather than an OR-shaped whole-catalog query. Absent or partially
written historical lease triples have separate incomplete-lease queues;
structurally complete expired leases remain in expiry-ordered queues. A crash
after claim 20 is therefore terminal for automatic
work but not stranded: the exact operator retry path repairs expired COPYING or
EVICTING ownership conservatively, resets the budget, and rematerializes the
digest. The worker rotates those queue kinds in process, so each probe reads one
ordered index entry without letting a continuous import stream starve retry or
verification. READY verification additionally requires
a published target reference at both probe and claim time, so a corrupt row
cannot reach the worker as a nominally verifiable route. Every claim and retry
deadline reads a
fresh clock value, so a long earlier transfer cannot make later leases or
retries stale before they start. A background heartbeat extends copy, inspect,
and delete leases. Losing a lease sets a cancellation event consumed by the
OCI subprocess and prevents publication or destructive completion. Provider
credential callbacks execute only inside the isolated worker process.

Workers read at most 16 active profile generations per operation through a
primary-key keyset page and a process-local cursor, then perform a two-phase
claim. Reconciliation listing, reconciliation claiming, eviction listing, and
eviction claiming use independent cursors. End-of-table wrap is another
bounded keyset query, never an `OFFSET` or full-table materialization. Repeated
calls therefore revisit every profile while each call retains a fixed work
budget even if the durable catalog contains hundreds of thousands of profiles.
Each cursor also rotates the returned page, preserving alternating profile
fairness when a small catalog wraps on every call.
For regional work, the first phase starts from state-specific partial indexes
containing only rows whose denormalized exact-canonical dependency is READY.
The READY dependency is therefore an index predicate, not a filter that the
optimizer can satisfy by scanning dependency-blocked regional rows.
PostgreSQL partial-index planner matching is verified rather than assumed.
The second phase takes the shared
profile-generation lock, the exact canonical lock, and the regional row lock
in the established order, then rechecks every safety predicate on the selected
primary-key row. PostgreSQL uses `FOR UPDATE SKIP LOCKED` for the final row
lock. Workers that initially route to the same row seek forward through the
index until they claim distinct work, with a hard per-profile seek budget so a
single claim cannot retain the generation lock across an unbounded race set.
Claiming never groups or sorts the full eligible location queue. Reconcilers
take compatible `FOR KEY SHARE` locks on
one generation, so hundreds of workers can claim different rows in the same
dominant profile while profile activation still waits for those short claim
transactions. A worker that finds a generation under activation or exhausts
its rows continues through the remainder of its bounded page; the next call
starts after that page, so later profiles remain reachable without making one
operation proportional to catalog cardinality. Eviction uses the same
two-phase seek protocol and separate profile-prefixed READY and expired-lease
indexes.
Unchanged profile/location ensures take the shared generation lock; only a new
or conflicting generation takes the exclusive activation lock. Fleet launches
therefore serialize only when they target the same physical location row or an
administrator is actually changing the profile.

SkyServe placement identity retains a field-tagged tuple of every immutable
container selector field (`ref`, `release`, and `artifact_id`) plus the
distribution override. Equal text in different selector namespaces cannot
collapse two `any_of` candidates, and a combined ref-plus-release selector is
distinct from a ref-only selector. Once a service version is accepted, those
candidate selectors are snapshotted to one artifact ID before persistence, so
placement differences cannot make replicas in one version adopt different
content.

The concrete post-optimization placement carries a finite Linux OCI platform
set from its cloud catalog or exact administrator runtime binding. READY route
snapshots carry the artifact's verified platform set. The artifact must cover
every possible placement platform; a mismatch is a placement-specific
unavailable route, allowing normal cloud or region failover. The check repeats
in the atomic cluster-handle/reference commit so an internal or stale caller
cannot persist an incompatible plan. If neither the provider nor an exact
binding can produce that finite set, admission fails closed. Unknown is never
guessed and never treated as compatible. A proved runtime never treats missing
READY platform metadata as compatible, while pre-READY artifacts may retain an
empty platform field only as explicit metadata-not-yet-imported state for the
request-scoped source fallback.
GCP records Compute Engine's machine-type architecture in its generated
catalog and implements the same cloud abstraction used by placement and
durable-handle fencing. Pre-architecture GCP catalogs have a bounded
compatibility mapping for their known Arm families, so T2A, C4A, N4A, and A4X
cannot silently become unknown architecture.

An empty pre-READY platform set is not a verified managed route, but it also
does not make the caller's exact digest-pinned source incompatible. Under the
explicit managed-preferred plus prefer policy, warming resolution and its
durable commit may therefore use that source while metadata remains empty.
Nonempty verified metadata that excludes the runtime still fails before
fallback, and every READY or pinned managed route still requires proof.
Bootstrap tools used for runtime registry authentication, including AWS CLI
v2 for ECR, select their binary from the node architecture.

Regional verification discovery, claim, and completion, plus eviction claim
and completion, all recheck the exact `canonical_location_id` at the same
profile revision. An eviction error before the delete callback preserves
READY. Once deletion may have started, any ambiguous outcome transitions the
regional row to MISSING rather than asserting that a potentially deleted
manifest is still READY.

The generic OCI execution layer uses bounded subprocesses and digest-only
verification and deletion. Registry writes use a deterministic immutable
`sha256-<hex>` tag because standard registry pushes are tag-addressed; runtime
plans and durable locations expose only the verified digest reference. It
never transports layers through the API server. The exact rendered shard
repository is passed to the provider adapter for ownership-aware provisioning
and short-lived credential scoping, and those provider responsibilities must
be completed before declaring a managed profile production-ready. The complete
rendered destination is validated against OCI grammar and Docker's
repository-name limit before repository provisioning, credential issuance, or
copy I/O begins.

Deletion has its own provider adapter authorization. A YAML profile declaration
with `ownership: managed` is necessary but never sufficient: immediately before
the delete callback, the adapter must prove that SkyPilot owns the exact
repository under the configured realm. Until an AWS, GCP, Nebius, or generic
adapter can verify provider ownership metadata, automatic eviction for that
adapter fails closed before registry I/O and leaves the regional route READY.

## Administrative lifecycle and retention

A million-artifact catalog cannot retain canonical customer content forever.
The first production slice therefore has a safe manual erasure path even though
the dashboard intentionally has no delete button:

```text
sky image tombstone --artifact-id UUID --reason CODE --yes
sky image purge --artifact-id UUID [--retry] --yes   # admin only
sky image purge acknowledge-external --artifact-id UUID \
    --location-id UUID --evidence-reference REF --yes
```

Tombstone is an admin-only PostgreSQL transaction. It follows the global lock
order through artifact, source/build producer, locations, references,
publications, and releases; rejects any live durable consumer, live
data-plane or build-publication lease, or active publication; marks the artifact
TOMBSTONED; and makes all selectors unavailable for new launches.

The catalog defines a narrow `CatalogLifecycleExtension` protocol whose
`retire_outputs_in_session(session, artifact_id)` callback receives the existing
transaction. Producer modules register implementations at API startup; catalog
lifecycle code never imports `build_state.py`. With no extension registered,
SOURCE artifacts need no callback. A managed-build artifact fails closed with
`PRODUCER_LIFECYCLE_EXTENSION_UNAVAILABLE` rather than committing a partial
tombstone. Migration 024 and API-63 startup register the builder implementation
even when the separate build controller is disabled. It changes every READY
build producing the artifact to `OUTPUT_RETIRED` in the same transaction. Build
cache-hit transactions independently read the output ID, lock artifact then
build row, and revalidate lifecycle as defense in depth. Tombstone preserves
immutable identity and actor-attributed audit metadata. Existing running
consumers are a hard precondition failure, never a force flag.

Purge changes TOMBSTONED to PURGING and queues every managed regional and
canonical location for deletion, regional first. External locations enter
`EXTERNAL_PURGE_PENDING` and require an admin to record a bounded, non-secret evidence
reference after the external owner deletes them. Every worker
attempt rechecks lifecycle state, zero references, exact digest, active profile
revision for availability work, or the location's retained historical custody
for purge work, plus provider capability and repository ownership immediately before
registry I/O. Ambiguous managed deletion enters `DELETE_UNKNOWN`, distinct from
ordinary availability `MISSING`. It blocks canonical deletion, quota release,
and PURGED. A fenced inspection transitions it to EVICTED only after an exact
digest lookup proves absence, or back to purge-pending when the manifest still
exists; inconclusive inspection remains DELETE_UNKNOWN. A preexisting MISSING
row requires the same absence proof. There is no acknowledgement override for
managed content. Canonical deletion is allowed only after every managed
regional location is EVICTED and every external location is acknowledged.

PURGED is committed only when every SkyPilot-controlled manifest reference has
been proven absent, every managed location is EVICTED, and every external
location is `EXTERNAL_PURGED`. This proves loss of runtime reachability
through SkyPilot, not physical blob erasure: ECR and other registries may retain
unreferenced child manifests or layers until provider garbage collection.
SkyPilot never labels this state legal byte erasure unless a provider-specific
extension supplies an auditable blob-GC guarantee or artifact-scoped encryption
destruction. Successful release rows remain as nonresolving tombstones so a
name that was ever launchable can never be reused. RELEASED never-launchable
publication generations remain in audit history but do not block the explicit
new reservation generation. Raw source
references and operational details are cleared under retention policy, leaving
only nonreversible hashes when audit policy requires provenance. The compact
record retains artifact ID/digest, successful release bindings and released
reservation generations, closed deletion
outcomes, timestamps, and audit events, and releases artifact byte quota. A
Failed purges retain a closed error and can be retried by an admin.
Automatic regional cache retention continues to evict only noncanonical
unreferenced content. Automatic canonical purge remains out of scope except for
the narrow migration-024 abandoned-build rule: after retention, a system actor
may tombstone a `managed_build` artifact only when every producing build is
FAILED or CANCELLED, every output lease is settled, and there is no release,
publication, source alias, durable reference, or READY build. It records
`BUILD_OUTPUT_ABANDONED` and then invokes the same artifact-scoped purge engine,
ownership checks, DELETE_UNKNOWN recovery, and audit path as an administrator.

Default retention is explicit: terminal publication attempts are retained for
30 days, copy and purge attempt diagnostics for 90 days, and compact audit
records for the platform audit policy. The catalog exposes aggregate retained
bytes and tombstoned age so administrators can act before workspace quotas are
exhausted. No cleanup deletes a row with a live lease or durable reference.

## Provisioning and registry replication

SkyPilot should provision registry repositories automatically only inside a
pre-bootstrapped, explicitly managed realm. Bootstrap remains an administrator
operation because accounts, projects, organization policy, KMS, audit policy,
and IAM trust are outside a workload scheduler's safe authority.

The manager identity may receive:

- repository or registry namespace creation;
- content push and exact-digest verification;
- regional cache manifest deletion;
- read-only ownership-tag inspection.

It must not receive:

- account, project, IAM, or KMS administration;
- repository deletion;
- canonical manifest deletion;
- mutation of colliding resources without SkyPilot ownership metadata.

Canonical purge uses a separate, opt-in purge identity. It can delete a
manifest only inside ownership-tagged SkyPilot repositories after the central
database has issued an artifact-scoped purge lease. The ordinary API, copy
worker, and runtime identities cannot assume it. Repository deletion remains
forbidden even to the purge identity.

Do not copy every image to every configured region. The intended strategy is:

1. create canonical intent when an artifact is first used;
2. create a regional intent only for an observed placement or explicit
   prepare request;
3. use explicit worker copies in v1; evaluate native replication only through
   a later tracked-eager contract;
4. call a location READY only after verifying the exact digest in that region;
5. evict unused managed regional content after policy retention, never while
   a durable consumer references it.

Provider-native replication may reduce later controller work, but an
untracked provider rule is not a READY image and violates lazy quota authority.
V1 does not enable it. Any later tracked-eager implementation must create and
charge all destination intents before the source push, then wait for exact
digest visibility; asynchronous results remain COPYING or WARMING until
verified.

### First managed provider: AWS ECR

The AWS adapter implements one complete path before any other provider is
called managed. `ensure_repository` describes the deterministic shard, creates
it only when absent with required realm/workspace/manager tags, and rejects a
name collision whose tags, encryption, mutability, or scan policy do not match.
Create permission is bounded by repository prefix and required request tags.
The adapter never changes a colliding repository into compliance.

For writes, the worker assumes a short-lived copy role with an artifact-target
session name and repository-scoped session policy, then obtains the normal ECR
authorization token. ECR tokens are registry scoped, but the effective STS/IAM
permissions remain limited to the exact repository actions. Source and
destination authority are acquired independently; no destination credential is
sent to a source registry. Tokens live only in worker memory and subprocess
stdin/environment for the bounded copy, are redacted from process diagnostics,
and never enter PostgreSQL, task YAML, or logs.

V1 canonical import accepts only an anonymous digest-pinned source or a
provider source reachable through the configured worker identity. It has no
user-supplied generic OCI credential schema and cannot privately pull GHCR or
another external registry merely because the destination is managed. Private
external content is pushed to the configured canonical destination out of band
and enters through the exact-digest external-adoption primitive. A later source
credential broker is a separate security feature. Unsupported private imports
fail before a copy lease and never persist rejected credential material.

External adoption reserves artifact and location-record quota at intent time.
Because size is unknown until inspection, READY completion locks the workspace
quota and reserves verified bytes before adopting; insufficient byte budget
leaves `AWAITING_EXTERNAL_PUSH` with closed
`EXTERNAL_BYTE_QUOTA_EXCEEDED`, never a false READY row. The operator can delete
the external bytes or an authorized user can retry after quota changes. Register,
publish, retry, and diagnostics use the ordinary workspace RBAC and audit paths.
The API and UI never return registry credentials or claim that SkyPilot pushed
external content.

The adapter uses exact manifest digest reads before and after copy. A matching
existing digest is an idempotent success. A mismatch can never overwrite a
managed immutable tag. Runtime EC2 uses the workload role's ECR pull actions;
Kubernetes uses an exact node-identity context binding. Regional eviction and
admin purge delete only the exact manifest digest after ownership checks;
repository deletion is never granted. Integration tests cover create, adopt,
copy, verify, pull, rotate, delete, tag collision, wrong account/region, expired
STS, and every denied IAM action.

### Infrastructure modules and activation

The first production bootstrap is AWS-specific and delivered as composable
Terraform rather than hidden placement-time side effects. These are target
paths created and validated by this PR; the current branch has no
`infra/terraform` tree:

```text
infra/terraform/modules/aws-control-plane
infra/terraform/modules/aws-vm-pool
infra/terraform/modules/aws-image-distribution
infra/terraform/examples/aws-dedicated-account
```

`aws-control-plane` creates the API and image-worker identities plus policy
attachment points. `aws-vm-pool` creates workload identities and ECR pull
permissions without assuming one GPU per VM. `aws-image-distribution` creates
the managed realm, KMS/log/metric policy when requested, least-privilege worker
permissions, and ownership tags. The separately gated builder design owns any
later `aws-image-builder` module. PostgreSQL remains the work queue, so the
distribution module does not create a second SQS or dead-letter source of truth.

The modules create no per-image resources and copy no content. The AWS adapter
creates a deterministic ECR shard repository just in time under the
pre-authorized realm, using request tags and a bounded repository-name prefix,
then verifies those ownership tags before mutation. This happens in the copy
worker, never during placement admission. A dedicated-account example wires
all enabled regions while preserving region-level opt-in.

Shard sizing is a two-sided feasibility proof for every `(account, region)`.
The immutable realm shard count is a power of two from 1 through the public v1
ceiling of 256. This bounds repository fan-out and matches the two-hex-character
digest prefix, but a realm need not allocate all 256. The module reads applied
ECR repository and images-per-repository quotas through Service Quotas. It does
not claim Terraform can enumerate every existing image. An accompanying
read-only `sky image infrastructure inventory aws` preflight uses paginated ECR
APIs to emit a timestamped, account/region-bound JSON file with existing
repository and per-repository manifest counts. Terraform accepts that audited
file plus explicit safety headroom, workspace capacity, maximum retained
artifacts per workspace, and allowed platforms per artifact. A stale,
wrong-account, incomplete, or omitted inventory fails activation unless the
account is explicitly asserted empty through a separately audited bootstrap
mode. One single-platform artifact consumes one manifest unit; a runnable index
is conservatively charged as one root plus one unit for every allowed platform,
without assuming cross-artifact blob or child-manifest deduplication.

For candidate power-of-two shard count `S`, every runtime repository must
satisfy:

```text
ceil(max_retained_artifacts_per_workspace / S)
  * max_manifest_units_per_artifact
  + existing_manifest_units
  + image_headroom
<= applied_images_per_repository_quota
```

The regional repository budget must simultaneously cover
`workspace_capacity * S`, inventoried existing repositories, and repository
headroom. Workspace `max_artifacts` counts ACTIVE and retained lifecycle rows
until confirmed purge/compaction, so tombstones cannot hide manifest demand.
Terraform selects or validates one feasible `S` at or below 256 and fails
activation when the image lower bound and repository upper bound do not
intersect. More shards relieve image density but consume repository quota;
separate realms or approved quota increases are the escape hatch.

For the documented Boltz AMD64 example, one million retained single-manifest
artifacts and an applied 100,000-images-per-repository quota require at least 10
repositories, so the next power of two is 16 before headroom. This is an example
calculation, not an assumed account quota. The module always uses the live
applied values and refuses an infeasible plan.

The secret-free Terraform output records the exact applied quota snapshot,
inventory digest/time, sizing inputs, selected shard count,
`workspace_capacity`, and realm generation.
PostgreSQL reserves one durable realm slot before a workspace can activate its
first managed profile; capacity exhaustion fails before repository I/O with
`REGISTRY_WORKSPACE_CAPACITY_EXHAUSTED`. Removing a workspace does not silently
reuse its namespace slot while retained artifacts exist. Activation and every
just-in-time repository creation recheck stored capacity against provider drift;
closed `REPOSITORY_QUOTA_EXHAUSTED` and
`IMAGES_PER_REPOSITORY_QUOTA_EXHAUSTED` outcomes are retryable and point to the
quota/headroom runbook.

An existing namespace never silently changes shard count. Capacity expansion
creates a new realm generation whose mandatory `{realm_generation}` rendering
produces a different repository prefix, plus a new profile revision. Activation
proves the new prefix does not overlap any retained generation before it directs
new writes there. It keeps old READY routes readable, lazily prepares content
on observed use, and retains old durable references until they drain. Only then
does ordinary ownership-fenced lifecycle purge the old generation. The
migration has no all-artifact eager-copy step.

Terraform outputs a secret-free profile fragment without `revision`, plus the
complete normalized `profile_fingerprint`. The administrator supplies the
monotonic revision in SkyPilot config. CI rejects a fingerprint change without
a revision increment. Terraform cannot infer or mutate the active generation
stored in PostgreSQL, so it never pretends a content hash is that generation.
The outputs also include role ARNs, registry prefixes, and runtime policy
attachments, but never credentials.

Provider-native pull-through cache remains a possible source accelerator only
when its upstream, account, region, encryption, and immutability behavior
matches the profile; a miss still creates durable intent and READY still
requires exact verification. Native ECR cross-region replication is excluded
from v1 because prefix rules replicate every matching new push, create content
without per-artifact lazy intent or byte reservation, and do not mirror delete
lifecycle. A future explicit `replication_mode: tracked_eager` would have to
reserve every destination location and worst-case byte/manifest quota before
canonical push, reconcile each result, and delete each destination explicitly.
It is not a transparent optimization of lazy placement.

Placement remains nonblocking under the default `locality: prefer` policy:

1. one bounded PostgreSQL read chooses a READY local location when available;
2. otherwise it enqueues an idempotent local intent without waiting for
   repository creation, copy, or verification; and
3. it immediately uses an already READY authorized canonical route, or the
   exact digest-pinned source only when that source was present in this request.

A release-only or artifact-only selector with no READY route fails fast with a
closed `IMAGE_WARMING` result and a publication or preparation ID. It never
inherits a historical source alias. This is nonblocking but not fictitious
availability: callers may wait on the direct status resource or prepare before
fleet deployment.

Only `locality: require` waits for the selected target, and that wait is
visible, cancellable, and bounded by the normal deployment timeout. Provider
API latency, registry throttling, and a failed warm therefore cannot add hidden
latency to ordinary deployment admission. Workers scale independently from
the API service. The Helm chart installs a separate `Deployment`, not an API
sidecar, running `python -m sky.container_images.worker_service`. It has
configurable replicas, graceful lease release, readiness/liveness probes, and
`max_in_flight` per pod, plus local smoothing buckets layered beneath the
shared registry-permit scheduler. The
default is not a hidden global concurrency of five. Queue depth, oldest due
age, and bytes in flight drive an operator-set replica count or an optional
metrics-backed autoscaler. One image copied to three targets creates three jobs
whether the fleet has one GPU or 1,000; GPU replica count is not itself
copy-worker concurrency.

The provider adapter is a capability boundary. Each provider advertises
namespace provisioning, short-lived copy authentication, pull authentication,
verification, native replication, ownership proof, and deletion support. A
profile cannot enable an operation the adapter does not prove. The AWS ECR
adapter is the first managed implementation: repository creation, short-lived
writer authentication, exact digest verification, runtime pull identity,
ownership proof, and manifest deletion all have integration and negative-IAM
tests. GCP, Nebius, and generic OCI begin as `ownership: external`; their
operators provision namespaces and credentials, and SkyPilot may copy only
through an explicitly implemented capability. Core catalog, resolver, and
worker state contain no Nebius-specific branch.

Cloudflare is experimental until an official OCI registry product exposes
documented repository ownership, scoped credentials, pull compatibility, and
deletion APIs that pass a production pull test. An operator may configure such
an endpoint through the generic external OCI adapter. Terraform does not claim
to provision it automatically. Raw R2 remains suitable for the builder's
S3-compatible context/bounded-log store or an explicit rollback archive, never
as a BuildKit registry cache or runtime image fallback. A separately deployed
OCI Distribution service may use R2 for staging/cache blobs only after the
builder-specific conformance gate; clients still speak OCI to that service,
not S3 to R2.

## Runtime and multi-GPU behavior

Distribution is node-scoped. An EC2 instance with four GPUs pulls one image
for the node. The workload entrypoint or process supervisor starts one model
process per visible GPU when that topology is desired. Registry code must not
copy or pull the image four times.

On Kubernetes, one pod per GPU is expressed through replica count and GPU
requests. Registry resolution happens per workload template; normal node-level
containerd caching deduplicates layer pulls. SkyPilot does not invent
Kubernetes pods on raw VMs.

Cloud conditionals should be confined to two adapter boundaries:

- materialization: namespace ownership/provisioning and short-lived writer
  credentials;
- runtime: provider/backend-specific pull capability and secret-free auth
  strategy.

Artifact, release, location, reference, lease, retry, and resolver logic stay
provider-neutral.

## Safety and failure semantics

- Serialized tasks, launch handles, database rows, status responses, and logs
  contain strategy names and references, never tokens or passwords.
- Legacy Docker `image_id` references are validated and canonicalized before a
  value-free deprecation warning is emitted. Rejected references produce no
  warning, and ambiguity errors never interpolate the untrusted image values.
- Explicit `container_image` tasks reject nonempty inline Docker username or
  password credentials at resource construction, atomic task mutation,
  serialization, pickle restoration, server preflight, final request-row
  encoding, and cluster-handle persistence. This rule spans all resource
  alternatives and restored v35+ resources, and is reapplied before pickle
  bytes are allocated; legacy `image_id: docker:...` tasks retain their
  compatibility path. Managed routes use public access or server-side workload
  identity instead of serializing registry secrets.
- Image-route request validation never reflects rejected raw inputs or
  Pydantic contexts in 422 responses; query-model failures use the same
  value-free boundary.
- Uvicorn access logging strips the complete query string before formatting,
  so rejected selectors and workspace values cannot reach access logs before
  route validation. Structured task YAML validates the shape of `any_of` and
  `ordered` wrappers plus every direct and alternative `container_image` field
  before generic JSON-schema reporting, then collapses failures to one
  value-free error without an exception cause.
- Release labels use one secret-free OCI-tag grammar at model, API, YAML,
  pickle, state, and catalog-response boundaries, and invalid values are never
  reflected into terminal-facing errors.
- Artifact/location UUIDs and distribution/target identifiers use bounded,
  secret-free grammars across request, config, YAML, pickle, state, runtime,
  and response boundaries; response validators hide rejected input values.
- Restartable managed pull plans carry and atomically validate their complete
  distribution policy snapshot; policy-only auth changes cannot reuse stale
  serialized authority.
- Image routes and resolved runtime plans enforce the same secret-free OCI
  reference grammar and a closed set of auth-strategy names at construction.
- Mutable managed tags fail before catalog registration.
- Digest mismatch never publishes READY.
- `managed_preferred` may fall back only to the explicitly supplied pinned
  source and only when policy permits it.
- Private cross-cloud canonical fallback is infeasible unless the placement
  declares a safe runtime pull strategy.
- A failed regional warm does not interrupt a running source or canonical
  pull; it is visible as FAILED for later retry.
- Canonical content is never an automatic eviction candidate.
- External locations are never automatically deleted.
- Managed regional eviction rechecks age, state, ownership, digest, lease, and
  durable references at the claim and completion boundaries. Completion also
  requires the exact canonical origin to remain READY.
- The generic durable-reference API refuses `consumer_type: cluster`. Cluster
  references use the cluster-handle transaction, keeping one lock ordering and
  preventing a generic-reference versus cluster-commit inversion.
- Explicit and provider-derived registry authorities share one canonicalizer
  for DNS case, trailing DNS dots, IPv4/IPv6 spelling, and the default HTTPS
  port before identity comparison and reference rendering. Configured and
  templated repository paths use the OCI lowercase component grammar; managed
  artifact paths use the namespace generation's quota-proved fixed
  power-of-two shard count, and invalid paths are rejected rather than
  rewritten.

## Operational surfaces

```text
sky image register REF [--distribution NAME]
sky image publish REF --release RELEASE [--distribution NAME] [--no-wait]
sky image publish --artifact-id UUID --release RELEASE \
    --distribution NAME [--no-wait]
sky image status [ARTIFACT|RELEASE|REF]
sky image prepare IMAGE --targets TARGET[,TARGET...] [--distribution NAME]
sky image retry IMAGE --target TARGET
sky image infrastructure inventory aws --account ACCOUNT --regions R[,R...] \
    --output inventory.json                         # admin/read-only
sky image publication release-name PUBLICATION_ID --reason CODE --yes # admin
sky image tombstone --artifact-id UUID --reason CODE --yes       # admin
sky image purge --artifact-id UUID [--retry] --yes               # admin
```

`register` establishes immutable catalog identity and canonical intent.
`publish` resolves exactly one source or artifact selector, reserves a release,
and by default waits client-side until canonical verification atomically makes
that release available. Artifact publication never synthesizes source
provenance. `prepare` establishes
requested physical-cache intent. No operation waits inside the API executor
for OCI transfer, and none copies to unrequested regions.

Bare operational selectors remain untyped through CLI and HTTP transport.
The server resolves artifact, release, and source candidates together. The
distribution override is a separate prepare field, so adding
`--distribution` cannot silently reinterpret a release as a source reference.
When a selector includes both `ref` and `release`, both fields must identify the
same digest and preparation always registers or validates the source alias;
neither identity field may be silently ignored.

Status reports artifact identity, source aliases, releases, producer metadata,
and every materialization with location ID, distribution, endpoint revision,
canonical flag, state, attempts, verification time, use time, and a closed
diagnostic code. Copy callbacks are untrusted metadata producers: platform
strings and compressed size are bounded before READY persistence, validated
again when catalog rows are decoded, and validated a third time by the response
model. Producer metadata follows the same three-boundary rule: producer kind is
closed, the optional producer spec hash is 64 hexadecimal characters, and the
optional builder version uses a bounded control-plane identifier. Registry
control values use closed or bounded secret-free grammars. Every
status-exposed location string, including both fingerprints, is revalidated
when its catalog row is decoded and when its response model is constructed.
Image workspace values use SkyPilot's established bounded workspace grammar
at request, catalog-write, catalog-decode, direct-core, and response
boundaries. A workspace-resolution denial or ambiguity is translated before
scheduling into a closed value-free 403 or 422 response.
The image CLI passes `--workspace` through the explicit SDK/HTTP workspace
field for publish, status, prepare, and retry; it does not rely on the generic
client config override being interpreted by these purpose-built routes.
Direct core calls default the workspace only when the caller supplies `None`;
an explicit empty or otherwise invalid value is rejected before any catalog
read or write.
Provider validation errors never interpolate rejected values. As a final
defense, task-bearing launch, exec, optimize, managed-jobs, pool, and Serve
payloads preflight every `resources.container_image` selector and every
effective legacy Docker `resources.image_id` form, including the reserved
`docker` key, `docker:` values, Kubernetes interpretation, and inherited
`any_of` or `ordered` candidates, before the raw YAML can enter the request
database. The candidate traversal is iterative, bounded, and cycle-safe for
YAML aliases. Rejected task inputs use the same value-free 422 boundary as
image routes. Every direct image request and every task or DAG using either
modern or legacy container-image syntax replaces terminal exceptions with one
closed, value-free error before executor logging, request-database persistence,
or `/api/get` encoding. Provider exceptions and caller-supplied strings are
never serialized into logs, the catalog, launch state, async request database,
or terminal API response. Config, CLI override, Task, DAG, Jobs recovery, and
Serve snapshot parsing reject malformed YAML with a fixed value-free error;
administrator config and task/DAG entrypoints also reject duplicate mapping
keys before PyYAML can silently weaken the effective policy. Duplicate errors
never reflect the key text. The YAML node-graph scan tracks active and visited
node identities, rejects recursive aliases, and enforces a fixed edge budget
so alias cycles or amplification cannot recurse or consume unbounded work.
Unfiltered legacy status remains bounded for SDK compatibility. Large catalogs
use the direct keyset-paginated catalog endpoint. Sources, releases, and
locations are loaded in three batch queries rather than three queries per
artifact. Artifact,
source, creation-order,
materialization-queue, verification-queue, and eviction-queue lookups are
indexed so hot paths do not scan a million-row catalog. Canonical and regional
queue indexes are separate so PostgreSQL cannot prefer a global due-order
index that filters every blocked regional row. Workspace is denormalized onto
each location row specifically so queue probes do not join through artifact
identity. No frozen performance result is claimed until the repository contains
the reproducible PostgreSQL fixture, captured plans, database settings, host
description, and raw result artifact. The release gate requires those plans to
use the READY-canonical and canonical-location-prefixed regional partial indexes
without scanning dependency-blocked rows.
A separate PostgreSQL page-advance regression places work beyond the first
eight 16-profile pages and proves reconciliation and eviction listing plus
atomic claiming eventually reach it.

Dashboard catalog scale is a separate gate from worker-queue scale. A frozen
PostgreSQL 16 fixture with one million artifacts captures
`EXPLAIN (ANALYZE, BUFFERS)` and wall time for the first page, a deep keyset
page, exact artifact/release/source selectors, every explicitly supported
filter shape above, zero-match filters, and the fixed-query
alias/location-summary batch load. The same fixture covers first, deep,
current-only, and historical location-detail pages for the highest-cardinality
artifact. Each request must issue a constant number of SQL statements, avoid
disk spill and sequential million-row scans, and complete under 200 ms p95 on
the release benchmark host. An unfiltered page may examine at most twice its
artifact limit. A distribution-filtered page first resolves one profile head,
must use the exact active-revision facet index, and may examine at most
`limit * 128` qualifying facet rows under the database-enforced per-artifact
hard bound to deduplicate one page. An exact zero-match probe is an index-only
lookup over one distribution and revision. State-only queries do not exist, so
historical facets and an unbounded number of profile heads cannot invalidate the
claim. Executor memory stays below 64 MiB. The fixture also injects
projection drift, proves authority paths ignore it, verifies detection, and
exercises bounded repair. Evidence
records PostgreSQL settings and hardware; a unit test alone is not accepted as
proof.

The worker deployment must expose queue depth, claim age, copy bytes and
duration, verification mismatch, retry count, source-fallback selection,
canonical selection, regional selection, and eviction outcome. A production
rollout is incomplete until this worker is deployed independently from API
request executors and restart recovery is exercised.

## Rollout

### Foundation implemented on this branch

- first-class `resources.container_image` and compatibility normalization;
- source registration and the original publication/prepare foundation, with
  the corrected READY-gated release reservation still to be implemented;
- all-target prepare prevalidation before any catalog mutation;
- immutable artifact and many-release identity;
- physically revisioned canonical/regional locations with independent policy
  revisions, monotonic catalog generations, and exact canonical bindings;
- fenced copy and eviction leases;
- constant-work profile activation plus lazy exact-row settlement of expired
  copy and eviction transitions without requiring the old canonical manifest;
- half-open lease expiry, semantic lease-state constraints, and inactive-token
  clearing across exact profile transfer;
- exact-digest external adoption and copy primitives;
- bounded per-process profile and queue-kind rotation, state-specific partial
  canonical and regional indexes, PostgreSQL dependency-first lateral probes,
  compatible shared generation locks, and two-phase seek-forward
  reconciliation and eviction without whole-queue aggregation or blocked-row
  scans;
- 16-profile primary-key keyset pages with independent operation cursors,
  bounded end-of-catalog wrap, 200,000-profile fixed-work proofs, and
  reconciliation/eviction later-page liveness proofs;
- denormalized workspace queue routing and the indexes required by the pending
  checked-in million-row PostgreSQL benchmark;
- heartbeat cancellation, jittered retry backoff, and READY revalidation
  primitives;
- ensure-on-use after quota checks, with dry-run and optimizer purity;
- immediate pinned-source fallback with visible WARMING state;
- one immutable SkyServe artifact snapshot per service version across all
  resource candidates, taken before version/controller persistence;
- placement-aware OCI platform filtering, final durable architecture fencing,
  and architecture-aware ECR tooling bootstrap;
- explicit Kubernetes context pull bindings;
- durable launch references and reference-aware eviction;
- provider-proved deletion authority and fail-closed managed eviction;
- atomic cluster-handle/reference persistence, refresh no-ops, exact
  controller catalog authority, and old-server legacy image down-conversion;
- final-commit selector, complete-profile, placement-auth, derived-login, source
  fallback, and revision-fingerprint validation, plus a complete execution-state
  refresh fence, INIT-to-READY execution-plan preservation, and current-policy
  re-resolution on later stopped-cluster restarts;
- REST, SDK, CLI, schema, database migration, and focused unit coverage.

### Productization in the current implementation round

- [ ] Rewrite migration 023 as the frozen literal 17-table PostgreSQL schema,
  including config-generation authority, realm-prefix uniqueness, projections,
  limiters, constraints, and exact indexes.
- [ ] Replace best-effort image config reload with the shared central-config and
  profile-head compare-and-swap protocol, replica convergence, and crash tests.
- [ ] Add READY-gated publication reservations and client-side wait semantics;
  remove launchability of pending releases.
- [ ] Fail closed for unknown runtime architectures unless an exact runtime
  binding proves a finite platform set covered by the artifact.
- [ ] Add cursor-paginated catalog and secret-free profile-summary APIs.
- [ ] Add transactionally maintained summaries/facets for only the closed query
  shapes and check in their million-row PostgreSQL plans before activation.
- [ ] Build the complete Images dashboard, artifact detail, safe actions,
  workspace handling, responsive states, and dashboard test/build coverage.
- [ ] Add admin-only tombstone/purge state, CLI/API, audit, retry, and AWS
  ownership fencing without a dashboard delete action.
- [ ] Implement the complete AWS ECR capability slice and reusable AWS control
  plane, VM-pool, image-distribution, and dedicated-account Terraform example,
  including generation-qualified shard identity and the closed per-call ECR
  limiter map.
- [ ] Deploy the independently scalable copy worker through Helm, with metrics,
  configurable concurrency, probes, restart recovery, and rollback runbook.
- [ ] Implement only the managed-builder prototype and evidence gate described
  in `managed-container-image-builder.md`; do not add migration 024, public
  build syntax, durable builder workers, or Build UI before it passes.
- [ ] Run the focused backend/dashboard/Terraform suites, formatting, dashboard
  production build, and manual PostgreSQL/browser verification.
- [ ] Capture the independent million-artifact catalog plans and the AWS
  integration/negative-IAM evidence. Treat large-fleet timing as an activation
  gate when real fleet capacity is available.
- [ ] Freeze the exact design and implementation, then complete six fresh
  paired Codex 5.6/Fable rounds with three consecutive paired `PURSUE`
  verdicts on the unchanged patch.

### Activation sequence keeps the first operational slice small

The code, schema, typed API, and complete UI are reviewed and merged together,
but every new surface defaults disabled. Deployment first applies migration 023
with workers and managed profile creation off. The second phase enables bounded
catalog reads and the Images UI only after schema parity and checked-in query
plans pass. The third phase enables the typed editor only after config/head crash
recovery and replica-generation convergence pass. The first data-plane canary is
one managed AWS realm generation, one fixed shard plan, canonical publication,
and one locality target. Additional regions and worker replicas may scale that
same generation after limiter evidence. Creating a second realm generation is a
separate canary after old-generation drain and prefix-nonoverlap tests.

This staging does not omit the requested UI or large-catalog implementation
from the release artifact. It prevents an unproven dashboard projection,
configuration editor, million-row claim, or shard-expansion path from becoming
production authority merely because its code was deployed. The digest-only
external profile remains the rollback path at every phase.

### Required before a managed production profile

- isolated mutable-tag resolver/import worker, or keep the digest-only API;
- independently deployed materialization reconciler using the implemented
  lease/backoff loop, plus metrics and restart exercises;
- provider namespace provisioning, short-lived copy credentials, and exact
  repository deletion authorization for each advertised managed provider;
- completed manual canonical lifecycle and purge-recovery exercise;
- Kubernetes secret-name injection if that auth mode is advertised;
- READY periodic revalidation and orphaned profile-revision cleanup;
- PostgreSQL migration tests;
- old-client/new-server and new-client/old-server compatibility validation;
- full provider integration tests, including negative IAM tests.

### Boltz L4 fleet conversion

1. Publish the existing Boltz digest under an immutable release.
2. Adopt or prepare canonical ECR and verify the exact digest.
3. Prepare the AWS location through the managed slice. Use externally
   provisioned profiles for GCP and Nebius until their adapters qualify.
4. Run one-instance pull and process-topology canaries in each backend.
5. Compare cold-start time and registry throttling at 100, 500, and 1,000
   replicas against direct cross-region and operator-prewarmed controls.
6. Gate fleet scale-up on `locality: require` readiness.
7. Keep the R2 archive as a bounded rollback path during soak, then remove the
   per-cloud archive/load workaround.

## Test strategy

Unit tests must cover:

- register creating immutable identity and canonical intent without a release;
  publish creating a unique durable reservation while leaving release lookup
  unavailable until exact canonical READY completion atomically inserts the
  release and marks publication READY; failed, retried, cancelled, conflicting,
  identical concurrent, and admin RELEASED generations preserve those
  invariants; a never-READY name can be released only under every stated
  precondition, restores quota once, and rejects stale finalizers, while a name
  that was ever READY remains immutable;
  publishing another release for an already READY canonical route finalizes
  immediately through the same locked validator;
  artifact-ID publication creates no source row, requires one authorized ACTIVE
  READY canonical distribution, shares reservation quota and finalization, and
  rejects selector/distribution ambiguity;
  prepare remains the only explicit regional fan-out operation;
- fresh real-PostgreSQL 022-to-023 migration from frozen literal DDL, exact
  parity with checked-in distribution metadata, the exact 17-table inventory
  listed above with every named check/index/foreign key, absence of obsolete
  legacy tables/columns, and no live-metadata imports. The separately gated builder
  release owns its later 022-to-023-to-024 parity and pre-024 compatibility
  probe;
- the named 023 external-OCI-only producer and SOURCE-only origin constraints.
  Their future atomic 024 replacement, build-token locking, and BUILD provenance
  are builder-release gates rather than distribution activation claims;
- scalar/object selectors and compatibility aliases;
- combined source-plus-release selectors validate both identities and register
  every supplied source alias;
- explicit operational selector namespaces, rejection of cross-namespace
  ambiguity, acceptance only when a bare value matches the artifact, release,
  or OCI-source grammar, and value-free rejection of all other values before
  request persistence or terminal errors;
- strict secret-free OCI source-reference grammar before model, API, or state
  persistence, at Docker's repository-length boundary, and again at worker
  destination construction, runtime-plan construction, and completion;
- secret-free immutable release grammar across model, REST, task YAML, pickle,
  state, and status response boundaries, including credential URL and terminal
  control regressions;
- canonical artifact/location UUIDs and bounded distribution/target names at
  model, REST, config, task YAML, pickle, state, runtime-plan, and response
  boundaries, including credential URL and terminal-control rejection without
  reflected values;
- structured publish and prepare dictionaries sanitize unsupported keys before
  Pydantic can retain their names or values in local validation details, and
  malformed registry authorities clear raw parser cause chains across
  Resources, OCI, and administrator-config entrypoints;
- bounded provider, region, account, project, manager-identity, and closed
  pull-auth configuration at model and adapter boundaries, plus value-free
  terminal persistence and `/api/get` round trips for every image request name;
- rejection of mixed managed/external ownership in one profile and of
  overlapping target locality at configuration admission;
- reserved `distribution: direct` accepted only as an explicit
  managed-preferred source bypass, rejected as a profile/default or under
  managed-required, and removal/rejection of the no-op
  `require_digest_at_runtime` key while all managed runtime references remain
  digest pinned;
- pre-persistence, value-free task-YAML validation across launch, exec,
  optimize, managed jobs, pools, and Serve for both `container_image` and every
  effective legacy Docker `image_id` form, plus actual-router 422 matrices for
  direct, `any_of`, and `ordered` resources and process/coroutine terminal-error
  regressions proving credential-bearing image values and provider errors
  cannot cross logs, the request database, or the API;
- malformed config, CLI override, Task, DAG, Jobs, and Serve YAML fails closed
  with no parser value or cause-chain reflection, while duplicate administrator
  policy and task keys are rejected before effective-value selection, with
  value-free duplicate errors, recursive-alias rejection, and a fixed graph
  budget;
- direct construction and local DAG parsing regressions proving legacy Docker
  validation precedes its value-free deprecation warning, plus value-free
  ambiguity errors;
- implicit Docker Hub digest-pinned fallback on VM and Kubernetes, plus inline
  Docker credential rejection across direct, inherited, and sibling resource
  alternatives, all eight task-bearing bodies and routers, atomic local task
  mutation, final request-row persistence, restored explicit-direct v35+
  resources, YAML export, pickle encode/decode, and cluster-handle persistence,
  with a legacy Docker `image_id` round-trip compatibility proof;
- conservative validation and terminal classification of unprefixed legacy
  `image_id` values whenever cloud selection can still choose Kubernetes,
  including `infra: '*'` and `infra: '*/region'` through the same
  `InfraInfo` wildcard parser used by `Resources`, and every registered
  Kubernetes-derived cloud such as SSH node pools through registry class
  semantics, while an explicitly non-Kubernetes cloud image remains outside
  the container-image error boundary;
- separate raw-field credential-shape scanning from effective runtime
  classification: inherited non-container clouds retain valid provider VM
  image syntax such as Azure marketplace and community IDs, while unsafe raw
  values are rejected even if a child later overrides them and unconstrained
  leaves are still conservatively treated as Kubernetes-eligible;
- bounded unique OCI platform metadata and compressed size at callback, READY
  persistence, catalog decode, and response boundaries, with secret-bearing
  platform text neither persisted nor reflected;
- rejection of digest-only copy and external-adoption callbacks, rejection of
  direct READY completion with an empty platform set, OCI index descriptor and
  single-manifest config platform extraction, and no empty-platform match for a
  known runtime, plus root artifact-index rejection, exact descriptor-size
  verification, one common structural boundary for every root, config, layer,
  image-child, and non-image descriptor, and whole-index failure for malformed
  image children beside a valid child while structurally valid referrers remain
  ignored;
- external-profile register and publish returning the deterministic destination
  in `AWAITING_EXTERNAL_PUSH`, never issuing source credentials or copy I/O,
  exact-destination verification and retry transitions, byte-quota denial
  without false READY, and managed-profile register retaining import semantics;
- known AMD64/ARM64 placement filtering before provisioning, multi-platform
  coverage of every allowed placement architecture, fail-closed unbound
  Kubernetes/Nebius/generic placements, exact admin-binding acceptance, final
  cluster-commit architecture fencing, and architecture-aware ECR AWS CLI
  bootstrap, including GCP catalog architecture propagation and T2A
  preprovision plus durable-commit rejection;
- policy-authorized WARMING fallback with a known runtime and empty pre-READY
  metadata, while a nonempty incompatible platform set remains rejected;
- real-PostgreSQL persistence of compressed sizes at `2**31` and the maximum
  signed-64-bit value, plus migration metadata compiling the column as
  `BIGINT`;
- immutable artifact-wide platform and compressed-size evidence, with
  serialized real-PostgreSQL conflicting canonical completions proving exactly
  one winner and a failed loser, plus generation-scoped canonical dependency
  transitions in both directions;
- all prepare targets validated before source publication or any location
  intent, with an invalid later target leaving no catalog state;
- release immutability and multiple aliases for one digest;
- auth rotation preserving artifact/location identity;
- auth rotation refreshing a stopped pull plan on the same physical bytes,
  with stale direct cluster-handle persistence rolling back atomically;
- stopped-cluster YAML restoration preserving the freshly resolved VM and
  Kubernetes image reference while deleting an obsolete runtime login, with
  named-container matching that fails closed instead of overwriting sidecars;
- Kubernetes pod-config composition cannot replace the managed `ray-node` or
  HA initialization image while leaving catalog state pinned to other bytes;
- rejection of an independently changed auth strategy paired with an otherwise
  current location/policy snapshot, including through the status-refresh path;
- rejection of a changed runtime login instruction paired with an otherwise
  current managed plan, including through the status-refresh path;
- final cluster-commit rejection of a READY row whose physical destination
  fingerprint does not match its current normalized registry target;
- final cluster-commit rejection of a digest-matching target reference that is
  not the deterministic managed reference for the artifact and target;
- exact source-registry authority binding for WARMING login instructions;
- AWS ECR and GCP GAR adapter rejection of registry authorities outside the
  configured account, project, and region;
- exact Kubernetes context-to-registry-prefix authorization, including
  rejection of another account or project in the same region;
- Kubernetes binding-region normalization and provider-bounded admission,
  plus exact binding authority taking precedence over anonymous target access
  for AWS, GCP, Nebius, and generic registries;
- first-use Kubernetes target selection filters same-region targets by exact
  runtime pull capability before deterministic name ordering, including when
  a lexicographically earlier anonymous locality sibling is also accessible;
- same-region ECR-account or GAR-project ambiguity on VM placements fails
  before creating catalog intents, while exact Kubernetes bindings still
  select the authorized endpoint;
- profile rotation between INIT and READY preserves the already-rendered pull
  plan and its original durable reference;
- final cluster-commit validation of artifact, source-alias, and release
  selector bindings;
- canonical and regional READY references surviving an A-to-B canonical source
  rotation across different source repositories, with the existing regional
  route remaining selectable and successfully revalidated;
- two different digests sharing one immutable shard repository while configured
  digest-prefix bits select from the immutable power-of-two set of no more than
  256 workspace repositories; mandatory generation-qualified rendering, physical
  fingerprints, and uniqueness prevent two realm generations or shard policies
  from claiming the same repository prefix; and
  copy I/O writing through a deterministic immutable tag before digest-only
  verification;
- final cluster-commit acceptance of release and digest-equivalent source
  selectors after a failed canonical import rotates to a new immutable source
  binding, with prepare, retry, release, artifact, explicit-source, and
  active-B-lease ensures proving no source-less path reverts to source A;
- final cluster-commit rejection of release-only, artifact-only, mismatched,
  policy-disallowed, credential-bearing, and unresolved WARMING fallbacks;
- canonical endpoint normalization and complete provider-policy identity;
- monotonic profile generations, stale-replica rollback rejection, and
  active-lease revision fencing, including real PostgreSQL activation/claim
  interleavings;
- expired COPYING and EVICTING settlement after canonical loss, followed by
  successful profile repair in real PostgreSQL;
- exact-expiry READY verification fencing and malformed future COPY lease
  settlement in real PostgreSQL;
- absent and partially populated historical COPYING/EVICTING lease triples are
  visible to both list and indexed automatic claim paths and are repaired into
  complete fenced ownership;
- impossible READY ownership is rejected by the schema and repaired at the
  unchanged-generation exact-row boundary for historical corruption;
- dominant-profile activation does not rewrite untouched location rows, while
  old generations remain immediately unclaimable in PostgreSQL;
- exact regional-to-canonical revision binding across endpoint migrations;
- final exact-canonical fencing for standalone and cluster-handle references;
- real PostgreSQL overlaps in which those transactions hold the canonical row
  while waiting on a locked regional row, proving canonical loss serializes;
- field-tagged Serve placement identity across ref, release, artifact ID, and
  combined ref-plus-release selectors;
- SkyServe version snapshot convergence across different selectors for one
  artifact, plus rejection of mixed content, mixed managed/direct semantics,
  and missing-image candidates before durable version creation, with zero
  catalog writes for rejected first-use candidates, atomic same-digest
  multi-source publication, and whole-batch rollback proofs in real PostgreSQL,
  plus a forced crossed-release PostgreSQL overlap proving global
  phased lock order yields one success and one value conflict without a
  database deadlock;
- ownership and auth policy transfer reusing the same physical bytes;
- target-alias rename reusing the same physical row under a new revision;
- endpoint revision producing a distinct materialization;
- fail-closed managed-required dry runs and no optimizer mutation;
- ensure-on-use and WARMING source fallback;
- exact ECR, GAR, and Kubernetes source-fallback runtime identity, plus
  fail-closed off-provider private-source placement;
- restart-time upgrade from a WARMING source fallback to a READY managed route;
- locality and Kubernetes context capability;
- exact-digest adoption/copy and READY revalidation;
- immutable-tag-safe OCI recovery after committed-but-unverified copies and
  ambiguous copy failures, with exact-digest acceptance and mismatched-digest
  rejection;
- lease expiry, stale-token fencing, and idempotent recovery;
- claim-20 worker crashes remain excluded from automatic queues while an exact
  operator retry repairs COPY and EVICT uncertainty and resets the budget;
- lease heartbeat cancellation and non-synchronized retry timing;
- atomic one-row claims without contention being reported as failure;
- corrupt READY rows without a target reference excluded from verification at
  both listing and claim boundaries;
- bounded rotating profile and state-queue claims with no reconciliation or
  eviction eligible-queue `GROUP BY` or whole-queue sort, including
  simultaneous PostgreSQL claims from one profile while every worker holds the
  shared generation fence;
- partial reconciliation and eviction index predicates exclude exhausted
  automatic attempts, including zero-eligible PostgreSQL proofs;
- disjoint fresh and deferred verification/eviction queues, state-specific
  active-lease probes, PostgreSQL partial-index plans, and bounded
  `EXPLAIN ANALYZE` behavior for activation plus all list/claim paths;
- large-profile PostgreSQL keyset page-advance liveness for reconciliation
  listing and claiming;
- seek-forward PostgreSQL eviction collisions in which eight workers initially
  route to one oldest row and still claim eight distinct locations;
- one-million-row PostgreSQL 16 `EXPLAIN ANALYZE` and end-to-end claim
  benchmarks for reconciliation and eviction, including zero-eligible and
  late-eligible queues whose due regional rows have non-READY canonical
  dependencies;
- complete multi-profile reselection when earlier profiles are fully locked;
- constant-query unfiltered status association loading;
- fresh per-item claim and retry timestamps across long reconciliation batches;
- durable cluster references across stop and termination;
- locationless WARMING source-fallback references across launch, stop,
  restart-to-managed replacement, tombstone rejection, and termination, with
  cluster handle plus artifact/source reference committed atomically;
- generic-reference rejection of cluster consumers reserved for the atomic
  cluster-handle transaction, for both acquire and release;
- metadata-only cluster-handle updates reject any image execution-state change
  that would bypass the atomic cluster/reference transaction;
- no-op cluster refresh during location repair and verification fencing for
  new references;
- same-handle real relaunch revalidating a location that became MISSING;
- READY continuation preserving the INIT plan after a policy revision activates
  during provisioning, followed by current-revision resolution on restart;
- exact catalog UUID enforcement for Serve and managed-jobs controllers;
- reference-aware regional eviction and canonical protection;
- provider ownership proof before any destructive registry callback;
- endpoint, namespace, account, manager-identity, and credential-reference
  rotation retaining historical target custody until old locations and
  DELETE_UNKNOWN outcomes drain, including purge through that old authority;
- profile-head activation immediately excluding old-revision facets without
  population rewrites, plus projection drift detection/repair and authority-path
  independence;
- the complete batch-publication, finalizer, release-name, tombstone, and future
  builder 14-phase lock graph, including both quota rows, daily usage, context
  uploads/objects/cache, attempts/output/staging/logs, running as real PostgreSQL
  overlaps without ABBA deadlock;
- one limiter row with multiple independent permit leases, per-lease expiry and
  fencing, a closed mapping for every data-plane ECR auth, metadata, repository,
  layer, manifest, verify, and delete call, shared throttle penalty, and
  external-account traffic explicitly outside the guarantee;
- admin-only tombstone rejection with live references, leases, or publications;
  selector invisibility after commit; deterministic regional-before-canonical
  purge; the exact `AWAITING_EXTERNAL_PUSH|VERIFYING ->
  EXTERNAL_PURGE_PENDING -> EXTERNAL_PURGED` acknowledgement
  schema, ordering, idempotence, correction audit, and managed/external state
  constraints; exact ownership and digest fences; DELETE_UNKNOWN and
  preexisting MISSING absence inspection; retry; build-output lease exclusion
  plus narrowly qualified `BUILD_OUTPUT_ABANDONED`; audit retention; quota
  release only after proved deletion and at PURGED; and absence of delete
  actions from the dashboard;
- secret-free serialization across API and launch state;
- closed diagnostic and source-fallback codes rejecting free-form secrets;
- conservative redaction when `workspaces` or a workspace entry is malformed,
  before generic schema validation can reflect the rejected value;
- conservative whole-subtree redaction and value-free rejection when a
  workspace mapping key is not a bounded valid workspace name, including
  credential-shaped, non-string, and overlong keys;
- database-backed server config passing the same duplicate-key, schema, and
  semantic admission boundary as file-backed config, with value-free failures;
- raw and typed config updates racing through one generation compare-and-swap,
  atomic config-YAML/profile-head/custody/realm commit, lost-response
  idempotency, crashes before and after commit, NOTIFY loss, stale replica
  fail-closed behavior, and eventual poll-based convergence without a
  representable config/head split;
- status selectors and workspaces validated before an SDK creates HTTP query
  parameters, so client HTTP errors cannot retain rejected values;
- server-owned workspace, registry, database, daemon, and controller policy
  removed before a request body is durably persisted, while preserving the
  existing client-side override compatibility contract;
- server-managed `_resolved_container_image` state rejected at the root and in
  every `any_of` or `ordered` candidate before task schema validation;
- every lease claim, heartbeat, completion, failure, verification, retry, and
  eviction transition reading wall time only after its complete row-lock set,
  with stale-token proofs for all transition classes;
- PostgreSQL publication, preparation, and location creation converging under
  concurrent identical publications;
- PostgreSQL FAILED and MISSING retry probes using the intended partial queue
  index, proven with the actual SQLAlchemy query and `EXPLAIN`;
- reference-heavy eviction discovery bounded to fixed keyset pages, eventually
  reaching later unreferenced work, and retaining an active-reference fence in
  the final locked claim statement;
- eviction keyset pages completing one bounded cyclic pass when a persisted
  cursor starts inside the due set, so a suffix candidate that loses its final
  fence cannot hide an eligible prefix candidate until a later worker poll;
- restart resolution and final durable cluster commit rechecking the current
  workspace locality policy, while the exact persisted INIT-to-UP continuation
  remains the only policy-rotation exception;
- node-scoped behavior for multi-GPU resources.

Builder coverage and release gates are owned by
[`managed-container-image-builder.md`](managed-container-image-builder.md).
Distribution tests consume only its verified READY artifact contract.

Dashboard and catalog coverage additionally proves:

- keyset pagination has no duplicates or omissions across equal timestamps and
  rejects malformed/version-mismatched cursors without reflecting input;
- workspace, selector, lifecycle, distribution, target, state, canonical, and
  limit validation occurs before request persistence; state, target, and
  canonical without distribution are rejected, legacy unfiltered status fails
  rather than truncates beyond 1,000 artifacts, and every accepted query remains
  bounded;
- catalog pages batch-load associations in constant query count and exercise
  indexed artifact/facet/summary plans at million-artifact scale; every
  location/lifecycle mutation updates its projection transactionally, profile
  head changes take effect through revision joins without facet rewrites, and
  injected projection drift is detected, ignored by authority paths, and
  converged by bounded repair;
- catalog pages return bounded location aggregates while current and historical
  detail locations use duplicate-free keyset pagination under revision churn;
- profile summaries omit manager identities, credential references, and raw
  configuration while preserving useful topology;
- stale workspace responses cannot overwrite a newer selection;
- publish, prepare, and retry dialogs handle pending/success/failure and prevent
  duplicate submissions;
- the admin Image distribution editor round-trips every profile/binding/policy
  field, validates and previews a generation-fenced diff, never exposes secret
  values, and reports infrastructure/capability states; and
- navigation, responsive rendering, keyboard focus, empty/error/permission,
  and old-server states pass Jest and a production Next.js build.

Terraform validation additionally runs `terraform fmt -check`, `terraform
validate`, static least-privilege assertions, and plans for single-region,
multi-region, and existing-ECR configurations. Plans must prove no image-content
fan-out, no account-wide administrative grants, no repository deletion
permission, no ordinary-worker canonical delete permission, and no
placement-time resource dependency. Boundary plans cover each account/region's
applied repository and image quotas, existing repositories and manifests,
safety headroom, worst-case multi-platform manifest units, the feasible shard
interval at or below 256, audited inventory input, durable workspace slot,
capacity exhaustion, generation-qualified prefix rendering and collision
rejection, namespace-generation expansion, and provider-quota drift error
mapping. Tests also prove v1 rejects native
replication configuration instead of creating untracked regional content.

Concurrency tests run multiple copy-worker replicas against one account/region.
They prove per-operation limiter/lease bounds, independent crash expiry,
throttle penalties, fair progress across operation classes, and that autoscaling
pods cannot multiply repository, layer, manifest, verify, or delete authority.
The later trusted builder publisher must pass the same suite before activation.

Release gates additionally require PostgreSQL migration tests, AWS provider
integration and negative-IAM tests, worker/API restart tests, external-profile
pull tests, and lifecycle recovery. Measured 100, 500, and 1,000-replica
cold-start evidence is a managed-profile production-activation gate, not a
source-code merge or disabled-feature release gate; until capacity is available,
the managed profile remains disabled and no performance claim is made.

## Explicit non-goals

- a general artifact store for model weights or arbitrary files;
- lazy container filesystems or memory snapshots;
- organization/account creation, organization policy, or cross-account trust
  bootstrap outside the supplied least-privilege modules;
- automatic repository or canonical deletion outside the exact
  abandoned-build lifecycle rule;
- copying all artifacts to all regions;
- transparent mid-pull registry failover;
- treating R2 buckets as OCI registries;
- per-GPU process orchestration inside registry distribution;
- automatic discovery of a VM's eventual registry account/project identity;
- a mutable named-image channel without a shared generationed deployment
  snapshot across clusters, managed jobs, and SkyServe;
- claiming Modal-style lazy filesystem startup without an explicitly detected
  compatible VM or Kubernetes node snapshotter.

## Review protocol

This design and implementation are being challenged independently by Codex
5.6 and Claude Fable. A review round is valid only when the reviewer inspects
the current plan, code, diff, and tests and returns one of `PURSUE`, `RESHAPE`,
or `DROP` with evidence. Material implementation or architecture changes reset
both consecutive-PURSUE streaks. Completion requires at least six paired
rounds and three consecutive PURSUE verdicts from each reviewer against the
same materially unchanged patch.

Before those acceptance rounds, one discovery sweep audits the whole patch by
invariant rather than stopping at the first blocker. Every discovery reviewer
must enumerate all reproducible findings across its assigned matrix, including
nearby instances of the same defect class. The combined findings are
deduplicated and fixed as one batch, with table-driven or cross-boundary tests
where an invariant is shared. Only after that batch passes the complete focused
suite is the implementation hash frozen and the six paired acceptance rounds
started. A rejected acceptance snapshot is handled the same way: all active
reviews are allowed to finish their exhaustive inventories, their findings are
batched, and a new frozen snapshot starts a fresh six-pair gate. A first finding
is never used as a reason to stop inspecting the rest of the rejected snapshot.

Before productization code begins, both reviewers also challenge this exact
canonical design and the linked builder design for product worth, Modal
comparison, component boundaries,
API compatibility, million-artifact behavior, multi-cloud portability,
security, operability, and rollout. Those design rounds are discovery rounds
and do not count toward the six fresh exact-head acceptance pairs. Any accepted
design correction is made here before implementation. The final acceptance
brief includes the exact Git tree hash, canonical design path, complete diff,
test evidence, dashboard build evidence, and remaining explicitly operational
gates; reviewers may not assume unimplemented infrastructure is production
ready.
