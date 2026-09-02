# SkyServe centralized placement catalog

_Status: implementation and local verification complete; pull request and
production deployment pending. Created: 2026-07-24. Last updated: 2026-08-28._

## Decision summary

SkyServe will materialize one complete placement catalog for each immutable
service version and store it on that version's PostgreSQL row. A catalog
contains every exact `Location` candidate and its nominal hourly cost. The
service parent, controller child, replica manager, autoscaler, reserved-capacity
poller, paid-capacity admission, and update preflight all consume that same
versioned record.

Catalog construction is allowed only before a new version is committed or
while lazily backfilling a legacy committed version whose catalog is absent.
It is not allowed in controller request handling, autoscaler decisions,
placement selection, load-balancer synchronization, or background capacity
polling. Existing versions are backfilled with a compare-and-set update so
concurrent recovery cannot replace a catalog after another owner publishes it.

This supersedes the partial in-memory cost-cache contract described by
`docs/designs/skyserve-accelerator-compatibility.md`. The startup-liveness
requirement in that design remains: provider feasibility and pricing work
cannot delay the controller child from binding its health endpoint. For a
legacy version, its service parent completes and persists the one-time backfill
before spawning the supervised child.

## Context and problem

The placement policy already shares one `SpotPlacer` object among a running
controller's replica manager, autoscaler, and capacity helpers. That object is
not durable, however:

- API validation constructs and discards a placer.
- Fresh controller startup and every controller recovery enumerate candidates
  again.
- Update preflight constructs a candidate placer and `update_version`
  immediately constructs the same placer a second time.
- Only Kubernetes costs are initially present. Calls named `cost_per_hour()` or
  `zero_cost_locations()` can discover a cache miss and rerun feasibility and
  pricing across an increasingly large candidate set.

Exact instance-shape expansion grew the production L4 fleet from 659 to more
than 1,050 candidates. Repeating synchronous feasibility work made controller
readiness exceed the supervisor's 420-second registration timeout. The bounded
startup fix removed provider resolution from critical sections but deliberately
left a partial cache and repeated construction in place. This design removes
that remaining source of unbounded work.

## Goals

- Construct placement candidates and nominal costs once per immutable service
  version.
- Make every runtime cost and zero-cost lookup an in-memory read over a complete
  catalog, with no fallback provider resolution.
- Persist the catalog in the central PostgreSQL Serve database and recover it
  across controller/API pod restarts.
- Backfill existing service versions safely and idempotently.
- Preserve exact location identity, heterogeneous `any_of` resource fields,
  per-GPU instance-shape expansion, preemption benches, paid-capacity pool
  identity, and placement order.
- Keep fresh-version validation deterministic before publication: a failed
  catalog build must not create an elected version that cannot be applied.

## Non-goals

- A global cross-service cloud-price cache or a new replacement for SkyPilot's
  provider catalogs.
- Live spot-market bidding, availability prediction, or periodic price refresh.
- Changing user-facing YAML, placement policy selection, or autoscaling
  semantics.
- Persisting transient preemption/bench state in the placement catalog.
- Supporting the central Serve catalog on SQLite. The API-server database is
  PostgreSQL-only; existing local-controller SQLite behavior remains legacy
  compatibility, not a design target.

## Durable and runtime contract

`version_specs.placement_catalog` is a nullable JSON document stored as JSONB
on PostgreSQL. SQLAlchemy uses `none_as_null=True` so an absent legacy catalog
is SQL `NULL`, distinct from JSON `null` cost values inside a catalog. Schema
version 1 has the following logical form:

```json
{
  "schema_version": 1,
  "entries": [
    {
      "location": {
        "cloud": "AWS",
        "region": "us-east-1",
        "zone": "us-east-1a",
        "accelerators": {"L4": 1},
        "use_spot": true,
        "image_id": [
          {
            "region": null,
            "image": "docker:registry.example/model:v1"
          }
        ],
        "container_image": null,
        "disk_tier": null,
        "ephemeral_storage": null,
        "instance_type": "g6.xlarge"
      },
      "hourly_cost": 0.212
    }
  ]
}
```

An unavailable nominal price is encoded as JSON `null` and interpreted as
positive infinity. JSON `NaN` and `Infinity` are never written to PostgreSQL.
Entries are deterministically ordered by the existing `Location.sort_key()`.
`image_id` is encoded as a list of region/image records because JSON object
keys cannot preserve SkyPilot's region-independent `None` key. Candidate
enumeration deduplicates exact locations before their one cost is materialized.
Kubernetes entries are always zero-cost, as in the existing SkyServe policy.

The catalog is immutable once non-null. Its identity is the containing
`(service_name, version)` row, whose YAML and pickled service specification are
already immutable. A new service update creates a new catalog, even when its
resources happen to match the preceding version. This bounds staleness to the
service version and avoids unsafe cross-version fingerprint inference.

Provider eligibility is part of construction, not a runtime fallback. For AWS,
a VM offering that relies on SkyPilot's catalog default is eligible only when
the exact region also has the default image tag selected for that instance
type. This includes the x86 CUDA 13 image for L4 instances. A VM-catalog row
without a syntactically valid AMI ID in that image-catalog row is not launchable
and must not become a durable location. An explicitly configured cloud image
bypasses the SkyPilot-default-image check and retains its existing regional
validation contract. A container image does not bypass the check because its
VM still needs a base image. The `network_tier: best` EFA path is also exempt:
it live-discovers an AWS-owned EFA image before falling back to the catalog
default, so this change preserves its existing launch-time contract.

An absent or placeholder image causes at most one forced image-catalog refresh
per API request. This lets a service version constructed immediately after
publication observe a new regional AMI without waiting for the ordinary
seven-hour cache interval or downloading the catalog once per missing region.

A syntactically valid image row is local eligibility evidence, not proof of the
AMI's live state. The hosted-catalog release process separately owns image
qualification: before publication, an operator must prove the AMI is public,
available, architecture-compatible, and launchable cross-account. The current
CSV schema does not encode those attestations and catalog construction performs
no live AWS call. Those checks are explicit publication gates below. Publishing
a new AWS VM region therefore cannot make standard regionless Serve placement
select that region before its required default image row is published. The
region becomes eligible only in a subsequently constructed service-version
catalog.

At runtime `SpotPlacer.location2cost` is complete for every key in
`location2status`. `cost_per_hour()` returns the materialized value or infinity
for an unknown location. `zero_cost_locations()` filters the materialized
catalog only. The former partial-cache accessors and fallback feasibility
resolver are removed, so no runtime caller can accidentally recreate the
previous cache-miss behavior.

## Construction and ownership flow

### Fresh service

1. The service parent loads the final policy-mutated task YAML.
2. It validates the placer resource-shape contract without enumerating
   providers.
3. It constructs the complete catalog.
4. `add_service()` atomically inserts the service, initial immutable version,
   and catalog.
5. The controller child loads the catalog from PostgreSQL and does no provider
   feasibility or pricing work during construction.

### Update

1. The controller parses the candidate immutable YAML and constructs the
   complete catalog before calling `add_or_update_version()`.
2. The version YAML, pickled spec, election pointer, and catalog commit in the
   same PostgreSQL transaction.
3. Update preflight loads and validates that catalog, returning the constructed
   placer to `update_version()` so the same runtime object is published rather
   than rebuilt.
4. An idempotent retry uses the already committed spec and catalog bytes.

### Legacy recovery

1. The service parent loads the elected committed version before spawning the
   child.
2. If its catalog is null, the parent builds one complete catalog and performs
   `UPDATE ... SET placement_catalog = :catalog WHERE placement_catalog IS
   NULL`.
3. It rereads the winning catalog if another owner won the compare-and-set.
4. Only then does it spawn the supervised controller child.

A pre-version service with no committed immutable YAML cannot safely own a
versioned catalog and fails recovery explicitly. Such rows predate the
supported PostgreSQL Serve state contract; operators must recreate the
service rather than let a new child rebuild a non-durable catalog.

An invalid or unsupported stored schema version fails explicitly. It is not
silently reinterpreted or replaced because that would let rolling binaries
disagree about one version's placement identity.

## Architecture invariants

1. A committed new version with a configured spot placer has a non-null,
   complete catalog in the same transaction.
2. Every catalog location has exactly one materialized cost value, including
   infinity for an unavailable price.
3. Runtime code never calls cloud feasibility or resource pricing because a
   catalog key is missing.
4. The controller child never builds or backfills a catalog during health
   endpoint startup.
5. Preemption status and timestamps remain runtime state keyed by immutable
   catalog locations; updates inherit benches only for exactly equal locations.
6. A catalog does not grant capacity or prove availability. Provider launch
   results, reserved-capacity observations, and paid-capacity admission remain
   separate authorities.
7. Every AWS location that relies on a SkyPilot default image has a valid
   AMI-shaped image-catalog mapping for the exact region and
   instance-type-selected tag at construction time.
8. AWS EFA placement and explicit cloud images retain their pre-existing image
   selection and validation paths; neither is rejected for lacking the
   SkyPilot catalog default.
9. Rollback to an older binary remains possible because the new column is
   nullable and ignored by older code. The migration downgrade is intentionally
   non-destructive.

## Implementation phases

1. Add Serve database revision 028 and the nullable JSONB-backed
   `placement_catalog` column.
2. Add a versioned `PlacementCatalog` value type, deterministic serialization,
   complete cost materialization, and pure runtime lookup semantics.
3. Build and commit catalogs in fresh-service and update flows; lazily backfill
   legacy versions before child spawn.
4. Route replica manager/update preflight through the persisted catalog and
   remove child-side backfill and production references to partial-cache APIs.
5. Add state, serialization, no-provider-runtime, restart-reuse, and
   single-build update tests.

## Deployment and rollback

Deploy the API/controller image with `helm upgrade --reuse-values`. Migration
028 runs before controllers access the new column. Existing services incur one
catalog build on their next parent recovery; the catalog is then durable across
subsequent restarts. New services and versions commit a catalog immediately.

Roll back the Helm release to the preceding image if controller health,
placement, or migration verification fails. The nullable column is additive,
older binaries ignore it, and catalog data does not alter the immutable YAML or
replica rows. Do not drop the column during emergency rollback.

## Verification plan

- Round-trip catalogs containing cloud and Kubernetes locations, container
  images, exact instance types, and unavailable prices.
- Prove every enumerated location receives a cost and runtime cost/zero-cost
  methods do not invoke feasibility or pricing.
- Prove `add_service()` and `add_or_update_version()` persist catalogs and
  identical retries preserve the original bytes.
- Prove compare-and-set backfill cannot overwrite an existing catalog.
- Prove update preflight returns one persisted placer and `update_version()`
  publishes it without a second `SpotPlacer.from_task()` call.
- Prove regionless AWS construction excludes a real VM offering when the exact
  default image tag is absent or a placeholder in that region, retains it when
  present for a container-backed VM, and does not apply the default-image check
  to an explicit cloud image, EFA placement, or provisioning after selection.
- Prove a missing regional image forces only one refresh per request and a
  newly published AMI can participate immediately after that refresh.
- Run focused Serve state, placer, controller, replica-manager, paid-capacity,
  reserved-capacity, and concurrency autoscaler tests.
- After deployment, verify migration revision 028, non-null catalog for the
  production service version, controller/LB health, stable controller owner
  identity, successful LB sync, and continuity of existing ready replicas.
- Restart or respawn one canary controller and verify logs contain a persisted
  catalog load but no candidate enumeration before the health endpoint binds.

## Verification evidence

The implementation passed:

- `bash format.sh --files ...`: YAPF, isort, mypy across 745 source files,
  pylint at 10.00/10, and dashboard ESLint/Prettier.
- Focused catalog, state, parent recovery, controller update-handoff,
  replica-manager, autoscaler, reserved-capacity, paid-capacity, preemption,
  cheapest-first, and migration-utils unit suites.
- The AWS provider, image-catalog, catalog-source, and hybrid-placement suites,
  including hermetic regionless default-image qualification (`152 passed`).
- The regionless test also passed with an empty temporary runtime directory and
  an intentionally unreachable hosted-catalog URL, creating no catalog files.
- The generic catalog, catalog-source-override, and resource suites
  (`130 passed`).
- A combined regression run of the 13 affected unit-test modules with
  `pytest -q -n 0 ... -x`.

PostgreSQL migration and production restart evidence remain deployment gates
because no disposable PostgreSQL test URI is available in this workspace.

## Adversarial review

The initial 2026-07-24 review was run against the placement-catalog
implementation. It found and corrected four contract gaps:

1. region-independent image IDs use a Python `None` map key, which generic
   JSON object encoding would corrupt into the string `"null"`; schema v1 now
   encodes image IDs as region/image records;
2. SQLAlchemy JSON `None` could become JSON `null`, breaking the SQL `IS NULL`
   compare-and-set; the column now uses `none_as_null=True`;
3. controller-child fallback construction violated the no-provider-startup
   invariant; child-side backfill and all partial-cache APIs are removed; and
4. a pre-version legacy service cannot own an immutable version catalog and
   now fails explicitly instead of running a non-durable compatibility path.

The 2026-08-28 follow-up review covered AWS regional image eligibility. It
found and corrected four additional gaps:

1. EFA uses a live-discovered AWS image and is now explicitly exempt from the
   catalog-default filter;
2. a miss now forces one shared refresh per request rather than remaining stale
   for seven hours or downloading once per missing region;
3. generator placeholders and malformed IDs are rejected instead of counting
   as image availability; and
4. the regionless container-backed test installs all catalog mocks before task
   parsing, so it is hermetic and cannot download hosted catalogs.

The 2026-09-02 post-merge audit of that change found and corrected two
gaps:

1. the forced-refresh frame had been bound to the module-level image catalog,
   so after one miss every later API request re-downloaded the image catalog
   on its first lookup, hit or miss; the module catalog now keeps its regular
   pull cadence and only the miss path consults the per-request refresh; and
2. `is_image_tag_valid` had been routed through the single-image lookup, which
   asserts on the several regional rows of a region-agnostic tag; a
   region-agnostic `skypilot:` tag is valid again when any region has an AMI.

No unresolved source/design divergence remains. Live AMI qualification and
hosted publication remain explicit operational gates.

## Open gates

- Pull-request review and CI.
- Production deployment and post-deploy stability observation.
- Before activating a newly published AWS region, attest that its default AMI
  is public, available, architecture-compatible, and launchable from a separate
  account.
- Verify the generated VM catalog contains real G6/L4 Spot rows with non-null
  prices and that the GitHub and S3 v8 catalog objects converge byte-for-byte.
