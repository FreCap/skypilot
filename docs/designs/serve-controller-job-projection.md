# Persisted SkyServe Controller and Worker Placement Projection

**Status:** Protocol v9 and capability cohort 9 are deployed homogeneously in
SkyPilot `1.1.1470`. Production then exposed one remaining cache-admissibility
bug: Boltz workers on one node deliberately share one locked, content-addressed
service cache, but the generic v8/v9 init script requires a fresh
`required_bytes_per_replica * max_replicas_per_node` envelope before *each*
worker starts. On the observed East node this demanded 4.5 TB although that
shared cache needs a 1.0 TB cold-start-plus-reserve envelope. Protocol v10 is
the sole next-write fix. It adds an explicit, digest-bound `budget_scope` to
the server-owned attestation instead of silently treating every generic cache
as shared. `replica_additive` retains the full-packing rule;
`node_shared_immutable` checks one shared envelope and is valid only when the
operator attests application-level locking and immutable publication.
Protocols v8 and v9 retain their distinct released renderer/init-script bytes
exactly and remain implicitly `replica_additive` historical adoption,
inspection, settlement, and cleanup readers. PostgreSQL remains authoritative;
Helm storage remains disabled; no
Kueue or platform-infrastructure change is part of this correction. The same
successor image also carries the source-under-review bounded-memory correction
required before fleet teardown: exact-name, streaming, single-flight
Kubernetes presence reads and one reusable readiness executor replace repeated
full-Pod materialization and per-tick thread-pool construction. Neither source
correction is deployed or production-proven yet.
**Last updated:** 2026-08-24

## Goals

- Freeze one server-owned Kubernetes controller-home identity for each
  committed SkyServe version independently from the version's worker routes.
- Freeze every eligible Kubernetes worker candidate's context, namespace,
  service account, priority class, accelerator shape, and disposable-cache
  contract, plus its provisioning timeout, so east and PHX can coexist in one
  heterogeneous service.
- Freeze each worker's Kueue LocalQueue and WorkloadPriorityClass in the same
  candidate and expose one canonical digest for downstream claim and launch
  fences; mutable task/config input cannot select a second admission path.
- Make protocol-v10 projection the sole new-write scheduler, binding, cache,
  scratch, bootstrap, readiness, and provisioning-wait path. Projected Pods
  use the exact
  server-owned scheduler and timeout frozen in the candidate, cannot carry a
  direct `nodeName`, and cannot be reported ready until the freshly bound Node
  has the exact projected accelerator label.
- Give each new worker an explicit server-owned scratch contract: either no
  owned `/tmp`, or one bounded memory-backed `/tmp` with a fixed volume and
  mount identity. Task YAML and campaign configuration cannot select or mutate
  this contract. Its node-local cache bootstrap reserves one shared cache
  envelope per service leaf rather than multiplying the same leaf by the
  number of logical GPU workers packed on the node, but only when the frozen
  cache attestation explicitly selects `node_shared_immutable`. Generic
  `replica_additive` caches retain the maximum-packing envelope.
- When the current projection selects memory-backed `/tmp`, move SkyPilot's
  runtime, uv cache, and uv-managed Python writes into that bounded filesystem
  for both the base bootstrap and every later fresh `kubectl exec`. Keep the
  small uv executable at its existing image/user location; this change does
  not add a persistent volume or a second bootstrap path.
- Expose authenticated, versioned metadata that a campaign launcher can consume
  without copying arbitrary `pod_config`, credentials, PVCs, or host paths.
- Allow fresh campaigns to launch while explicitly deferring automated
  external campaign-controller recovery and replay until its authority is
  designed and proven end-to-end.

## Non-goals

- This contract does not persist campaign inputs or outputs. Canonical `s3://`
  identities and immutable manifests remain durable truth. Workers access
  their approved prefixes through the projected server-owned workload
  identity; transfer estimates remain operator planning notes outside this
  schema.
- It does not make node-local cache durable, shared between regions, or safe by
  naming a path. A cache is advertised only with a complete platform
  attestation and must still be verified by the admitted worker.
- It does not create a service per campaign or source digest. One independently
  runnable model version keeps one stable service and uses SkyServe YAML
  versions for updates.
- It does not backfill historical service versions from mutable configuration or
  live pods.

## Public contract

The admin-only service-version-history response advertises
`placement_projection_protocol_version: 10`. Each version entry adds two
nullable placement fields plus a nullable controller-cache field. A consumer
must require the protocol field and strictly validate every non-null
projection; API revision alone is not capability evidence.

API 75 introduced these retained placement fields. API 76 removes the
abandoned storage-broker configuration and version-history field; it adds no
replacement transport or compatibility branch. API 77 advances the retained
worker placement projection to protocol v2 with immutable Kueue admission. API
78 adds preemptible-attribution reporting without changing this projection.
API 79 advances all new worker projection writes to protocol v3 with typed
server-owned scratch and a frozen server-owned provisioning timeout. Protocol
v4 keeps the same closed payload shape and adds UID-bound base-runtime
readiness. Two released implementations then wrote the same protocol-v5
discriminator while rendering different bootstrap environments. Their cohort
4/5 fence keeps those historical effects distinguishable for settlement, but
the projection alone is not a truthful replay discriminator. Protocol v6
uniquely binds the final three bootstrap write roots below to memory-backed
scratch, and protocol v7 repairs apt/dpkg supervision. Protocol v8 adds the
attested node-local cache-leaf bootstrap described below; protocol v9
normalizes a safe released cache leaf. Protocol v10 adds an explicit cache
budget-scope discriminator and corrects free-space admission only for an
attested shared-immutable leaf. New effects require exact
protocol v10 and capability cohort 10; v8/v9 remain exact historical
read/inspect/adopt/settle/teardown contracts, and v1-v7 remain read/inspect/
settle/teardown only. The discriminator, not a mutable API-server setting,
selects semantics.

`controller_job_identity` is either null or exactly:

```json
{
  "workspace": "rescluster-k8s-prod-east1",
  "kubernetes_context": "prod_research_cluster_eks",
  "namespace": "rescluster-k8s-prod-east1-controller",
  "service_account_name": "skyserve-controller-sa",
  "priority_class_name": "rescluster-k8s-prod-east1-controller",
  "lb_data_plane_auth": {
    "secret_name": "skypilot-serve-lb-data-plane-auth",
    "secret_key": "tokens",
    "mount_path": "/etc/skypilot/serve-auth/lb-data-plane/tokens"
  }
}
```

The first four fields are non-empty strings. `priority_class_name` is a
non-empty string or null. The service workspace selects an explicit
server-owned `serve_controller_workspace`; the context and remaining identity
are then resolved inside that target workspace. They are not inferred from an
inference worker context or identity. Controller priority is resolved only
from the target workspace/context's typed server-owned
`serve_controller_priority_class_name`; it is never inferred from
`pod_config`. An absent property projects null.

`lb_data_plane_auth` is a server-owned Secret reference, never Secret bytes.
Its name and key come from the target controller context's
`serve_controller_lb_data_plane_auth`; the mount path is fixed. The trusted Clin
consumer must mount only that key, read-only, non-optional, mode `0400`, and
point `SKYPILOT_SERVE_LB_AUTH_TOKENS_FILE` at the mounted file. The controller
must read the newline-delimited ring afresh for each request and present its
first token, so overlap-token rotation can update a running Job through
Kubernetes projected Secret refresh. It must never receive
`SKYSERVE_BEARER_TOKEN`, copy the token into version history/YAML, or refer to a
campaign Secret. Clin rendering and admission verification are rollout gates;
this API change persists and exposes the projection but does not create that
Job. Replacing bearer auth with service-account JWT validation or mTLS is a
separate LB authentication design.

`worker_placement_identities` is null for a legacy or unprojectable version.
Otherwise it is a non-empty array with one record for every eligible
Kubernetes entry in the immutable placement catalog:

```json
{
  "projection_version": 10,
  "candidate_id": "kubernetes-0002",
  "kubernetes_context": "phx_research_cluster_eks",
  "namespace": "rescluster-k8s-phx",
  "service_account_name": "skypilot-pool-sa",
  "scheduler_name": "default-scheduler",
  "priority_class_name": "rescluster-k8s-prod-east1-preemptible-inference-low",
  "priority_value": -1000,
  "preemption_policy": "Never",
  "kueue_admission": {
    "local_queue_name": "be",
    "workload_priority_class_name": "be-lt"
  },
  "provision_timeout": 15,
  "pod_identity_role_arn": null,
  "accelerator_name": "H200",
  "accelerator_count": 1,
  "accelerator_scheduling": {
    "label_key": "nvidia.com/gpu.product",
    "label_values": ["NVIDIA-H200"],
    "resource_key": "nvidia.com/gpu"
  },
  "cache": {"kind": "none"},
  "scratch": {
    "kind": "memory",
    "mount_path": "/tmp",
    "volume_name": "skypilot-serve-worker-tmp",
    "size_limit_bytes": 21474836480
  }
}
```

`projection_version` is exactly `10` for every new write. `kueue_admission` is
either null or
exactly the two non-empty strings shown above. A non-null value means Kueue
management is required; `require_managed` is derived and is not persisted as a
second independently mutable boolean. The LocalQueue comes from the effective
server/workspace `kubernetes.kueue.local_queue_name` or
`kubernetes.quota.queue`; the WorkloadPriorityClass comes only from
`serve_worker_kueue_workload_priority_class_name`. They must be both present or
both absent. The complete validated candidate is encoded as canonical sorted
JSON and SHA-256 hashed; `worker_projection_sha256()` is the sole digest helper
used by reserved-fill claims, allocations, replica provenance, and launch
fences.

For the current PHX context, that existing contract is LocalQueue `be`, its
observed ClusterQueue output `skypilot-be`, WorkloadPriorityClass `be-lt`, and
the independently projected Pod PriorityClass at numeric value `-1000` with
`preemptionPolicy: Never`. The candidate persists `be` and `be-lt`; the
admission receipt proves the `be` -> `skypilot-be` output. SkyPilot verifies
these objects from Simone's configuration and creates or mutates none of them.
East keeps `kueue_admission: null` and its existing non-Kueue scheduler path.

`provision_timeout` is a non-boolean integer that is either `-1` (wait
indefinitely) or non-negative. The builder freezes the effective
server/workspace/context `kubernetes.provision_timeout`. When it is absent, the
builder freezes the existing Kubernetes default once, using the same replica
node count, volume state, and DWS mode that determine ordinary scheduler
placement. Kueue does not inflate this projected value: its former 24-hour
queue-aware default is now the separate admission-phase bound. Task/resource
overrides are rejected recursively. A protocol-v3-v10 terminal launch consumes
only this signed value and never consults the API server's ambient config or
launch-time cluster overrides. For required Kueue, a separate provider-owned
24-hour admission watcher first binds the exact Pod UID and admitted queue
identity. Its gated observations retain no service/fleet advisory authority or
process provider phase; after observing admission, one fresh short effect epoch
revalidates the exact association, Pod UID, and queue outputs before this
projected timeout starts as a fresh scheduler-binding clock. Non-Kueue launches
retain the existing single scheduling clock. This timeout is not a capacity or
reclaim classifier:
`NO_CAPACITY`, queue waiting, scheduling, and initialization remain typed
observer/provider states, and a finite timeout is never proof that committed
capacity disappeared. If the 24-hour watcher expires for protocol-v2 reserved
fill, it raises the wire-safe typed `ReservedFillProviderPresentError` with the
exact observed Pod names and UIDs. It neither reacquires request-owned deletion
authority nor classifies the provider as `NOT_STARTED` or `ABSENT`; the durable
association stays pinned for the #1608 `PRESENT` -> UID-fenced down -> fresh
`ABSENT` adjudication path. Generic required-Kueue launches keep the ordinary
Kubernetes timeout error.

`scheduler_name` is a non-empty string frozen from the effective server-owned
context/workspace `pod_config.spec.schedulerName`, defaulting to
`default-scheduler`; request-scoped Pod configuration cannot populate it. The
typed reclaim-policy view includes the same field, so deployment authorization
and provider attestation agree on one scheduler identity.

`candidate_id` is opaque. It is `kubernetes-%04d`, where the integer is the
entry's index in the persisted placement catalog's deterministic sorted order.
Filtering non-Kubernetes candidates does not renumber it. Consumers correlate
the ID with the other record fields and never derive scientific routing from
the ID. A non-null priority class requires a Kubernetes-range integer
`priority_value` and a `preemption_policy` of `Never` or
`PreemptLowerPriority`; when the class is null, both fields are null.
`accelerator_scheduling` freezes the exact server-owned Kubernetes label key,
ordered unique 1..16 allowed values, and extended resource key used for this
logical accelerator. It is not inferred again from live nodes, an autoscaler,
`CUSTOM_GPU_RESOURCE_KEY`, or mutable configuration during replica launch.
The strict admission contract shared by protocols v2-v10 owns the accelerator
request across the whole Pod, not only the runtime container: after every
custom/legacy merge it removes all
supported GPU/TPU extended-resource requests and limits from every regular and
init container, Pod-level `resources`, and Pod `overhead`, then installs the
exact projected request and limit on the sole `ray-node` container. Any Pod-
or container-level Dynamic Resource Allocation claim is rejected because its
opaque device selection cannot be proven equivalent to this resource-key
contract.

Historical protocol-v1 candidates have the exact old key set, without
`projection_version`, `scheduler_name`, `kueue_admission`, `scratch`, or
`provision_timeout`. They remain readable only for ordinary launches during the
transition and can never authorize sequenced reserved fill. Historical
protocol-v2 candidates have the exact v2 key set, without `scratch` or
`provision_timeout`; they retain their strict Kueue, scheduler, digest,
ordinary-launch, and reserved-fill semantics for already committed versions.
The v2 decoder is isolated and hashes the exact historical v2 shape, so newer
protocols cannot reinterpret or change an existing digest. Historical v3 adds
typed scratch and timeout; historical v4 adds UID-bound runtime readiness.
Protocol v5 has one closed payload shape but two released renderer meanings and
therefore cannot be replay authority. Protocol v6 uniquely binds the scratch
bootstrap environment, v7 repairs its supervisor, v8 adds attested cache-leaf
bootstrap, and v9 safely normalizes only the final leaf. Protocol v10 adds the
closed cache `budget_scope`; it does not reinterpret any earlier cache object.
V1-v7 remain exactly decodable for inspection, settlement, and teardown. V8/v9
add exact historical Pod adoption under their own byte-stable renderers. V1/v2
terminal launches retain their historical launch-time timeout resolution.
There is no operator setting that selects an older protocol, and all new
version commits emit v10.

Historical cleanup removes v1-v7 decoders and transition tests only after the
objective drain gate below passes. The exact v8/v9 readers remain as the N-2/
N-1 window. Compatibility cannot create a second writer, reinterpret an
immutable row, or admit new provider effects from an older projection or
capability cohort.

`scratch` is a closed protocol-v3-v10 union. The default and explicit disabled form
is exactly:

```json
{"kind": "none"}
```

The memory-backed form is exactly the expanded record shown in the worker
example. `kind` is `memory`, `mount_path` is fixed to `/tmp`, `volume_name` is
fixed to `skypilot-serve-worker-tmp`, and `size_limit_bytes` is a positive,
non-boolean integer no larger than `2^63 - 1`. Operators select only `kind` and
`size_limit_bytes` through the effective server/workspace/context
`kubernetes.serve_worker_scratch`; the builder supplies the fixed path and
volume identity. Context configuration overrides workspace configuration.
Clients, service/task YAML, `pod_config`, resource labels, volumes, mounts, and
replica-launch inputs cannot supply any part of the scratch contract. Resource
labels are rejected before version commit and again before launch because a
caller label can activate mutating or validating admission policy. This does
not remove the trusted labels that the server renders after that boundary.

For `memory`, final rendering creates exactly one volume and one runtime mount:

```yaml
volumes:
  - name: skypilot-serve-worker-tmp
    emptyDir:
      medium: Memory
      sizeLimit: "21474836480"
containers:
  - name: ray-node
    volumeMounts:
      - name: skypilot-serve-worker-tmp
        mountPath: /tmp
```

The volume is mounted only into the sole `ray-node` runtime container. A
pre-existing volume with the reserved name, any regular/init/ephemeral
container mount that uses the reserved name or overlaps the `/tmp` root or any
of its descendants, a volume device with the reserved name, an alternate
source, duplicate, subpath,
read-only or propagation mutation, or a cache using either reserved identity
fails closed. A nested mount such as `/tmp/cache` could hide bytes from the
bounded memory-backed filesystem and therefore collides with the same single
owner. Exact already-rendered state is canonicalized after cache state, so a
repeated render is byte-for-byte stable. For `none`, both the reserved volume
and every mount that overlaps the `/tmp` tree must be absent; disabled scratch
never silently inherits an older owner.

Memory-backed `emptyDir` is disposable worker scratch, not a cache or durable
workspace. Its pages count against Pod/container memory, and `sizeLimit` is a
quota rather than a memory reservation; platform resource sizing must leave
room for the chosen limit. An eviction, OOM, or replica replacement may discard
all contents. Durable inputs and outputs remain in their separately authorized
object-store prefixes.

### Protocol-v6 scratch-backed bootstrap

Protocol v6 changes behavior only for a projected worker whose frozen scratch
kind is `memory`. The sole `ray-node` container receives exactly these three
server-owned Pod environment entries:

```text
SKY_RUNTIME_DIR=/tmp/.skypilot-runtime/root
UV_CACHE_DIR=/tmp/.skypilot-runtime/uv-cache
UV_PYTHON_INSTALL_DIR=/tmp/.skypilot-runtime/uv-python
```

The same exact values are explicitly exported once in the canonical bootstrap
script after trusted `runcmd` and before SkyPilot/uv setup. Pod-level environment
is required in addition to the exports because setup, run, recovery, and
control commands use independent `kubectl exec ... /bin/bash -c` processes;
shell exports from PID 1 cannot propagate into those fresh processes. Task
environments and secrets cannot override the three names. The uv executable
remains at `$HOME/.local/bin/uv`; relocating its approximately 55 MB is outside
this correction, while the measured approximately 1.34 GB per replica of
SkyPilot runtime, uv cache, and uv-managed Python writes moves off node rootfs.

The canonical bootstrap identity hashes its exact command, script, lifecycle,
and the three owned Pod environment entries. Render-time validation requires
one marker, one exact export for each value, and one exact literal Pod env entry
for each name. The existing finalized SHA crosses the provisioner boundary and
is reattested on the create response, adopted Pod, admitted Pod, and final fresh
read; webhook mutation or mixed rendering therefore fails closed before
provider success. Fresh v8 rendering uses the unique
`SKYPILOT_SERVE_WORKER_BOOTSTRAP_ENV_V8` marker. Protocol v4 hashes and behavior
remain byte-for-byte historical. Protocol v5 is retained only as a closed
decoder: release `1.1.1440` rendered an older environment under that
discriminator, while `1.1.1442` rendered the three values above with a v5
marker. Cohort 4 versus 5 is durable lineage for settlement, but neither v5
variant may be rendered or enter provider I/O under a v8/cohort-8 binary.
Protocols v6 and v7 retain their exact historical markers for decoding and
terminal settlement only. Any projection with `scratch.kind: none` injects
none of these names.

This uses the already bounded 20 GiB memory-backed `/tmp`; it adds no EFS, PVC,
host path, persistent cache, KubeRay, Terraform, or task-resource dependency.
The bytes count against Pod/node memory and remain disposable. It also changes
no LocalQueue, ClusterQueue, cohort, ResourceFlavor, quota, borrowing,
preemption, priority, scheduler, namespace, or service account. PHX continues
to submit through the existing lowest-priority Kueue lane, while East keeps its
existing non-Kueue scheduler path.

`controller_work_cache` is a separate nullable sibling. It is server/workspace
owned and never inferred from worker volumes or a campaign PVC. A bounded
Kubernetes `emptyDir` is exactly:

```json
{
  "kind": "empty_dir",
  "mount_path": "/mnt/controller-work",
  "required_bytes": 100000000000,
  "required_inodes": 100000,
  "size_limit_bytes": 120000000000
}
```

All integer values are positive and `size_limit_bytes >= required_bytes`. The
trusted Clin consumer must render that exact or larger `emptyDir.sizeLimit`. A
node-local controller cache is exactly:

```json
{
  "kind": "node_local",
  "mount_path": "/mnt/controller-work",
  "volume_name": "east-controller-nvme",
  "host_path": "/mnt/local-nvme/controller-work",
  "required_bytes": 100000000000,
  "required_inodes": 100000,
  "attestation": {"attestation_id": "...", "device_source_pattern": "^...$", "filesystem_type": "xfs", "required_bytes_per_replica": 100000000000, "required_inodes_per_replica": 100000, "max_replicas_per_node": 1, "reserved_bytes_per_node": 0, "reserved_inodes_per_node": 0, "usable_bytes_per_node": 100000000000, "usable_inodes_per_node": 100000}
}
```

Its requirements cannot exceed the attestation's per-replica budgets. The
controller cache is disposable and reconstructable from S3 in both cases.

The only worker-cache kinds in every retained projection protocol are `none`
and `node_local`. `none` has no other keys. Protocols v1-v7 retain their
historical direct-leaf shape for decoding and terminal cleanup only. Protocols
v8 and v9 `node_local` are exactly:

```json
{
  "kind": "node_local",
  "mount_path": "/mnt/sky-cache",
  "volume_name": "phx-h200-local-nvme",
  "host_path": "/opt/dlami/nvme/boltz-platform/boltz-l4-fleet",
  "host_mount_path": "/opt/dlami/nvme",
  "relative_path": "boltz-platform/boltz-l4-fleet",
  "bootstrap_image": "registry.example/skypilot@sha256:<64 lowercase hex>",
  "attestation": {
    "attestation_id": "phx-h200-cache-v1",
    "device_source_pattern": "^/dev/mapper/vg\\.01-lv_ephemeral$",
    "filesystem_type": "ext4",
    "required_bytes_per_replica": 500000000000,
    "required_inodes_per_replica": 1000000,
    "max_replicas_per_node": 8,
    "reserved_bytes_per_node": 500000000000,
    "reserved_inodes_per_node": 1000000,
    "usable_bytes_per_node": 4000000000000,
    "usable_inodes_per_node": 10000000
  }
}
```

Protocol v10 adds exactly one top-level cache key to that closed object:

```json
{"budget_scope": "replica_additive | node_shared_immutable"}
```

There is no default. A v8/v9 object carrying `budget_scope` and a v10
`node_local` object missing it both fail closed. `none` retains its one-key
shape in every protocol.

All strings are non-empty. Paths are absolute. Required, maximum-packing, and
usable integer fields are positive; reserved fields are non-negative. The
server validates:

```text
required_bytes_per_replica * max_replicas_per_node <= usable_bytes_per_node
required_inodes_per_replica * max_replicas_per_node <= usable_inodes_per_node
```

`host_mount_path` is a normalized, non-root absolute path and `relative_path`
is a normalized strict descendant which resolves with it to exactly
`host_path`. `bootstrap_image` is a trusted server-owned helper or worker
artifact pinned by SHA256 digest. The construction-only values come from the
exact context's `serve_worker_cache.host_mount_path` and `bootstrap_image`, or from
the operator-owned API-server environment
`SKYPILOT_SERVE_WORKER_CACHE_BOOTSTRAP_HOST_MOUNT_PATH` and
`SKYPILOT_SERVE_WORKER_CACHE_BOOTSTRAP_IMAGE`. Protocol v10 obtains
`budget_scope` only from the operator-owned JSON object in
`SKYPILOT_SERVE_WORKER_CACHE_BUDGET_SCOPES_JSON`, keyed by the exact
`attestation_id`; the object must contain only non-empty IDs and the two closed
scope values. A missing current attestation ID fails version construction.
This deployment-level registry is intentionally understood only by v10, so a
v8/v9 binary can continue to read the unchanged workspace configuration during
an N-2 rollback. All construction values are frozen into the immutable
projection; the provider never re-reads mutable configuration.

Protocol v8 mounts only the attested parent with
`hostPath.type: Directory`. A fixed, root-running, no-capability, read-only-
rootfs init container first verifies exact `findmnt` source, filesystem and
target evidence, total byte/inode budgets for the configured maximum packing,
and safe root-owned parent permissions. It then
creates every missing descendant component with dirfd-relative `mkdir` and
`O_NOFOLLOW`, requiring every resulting component to be root-owned mode
`0755`. The model container mounts the same volume at `mount_path` with the
frozen `subPath`. A missing or wrong parent mount therefore fails before any
directory creation; an absent leaf is safe and idempotently bootstrapped.
Kubernetes API readback must match this entire init/volume/subPath contract.
Protocol v9 preserves the same mount topology and `replica_additive` free-space
math and adds only the narrowly validated normalization of a safe root-owned
final leaf. The v8 and v9 scripts are different released artifacts; each is
retained byte-for-byte under its own discriminator.

Protocol v10 preserves that object topology and every attestation field, but
makes the leaf's budget semantics explicit. `replica_additive` retains the
v8/v9 live free-space check. `node_shared_immutable` asserts that all workers
mounting this service leaf consume one locked, content-addressed generation;
requiring the full `required_bytes_per_replica * max_replicas_per_node` amount
to remain free before every start would double-count that one generation and
eventually reject healthy warm-cache nodes. The exact checks are:

```text
replica_additive:
  free_bytes  >= required_bytes_per_replica  * max_replicas_per_node + reserved_bytes_per_node
  free_inodes >= required_inodes_per_replica * max_replicas_per_node + reserved_inodes_per_node
node_shared_immutable:
  free_bytes  >= required_bytes_per_replica  + reserved_bytes_per_node
  free_inodes >= required_inodes_per_replica + reserved_inodes_per_node
```

The historical field names remain part of the immutable projection wire shape.
For `node_shared_immutable`, they describe the one cold-fill working budget.
The static `required * maximum <= usable` checks remain for both scopes, so a
malformed platform attestation still fails before launch. The scope is frozen
into the projection digest, init-container environment, Pod readback identity,
and model runtime environment as `SKYPILOT_SERVE_CACHE_BUDGET_SCOPE`. This
correction does not weaken source/filesystem/size/ownership/mode/path checks,
create storage, delete cache contents, or authorize task-owned mounts.

`device_source_pattern` is an anchored platform-owned regular expression
because the concrete NVMe device name may differ by node. `attestation_id`
identifies externally retained platform evidence. Missing or partial evidence
produces `{"kind":"none"}`, never a weaker node-local claim.

## Fresh-launch rollout boundary

API 75 introduced protocol-v1 placement and identity projections, API 77
advanced worker placement to protocol v2, and API 79 advances new writes to
protocol v3. Later protocol v4 and v5 discriminators retain the same payload
shape while adding readiness and scratch-backed bootstrap semantics. None adds
campaign recovery state or a load-balancer control API.
The rollout is therefore limited to fresh campaigns. Before a nonempty Clin
campaign is enabled, Clin must ship and
verify an explicit fresh one-shot mode that launches and completes without
depending on external campaign-controller recovery or replacement.

Automated external campaign-controller recovery and takeover are deferred.
Enabling either requires a separate end-to-end design that validates the
immutable campaign scope and authoritative S3 lease before any queue replay can
occur. That future contract needs coordinated Clin rollout, explicit fix-
forward gates, and durable proof that replay cannot duplicate accepted work. Existing
SkyServe service-controller and ordinary-launch HA behavior is outside this
deferral.

## Architecture and invariants

The Serve controller computes all projections while committing the initial or
updated version under its frozen server configuration and durable workspace.
The persistence boundary strictly validates and copies the JSON. A committed
non-null projection is immutable; an identical lost-response retry must match
the stored value and cannot populate or change a legacy null projection.

The service workspace selects the controller home using server-owned
`kubernetes.serve_controller_workspace`. That named workspace selects
`kubernetes.serve_controller_context`; namespace, service account, priority,
and controller cache are all resolved within that controller workspace. The
named workspace must exist and differ from the service's inference workspace. A
context without an explicit controller workspace fails closed so the
controller can never inherit a worker's inference namespace or service
account. Priority uses the narrow
`serve_controller_priority_class_name` property with context-over-workspace
precedence rather than the workspace's broad `pod_config`. Worker candidates
continue resolving only from the original service workspace.

Worker candidates come from the immutable placement catalog when present.
Every task resource alternative must have an exact persisted-catalog match on
Kubernetes context, accelerator name, and whole accelerator count; live
capacity is not a substitute for catalog coverage. Every Kubernetes candidate
must pin a context and exactly one whole accelerator shape. Each context must
configure a strict server-owned `serve_worker_accelerator_scheduling` map from
logical accelerator to exactly `label_key`, `label_values`, and `resource_key`.
Logical keys are case-insensitively unique, label values cannot overlap between
logical accelerators sharing a label/resource dialect, and every catalog
candidate must resolve exactly one entry. The east deployment freezes A100 to
`nvidia.com/gpu.product=NVIDIA-A100-SXM4-40GB`, A100-80GB to
`NVIDIA-A100-SXM4-80GB`, and PHX freezes H200 to `NVIDIA-H200`; all three use
`nvidia.com/gpu`. These values came from direct cluster verification, not a
generic autoscaler formatter. Identity is resolved independently per candidate, so east
A100/A100-80GB and PHX H200 do not collapse into one tuple. An unconstrained
cloud candidate, unpinned Kubernetes context, malformed accelerator shape,
incomplete server-owned cache attestation, or malformed server-owned scratch
makes worker projection null and external launchers fail closed. Each non-null
tuple binds context, namespace, service account, cache, and scratch. Its
server-owned `pod_identity_role_arn` is either null (the context intentionally
uses no AWS Pod Identity role) or one strict AWS role ARN; the service account
remains exact in both cases. Explicit non-Kubernetes worker
candidates do not prevent projection of exact Kubernetes candidates. Candidate
projections must be unique by the runtime selection tuple `(context,
case-insensitive accelerator name, count)`; ambiguous catalog alternatives are
rejected at version commit rather than failing later at replica launch.

Each strict candidate (historical v2-v9 or canonical v10) also freezes the
effective server-owned Kueue admission contract in the same workspace/context
resolution pass. The LocalQueue uses
the existing workspace-aware queue resolver with request overrides omitted.
The WorkloadPriorityClass uses the context-aware
`serve_worker_kueue_workload_priority_class_name`; a managed queue and class
are an all-or-none pair. Task resource `priority_class`, nonempty task resource
labels, and task-owned `kubernetes.kueue` or `kubernetes.quota.queue` overrides
are rejected at version commit and again at launch. They are not silently
allowed to compete with the persisted projection. The same recursive boundary
rejects task-owned `auto_mounts`, `enable_docker`, and `custom_metadata`,
including context-scoped and future nested forms. These settings can otherwise
add PVCs, privileged sidecars, finalizers, or admission-triggering metadata
after the service version has been committed. Server workspace/context
configuration and the final renderer remain the only owners of Kubernetes
settings and labels after this boundary. The validator has three closed, exact
top-level key sets: v1 is isolated ordinary-launch compatibility, v2 is
isolated strict historical compatibility, and v3-v10 share one top-level
payload key set while retaining discriminator-specific cache and renderer
semantics. V2-v10 expose a deterministic
digest over their complete validated shape. No subset/superset recognition or
field-presence inference is allowed.

Protocols v3-v10 additionally freeze the effective provisioning timeout during
the same server/workspace/context build. The recursive task-input boundary
rejects `provision_timeout` at cloud-relative, Kubernetes-root, context, list,
and future nested scopes before commit and again before launch. V1/v2 retain
their exact historical key sets and launch-time timeout behavior; v3-v10 route
the signed candidate timeout into provider configuration.

Projected rendering selects the frozen candidate before Kubernetes deployment
variables are built. It bypasses live label discovery, GPU resource-key
discovery, allocatable-node probing, autoscaler formatting, and mutable
environment overrides, then reapplies the exact accelerator affinity and
request/limit after all Pod configuration merging and legacy YAML restoration.
It also bypasses live LocalQueue resolution and task priority. Cloud deploy
variables receive the selected candidate and derive `kueue_require_managed`,
LocalQueue, WorkloadPriorityClass, and (for v3-v10) `timeout` solely from that
candidate. Projected timeout resolution is terminal: mutable API-server configuration
and task cluster overrides are not fallback inputs.
Post-merge and legacy-YAML restoration reapply the exact provider fields,
Kueue queue/priority labels, and Pod PriorityClass so no restored caller value
can reopen another admission path. The provider config carries the explicit
`serve_worker_projection_protocol_version`; one named strict-admission
capability predicate selects the shared v2-v10 whole-Pod and Kueue behavior,
never whichever fields happen to be present. Version 1 remains confined to its
historical decoder. Protocol v2 introduced `scheduler_name` to the immutable
candidate, and v3-v10 retain it unchanged. It is read
only from the effective server-owned context/workspace `pod_config.spec`,
defaults to `default-scheduler`, and is included in the candidate digest and
typed reclaim-policy view. Request/task overrides are ignored or rejected.
Finalization removes `spec.nodeName` after every merge and writes exactly the
projected `spec.schedulerName`. A caller, restored legacy YAML, or mutable
launch-time Pod configuration therefore cannot direct-bind a Pod around the
frozen affinity or select an unverified scheduler.

The provider boundary carries the frozen label key/values, resource key, and
count. One shared whole-Pod accelerator helper owns both render-time rewrite
and admitted-object attestation. The provisioner requires exactly one
`ray-node` container with the frozen request and limit, no supported
accelerator resource on any other regular/init/Pod-level/overhead surface, no
Dynamic Resource Allocation claim, and the exact frozen `In` expression in
every required node-selector term. A missing or alternate OR term, changed
label values, changed resource count, duplicate runtime container, hidden
accelerator request, DRA claim, or stripped affinity fails provisioning closed.
Immediate guarded rejection attempts exact deletion and confirms absence; a
materialized strict-projection reserved-fill request that cannot confirm absence uses
the durable-owner cleanup boundary below.

The same provider boundary owns actual binding proof. Every strict v2-v10 Pod
must retain its exact projected `schedulerName`. The immediate create response
and any still-Kueue-gated adoption must have no `nodeName`; a webhook-injected
direct binding is rejected and deleted. An admitted adoption may still be
Pending and unbound, but if it is already bound its Node is read and must carry
the exact projected accelerator label key with one of the frozen values. After
the passive Running wait, the exact fresh-read Pod must have a non-empty
`nodeName`, and a fresh read of that exact Node under the same provider
authority guard must satisfy the same label check. Affinity on the Pod is thus
necessary but never treated as proof that the workload actually landed on the
projected accelerator class.

Protocols v3-v10 carry the complete frozen `scratch` record and
`provision_timeout` through the provider config as
`serve_worker_expected_scratch` and `timeout`. A v3-v10 projection without either
field, or a v1/v2 projection with either field, is malformed; historical v1/v2
provider configs still carry their launch-time-resolved `timeout`. Final
rendering strips caller scratch environment variables, installs the canonical
cache-then-scratch volume/mount order, and passes exactly one runtime container
into scratch injection. One
exception-neutral Kubernetes Pod-spec helper owns protocol capabilities, the
closed scratch identity, render-time rewrite, and admitted-object observation.
Serve and the provisioner only translate its typed failures at their
boundaries. The provisioner rechecks the fully finalized pre-create Pod with
that same helper and attests the immediate create response, adopted Pod, and
fresh post-Running Pod.
Any admission mutation of the volume source, `Memory` medium, byte limit,
reserved identity, mount semantics, container owner, or disabled state rejects
and exactly deletes the unsafe Pod through the existing identity-cleanup path.
Rendering uses a base-10 byte string; admitted-object attestation parses
Kubernetes Quantity syntax so an API-canonicalized equivalent such as `20Gi`
still proves the exact same byte limit.
This is admission proof of the bounded Kubernetes object; the platform worker
startup separately proves that `/tmp` is actually a `tmpfs` mount before model
setup begins.

Strict v2-v10 Kueue admission uses the same explicit marker. Rendering strips
all caller-supplied Kueue-prefixed labels/annotations and scheduling gates,
then installs one exact queue, group/count, `retriable-in-group=false`,
WorkloadPriorityClass, and admission-gate request. The immediate response must
have Kueue's managed label, exact lowercase eight-hex role hash, and the gated
pre-admission metadata phase. Reuse accepts either that state or an ungated
admitted state with Kueue's managed finalizer and `podset=role-hash`; TAS
workload identity is optional, while the exact LocalQueue/ClusterQueue output
pair is mandatory and requires `AssignQueueLabelsForPods`. This binds admission
to the ClusterQueue read at preflight even if a LocalQueue changes during a
long scheduling wait. Kueue v0.19 implicit TAS may add exactly
`podset-unconstrained-topology="true"` to an otherwise exact admitted, ungated
Pod; it remains forbidden before admission, and any other value or unknown
Kueue metadata fails closed. Non-Kueue scheduling gates also fail closed. A
managed finalizer without the PodSet and queue bindings is not admission proof
because Kueue installs the finalizer before quota grant. After
the passive scheduling/running wait, SkyPilot fresh-reads and reattests every
Pod under a new guard before publishing provisioning success. The fresh object
must retain the exact UID whose all-running observation ended the wait, remain
`Running`, and satisfy the admitted-only contract; a same-name replacement or
still-gated object cannot inherit the earlier evidence.

The inference context's narrow server-owned
`serve_worker_priority_class_name`, `serve_worker_priority_value`, and
`serve_worker_preemption_policy` form one admitted priority contract, including
when platform admission rather than `pod_config` supplies it. A non-null class
requires an explicit Kubernetes numeric value and `Never` or
`PreemptLowerPriority`; a null class requires both semantic fields null. The
expectation is frozen before launch. The generated provider config carries it
through the provisioner, which rejects and deletes a newly admitted or reused
Pod if its class, numeric value, or preemption policy differs. Immediate
rejection uses bounded deletion retries and confirms API absence before
returning the attestation error. An ordinary fence-less request whose deletion
cannot be confirmed fails provisioning closed and enters the ordinary
whole-cluster teardown path. A materialized strict-projection reserved-fill request
instead returns a typed terminal fence, forbids request-owned broad teardown or
placement failover, and leaves exact cleanup to the durable replica owner under
fresh authority, as defined by the reserved-capacity-fill design. A null class
attests class absence only because Kubernetes materializes its own default
numeric value/policy.
Neither priority, Kueue admission, nor Pod Identity role is read from campaign
`pod_config`, task `priority_class`, request queue overrides, or mutable
launch-time server configuration.

Reserved fill binds this source projection without creating another ownership
model. A sequenced claim stores the committed service version and candidate
digest, allocation schema 5 authenticates them, and replica state v17 and the
separate reserved-fill launch-context protocol v2 echo them. Both locked
replica admission and each provider-effect guard reselect the exact persisted
candidate and recompute its digest. An opaque strict-projection provisioner is
rejected before provider mutation;
only the instrumented in-tree Kubernetes path can create or adopt its Pod. The
first exact Pod create/adoption advances the association to provider I/O and is
the provider-present boundary. A failure before the complete built-in bulk
return is terminal and retains that association; specifically, Kueue admission
timeout publishes the typed exact-Pod handoff above. The successful bulk return
begins the post-admission materialized tail, and config-hash reuse is disabled
so an existing Pod re-enters the same attestation. After provider presence
there is no capacity failover or broad request-owned teardown. Runtime
preparation, internal mounts, Ray/skylet
startup, workdir/file synchronization, task setup, autostop/hooks, port
reconciliation, and job execution each require fresh bounded authority. The
same short-epoch rule applies before materialization: physical-cluster capture,
optional ephemeral-volume provisioning, Kubernetes bootstrap mutations, Pod
create/adoption, admitted handoff, and exact cleanup each reacquire the
idempotent bound association plus the v2 provider phase only around that
bounded concrete effect. A passive Kueue or scheduler wait retains only the
immutable physical-cluster capture token, never the service advisory lock or
process provider phase. Thus one waiting pool cannot block database-only service
updates, correctly fenced same-UID protocol-v2 recovery, or compatible v2
materialization. The capture itself remains an intentional per-context fence:
same-UID v2 callers may join it, tokenless legacy work against that context
fails busy until retirement, and isolated ambient work for another context is
independent. The
typed reclaim-policy scope is a view derived from that candidate and contains
its namespace, service account, Pod priority, Kueue admission, context,
accelerator, and count. A service update or any admission-field change rotates
claim generation and invalidates prior allocations.

PR #1608 is the sole cleanup consumer for a typed provider-present handoff. It
must be merged and deployed with or before activation of this timeout path. It
admits cleanup only after execution quiescence and a fresh exact `PRESENT`
observation, retains the association/request pin through UID-fenced down, and
releases them only after a fresh `ABSENT` observation. #1607 deliberately does
not duplicate that deletion authority.

Projection-bound workers receive no static cloud credential file mounts from
the API server, including kubeconfig, AWS/GCP credentials, or logging-agent
credential files. Their only cloud/storage authority is the frozen Kubernetes
service account / Pod Identity. Logging may use workload identity, but adding a
file-backed logging credential requires a future explicit server-owned
projection and cannot reuse the generic mount path.

This suppression applies only to credentials implicitly inherited from the API
server. Stable service YAML, runtime secrets, and file mounts are trusted,
operator-owned immutable contents of the committed service version and may
deliberately contain model runtime material. Mutable campaign and replica-launch
inputs cannot add or alter those version-owned fields. Clin's prepared service
YAML is separately audited to contain no direct S3 or KMS credentials. This
includes immutable `storage_mounts` in `MOUNT` or `MOUNT_CACHED` mode and their
fixed in-tree FUSE scaffold. `resource._requires_fuse` is accepted only when it
equals the state derived from those committed mount declarations; a direct
FUSE activation without a mount is rejected at commit and launch. This does not
authorize arbitrary task `hostPath`, PVC, `auto_mounts`, or Docker cache-volume
injection.

The workspace/context property `serve_worker_cache` may select `none` or refer
to a node-local registered volume and its attestation. For `node_local`, the
builder also verifies that the effective context `auto_mounts` contains the
exact volume/mount pair and that the registered volume is a context-matching
`k8s-hostpath`; the persisted host path comes from that registered volume, not
from campaign YAML. Protocol v8 also requires the trusted parent-mount path and
digest-pinned bootstrap image described above and persists their derived exact
leaf identity. Platform admission and worker startup must additionally
reject a missing mount, `overlay`, `tmpfs`, `ramfs`, a source outside the
anchored pattern, an unexpected filesystem, insufficient bytes/inodes, or
packing beyond the frozen budget.

The server applies the selected candidate's frozen cache projection after
caller environment merging. Campaign YAML does not duplicate the mount path or
budgets. For `none`, it sets only:

```text
SKYPILOT_SERVE_CACHE_KIND=none
```

For `node_local`, it sets `SKYPILOT_SERVE_CACHE_KIND=node_local` plus these
exact variables; integer values use base-10 strings:

```text
SKYPILOT_SERVE_CACHE_MOUNT_PATH
SKYPILOT_SERVE_CACHE_BUDGET_SCOPE
SKYPILOT_SERVE_CACHE_ATTESTATION_ID
SKYPILOT_SERVE_CACHE_DEVICE_SOURCE_PATTERN
SKYPILOT_SERVE_CACHE_FILESYSTEM_TYPE
SKYPILOT_SERVE_CACHE_REQUIRED_BYTES_PER_REPLICA
SKYPILOT_SERVE_CACHE_REQUIRED_INODES_PER_REPLICA
SKYPILOT_SERVE_CACHE_MAX_REPLICAS_PER_NODE
SKYPILOT_SERVE_CACHE_RESERVED_BYTES_PER_NODE
SKYPILOT_SERVE_CACHE_RESERVED_INODES_PER_NODE
SKYPILOT_SERVE_CACHE_USABLE_BYTES_PER_NODE
SKYPILOT_SERVE_CACHE_USABLE_INODES_PER_NODE
```

The registered volume name and host path are control-plane details and are not
injected into the workload environment. A model-specific runtime may map these
generic variables to its own cache settings, but must not provide fallback
values that turn a missing server contract into an unverified `/tmp`, rootfs,
PVC, or host path.

Protocol-v3-v10 workers receive one separate scratch environment contract after all
caller values with the reserved prefix are removed. `none` sets exactly:

```text
SKYPILOT_SERVE_SCRATCH_KIND=none
```

`memory` sets exactly these base values, with the integer rendered in base 10:

```text
SKYPILOT_SERVE_SCRATCH_KIND=memory
SKYPILOT_SERVE_SCRATCH_MOUNT_PATH=/tmp
SKYPILOT_SERVE_SCRATCH_SIZE_LIMIT_BYTES=21474836480
```

The trusted platform worker startup consumes these values. `memory` requires
`findmnt` to resolve `/tmp` itself as `tmpfs` before model setup; `none` requires
no server-reserved `/tmp` owner. The provisioner, rather than a workload-local
`df` estimate, proves the exact Kubernetes `emptyDir.sizeLimit` from the
admitted Pod object.

The trusted Clin consumer must render
`SKYPILOT_CONTROLLER_WORK_CACHE_KIND`, `_MOUNT_PATH`, `_REQUIRED_BYTES`, and
`_REQUIRED_INODES` into the external campaign controller Job. EmptyDir adds
`_SIZE_LIMIT_BYTES`. Node-local adds the attestation fields under
`SKYPILOT_CONTROLLER_WORK_CACHE_ATTESTATION_*`. Registered volume and host-path
values remain control-plane-only. This API change provides the validated
projection and environment mapping helper; Clin rendering and admission
verification remain rollout gates.

Replica launch remains fenced by service incarnation/version. This change does
not accept caller `pod_config`, task `volumes`/`volume_mounts`, service
accounts, priority classes, host paths, PVCs, storage classes, cache assertions,
scratch assertions, or provisioning-timeout overrides. Server-owned projected
cache, scratch, timeout, and workspace volumes are applied only after this
rejection boundary.

## Storage and migration

Additive PostgreSQL migration 043 (down-revision 042) adds nullable JSONB columns
`version_specs.controller_job_projection`,
`version_specs.controller_work_cache`, and
`version_specs.worker_placement_projections`. Existing rows stay null. The
historical migration also added nullable `version_specs.storage_broker`; that
abandoned field is absent from current SQLAlchemy metadata and every runtime,
configuration, persistence, and API path. The physical column remains inert so
a rolling deployment cannot break an older binary that still selects it; no
new writer may populate it. The retained projection columns survive rolling
mixed-version deployment so a later fix-forward binary can reuse immutable
metadata. The Serve controller's separately supported local SQLite topology
may use SQLAlchemy JSON for tests and local operation; there is no new
central-API SQLite migration target.

## Historical decoder removal gate

Protocol v10 is the sole fresh-effect winner. V1-v7 exist only as exact readers
and settlers for immutable historical rows. Protocols v8 and v9 are the
deliberate N-2/N-1 compatibility window: their distinct released render/init
bytes remain exact for historical adoption, inspection, settlement, and
cleanup. None is a feature flag, fallback, or alternate happy path. In
particular, no v10 binary may start a provider effect from a v1-v9 projection,
even when its digest is otherwise valid. Cleanup of the older v1-v7 decoders
remains blocked until one
retained evidence report proves all of the following at the same deployment
revision:

1. Every running API server, Serve controller, reserved-fill executor, and
   platform consumer advertises/accepts projection v10 and cohort epoch 10.
2. PostgreSQL contains zero non-null v1-v7 worker projections across every
   retained `version_specs` row, not merely the latest or active versions.
   Immutable rows are drained and removed by the normal retention procedure;
   they are never reconstructed from mutable configuration.
3. There are zero nonterminal claims, allocations, replica launch records, or
   durable cleanup records whose authenticated candidate discriminator/digest
   refers to a v1-v7 projection. Generation rotation has invalidated every
   older claim before the census is taken.
4. Ordinary-launch and reserved-fill telemetry has observed only v10 for one
   complete version-retention and replica-cleanup window, and no external
   consumer still requests a historical projection.
5. The v10 render/admission/startup checks, including both `scratch.kind`
   values, exact projected provisioning timeout, cache-parent attestation and
   leaf bootstrap, exact Pod environment, post-`runcmd` exports, runtime SHA,
   and fresh-`kubectl exec` inheritance, have passed on every enabled worker
   context.

Once those gates pass, the cleanup removes the v1-v7 key sets and decoders,
their historical digest branches, their ordinary-launch compatibility,
transition-only tests, and transition-only telemetry in one change. It retains
the byte-exact v8/v9 readers as the standard N-2/N-1 window. The supported
decoder set then becomes exactly `{8, 9, 10}`, while the fresh-effect set is
exactly `{10}`. A defect is corrected by a successor API/image; immutable v8
or v9 state is never reinterpreted through v10 semantics.

## Implementation phases

1. Commit this canonical design and strict projection model/builder.
2. Add PostgreSQL version storage, immutable persistence, API protocol marker,
   and history fields with focused migration/state/API tests.
3. Advance worker projection writes to protocol v2, freeze Kueue admission,
   reject caller admission overrides, and route rendering/restoration through
   the selected persisted candidate.
4. Bind the v2 digest and committed service version through Serve046 reserved-
   fill claims, allocation schema 5, replica state v17, and terminal policy
   authorization as specified by the reserved-fill design.
5. Keep external campaign-controller recovery and takeover unavailable; require
   a verified Clin fresh one-shot mode as an explicit rollout prerequisite.
6. Configure the dedicated east controller workspace and the inference
   workspace's controller auth Secret reference, per-context worker priorities,
   Kueue queues and WorkloadPriorityClasses, Pod Identity role ARNs, exact
   accelerator scheduling maps, explicit priority values/policies, and fully
   attested caches;
   update the stable service and verify all old replicas are gone.
7. Advance the sole new-write path to protocol v3/API 79; freeze typed worker
   scratch and the effective provisioning timeout, separate bounded Kueue
   admission from the fresh post-admission scheduling timer, reject task-owned
   timeout, scratch, and Pod configuration, render and attest the final Pod,
   retain exact v1/v2 readers only until the objective cleanup gate, and update
   the stacked cleanup to remove both readers.
8. Advance the same closed payload to protocol v4 and bind provider success to
   one UID-scoped authenticated base-runtime readiness producer.
9. Advance the sole new-write path to protocol v5 and capability cohort epoch
   5. For memory scratch only, install the exact three Pod env values and the
   identical post-`runcmd` exports, include them in the canonical bootstrap
   digest, retain v1-v4 read/settle compatibility, and make no Kueue or platform
   configuration change.
10. After the released v5 discriminator collision, advance the sole new-write
    path to protocol v6 and capability cohort epoch 6. Use the unique
    `SKYPILOT_SERVE_WORKER_BOOTSTRAP_ENV_V6` marker with
    `SKY_RUNTIME_DIR=/tmp/.skypilot-runtime/root`,
    `UV_CACHE_DIR=/tmp/.skypilot-runtime/uv-cache`, and
    `UV_PYTHON_INSTALL_DIR=/tmp/.skypilot-runtime/uv-python`. Retain v1-v5
    validation and evidence-backed settlement/teardown, but reject every v1-v5
    provider-effect start. This is a discriminator/fence correction only: no
    schema, Kueue, platform, task-resource, storage, or paid-path change.
11. Advance the sole new-write path to protocol v7/cohort 7 with bounded
    apt/dpkg supervision and an unambiguous terminal bootstrap marker. Retain
    v6 and earlier only for their exact historical read/settle/teardown
    boundaries; never edit a released protocol discriminator in place.
12. Advance the sole new-write path to protocol v8/API 91/cohort 8. Freeze the
    registered cache leaf, its attested host parent, relative descendant, and
    digest-pinned server-owned bootstrap image in the immutable projection.
    Render one parent `hostPath.type: Directory`, verify the device and full
    packing envelope in a least-privilege init container, create the canonical
    root-owned descendant without following symlinks, and expose only that
    descendant to `ray-node` through the exact `subPath`. Retain v1-v7 only for
    decode, evidence-backed settlement, and bounded teardown.
13. Advance the sole new-write path to protocol v9/cohort 9 without editing v8
    bytes. Normalize only a root-owned, non-symlink final cache leaf which is
    otherwise safe; retain both released scripts under their exact historical
    discriminators.
14. Advance the sole new-write path to protocol v10/API 93/cohort 10. Add the
    closed `budget_scope` field, bind it into the candidate digest and Pod
    attestation, obtain it only from the operator-owned attestation-ID registry,
    and retain v8/v9 bytes and `replica_additive` semantics exactly.

## Deployment and fix forward

Remove the abandoned broker configuration before deploying API 76 or later;
API 75 tolerates its absence. Deploy the database migrations before or with the
API 77 server/controller/load-balancer binaries. Migration 043 is already
present, and its historical nullable broker column remains inert so an older
binary cannot fail during a rolling deployment; no current runtime or API path
uses it.

Before API 79 can commit a v3 version, deploy the platform configuration with
an explicit timeout for reserved contexts and worker startup that understands
the exact scratch environment. Then replace the
API, Serve controller, and every replica executor with the same API 79 image
while service updates and new replica materialization are paused; resume only
after all roles report the new image/API. New service updates then freeze v3.
Historical v1 versions remain ordinary-launch-only; exact v2 versions continue
through their historical strict path until naturally drained. An active
service needs an explicit update to receive a v3 scratch/timeout
contract—immutable v2 rows are never rewritten from current configuration.

Before a v6 service version can be committed, hold new replica materialization
and replace the API, Serve controllers, provider-proof daemon, and request
executors with one exact cohort-6 image. Adjacent cohort 5 may continue only
read, recovery, settlement, and cleanup of work it already owns; it cannot
admit a new request or enter provider I/O. Validate the exact v6 projection and
bootstrap SHA from the complete cohort, then create the service so a new
immutable version is born at v6. Existing v1-v5 rows are never rewritten.
This rollout uses direct Helm fix-forward and does not require an EFS volume,
database migration, platform pin, Terraform/Terragrunt apply, or change to the
existing Kueue policy.

The v6-v9 rollout paragraphs and generation transitions are retained chronology
only. For the current v10 rollout, hold fresh provider effects and replace all
API, Serve controller, provider-proof, and executor writers with one exact
API-93/cohort-10 image through direct Helm `--reuse-values`. Install the strict
`SKYPILOT_SERVE_WORKER_CACHE_BUDGET_SCOPES_JSON` construction registry in the
API environment inherited by controller construction before version creation;
executors consume only the frozen projected value and do not re-read the
registry. The same image must contain the bounded controller-memory
correction: exact on-cloud cluster label reads
when the durable handle is available, streaming metadata-only fallback,
per-target single-flight reads which cannot reuse pre-teardown absence, handle
reuse, and one bounded reusable readiness executor with terminal shutdown.
V8/v9 may adopt, inspect, settle, and clean exact historical work during the
bounded overlap but cannot start a fresh effect.

Once every writer is homogeneous and controller memory is stable, normally
tear down the test-only `boltz-l4-fleet` with ordinary `serve down` (not a
historical purge procedure), prove exact database, Pod, Workload, and provider
absence, and create a fresh version 1 from the frozen local
`ml_models/providers/skypilot/**` source, currently tested at
`a69fb3409ede26f98c02f7730f764021c1ef0146`. This rollout uses no projection
migration, SQL relabel, manual Pod deletion, provider Platform PR,
`boltz-platform` runtime pin, EFS, Terraform/Terragrunt, or Kueue change. Do not
replay a historical activation generation. Re-read the fresh protocol-v10/
cohort-10 claim scope before allowing provider effects, then prove cache
bootstrap and complete scheduler-authorized occupancy.

This is a fix-forward rollout: no version re-derives projections, a successor
image/version corrects defects, and cleanup removes older rows only after their
work has drained. A full binary rollback which authorizes fresh effects is not
a supported gate; the v8/v9 byte-exact N-2/N-1 historical adoption,
inspection, settlement, and cleanup window remains supported. External
campaign-controller takeover remains unavailable before, during, and after
this rollout; an older binary cannot authorize campaign replay.

## Verification

Automated tests must cover strict schemas, cross-context controller/worker
projection, deterministic candidate IDs, configuration precedence, incomplete
cache attestation, registered-volume/context/automount mismatch, packing
inequalities, exact context/accelerator/count catalog coverage, scratch schema
and context-over-workspace precedence, provisioning-timeout type/range,
context-over-workspace precedence and default preservation, caller `pod_config`,
task-volume, task-scratch and recursively nested task-timeout rejection, task
resource-label rejection, recursive task
`auto_mounts`/`enable_docker`/`custom_metadata` rejection, derived-FUSE
consistency with trusted immutable
storage mounts, nullable legacy rows, immutable retries,
stored-only version history, suppression of all static worker credential
mounts, zero-live-capacity rendering without discovery, frozen GPU resource-key
provisioning, frozen LocalQueue/WorkloadPriorityClass, digest sensitivity to
every candidate field, caller priority/queue rejection, projected rendering
and legacy restoration that ignore mutable queue/task priority, exact v1-v9
historical compatibility, mixed-version persisted-record rejection, sequenced
v1 rejection, frozen historical digest/cleanup behavior, old/new-v5 collision
fixtures, v6/v7 digest stability, v8/v9 byte stability,
cache-parent/relative-path/bootstrap-image and v10 budget-scope sensitivity,
strict current-v10 fresh effects, stale version/digest
claim and launch rejection, immediate guarded cleanup of admitted
identity/scheduling/admission mismatches, and durable-owner cleanup when a
materialized reserved-fill request cannot confirm immediate absence. Provider
tests must also reject an opaque strict-projection provisioner before mutation, prove
the one-way built-in materialization marker, disable config-hash shortcuts, and
show that post-materialization failures cannot enter capacity failover or broad
request-owned teardown while every later effect requires fresh authority.

The accelerator suites exercise YAML and real Kubernetes client models across
all regular containers, init containers, Pod-level resources, overhead, Pod-
and container-level DRA, duplicate runtime containers, affinity OR terms, and
render/admission symmetry. They also prove caller and webhook `nodeName`
injection is removed or rejected, caller/webhook scheduler changes cannot
override the exact projected scheduler (including a server-owned custom
scheduler), and admitted/adopted and post-wait Running Pods are rejected when
unbound or when their freshly read Node lacks the exact projected accelerator
label. Kueue suites exercise exact group/count/retriable/role-hash
identity, pre-admission/adoption/admitted-only phase coupling, TAS-off
acceptance, fail-closed queue-label-feature-off rejection, exact post-wait UID
continuity, unknown response metadata/gates, and rejection of an ungated
finalizer-only Pod.

Scratch suites must cover exact v3-v10 round-trip validation, default `none`,
positive integer bounds, cache/scratch identity collision, pre-existing volume,
mount, init/ephemeral-container and volume-device collisions, `/tmp` path
aliases and descendant mounts, duplicate exact owners, one-runtime-container
enforcement, deterministic
cache-then-scratch ordering across repeated render/restoration, exact provider
handoff, pre-create finalization drift, Kubernetes client-model admission,
admission mutation/deletion, and the negative `none` contract. The combined
test must prove Kueue, authenticated parent `hostPath.type: Directory` cache,
the v8 init bootstrap plus main-container `subPath`, and memory scratch coexist
in one final Pod without reopening task `pod_config`,
automatic mounts, Docker sidecars/cache PVCs, or custom admission metadata.

Protocol-v8 bootstrap suites must additionally prove the exact three literal
Pod environment entries, removal of task env/secret collisions, one identical
post-`runcmd` export per name, inclusion of owned env in the authenticated
bootstrap SHA, rejection of marker/export/env drift at final render and
provider attestation, unchanged v4 hashes, and no injection for
`scratch.kind: none`. A rendered Pod environment plus a fresh
`KubernetesCommandRunner` `/bin/bash -c` setup invocation must demonstrate that
independent `kubectl exec` commands inherit the paths without a per-command
prefix.

Protocol-v8 cache suites additionally execute the real init script against a
fake exact `findmnt` boundary. They cover missing-leaf success, idempotency and
concurrent creation; accepted root-owned `0755` and sticky `1777` parents;
rejected non-sticky writable parents, symlinks, files, ownership/mode drift,
wrong source/filesystem/target, rootfs-like filesystems, and insufficient
full-packing free budgets. Dict and real Kubernetes-client Pod models must
round-trip with Kubernetes-defaulted init fields while image, command,
environment, security context, volume, mount, or `subPath` drift fails at both
the final-render and provider-readback boundaries.

Protocol-v10 qualification runs the same negative suite and adds a filesystem
boundary where free space is greater than one required-plus-reserved cache
envelope but less than the historical full-packing envelope. V9 must reject
that boundary and v10 must accept it. Both versions must reject one byte or one
inode below their respective exact threshold, and the v8/v9 script bytes must
remain unchanged. Projection and cohort tests require v10 for all fresh
effects while retaining v1-v9 decode/settlement behavior.

Non-compute deployment verification must inspect immutable version history and
existing admitted workers for the frozen service account, exact accelerator
affinity/resource key and count, priority class (numeric `-1000`,
`preemptionPolicy: Never`), exact Kueue LocalQueue, required-managed gate, and
WorkloadPriorityClass, exact v10 provisioning timeout, and exact v10 scratch
contract. In PHX the LocalQueue must be `be`, the admitted ClusterQueue output
must be `skypilot-be`, and the WorkloadPriorityClass must be `be-lt`, with Pod
priority `-1000`/`Never`. Fresh-read each admitted Pod's exact `nodeName`,
inspect that Node, and require the projected accelerator label key/value before
counting it as usable capacity. On every already-advertised node-local
candidate, use existing startup evidence to verify `findmnt`, source,
filesystem, free bytes/inodes, and per-node packing. For memory scratch, verify
the final Pod has the fixed volume/mount, `medium: Memory`, and exact byte-value
`sizeLimit` (allowing Kubernetes Quantity canonicalization), and startup
accepted `/tmp` as `tmpfs`; for `none`, verify the reserved volume and `/tmp`
owner are absent. For a memory-backed v8 worker, run a fresh `kubectl exec` and
require all three paths to resolve beneath `/tmp`; verify the runtime, cache,
and Python trees consume the memory-backed mount rather than node rootfs and
that the authenticated bootstrap digest equals the committed provider
expectation. The uv executable may remain under `$HOME/.local/bin`; no other
large SkyPilot bootstrap tree may remain on rootfs. Restart the API and Serve
controller and prove existing committed projections and terminal provider
timeouts are unchanged by mutable queue, priority, namespace, scheduling,
timeout, and scratch server configuration. Confirm external
campaign-controller recovery/takeover remains blocked rather than replaying
ambiguous work. Do not deploy a synthetic service or launch a fresh campaign
for this rollout.

## Open gates

- Merge protocol-v10 source with focused tests proving v8/v9 byte identity is
  unchanged and v10 accepts one shared-cache reserve while continuing to
  reject insufficient bytes/inodes, unsafe paths, ownership/mode drift, wrong
  devices/filesystems, and malformed static packing attestations.
- Publish one immutable SkyPilot image and deploy it homogeneously to API,
  controller, and executor roles through Helm `--reuse-values`. Require API 93,
  projection protocol 10, and capability cohort 10 from every writer before a
  new provider effect.
- Merge the source-under-review controller-memory correction into that image.
  Production observed three confirmed controller OOM restarts (two on Pod
  suffix `jxgkf`, one on `wsrfc`) and the active controller cgroup at
  8,408,256,512 bytes (about 97.9% of its 8-GiB limit).
  Prove bounded RSS/cgroup memory, bounded thread count, and no new OOM across
  teardown, recreation, full refill, and one complete stale/quiescence window;
  raising the memory limit is not the correction.
- Because `boltz-l4-fleet` is test-only and its durable reserved-fill origins
  make replica versions immutable, normally tear it down and recreate it from
  tested local provider commit
  `a69fb3409ede26f98c02f7730f764021c1ef0146`. Do not SQL-relabel replicas or
  migrate old projections. Stop if normal teardown cannot prove exact
  Pod/provider absence.
- Prove the East node that previously failed the 4.5-TB v9 gate accepts the
  v10 1.0-TB shared-cache gate, then reaches Ready without deleting cache data
  or changing Kubernetes policy. Prove every East grant is Ready or carries a
  fresh typed physical incompatibility reason.
- Prove PHX continues to use Simone's unchanged LocalQueue `be` -> existing
  ClusterQueue `skypilot-be`, WorkloadPriorityClass `be-lt`, and independent
  Pod priority `-1000`/`Never`. Count only `POLICY_ADMITTED` capacity as usable.
  GPU-only inventory that
  Kueue rejects for joint CPU/topology fit is not part of the utilization
  denominator and must remain visibly attributed rather than causing paid
  suppression or a Kueue mutation.
- Run the bounded paid-Spot residual/drain exercise only after the fresh
  service is idle, telemetry is fresh/complete, provider observers are ready,
  and the approved ten-logical-L4 cap is read back from PostgreSQL. Ordinary
  on-demand remains structurally absent.

## Historical protocol-v8 rollout record

The list below records the predecessor rollout contract. It is retained as
historical rationale and is not the current gate list; protocol v10 and the
gates above supersede its pending tense.

- Merge and publish the reviewed protocol-v8 source. Use an immutable trusted
  helper/worker image digest as the v8 cache-bootstrap image and verify it
  contains
  `/bin/bash`, `python3`, and `findmnt`. No EFS, Kueue, Terraform/Terragrunt,
  KubeRay, task-Pod override, or platform runtime pin is part of this change.
- Direct-Helm deploy all API/controller/executor writers with `--reuse-values`
  and the two construction inputs above. Keep provider-effect authority held
  until every writer reports the same image digest and API 91/projection v8.
  Protocol v7 may decode and settle retained work but cannot render a fresh
  Pod.
- Because SkyPilot is test-only, normally delete and recreate
  `boltz-l4-fleet` after homogeneous deployment instead of adding a projection
  migration. Wait for typed absence, prove the fresh version is protocol
  v8/cohort 8 and its claim scope is complete, then perform exactly one
  reviewed generation-37-to-38 active-to-active cohort activation. Never
  activate before recreation and never replay an earlier generation
  transition.
- Prove a fresh PHX worker on a node with an absent leaf: the init container
  must attest `/opt/dlami/nvme`, create the exact leaf hierarchy, and allow the
  main `subPath` mount to start. Then prove every free GPU admitted by Simone's
  unchanged Kueue configuration is assigned, East consumes every compatible
  scheduler-fit free GPU, and paid claims/waiters/Spot remain zero while
  reserved capacity covers demand.
- The distinct east controller workspace/context/namespace/service account is
  configured and applied. Verify a fresh admitted controller Job's identity,
  64 GiB ephemeral-storage request, 80 GiB limit, and
  `/mnt/scratch` byte/inode checks. Its ambient role needs the controller's
  S3/KMS permissions; worker contexts with a non-null projected Pod Identity
  role must use it for the approved input and output prefixes.
- Live PHX H200 and east probes supplied the filesystem, source, free
  byte/inode, reserve, and maximum-packing evidence now encoded by the platform
  change. The v8 trusted init must verify `/opt/dlami/nvme` is the expected
  non-root device/filesystem before creating the exact registered descendant.
  The final Pod must mount the parent with `hostPath.type: Directory`, never
  `DirectoryOrCreate`, and expose the registered leaf only through the exact
  `subPath`. After rollout, repeat the worker startup check on every admitted
  node; any mismatch must fail that worker rather than weaken the cache
  contract.
- The required inference priority class is encoded for both contexts. Verify
  admitted east and PHX workers resolve it to value `-1000` with
  `preemptionPolicy: Never` before bulk traffic.
- The server-owned PHX LocalQueue,
  `serve_worker_kueue_workload_priority_class_name`, and
  `AssignQueueLabelsForPods` inputs are configured and applied; east remains
  intentionally outside Kueue. Verify a fresh protocol-v8 projection freezes
  the PHX pair, task/request overrides are rejected, the admitted Pod reports
  the exact preflight LocalQueue/ClusterQueue pair, Kueue reports the Workload
  at that exact class, and higher-priority BCL/research work retains the
  existing preemption relationship. This is verification of Simone's unchanged
  policy, not authority to mutate Kueue topology or policy.
- The verified east A100/A100-80GB and PHX H200
  `serve_worker_accelerator_scheduling` entries are configured and applied.
  From a fresh protocol-v8 projection, render an H200 worker with no live PHX
  GPU nodes visible and prove no node, label, resource-key, autoscaler, or
  `CUSTOM_GPU_RESOURCE_KEY` lookup occurs; the admitted Pod must still require
  `nvidia.com/gpu.product In [NVIDIA-H200]` and `nvidia.com/gpu: 1`.
- Typed memory-backed `serve_worker_scratch` is configured and applied for
  every enabled reserved inference context. After the protocol-v8 successor is
  deployed, verify fresh protocol-v8 history freezes the effective
  `provision_timeout`, Kueue quota wait does not consume that timer, and final
  Pods and startup evidence agree on kind, `/tmp`, and exact size. No task or
  service YAML may contain a scratch volume or `pod_config`;
  task Kubernetes config also cannot select `provision_timeout`, `auto_mounts`,
  `enable_docker`, or `custom_metadata`, and task resources cannot set labels
  for projected workers. Verify a terminal v8 launch renders the version's
  exact frozen timeout even when the API server's ambient config later differs.
- The dedicated inference workspace is configured and applied. Verify the
  freshly recreated protocol-v8 service resolves its worker projections only
  from that workspace; no further shared-fleet or workspace mutation is a gate.
- Do not enable nonempty campaigns until Clin ships and verifies a fresh
  one-shot mode independent of external campaign-controller recovery or
  takeover.
- A separately designed S3 lease-authority contract and coordinated Clin
  rollout remain required before enabling external campaign-controller
  recovery or takeover.
- Defer east+PHX H200 bulk qualification to its separately owned rollout; it is
  not a reserved-fill activation gate and must not manufacture a GPU canary for
  this change.
