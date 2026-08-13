# Persisted SkyServe Controller and Worker Placement Projection

**Status:** Implementation in progress; platform attestation and deployment
smoke tests pending
**Last updated:** 2026-08-13

## Goals

- Freeze one server-owned Kubernetes controller-home identity for each
  committed SkyServe version independently from the version's worker routes.
- Freeze every eligible Kubernetes worker candidate's context, namespace,
  service account, priority class, accelerator shape, and disposable-cache
  contract so east and PHX can coexist in one heterogeneous service.
- Expose authenticated, versioned metadata that a campaign launcher can consume
  without copying arbitrary `pod_config`, credentials, PVCs, or host paths.
- Allow fresh campaigns to launch while explicitly deferring automated
  external campaign-controller recovery and replay until storage-lease
  authority is end-to-end.

## Non-goals

- This contract does not persist campaign inputs or outputs. Canonical `s3://`
  identities and immutable manifests remain durable truth; signed URLs are
  minted only after dispatch and may cross regions only while expected
  aggregate transfer is below 3 TB.
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
`placement_projection_protocol_version: 1`. Each version entry adds two
nullable placement fields plus nullable controller-cache and storage-broker
fields. A consumer must require the protocol field and strictly validate every
non-null projection; API revision alone is not capability evidence.

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
inference worker context or identity.

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
  "candidate_id": "kubernetes-0002",
  "kubernetes_context": "phx_research_cluster_eks",
  "namespace": "rescluster-k8s-phx",
  "service_account_name": "skypilot-pool-sa",
  "priority_class_name": "rescluster-k8s-prod-east1-preemptible-inference-low",
  "priority_value": -1000,
  "preemption_policy": "Never",
  "pod_identity_role_arn": "arn:aws:iam::123456789012:role/skyserve-worker-phx",
  "accelerator_name": "H200",
  "accelerator_count": 1,
  "accelerator_scheduling": {
    "label_key": "nvidia.com/gpu.product",
    "label_values": ["NVIDIA-H200"],
    "resource_key": "nvidia.com/gpu"
  },
  "cache": {"kind": "none"}
}
```

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

`storage_broker` is another immutable nullable sibling. Historical versions
remain null. New versions freeze the server/workspace-owned non-secret broker
descriptor exactly:

```json
{
  "endpoint": "https://storage-broker.int.boltz.bio/v3/grants",
  "audience": "boltz-skyserve-worker",
  "api_version": 3,
  "grant_uri_prefix": "s3://boltz-skyserve-grants/prod",
  "authenticated_worker_role_arns": [
    "arn:aws:iam::123456789012:role/skyserve-worker-east",
    "arn:aws:iam::123456789012:role/skyserve-worker-phx"
  ],
  "kms_key_id": "alias/skyserve-grants",
  "transfer_authorization_limit_bytes": 3000000000000
}
```

The endpoint is an absolute HTTPS URL without credentials, query, or fragment.
The grant prefix is an absolute `s3://` URI without credentials, port, query, or
fragment. Its authority is one canonical S3 bucket. Its nonempty path rejects
empty or dot/traversal segments, backslashes, and all percent encodings so the
persisted prefix has one authorization meaning. The
bucket grammar also rejects AWS-reserved alias, access-point, MRAP, directory,
and table-bucket name forms; the authority cannot silently name a different S3
resource type. The
worker-role array is ordered, non-empty, duplicate-free, and every entry is an
IAM role ARN; it binds a grant to the finite set of ambient worker principals
authorized across the service's regions.
`transfer_authorization_limit_bytes` is the exact integer
`3000000000000`; publishing it in the immutable server projection makes the
broker's strictly-less-than-3-TB cumulative logical-byte fence part of the
consumer contract instead of an independently configured campaign assumption.
`api_version` is exactly 3. Version 3
is the clean-cutover contract that requires server-owned, atomic cumulative
campaign transfer reservations before byte-moving authorization; version 2's
per-grant accounting cannot establish the campaign-wide transfer ceiling. The
object never contains a bearer token, secret, temporary credentials, signed
URL, or grant.
Clients require SkyPilot API >= 76 as well as placement protocol 1 before
consuming a storage-broker projection with the transfer ceiling. API 75
advertises the base placement/storage descriptor but is not activation-capable
for this broker contract.

API 76 forward-reads a persisted API-75 six-key descriptor without synthesizing
the ceiling. Its history response preserves that old shape, so a strict API-76
consumer fails closed instead of treating an old row as newly authorized. Only
a version committed from the API-76 seven-key server configuration carries the
ceiling.

The only cache kinds in protocol v1 are `none` and `node_local`. `none` has no
other keys. `node_local` is exactly:

```json
{
  "kind": "node_local",
  "mount_path": "/mnt/sky-cache",
  "volume_name": "phx-h200-local-nvme",
  "host_path": "/mnt/local-nvme/skypilot-cache",
  "attestation": {
    "attestation_id": "phx-h200-cache-v1",
    "device_source_pattern": "^/dev/nvme[0-9]+n[0-9]+p?[0-9]*$",
    "filesystem_type": "xfs",
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

All strings are non-empty. Paths are absolute. Required, maximum-packing, and
usable integer fields are positive; reserved fields are non-negative. The
server validates:

```text
required_bytes_per_replica * max_replicas_per_node <= usable_bytes_per_node
required_inodes_per_replica * max_replicas_per_node <= usable_inodes_per_node
```

`device_source_pattern` is an anchored platform-owned regular expression
because the concrete NVMe device name may differ by node. `attestation_id`
identifies externally retained platform evidence. Missing or partial evidence
produces `{"kind":"none"}`, never a weaker node-local claim.

## Fresh-launch rollout boundary

API 75 freezes placement and identity projections only. It adds no campaign
recovery state or load-balancer control API. The rollout is therefore limited
to fresh campaigns. Before a nonempty Clin campaign is enabled, Clin must ship
and verify an explicit fresh one-shot mode that launches and completes without
depending on external campaign-controller recovery or replacement.

Automated external campaign-controller recovery and takeover are deferred.
Enabling either requires a separate end-to-end design in which the storage
broker validates the immutable campaign scope and authoritative S3 lease before
any queue replay can occur. That future contract needs its own credentials,
coordinated Clin rollout, mixed-version behavior, rollback gates, and durable
proof that replay cannot duplicate accepted work. Existing SkyServe service
controller and ordinary-launch HA behavior is outside this deferral.

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
controller can never inherit a worker's inference namespace or broker-only
service account. Worker candidates continue resolving only from the original
service workspace.

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
cloud candidate, unpinned Kubernetes context, malformed accelerator shape, or
missing per-context `serve_worker_pod_identity_role_arn`, or incomplete
server-owned cache attestation makes worker projection null and external
launchers fail closed. Each non-null tuple binds context, namespace, service
account, and exact AWS Pod Identity role ARN. If a storage broker is frozen,
every projected role must occur in its ordered 1..16-role allowlist. SkyPilot
core does not require exactly two roles; east+PHX is this deployment's profile.
Explicit non-Kubernetes worker candidates do not prevent projection of exact
Kubernetes candidates. Candidate projections must be unique by the runtime
selection tuple `(context, case-insensitive accelerator name, count)`; ambiguous
catalog alternatives are rejected at version commit rather than failing later
at replica launch.

Projected rendering selects the frozen candidate before Kubernetes deployment
variables are built. It bypasses live label discovery, GPU resource-key
discovery, allocatable-node probing, autoscaler formatting, and mutable
environment overrides, then reapplies the exact accelerator affinity and
request/limit after all Pod configuration merging and legacy YAML restoration.
The provider boundary carries the frozen label key/values, resource key, and
count. The provisioner uses that resource key for runtime finalization and
attests every newly admitted or reused Pod: exactly one `ray-node` container
must request and limit the frozen resource count, and every required node
selector term must contain the exact frozen `In` expression. A missing or
alternate OR term, changed label values, changed resource count, or stripped
affinity is deleted with confirmed absence and fails provisioning closed.

The inference context's narrow server-owned
`serve_worker_priority_class_name`, `serve_worker_priority_value`, and
`serve_worker_preemption_policy` form one admitted priority contract, including
when platform admission rather than `pod_config` supplies it. A non-null class
requires an explicit Kubernetes numeric value and `Never` or
`PreemptLowerPriority`; a null class requires both semantic fields null. The
expectation is frozen before launch. The generated provider config carries it
through the provisioner, which rejects and deletes a newly admitted or reused
Pod if its class, numeric value, or preemption policy differs. Rejection uses
bounded deletion retries and confirms API absence before returning the
attestation error; an unconfirmed deletion fails provisioning closed and enters
the ordinary whole-cluster teardown path. A null class attests class absence
only because Kubernetes materializes its own default numeric value/policy.
Neither priority nor Pod Identity role is read from campaign `pod_config`.

Projection-bound workers receive no static cloud credential file mounts from
the API server, including kubeconfig, AWS/GCP credentials, or logging-agent
credential files. Their only cloud/storage authority is the frozen Kubernetes
service account / Pod Identity plus the non-secret broker descriptor. Logging
may use workload identity, but adding a file-backed logging credential requires
a future explicit server-owned projection and cannot reuse the generic mount
path.

This suppression applies only to credentials implicitly inherited from the API
server. Stable service YAML, runtime secrets, and file mounts are trusted,
operator-owned immutable contents of the committed service version and may
deliberately contain model runtime material. Mutable campaign and replica-launch
inputs cannot add or alter those version-owned fields. Clin's prepared service
YAML is separately audited to contain no direct S3 or KMS credentials.

The workspace/context property `serve_worker_cache` may select `none` or refer
to a node-local registered volume and its attestation. For `node_local`, the
builder also verifies that the effective context `auto_mounts` contains the
exact volume/mount pair and that the registered volume is a context-matching
`k8s-hostpath`; the persisted host path comes from that registered volume, not
from campaign YAML. Platform admission and worker startup must additionally
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
accounts, priority classes, host paths, PVCs, storage classes, or cache
assertions. Server-owned projected cache/workspace volumes are applied only
after this rejection boundary.

## Storage and migration

Additive PostgreSQL migration 043 (down-revision 042) adds nullable JSONB columns
`version_specs.controller_job_projection`,
`version_specs.controller_work_cache`, and
`version_specs.worker_placement_projections`, plus
`version_specs.storage_broker`. Existing rows stay null. The
columns are retained during application rollback so a later forward deployment
can reuse immutable metadata. The Serve controller's separately supported
local SQLite topology may use SQLAlchemy JSON for tests and local operation;
there is no new central-API SQLite migration target.

## Implementation phases

1. Commit this canonical design and strict projection model/builder.
2. Add PostgreSQL version storage, immutable persistence, API protocol marker,
   and history fields with focused migration/state/API tests.
3. Keep external campaign-controller recovery and takeover unavailable; require
   a verified Clin fresh one-shot mode as an explicit rollout prerequisite.
4. Configure the dedicated east controller workspace and the inference
   workspace's controller auth Secret reference, per-context worker priorities,
   Pod Identity role ARNs, exact accelerator scheduling maps, explicit priority
   values/policies, and fully attested caches;
   update the stable service and verify all old replicas are gone.

## Deployment and rollback

Deploy the database migration before or with server/controller/load-balancer
binaries. Older binaries ignore additive columns. New service updates freeze
projections; old versions remain null and ineligible. Rollback never re-derives
or deletes projections. External campaign-controller takeover remains
unavailable before, during, and after this rollout; rollback cannot authorize
campaign replay.

API 76 must be running before the first seven-key configuration is exposed; a
deployment that already has the API-75 six-key configuration upgrades image and
configuration atomically. The initial Boltz rollout may deploy the image first
only after verifying that production has no broker configuration and no
non-null persisted broker row, then add the first seven-key configuration in a
later reviewed apply. API 75 rejects the seventh key, while API 76 requires it
for every new non-null broker projection. Before an API-76 version is
committed, rollback restores the absent or six-key configuration before or with
the API-75 image. After activation, API 75 cannot read a seven-key persisted
row. A rollback must therefore keep API 76 running, drain and remove every
service containing such a version, verify that no seven-key rows remain, and
only then restore the API-75 image and configuration. If preserving those
versions is required, roll forward with an API-76 fix instead. Never deploy the
old binary alone as a rollback.

## Verification

Automated tests must cover strict schemas, cross-context controller/worker
projection, deterministic candidate IDs, configuration precedence, incomplete
cache attestation, registered-volume/context/automount mismatch, packing
inequalities, exact context/accelerator/count catalog coverage, caller
`pod_config` and task-volume rejection, nullable legacy rows, immutable retries,
stored-only version history, suppression of all static worker credential
mounts, zero-live-capacity rendering without discovery, frozen GPU resource-key
provisioning, and confirmed cleanup of admitted identity/scheduling mismatches.

Deployment smoke tests must update a service with east A100/A100-80GB plus PHX
H200, then inspect admitted pods for the frozen service account, exact
accelerator affinity/resource key and count, and priority class (numeric
`-1000`, `preemptionPolicy: Never`). On every advertised
node-local candidate, verify `findmnt`, source, filesystem, free bytes/inodes,
and per-node packing before cold traffic. Restart the API and Serve controller,
change mutable server config, and prove replacement replicas retain the
committed projection. Confirm fresh campaign launch works, and confirm
external campaign-controller recovery/takeover remains blocked rather than
replaying ambiguous work.

## Open gates

- The distinct east controller workspace/context/namespace/service account is
  encoded in the platform change. Deploy it and verify the admitted controller
  Job's identity, 64 GiB ephemeral-storage request, 80 GiB limit, and
  `/mnt/scratch` byte/inode checks. Its ambient role needs the controller's
  S3/KMS permissions; worker service accounts must retain only broker-invoke
  authority.
- The broker descriptor must list the exact distinct east and PHX worker IAM
  role ARNs, and the broker must prove that either ambient principal can redeem
  only a grant bound to that finite set.
- Live PHX H200 and east probes supplied the filesystem, source, free
  byte/inode, reserve, and maximum-packing evidence now encoded by the platform
  change. After rollout, repeat the worker startup check on every admitted node;
  any mismatch must fail that worker rather than weaken the cache contract.
- The required inference priority class is encoded for both contexts. Verify
  admitted east and PHX workers resolve it to value `-1000` with
  `preemptionPolicy: Never` before bulk traffic.
- Configure the verified east A100/A100-80GB and PHX H200
  `serve_worker_accelerator_scheduling` entries. Render an H200 worker with no
  live PHX GPU nodes visible and prove no node, label, resource-key, autoscaler,
  or `CUSTOM_GPU_RESOURCE_KEY` lookup occurs; the Pod must still require
  `nvidia.com/gpu.product In [NVIDIA-H200]` and `nvidia.com/gpu: 1`.
- The dedicated inference workspace must be updated without disrupting the
  existing shared service fleet, or migrated during a planned drain.
- Do not enable nonempty campaigns until Clin ships and verifies a fresh
  one-shot mode independent of external campaign-controller recovery or
  takeover.
- A separately designed broker-validated S3 lease contract and coordinated
  Clin rollout remain required before enabling external campaign-controller
  recovery or takeover.
- The east+PHX deployment smoke remains required before enabling H200 bulk
  traffic.
