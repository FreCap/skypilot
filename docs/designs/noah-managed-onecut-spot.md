# Noah one-cut workflow on Managed Jobs and Spot

## Problem

Noah's full one-cut pair and pose-RMSD workflow currently runs as two large,
homogeneous Ray clusters: 20 on-demand nodes for ChEMBL and 56 on-demand nodes
for GoSTAR. The programs already make each of 1,024 assay buckets durable with
a validated per-bucket manifest and `_SUCCESS` marker, but dynamic Ray
scheduling means a submitted job does not own a deterministic subset of those
buckets. A preemption of any node also makes the whole multi-node task the
recovery unit.

The shared SkyPilot control plane cannot currently run pools. Its configuration
does not explicitly enable Managed Jobs consolidation, and a stale stopped
jobs-controller database row prevents deploy-mode auto-detection from enabling
it. The provider instance for that row no longer exists, while 120 nonterminal
Managed Jobs remain in the central PostgreSQL database. Those rows must be
preserved and reconciled, not silently cancelled or discarded.

## Goals

1. Make the control plane's Managed Jobs mode explicit and restart-safe.
2. Preserve and recover existing Managed Job rows through the mode transition.
3. Give each one-cut shard deterministic, disjoint bucket ownership.
4. Run shards as independently recoverable, single-node Managed Jobs.
5. Prefer Spot for every worker and fall back to on-demand when Spot capacity
   cannot be provisioned.
6. Preserve the current unsharded program behavior for existing callers.
7. Prove the topology with an isolated 64-worker, small-machine experiment,
   including an intentional worker interruption and complete cleanup.
8. Add the missing, backward-compatible pool placement interface needed for
   independent Spot/on-demand worker selection.

## Non-goals

- Mixing an on-demand Ray head with Spot Ray workers in one task.
- Preserving the 20-node and 56-node gangs as the long-term failure boundary.
- Changing pair construction or RMSD scoring semantics.
- Mutating the active ChEMBL or GoSTAR output prefixes.
- Cancelling the 120 existing Managed Jobs as part of the migration.

## Existing behavior and invariants

- Pair construction derives a stable, sorted list of bucket specifications.
- A pair bucket is committed by its validated `_manifest.parquet` followed by
  `_SUCCESS`.
- RMSD prepare and score stages use the same per-bucket commit protocol.
- The pair program currently writes a source-global manifest and `_SUCCESS`
  after processing every bucket.
- The RMSD program requires the pair-global `_SUCCESS`, then writes its own
  source-global manifest and `_SUCCESS`.
- The joint finalizer commits the run root only after both source-global
  results validate.
- Durable inputs and outputs live in the approved dropzone S3 bucket. Local
  random-access work is staged under `/tmp`.

These commit boundaries remain authoritative. A shard may skip a bucket only
when the existing manifest validator accepts that bucket's committed output.

## Design

### 1. Control-plane consolidation and recovery

Add the following explicit server configuration to the canonical Rainier
control-plane Terragrunt input:

```yaml
jobs:
  controller:
    consolidation_mode: true
```

The configuration is seeded into the PostgreSQL-backed SkyPilot config. Restart
the API server after the value is present so the consolidation signal is
created during startup. The leader-elected Managed Job refresh thread then:

1. acquires the PostgreSQL consolidation lock;
2. resets controller-owned nonterminal rows to `WAITING` when their recorded
   controller process is not alive;
3. starts local controller processes; and
4. lets the scheduler reclaim the preserved jobs.

Before the restart, record the exact nonterminal job IDs, task statuses,
schedule states, and controller PIDs. Reconfirm that the stale external
jobs-controller cluster has no provider instance. Do not delete the controller
row before recovery. After adoption is proven, clean the stale cluster record
through the normal cluster reconciliation path or a narrowly scoped,
transactional database repair if normal reconciliation cannot remove it.

Success requires:

- exactly one API replica owns the consolidation lock;
- no existing nonterminal job becomes terminal solely because of the restart;
- every nonterminal row is either claimed by a live local controller or remains
  queued under the scheduler's bounded parallelism;
- no external jobs-controller VM is launched; and
- pool operations no longer fail the consolidation-mode gate.

### 2. Deterministic shard ownership

Both bucket runners gain optional `--shard-index` and `--num-shards`
arguments. They are valid only as a pair and enforce:

```text
0 <= shard_index < num_shards
```

The stable ownership rule is:

```text
bucket_id % num_shards == shard_index
```

The rule gives complete coverage and no overlap independent of discovery
order. Modulo assignment is preferred over contiguous ranges because bucket
runtimes and sizes vary. The pool jobs use the runners' local sequential mode:
one small VM spends its CPU and memory on one bucket at a time without
replacing SkyPilot's worker runtime dependencies. The legacy launchers retain
Ray execution. Worker scheduling is not part of the correctness contract.

An optional, sorted `--bucket-ids` selection restricts the expected bucket set.
It is applied before shard ownership and is passed unchanged to execution and
finalization. Unknown or duplicate IDs are rejected. This is intended for
bounded qualification runs, not as an implicit production default.

With no shard or bucket-selection arguments, both programs retain their current
all-bucket behavior.

### 3. Separate execution from global commit

Sharded execution never writes source-global manifests or success markers.
This prevents multiple Managed Jobs from racing on the same immutable S3
objects.

The pair runner supports:

- normal mode: process every selected bucket and, when unsharded, finalize as
  today;
- shard mode: process only the owned buckets and write only per-bucket commits;
- finalize-only mode: rebuild the complete expected bucket list, validate every
  per-bucket commit, then write the source-global pair manifest, summary, and
  `_SUCCESS`.

The RMSD runner supports the same split. Its finalize-only mode validates both
the expected pair count and every score-bucket commit before writing the
source-global RMSD manifest, summary, and `_SUCCESS`.

Pair finalization is a hard barrier before any RMSD shard starts. RMSD source
finalization is a hard barrier before the existing joint ChEMBL/GoSTAR
finalizer runs.

Failure records, if retained, are written under an attempt-specific path. No
worker writes a source-global `_FAILED` object that could poison a later
successful recovery.

### 4. Managed pool topology

Use one fixed-size or bounded autoscaling pool whose workers are single-node
clusters. The immutable code bundle and Python environment belong to the pool
setup. Pool job YAMLs contain only resource requirements and run commands.

SkyPilot currently rejects both ordered pool resources and every Spot placement
policy on a pool. Add an optional `pool.spot_placer` field. Initially accept
only `dynamic_fallback`, because `dynamic_fallback_per_gpu` changes replica
counts into logical GPU slots and is incompatible with the physical worker
count promised by a pool. Preserve this field through service-spec parsing,
serialization, updates, and controller restart. Existing pool YAMLs without
the field are unchanged.

Pools reject `resources.ordered`, and a plain resource set cannot directly mix
Spot and on-demand entries. Use one `resources.any_of` set containing:

1. `r6a.xlarge` Spot;
2. `r6i.xlarge` Spot;
3. `r6a.xlarge` on-demand;
4. `r6i.xlarge` on-demand.

Configure `pool.spot_placer: dynamic_fallback`. The placer ranks exact
locations by hourly cost, so the Spot locations precede the equivalent
on-demand locations. A capacity or quota failure benches only the failed exact
location and the next worker launch falls through to the next active
candidate. This avoids the `dynamic_ondemand_fallback` startup hedge, which can
temporarily provision both Spot and on-demand copies while Spot is still
becoming ready.

A worker may therefore fall back to on-demand without forcing other workers to
do so. Pool semantics already disable service cost rebalancing, so a
later-cheaper Spot location does not interrupt a running on-demand shard.
There is no multi-node Spot gang and no mixed-purchasing-model task. Pool
recovery replaces a preempted worker and the Managed Job replays its
deterministic shard, whose completed buckets are skipped by manifest
validation.

For a phase with `N` shards:

```bash
sky jobs launch --pool <pool> --num-jobs N <phase-job.yaml>
```

The job maps `SKYPILOT_JOB_RANK` to `shard_index` and
`SKYPILOT_NUM_JOBS` to `num_shards`.

### 5. Phase orchestration

The launcher creates a unique run ID and immutable code bundle, applies the
pool, and uses unique phase names. It submits and waits for these barriers:

1. ChEMBL and GoSTAR pair shard batches.
2. One pair finalizer per source.
3. ChEMBL and GoSTAR RMSD shard batches.
4. One RMSD source finalizer per source.
5. The existing joint finalizer.

Status polling uses structured `sky jobs queue --format json` output and
requires the expected job count. A phase advances only when every matching job
is `SUCCEEDED`; any terminal failure stops orchestration with the pool and
artifacts intact for diagnosis. Re-running the launcher with the same run ID is
idempotent because names and durable bucket commits are stable.

The launcher always prints the pool name, run root, code bundle URI, and phase
names. Automatic teardown is used for experiments. Production runs require an
explicit teardown decision after the joint commit succeeds.

## Alternatives considered

### One large Spot Ray cluster

This is the smallest launcher change, but it preserves the 20-node or 56-node
gang as one provisioning and recovery unit. One preemption restarts all work,
and on-demand fallback applies to the whole task. It does not use the existing
per-bucket durability to reduce the failure domain.

### On-demand head with Spot workers

A standard SkyPilot multi-node task selects one homogeneous resource candidate
for the entire task. Implementing a heterogeneous head/worker topology would be
a separate orchestration system. It is unnecessary once each shard is a
single-node Managed Job.

### Independent Managed Jobs without a pool

This avoids the pool control path but requires dozens of full cluster setup
cycles per phase and more fragile client-side submission bookkeeping. A pool
reuses the immutable environment across pair and RMSD phases and gives a
bounded worker count.

### Each shard writes a partial global manifest

This introduces merge races and makes S3 overwrite semantics part of
correctness. Central barrier finalizers are simpler and retain the current
global manifest contract.

## Compatibility and migration

- Existing invocations without shard or finalize-only flags behave as before.
- Existing bucket manifests remain valid and are reused.
- Existing global manifests keep their schema and location.
- No active production output prefix is modified by the experiment.
- The control-plane mode change is forward-only while nonterminal centralized
  jobs or pools exist. Disabling consolidation first would hide history and
  strand controllers.

## Test plan

### Unit tests

- shard argument validation;
- explicit bucket-selection validation;
- deterministic complete coverage for several bucket and shard counts;
- no overlap between shards;
- stable assignment under input reordering;
- shard execution does not write a global manifest or `_SUCCESS`;
- finalize-only rejects missing, malformed, wrong-build, wrong-source, and
  wrong-input bucket manifests;
- finalize-only accepts a complete set and produces the same global ordering
  and counts as unsharded execution;
- completed buckets are skipped and partial buckets are replayed;
- empty shards succeed without weakening global coverage checks.
- `pool.spot_placer` schema acceptance and invalid-policy rejection;
- service-spec parse/serialize/restart round trip for pool placement;
- mixed Spot/on-demand pool resources remain rejected without a placer and
  validate with `dynamic_fallback`;
- existing pools without placement policy preserve their serialized form;

### Control-plane verification

- capture the pre-rollout PostgreSQL job snapshot;
- deploy the explicit consolidation config and restart one API replica;
- verify Helm revision, image digest, pod readiness, migration revision, and
  consolidation-lock ownership;
- verify existing job IDs and statuses remain present;
- verify controller PIDs are local to the active API pod;
- create and tear down a one-worker smoke pool.

### 64-worker experiment

Use a unique, disposable S3 prefix and a 90-minute upper bound. Apply a
64-worker `r6a.xlarge`-class pool with Spot-first/on-demand fallback. Select 64
measured-small real buckets with distinct `bucket_id % 64` values and pass that
same explicit selection to every execution and finalization phase. Each rank
therefore owns exactly one real bucket, exercising the real pair and RMSD code
paths without attempting the full production corpus.

During the shard phase, terminate one exact experiment Spot instance after its
job reaches `RUNNING`. Verify that:

- the job enters `RECOVERING` and later `SUCCEEDED`;
- the replacement worker may use Spot or the ordered on-demand fallback;
- all 64 shard ranks reach `SUCCEEDED`;
- bucket ownership has complete coverage and no overlap;
- committed bucket outputs validate and no global object was written by a
  shard;
- both barrier finalizers succeed;
- repeated execution skips committed buckets;
- the pool, worker clusters, and provider instances are gone after teardown.

## Rollout and rollback

1. Land and deploy the explicit control-plane consolidation config.
2. Prove central job adoption and a one-worker pool before workload rollout.
3. Land the backward-compatible sharding and finalization code plus tests.
4. Run the isolated 64-worker experiment.
5. Keep the legacy launcher available until the experiment and one bounded
   production source complete.

If workload validation fails, tear down only the experiment pool and retain the
isolated prefix for diagnosis. The legacy on-demand gang launcher remains
unchanged. If control-plane adoption fails, roll back the API image while
keeping consolidation intent true, because changing modes with live centralized
rows would create a second continuity break.
