# SkyServe orphan cluster reconciliation

## Problem

The background cluster-status sweep deliberately excludes managed clusters
because their controllers own lifecycle decisions. That ownership boundary
breaks after both a SkyServe service row and its exact replica row are gone. A
terminated provider resource can then remain `INIT` or `UP` in the central
`clusters` table with an open `cluster_history.usage_intervals` entry. The
estimated-spend materializer continues charging that interval even though no
provider resource exists.

This occurred for 85 legacy OpenDDE AWS L4 rows. The current service
incarnation has different exact replica cluster names and runs on reserved
Kubernetes capacity, so matching by reusable service name would be unsafe.

## Behavior contract

In consolidated SkyServe deployments, the existing background cluster refresh
sweep also considers a managed cluster when all of the following hold:

1. `clusters.is_managed` is true.
2. `clusters.workload_type` is `service`.
3. No current `replicas.cluster_name` exactly equals `clusters.name`.

Exact replica-name presence remains the ownership fence. Active managed
clusters with a replica row are never added to the general sweep.

Candidate status refresh reuses the existing workspace selection, provider
query, per-cluster status lock, resource-operation lock, retry, and cleanup
path. It has these outcomes:

- Provider confirms the resource is absent: remove the current cluster row and
  close the open history interval through the existing teardown cleanup.
- Provider confirms the resource still exists: retain the row and refreshed
  status. Do not terminate it.
- Credentials, provider status, or locking are unavailable: retain the row and
  retry on a later sweep.

Non-consolidated SkyServe is unchanged. Its replica database lives on the
remote controller, so the API server cannot safely infer exact ownership.
Pools and managed jobs are outside this change.

## Data flow

```text
central clusters                    central replicas
managed + workload_type=service     exact cluster_name owners
              \                       /
               \----- set difference /
                         |
                         v
             ordinary cluster refresh
                         |
              provider status query
                   /           \
               absent          present/error
                 |                  |
       existing remove path       retain row
       closes usage interval
```

Replica launch persists its replica row before submitting `sky.launch`.
Normal replica and service teardown terminate the cluster before deleting the
replica row. The set difference therefore identifies broken or legacy
inventory, while the provider query remains the final authority for deletion.
The existing cluster and resource-operation locks protect races with a launch
or teardown that is already in progress.

## Integration

`global_user_state` exposes a plain-column query for managed clusters of one
workload type. `serve_state` exposes a plain-column snapshot of current exact
replica cluster names. `serve_utils` combines them only when Serve
consolidation mode is enabled.

`backend_utils.refresh_cluster_records()` merges the returned candidate status
fields into its existing unmanaged snapshot. Candidate-discovery failure is
fault-isolated: it logs and preserves the existing unmanaged sweep. Per-cluster
refresh failure already returns an `UNKNOWN` sentinel without aborting other
clusters.

No new daemon, schema, configuration, API, or teardown path is introduced.

## Alternatives

Automatically downing ownerless resources would repair actual cloud leaks but
is too destructive: a missing metadata row alone is not sufficient teardown
authority.

Refreshing every managed cluster would create competing lifecycle writers for
healthy services and pools.

Checking only for a missing service name would mishandle same-name successor
incarnations. Exact replica cluster names are the existing durable identities.

A one-time database correction would repair OpenDDE but would not close future
provider-absent remnants.

## Milestones

### v0: reconciliation

Add candidate queries, integrate them into the existing sweep, and cover
consolidation gating, exact ownership, managed-type filtering, candidate
inclusion, and discovery failure with unit tests.

### v1: production repair

Before deploying the reconciler, back up every ownerless candidate's cluster,
cluster-history, and estimated-spend rows with a digest. The deployment may
provider-refresh candidates outside OpenDDE, so the rollback inventory must
cover the full selected set. Deploy the exact merged image and chart with Helm
`--reuse-values`.

Then, in a target-only PostgreSQL transaction, cap open intervals at the
earliest timestamp that proves the predecessor service was absent, delete any
remaining OpenDDE target rows from `clusters`, and rebuild only those 85
cluster hashes in `estimated_spend_daily`. Current-service Kubernetes rows and
unrelated historical spend are excluded.

## Rollout and rollback

The code rollout is additive and fail-safe. Rollback restores the prior sweep,
leaving managed candidates untouched. It does not recreate removed cluster
rows or reopen history intervals.

The production rollout requires a JSON backup and SHA-256 digest for every
selected candidate before the new sweep runs. The manual data correction
remains limited to the 85 OpenDDE hashes. Rollback restores affected rows from
that backup and rebuilds the same target materialization. Validation must
compare exact hash sets, row counts, open intervals, and estimated cost before
and after.

## Test plan

Automated tests must prove:

- consolidation mode returns exact managed service candidates absent from the
  replica-name snapshot;
- owned service clusters, pools, managed jobs, and ordinary user clusters are
  not candidates;
- the ordinary sweep includes candidates but still excludes other managed
  clusters;
- candidate-discovery failure does not abort ordinary refresh;
- one candidate refresh failure does not abort other candidates.

Run the focused unit tests, formatter and linters for changed Python files, and
the relevant broader backend and Serve unit-test slices.

Manual production verification must prove:

- exact PR head, CI rollup, merge SHA, release chart, image digest, and commit;
- Helm upgrade used `--reuse-values` and the final pod is ready and healthy;
- all 85 target current rows are absent;
- none of the 85 target history rows has an open interval;
- rebuilt target spend stops at the evidence-backed cutoff;
- current OpenDDE Kubernetes replicas and unrelated AWS history are unchanged.
