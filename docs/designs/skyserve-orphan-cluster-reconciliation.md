# SkyServe orphan cluster reconciliation

Status: Implemented; production repair remains gated by the v1 rollout
Last updated: 2026-08-11

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

1. `clusters.is_managed` is true and its cluster hash is non-empty, so the
   selected generation can be fenced through reconciliation.
2. `clusters.workload_type` is `service`.
3. No current `replicas.cluster_name` exactly equals `clusters.name`.

Exact replica-name presence remains the ownership fence in the discovery
snapshot. Active managed clusters with a replica row in that snapshot are
never added to the general sweep. A replica row may appear after discovery;
the non-destructive provider refresh and the existing cluster/resource locks
remain the actuation safety boundary for that separate race.

Candidate status refresh reuses the existing workspace selection, provider
query, per-cluster status lock, resource-operation lock, retry, and cleanup
path. It has these outcomes:

- Provider confirms the resource is absent: remove the current cluster row and
  close the open history interval through the existing teardown cleanup.
- Provider confirms the resource still exists: retain the row and refreshed
  status. Do not terminate it.
- Credentials, provider status, or locking are unavailable: retain the row and
  retry on a later sweep.
- The persisted handle has no cluster YAML, so the provider cannot be queried:
  retain the sweep-nominated ownerless managed service generation and its open
  usage interval for investigation. The existing cleanup behavior remains
  unchanged for every non-nominated row, including pools and managed jobs.

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

A missing cluster YAML is not provider-absence evidence. Although the ordinary
refresh path historically removes such incomplete cache rows, applying that
shortcut to the exact ownerless managed service generation nominated by the
sweep could discard the only tracking row for a live resource and close its
usage interval. The nomination carries the candidate cluster hash and is
honored only when the locked current row is still managed, still a service, and
still has that hash. All other callers retain the historical cleanup behavior.

## Integration

`global_user_state` exposes a plain-column query for managed clusters of one
workload type, including the stable cluster hash used as a generation fence.
`serve_state` exposes a plain-column snapshot of current exact replica cluster
names. `serve_utils` combines them only when Serve consolidation mode is
enabled. Rows without a non-empty hash remain outside the sweep and therefore
fail closed for operator investigation.

`backend_utils.refresh_cluster_records()` merges the returned candidate status
fields into its existing unmanaged snapshot and passes the generation-bound
nomination only for that candidate's refresh. Direct refresh, relaunch,
managed-job, pool, and owned-service callers never receive this authority.
Candidate-discovery failure is fault-isolated: it logs and preserves the
existing unmanaged sweep. Per-cluster refresh failure already returns an
`UNKNOWN` sentinel without aborting other clusters.

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
- one candidate refresh failure does not abort other candidates;
- a nominated ownerless managed service generation without a cluster YAML is
  retained; non-nominated managed jobs, pools, owned services, and ordinary
  user clusters keep the legacy no-YAML cleanup behavior; and a stale
  nomination cannot perform provider, event, or cleanup work on a same-name
  successor generation.

Run the focused unit tests, formatter and linters for changed Python files, and
the relevant broader backend and Serve unit-test slices.

## Verification evidence

The post-merge audit reproduced the pre-correction failure through the managed
job relaunch path: a managed no-YAML row was retained and reused. The corrected
path removes non-nominated managed-job rows before relaunch. Deterministic tests
also replace a nominated row after the initial full read with a same-hash job,
same-hash pool, different-hash service generation, and unmanaged successor;
none can reach provider, event, or cleanup work through the stale nomination.

Local component verification passed 670 tests. Candidate discovery remains one
SELECT at 10,000 rows, and each locked refresh remains one full read plus the
existing one plain-field SELECT. On the audit host, adding the generation fence
cost 6.05 ms for the 10,000-row bulk snapshot and 19.4 microseconds per cheap
refresh-field read; it adds no network/provider calls and no N+1 query.

The exact commands and exact-head CI evidence are recorded in the follow-up PR
linked to the audited PR. No billable cloud smoke launch is required for this
database/dispatch correction; the provider boundary is mocked with call-count
assertions, while CI runs the full unit and static suites.

## Open gates

The v1 production backup, targeted correction, deployment, and rollback
verification below remain operational gates. They are not completed by the
code-level reconciliation or its audit correction.

Manual production verification must prove:

- exact PR head, CI rollup, merge SHA, release chart, image digest, and commit;
- Helm upgrade used `--reuse-values` and the final pod is ready and healthy;
- all 85 target current rows are absent;
- none of the 85 target history rows has an open interval;
- rebuilt target spend stops at the evidence-backed cutoff;
- current OpenDDE Kubernetes replicas and unrelated AWS history are unchanged.
