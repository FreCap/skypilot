# Kubernetes Autodown Reconciliation

## Problem

Kubernetes autodown is initiated by the skylet inside the cluster head pod.
The skylet writes its durable autostopping marker before running teardown hooks,
stopping Ray, and deleting the Kubernetes resources. The API server's periodic
cluster refresh observes that marker and records `AUTOSTOPPING`, but it only
observes subsequent provider state. If the skylet never reaches or completes
the Kubernetes delete, every refresh records `AUTOSTOPPING` again while the pod
continues consuming capacity.

The production incident that motivated this design had five Kubernetes
clusters in this state. All five database rows were `AUTOSTOPPING` with
`to_down=true`, while the Kubernetes provider view still reported their pods as
running. Four of those pods reserved one A100 each. The state was therefore a
physical resource leak, not only stale dashboard state.

An API server restart does not stop the skylet process in a workload pod, and
four affected clusters entered autodown after the earlier server restart. The
restart is therefore not the primary trigger. It does expose a recovery gap:
the restarted server resumes observation, but cannot complete an interrupted
autodown.

## Root cause

The production trigger is confirmed, and two code-level liveness gaps make it
persistent.

1. The affected workload skylets run as `skypilot-pool-sa`. In both affected
   namespaces, that service account receives `403 Forbidden` while listing
   pods, so each minute-level autodown retry fails before it can delete the
   cluster pod. The API server identity independently has list and delete
   permission in both namespaces, so it can safely act as the recovery
   authority for the persisted intent.
2. The API server refresh loop treats `AUTOSTOPPING` as observation-only. It
   never re-drives the idempotent Kubernetes termination, even after a durable
   Kubernetes autodown Event proves that hooks completed and teardown began, or
   after the recorded transition is too old to be an in-progress hook.
3. Independently, the new provisioner skylet path runs `ray stop` with
   `check=True` before it calls the provider's termination function. Ray is no
   longer an autoscaler authority for clouds using
   `ProvisionerVersion.SKYPILOT`, so the command is unnecessary. A nonzero
   result would create the same stuck state by preventing the Kubernetes delete
   from being attempted while leaving the skylet marker set. Removing this
   obsolete gate closes that additional pre-delete failure mode.

## Behavior contract

### Skylet

For clouds using the SkyPilot provisioner, the skylet must call the provider
stop or terminate operation without first running `ray stop`. Teardown hooks
remain ordered before provider deletion. For Kubernetes autodown, the durable
Kubernetes Event remains ordered after hooks and immediately before provider
deletion.

### API server reconciliation

The existing periodic cluster refresh is the reconciliation driver. It may
re-drive termination only when all of these conditions hold:

- the persisted cluster status at the start of the refresh is
  `AUTOSTOPPING`;
- the persisted intent is autodown (`autostop >= 0` and `to_down=true`);
- the cloud is Kubernetes;
- the provider still reports at least one cluster pod, without exceeding the
  handle's expected node count; and
- either a generation-scoped Kubernetes autodown Event exists, or the original
  `AUTOSTOPPING` transition is older than a conservative hook grace period.

The hook grace period is the larger of the default hook timeout and the sum of
all declared `down` hook timeouts, plus a five-minute scheduling and provider
buffer. This keeps normal hook execution under skylet authority. It also gives
older runtimes, which set the marker before `ray stop`, a bounded server-side
recovery path.

The reconciler calls the Kubernetes provisioner's idempotent
`terminate_instances` operation directly. It does not invoke the public
stop/down API, rerun user hooks, or remove the database row. Normal provider
observation on the next refresh performs post-teardown cleanup after no pods
remain.

Repeated reconciliation and a race with the skylet are safe. Kubernetes delete
requests are idempotent for this path, and 404 responses are treated as
success. A failed provider delete leaves the row intact so a later sweep can
retry.

The grace deadline comes from the generation-scoped cluster event whose ending
status is `AUTOSTOPPING`. It must not use `status_updated_at`, because that field
correctly advances whenever provider state is refreshed and represents cache
freshness rather than the original transition time.

### Keeping the grace anchor stable

The refresh loop both reads that transition event and writes it, so the anchor is
only as stable as the loop's own status decisions. Whenever a refresh demotes an
autodowning cluster to `UP` or `INIT`, the next sweep records a *new*
`AUTOSTOPPING` transition, and the deadline restarts from zero.

The skylet autostopping probe is the demotion path that matters. It answers over
gRPC or SSH from inside the head pod, and a transient failure there is exactly
the condition a stalled autodown produces. A probe that reports "not
autostopping" for a failed call is therefore indistinguishable from a genuine
negative, and a probe flapping more often than the grace period defers recovery
forever: the leaked pods this design exists to reclaim are never reclaimed.

The probe must report three outcomes: autostopping, not autostopping, and
unknown. On unknown, the refresh keeps the persisted status when the cluster is
already `AUTOSTOPPING` and the autostop intent is still armed (`autostop >= 0`).
This holds both the status and the anchor, and on the abnormal-cluster path it
also stops an unreachable skylet from disarming autostop, which would gate the
reconciler off permanently.

The hold is deliberately narrow. It never invents an autodown for a cluster that
is not already `AUTOSTOPPING`, a definitive probe answer always wins, and
cancelling autostop releases the hold on the next sweep. A cluster whose skylet
is permanently unreachable therefore stays `AUTOSTOPPING` until its autostop is
cancelled, relaunched, or the reconciler completes the teardown.

The anchor stays the *most recent* `AUTOSTOPPING` transition rather than the
generation's first. Using the first would let a second autodown episode in the
same generation inherit a long-expired deadline and terminate a pod while a
legitimate `down` hook is still running, which is the semantic regression the
Event-or-grace gate exists to prevent.

## Alternatives

### Only remove `ray stop`

This fixes newly launched or upgraded skylets, but it cannot recover the pods
that are already running the old code. It also leaves other interruptions
between marker creation and provider deletion unrecoverable.

### Invoke the public down API for each affected cluster

This treats the symptom operationally and depends on a user or external
automation. It can rerun higher-level teardown behavior and does not repair the
server's restart recovery contract.

### Re-drive every observed `AUTOSTOPPING` cluster immediately

This can delete a pod while a legitimate lifecycle hook is still running. The
Event-or-grace gate avoids that semantic regression.

### Add a new reconciliation daemon

The existing minute-level cluster refresh already owns provider-state
reconciliation and holds the per-cluster resource lock. A second daemon would
add scheduling, locking, and observability surface without improving recovery.

## Milestones

1. Remove the obsolete `ray stop` call from the new-provisioner skylet path.
2. Add the Kubernetes-only Event-or-grace reconciler to status refresh.
3. Add focused unit coverage for gating, idempotent re-drive, failure retry,
   event-time grace calculation, and the skylet provider-call ordering.

## Rollout and rollback

Deploy the API server change normally. No migration or configuration change is
required. The next background sweeps evaluate existing `AUTOSTOPPING` rows.
Rows with a durable Event reconcile immediately. Older rows without the Event
reconcile once their recorded transition exceeds the hook-aware grace period.

The change is Kubernetes-only. Rollback restores observation-only refresh; it
does not recreate already-deleted pods. Because reconciliation acts only on an
existing persisted autodown intent, rollback does not require data repair.

## Test plan

- Unit test that the new-provisioner skylet path reaches provider termination
  without invoking `ray stop`.
- Unit test that a fresh `AUTOSTOPPING` row without a Kubernetes Event is not
  re-driven.
- Unit test that a row with a generation-scoped Kubernetes Event is re-driven.
- Unit test that an old transition is re-driven after the declared-hook grace
  period.
- Unit test that non-Kubernetes, autostop-without-down, non-`AUTOSTOPPING`, and
  pod-absent records are not re-driven.
- Unit test that a termination failure propagates out of the per-cluster
  refresh, leaving the row available for the next fault-isolated sweep.
- Unit test that grace uses the original `AUTOSTOPPING` event time even when
  `status_updated_at` is newer.
- Unit test, against a real event log rather than a mocked lookup, that a
  transient skylet-probe failure inside the grace period does not re-anchor the
  deadline, and that the stalled autodown is still reconciled once the original
  deadline passes. The same test with no probe failure is the control.
- Unit test that the probe reports unknown for a failed gRPC call and for an
  empty or nonzero-exit SSH payload, while `is_definitely_autostopping` keeps
  its boolean contract.
- Unit test that an unknown probe holds only an armed autodown: it does not hold
  a cluster that is not `AUTOSTOPPING`, and it does not hold one whose autostop
  was cancelled.
- Unit test that the abnormal-cluster path does not disarm autostop when the
  skylet is unreachable.
- Run the focused pytest files, then run `bash format.sh --files` for every
  changed Python file.

## Manual verification

1. Launch a disposable Kubernetes cluster with `autostop.down: true` and a
   short idle timeout.
2. Before the idle deadline, temporarily make the in-pod termination call fail
   after the durable Kubernetes Event is emitted.
3. Verify that the dashboard first reports `AUTOSTOPPING` while the pod remains
   present.
4. Restore Kubernetes delete permission without calling the stop or down API.
5. Verify that the background API server refresh deletes the pod, cleans the
   cluster row on the following observation, and records no duplicate hook
   execution.
6. Repeat with an API server restart between steps 3 and 4 to verify that the
   persisted status and Event are sufficient for recovery.
