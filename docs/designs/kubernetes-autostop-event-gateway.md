# Kubernetes autostop event gateway

## Context

`sky/provision/kubernetes/instance.py` is the stable Kubernetes provisioner
entrypoint. It currently owns live pod provisioning, readiness, teardown,
inventory, and diagnostics. It also owns a smaller cross-process protocol that
uses Kubernetes Events as durable breadcrumbs when skylet autodowns a cluster.

The breadcrumb protocol has materially different callers and failure handling
from instance lifecycle operations. Skylet writes the event immediately before
teardown, while server-side status refresh reads it after the pods may already
be gone. Both operations are deliberately best effort and have no in-process
state.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Pod bootstrap and readiness | Kubernetes provisioner | pod APIs, command runners, runtime setup | transient pod and retry state | init failure, timeout, unhealthy containers | launch latency and API call count | runtime and image setup |
| Pod construction and scheduling | `run_instances`, `wait_instances` | pod specs, scheduling facade, volumes, autoscaler events | transient launch batch | capacity, binding, naming, and scheduling failures | launch latency and polling count | Kubernetes placement behavior |
| Teardown and resource cleanup | stop, terminate, and cleanup entrypoints | pod, deployment, service, and extension APIs | transient deletion batch | finalizers, API errors, unsafe force deletion | teardown latency and parallel call count | lifecycle and cleanup policy |
| Inventory and status diagnostics | backend refresh and cluster info callers | pod, node, event, and host-network APIs | transient query projections plus cluster-event writes | missing pods, partial API failure, stale events | status latency and query count | observability and health semantics |
| Autostop breadcrumb gateway | skylet writer and backend status reader | Kubernetes Event API and provider namespace/context lookup | per-call event projection only | all API and payload failures degrade to no breadcrumb | one write per autodown and one bounded read on terminal refresh | autostop attribution protocol |
| Command-runner projection | backend and SSH provisioner callers | pod inventory and command-runner constructors | returned runner objects | missing cluster metadata | status and command startup latency | transport configuration |

## Behavior contract

- Keep `AUTOSTOP_EVENT_REASON`, `emit_autostop_event_best_effort`, and
  `get_cluster_autostop_event` importable from
  `sky.provision.kubernetes.instance` with their existing signatures, module
  identities, and pickle behavior.
- Preserve the exact Kubernetes Event payload, head-pod naming, field selector,
  timestamp selection, `since` filtering, API timeout, logging namespace, and
  broad best-effort exception boundary.
- Add no wrapper call frame, Kubernetes request, retry, copy, or persistent
  cache.
- Make no change to the external Kubernetes Event or cluster-event formats.

## Chosen seam

Move the constant and the writer/reader functions to a plain
`sky.provision.kubernetes.autostop_events` module. Keep direct aliases in the
historical `instance` facade and assign the historical module identity there.
The new module is a gateway because it owns translation between the internal
autostop attribution contract and the external Kubernetes Event resource.

A separate adapter hierarchy is unnecessary because there is one external API
and no competing implementation. Observer and strategy patterns do not fit:
there is no fan-out and no algorithm selection. Leaving the code in `instance`
would keep a cross-process attribution protocol coupled to live instance
lifecycle code despite its distinct callers and reasons to change.

## Milestones

1. Characterize the existing writer, reader, and historical facade.
2. Move the implementation without changing function bodies.
3. Prove direct facade identity, preserved serialization, exact AST bodies, and
   unchanged Kubernetes API call counts.

## Test and CI plan

- Run the autostop event unit tests against the unsplit base before movement.
- Run the final autostop tests plus focused backend cleanup and skylet autostop
  tests that exercise both callers.
- Run `bash format.sh --files` for every changed Python file, compile the moved
  modules, and run `git diff --check`.
- Measure alternating cold imports of the instance facade before and after the
  extraction. The extraction must remain within measurement noise and add no
  Kubernetes API operations.
- The `Python Tests - Unit Tests` CI matrix includes the characterization file
  and has no relevant path filter. Format, static-analysis, and Pylint workflows
  also run for the changed production paths.

## Rollout

This is a structural extraction with no feature flag or data migration. Revert
the single change if identity, import, or event-protocol behavior differs.
