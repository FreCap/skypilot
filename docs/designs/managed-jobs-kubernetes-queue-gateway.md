# Managed jobs Kubernetes queue gateway

## Context

`sky/jobs/server/core.py` combines managed-job launch and controller lifecycle,
queue projection, cancellation and wait orchestration, and log, pool, and event
transport.  The module is 1,909 lines and imports both controller-level and
provider-level dependencies.

`queue_from_kubernetes_pod()` is a bounded provider gateway within that module.
It constructs a Kubernetes command runner for one controller pod, executes the
managed-job table protocol, decodes the response, and preserves the legacy
client-side `skip_finished` fallback.  Its caller is Kubernetes status
aggregation rather than the jobs API router or controller lifecycle paths.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
|---|---|---|---|---|---|---|
| Launch and controller lifecycle | jobs launch API | execution, controller utilities, storage, admin policy | controller and submission lifecycle | provisioning, upload, token, and submission failures | high, launches and file transfers | controller and launch features |
| Main queue and response projection | jobs queue API, debug dump, resource checker, wait | controller restart, gRPC, runner registry, workspaces, users | no durable state, but controller availability is mutated on refresh | controller unavailable, protocol fallback, invalid filters | high, dashboard polling and query count | filtering and API evolution |
| Kubernetes pod queue gateway | Kubernetes status aggregation | Kubernetes command runner, managed-job codegen and decoder | none | pod transport or remote command failure, legacy response shape | high fan-out across controller pods | Kubernetes transport compatibility |
| Cancellation and wait lifecycle | jobs cancel and wait APIs | controller state, runner registry, polling clock | cancellation and deadline lifecycle | races with completion, timeouts, controller failure | high, lifecycle ordering | cancellation and recovery fixes |
| Log, pool, and event transport | jobs logs, pool APIs, events API | backend log transport, Serve pool implementation, event projector | stream and pool request lifecycle | missing jobs, remote I/O, pool and event lookup failures | streaming and request latency | independent product surfaces |

## Decision

Move `queue_from_kubernetes_pod()` and its field projection constant into a
plain `sky.jobs.server.kubernetes_queue` module.  Keep direct aliases in
`sky.jobs.server.core`, which remains the stable facade and preserves existing
call sites without a wrapper frame.

This is a provider gateway, not a new adapter hierarchy.  A class, protocol,
registry, or dependency-injection layer would add variation that does not
exist.  Moving the full queue family is rejected because `queue_v2()` shares
controller restart behavior and established facade-level monkeypatch seams.
Moving the default runner is rejected because its registry bootstrap and class
identity make it a less isolated first extraction.

## Behavior contract

- Preserve the `sky.jobs.server.core.queue_from_kubernetes_pod` import path,
  signature, callable identity, and exception behavior through a direct alias.
- Preserve the exact Kubernetes `ClusterInfo` and command-runner construction.
- Preserve the selected managed-job fields and one remote command per call.
- Return structured controller responses without client-side filtering.
- For legacy list responses, preserve whole-job `skip_finished` filtering.
- Do not change controller restart, main queue, launch, cancellation, log,
  pool, event, database, config, or serialized data behavior.

## Milestones

1. Add characterization tests against the historical facade.
2. Move the gateway implementation without behavioral edits.
3. Add facade and source-identity checks and run the mapped suites.

## Test and CI plan

| Changed path | Covered behavior | Tests and checks |
|---|---|---|
| `sky/jobs/server/kubernetes_queue.py` | runner construction, codegen fields, command contract, decoding, legacy fallback, errors | `tests/unit_tests/test_sky/jobs/test_server_queue.py` |
| `sky/jobs/server/core.py` | stable facade and no wrapper | facade identity and normalized AST checks |
| `tests/unit_tests/test_sky/jobs/test_server_queue.py` | characterization and regression coverage | focused pytest file |
| `docs/designs/managed-jobs-kubernetes-queue-gateway.md` | canonical contract | docs path review and diff checks |

The Python Tests workflow has no pull-request path filter, and its Unit Tests
job collects the focused test file.  Run the focused jobs server, Kubernetes
status, debug utility, resource checker, and wait suites, then formatting,
static analysis, compile checks, and `git diff --check`.

## Performance and rollout

The direct facade alias adds no wrapper, request, subprocess, copy, or
allocation.  Characterization pins one code-generation call, one command
runner lookup, one remote execution, and one decode per invocation.  Compare
cold imports before and after the extraction.  The change is internal and can
roll back by moving the function and constant back into the facade.
