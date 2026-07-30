# Cloud VM Skylet Client Gateway

## Status

Implemented.

## Context

`sky/backends/cloud_vm_ray_backend.py` is 7,600 lines and contains the Cloud VM
provisioning and lifecycle orchestrator, resource-handle state, failover
policy, task execution, log transport, and the typed client for Skylet gRPC
services. File size is only a prioritization signal. This design extracts the
Skylet client because it has a complete boundary and independent consumers,
not merely because the containing module is large.

The existing `sky.backends.cloud_vm_ray_backend.SkyletClient` and
`sky.backends.SkyletClient` paths are public integration seams. Several
callers patch those paths in tests, and serialized references may contain the
historical class module. They must remain stable.

## Before responsibility map

### Typed Skylet RPC gateway

- Callers: Core job queue operations, Managed Jobs controllers and servers,
  Serve RPC utilities, backend health checks, autostop, and Cloud VM backend
  lifecycle methods.
- Dependencies: generated Autostop, Jobs, Serve, Managed Jobs, and Health gRPC
  stubs; Skylet timeout constants; Serve-specific admission deadlines; and the
  cancellable unary/streaming transport helpers.
- State owned: five per-channel gRPC stubs and the immutable set of streaming
  method names.
- Failure modes: calling the wrong service, dropping a timeout, prematurely
  ending a long Serve admission, failing to cancel an in-flight RPC, or
  treating a stream as unary.
- Performance sensitivity: construction is per channel or retry attempt and
  every method is a single direct delegation. The extraction must add no
  wrapper frame, copy, retry, query, or network call.
- Change cadence: proto service additions, RPC deadline policy, cancellation,
  and client/server compatibility.

### Cloud VM provisioning and failover

- Callers: launch, start, recovery, provisioning, and cluster reconciliation.
- Dependencies: cloud providers, zone failover, capacity caches, Ray cluster
  configuration, resource features, and persistent cluster state.
- State owned: provisioning attempts, blocked resources, cluster generations,
  retry histories, and resource handles.
- Failure modes: capacity storms, leaked infrastructure, stale generation
  writes, provider-specific failover errors, and partially configured clusters.
- Performance sensitivity: provider calls, SSH readiness, lock duration, and
  retry/backoff counts dominate.
- Change cadence: provider reliability, capacity policy, image placement,
  cluster locking, and provisioning lifecycle.

### Cloud VM execution and lifecycle orchestration

- Callers: SDK and CLI cluster operations, jobs, logs, storage, and teardown.
- Dependencies: resource handles, Skylet or SSH transport, task codegen,
  storage mounts, file synchronization, and global user state.
- State owned: runtime setup state, job metadata, log locations, storage
  metadata, and teardown ordering.
- Failure modes: remote-command drift, incorrect lifecycle ordering, lost
  logs, stale status, or incomplete cleanup.
- Performance sensitivity: remote command count, file transfers, database
  writes, and teardown latency.
- Change cadence: task execution, logging, storage, and lifecycle behavior.

## Chosen seam

Move `_CancelAwareStub` and `SkyletClient` to
`sky/backends/skylet_client.py`. Keep direct aliases in
`cloud_vm_ray_backend.py`, and continue exporting `SkyletClient` from
`sky.backends`.

`skylet_client.py` is a plain gateway module. It owns typed service
delegation and deadline policy while using the existing `backend_utils`
transport façade so late-bound cancellation helper patching remains intact.
The implementation classes retain
`sky.backends.cloud_vm_ray_backend` as their historical `__module__`, so
pickled and reflective identities continue resolving through the façade.

## Why this abstraction

A façade-first plain module is sufficient:

- An adapter accurately describes the client at the system boundary, but an
  adapter base class or protocol would add no value because there is one
  Skylet protocol and no second implementation.
- A strategy is inappropriate because RPC selection and deadlines are not
  interchangeable policies.
- A factory or builder is unnecessary because construction is one channel
  mapped to five generated stubs.
- A decorator would fragment cancellation across methods instead of preserving
  the existing shared proxy.
- Moving the client into `skylet_rpc.py` would mix typed service/deadline
  policy with generic cancellation and retry mechanics and would change the
  existing late-bound `backend_utils` patch seam.

The extraction moves the complete gateway rather than individual service
methods. Splitting by service would multiply construction and ownership
without independent lifecycles.

## Behavior contract

- `sky.backends.SkyletClient` and
  `sky.backends.cloud_vm_ray_backend.SkyletClient` remain the same class
  object.
- The class retains its historical module and pickle identity.
- Existing method names, signatures, default timeouts, response types, and
  delegation targets are unchanged.
- `TailLogs` remains the only streaming method.
- Serve termination, registration, and update deadlines remain unchanged.
- `_CancelAwareStub` continues resolving cancellable unary and streaming
  helpers through `backend_utils`.
- The Cloud VM backend keeps using its façade-local `SkyletClient` global so
  current monkeypatch sites remain valid.
- Imports remain lazy for grpcio and generated protobuf modules.

## Implementation milestones

1. Add characterization tests on the unchanged implementation.
2. Add `skylet_client.py` with the unchanged gateway implementation.
3. Replace the original definitions with direct façade aliases and remove
   imports used only by the moved code.
4. Run the focused and component test matrix, static tools, import checks, and
   performance comparison.

## Changed-path-to-test matrix

| Changed path / seam | Tests |
| --- | --- |
| `sky/backends/skylet_client.py` | `test_skylet_client_contract.py`, `test_skylet_grpc_cancellable.py` |
| `cloud_vm_ray_backend.py` façade and internal callers | `test_sky/backends/test_cloud_vm_ray_backend.py`, backend integration collection |
| Core and Managed Jobs callers | `test_core_job_queue.py`, `test_sky/jobs/test_server_core.py` |
| Serve deadline callers | `test_serve_terminate_transport.py`, `test_serve_terminate_validation.py` |
| health and cancellation transport | `test_skylet_health_service.py`, `test_skylet_grpc_cancellable.py` |
| import and serialization compatibility | `test_skylet_client_contract.py` |

## Validation evidence

The focused matrix collects and passes 144 tests across the gateway contract,
cancellable transport, Cloud VM backend, Core queue, Managed Jobs server,
Serve deadlines and validation, and Skylet health service. The Cloud VM
backend integration suite collects four parameterized live-cluster cases. A
paid live cluster run is not required for this pure extraction because the
wire requests, generated stubs, method bodies, and transport calls are
unchanged.

`format.sh --files` passes YAPF, isort, mypy over 767 source files, Pylint
10.00/10, dashboard lint, and dashboard formatting. Python 3.11 and 3.14
compile checks and both staged and working-tree `git diff --check` pass.

Six alternating cold imports of `sky.backends.cloud_vm_ray_backend` measured
a 0.952993-second base median and 0.946938-second extracted median, a favorable
0.635% delta within measurement noise. Direct façade aliases add no RPC frame,
copy, retry, query, or network call.

The pull-request workflows target `improvements` without changed-path filters.
The Unit Tests job collects the entire `tests/unit_tests` tree, while format,
mypy, Pylint, and static-analysis workflows cover the Python implementation.

## Rollout and rollback

This is a local structural extraction with no API, wire, schema, database,
configuration, or remote-command change. Normal client/server compatibility
applies because the same generated requests and stubs are used. Rollback is a
single commit revert.
