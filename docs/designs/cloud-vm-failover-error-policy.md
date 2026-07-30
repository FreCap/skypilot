# Cloud VM Failover Error Policy

## Status

Proposed.

## Context

`sky/backends/cloud_vm_ray_backend.py` is 7,002 lines and owns Cloud VM
provision retries, resource-handle state, task execution, teardown, log
transport, and provider-specific failure-to-blocklist policy. File size is
only a prioritization signal. This design extracts the failure policy because
it is a complete, stateless boundary with different dependencies, failure
modes, and reasons to change from the stateful retry orchestrator.

The policy has two compatibility generations. V1 parses Ray provisioner
stdout and stderr for clouds still using the legacy provisioner. V2 consumes
structured provisioner exceptions and maps provider-specific failures to the
narrowest safe resource, zone, region, or cloud block. Both generations
produce changes to a caller-owned set and neither performs provisioning,
cleanup, database access, retries, or network I/O.

## Before responsibility map

### Provider failure parsing and block-list projection

- Callers: `RetryingVmProvisioner._retry_zones`, focused failover tests, and
  the AWS failover smoke test through the historical backend symbols.
- Dependencies: provider exception shapes, Ray stdout and stderr, `Resources`
  blocking semantics, cloud region and zone models, Lambda service catalog,
  terminal formatting, and compatibility error messages.
- State owned: none. The policy mutates only the `blocked_resources` set
  supplied by the retry orchestrator.
- Failure modes: blocking too broadly, retrying a globally invalid request,
  missing a zone-specific capacity failure, treating a head request as never
  launched, hiding the rsync compatibility error, or losing diagnostic text.
- Performance sensitivity: failure-path CPU and string parsing only. The move
  must add no wrapper frame, resource copy, provider query, retry, or cleanup.
- Change cadence: provider error codes, cloud migrations from V1 to V2, and
  resource-blocking compatibility.

### Provision retry and lifecycle orchestration

- Callers: Cloud VM launch, start, recovery, and provider failover paths.
- Dependencies: optimizer candidates, zone iteration, capacity cache,
  provider provisioning, cluster config, global state, handles, cleanup,
  metrics, notifications, placement history, and progress UI.
- State owned: blocked resources, active resource candidate, attempt and
  failover histories, provision records, resource handles, cluster hashes,
  wheel state, and lifecycle ordering.
- Failure modes: leaked infrastructure, stale cluster state, invalid failover,
  repeated provider calls, incomplete cleanup, or partially configured
  clusters.
- Performance sensitivity: provider calls, SSH, teardown, locks, retry
  backoff, and config generation dominate.
- Change cadence: lifecycle, recovery, cleanup fencing, capacity caching,
  managed images, runtime setup, and cluster-state compatibility.

### Cluster resource handle and task execution

- Callers: SDK and CLI cluster operations, Managed Jobs, SkyServe, and backend
  lifecycle entrypoints.
- Dependencies: persisted and pickled handles, SSH and gRPC transport, job
  queue RPCs, log streaming, storage mounts, autostop, and teardown.
- State owned: serialized handle fields, cached cluster connection metadata,
  tunnels, task and job execution state, and storage metadata.
- Failure modes: serialization drift, stale connection data, broken remote
  commands, reordered setup or teardown, or leaked transport processes.
- Performance sensitivity: hot RPC, SSH, streaming, and remote-command paths.
- Change cadence: client-server compatibility, transport, jobs, storage, and
  cluster lifecycle.

## Chosen seam

Move `_add_to_blocked_resources`, `FailoverCloudErrorHandlerV1`, and
`FailoverCloudErrorHandlerV2` unchanged to
`sky/provision/failover_error_policy.py`. Keep direct aliases in
`sky.backends.cloud_vm_ray_backend` for all historical symbols. The helper,
classes, and static methods retain
`sky.backends.cloud_vm_ray_backend` as their historical `__module__`, so
reflection, monkeypatching, and pickle lookup continue through the facade.

Keep `GangSchedulingStatus`, `_ResourcesFeaturesUnsupportedError`, every retry
loop, and all mutable state in the backend. Keep the existing backend logger
name for moved diagnostics.

## Why this abstraction

A facade-first plain module is sufficient:

- A strategy would imply interchangeable failover algorithms. V1 and V2 are
  compatibility generations selected by the provisioner, not runtime
  strategies behind a new contract.
- An adapter would imply translation of a provider API. The policy consumes
  exceptions and resources already normalized by existing provisioners.
- A class hierarchy or registry would add dispatch and ownership that do not
  exist. Existing static classes and their dynamic provider method lookup are
  preserved exactly.
- Moving `RetryingVmProvisioner` would relocate a 1,465-line stateful
  orchestrator without separating responsibilities.
- Extracting only the GCP handler would fragment one dispatch family and leave
  shared blocking semantics split across modules.
- Moving the resource handle is a larger stateful change with serialization,
  transport, and import-cycle risk. It remains deferred.

The selected boundary moves the complete low-state policy leaf and leaves
orchestration, state, side effects, and lifecycle ordering in their current
owner.

## Behavior contract

- Historical backend symbols remain direct aliases, not wrappers.
- Callable signatures, return values, exceptions, logs, detailed reasons,
  historical `__module__`, qualified names, pickle identity, and monkeypatch
  behavior remain unchanged.
- V1 keeps exact stdout and stderr parsing and the conservative
  `definitely_no_nodes_launched` proof.
- V2 keeps exact provider dispatch and resource, zone, region, and cloud block
  widths.
- Duplicate or already-covered resources remain absent from the caller-owned
  block list.
- No provider, database, filesystem, metric, notification, retry, copy, or
  cleanup operation is added, removed, or reordered.

## Implementation milestones

1. Add characterization assertions for signatures, module and qualified-name
   identity, pickle identity, and class method ownership on the unchanged
   implementation.
2. Add `sky/provision/failover_error_policy.py` with unchanged bodies and the
   historical logger identity.
3. Replace the original definitions with direct facade aliases and remove only
   imports and constants no longer used by the backend.
4. Extend characterization to prove implementation-module and facade identity.
5. Run the focused and component matrix, static tools, import checks, AST
   equivalence, and alternating cold-import measurements.

## Changed-path-to-test matrix

| Changed path or seam | Tests |
| --- | --- |
| `sky/provision/failover_error_policy.py` dispatch and block width | `test_failover_classification.py`, `test_failover.py` collection |
| legacy stdout and stderr policy | characterization contract plus existing failover coverage |
| `cloud_vm_ray_backend.py` direct aliases and retry callers | `test_failover_classification.py`, `test_cloud_vm_ray_backend.py` |
| AWS and GCP structured error handling | `test_failover_classification.py`, `test_aws.py`, failover smoke-test collection |
| import and serialization compatibility | signature, direct identity, historical module, qualified name, and pickle assertions |

## Performance evidence plan

Compare alternating fresh-process imports of
`sky.backends.cloud_vm_ray_backend` before and after the extraction. Verify
that facade aliases are direct identities. Compare normalized ASTs for the
helper, both classes, and every static method against the exact base, proving
that no resource copy, provider call, dispatch, loop, or retry changed.

## CI mapping

Pull-request workflows targeting `improvements` have no relevant changed-path
filters. Unit Tests collects the focused contracts and backend tests. Failover
Tests covers provider retry behavior. Format, mypy, Pylint, BasedPyright,
import-linter, limited-dependency, and compile checks cover the new production
module and facade.

## Rollout and rollback

This is a local structural extraction with no CLI, API, wire, schema, database,
configuration, serialized-data, or remote-command change. Rollback is a
single commit revert.
