# Cloud VM resource handle serialization boundary

_Created: 2026-07-31_

## Problem

`CloudVmRayResourceHandle` is the durable cluster identity used by cluster,
Managed Jobs, Serve, SDK, CLI, and request-serialization paths.  Its 873-line
implementation currently combines live cluster connection behavior with four
versioned persistence hooks: dictionary encoding and decoding, pickle state
encoding, and thirteen generations of pickle migration.  Persistence changes
therefore require editing the same class body as IP refresh, command-runner
construction, SSH tunnel ownership, and gRPC channel setup.

The containing `cloud_vm_ray_backend.py` module is 6,606 lines and also owns
provision retry and task-execution orchestration.  Line count is only a
prioritization signal; the extraction is justified by the persistence hooks'
separate callers, dependencies, failure modes, and change cadence.

## Responsibility map

### Versioned serialization and migration

Callers are REST response encoders and decoders, global cluster-state
persistence, Serve replica transport, and Python pickle.  Dependencies are
`Resources` YAML projection, `ProvisionRuntimeMetadata`, historical handle
versions, Kubernetes context migration, and the handle's public refresh
methods.  It owns no independent state, but defines the durable representation
of every handle field.  Failures include unreadable old records, dropped
runtime flags, changed pickle identity, invalid JSON payloads, and accidental
provider refreshes.  The path is compatibility-sensitive rather than
latency-sensitive, and changes when durable fields or migration rules change.

### Live connection projection and transport

Callers are backend lifecycle operations, Core, Managed Jobs, Serve, and status
rendering.  Dependencies include provider cluster discovery, global state,
command runners, SSH credentials, locks, processes, sockets, and gRPC.  It owns
cached IPs, SSH ports, cluster info, Docker user state, and persisted tunnel
metadata.  Failures include stale addresses, cardinality mismatch, leaked SSH
tunnels, lock timeout, and unavailable remote commands.  Provider and network
calls are performance-sensitive, and changes track provisioning and transport
behavior.

### Provisioning and execution orchestration

Callers are launch, start, recovery, teardown, jobs, and task execution.
Dependencies include optimizer candidates, providers, capacity policy,
cluster configuration, mounts, runtime setup, logs, and persistent cluster
state.  It owns retry histories, provision records, lifecycle ordering, and
task execution state.  Failures include leaked infrastructure, stale recovery,
or reordered cleanup.  Provider, SSH, and remote-command call counts dominate,
and changes track lifecycle and execution features.

## Decision

Move the bodies of `to_dict`, `from_dict`, `__getstate__`, and `__setstate__`
to a plain `cloud_vm_resource_handle_serialization` module.  Attach those exact
function objects to `CloudVmRayResourceHandle` in its existing class body;
`from_dict` remains a classmethod.  The class, constructor, version constant,
live methods, and every public import remain in `cloud_vm_ray_backend.py`.

The implementation functions retain the historical module and qualified-name
metadata.  This is a facade-first plain-module boundary, not a wrapper: method
calls add no frame, copy, query, provider operation, or dispatch.  The helper
module may call public handle methods during old-version migration, but it does
not own the handle, cache, tunnel, or lifecycle.

## Behavior contract

- Preserve `CloudVmRayResourceHandle` and `LocalResourcesHandle` class identity,
  module path, pickle identity, constructor, `_VERSION`, and serialized fields.
- Preserve all four bound signatures, method names, module names, qualified
  names, descriptors, and direct callable identity through the historical
  class.
- Preserve dictionary defaults, resource reconstruction, runtime-metadata
  unknown-field filtering, and nonmutation of caller-owned nested resource
  dictionaries.
- Preserve pickle field projection and every version gate in `__setstate__`,
  including legacy IP and port refresh call counts and tolerated fetch errors.
- Preserve provider, database, filesystem, SSH, gRPC, lock, process, and copy
  behavior.  No live connection method moves.

## Alternatives considered

Moving the complete resource-handle class would remove more lines, but would
redirect method-global lookup away from the historical facade.  Existing tests
and extensions patch `_is_tunnel_healthy` through that facade, so the larger
move is not compatibility-safe without callback plumbing or a forwarding
layer.

Extracting SSH or gRPC behavior into a manager would introduce a second
lifecycle owner for cached and persisted tunnel state.  A mixin would add class
hierarchy without another implementation.  Keeping the file unchanged avoids
all migration risk, but leaves the stable persistence boundary mixed with live
transport code despite the availability of direct, wrapper-free method
attachment.

## Milestones and rollout

First add characterization tests against the current implementation and commit
them before moving behavior.  Then perform an AST-identical extraction, attach
the exact functions, and validate method and class compatibility.  This is an
internal structural change with no schema or feature rollout.  Reverting the
implementation commit restores the original class bodies without data
migration.

## Test and performance plan

Characterization covers dictionary round trips, caller-input immutability,
runtime-metadata filtering, pickle round trips, historical callable metadata,
and representative legacy state migration and refresh counts.  Focused backend,
request-serializer, global-state, Jobs, and Serve handle tests cover consumers.

Run repository formatting and static analysis, import and pickle probes,
AST-body equivalence against the pre-extraction commit, `git diff --check`, and
the focused component suites.  Compare alternating fresh-process `import sky`
timings.  Pull-request workflows have no relevant changed-path filter; Unit
Tests cover the new characterization and consumer suites, while format, mypy,
Pylint, Ruff, BasedPyright, import-linter, worker-floor-import, and runtime-stub
checks cover the production boundary.
