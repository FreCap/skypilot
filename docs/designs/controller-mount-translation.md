# Controller Mount Translation Boundary

_Created: 2026-07-22_

## Problem

`sky/utils/controller_utils.py` is a stable facade for controller behavior, but
it also owns four implementation families with different callers and reasons
to change:

| Responsibility | Callers | Dependencies and state | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- |
| Controller identity and launch projection | CLI, SDK, Jobs, Serve, backend launch | Controller specs, user config, plugins, environment variables | Invalid controller names, incomplete config projection, incompatible runtime bootstrap | Import time and launch command generation | Controller UX, plugins, runtime setup |
| Controller resource selection | Jobs and Serve launch paths | Cloud and region selection, resource config, global cluster state | Invalid resources, incompatible cloud placement, stale controller state | Cloud lookup and launch latency | Placement and controller sizing |
| Mount translation and upload | Jobs and Serve submission paths | Task mount mutation, object stores, blob temp space, filesystem links, cleanup callback | Unsupported URLs, missing storage credentials, upload failure, leaked scoped storage, path collision | Upload call count, filesystem copies, task serialization | Storage providers, cleanup safety, file transport |
| Capacity and admission policy | API workers, Jobs, pools, Serve control loops | Consolidation signal, job and replica state, process memory, request caches | Over-admission, under-utilization, stale consolidation intent, expensive repeated scans | Request hot path, database scans, memory arithmetic | Autoscaling and API deployment policy |

The mount translation block is about 440 lines and depends on storage and
filesystem modules that the identity, resource, and capacity families do not
need. Its callers invoke two stable operations: either stage local mounts
through the controller, or translate and upload them through object storage.

## Goals

Move the complete mount translation implementation to a focused module while
preserving behavior, import paths, callable identities, task mutation order,
storage cleanup ownership, upload call counts, logging, and serialized
function identity.

## Solution

Add `sky.utils.controller_mount_translation` as a plain implementation module.
Move `_generate_run_uuid`, `translate_local_file_mounts_to_two_hop`, and
`maybe_translate_local_file_mounts_and_sync_up` without changing their control
flow. Keep `sky.utils.controller_utils` as the facade by binding its historical
names directly to the moved functions and assigning their historical
`__module__` value. A direct alias adds no call frame and lets existing Jobs
and Serve callers remain unchanged.

The extracted module owns the whole translation lifecycle: run identity,
first-hop task mutation, object-store selection, generated storage identities,
the pre-upload cleanup callback, upload, URI projection, and final task
mutation. Proxy-config rewriting remains in `controller_utils` because it is a
backend launch projection with a different caller and does not participate in
storage translation.

### Behavior contract

- Preserve deterministic mount ordering for an explicit `run_id`.
- Preserve all task mutations and callback ordering around
  `sync_storage_mounts`.
- Preserve shared-bucket subpath ownership and `force_delete` semantics.
- Preserve exception types and user-visible messages.
- Preserve `sky.utils.controller_utils` import, module, and pickle identities.
- Preserve one upload call and avoid additional filesystem copies or imports on
  the request hot path.

### Changed-path-to-test matrix

| Changed path | Local evidence | CI job |
| --- | --- | --- |
| `sky/utils/controller_mount_translation.py` | mount translation characterizations, normalized AST comparison, compile/import checks, Ruff, BasedPyright, mypy, Pylint | Python Tests - Unit Tests, Config Storage and Compatibility Tests, Limited Deps, format, mypy, pylint, ruff, basedpyright, import-linter |
| `sky/utils/controller_utils.py` | facade and pickle identity, controller-utils suite, Jobs and Serve callers, import timing | Same jobs plus Jobs and API Tests |
| `tests/unit_tests/test_controller_utils.py` | tests pass against unsplit base and extracted head | Python Tests - Unit Tests |
| `docs/designs/controller-mount-translation.md` | design review and `git diff --check` | format and static analysis |

## Alternatives considered

Leaving the code in place avoids churn but retains storage, blob, and
filesystem dependencies in a facade whose other responsibilities do not need
them. Splitting capacity policy instead would break existing late-bound test
and caller seams between `can_terminate`, `_get_request_parallelism`, and
related helpers unless wrappers or dependency plumbing were added. Moving
proxy-config rewriting with mount translation would combine backend launch
projection with storage transport, so it stays in the facade.

## Rollout and rollback

This is a structural extraction with no data or API migration. Reverting the
single commit restores the previous layout. Merge is gated on exact-head local
characterization, static checks, import timing, the full visible PR check
rollup, and no actionable review thread.
