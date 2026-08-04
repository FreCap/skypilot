# Serve Status Table Renderer Decomposition

_Created: 2026-07-29_

## Problem

`sky/serve/serve_utils.py` is 4,097 lines and owns several independently
changing responsibilities. It combines controller HTTP transport, lifecycle
fencing and recovery, task validation and workspace resolution, status
projection and wire encoding, teardown coordination, log streaming, CLI table
presentation, and remote command generation. Line count alone does not justify
a split, and most of those responsibilities share lifecycle state or
compatibility constraints.

The final status-table renderer is a bounded exception. Its callers need only
turn already-projected service and replica dictionaries into CLI text. It does
not own persistence, transport, lifecycle state, or remote execution.

## Responsibility Map

### Controller transport and owner fencing

Callers include server routes, controller proxying, service supervision, and
status reads. Dependencies include controller-owner snapshots, HTTP requests,
retry policy, IP normalization, and service status. It owns no durable state
but enforces incarnation identity. Failure modes include stale-owner routing,
unbounded waits, and cross-pod misrouting. This is latency-sensitive and
changes with HA and controller recovery.

### Lifecycle, validation, and recovery

Callers include service apply, update, teardown, managed-job pool recovery, and
replica management. Dependencies include file and database locks, lifecycle
epochs, configuration, task resources, workspaces, container images, and
process liveness. It owns lock handles and recovery ordering. Failure modes
include split ownership, leaked children, stale cleanup, and incompatible task
acceptance. This changes with lifecycle and recovery protocols.

### Status projection and wire compatibility

Callers include the CLI, server status endpoints, the dashboard, and controller
polling. Dependencies include service and replica persistence, cluster handles,
autoscaler HTTP responses, pickle and base64 compatibility, and legacy server
fallbacks. It owns the shape of status dictionaries and wire payloads. Failure
modes include excessive reads, incompatible old-client payloads, and incorrect
capacity summaries. This is query- and serialization-sensitive and changes
with API compatibility.

### Termination and log streaming

Callers include CLI and SDK termination and log commands, recovery cleanup, and
server handlers. Dependencies include lifecycle locks, request cancellation,
cluster teardown, log files, backends, and service-owner snapshots. It owns
termination ordering and active follow loops. Failure modes include deleting a
replacement service, leaked replicas, blank logs, and unbounded workers. This
changes with cleanup durability and remote execution.

### CLI status-table presentation

The public caller is `sky serve status` through
`sky.serve.format_service_table`; focused tests and the legacy
`sky.serve.serve_utils` path also call the private helpers. Dependencies are
`colorama`, table and readable-resource helpers, replica status classification,
and the legacy `CloudVmRayResourceHandle` fallback. It owns no persistent or
process state, but it intentionally mutates nested replica dictionaries to add
their service name before rendering. Failure modes include column drift,
changed truncation, lost ANSI status formatting, incorrect pool/service
differences, and breaking old-server handle fallback. It is not a controller
hot path and changes for CLI presentation or status-payload compatibility.

### Remote command generation

Server handlers use `ServeCodeGen` to generate version-gated Python snippets
for controller operations. It depends on the full `serve_utils` compatibility
surface, consolidation-mode configuration, shell quoting, user identity, and
the Serve protocol version. Its failure modes include old-controller
incompatibility, unsafe quoting, and wrong subprocess configuration. It changes
with the remote command protocol and remains in `serve_utils` because extracting
it would either create an import cycle or duplicate configuration policy.

## Solution

Move the presentation-only `_REPLICA_TRUNC_NUM`, `_get_replicas`,
`format_service_table`, and `_format_replica_table` to a plain module,
`sky/serve/serve_status_formatter.py`. Keep direct aliases in
`sky/serve/serve_utils.py`, including the functions' historical `__module__`,
so public imports, monkeypatch seams, function identity, and pickle lookup
continue to work without wrapper frames.

This is a facade-first module extraction. A class, protocol, strategy,
registry, or dependency-injection layer would add indirection without a second
implementation. The renderer continues to accept the existing dictionaries
and continues to use the existing SkyPilot table, resource, backend, and status
types.

The extraction is structural only. Input mutation, output bytes, column order,
truncation, old-server handle fallback, and exception behavior remain
unchanged.

## Alternatives Considered

Leaving the renderer in place avoids one file but preserves a presentation
responsibility in a lifecycle and transport module. The renderer has materially
different callers, dependencies, failure modes, and cadence, so the small
module cost is justified.

Extracting `ServeCodeGen` would remove a similarly cohesive block but requires
calling consolidation-mode policy still owned by `serve_utils`. Solving that
with callbacks, local imports, or duplicated configuration logic is riskier
than this leaf extraction.

Splitting controller transport, lifecycle recovery, status projection, or
termination in this pass is rejected. Those areas have active shared state,
performance constraints, and cross-responsibility ordering that require a
larger design.

## Test and Rollout Plan

Characterization tests run against the unchanged implementation before the
move. They pin service and pool output, full and truncated replica output,
authoritative and legacy replica counts, old-handle fallback, nested-record
mutation, facade metadata, and pickle round trips. After extraction, the same
tests must pass and must prove direct identity between the facade and the new
module.

The changed-path-to-test matrix is:

| Changed path | Responsibility | Tests |
| --- | --- | --- |
| `sky/serve/serve_status_formatter.py` | Status-table presentation | New contract tests, `test_serve_utils.py`, `test_serve_lazy_handle.py` |
| `sky/serve/serve_utils.py` | Historical facade | New identity and pickle tests, API compatibility tests |
| `docs/designs/serve-status-table-renderer.md` | Canonical design | Documentation and diff checks |

Run the focused tests, the broader Serve unit suite, `bash format.sh --files`
for both production modules and the contract test, `git diff --check`,
`compileall`, mypy, and Pylint. Inspect pull-request workflow filters and prove
that the Unit Tests job collects the new contract test.

Performance proof consists of identical direct function bindings, identical
formatted output for representative inputs, and a repeated renderer benchmark
against the exact base. The extracted facade adds no wrapper, allocation,
copy, query, lock, or provider call.

Publish one branch,
`codex/responsibility-split-serve-status-renderer`, and merge only after all
relevant checks and review threads are green on the exact pushed SHA.
