# CLI managed-jobs queue boundary

_Created: 2026-07-31_

## Problem

`sky/client/cli/command.py` is the stable root CLI facade and contains 6,765
lines. It owns root Click registration and help ordering, task construction,
cluster lifecycle, jobs and Serve orchestration, status presentation, storage
and volume commands, and managed-jobs queue input and output translation.

The managed-jobs queue responsibility is split across one request-result
handler near the root status command and the complete `sky jobs queue` command
near the jobs command group. Together they parse queue-specific status and time
filters, choose the API projection, fetch jobs and pool status concurrently,
decode legacy and v2 results, count active jobs, translate controller failures,
format queue tables or JSON, and print queue-specific hints. The root `status`
command is the only caller outside the `jobs queue` command, and it uses the
same result handler to render its managed-jobs subsection.

This responsibility has changed for queue v2, pool worker presentation, JSON
output, status filters, and time-range filters independently of task launch,
cluster lifecycle, storage, Serve, and most other managed-jobs commands. It has
no long-lived mutable state. Network requests dominate its runtime; local work
is bounded parsing, projection, and table formatting.

The adjacent jobs launch, cancel, logs, dashboard, and pool commands are not
safe extraction targets in this change. They own task construction,
confirmation, controller log lifecycles, cancellation ordering, and pool
mutation. Moving them with the queue would create a second mixed module rather
than isolate one responsibility.

## Goals

Move the complete managed-jobs queue input and output translation into one
focused CLI module while keeping `sky.client.cli.command` as the stable public
facade. Preserve:

- root and nested Click command order, names, options, help, and callbacks;
- historical command, helper, type, and constant import paths;
- callback and helper module and qualified names;
- legacy and v2 queue result handling, request call counts, and concurrency;
- status, time-window, user, limit, verbose, refresh, and JSON projections;
- controller fallback messages, usage accounting, table output, and hints;
- root `sky status` integration and existing monkeypatch points; and
- import, help-render, and representative result-processing performance.

## Responsibility map

The root facade has six high-level responsibilities:

1. Root Click registration and help ordering. CLI invocation, completion, tests,
   and extensions depend on insertion order, stable object identity, callback
   metadata, and historical import paths. It owns command-tree state and fails
   through parsing, help, or registration drift. This is import-latency
   sensitive and changes with CLI navigation.
2. Task and DAG construction. Launch, exec, managed-jobs launch, pools, and
   Serve callers depend on YAML, recipes, resources, secrets, and temporary
   files. It owns task mutation and fails through override or serialization
   drift. It changes with task schema and launch behavior.
3. Cluster lifecycle orchestration. Launch, status, logs, stop, start, down, and
   development-environment callers depend on SDK requests, SSH config,
   confirmation, and controller lifecycle. It owns remote mutation and request
   ordering; provider and network latency dominate performance.
4. Managed-jobs mutation and lifecycle. Launch, cancel, logs, dashboard, and
   pool callers depend on task construction, controller APIs, cancellation,
   log streaming, and pool mutation. They own remote lifecycle ordering and
   change with managed workload behavior.
5. Cross-product status presentation. The root status command combines cluster,
   workspace, cloud, managed-jobs, pools, and Serve results. It owns concurrent
   request coordination and section ordering, and fails through partial-result
   and output drift. It changes with the global status UX.
6. Managed-jobs queue translation. `sky jobs queue` and the managed-jobs section
   of `sky status` are its only callers. It depends on queue-specific CLI flags,
   `cli_utils.get_managed_job_queue`, `managed_jobs.pool_status`, legacy and v2
   response shapes, controller-status fallbacks, and the managed-jobs table
   formatter. It owns no long-lived state. Its failures are invalid filters,
   stopped or unreachable controllers, partial pool-status failure, response
   version drift, or output drift. It changes with queue API and presentation
   contracts and is sensitive to request concurrency, result size, and table
   formatting cost.

The sixth responsibility is a stable seam. Its two callers share one complete
projection and presentation policy, while its dependencies, failure modes,
state profile, and reasons to change differ materially from the root facade's
other responsibilities.

## Solution

Add `sky.client.cli.managed_jobs_queue` and move the unchanged queue constants,
`StatusList`, absolute-time parser, request-result handler, and `jobs_queue`
Click command there. Define `jobs_queue` as a standalone Click command in the
new module. Import it from `sky.client.cli.command`, expose direct aliases for
all historical symbols, restore callback and helper metadata, and register the
command on the existing `jobs` group at the same point between `launch` and
`cancel`.

The root status command continues to call the direct helper alias and use the
direct constant aliases. Both modules retain references to the same imported
SDK, utility, and formatter module objects so existing module-attribute
monkeypatches remain late-bound. No wrapper or duplicated request path is
introduced.

This is a facade-first plain-module extraction. It adds no abstract base class,
registry, strategy, factory, dependency injection layer, package hierarchy, or
parallel managed-jobs concept. Direct aliases add no callback frame, network
request, query, retry, copy, or serialization step.

## Alternatives considered

Leaving the code in place avoids one module, but keeps a complete, independently
evolving queue adapter and its controller fallback policy split across a
6,765-line facade. Moving only `jobs_queue` would leave its parser types,
constants, and shared result handler behind and would not establish clear
ownership. Moving the whole jobs group would combine task construction,
mutation, logs, pools, and presentation in a second large mixed module. A class,
protocol, strategy, registry, or factory would imply state or variation that
does not exist. Injecting the root `jobs` group into the new module would create
an unnecessary construction dependency; standalone command definition plus
existing-group registration is smaller.

## Milestones and rollout

First add characterization tests on the unsplit facade. Then move the queue
responsibility without behavioral edits and prove callback and helper bodies
are unchanged. This is an internal structural change with no database, config,
API, serialized-data, or rollout migration. Reverting the extraction restores
the prior layout.

## Test plan

Characterization covers nested command order and direct object identity, exact
help output, historical callback and helper metadata, version-stable AST body
hashes, all queue constants, status parsing, time parsing, SDK projections and
call counts, request concurrency, JSON and table output, controller fallback
branches, pool-status degradation, root status integration, and both import
orders. Final validation runs the focused managed-jobs queue and CLI helper
tests, root CLI and jobs tests, formatter and static analysis, help and identity
probes, an alternating cold-import comparison, a representative result-handler
timing comparison, `git diff --check`, and the CI workflow path-to-test mapping.
