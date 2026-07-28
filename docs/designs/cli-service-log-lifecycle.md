# CLI service log lifecycle

_Created: 2026-07-28_

## Problem

`sky/client/cli/command.py` is the stable root CLI facade and contains 6,751
lines. It owns Click registration and help ordering, task construction,
cluster and controller lifecycle commands, status and table presentation, and
the shared Serve and managed-job pool log lifecycle.

The log lifecycle is invoked by both `sky serve logs` and
`sky jobs pool logs`, but it does not register either command. It independently
selects controller, load-balancer, replica, or worker targets; validates tail
and follow combinations; chooses streaming versus sync-down behavior; owns the
local download directory; dispatches to the Serve or managed-jobs SDK; and
translates controller availability failures for the CLI.

The broader status and table families are not safe extraction targets in this
change. They combine parallel remote requests, controller fallbacks, Click
output, JSON compatibility, historical facade patch points, and command-level
state. Moving them directly would change late-bound behavior, while preserving
it would require wrappers or dependency injection.

## Goals

Move the complete shared Serve and pool log lifecycle into one focused module
while keeping `sky.client.cli.command` as the stable CLI facade. Preserve:

- both Click commands, their order, names, options, help, and callbacks;
- the historical `_handle_serve_logs` import, module, and pickle identity;
- target-selection and validation behavior;
- SDK calls, arguments, call counts, and exception behavior;
- local directory names and creation ordering;
- spinner, warning, and completion messages; and
- import and runtime performance.

## Responsibility map

The root facade has five high-level responsibilities:

1. Root Click registration and help ordering. CLI invocation and completion
   callers depend on module definition order and Click command objects. It owns
   registration state and fails through parsing, callback, or help drift.
2. Task and DAG construction. Launch, exec, managed jobs, pools, and Serve
   callers depend on YAML, recipes, resources, secrets, and temporary files. It
   owns task mutation and fails through override or serialization drift.
3. Product command orchestration. Cluster, jobs, Serve, storage, volume, and
   image handlers depend on SDK requests, confirmations, streaming, and
   controller lifecycle. They own remote mutation and request ordering.
4. Status, queue, and table presentation. Human and JSON callers depend on
   parallel status reads, fallbacks, filtering, and formatting. They own
   transient result projection and latency-sensitive output contracts.
5. Shared service and pool log lifecycle. `serve_logs` and `jobs_pool_logs` are
   its only production callers. It depends on the Serve and managed-jobs SDKs,
   filesystem paths, timestamps, spinners, and terminal logging. It owns the
   local log directory and target-selection state. Its failures are invalid
   target combinations, incompatible tail and follow modes, download failures,
   and unavailable controllers. It changes with log component policy and SDK
   behavior rather than root CLI registration or task construction.

The fifth responsibility is a stable seam. Its two callers belong to
materially different product command groups, while the shared operation has a
single cohesive policy and side-effect boundary.

## Solution

Add `sky.client.cli.service_logs` and move `_handle_serve_logs` there unchanged.
Import the module from `sky.client.cli.command`, expose a direct alias under the
historical private name, and set the function metadata to the historical
command module. Initialize the extracted module's logger with the historical
logger name so diagnostics and monkeypatch behavior remain stable.

This is a facade-first plain-module extraction. It adds no wrapper, abstract
base class, registry, strategy, adapter hierarchy, dependency injection layer,
or package hierarchy. The direct alias adds no call frame. The Click-decorated
commands stay in the root facade and continue calling the same historical
global name.

## Alternatives considered

Leaving the helper in place avoids one module but keeps a complete
cross-product filesystem and transport lifecycle mixed into command
registration and orchestration. Moving the Click commands would disturb their
definition order and help output. Splitting target policy from SDK dispatch
would divide one operation and add indirection without an independent caller.
A strategy hierarchy for Serve versus pools is unnecessary because there are
only two direct SDK branches with the same operation contract.

## Milestones and rollout

First add characterization tests on the unsplit facade. Then move the helper
without behavioral edits and prove its AST is unchanged apart from module
placement and historical metadata. This is an internal structural change with
no migration or rollout switch. Reverting the extraction restores the prior
layout without changing remote or local data formats.

## Test plan

Characterization covers facade and pickle identity, logger identity, service
and pool default sync-down targets, local directory construction, SDK argument
projection, tail and follow interaction, single-target tailing, invalid target
combinations, and both Click help surfaces. Final validation runs the focused
CLI tests, adjacent Serve and jobs tests, formatter, type and lint checks, root
and subcommand help snapshots, import and identity probes, an alternating cold
import comparison, `git diff --check`, and the CI workflow path-to-test mapping.
