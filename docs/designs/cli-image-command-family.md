# CLI image command family

_Created: 2026-07-31_

## Problem

`sky/client/cli/command.py` is the stable root CLI facade and contains 6,955
lines. It owns root Click registration and help ordering, task construction,
cluster lifecycle, jobs and Serve orchestration, status presentation, storage
and volume commands, and the complete managed-container-image command family.

The image family is a distinct 169-line CLI adapter. Its `publish`, `status`,
`prepare`, `retry`, `profile qualify`, and `profile canary` commands translate
Click inputs into `sky.container_images` SDK calls and table output. No other
root command calls these handlers, and the handlers do not call root cluster,
job, Serve, status, or task-construction helpers.

The adjacent `dev` family is not a safe extraction target in this change. It
was introduced two days ago, changed again in the next commit, calls root
`status`, `stop`, and `down` commands, and has 36 test monkeypatch sites against
root-facade helpers. Moving it would require dependency plumbing or changed
late-bound patch behavior rather than a structural move.

## Goals

Move the complete image command family into one focused CLI module while
keeping `sky.client.cli.command` as the stable public facade. Preserve:

- root and nested Click command order, names, options, help, and callbacks;
- historical command object aliases and callback module and qualified names;
- image selector validation and error messages;
- SDK methods, arguments, call counts, and wait behavior;
- workspace callbacks, confirmation behavior, and table output;
- JSON qualification-manifest parsing and error translation; and
- import and runtime performance.

## Responsibility map

The root facade has six high-level responsibilities:

1. Root Click registration and help ordering. CLI invocation and completion
   callers depend on command insertion order and shared Click command classes.
   It owns registration state and fails through parsing, callback, or help
   drift. This is import-latency sensitive and changes with CLI navigation.
2. Task and DAG construction. Launch, exec, managed jobs, pools, and Serve
   callers depend on YAML, recipes, resources, secrets, and temporary files. It
   owns task mutation and fails through override or serialization drift. It
   changes with task-schema and launch behavior.
3. Cluster lifecycle orchestration. Launch, status, logs, stop, start, down,
   and development-environment callers depend on SDK requests, SSH config,
   confirmation, and controller lifecycle. It owns remote mutation and request
   ordering, with provider and network latency dominating performance.
4. Jobs, pools, and Serve orchestration. These command families depend on
   controller APIs, status fallback, logs, and remote lifecycle state. They own
   request and controller ordering and change with managed workload behavior.
5. Status and table presentation. Human and JSON callers depend on parallel
   reads, filtering, fallbacks, and formatting. It owns transient projections
   and latency-sensitive output contracts.
6. Managed-container-image CLI translation. Only the `sky image` Click tree
   calls it. It depends on container-image models and SDK methods, workspace
   selection, JSON files, confirmation, and image-specific table formatters. It
   owns no long-lived state. Its failures are ambiguous selectors, mutable
   references, unreadable qualification manifests, non-unique retry targets,
   and rejected billable canaries. It changes with the image control-plane API,
   independently of task, cluster, jobs, Serve, and status behavior.

The sixth responsibility is a stable seam. Its callers, dependencies, failure
modes, and change cadence differ materially from the root facade's other
responsibilities, and the full Click subtree can move as one unit.

## Solution

Add `sky.client.cli.images` and move the unchanged image Click group and its
seven nested command objects there. Use the existing `NaturalOrderGroup` and
`DocumentedCodeCommand` seams from `sky.client.cli.click_utils`.

Import the extracted module from `sky.client.cli.command`, expose direct aliases
for all eight historical command objects, restore every callback's historical
module metadata, and register the root image group at the same point between
storage and volumes. Keep the historical container-image module aliases so
late-bound module monkeypatches continue to affect the same module objects.

This is a facade-first plain-module extraction. It adds no wrapper, abstract
base class, registry, strategy, factory, dependency injection layer, or new
package hierarchy. Direct command aliases add no callback frame or SDK call.

## Alternatives considered

Leaving the family in place avoids one module, but keeps a complete product
adapter and its control-plane dependencies mixed into the root facade despite
an established precedent for extracted API, workspace, and GPU command
families. Moving only image formatting or retry selection would split one
cohesive command family and leave registration mixed. Extracting the newer dev
family would require root-command dependency injection and would change active
monkeypatch seams. A generic command registry or command factory would add a
concept without a second construction policy.

## Milestones and rollout

First add characterization tests on the unsplit facade. Then move the family
without behavioral edits and prove each callback body is unchanged. This is an
internal structural change with no database, config, API, or rollout migration.
Reverting the extraction restores the prior layout.

## Test plan

Characterization covers the root and nested command hierarchy, exact help
snapshots, historical callback metadata, version-stable AST body hashes, and
representative SDK projections for every command path, including both retry
branches. Final validation runs the focused image CLI tests, root CLI tests,
container-image client and model tests, formatter and static analysis, root and
subcommand help snapshots, import and identity probes, an alternating cold
import comparison, `git diff --check`, and the CI workflow path-to-test mapping.
