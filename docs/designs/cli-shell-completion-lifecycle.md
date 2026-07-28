# CLI shell completion lifecycle

_Created: 2026-07-28_

## Problem

`sky/client/cli/command.py` is the stable root CLI facade and currently contains
6,874 lines. It owns command registration and help ordering, task and DAG
construction, cluster and controller lifecycle commands, status presentation,
and the shell completion installation lifecycle. Shell completion installation
and removal mutate shell profile files by running shell-specific subprocesses.
Those dependencies, state, failure modes, and change cadence are independent of
normal SkyPilot command execution.

The broader task-construction family is not a safe extraction target in this
change. Its YAML helpers are shared with volume parsing and are patched through
the historical command facade. Moving that family directly would change those
late-bound patch points, while preserving them would require wrappers or
callback injection.

## Goals

Move the complete shell completion install and uninstall lifecycle into one
focused module while keeping `sky.client.cli.command` as the stable facade.
Preserve root command order, option names, Click callback identity, historical
function module and pickle identity, subprocess commands and call counts,
messages, exit behavior, and shell-specific reload guidance.

## Background and responsibility map

The root module has five high-level responsibilities:

1. Root command registration and help ordering. Click callers depend on the
   existing `cli` group and definition order. This owns Click command objects
   and fails through parsing or registration drift.
2. Task and DAG entrypoint construction. Launch, exec, managed jobs, pools, and
   Serve callers depend on YAML, recipes, resources, secrets, and workdir
   projection. It mutates task objects and temporary recipe files.
3. Cluster, jobs, Serve, storage, volume, and image command orchestration. These
   callers depend on SDK requests, streaming, controller lifecycle, and user
   confirmation. They own remote request and presentation failure modes.
4. Status, log, and table presentation. Human and JSON-output callers depend on
   formatting, filtering, and terminal output, with latency-sensitive remote
   reads.
5. Shell completion installation and removal. The root Click options are its
   only callers. It depends on shell detection, profile paths, shell commands,
   `subprocess.run`, and reload guidance. It owns no in-process state; its state
   is the completion files and shell profile entries. Its failures are missing
   shell detection, unsupported shells, subprocess errors, and duplicate or
   stale profile entries. It changes only when Click completion or supported
   shell behavior changes.

The fifth responsibility is a stable leaf because it has distinct callers,
dependencies, state, failure modes, and change cadence. It has no dependency on
the root command group, SDK, task construction, or other command handlers.

## Solution

Add `sky.client.cli.shell_completion` and move the two callbacks plus their
reload-command constants there unchanged. Import the module from
`sky.client.cli.command` and expose direct aliases under the historical private
names. Set the alias function metadata to the historical command module so
pickle identity and diagnostics remain stable. The existing root Click options
continue to reference those aliases at the same positions.

This is a facade-first plain-module extraction. It adds no wrapper, abstract
base class, registry, strategy, dependency injection layer, or package
hierarchy. Direct aliases add no call frame.

## Alternatives considered

Leaving the code in place avoids one module but keeps external shell-profile
mutation mixed with all runtime commands. Moving the whole task-construction
family would remove more lines but currently breaks historical facade patch
behavior. A shell adapter hierarchy has no concrete variation beyond simple
branches and would add carrying cost. Moving the callbacks into generic
`cli/utils.py` would make that module own external lifecycle side effects in
addition to pure CLI helpers.

## Milestones and rollout

First add characterization tests on the unsplit facade. Then perform the direct
move and prove the moved callback ASTs are unchanged apart from module
placement. This is an internal structural change with no migration or rollout
switch. Reverting the extraction restores the prior layout without changing
external state formats.

## Test plan

Characterization covers facade and pickle identity, root option callback
registration, bash, zsh, and fish install and uninstall subprocess commands,
single-call behavior, reload guidance, success messages, and the missing-shell
auto-detection failure. Final validation runs the focused CLI tests, root help
snapshot, import and identity probes, formatter, type and lint checks, a
production import-time comparison, `git diff --check`, and the CI workflow
path-to-test mapping.
