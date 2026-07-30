# First-Class Development Environments

Status: accepted for implementation

## Summary

SkyPilot will add a first-class `sky dev` command group for provisioning,
reconciling, connecting to, stopping, and deleting interactive development
environments.

A development environment is a client-side projection over one ordinary,
named, single-node SkyPilot cluster. It is not a new server resource. The
existing cluster record remains the durable source of truth, and the existing
launch, optimizer, workdir synchronization, setup, storage, workspace, RBAC,
autostop, SSH, stop, and down paths remain authoritative.

The first version adds:

- `sky dev up`;
- `sky dev open`;
- `sky dev ssh`;
- `sky dev status`;
- `sky dev stop`;
- `sky dev down`;
- a client-only `dev` manifest section for the environment name, editor, and
  remote path.

It does not add an API endpoint, request type, database table, controller,
scheduler path, or API version.

## Motivation and prior art

dstack's development environments are more mature as a product seam. A typed
development manifest is applied through one lifecycle, and the result includes
direct SSH and desktop editor connection hints. Underneath, dstack still uses
its ordinary run and job infrastructure.

SkyPilot already has most of the stronger substrate:

- named clusters across clouds and Kubernetes;
- optimizer and resource failover;
- local and Git workdirs;
- repeatable workdir and file-mount synchronization;
- setup commands;
- storage and volume mounts;
- workspace authorization;
- SSH aliases;
- autostop and autodown;
- a fast launch path that retains synchronization while skipping unnecessary
  provisioning and setup for an already-UP cluster.

The useful concept to port is the coherent lifecycle and connection UX. A
second development resource model would duplicate stronger existing behavior
and create compatibility debt.

## Goals

1. Provision or reconcile an interactive environment with one command.
2. Make repeated source synchronization fast without rerunning setup by
   default on an already-UP cluster.
3. Make direct SSH and editor connection discoverable and safe.
4. Preserve all existing cluster, resource, workdir, workspace, RBAC, and
   autostop semantics.
5. Work with old API servers that already support the underlying cluster
   operations.
6. Keep the feature additive and client-only.
7. Never destroy or recreate an existing cluster implicitly.

## Non-goals

- A development-environment database row, API endpoint, request type,
  controller, scheduler, or dashboard resource.
- A second resource, volume, port, image, retry, or cloud-selection schema.
- A source patch store, repository uploader, or continuous source watcher.
- Automatic IDE server installation or ipykernel mutation.
- Automatic recovery or recreation after interruption.
- Schedules or fleet placement aliases.
- Metrics, utilization collection, Datadog integration, or a new inactivity
  policy.
- Multi-node, DAG, service, pool, or task-run development environments in v1.
- Persisting editor preferences on the server.

## User contract

### Manifest

`sky dev up` accepts one UTF-8 YAML document through `--file` / `-f`. The
document is an ordinary single-task SkyPilot YAML plus one client-only `dev`
mapping:

```yaml
dev:
  version: 1
  name: gpu-dev
  ide: cursor
  remote_path: ~/sky_workdir

workdir: .
resources:
  accelerators: L4:1
  autostop:
    idle_minutes: 120
    down: true
setup: |
  uv sync
```

The v1 `dev` schema is closed:

| Field | Required | Contract |
| --- | --- | --- |
| `version` | yes | integer literal `1` |
| `name` | yes | existing SkyPilot cluster-name validation |
| `ide` | no | `vscode`, `cursor`, `windsurf`, `zed`, or `none`; default `vscode` |
| `remote_path` | no | absolute or home-relative remote path; default `~/sky_workdir` |

Unknown keys, duplicate YAML keys, multiple YAML documents, non-mapping
documents, files larger than 1 MiB, and control characters in client-only
strings are rejected before any API request.

After validation, the client removes `dev` and parses the remaining mapping
with the existing `Task` parser. The client defaults an omitted `workdir` to
`.`. Relative workdirs retain the same current-working-directory semantics as
`sky launch`.

The stripped manifest, not the original manifest, becomes the Task's
user-specified YAML. Consequently, `dev` is absent from validation and launch
payloads and is never sent to or persisted by the API server.

The file loader preserves the ordinary YAML-directory Git commit metadata when
the Task does not already have workdir-derived Git metadata.

V1 rejects:

- a top-level `run` key, including an empty or null value;
- a top-level `service` key, including pool configuration;
- more than one YAML document or task;
- `num_nodes` other than `1`.

Initialization belongs in `setup`. The development environment is kept alive
by the cluster lifecycle, not by a submitted no-op job.

### Commands

```bash
# Provision or reconcile, sync the workdir, and print connection hints.
sky dev up -f dev.yaml

# Re-run setup even when the cluster is already UP.
sky dev up -f dev.yaml --reconfigure

# Open the configured local editor after reconciliation.
sky dev up -f dev.yaml --open

# Print or open a connection URI for an existing named cluster.
sky dev open gpu-dev --ide cursor --remote-path '~/sky_workdir'
sky dev open gpu-dev --ide cursor --print-only

# Start native interactive SSH and propagate its exit status.
sky dev ssh gpu-dev

# Reuse ordinary cluster lifecycle behavior.
sky dev status gpu-dev
sky dev stop gpu-dev
sky dev down gpu-dev
```

`up` supports the ordinary `--workspace` selection and `--yes` confirmation
behavior. The file is required in v1. The other commands use an explicit
cluster name and do not require the original manifest because there is no
development registry.

### `up` behavior

`sky dev up`:

1. Reads, bounds, parses, and validates the manifest on the client.
2. Warns when any Task resource alternative lacks enabled ordinary autostop.
   This includes explicit `autostop: false`. It does not invent an idle
   timeout.
3. Calls the existing launch SDK with:
   - the stripped Task;
   - `cluster_name=dev.name`;
   - `fast=True` by default;
   - `fast=False` with `--reconfigure`;
   - `_include_credentials=True`;
   - ordinary `--config` overrides embedded in the Task;
   - the ordinary confirmation and active-workspace behavior.
4. Accepts the current three-element launch result when credentials are
   bundled and the legacy two-element result from old servers.
5. Writes the ordinary SkyPilot SSH config entry directly from bundled
   credentials, or falls back to the existing status-with-credentials path.
6. If the existing detached-setup path returns its setup-carrier job ID, tails
   that job to a terminal state before reporting readiness. A job ID returned
   when the Task has no setup is an invariant violation.
7. Prints `ssh <name>` and the safely encoded editor URI unless `ide: none`.
8. Invokes the local operating-system URL opener only when `--open` is passed.

For an already-UP matching cluster, the existing fast launch path still syncs
the workdir and file mounts but skips unnecessary provisioning and setup.
`--reconfigure` uses the ordinary non-fast path so setup runs again.

Provider-owned runtimes remain authoritative when their
`ProvisionRuntimeMetadata` explicitly declares workdir synchronization or
setup complete. This is the existing cross-provider contract: ordinary VM and
Kubernetes runtimes perform repeated sync and reconfiguration, while a custom
provider that owns one of those phases also owns its reconciliation semantics.
`sky dev` does not override that metadata.

For a stopped, initializing, or absent cluster, the existing launch behavior
remains authoritative. Resource mismatch, ownership, RBAC, quota, and provider
errors propagate unchanged. The command never calls `down` or recreates a
cluster automatically.

### Connection behavior

`open` and `ssh` first refresh the named cluster's ordinary SSH config through
the existing status-with-credentials path. They require exactly one visible,
UP cluster. Missing, inaccessible, stopped, or initializing clusters fail
without invoking an editor or SSH.

Editor URI mappings are:

| Editor | URI |
| --- | --- |
| VS Code | `vscode://vscode-remote/ssh-remote+<name><path>` |
| Cursor | `cursor://vscode-remote/ssh-remote+<name><path>` |
| Windsurf | `windsurf://vscode-remote/ssh-remote+<name><path>` |
| Zed | `zed://ssh/<name><path>` |

Editor protocols require an absolute remote path. If `remote_path` begins with
`~/`, the CLI resolves the remote home after writing the SSH config by running
one fixed, non-interactive command through the generated alias. The command is
not derived from manifest input. Its output must be one bounded absolute POSIX
path. A resolution failure leaves the cluster running and asks the user to
retry with an explicit absolute `--remote-path`.

The cluster name and resolved remote path are URL-encoded by component. Path
separators remain path syntax; spaces, fragments, queries, percent signs, and
other reserved data are encoded. Credentials, proxy commands, identity paths,
tokens, and manifest contents never appear in a URI.

`--print-only` is the default-safe automation path for `open`. Without it,
`open` prints the URI and then asks the existing platform-aware URL opener to
open it. An opener failure produces an actionable error while leaving the URI
visible.

`ssh` starts `ssh <name>` through the established argv-only subprocess path,
waits for it, and propagates its exit status through Click. It never constructs
a shell string. This preserves native terminal, signal, SSH configuration, and
exit behavior while still allowing SkyPilot's entrypoint cleanup and
privacy-safe usage message to flush.

### Lifecycle wrappers

`status`, `stop`, and `down` delegate to the existing root CLI command objects
with one explicit cluster. This preserves formatting, status refresh behavior,
confirmation, graceful handling, request streaming, error classification, and
SSH-config cleanup. V1 does not duplicate these implementations.

## Implementation

### Pure client model

Add `sky/client/dev.py` with:

- a closed editor enum;
- an immutable v1 manifest model;
- bounded one-document parsing and Task extraction;
- validation for unsupported Task shapes;
- autostop detection;
- pure editor URI construction.

This module does not import Click and does not call the network, subprocesses,
or the OS opener. It is directly unit-testable.

### CLI integration

Register a natural-order `dev` group in `sky/client/cli/command.py` after the
ordinary cluster lifecycle commands are defined. Keeping the thin wrappers at
that seam lets Click invoke the established `status`, `stop`, and `down`
command objects without a circular import or a second formatting layer.

The existing SSH-config helpers remain the single implementation for launch,
status, `dev up`, `dev open`, and `dev ssh`. No compatibility aliases or
module identities are changed.

`up` applies exposed `--config` overrides to the Task's existing
`_cluster_config_overrides` field, matching root `sky launch`. It records
privacy-safe usage metadata from the stripped Task YAML only, never from the
original manifest containing `dev`.

### Compatibility

The server sees an ordinary `sky.launch` request containing an ordinary Task.
No API version bump or new minimum server version is needed.

The current launch-credential response is an optimization. A legacy two-item
launch response falls back to the established status credential fetch. The
feature must remain usable when that optimization is unavailable.

Because `sky dev` is a new root command and the Task sent to the server is
unchanged, old clients and old servers are unaffected. A server-side admin
policy may modify the Task or reject the request exactly as it can for
`sky launch`.

## Failure and safety behavior

- Parsing and client-only validation happen before server discovery or
  mutation.
- Client-only syntax and `dev` validation errors never echo source excerpts or
  values. Validation of the stripped Task retains the existing `sky launch`
  error contract.
- Editor opening occurs only after a successful launch and SSH-config update.
- `--open` with `ide: none` fails before launch.
- A launch response containing a job ID without Task setup is treated as an
  invariant violation. When setup is present, the ID belongs to SkyPilot's
  existing detached setup carrier and is tailed before readiness. A failed
  setup carrier leaves the environment running for inspection. It is never
  canceled or deleted automatically.
- A URI opener failure does not alter the cluster.
- `stop` and `down` retain their existing confirmations and failure behavior.
- No automatic cluster replacement is attempted after a resource mismatch.

## Alternatives considered

### A server-side development resource

Rejected for v1. It would duplicate cluster state, authorization, scheduling,
and cleanup while adding no required runtime behavior.

### A long-running no-op job

Rejected. It distorts job status and autostop, and cluster lifecycle already
provides the durable environment anchor.

### A separate source uploader or patch store

Rejected. SkyPilot workdir synchronization and Git workdirs are more general
and already handle repeated reconciliation.

### Automatically install editor servers

Rejected. Desktop editors negotiate their own remote versions. Setup remains
explicit and user-controlled.

### Persist the manifest or editor preference

Rejected. The cluster is the only durable runtime state needed for v1.
Explicit-name connection commands work without a manifest, and server
persistence would require a new compatibility contract.

### Invoke root commands through shell subprocesses

Rejected. In-process Click delegation and argv-only SSH preserve configuration,
errors, terminal behavior, and tests without quoting or recursion hazards.

## Test plan

### Pure unit tests

1. Accept a minimal manifest and apply editor/path/workdir defaults.
2. Reject missing or unsupported versions, missing/invalid names, unsupported
   editors, unknown dev keys, control characters, duplicate keys, oversized
   input, multiple documents, and non-mapping YAML. Client-only validation
   errors do not echo source values or unknown field names.
3. Reject `run`, `service`, DAG-like documents, and multi-node Tasks.
4. Verify ordinary Task fields survive extraction and `dev` is absent from the
   Task's serialized and user-specified YAML.
5. Verify file-based loading preserves ordinary YAML-directory Git metadata
   when workdir metadata is absent.
6. Verify all editor URIs, including spaces and reserved characters.
7. Verify no manifest secrets or SSH credentials are included in URIs or
   errors.

### CLI unit tests

1. Root and subgroup registration, help text, and command order.
2. `up` passes `fast=True`; `--reconfigure` passes `fast=False`.
3. `--open` is the only `up` path that calls the OS opener, and `ide: none`
   rejects it before launch.
4. Current three-item launch responses write SSH config directly.
5. Legacy two-item launch responses use the status credential fallback.
6. A setup-carrier job is tailed before readiness; failure preserves the
   cluster, while an ID returned without setup fails as an invariant violation.
7. `open` requires one visible UP cluster and prints before opening.
8. Home-relative editor paths are resolved by one fixed argv SSH command;
   invalid, failed, oversized, or non-absolute responses fail safely.
9. `ssh` refreshes SSH config, uses the argv-only subprocess path, and
   propagates the child return code.
10. `status`, `stop`, and `down` delegate one cluster and preserve confirmation
   defaults.
11. Workspace/config callbacks remain available on `up`.

### Existing regression tests

- focused client, task-YAML, SSH-config, and CLI helper tests;
- `bash format.sh --files` for every changed Python file;
- import and CLI help smoke tests.

### Live canary

Against an existing API server:

1. Use a low-cost, single-node Kubernetes CPU manifest with an autostop policy
   and a workdir sentinel.
2. Run `sky dev up` and verify one cluster, a terminal setup-carrier job but no
   user run workload, a usable SSH alias, and the expected
   `open --print-only` URI.
3. Change the sentinel and run `up` again. Verify synchronization and that a
   setup sentinel did not change.
4. Run `up --reconfigure` and verify the setup sentinel changes.
5. Run `status`. On a provider that supports stopped instances, run `stop` and
   `up`; on Kubernetes, verify the wrapper preserves the established
   unsupported result without mutation.
6. Run `down`, confirm no unexpected API, executor, or database errors, and
   clean all canary resources.

## Rollout

1. Merge the additive client parser, CLI, tests, docs, and example.
2. Validate the exact merged client against the current isolated API server.
3. Do not deploy a new server image because the feature changes no server
   runtime code, schema, chart, or API contract.
4. Ship through the normal client release path.
5. Keep the root cluster commands as the compatibility escape hatch.

## Follow-up: connection-aware inactivity

dstack counts established SSH TCP connections, including non-PTY forwarding.
SkyPilot's current `jobs_and_ssh` policy detects PTY process ancestry and may
miss a long-lived `ssh -N` connection or an editor session without a PTY.

This is a real general autostop gap, but it is not safe to couple to the v1 CLI
facade. A follow-up must first add a red test and identify provider-neutral
session evidence without assuming external port 22. It then requires both VM
and Kubernetes canaries proving:

- short SkyPilot status and probe connections do not reset activity;
- a connection lasting longer than the noise threshold prevents autostop;
- closing the connection allows autostop;
- the existing PTY detector remains a compatibility fallback.

The result should improve the ordinary `jobs_and_ssh` contract, not create
development-only timer state.

## Acceptance criteria

- `sky dev` exposes all six v1 commands.
- `dev` never crosses the client-server boundary.
- An already-UP ordinary VM or Kubernetes environment resynchronizes without
  rerunning setup by default.
- `--reconfigure` reruns setup without implicit recreation.
- Editor and SSH connection commands use the existing generated SSH alias.
- Old-server launch response fallback is covered by tests.
- Root cluster behavior and command identity remain unchanged.
- No server, database, API-version, scheduler, or controller change is needed.
- The exact merged client passes focused tests and the live lifecycle canary.
