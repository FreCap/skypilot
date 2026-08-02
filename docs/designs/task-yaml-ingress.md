# Task YAML ingress extraction

## Problem

`sky/task.py` is 2,086 lines and its `Task` class owns several distinct
responsibilities. The mutable task model validates local paths, manages resources,
mounts, volumes, environment, storage synchronization, and runtime-facing task
state. Separately, the 382-line `Task.from_yaml_config()` implementation validates
and destructively consumes an external YAML dictionary, expands environment
references, constructs nested storage, resource, service, secret, and volume
objects, and projects them into the task model.

YAML egress already lives in `sky/task_yaml.py` behind the stable
`Task.to_yaml_config()` facade. Leaving ingress in the model splits ownership of
the YAML boundary and keeps schema, parser, and nested-config dependencies mixed
with stateful task operations.

## Responsibility map

### Mutable task model and validation

Callers include the optimizer, launch and execution paths, Managed Jobs,
SkyServe, SDK and CLI builders, and plugins. It depends on cloud resources, local
files and Git state, storage synchronization, volume resolution, Docker credential
policy, and registered validators. It owns live task fields and their mutation
ordering. Its failures include invalid paths, inconsistent resources, leaked or
incorrect storage state, and changed runtime behavior. Construction and mutation
are performance-sensitive and change with task lifecycle and provider behavior.

### YAML ingress and nested construction

Callers include `Task.from_yaml()`, `Task.from_yaml_str()`, the CLI, SDK/server
transport, admin policy, Managed Jobs, SkyServe, batch, recipes, and tests. It
depends on the task schema, YAML normalization, environment substitution, and the
Storage, Resources, SkyServiceSpec, ManagedSecretRef, and VolumeMount config
constructors. It owns no durable state; it destructively consumes the supplied
dictionary and returns a populated task. Its failures are schema errors, changed
error text, incorrect override precedence, secret exposure or identity drift,
changed nested-constructor ordering, and lossy YAML round trips. Parse time and
nested constructor call counts are sensitive. Its change cadence follows the task
YAML schema and client/server compatibility boundary.

### YAML egress and redaction

Callers include SDK/server transport, admin policy, debug dumps, persistence,
and user-visible YAML output. It depends on nested serializers and secret
redaction rules and owns no durable state. It already resides in
`sky/task_yaml.py`, behind `Task.to_yaml_config()`.

## Solution

Move the complete `Task.from_yaml_config()` implementation and its ingress-only
environment-substitution helper into `sky/task_yaml.py`. Keep the exact public
staticmethod, signature, annotations, module, qualified name, and behavior in
`sky.task`; the method becomes a facade that passes the historical `Task` and
`ManagedSecretRef` constructors into the YAML gateway. Keep file and string I/O
in `Task.from_yaml()` and `Task.from_yaml_str()` so the extraction does not mix
filesystem access with dictionary parsing.

The helper remains a plain function. There is one construction algorithm, so an
adapter, strategy, factory hierarchy, protocol registry, or dependency injection
layer would add an unproven concept. Passing the two historical constructors is
the minimum needed to avoid a circular import while preserving their identities.

The extraction must preserve:

- destructive mutation of the supplied config and override precedence;
- schema validation, normalization, and error text;
- task constructor and setter ordering;
- inline and managed secret formats and `ManagedSecretRef` identity;
- nested Storage, Resources, SkyServiceSpec, and VolumeMount construction;
- file-mount volume translation and exception rewriting;
- config hooks, resource ordering, service versus pool behavior, metadata, and
  user-specified YAML retention;
- public facade identity and all YAML round trips;
- historical private helper paths through direct aliases;
- parse operation counts and import and representative parse performance.

## Alternatives considered

Leaving the method in place avoids a facade call but keeps the established YAML
boundary split across two modules and retains approximately 380 lines of schema
gateway logic in the mutable model.

Moving all task validation or storage synchronization would cross stateful
lifecycle boundaries and would be a broader, riskier extraction.

Moving `Task`, `ManagedSecretRef`, or file/string loading would risk public and
serialized identity or mix transport and filesystem responsibilities into the
dictionary gateway.

## Test and rollout plan

Before moving production behavior, add characterizations for facade identity and
signature, destructive input consumption, override and secret projection, nested
constructor cardinality and ordering, representative round trips, and errors.
Run them against the exact base and unchanged after extraction.

The changed-path matrix is:

| Changed responsibility | Local tests | CI jobs |
| --- | --- | --- |
| YAML facade and config consumption | new ingress contract, task tests, YAML parser tests | Unit Tests, Config Storage and Compatibility |
| nested resources, storage, service, and volume construction | task, storage, Jobs and Serve tests | Unit Tests, Jobs and API Tests, optimizer shards |
| CLI/server transport round trips | CLI/task and DAG tests | CLI Tests, Jobs and API Tests |
| imports and typing | format, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall | static-analysis and worker-import jobs |

Measure alternating cold `import sky.task` samples and repeated representative
`Task.from_yaml_config()` parses against the exact base. The extraction must not
add nested construction, copy, filesystem, network, or database operations, and
must remain within timing noise.
