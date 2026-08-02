# Storage and Volume Schema Extraction

_Created: 2026-08-02_

## Problem

`sky/utils/schemas.py` is 3,228 lines and constructs schemas for resources,
storage, volumes, tasks, clusters, global operator configuration, remote
identity, and plugin extensions. Its size is only a prioritization signal. The
useful seam is the complete persistent-storage and volume schema family, whose
callers and domain dependencies differ from the resource, task, and operator
configuration families.

The broader resource and global-config boundaries are not suitable for this
run. They share mutable plugin registries, task override filtering, and the
client/server additional-properties policy. An older conflicting PR also
changes the resource and global-config sections. Moving either boundary would
require callback plumbing or split ownership of extension state.

## Responsibility map

| Responsibility | Callers | Dependencies and state | Failure modes | Sensitivity and cadence |
| --- | --- | --- | --- | --- |
| Resource schema construction | `Resources.from_yaml_config`, optimizer tests, and global config construction | cloud names, accelerator and image fields, autostop, hooks, and the mutable job-recovery plugin registry | rejected existing YAML, permissive plugin validation, or changed task placement | import and validation latency; changes with placement and recovery policy |
| Storage and volume schema construction | storage loading, volume creation, task volume mounts, recipes, CLI, and tests | `StoreType`, `StorageMode`, `FileMountType`, `VolumeType`, `VolumeAccessMode`, infra syntax, rclone limits, labels, and sub-path rules; owns no runtime state | incompatible YAML, changed enum acceptance, invalid infra acceptance, or rclone option drift | schema construction and validation latency; changes with storage and volume products |
| Task and cluster schema construction | task YAML ingress and legacy cluster YAML loading | task topology, environment and secret shapes, task-only hooks, config overrides, and volume mounts | broken task compatibility, lost overrides, or invalid nested validation | task parse latency; changes with task syntax |
| Global operator-config construction and plugin extension state | config loading, workspaces, server startup, cloud setup, and plugins | server role, plugin registries, resource schema, cloud sections, workspaces, RBAC, daemons, and container-image policy | client/server validation mismatch, plugin rejection, or config compatibility drift | startup and config-reload latency; changes across operator features |

## Solution

Create `sky/utils/storage_schemas.py` containing the complete storage and volume
schema constructors, the volume-infrastructure pattern helper, and the labels
schema fragment used by both volume and global configuration construction.
Import and directly re-export those exact objects from `sky.utils.schemas`.
Preserve historical function names, signatures, module identities, and private
fragment names through the facade.

The move is structural only. Function bodies and schema dictionaries remain
unchanged apart from their module location. Deferred imports of storage and
volume enums remain deferred, so import topology and startup cost do not gain
new domain imports. No schema output, serialized format, validation behavior,
CLI output, remote command, database operation, or lifecycle ordering changes.

## Alternatives considered

Leaving the file unchanged avoids one module but keeps an independently used,
low-state product schema family inside a high-change global schema assembler.
Extracting only `get_storage_schema` or only the volume constructors would add
a module for a partial persistent-data boundary and leave closely related
schema ownership split.

Extracting the 1,547-line `get_config_schema` function would move more lines but
would not separate its internal responsibilities, and it would leave mutable
extension state in the facade or require a new registry abstraction. Extracting
the resource family would overlap an existing PR and has the same plugin-state
coupling. A class, protocol, registry, factory, or dependency-injection layer
has no second implementation and would add carrying cost.

## Implementation and verification

First add exact-base characterization for public identities, fresh output
construction, representative accepted and rejected storage, volume, and mount
documents, enum-derived values, rclone patterns, infra syntax, labels, and
sub-path rules. Then move the implementation and add facade-to-owner identity
checks.

Run the focused schema contract and full schema suite, storage and volume unit
suites, task YAML and recipe callers, and relevant smoke-test collection. Run
`format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import checks, compileall,
and `git diff --check`. Compare repeated cold imports and repeated schema
construction and validation against the exact base. CI must execute every
mapped suite on the exact pushed SHA before a normal merge.

Rollback is a normal revert because no schema or persisted data changes.
