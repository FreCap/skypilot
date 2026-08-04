# Global User State Storage Repository

## Context

`sky/global_user_state.py` is a 3,732-line compatibility facade over the
process-wide state database. It still contains the complete repository for the
`storage` table next to cluster lifecycle, user, volume, SSH-key, and database
lifecycle code. File size is only the prioritization signal. The extraction is
justified because the storage operations form one complete table boundary with
callers, state, failures, and change cadence that differ from the surrounding
responsibilities.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and schema facade | Server startup, migrations, and every global-state repository | `DatabaseManager`, migration utilities, SQLAlchemy metadata, and runtime configuration | Engine, pool, sessions, table identities, migration ordering, and the historical `_db_manager` replacement seam | Wrong database, pool exhaustion, migration drift, duplicate tables, or broken test and embedding isolation | Engine lookup, connection, transaction, and import counts | Database topology and schema evolution |
| Cluster lifecycle and history repository | CLI, SDK, dashboard, jobs, Serve, and refresh workers | Serialized handles, status transitions, event history, workspaces, image ownership, and usage intervals | Cluster rows, lifecycle events, history projections, handles, and shared transactions | State corruption, stale status, invalid transitions, serialization drift, or reordered locks | Query counts, row copies, serialization, and lock ordering | Cluster lifecycle and product behavior |
| Storage-table repository | `Storage`, `sky storage`, server completion, task mounting, and tests | One SQLAlchemy table, `StorageStatus`, `StorageMetadata` pickle payloads, command attribution, wall-clock time, and SQLite/PostgreSQL glob and upsert dialects | Storage rows containing handle, last command, launch timestamp, and lifecycle status | Invalid status, missing rows, pickle drift, wrong dialect, changed glob semantics, lost commit, or changed historical error text | Storage construction and CLI paths with exact engine, statement, transaction, serialization, and projection counts | Storage lifecycle and persistence compatibility |
| Volume repository | Volume server, provisioners, Kubernetes and RunPod volume paths, dashboard, and tests | Volume model/config serialization, status transitions, provider metadata, and one SQLAlchemy table | Volume rows and lifecycle projections | Invalid transitions, config drift, stale projections, or extra queries | Query counts, serialization, and list projections | Volume product behavior |

The storage repository is materially different from the shared database
lifecycle because it owns storage-domain serialization, status validation,
command attribution, and glob semantics. It is materially different from the
cluster and volume repositories because it has independent callers, a separate
table and lifecycle, and its own historical compatibility strings.

## Chosen seam

Move the complete nine-operation `storage` table repository to
`sky/global_user_state_storage.py`:

- add or update a storage row;
- remove a storage row;
- set and get lifecycle status;
- set and get the serialized `StorageMetadata` handle;
- resolve glob and prefix names; and
- project all storage rows for `sky storage ls`.

Keep the historical functions in `sky.global_user_state` as timed facade
wrappers. The facade continues to resolve `_db_manager`, the table identity,
the SQLAlchemy session factory, dialect modules, and the glob converter at call
time. The extracted module receives those resolved dependencies, so existing
monkeypatch and embedding seams remain late-bound. Standard-library and SkyPilot
module objects such as `pickle`, `time`, `common_utils`, and `status_lib` are
shared imports, preserving attribute-level monkeypatches.

This is a facade-first plain-module repository extraction. A class, protocol,
generic key-value repository, registry, factory, or dependency-injection layer
would add a second abstraction without a second implementation. Splitting
serialization, mutation, and queries into separate objects would fragment one
small table lifecycle and add forwarding layers.

## Behavior contract

- Public names, signatures, module names, qualified names, and decorator depths
  remain unchanged.
- `Storage.StorageMetadata` pickle identities and payloads remain unchanged.
- Status values, validation behavior, command attribution, timestamps, row
  projection keys, and result ordering remain unchanged.
- Missing-row errors remain byte-for-byte compatible, including the historical
  missing space in `Storage{name} not found.` for handle updates.
- SQLite uses `GLOB`; PostgreSQL uses `SIMILAR TO` with the historical
  `_glob_to_similar()` translation.
- Unsupported dialect errors and assertion behavior remain unchanged.
- Every facade call performs the same engine lookups, SQL statements,
  transactions, serialization operations, and row copies as before.
- There is no schema, configuration, CLI, remote-command, or lifecycle-ordering
  change.

## Alternatives

- Leave the repository in the facade: rejected because this complete table
  boundary keeps storage-specific serialization and lifecycle policy mixed with
  unrelated global state domains.
- Move only writes or only queries: rejected because each would leave split
  ownership of one table and force future changes across both modules.
- Move `StorageMetadata` or `StorageStatus`: rejected because their public and
  pickle identities belong to the storage model and status library, not the
  persistence gateway.
- Extract the neighboring volume repository too: rejected as a second domain
  with different callers, payloads, failure modes, and lifecycle policy.

## Milestones

1. Add characterization tests and run them unchanged against the exact base.
2. Move the storage repository behind the existing facade without behavior
   changes.
3. Re-run the characterization tests and relevant storage, task, core, server,
   and global-state suites.
4. Prove import and representative SQLite-read performance, then run formatting
   and static analysis.

## Test plan

| Changed path or seam | Evidence |
| --- | --- |
| `sky/global_user_state.py` facade | Public signature, module identity, qualified name, decorator-depth, late-bound manager/table/dialect/glob tests, global-state suite, and import contracts |
| `sky/global_user_state_storage.py` repository | SQLite lifecycle and statement-count characterization, PostgreSQL upsert/glob shape, exact validation and missing-row errors, and unsupported-dialect behavior |
| `Storage.StorageMetadata` payloads | Real metadata round trip through add, list, get, and handle update with exact class identity |
| `StorageStatus` lifecycle | Existing storage unit tests plus status validation, update, missing-row, and projection characterizations |
| Storage callers | Storage unit tests, task tests, core tests, server tests, and storage smoke-test collection where local execution does not require cloud resources |

No live-cloud smoke test is required for a pure repository extraction that does
not change providers, remote commands, schemas, network protocols, or process
lifecycle. CI must collect the new contract and the relevant storage and global
state suites for both changed production paths.

## Rollout

Ship as one structural commit. Preserve the facade indefinitely. If any
characterization, static check, import check, performance sample, review thread,
or relevant CI job fails on the exact pushed head, leave the PR open and record
the blocker rather than merging.
