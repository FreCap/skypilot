# Global User State Volume Repository

## Context

`sky/global_user_state.py` is a 3,652-line compatibility facade over the
process-wide state database. It still contains the complete repository for the
`volumes` table next to cluster lifecycle, users, SSH keys, and database
lifecycle code. The file size is only a prioritization signal. This extraction
is justified because the volume operations own one complete table boundary
whose callers, payloads, failure modes, and change cadence differ from the
surrounding responsibilities.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and schema facade | Server startup, migrations, and every global-state repository | `DatabaseManager`, migration utilities, SQLAlchemy metadata, and runtime configuration | Engine, pool, sessions, table identities, migration ordering, and the historical `_db_manager` replacement seam | Wrong database, pool exhaustion, migration drift, duplicate tables, or broken test and embedding isolation | Engine lookup, connection, transaction, and import counts | Database topology and schema evolution |
| Cluster lifecycle and history repository | CLI, SDK, dashboard, jobs, Serve, and refresh workers | Serialized handles, status transitions, event history, workspaces, image ownership, and usage intervals | Cluster rows, lifecycle events, history projections, handles, and shared transactions | State corruption, stale status, invalid transitions, serialization drift, or reordered locks | Query counts, row copies, serialization, and lock ordering | Cluster lifecycle and product behavior |
| Volume-table repository | Volume APIs, provisioners, Kubernetes auto-mount, task validation, dashboard completion, and tests | One SQLAlchemy table, `VolumeConfig` pickle payloads, `VolumeStatus`, JSON attachment projections, current user and workspace attribution, wall-clock time, and SQLite/PostgreSQL insert dialects | Volume rows containing configuration, ownership, workspace, lifecycle status, attachment timestamps and consumers, errors, and creation YAML | Pickle or JSON drift, incorrect ephemeral coercion, changed projection shape, lost attribution, unsupported dialect, duplicate overwrite, stale attachment state, or extra queries | Exact engine, statement, transaction, serialization, and projection counts on volume and cluster launch paths | Volume product behavior, provider support, auto-mounting, and dashboard detail reads |
| SSH-key repository | Authentication, remote execution, and user bootstrap | Key material, one SQLAlchemy table, and user identity | Public and private keys keyed by user hash | Credential loss, wrong-user lookup, or unsafe overwrite | Authentication and launch latency | Authentication and remote-execution policy |

The volume repository has materially different callers, dependencies, state,
failure modes, and reasons to change from the database lifecycle and cluster or
SSH-key repositories. Its nine operations cover all reads and writes of the
`volumes` table, so the seam does not split ownership.

## Design

Move the complete volume-table repository into
`sky/global_user_state_volumes.py`. Keep every historical public function in
`sky.global_user_state` as a timed facade wrapper. Each wrapper obtains the
current engine and passes the historical session factory, table, and dialect
objects at call time. This preserves late-bound `_db_manager`, `orm.Session`,
`volume_table`, `sqlite`, and `postgresql` patch points while the implementation
module owns volume serialization, projection, and lifecycle persistence.

Use a plain module rather than a class, protocol, generic repository, registry,
factory, strategy, or dependency-injection layer. There is no second volume
repository implementation or varying algorithm. The facade is the relevant
pattern because the public import and monkeypatch surface must remain stable.

## Behavior contract

- Preserve all public names, signatures, module and qualified names, decorator
  depth, CLI behavior, and caller patch paths.
- Preserve `VolumeConfig` pickle identity and field values, `VolumeStatus`
  identity, JSON attachment decoding, exact projection keys, and the historical
  omission of `is_ephemeral` from `get_volume_by_name()`.
- Preserve integer storage and filtering for `is_ephemeral`, ephemeral
  `IN_USE` coercion and attachment timestamping, current command, user, and
  workspace attribution, creation YAML, and conflict-ignore insert behavior.
- Preserve query cardinality, session and transaction ownership, empty batch
  query elision, duplicate-name deduplication order, unsupported-dialect error,
  and missing-row behavior.
- Do not change the schema, serialized formats, lifecycle policy, provider
  calls, remote commands, or public models.

## Alternatives considered

- Leave the repository in the facade: rejected because one complete table
  boundary remains mixed with unrelated global-state domains.
- Move only queries or only mutations: rejected because that would split table
  ownership and make volume changes span both modules.
- Move `VolumeConfig`, `VolumeStatus`, or provider lifecycle logic: rejected
  because those public models and policies have their own established owners.
- Introduce a generic storage/volume repository: rejected because the two
  tables have different payloads, transitions, projections, and error policy.
- Replace wrappers with direct aliases: rejected because callers and tests
  replace facade-owned database and dialect dependencies at runtime.

## Milestones

1. Add characterization tests and run them unchanged against the exact base.
2. Move the volume repository behind the existing facade without behavior
   changes.
3. Re-run the characterization tests and mapped volume, task, backend, server,
   and global-state suites.
4. Prove import and representative SQLite-read performance, then run formatting
   and static analysis.

## Test plan

| Changed path or seam | Evidence |
| --- | --- |
| `sky/global_user_state.py` facade | Public signature, module identity, qualified name, decorator depth, late-bound manager, session, table, and dialect tests, global-state suite, and import contracts |
| `sky/global_user_state_volumes.py` repository | Real SQLite lifecycle and statement-count characterization, PostgreSQL insert shape, unsupported dialect behavior, empty-batch behavior, and existing volume database tests |
| `VolumeConfig` payloads | Real add, point read, list, batch read, and configuration-update round trips with exact class identity |
| Volume lifecycle projection | Status, ephemeral integer/boolean conversion, attachment consumers, error clearing, creation YAML, and exact point/list projection keys |
| Volume callers | Volume server and provision tests, backend-utils auto-mount tests, task tests, completion/server tests, and smoke-test collection where local execution does not require cloud resources |

No live-cloud smoke test is required for a pure repository extraction that does
not change providers, remote commands, schemas, network protocols, or process
lifecycle. CI must collect the new contract and the relevant volume and global
state suites for both changed production paths.

## Rollout

Ship as one structural commit. Preserve the facade indefinitely. If any
characterization, static check, import check, performance sample, review thread,
or relevant CI job fails on the exact pushed head, leave the PR open and record
the blocker rather than merging.
