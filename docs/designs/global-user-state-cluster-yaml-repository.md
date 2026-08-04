# Global User State Cluster YAML Repository Boundary

_Created: 2026-08-01_

## Problem

`sky/global_user_state.py` is 3,746 lines after several facade-first
decompositions. It still combines shared database lifecycle, cluster lifecycle
state, storage and volume state, SSH credentials, and cluster YAML storage.
The line count is only a prioritization signal. The useful seam is the bounded
`cluster_yaml` table gateway, not the entire cluster YAML family.

## Responsibility map

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Shared database lifecycle and schema facade | Server startup, migrations, and every global-state repository | `DatabaseManager`, migration utilities, SQLAlchemy metadata, runtime configuration | Engine, pool, sessions, table identities, migration ordering, historical `_db_manager` replacement seam | Wrong database, pool exhaustion, migration drift, duplicate tables, broken test and embedding isolation | Engine lookup, connection, transaction, and import counts | Database topology and schema evolution |
| Cluster lifecycle and history repository | CLI, SDK, dashboard, jobs, Serve, and refresh workers | Serialized handles, status transitions, event history, workspaces, image ownership, usage intervals | Cluster rows, lifecycle events, history projections, handles, and shared transactions | State corruption, stale status, invalid transitions, serialization drift, or reordered locks | Query counts, row copies, serialization, and lock ordering | Cluster lifecycle and product behavior |
| Cluster YAML table gateway | Provisioning, backend utilities, Serve configuration batching, and legacy handle restoration through the facade | One table, SQLAlchemy sessions, SQLite and PostgreSQL upserts | Durable YAML strings keyed by cluster name | Missing rows, wrong upsert dialect, lost commit, extra queries, or changed batch query shape | Provisioning and controller hot paths; exact engine and statement counts | Persistence dialect and legacy-state compatibility |
| Legacy local-file migration | Historical handles whose YAML has not yet been copied into the database | Filesystem path rules, `.debug` fallback, UTF-8 reads, and the facade setter | No durable state directly; conditionally seeds the table gateway | Wrong fallback precedence, file errors, stale local content, or bypassed facade monkeypatches | File I/O occurs only on a database miss | Legacy migration and eventual removal policy |
| YAML projection and batch reconstruction | Provisioner, VM backend, Kubernetes, jobs, and Serve | `yaml_utils.safe_load`, input path parsing, missing-row errors, and caller order | In-memory dictionaries and input-to-output positional correspondence | Parse errors, changed `ValueError` text, duplicate collapse, or reordered results | One batch query plus decoding; no extra copies or per-item reads | Consumer schema and presentation needs |

The table gateway has distinct dependencies, state, failures, performance
constraints, and reasons to change from both local-file migration and YAML
projection. It owns every direct read, upsert, and delete transaction for the
table. File fallback and projection remain cohesive with their callers and do
not move in this change.

## Decision

Add `sky/global_user_state_cluster_yaml.py` as a plain repository module. Keep
all historical functions in `sky.global_user_state` as the stable facade. The
facade continues to own path-to-cluster-name parsing, missing-row fallback,
`.debug` precedence, file reads, input-order and cardinality reconstruction,
YAML decoding, decorators, and public exceptions.

The repository accepts the already-resolved engine and historical dependency
objects. It owns single and batched table reads, dialect-specific upsert SQL,
delete SQL, and transaction commits. Passing the facade's `orm.Session`,
SQLite insert module, PostgreSQL insert module, and table object preserves
late-bound test and embedding seams without a reverse import or registry.

This is a facade-first repository extraction with plain functions. No class,
protocol, abstract repository, factory, dependency-injection layer, or package
hierarchy is introduced. The extraction is structural only.

## Alternatives considered

- Move all cluster YAML functions: rejected because that would combine
  persistence, filesystem migration, positional reconstruction, and YAML
  presentation in the new module while weakening historical facade
  monkeypatch behavior.
- Directly alias moved functions: rejected because engine, session, dialect,
  file, parser, and sibling-function lookups would stop resolving through the
  historical facade.
- Extract only the two upsert/delete functions: rejected because reads and
  writes would retain split ownership of the same table gateway.
- Generalize a reusable key-value repository: rejected because no second table
  shares the cluster-name key, batch lookup, YAML value, or migration contract.
- Leave the gateway in place: safe, but it retains a complete independent
  persistence boundary inside the multi-domain facade.

## Behavior contract

- Public import paths, signatures, qualified names, decorator depths,
  exceptions, and monkeypatch paths remain unchanged.
- Single reads perform one engine lookup and one query. Missing rows preserve
  local-file fallback and `.debug` precedence, while present rows whose value
  is `NULL` remain distinguishable and do not trigger fallback.
- Batched reads perform no engine lookup for empty input and otherwise one
  engine lookup and one query over unique cluster names. Results preserve input
  order, duplicates, and cardinality.
- Each upsert preserves two facade engine lookups, dialect selection, one
  statement, and one commit. Deletes preserve one engine lookup, one delete,
  and one commit.
- File encoding, YAML parsing, missing-row error text, durable key/value
  formats, SQL conflict target, and transaction ordering do not change.

## Milestones and rollout

1. Add and run characterization tests against the unchanged facade for public
   identity, engine and statement counts, batching, file fallback, YAML
   projection, and dialect dependency paths.
2. Move only direct `cluster_yaml` table operations behind the facade.
3. Run mapped global-state, backend, provisioner, jobs, Serve, static-analysis,
   import, compile, and performance gates.
4. Publish only from the latest `origin/improvements` and merge only when every
   relevant check and review thread is green on the exact pushed SHA.

Rollback is a normal revert because no schema, data, wire, CLI, or public API
transition is introduced.

## Changed-path-to-test matrix

| Changed path or seam | Tests and checks |
| --- | --- |
| Historical facade, signatures, decorators, `_db_manager`, ORM, and dialect patch seams | New cluster-YAML repository contract; import contracts |
| Single read, upsert, delete, commits, and exact operation counts | New real-SQLite and mocked-PostgreSQL contracts; global-state tests |
| Batch deduplication, ordering, duplicate cardinality, and empty input | New contract; existing duplicate-input regression; Serve configuration tests |
| Exact-file and `.debug` fallback plus facade setter lookup | New filesystem and monkeypatch contract |
| YAML decoding and missing-row errors through facade sibling lookups | New projection contract; backend and provisioner tests |
| Structural and type behavior | `format.sh --files`, mypy, Pylint, Ruff, BasedPyright, import-linter, compileall, and `git diff --check` |

No live-cloud smoke is planned because the seam changes no provider, remote
command, process lifecycle, network protocol, schema, or cloud resource
behavior.
