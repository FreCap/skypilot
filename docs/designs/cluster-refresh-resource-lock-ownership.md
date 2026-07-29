# Cluster Refresh Resource-Lock Ownership

## Problem

Cluster status refresh still queries the API request table to avoid refreshing a
cluster with a running `sky.launch` request. That guard predates the
per-cluster resource-operation lock. Every non-dry-run provision now holds that
lock, and refresh acquires it non-blockingly before provider or shared-file
work.

The duplicate guard has two failure modes. A request-table error aborts the
entire background sweep or user status refresh, even though cluster state is
available. A stale RUNNING request can suppress reconciliation indefinitely
after its launch no longer owns cluster resources.

## Behavior contract

The resource-operation lock is the sole owner of exclusion between status
refresh and provider-mutating cluster operations:

- an active provision or teardown keeps refresh on the cached record because
  refresh cannot acquire the resource-operation lock;
- a stale or unavailable request-table row cannot suppress unrelated cluster
  refreshes;
- background refresh remains fault-isolated per cluster and keeps its INIT-first
  ordering;
- user-requested refresh preserves input order and its existing removed/unknown
  cluster handling;
- bulk and background refresh never wait behind an active status or resource
  operation, and return the cached record on either contention boundary;
- incomplete launch handles return cached state before owner or lock work
  because they do not yet contain provider metadata that can be refreshed;
- no provider call is added while a resource operation owns the cluster.

## Solution

Remove both launch-request table scans and submit every selected cluster to the
existing refresh path. Bulk and background callers use a zero status-lock
timeout, matching the old immediate launch-request skip without retaining a
second ownership model. Incomplete launch handles return cached state before
identity or lock work. Complete handles keep the resource-operation lock as the
gate before any provider work.
This deletes the duplicate ownership model instead of adding another fallback.

The normal path removes one request-database query and one set/list filtering
pass. A contended cluster performs one non-blocking status-lock attempt and
returns the cached record; if the status lock is available but a provider
operation owns the resource lock, the existing non-blocking resource check
returns cached state without provider I/O. Refresh worker counts, retry
behavior, and cloud query behavior are unchanged for uncontended clusters.

## Alternatives considered

Catching request-query failures and skipping INIT rows keeps two ownership
models and still lets stale request rows block liveness. Treating a failed query
as an empty result is simpler but leaves the redundant success-path query and
does not address stale rows. Removing the request guard is safe because
provision, teardown, and refresh already share the non-force-released
resource-operation lock.

## Changed-path-to-test matrix

| Changed path | Invariant | Test and command |
| --- | --- | --- |
| `sky/backends/backend_utils.py` | Background sweep no longer reads request tasks, covers every cluster, preserves ordering and per-cluster fault isolation | `pytest -q -o addopts='' tests/unit_tests/test_refresh_sweep.py` |
| `sky/backends/backend_utils.py` | Manual refresh no longer reads request tasks, preserves INIT/removed/unknown records and final enrichment | `pytest -q -o addopts='' tests/unit_tests/test_backend_utils.py` |
| `sky/backends/backend_utils.py` lock paths | Incomplete INIT metadata returns before identity/lock work; status contention performs no wait; resource contention returns cached state without provider mutation | `pytest -q -o addopts='' tests/unit_tests/test_sky/backends/test_cluster_resource_operation_lock.py` |
| Performance | Zero request-table queries; unchanged worker fan-out; incomplete records perform zero lock attempts; contention performs zero sleeps and zero provider updates | call-count assertions in the three focused files |

## CI and rollout

`.github/workflows/pytest.yml` runs all three files in `Python Tests - Unit
Tests` for pull requests to `improvements`, without path exclusions. The Python
format, mypy, Pylint, Ruff, basedpyright, async-lifecycle, and import-contract
workflows cover the production path. No migration or compatibility shim is
required.
