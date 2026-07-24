## Summary

Post-merge audit follow-up for #927 ([API] Enforce PostgreSQL connection pool budgets, merge `b6ac13b5148a868ffa19be9e06b939f72a884bb1`).

#927's budget **goal** is correct and verified, but its `max_overflow=0` implementation introduced a **connection-starvation regression**: three `global_user_state` operations open a *second* `orm.Session` on the shared synchronous PostgreSQL engine while the first still holds its connection. With `max_overflow=0` and `pool_size` as low as **1** (a hard 1 for the API-server main process, which calls `db_utils.set_max_connections(1)`), the nested checkout blocks for `pool_timeout` (15s in prod) and raises `sqlalchemy.exc.TimeoutError`. Before #927 the removed `max_overflow=4` silently absorbed the second checkout.

Fix: thread the caller's open session through the nested helpers so each logical operation uses a **single** connection. This **preserves #927's strict per-process budget** (no overflow is restored). The usage-interval write helper defers its commit to the caller when a session is supplied, so `remove_cluster`'s advisory/row locks are not released early.

Audited PR: #927 · Verdict: **PREMISE_PARTIALLY_FALSE** (budget cap correct; overflow-removal starves nested checkouts) · **medium regression**.

## Premise table (audit of #927)

| Claim (from #927) | Supporting evidence | Falsifying scenario | Pre-fix test coverage | Verdict |
|---|---|---|---|---|
| `get_max_db_connections()` overcounted usable connections by including `superuser_reserved_connections`/`reserved_connections` | `global_user_state.py` now subtracts them; PG16 `reserved_connections` via `current_setting(..., true)` | — | `test_global_user_state_db_capacity.py` (93, 97) | **CONFIRMED** |
| Old per-worker allocation (`=1` + `max_overflow=max(0,5-1)=4`) could exceed the usable budget (`N×5`) | New `min(5, budget//procs)` with `max_overflow=0` bounds total by construction; `max_parallel_all_workers == process_count` | — | `test_connection_pool_config.py` (4/5/0) | **CONFIRMED** |
| A strict per-process pool of `pool_size` with **no overflow** is safe down to `pool_size=1` | — | **FALSE:** 3 single-thread call chains hold 2 concurrent sync connections; at `pool_size=1` the 2nd checkout raises `sqlalchemy.exc.TimeoutError` after `pool_timeout` | **None** (gap this PR closes) | **PREMISE_PARTIALLY_FALSE** |

### The three nested single-thread checkouts (all on the one shared sync pool)

| Outer op (holds conn #1) | Nested helper (needs conn #2) | Reached from |
|---|---|---|
| `add_cluster_event(nop_if_duplicate=True)` (query at `:1414`) | `get_last_cluster_event` (`:1422`) | k8s provision, `backend_utils`, `cloud_vm_ray_backend`, and the main-process `surface_interrupted_cluster_launches` thread (`requests.py:1141`) |
| `remove_cluster` (advisory-lock + `FOR UPDATE` at `:1915/:1927`) | `_get_cluster_usage_intervals` (`:1961`), `_set_cluster_usage_intervals` (`:1969`) | `sky down`/`sky stop` teardown |
| `get_clusters_from_names(include_user_info=True)` (`.all()`) | `get_user` (`:2690`) | cluster listing/status |

An exhaustive AST scan across all three shared-pool modules (`global_user_state.py`, `jobs/state.py`, `serve/serve_state.py`, 267 session-opening functions) found **exactly** these 3 real chains. Two AST candidates were verified false: `get_cluster_history_provision_log_path→_get_hash_for_existing_cluster` (sequential — outer session's first statement runs *after* the inner returns) and `add_or_update_user→_sqlite_supports_returning` (SQLite-only branch, never on the PG pool).

## Red → green proof

Deterministic repro on a real `QueuePool(pool_size=1, max_overflow=0, pool_timeout=1)` engine (SQLite so no DB daemon; the advisory-lock helper is a no-op off PostgreSQL and SQLite ignores `FOR UPDATE`, so `remove_cluster` still exercises its nested helpers).

**RED — on the merged commit `b6ac13b514` (pre-fix), new tests run against pre-fix production code:**
```
FAILED test_remove_cluster_uses_single_connection
FAILED test_add_cluster_event_nop_if_duplicate_uses_single_connection
FAILED test_get_clusters_from_names_with_user_info_uses_single_connection
E   sqlalchemy.exc.TimeoutError: QueuePool limit of size 1 overflow 0 reached, connection timed out, timeout 1.00
3 failed in 5.04s
```

**GREEN — on this branch:**
```
4 passed in 4.19s
```

Existing suites still green on this branch: `test_global_user_state_{remove_cluster,cluster_events,batched_clusters,service_accounts}.py` + `test_orphaned_inflight_requests.py` → **60 passed**.

## Changed-path → test matrix

| Production change | Test |
|---|---|
| `get_last_cluster_event(session=...)` + `add_cluster_event` reuse | `test_add_cluster_event_nop_if_duplicate_uses_single_connection` |
| `_get_cluster_usage_intervals(session=...)` / `_set_cluster_usage_intervals(session=...)` + `remove_cluster` reuse | `test_remove_cluster_uses_single_connection` |
| `get_user(session=...)` + `get_clusters_from_names` reuse | `test_get_clusters_from_names_with_user_info_uses_single_connection` |
| write helper defers commit when a session is supplied | `test_set_cluster_usage_intervals_defers_commit_to_supplied_session` |

## Exact commands

```bash
# RED (pre-fix): copy this test onto merged b6ac13b514, run the 3 deadlock tests
python -m pytest tests/unit_tests/test_sky/test_global_user_state_nested_sessions.py \
  -k "not defers_commit"          # -> 3 failed (sqlalchemy.exc.TimeoutError)

# GREEN (this branch)
python -m pytest tests/unit_tests/test_sky/test_global_user_state_nested_sessions.py   # 4 passed
python -m pytest tests/unit_tests/test_sky/test_global_user_state_remove_cluster.py \
  tests/unit_tests/test_sky/test_global_user_state_cluster_events.py \
  tests/unit_tests/test_sky/test_global_user_state_batched_clusters.py \
  tests/unit_tests/test_sky/test_global_user_state_service_accounts.py \
  tests/unit_tests/test_orphaned_inflight_requests.py                                   # 60 passed

# Quality gates
mypy sky/global_user_state.py            # Success: no issues found
pylint --rcfile=.pylintrc sky/global_user_state.py   # 10.00/10
yapf --style google --diff (changed lines only)      # clean (no new hunks)
git diff --check                          # clean
```

## CI job mapping

The new file lives under `tests/unit_tests/test_sky/`, collected by the repo's **Unit Tests** job (`pytest tests/unit_tests/`, pytest-xdist). It needs no cloud/DB backend (SQLite QueuePool), so it runs in the default unit lane without the real-PostgreSQL gate. Changed production file `sky/global_user_state.py` is exercised by that same lane plus the existing `test_global_user_state_*` suites.

## Performance evidence

Zero added call-count/complexity cost: the fix removes one `orm.Session` open+connect per nested call (`add_cluster_event` dup-check, `remove_cluster` teardown, and one per row in `get_clusters_from_names(include_user_info=True)` — the last is now O(rows) → O(1) sessions per batch). No new queries; each affected operation issues the same statements on **one** connection instead of contending for two. Async-engine behavior (NullPool) is unchanged.

## Backward compatibility

Every helper gains only an optional `session=None` param; all existing callers keep prior behavior (open+close their own session). Verified no existing caller passes a positional arg that collides with the new param.
