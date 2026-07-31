# SkyServe replica duty lifecycle

The `SkyPilotReplicaManager` owner watchdog is the authoritative local signal that a controller incarnation has lost ownership. Transient owner-query failures do not set `_ownership_lost`; a missing, replaced, or teardown-blocked owner does.

The refresher, job-status fetcher, and replica prober must share that signal. Once it is set, none may start another reconciliation round and their supervisors must not restart them. An interval wait must also wake promptly so a stale controller does not continue database reads, SSH walks, endpoint probes, or cleanup decisions until its process happens to exit.

In-flight work is left to its existing database and launch fences. Interrupting arbitrary cloud or SSH calls mid-operation would create a second cancellation protocol and is outside this bounded change.

## Behavior contract

- Active owners run the same three duties at the same intervals.
- A pre-set ownership-loss event prevents the first duty round.
- Ownership loss during an interval wait ends the duty before another round.
- A duty that exits because ownership was lost is not restarted by its supervisor.
- Transient owner lookup failures retain the existing retry behavior and do not stop duties.

## Changed-path-to-test matrix

| Production path | Lifecycle, correctness, or performance invariant | Test file and command |
|---|---|---|
| `sky/serve/replica_managers.py::SkyPilotReplicaManager.__init__` | every supervised duty uses the manager's exact ownership-loss event | `tests/unit_tests/test_serve_replica_managers.py`; `pytest -q tests/unit_tests/test_serve_replica_managers.py` |
| `sky/serve/replica_managers.py::_thread_pool_refresher` | no pool refresh starts after loss and its wait is interruptible | same focused file and command |
| `sky/serve/replica_managers.py::_job_status_fetcher` | no status or SSH sweep starts after loss | same focused file and command |
| `sky/serve/replica_managers.py::_replica_prober` | no probe or service-status write starts after loss | same focused file and command |
| all three duties | active-owner call count and asymptotic cost stay unchanged; stale-owner work becomes zero | focused call-count assertions plus the SkyServe unit suite |

## Alternatives

Hard-exiting the controller from the watchdog duplicates process supervision and broadens failure behavior. Independent stop flags can diverge from the ownership fence. Re-reading ownership in every duty adds hot-path database work. Reusing the existing event is the smallest state model and has no active-owner query cost.

## Rollout and rollback

This is process-local and requires no migration. Reverting restores unconditional background loops. Existing database and launch fences remain the safety net during rollout.
