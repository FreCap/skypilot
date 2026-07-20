# SkyServe per-service cleanup concurrency cap

## Problem

SkyServe uses durable replica rows to queue launch and teardown work. The
global weighted admission budget prevents aggregate provisioning overload, but
a single service can still admit hundreds of teardown threads in one refresh
tick when the global budget is large. In production, one cleanup wave admitted
270 workers, grew the controller child to more than 300 threads and about 8 GiB
RSS, delayed autoscaler reads, caused load-balancer synchronization failures,
and triggered the controller watchdog.

The durable retirement queue is correct. The unsafe behavior is starting every
eligible worker at once.

## Behavior contract

- A service may run at most 64 teardown workers concurrently.
- Already-running teardown workers count toward the cap.
- Eligible workers above the cap remain durably `SCHEDULED` and stay in the
  local pool without starting an operating-system thread.
- Each later refresh tick fills newly available slots from the queued workers.
- Launch admission remains ahead of teardown admission and continues to share
  the existing cross-service weighted budget.
- Logical-retirement ownership and load-balancer fencing run before admission
  exactly as they do today.
- Cleanup retries remain indefinite. This cap changes concurrency, not retry or
  retention policy.

## Design

Add an internal per-service teardown concurrency constant in
`sky/serve/replica_managers.py`. During `_refresh_thread_pool`, count live down
threads from the same snapshot used to classify completed and scheduled work.
After launch admission, stop admitting down workers when the local running
count reaches the cap. Increment the local count only after a worker starts.

The cap is intentionally local to a service manager. Putting it in the global
budget helper would couple unrelated services and would not protect one
controller process when the global budget is high. Making it a user-facing
configuration knob would enlarge the service API without evidence that
operators need to tune it.

The initial value is 64. It reduces the observed 270-worker wave by more than
four times while allowing large cleanup backlogs to make material progress.
The global budget may lower the actual concurrency further.

## Alternatives considered

- Lower the global launch budget. Rejected because it slows every service and
  does not encode the per-process safety invariant.
- Admit all workers but use a Python executor. Rejected because it would be a
  larger lifecycle rewrite and the existing durable pool already represents
  queued work safely.
- Cap queued replica rows. Rejected because dropping durable cleanup intent can
  orphan provider resources.

## Rollout and observability

Ship as a normal API-server release. No migration or configuration change is
required. Monitor controller child thread count, RSS, watchdog restarts,
scheduled and running stopping replicas, load-balancer sync health, and total
provider instances during a retirement backlog.

## Test plan

- Unit test a backlog above the cap and assert only the capped number starts.
- Include already-live workers and prove they consume local slots.
- Complete a worker, refresh again, and prove one scheduled worker backfills.
- Preserve the one-scan-per-tick admission regression assertion.
- Run the focused Serve control-loop tests and formatter.
