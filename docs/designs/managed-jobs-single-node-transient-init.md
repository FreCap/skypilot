# Managed jobs single-node transient INIT confirmation

## Behavior contract

A managed-job monitor treats a single-node non-terminal remote job status as
authoritative healthy evidence. If the next status fetch fails transiently and
the cluster probe reports `INIT`, the controller requires three consecutive
`INIT` observations before recovery instead of tearing down the cluster on the
first ambiguous tick.

The confirmation hold applies only when all of these are true:

- the task has one node;
- the cluster probe reports `INIT`;
- the current job-status fetch failed transiently; and
- the last confirmed remote job status was non-terminal.

Without prior healthy evidence, the single-node path recovers immediately.
Terminal status, `STOPPED`, missing clusters, and non-transient fetch failures
also retain immediate recovery. Multi-node behavior is unchanged.

## Lifecycle and liveness

The hold reuses the existing cluster-not-UP debouncer. Its effective threshold
is recorded with each observation so the decision and the progress log share
one value. Resetting after an `UP` observation or recovery clears both the
observation count and the effective threshold. A successful single-node
non-terminal status read also resets the streak because that fast path skips
the cluster probe.

The third consecutive ambiguous observation proceeds to the existing recovery
path. The existing wall-clock status-fetch retry budget remains the backstop
for a cluster that flaps between `UP` and `INIT`, where each `UP` observation
resets the consecutive streak.

Cancellation, recovery cleanup, controller restart behavior, and durable job
state transitions are unchanged.

## Performance

The healthy single-node fast path still performs no cluster refresh. The
ambiguous path performs at most three existing monitor rounds before recovery,
and the no-evidence path performs one. The change adds O(1) status state and
integer comparisons, with no new database, network, provider, task, timer, or
polling operation.

## Alternatives

Holding every single-node `INIT` would delay recovery without evidence that a
job is alive. Trusting the prior status indefinitely would risk a liveness
failure after a real preemption. A separate log-only threshold would duplicate
the decision predicate and could drift from behavior.

## Test plan

- Pin the helper policy for prior healthy evidence and no-evidence boundaries.
- Drive the real monitor through a healthy status followed by transient
  failures and assert two waiting logs report `1/3` and `2/3`.
- Assert a successful `RUNNING` read between ambiguous observations resets the
  next waiting log to `1/3`.
- Assert recovery on the third observation.
- Preserve exact refresh counts for the healthy fast path and immediate
  no-evidence recovery.
- Run the focused managed-jobs unit inventory, repository formatting, static
  async lifecycle check, and exact-head GitHub CI.
