# SkyServe Autoscaler Version Fallback Tick

## Problem

Instance-aware request-rate and concurrency autoscalers recover the capacity
knob for a historical replica version from the durable service spec. Successful
reads are cached for the lifetime of that live version. A missing spec or a
transient database error deliberately is not cached so a later controller tick
can heal.

That retry contract currently has no tick boundary. When many replicas share an
unavailable historical version, every capacity calculation in the same
autoscaler tick repeats the same point read and warning. A state-store outage
therefore turns one fallback into work proportional to fleet size and increases
pressure on the failing dependency.

The tick boundary must not make the latest-version fallback authoritative for
rolling retirement. If the latest spec has much higher per-replica capacity
than the unavailable historical spec, reusing that fallback across an entire
old version can overstate the surviving fleet and retire serving replicas
before their replacements are ready. The bounded read therefore needs a
fail-closed drain rule as well as retry liveness.

## Behavior Contract

- Within one `generate_scaling_decisions()` call, each unavailable historical
  version causes at most one durable spec read per autoscaler instance.
- All replicas of that version use the existing latest-version fallback for the
  remainder of the tick for sizing and scale-up decisions.
- An unavailable historical version cannot authorize outdated-replica
  retirement while the latest fleet is still below its final target. The
  autoscaler keeps all old replicas for that tick and may still request the
  latest-version scale-up. Once enough latest-version replicas are ready to
  satisfy the final target, retirement remains safe without historical
  capacity data.
- The unavailable-version memo is discarded when the tick completes, whether
  the tick returns or raises. The next tick retries the durable read so a
  transient miss or database failure can heal.
- Successful version specs keep using the existing live-version cache.
- Instance-aware request-rate decisions remain serialized by
  `_instance_state_lock`; concurrency decisions remain serialized by
  `_logical_state_lock`. The tick-local memo is never shared across concurrent
  decision owners.
- The memo is bounded by the number of distinct unavailable live versions. It
  adds no database query, provider call, retry, timer, poll, or work
  proportional to replica count beyond constant-time set membership.

## Changed-Path-to-Test Matrix

| Changed production path or invariant | Concrete test | Command | CI job |
| --- | --- | --- | --- |
| `sky/serve/autoscalers.py`: request-rate historical QPS fallback is read once per unavailable version in a tick | `tests/unit_tests/test_serve_autoscaler.py`: missing/error fallback call-count regression with repeated same-version capacity resolutions | `pytest -n 0 tests/unit_tests/test_serve_autoscaler.py -k "version_fallback"` | `Python Tests - Unit Tests` |
| `sky/serve/autoscalers.py`: concurrency historical knob fallback is read once per unavailable version in a tick | `tests/unit_tests/test_concurrency_autoscaler.py`: the same regression through physical-backend capacity resolution | `pytest -n 0 tests/unit_tests/test_concurrency_autoscaler.py -k "version_fallback"` | `Python Tests - Unit Tests` |
| Failure recovery: a second tick retries and then adopts the recovered historical spec | Both focused test files, with a failed first read and successful second-tick read | Both focused commands above | `Python Tests - Unit Tests` |
| Cleanup: exceptional tick exit discards the unavailable-version memo | `tests/unit_tests/test_serve_autoscaler.py`: forced decision exception followed by a successful retry | Request-rate focused command above | `Python Tests - Unit Tests` |
| Request-rate drain safety: a one-shot historical read failure cannot multiply the latest-version fallback into old-fleet retirement | `tests/unit_tests/test_serve_autoscaler.py`: controller-restart rolling update with 100 low-capacity old replicas, one ready latest replica, and immediate state-store recovery | Request-rate focused command above | `Python Tests - Unit Tests` |
| Concurrency drain safety: unavailable historical knobs fail closed for physical-backend retirement | `tests/unit_tests/test_concurrency_autoscaler.py`: matching high-latest/low-old rolling-update regression | Concurrency focused command above | `Python Tests - Unit Tests` |
| Adjacent autoscaling, rollout, liveness, and performance behavior | Full request-rate and concurrency autoscaler unit files; existing version-cache, rolling-update, and decision tests plus new exact call-count assertions | `pytest -n 0 tests/unit_tests/test_serve_autoscaler.py tests/unit_tests/test_concurrency_autoscaler.py` | `Python Tests - Unit Tests` |
| SkyServe integration surface | Jobs and Serve integration suite | `pytest -n 0 tests/test_jobs_and_serve.py` | `Python Tests - Jobs & API Tests` |
| Formatting and static contracts | Changed Python files through repository formatter, plus diff validation | `bash format.sh --files sky/serve/autoscalers.py tests/unit_tests/test_serve_autoscaler.py tests/unit_tests/test_concurrency_autoscaler.py`; `git diff --check` | `format`, `mypy`, `Pylint`, `Ruff`, `basedpyright`, `async-lifecycle`, import-contract jobs |

The workflows have no changed-path exclusion for these files.
`.github/workflows/pytest.yml` runs all of `tests/unit_tests` in `Python Tests -
Unit Tests` and includes `tests/test_jobs_and_serve.py` in `Python Tests - Jobs
& API Tests`.

## Alternatives

1. Permanently cache the latest-version fallback. Rejected because a transient
   miss would never heal until the autoscaler is replaced.
2. Batch-load every live version at the start of every tick. Rejected for this
   bounded fix because successful versions already remain cached and many ticks
   need no historical read. An unconditional batch query would regress the
   healthy steady state.
3. Rate-limit warnings only. Rejected because it leaves the database retry
   fanout intact.
4. Continue capacity-aware old-replica retirement with the latest-version
   fallback. Rejected because the fallback may be larger than the historical
   capacity and can therefore retire serving capacity before replacements are
   ready. A one-tick drain hold is bounded, retryable, and conservative.

## Implementation and Rollout

1. Add one tick-local unavailable-version set to each affected autoscaler.
2. Install and clear the set around the existing locked decision scope with a
   `finally` cleanup.
3. Consult the set before historical reads and record both missing specs and
   read failures.
4. If either capacity-aware rolling drain encounters an unavailable historical
   version, suppress old-replica retirement for that tick while preserving
   scale-up decisions and the terminal branch where the latest fleet already
   satisfies the final target.
5. Prove the untouched parent repeats reads once per replica, then prove the
   exact head performs one read per unavailable version per tick and retries on
   the next tick.

No schema, API, configuration, migration, or staged rollout is required. A
normal controller restart also clears all in-memory state.
