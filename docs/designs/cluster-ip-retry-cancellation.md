# Cancel-aware legacy cluster IP retries

## Problem

Legacy Ray cluster IP discovery retries failed `ray get-head-ip` and
`ray get-worker-ips` subprocesses after raw blocking sleeps. If the owning API
request is cancelled during either backoff, the request worker remains asleep
and can start another subprocess after cancellation.

The lifecycle invariant is: cancellation during an IP-discovery retry backoff
must raise `asyncio.CancelledError` before the next head or worker IP probe.
Uncancelled and context-free callers must preserve the existing retry budgets,
backoff values, result parsing, failure reasons, and subprocess counts.

## Solution

Use the existing `context_utils.sleep_with_cancellation()` bridge for the two
legacy IP-discovery retry waits in `sky/backends/backend_utils.py`. The bridge
delegates to `time.sleep()` when no request context exists. With a request
context it registers one event callback, waits for either cancellation or the
same timeout, checks the cancellation bit at the timeout boundary, and always
unregisters the callback.

No retry loop, remote operation, exception mapping, or public interface changes.

## Alternatives

Explicit cancellation checks before and after `time.sleep()` would duplicate
request-context mechanics and would remain uninterruptible during the sleep.
Removing or reducing the retries would degrade cluster launch liveness.
Converting unrelated cluster sleeps in the same change would increase the blast
radius without strengthening this invariant.

## Changed-path-to-test matrix

| Production path or invariant | Test path and case | Command |
| --- | --- | --- |
| `sky/backends/backend_utils.py::_query_head_ip_with_retries`: cancellation stops before a second head subprocess | `tests/unit_tests/test_backend_utils_ray_ready.py::test_query_head_ip_cancellation_stops_before_next_probe` | `pytest -o addopts='' tests/unit_tests/test_backend_utils_ray_ready.py -q` |
| `sky/backends/backend_utils.py::get_node_ips`: cancellation stops before a second worker subprocess | `tests/unit_tests/test_backend_utils_ray_ready.py::test_get_node_ips_worker_retry_cancellation_stops_before_next_probe` | same |
| Active head and worker retries keep one wait per non-final failure, exact backoff values, success parsing, and subprocess counts | adjacent uncancelled boundary cases in `tests/unit_tests/test_backend_utils_ray_ready.py` | same |
| No-context sleep behavior, cancellation-at-timeout, callback cleanup, and pre-cancelled requests remain stable | `tests/unit_tests/test_sky/utils/test_context_utils.py` | `pytest -o addopts='' tests/unit_tests/test_sky/utils/test_context_utils.py -q` |
| Adjacent cluster readiness and handle behavior remain stable | `tests/unit_tests/test_sky/backends/test_cloud_vm_ray_backend.py` and `tests/unit_tests/test_sky/test_refresh_status_no_reread.py` | focused pytest commands |
| API cluster operations retain integration coverage | cluster-focused cases collected from `tests/test_api.py` | focused pytest command |
| No material performance regression | exact call-count assertions plus a zero-timeout bridge microbenchmark | standalone exact-head benchmark |

`.github/workflows/pytest.yml` runs the complete `tests/unit_tests` tree in the
`Unit Tests` job without a path filter excluding either mapped test file.
Repository static workflows cover formatting, mypy, pylint, Ruff, and import
boundaries for the production path.

## Performance contract

Cancelled requests remove later subprocesses. Active requests retain the same
number and duration of waits and the same remote-call complexity. The only
added active-path work is the already-established local request-context lookup
and event callback registration around a backoff of at least five seconds. A
zero-timeout microbenchmark must show that overhead is negligible relative to
that minimum wait before merge.

## Rollout and rollback

The change is internal and requires no migration. Reverting the two call sites
restores the prior behavior. Merge only after the mapped exact-head local tests,
performance proof, complete GitHub check rollup, and review audit succeed.
