"""Retry routing: failed-URL exclusion + configurable retriable statuses.

Reproduced flaw: the retry loop re-selected with no memory of failures,
and a dead-but-not-yet-pruned replica sits at load 0 (its failed
attempts release their slots) while healthy replicas carry traffic — so
least-load made the corpse the strict minimum and every retry
deterministically re-selected it, 500ing the client despite healthy
capacity.
"""
# pylint: disable=protected-access
import asyncio
import threading
import unittest
from unittest import mock

import fastapi
import httpx

from sky.serve import load_balancer as lb_module
from sky.serve import load_balancing_policies as lb_policies


def _request(method='POST'):
    request = mock.MagicMock()
    request.method = method
    request.url.path = '/x'
    request.url.query = ''
    request.headers.raw = []

    async def _body():
        return b''

    async def _is_disconnected():
        return False

    request.body = _body
    request.is_disconnected = _is_disconnected
    return request


class TestRetryExclusion(unittest.TestCase):
    """Retry selection excludes failed URLs without losing fallbacks."""

    def _busy_policy(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://dead:8080', 'http://busy:8080'])
        policy.load_map['http://busy:8080'] = 1
        policy.load_map['http://dead:8080'] = 0
        return policy

    def test_magnet_regression_retry_leaves_dead_replica(self):
        # Without exclusion this deterministically picks dead twice.
        policy = self._busy_policy()
        first = policy.select_replica(_request())
        self.assertEqual(first, 'http://dead:8080')
        second = policy.select_replica(_request(), exclude={first})
        self.assertEqual(second, 'http://busy:8080')

    def test_exclusion_falls_back_when_all_failed(self):
        # A lone replica with a transient blip gets the remaining
        # attempts instead of a guaranteed error.
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://only:8080'])
        picked = policy.select_replica(_request(), exclude={'http://only:8080'})
        self.assertEqual(picked, 'http://only:8080')

    def test_instance_aware_exclusion(self):
        policy = lb_policies.InstanceAwareLeastLoadPolicy()
        policy.set_ready_replicas(['http://dead:8080', 'http://busy:8080'])
        policy.load_map['http://busy:8080'] = 1
        policy.load_map['http://dead:8080'] = 0
        self.assertEqual(
            policy.select_replica(_request(), exclude={'http://dead:8080'}),
            'http://busy:8080')

    def test_empty_eligibility_never_falls_back_to_ready_set(self):
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://full:8080'])
        self.assertIsNone(policy.select_replica(_request(), eligible=set()))

    def test_instance_aware_eligibility_is_strict_before_load_scoring(self):
        policy = lb_policies.InstanceAwareLeastLoadPolicy()
        policy.set_ready_replicas(['http://full:8080', 'http://free:8080'])
        # The full URL otherwise wins least-load. Capacity eligibility must be
        # authoritative rather than a soft score.
        policy.load_map['http://full:8080'] = 0
        policy.load_map['http://free:8080'] = 10
        self.assertEqual(
            policy.select_replica(_request(), eligible={'http://free:8080'}),
            'http://free:8080')

    def test_proxy_loop_excludes_failed_urls(self):
        policy = self._busy_policy()
        balancer = object.__new__(lb_module.SkyServeLoadBalancer)
        balancer._load_balancing_policy = policy
        balancer._client_pool_lock = threading.Lock()
        balancer._request_aggregator = mock.MagicMock()
        balancer._max_retries = lb_module.constants.LB_MAX_RETRY
        balancer._retry_initial_backoff_seconds = (
            lb_module.constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)
        balancer._replica_dead_failures = {}
        balancer._replica_quarantine_until = {}
        attempts = []

        async def _proxy(url, request):
            del request
            attempts.append(url)
            if url == 'http://dead:8080':
                return httpx.ConnectError('dead')
            return fastapi.responses.Response(status_code=200)

        balancer._proxy_request_to = _proxy
        response = asyncio.run(balancer._proxy_with_retries(_request()))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(attempts, ['http://dead:8080', 'http://busy:8080'])


class TestRetriableStatusCodes(unittest.TestCase):
    """Configured response statuses participate safely in retry routing."""

    def _balancer(self, retriable, client):
        balancer = object.__new__(lb_module.SkyServeLoadBalancer)
        balancer._load_balancing_policy = mock.MagicMock()
        balancer._load_balancing_policy.pre_execute_hook.return_value = None
        balancer._client_pool = {'http://a:8080': client}
        balancer._client_pool_lock = threading.Lock()
        balancer._stream_timeout_seconds = 5
        balancer._client_close_tasks = set()
        balancer._retriable_status_codes = frozenset(retriable)
        return balancer

    def _client_returning(self, status_code):
        client = mock.MagicMock()
        setattr(client, lb_module._INFLIGHT_ATTR, 0)
        client.build_request = mock.Mock(return_value=mock.Mock())
        response = mock.MagicMock()
        response.status_code = status_code
        closed = {'v': False}

        async def _aclose():
            closed['v'] = True

        response.aclose = _aclose

        async def _send(*args, **kwargs):
            del args, kwargs
            return response

        client.send = _send
        return client, closed

    def test_configured_status_is_returned_as_retriable_error(self):
        client, closed = self._client_returning(503)
        balancer = self._balancer([503, 429], client)
        result = asyncio.run(
            balancer._proxy_request_to('http://a:8080', _request()))
        self.assertIsInstance(result, lb_module._RetriableStatusError)
        self.assertTrue(closed['v'])  # body discarded, never streamed
        # Slot + client refcount released via the not-released finally.
        balancer._load_balancing_policy.post_execute_hook.assert_called_once()
        self.assertEqual(getattr(client, lb_module._INFLIGHT_ATTR), 0)

    def test_default_passes_status_through(self):
        client, _ = self._client_returning(503)
        balancer = self._balancer([], client)
        result = asyncio.run(
            balancer._proxy_request_to('http://a:8080', _request()))
        # Not an exception: streamed to the client verbatim.
        self.assertNotIsInstance(result, Exception)

    def test_unlisted_status_passes_through(self):
        client, _ = self._client_returning(500)
        balancer = self._balancer([503, 429], client)
        result = asyncio.run(
            balancer._proxy_request_to('http://a:8080', _request()))
        self.assertNotIsInstance(result, Exception)


class TestRetryTuning(unittest.TestCase):
    """max_retries and retry backoff are service-configurable."""

    def _balancer(self, max_retries=None, backoff=None):
        balancer = object.__new__(lb_module.SkyServeLoadBalancer)
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
        balancer._load_balancing_policy = policy
        balancer._client_pool_lock = threading.Lock()
        balancer._request_aggregator = mock.MagicMock()
        balancer._max_retries = (max_retries if max_retries is not None else
                                 lb_module.constants.LB_MAX_RETRY)
        balancer._retry_initial_backoff_seconds = (
            backoff if backoff is not None else
            lb_module.constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)
        balancer._replica_dead_failures = {}
        balancer._replica_quarantine_until = {}
        return balancer

    def _run_all_failing(self, balancer):
        attempts = []

        async def _proxy(url, request):
            del request
            attempts.append(url)
            return httpx.ConnectError('down')

        balancer._proxy_request_to = _proxy
        with mock.patch('sky.serve.load_balancer.asyncio.sleep',
                        new=mock.AsyncMock()):
            with self.assertRaises(fastapi.HTTPException) as ctx:
                asyncio.run(balancer._proxy_with_retries(_request()))
        return attempts, ctx.exception

    def test_max_retries_bounds_attempts(self):
        attempts, exc = self._run_all_failing(self._balancer(max_retries=5))
        self.assertEqual(len(attempts), 5)
        self.assertEqual(exc.status_code, 503)
        self.assertIn('not dispatched', exc.detail)
        self.assertEqual(exc.headers['Retry-After'],
                         str(lb_module.constants.LB_503_RETRY_AFTER_SECONDS))

    def test_default_max_retries_unchanged(self):
        attempts, _ = self._run_all_failing(self._balancer())
        self.assertEqual(len(attempts), lb_module.constants.LB_MAX_RETRY)

    def test_backoff_seeded_from_config(self):
        balancer = self._balancer(max_retries=2, backoff=0.25)
        captured = {}
        real_backoff = lb_module.common_utils.Backoff

        def _spy(initial_backoff):
            captured['initial'] = initial_backoff
            return real_backoff(initial_backoff=initial_backoff)

        with mock.patch.object(lb_module.common_utils,
                               'Backoff',
                               side_effect=_spy):
            self._run_all_failing(balancer)
        self.assertEqual(captured['initial'], 0.25)

    def test_definitely_not_dispatched_classification(self):
        for error in (lb_module._PreDispatchError('no client'),
                      httpx.ConnectError('refused'),
                      httpx.ConnectTimeout('timed out'),
                      httpx.PoolTimeout('pool full')):
            with self.subTest(error=type(error).__name__):
                self.assertTrue(lb_module._is_definitely_not_dispatched(error))
        for error in (httpx.ReadError('reset after send'),
                      httpx.WriteError('reset while sending'),
                      httpx.RemoteProtocolError('bad response')):
            with self.subTest(error=type(error).__name__):
                self.assertFalse(lb_module._is_definitely_not_dispatched(error))


class TestRoutingSpecSync(unittest.TestCase):
    """Retry tuning must ride the live routing-spec sync: external LBs
    never see the spawn args, and `sky serve update` must apply without
    an LB respawn."""

    def _balancer(self):
        balancer = object.__new__(lb_module.SkyServeLoadBalancer)
        balancer._load_balancing_policy = lb_policies.LeastLoadPolicy()
        balancer._load_balancing_policy_name = 'least_load'
        balancer._client_pool_lock = threading.Lock()
        balancer._stream_timeout_seconds = 120
        balancer._retriable_status_codes = frozenset()
        balancer._max_retries = lb_module.constants.LB_MAX_RETRY
        balancer._retry_initial_backoff_seconds = (
            lb_module.constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)
        return balancer

    def test_sync_applies_retry_tuning(self):
        balancer = self._balancer()
        balancer._apply_routing_spec({
            'load_balancing_policy_name': 'least_load',
            'stream_timeout_seconds': 3700,
            'retriable_status_codes': [503, 429],
            'max_retries': 5,
            'retry_initial_backoff_seconds': 0.5,
        })
        self.assertEqual(balancer._retriable_status_codes, frozenset({503,
                                                                      429}))
        self.assertEqual(balancer._max_retries, 5)
        self.assertEqual(balancer._retry_initial_backoff_seconds, 0.5)

    def test_sync_unset_fields_reset_to_defaults(self):
        # A new service version that REMOVED the overrides must not leave
        # stale values behind.
        balancer = self._balancer()
        balancer._retriable_status_codes = frozenset({503})
        balancer._max_retries = 9
        balancer._retry_initial_backoff_seconds = 9.0
        balancer._apply_routing_spec({
            'load_balancing_policy_name': 'least_load',
            'retriable_status_codes': None,
            'max_retries': None,
            'retry_initial_backoff_seconds': None,
        })
        self.assertEqual(balancer._retriable_status_codes, frozenset())
        self.assertEqual(balancer._max_retries,
                         lb_module.constants.LB_MAX_RETRY)
        self.assertEqual(balancer._retry_initial_backoff_seconds,
                         lb_module.constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)


class TestRetryShortCircuit(unittest.TestCase):
    """Retries must stop when there is nothing left worth trying.

    Without this, an empty or fully-shedding fleet burned every attempt
    WITH full backoff (~7.5s of sleep at fleet values) before the 503 —
    multiplied by the client retry layer above during outages/warm-up.
    """

    def _balancer(self, replicas, proxy):
        balancer = object.__new__(lb_module.SkyServeLoadBalancer)
        policy = lb_policies.LeastLoadPolicy()
        policy.set_ready_replicas(list(replicas))
        balancer._load_balancing_policy = policy
        balancer._client_pool_lock = threading.Lock()
        balancer._request_aggregator = mock.MagicMock()
        balancer._max_retries = 5
        balancer._retry_initial_backoff_seconds = 0.5
        balancer._replica_dead_failures = {}
        balancer._replica_quarantine_until = {}
        balancer._proxy_request_to = proxy
        return balancer

    def _run(self, balancer, request=None):
        sleeps = []

        async def _sleep(t):
            sleeps.append(t)

        with mock.patch('sky.serve.load_balancer.asyncio.sleep', new=_sleep):
            with self.assertRaises(fastapi.HTTPException) as ctx:
                asyncio.run(balancer._proxy_with_retries(request or _request()))
        return sleeps, ctx.exception

    def test_no_replicas_fails_fast_with_retry_after(self):

        async def _proxy(url, request):
            raise AssertionError('must not be called')

        balancer = self._balancer([], _proxy)
        sleeps, exc = self._run(balancer)
        self.assertEqual(sleeps, [])  # zero backoff sleeps
        self.assertEqual(exc.status_code, 503)
        self.assertEqual(exc.headers['Retry-After'],
                         str(lb_module.constants.LB_503_RETRY_AFTER_SECONDS))
        balancer._request_aggregator.add_request_classification.assert_called_once_with(
            rejected=True)

    def test_all_replicas_shedding_short_circuits(self):
        attempts = []

        async def _proxy(url, request):
            del request
            attempts.append(url)
            return lb_module._RetriableStatusError(503, url)

        balancer = self._balancer(['http://a:8080', 'http://b:8080'], _proxy)
        sleeps, exc = self._run(balancer)
        # One shed per replica, then out — not 5 attempts / 4 sleeps.
        self.assertEqual(len(attempts), 2)
        self.assertEqual(len(sleeps), 1)
        self.assertEqual(exc.status_code, 503)
        self.assertIn('Retry-After', exc.headers)
        balancer._request_aggregator.add_request_classification.assert_called_once_with(
            rejected=True)

    def test_retry_budget_exhausted_on_shedding_is_unavailable(self):
        attempts = []

        async def _proxy(url, request):
            del request
            attempts.append(url)
            return lb_module._RetriableStatusError(429, url)

        balancer = self._balancer(
            ['http://a:8080', 'http://b:8080', 'http://c:8080'], _proxy)
        balancer._max_retries = 1
        request = _request()
        request.headers.get.return_value = 'stable-job-1'
        sleeps, exc = self._run(balancer, request)
        self.assertEqual(len(attempts), 1)
        self.assertEqual(sleeps, [])
        self.assertEqual(exc.status_code, 503)
        self.assertIn('configured retriable', exc.detail)
        self.assertEqual(exc.headers['Retry-After'],
                         str(lb_module.constants.LB_503_RETRY_AFTER_SECONDS))
        self.assertEqual(balancer._rejected_in_window(), 1)

    def test_transport_failures_keep_fallback_attempts(self):
        # A lone replica's pre-dispatch connection blip proves the POST did not
        # reach the application, so it can still recover transparently.
        attempts = []

        async def _proxy(url, request):
            del request
            attempts.append(url)
            if len(attempts) < 3:
                return httpx.ConnectError('blip')
            return fastapi.responses.Response(status_code=200)

        balancer = self._balancer(['http://only:8080'], _proxy)
        with mock.patch('sky.serve.load_balancer.asyncio.sleep',
                        new=mock.AsyncMock()):
            response = asyncio.run(balancer._proxy_with_retries(_request()))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(attempts), 3)

    def test_post_ambiguous_transport_failure_is_not_retried(self):
        # A read/protocol failure may have followed acceptance. Replaying the
        # POST on another replica would silently create a duplicate job.
        def _proxy_for(proxy_error):
            attempts = []

            async def _proxy(url, request):
                del request
                attempts.append(url)
                if len(attempts) == 1:
                    return proxy_error
                return fastapi.responses.Response(status_code=202)

            return attempts, _proxy

        for error in (httpx.ReadError('reset after send'),
                      httpx.RemoteProtocolError('bad response after send')):
            with self.subTest(error=type(error).__name__):
                attempts, proxy = _proxy_for(error)

                balancer = self._balancer(['http://a:8080', 'http://b:8080'],
                                          proxy)
                with mock.patch('sky.serve.load_balancer.asyncio.sleep',
                                new=mock.AsyncMock()):
                    with self.assertRaises(fastapi.HTTPException) as ctx:
                        asyncio.run(balancer._proxy_with_retries(_request()))
                self.assertEqual(ctx.exception.status_code, 502)
                self.assertIn('was not replayed', ctx.exception.detail)
                self.assertEqual(len(attempts), 1)

    def test_get_ambiguous_transport_failure_remains_retryable(self):
        attempts = []

        async def _proxy(url, request):
            del url, request
            attempts.append(1)
            if len(attempts) == 1:
                return httpx.ReadError('reset after send')
            return fastapi.responses.Response(status_code=200)

        balancer = self._balancer(['http://a:8080'], _proxy)
        with mock.patch('sky.serve.load_balancer.asyncio.sleep',
                        new=mock.AsyncMock()):
            response = asyncio.run(
                balancer._proxy_with_retries(_request(method='GET')))
        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(attempts), 2)

    def test_get_ambiguous_exhaustion_does_not_claim_no_dispatch(self):
        attempts = []

        async def _proxy(url, request):
            del url, request
            attempts.append(1)
            return httpx.ReadError('reset after send')

        balancer = self._balancer(['http://a:8080'], _proxy)
        balancer._max_retries = 2
        sleeps, exc = self._run(balancer, _request(method='GET'))
        self.assertEqual(len(attempts), 2)
        self.assertEqual(len(sleeps), 1)
        self.assertEqual(exc.status_code, 502)
        self.assertIn('Upstream outcome is unknown', exc.detail)

    def test_mixed_fleet_shed_then_healthy_succeeds(self):

        async def _proxy(url, request):
            del request
            if url == 'http://shed:8080':
                return lb_module._RetriableStatusError(503, url)
            return fastapi.responses.Response(status_code=200)

        balancer = self._balancer(['http://shed:8080', 'http://ok:8080'],
                                  _proxy)
        balancer._load_balancing_policy.load_map['http://ok:8080'] = 1
        with mock.patch('sky.serve.load_balancer.asyncio.sleep',
                        new=mock.AsyncMock()):
            response = asyncio.run(balancer._proxy_with_retries(_request()))
        self.assertEqual(response.status_code, 200)
