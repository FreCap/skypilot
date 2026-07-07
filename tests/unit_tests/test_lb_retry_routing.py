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


def _request():
    request = mock.MagicMock()
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

    def test_proxy_loop_excludes_failed_urls(self):
        policy = self._busy_policy()
        balancer = object.__new__(lb_module.SkyServeLoadBalancer)
        balancer._load_balancing_policy = policy
        balancer._client_pool_lock = threading.Lock()
        balancer._request_aggregator = mock.MagicMock()
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
