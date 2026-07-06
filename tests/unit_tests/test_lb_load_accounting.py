"""Load-map accounting must be exactly paired on every request outcome.

Adversarial routing review (2026-07-06): the LB incremented in-flight
load at selection but only decremented via the streaming background
task — httpx errors, missing clients, and body failures leaked slots,
permanently skewing routing away from replicas that ever failed; a
replica pruned mid-stream could be recreated at -1 (phantom capacity).
Deterministic min() tie-breaking also biased cold starts.
"""
# pylint: disable=protected-access
import asyncio
import threading
from unittest import mock

import httpx

from sky.serve import load_balancer as lb_module
from sky.serve import load_balancing_policies as lb_policies


def _make_least_load():
    policy = lb_policies.LeastLoadPolicy()
    policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
    return policy


class TestLoadMapPairing:
    """pre/post hooks keep counts exact under every outcome."""

    def test_pre_post_roundtrip(self):
        policy = _make_least_load()
        policy.pre_execute_hook('http://a:8080', None)
        policy.pre_execute_hook('http://a:8080', None)
        policy.post_execute_hook('http://a:8080', None)
        policy.post_execute_hook('http://a:8080', None)
        assert policy.load_map['http://a:8080'] == 0

    def test_post_on_pruned_key_does_not_recreate_negative(self):
        policy = _make_least_load()
        policy.pre_execute_hook('http://a:8080', None)
        # Replica leaves the ready set mid-stream: key pruned.
        policy.set_ready_replicas(['http://b:8080'])
        assert 'http://a:8080' not in policy.load_map
        # The stream finishes later: must not recreate the key at -1.
        policy.post_execute_hook('http://a:8080', None)
        assert 'http://a:8080' not in policy.load_map

    def test_stale_generation_release_ignored_after_prune_readd(self):
        """ABA: a release from before a prune must not decrement the
        re-added generation's counter (old-A release stealing new-A's
        in-flight slot)."""
        policy = _make_least_load()
        token_old = policy.pre_execute_hook('http://a:8080', None)
        # A pruned, then re-added (new generation).
        policy.set_ready_replicas(['http://b:8080'])
        policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
        # New stream on new-A.
        policy.pre_execute_hook('http://a:8080', None)
        assert policy.load_map['http://a:8080'] == 1
        # Old stream finally ends: stale-generation release ignored.
        policy.post_execute_hook('http://a:8080', None, token_old)
        assert policy.load_map['http://a:8080'] == 1

    def test_post_clamps_at_zero(self):
        policy = _make_least_load()
        policy.post_execute_hook('http://b:8080', None)
        assert policy.load_map['http://b:8080'] == 0


class TestTieBreakRandomization:
    """Cold-start ties spread instead of hammering URL-order-first."""

    def test_least_load_zero_ties_spread(self):
        policy = _make_least_load()
        seen = {policy._select_replica(None) for _ in range(100)}
        assert seen == {'http://a:8080', 'http://b:8080'}

    def test_instance_aware_zero_ties_spread(self):
        policy = lb_policies.InstanceAwareLeastLoadPolicy()
        policy.set_ready_replicas(['http://a:8080', 'http://b:8080'])
        policy.set_replica_info({
            'http://a:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            },
            'http://b:8080': {
                'gpu_type': 'L4',
                'gpu_count': '1'
            },
        })
        policy.set_target_qps_per_accelerator({'L4': 0.1})
        seen = {policy._select_replica(None) for _ in range(100)}
        assert seen == {'http://a:8080', 'http://b:8080'}


def _make_lb(policy, client_pool):
    balancer = object.__new__(lb_module.SkyServeLoadBalancer)
    balancer._load_balancing_policy = policy
    balancer._client_pool = client_pool
    balancer._client_pool_lock = threading.Lock()
    balancer._stream_timeout_seconds = 5
    return balancer


class TestProxySlotRelease:
    """Every non-streaming exit of _proxy_request_to releases the slot."""

    def _request(self):
        request = mock.MagicMock()
        request.url.path = '/x'
        request.url.query = ''
        request.headers.raw = []

        async def _body():
            return b''

        request.body = _body
        return request

    def test_missing_client_releases_slot(self):
        policy = mock.MagicMock()
        balancer = _make_lb(policy, client_pool={})
        result = asyncio.run(
            balancer._proxy_request_to('http://gone:8080', self._request()))
        assert isinstance(result, RuntimeError)
        policy.pre_execute_hook.assert_called_once()
        policy.post_execute_hook.assert_called_once()

    def test_httpx_error_releases_slot(self):
        policy = mock.MagicMock()
        client = mock.MagicMock()
        client.build_request.side_effect = httpx.RequestError('boom')
        balancer = _make_lb(policy, client_pool={'http://a:8080': client})
        result = asyncio.run(
            balancer._proxy_request_to('http://a:8080', self._request()))
        assert isinstance(result, httpx.RequestError)
        policy.post_execute_hook.assert_called_once()

    def test_streaming_success_transfers_slot_to_background(self):
        policy = mock.MagicMock()
        client = mock.MagicMock()
        send_response = mock.MagicMock()
        send_response.status_code = 200
        send_response.headers = {}

        async def _send(*args, **kwargs):
            return send_response

        client.send = _send
        balancer = _make_lb(policy, client_pool={'http://a:8080': client})
        result = asyncio.run(
            balancer._proxy_request_to('http://a:8080', self._request()))
        assert not isinstance(result, Exception)
        # Slot NOT released synchronously: it belongs to the stream's
        # background task now.
        policy.post_execute_hook.assert_not_called()

    def test_midstream_failure_releases_slot_via_iterator_finally(self):
        """Starlette runs background tasks only AFTER a successful
        stream: a mid-stream failure must release via the iterator's
        finally instead (and exactly once)."""
        policy = mock.MagicMock()
        client = mock.MagicMock()
        send_response = mock.MagicMock()
        send_response.status_code = 200
        send_response.headers = {}

        async def _aiter_raw():
            yield b'chunk-1'
            raise httpx.ReadError('upstream reset mid-stream')

        send_response.aiter_raw = _aiter_raw

        async def _aclose():
            return None

        send_response.aclose = _aclose

        async def _send(*args, **kwargs):
            return send_response

        client.send = _send
        balancer = _make_lb(policy, client_pool={'http://a:8080': client})

        async def _run():
            response = await balancer._proxy_request_to('http://a:8080',
                                                        self._request())
            assert not isinstance(response, Exception)
            # Drain the wrapped iterator like the server would; the
            # failure mid-stream must trigger the finally-release.
            chunks = []
            try:
                async for chunk in response.body_iterator:
                    chunks.append(chunk)
            except httpx.ReadError:
                pass
            return chunks

        chunks = asyncio.run(_run())
        assert chunks == [b'chunk-1']
        policy.post_execute_hook.assert_called_once()

    def test_release_is_idempotent_across_iterator_and_background(self):
        """Normal completion: iterator finally AND the background task
        both fire — the slot must be released exactly once."""
        policy = mock.MagicMock()
        client = mock.MagicMock()
        send_response = mock.MagicMock()
        send_response.status_code = 200
        send_response.headers = {}

        async def _aiter_raw():
            yield b'done'

        send_response.aiter_raw = _aiter_raw

        async def _aclose():
            return None

        send_response.aclose = _aclose

        async def _send(*args, **kwargs):
            return send_response

        client.send = _send
        balancer = _make_lb(policy, client_pool={'http://a:8080': client})

        async def _run():
            response = await balancer._proxy_request_to('http://a:8080',
                                                        self._request())
            async for _ in response.body_iterator:
                pass
            # Simulate Starlette running the background task afterwards.
            await response.background()

        asyncio.run(_run())
        policy.post_execute_hook.assert_called_once()
