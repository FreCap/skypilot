"""LoadBalancer: Distribute any incoming request to all ready replicas."""
import argparse
import asyncio
import contextlib
import logging
import os
import signal
import threading
import time
import traceback
from typing import Any, Dict, List, Optional, Set, Union

import aiohttp
import fastapi
import httpx
from starlette import background
import uvicorn

from sky import sky_logging
from sky.serve import constants
from sky.serve import load_balancing_policies as lb_policies
from sky.serve import serve_utils
from sky.utils import common_utils

logger = sky_logging.init_logger(__name__)

# Per-client in-flight request counter attribute. Attached to the
# httpx.AsyncClient OBJECT (not keyed by URL): a URL pruned and re-added
# gets a fresh client while the old one is still draining, and the two
# must not share a counter.
_INFLIGHT_ATTR = '_sky_inflight_requests'


class _RetriableStatusError(Exception):
    """A replica answered with a status the service marked retriable.

    Returned from _proxy_request_to like transport errors so
    _proxy_with_retries re-routes the (idempotent) request to another
    replica. Only statuses listed in the service's
    load_balancer.retriable_status_codes take this path — everything
    else streams to the client verbatim.
    """

    def __init__(self, status_code: int, url: str) -> None:
        super().__init__(
            f'replica {url} answered retriable status {status_code}')
        self.status_code = status_code


def _is_dead_connection_error(exc: Exception) -> bool:
    """Whether a proxy failure indicates a DEAD replica vs a saturated one.

    A healthy replica overloaded at high RPS trips the connect/read timeout
    (httpx.TimeoutException), so timeouts must NOT count toward eviction --
    evicting a merely-saturated replica shrinks capacity under load and
    cascades. Only genuine connection failures (refused/reset: NetworkError,
    ProtocolError) indicate a dead replica worth evicting.
    """
    if isinstance(exc, httpx.TimeoutException):
        return False
    return isinstance(exc, (httpx.NetworkError, httpx.ProtocolError))


class _DrainableServer(uvicorn.Server):
    """A uvicorn Server that drains gracefully on SIGTERM.

    uvicorn installs its own SIGTERM/SIGINT handlers inside
    ``Server.serve()`` via ``capture_signals()`` (there is no
    ``install_signal_handlers`` config knob in modern uvicorn), and its default
    handler sets ``should_exit`` immediately -- which would kill in-flight
    requests and skip the deregister step. We instead install our own
    event-loop signal handlers (asyncio-safe) and suppress uvicorn's, so
    SIGTERM begins draining (fail readiness + stop the controller sync) and the
    server only exits after ``LB_DRAIN_GRACE_SECONDS`` -- long enough for k8s to
    pull the pod from the Service and for in-flight requests to finish. A
    second signal / SIGINT exits promptly.
    """

    def __init__(self, config: 'uvicorn.Config', on_drain: 'Any') -> None:
        super().__init__(config)
        self._on_drain = on_drain
        self._own_signals = False

    @contextlib.contextmanager
    def capture_signals(self):
        # Suppress uvicorn's own signal handlers when we installed ours;
        # otherwise fall back to uvicorn's handling (e.g. platforms without
        # loop.add_signal_handler).
        if self._own_signals:
            yield
        else:
            with super().capture_signals():
                yield

    async def serve_with_drain(self) -> None:
        loop = asyncio.get_running_loop()

        def _on_sigterm() -> None:
            self._on_drain()
            loop.call_later(constants.LB_DRAIN_GRACE_SECONDS,
                            lambda: setattr(self, 'should_exit', True))

        try:
            loop.add_signal_handler(signal.SIGTERM, _on_sigterm)
            loop.add_signal_handler(signal.SIGINT,
                                    lambda: setattr(self, 'should_exit', True))
            self._own_signals = True
        except NotImplementedError:
            # add_signal_handler is unavailable (e.g. Windows); let uvicorn
            # manage signals (no graceful drain, but a correct shutdown).
            self._own_signals = False
        await self.serve()


class SkyServeLoadBalancer:
    """SkyServeLoadBalancer: distribute incoming traffic with proxy.

    This class accept any traffic to the controller and proxies it
    to the appropriate endpoint replica according to the load balancing
    policy.
    """

    def __init__(
        self,
        controller_url: str,
        load_balancer_port: int,
        load_balancing_policy_name: Optional[str] = None,
        tls_credential: Optional[serve_utils.TLSCredential] = None,
        target_qps_per_replica: Optional[Union[float, Dict[str, float]]] = None,
        stream_timeout_seconds: Optional[int] = None,
        retriable_status_codes: Optional[List[int]] = None,
        max_retries: Optional[int] = None,
        retry_initial_backoff_seconds: Optional[float] = None,
    ) -> None:
        """Initialize the load balancer.

        The routing spec -- load-balancing policy, per-replica target QPS, and
        stream timeout -- is fetched from the controller over the
        load_balancer_sync channel (see `_apply_routing_spec`), so `sky serve
        update` changes to those fields reach a running LB without re-rolling
        it. The corresponding constructor args are only a bootstrap seed (used
        by the in-pod caller, which already has the spec): until the first sync
        lands, the LB serves with whatever policy is built here, and the
        readiness gate keeps traffic away until that first sync arrives. A
        standalone LB passes None for all three and picks them up from sync.

        Args:
            controller_url: The URL of the controller.
            load_balancer_port: The port where the load balancer listens to.
            load_balancing_policy_name: Seed load balancing policy name.
                Defaults to None (the default policy until the first sync).
            tls_credentials: The TLS credentials for HTTPS endpoint. Defaults
                to None.
            target_qps_per_replica: Seed target QPS per replica for
                instance-aware load balancing. Can be a float or dict mapping
                GPU types to QPS. Defaults to None.
            stream_timeout_seconds: Seed timeout in seconds for proxied
                responses. Defaults to None (the built-in default until synced).
        """
        self._app = fastapi.FastAPI()
        self._controller_url: str = controller_url
        self._load_balancer_port: int = load_balancer_port
        # Use the registry to create the load balancing policy. Track the
        # resolved policy name so a sync only rebuilds the policy object when
        # the name actually changes (a policy swap is rare -- only on an
        # update that changes the policy).
        self._load_balancing_policy_name: str = (
            lb_policies.LoadBalancingPolicy.make_policy_name(
                load_balancing_policy_name))
        self._load_balancing_policy = lb_policies.LoadBalancingPolicy.make(
            self._load_balancing_policy_name)

        # Set accelerator QPS for instance-aware policies
        if (target_qps_per_replica and
                isinstance(target_qps_per_replica, dict) and
                isinstance(self._load_balancing_policy,
                           lb_policies.InstanceAwareLeastLoadPolicy)):
            self._load_balancing_policy.set_target_qps_per_accelerator(
                target_qps_per_replica)

        logger.info('Starting load balancer with policy '
                    f'{self._load_balancing_policy_name}.')
        self._request_aggregator: serve_utils.RequestsAggregator = (
            serve_utils.RequestTimestamp())
        self._tls_credential: Optional[serve_utils.TLSCredential] = (
            tls_credential)
        self._stream_timeout_seconds: int = (
            stream_timeout_seconds if stream_timeout_seconds is not None else
            constants.DEFAULT_LB_STREAM_TIMEOUT)
        # Replica responses with these statuses are re-routed like
        # transport failures (empty = never, the default). Safe only for
        # idempotent workloads and "not now" statuses (503/429): the body
        # is discarded before any byte reaches the client.
        self._retriable_status_codes = frozenset(retriable_status_codes or ())
        # Retry-loop tuning (service YAML load_balancer.max_retries /
        # retry_initial_backoff_seconds). With failed-URL exclusion, more
        # retries = more distinct replicas tried before the client sees an
        # error; the backoff prices how fast we fail over.
        self._max_retries: int = (max_retries if max_retries is not None else
                                  constants.LB_MAX_RETRY)
        self._retry_initial_backoff_seconds: float = (
            retry_initial_backoff_seconds if retry_initial_backoff_seconds
            is not None else constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)
        # TODO(tian): httpx.Client has a resource limit of 100 max connections
        # for each client. We should wait for feedback on the best max
        # connections.
        # Reference: https://www.python-httpx.org/advanced/resource-limits/
        #
        # If more than 100 requests are sent to the same replica, the
        # httpx.Client will queue the requests and send them when a
        # connection is available.
        # Reference: https://github.com/encode/httpcore/blob/a8f80980daaca98d556baea1783c5568775daadc/httpcore/_async/connection_pool.py#L69-L71 # pylint: disable=line-too-long
        self._client_pool: Dict[str, httpx.AsyncClient] = dict()
        # We need this lock to avoid getting from the client pool while
        # updating it from _sync_with_controller.
        self._client_pool_lock: threading.Lock = threading.Lock()
        # Passive replica eviction state, guarded by _client_pool_lock (the
        # same lock that guards the policy's ready set). _replica_dead_failures
        # counts consecutive dead-connection failures per replica;
        # _replica_quarantine_until maps a replica URL to the wall-clock time
        # until which it stays out of routing.
        self._replica_dead_failures: Dict[str, int] = dict()
        self._replica_quarantine_until: Dict[str, float] = dict()
        # Rollout state. `_ready` flips true after the first successful
        # controller sync, so k8s never routes to a cold LB. `_draining` flips
        # true on SIGTERM, which fails readiness and stops the controller sync
        # (deregister-before-drain).
        self._ready: bool = False
        self._draining: bool = False
        # Strong refs to in-progress drain-close tasks (see
        # _drain_and_close_client); a bare create_task result can be GCed.
        self._client_close_tasks: Set[asyncio.Task] = set()

    def _quarantined_replicas(self) -> Set[str]:
        """Replica URLs currently quarantined (TTL not yet expired).

        Must be called while holding `_client_pool_lock`.
        """
        now = time.time()
        return {
            url for url, until in self._replica_quarantine_until.items()
            if until > now
        }

    def _quarantine_replica(self, url: str) -> None:
        """Remove a dead replica from routing for the quarantine TTL.

        Must be called while holding `_client_pool_lock`. Drops the replica
        from the policy's ready set immediately so in-flight routing stops
        selecting it; the sync loop keeps it out (even if the controller still
        lists it as ready) until the TTL expires.
        """
        self._replica_quarantine_until[url] = (
            time.time() + constants.LB_EVICTION_QUARANTINE_SECONDS)
        self._replica_dead_failures.pop(url, None)
        remaining = [
            u for u in self._load_balancing_policy.ready_replicas if u != url
        ]
        self._load_balancing_policy.set_ready_replicas(remaining)
        logger.warning(
            f'Evicted replica {url} after '
            f'{constants.LB_EVICTION_CONSECUTIVE_FAILURES} consecutive '
            f'dead-connection failures; quarantined for '
            f'{constants.LB_EVICTION_QUARANTINE_SECONDS}s.')

    def _record_proxy_outcome(
        self, url: str,
        response_or_exception: Union['fastapi.responses.Response', Exception]
    ) -> None:
        """Update per-replica eviction bookkeeping after a proxy attempt.

        Eviction is deliberately scoped to TCP-dead replicas (connection
        refused/reset before headers): those are the ones whose drained
        in-flight slots read as least-loaded and attract preferential routing.
        A dead-connection failure increments the consecutive count and
        quarantines once the threshold is reached. Everything else -- a
        response of any status (incl. 5xx: the replica is reachable and
        releasing its slot), a saturation timeout, or a mid-stream failure
        after headers -- counts as a completed attempt and clears the streak.
        Exception: statuses configured as retriable arrive here as
        _RetriableStatusError and are INERT for eviction — neither a dead
        failure (the replica answered; it is alive) nor a streak-clearing
        success (a shedding replica should not launder an in-progress
        dead-connection streak).
        Application errors and saturation are intentionally NOT eviction
        signals (evicting a reachable-but-slow replica shrinks capacity).
        """
        with self._client_pool_lock:
            if isinstance(response_or_exception, Exception):
                if _is_dead_connection_error(response_or_exception):
                    count = self._replica_dead_failures.get(url, 0) + 1
                    self._replica_dead_failures[url] = count
                    if count >= constants.LB_EVICTION_CONSECUTIVE_FAILURES:
                        self._quarantine_replica(url)
            else:
                self._replica_dead_failures.pop(url, None)
                self._replica_quarantine_until.pop(url, None)

    def _apply_routing_spec(self, routing_spec: Dict[str, Any]) -> None:
        """Apply a routing spec fetched from the controller.

        Must be called while holding `_client_pool_lock` (it mutates the
        policy object the routing hot path reads under that lock). The three
        routing fields arrive over the sync channel so `sky serve update` can
        change them on a running LB without a re-roll:

        - policy: rebuild + swap the policy object ONLY when the resolved
          name changes (cheap + idempotent otherwise). The fresh policy is
          left empty; the caller's immediately-following `set_ready_replicas`
          re-populates it from this same sync, which is why we do not copy
          the old ready set over (that would short-circuit set_ready_replicas
          and skip load-map initialization).
        - target_qps_per_replica: applied to the active policy when it is
          instance-aware and the value is a per-accelerator dict.
        - stream_timeout_seconds: stored into the per-request instance var so
          it takes effect on subsequent proxied requests.
        """
        policy_name = routing_spec.get('load_balancing_policy_name')
        if (policy_name is not None and
                policy_name != self._load_balancing_policy_name):
            self._load_balancing_policy = lb_policies.LoadBalancingPolicy.make(
                policy_name)
            self._load_balancing_policy_name = (
                lb_policies.LoadBalancingPolicy.make_policy_name(policy_name))
        target_qps_per_replica = routing_spec.get('target_qps_per_replica')
        if (isinstance(target_qps_per_replica, dict) and
                isinstance(self._load_balancing_policy,
                           lb_policies.InstanceAwareLeastLoadPolicy)):
            self._load_balancing_policy.set_target_qps_per_accelerator(
                target_qps_per_replica)
        stream_timeout_seconds = routing_spec.get('stream_timeout_seconds')
        if stream_timeout_seconds is not None:
            self._stream_timeout_seconds = stream_timeout_seconds
        # Retry tuning rides the same channel so `sky serve update` (and
        # external LBs, which never see the spawn args) picks it up live.
        # `None` means "not set in the spec": fall back to the defaults
        # rather than keeping a stale override from a previous version.
        retriable = routing_spec.get('retriable_status_codes')
        self._retriable_status_codes = frozenset(retriable or ())
        max_retries = routing_spec.get('max_retries')
        self._max_retries = (max_retries if max_retries is not None else
                             constants.LB_MAX_RETRY)
        backoff_seconds = routing_spec.get('retry_initial_backoff_seconds')
        self._retry_initial_backoff_seconds = (
            backoff_seconds if backoff_seconds is not None else
            constants.LB_RETRY_INITIAL_BACKOFF_SECONDS)

    def _is_ready_to_serve(self) -> bool:
        """Readiness: true only once synced at least once and not draining."""
        return self._ready and not self._draining

    def _begin_draining(self) -> None:
        """Start draining (idempotent): fail readiness + stop syncing."""
        if not self._draining:
            logger.info('Draining load balancer: failing readiness and '
                        'deregistering from the controller sync.')
        self._draining = True

    async def _health(self,
                      request: fastapi.Request) -> fastapi.responses.Response:
        del request  # Unused.
        return fastapi.responses.Response(
            status_code=200 if self._is_ready_to_serve() else 503)

    async def _sync_with_controller_once(self) -> None:
        ready_replica_urls = []
        replica_info = {}
        routing_spec = None

        async with aiohttp.ClientSession() as session:
            try:
                # Send request information
                async with session.post(
                        self._controller_url + '/controller/load_balancer_sync',
                        json={
                            'request_aggregator':
                                self._request_aggregator.to_dict()
                        },
                        timeout=aiohttp.ClientTimeout(
                            constants.LB_CONTROLLER_SYNC_TIMEOUT_SECONDS),
                ) as response:
                    # Clean up after reporting request info to avoid OOM.
                    self._request_aggregator.clear()
                    response.raise_for_status()
                    response_json = await response.json()
                    replica_info = response_json.get('replica_info', {})
                    # [boltz fork] The controller ships the routing config
                    # (policy/target-qps/stream-timeout) alongside replica_info
                    # so `sky serve update` changes reach this LB without a
                    # re-roll. Older controllers omit the key -> None -> the LB
                    # keeps its launch-seeded policy.
                    routing_spec = response_json.get('routing_spec')
                    ready_replica_urls = list(replica_info.keys())
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f'An error occurred when syncing with '
                             f'the controller: {e}'
                             f'\nTraceback: {traceback.format_exc()}')
            else:
                logger.info(f'Available Replica URLs: {ready_replica_urls}')
                with self._client_pool_lock:
                    # Apply the fetched routing spec BEFORE (re)setting the
                    # ready replicas: if the policy object was swapped, the
                    # set_ready_replicas below re-populates the fresh policy
                    # from this same sync.
                    if routing_spec:
                        self._apply_routing_spec(routing_spec)
                    # Keep quarantined (locally-evicted) replicas out of
                    # routing even if the controller still lists them as
                    # ready, until their TTL expires -- otherwise a dead
                    # replica would be re-added on every sync and eviction
                    # would oscillate.
                    quarantined = self._quarantined_replicas()
                    routable = [
                        url for url in ready_replica_urls
                        if url not in quarantined
                    ]
                    self._load_balancing_policy.set_ready_replicas(routable)
                    # Set replica info for instance-aware policies
                    if isinstance(self._load_balancing_policy,
                                  lb_policies.InstanceAwareLeastLoadPolicy):
                        self._load_balancing_policy.set_replica_info(
                            replica_info)
                    # Drop eviction bookkeeping for replicas the controller no
                    # longer lists, and for expired quarantines, so the maps
                    # do not grow unbounded.
                    ready_set = set(ready_replica_urls)
                    now = time.time()
                    self._replica_dead_failures = {
                        url: count
                        for url, count in self._replica_dead_failures.items()
                        if url in ready_set
                    }
                    self._replica_quarantine_until = {
                        url: until
                        for url, until in
                        self._replica_quarantine_until.items()
                        if url in ready_set and until > now
                    }
                    for replica_url in ready_replica_urls:
                        if replica_url not in self._client_pool:
                            self._client_pool[replica_url] = httpx.AsyncClient(
                                base_url=replica_url)
                    urls_to_close = set(
                        self._client_pool.keys()) - set(ready_replica_urls)
                    client_to_close = []
                    for replica_url in urls_to_close:
                        client_to_close.append(
                            (replica_url, self._client_pool.pop(replica_url)))
                for replica_url, client in client_to_close:
                    # Fire-and-forget: a drain can legitimately take as long
                    # as the longest in-flight prediction; the sync loop must
                    # never wait on it. Strong refs held in the task set (a
                    # bare create_task result can be garbage collected).
                    task = asyncio.create_task(
                        self._drain_and_close_client(replica_url, client))
                    self._client_close_tasks.add(task)
                    task.add_done_callback(self._client_close_tasks.discard)
                # First successful sync -> ready to serve (readiness gate).
                self._ready = True

    async def _drain_and_close_client(self, url: str,
                                      client: httpx.AsyncClient) -> None:
        """Close a pruned replica's client once its in-flight work drains.

        aclose() cancels every request still running on the client, so
        closing at prune time turned every graceful replica removal
        (spot drain, rolling update, transient NOT_READY) into aborted
        in-flight predictions. Wait for the per-client in-flight counter
        (maintained by _proxy_request_to) to reach zero; the deadline
        (stream timeout + margin) bounds leaked connections if a counter
        is ever stuck.
        """
        deadline = (asyncio.get_event_loop().time() +
                    self._stream_timeout_seconds +
                    constants.LB_DRAIN_CLOSE_GRACE_SECONDS)
        while (getattr(client, _INFLIGHT_ATTR, 0) > 0 and
               asyncio.get_event_loop().time() < deadline):
            await asyncio.sleep(1)
        inflight = getattr(client, _INFLIGHT_ATTR, 0)
        if inflight > 0:
            logger.warning(f'Closing drained client for {url} with '
                           f'{inflight} request(s) still in flight '
                           '(drain deadline exceeded).')
        await client.aclose()

    async def _sync_with_controller(self):
        """Sync with controller periodically.

        Every `constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS` seconds, the
        load balancer will sync with the controller to get the latest
        information about available replicas; also, it report the request
        information to the controller, so that the controller can make
        autoscaling decisions.
        """
        # Sleep for a while to wait the controller bootstrap.
        await asyncio.sleep(5)

        while True:
            # Once draining, stop POSTing load_balancer_sync so the controller
            # stops counting this LB's request timestamps -- otherwise it would
            # double-count with the maxSurge replacement during a roll.
            if self._draining:
                logger.info('Draining: stopped syncing with the controller.')
                return
            try:
                await self._sync_with_controller_once()
                await asyncio.sleep(
                    constants.LB_CONTROLLER_SYNC_INTERVAL_SECONDS)
            except Exception as e:  # pylint: disable=broad-except
                logger.error(f'An error occurred when syncing with '
                             f'the controller: {e}'
                             f'\nTraceback: {traceback.format_exc()}')

    async def _proxy_request_to(
        self, url: str, request: fastapi.Request
    ) -> Union[fastapi.responses.Response, Exception]:
        """Proxy the request to the specified URL.

        Returns:
            The response from the endpoint replica. Return the exception
            encountered if anything goes wrong.
        """
        logger.info(f'Proxy request to {url}')
        # The token ties this request's release to the exact accounting
        # generation it incremented (see LoadBalancingPolicy hooks).
        slot_token = self._load_balancing_policy.pre_execute_hook(url, request)
        # Every exit that does NOT hand a streaming response to the client
        # must release the in-flight slot itself, or failed/aborted attempts
        # permanently inflate this replica's load and skew routing away
        # from it (each retry then leaks another slot on another replica).
        released = False
        try:
            # We defer the get of the client here on purpose, for case when the
            # replica is ready in `_proxy_with_retries` but refreshed before
            # entering this function. In that case we will return an error here
            # and retry to find next ready replica. We also need to wait for the
            # update of the client pool to finish before getting the client.
            with self._client_pool_lock:
                client = self._client_pool.get(url, None)
            if client is None:
                return RuntimeError(f'Client for {url} not found.')
            # Counted on the CLIENT object so a pruned client is closed
            # only after its in-flight work drains (a re-added URL gets a
            # fresh client with its own counter). Decremented exactly once
            # per request alongside the slot release below.
            setattr(client, _INFLIGHT_ATTR,
                    getattr(client, _INFLIGHT_ATTR, 0) + 1)
            client_refcount_dropped = False

            def _drop_client_refcount():
                nonlocal client_refcount_dropped
                if client_refcount_dropped:
                    return
                client_refcount_dropped = True
                setattr(client, _INFLIGHT_ATTR,
                        getattr(client, _INFLIGHT_ATTR, 1) - 1)

            worker_url = httpx.URL(path=request.url.path,
                                   query=request.url.query.encode('utf-8'))
            proxy_request = client.build_request(
                request.method,
                worker_url,
                headers=request.headers.raw,
                content=await request.body(),
                # A scalar here would ALSO set the connect timeout: with a
                # long stream timeout (sync model servers send no bytes
                # until compute completes, so read must cover the whole
                # prediction), a dead-but-still-routed replica would hang
                # requests for the full value during the un-route window
                # instead of failing fast into the retry loop.
                timeout=httpx.Timeout(
                    self._stream_timeout_seconds,
                    connect=constants.LB_CONNECT_TIMEOUT_SECONDS))
            proxy_response = await client.send(proxy_request, stream=True)

            if proxy_response.status_code in self._retriable_status_codes:
                # "Not now" from the replica (e.g. 503 while the model
                # warms, 429 shedding): discard and re-route. No byte has
                # reached the client — send() returns at headers with
                # stream=True. Slot + client refcount release via the
                # not-released finally below.
                await proxy_response.aclose()
                return _RetriableStatusError(proxy_response.status_code, url)

            # The slot is owned by the stream now. Starlette runs
            # BackgroundTasks strictly AFTER a successful stream — a
            # mid-stream failure (client disconnect, upstream reset)
            # skips them — so the release lives in the ITERATOR's
            # finally (generator close on any exit runs it) with the
            # background task as a second, idempotent safety net for
            # the stream-never-started edge.
            release_state = {'done': False}

            async def _release_slot():
                if release_state['done']:
                    return
                release_state['done'] = True
                try:
                    await proxy_response.aclose()
                finally:
                    self._load_balancing_policy.post_execute_hook(
                        url, request, slot_token)
                    _drop_client_refcount()

            async def _stream_with_release():
                try:
                    async for chunk in proxy_response.aiter_raw():
                        yield chunk
                finally:
                    await _release_slot()

            response = fastapi.responses.StreamingResponse(
                content=_stream_with_release(),
                status_code=proxy_response.status_code,
                headers=proxy_response.headers,
                background=background.BackgroundTask(_release_slot))
            # Ownership of the slot transfers to the stream/background pair
            # only once the response object exists and will be returned.
            released = True
            return response
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            logger.error(f'Error when proxy request to {url}: '
                         f'{common_utils.format_exception(e)}'
                         f'\nTraceback: {traceback.format_exc()}')
            return e
        finally:
            if not released:
                self._load_balancing_policy.post_execute_hook(
                    url, request, slot_token)
                # Only defined once the client was checked out; exits
                # before that (no client) have nothing to drop.
                if 'client' in locals() and client is not None:
                    _drop_client_refcount()

    async def _proxy_with_retries(
            self, request: fastapi.Request) -> fastapi.responses.Response:
        """Try to proxy the request to the endpoint replica with retries."""
        self._request_aggregator.add(request)
        # TODO(tian): Finetune backoff parameters.
        backoff = common_utils.Backoff(
            initial_backoff=self._retry_initial_backoff_seconds)
        # SkyServe supports serving on Spot Instances. To avoid preemptions
        # during request handling, we add a retry here.
        retry_cnt = 0
        # URLs that already failed THIS request: without exclusion,
        # least-load retries deterministically re-select a
        # dead-but-not-yet-pruned replica on a busy fleet (it sits at
        # load 0 while every healthy replica carries traffic).
        failed_urls: Set[str] = set()

        def _unavailable(detail: str) -> fastapi.HTTPException:
            # Retry-After lets a well-behaved client back off instead of
            # hammering; the ready set only changes on the controller
            # sync cadence, so in-LB sleeping is not a useful wait.
            return fastapi.HTTPException(
                status_code=503,
                detail=detail,
                headers={
                    'Retry-After': str(constants.LB_503_RETRY_AFTER_SECONDS)
                })

        while True:
            retry_cnt += 1
            with self._client_pool_lock:
                ready_replica_url = self._load_balancing_policy.select_replica(
                    request, exclude=failed_urls)
            if ready_replica_url is None:
                # Nothing to select at all: burning the remaining attempts
                # asleep only adds latency (and multiplies under the
                # client retry layer) — fail fast with a backoff hint.
                raise _unavailable('No ready replicas. '
                                   'Use "sky serve status [SERVICE_NAME]" '
                                   'to check the replica status.')
            else:
                response_or_exception = await self._proxy_request_to(
                    ready_replica_url, request)
                # Passively evict a replica that keeps failing with dead
                # connections during the controller-pause window.
                self._record_proxy_outcome(ready_replica_url,
                                           response_or_exception)
            if not isinstance(response_or_exception, Exception):
                return response_or_exception
            failed_urls.add(ready_replica_url)
            with self._client_pool_lock:
                all_ready_tried = failed_urls.issuperset(
                    self._load_balancing_policy.ready_replicas)
            # When the user aborts the request during streaming, the request
            # will be disconnected. We do not need to retry for this case.
            if await request.is_disconnected():
                # 499 means a client terminates the connection
                # before the server is able to respond.
                return fastapi.responses.Response(status_code=499)
            if (all_ready_tried and
                    isinstance(response_or_exception, _RetriableStatusError)):
                # Every ready replica already shed THIS request with a
                # retriable status (e.g. all busy at PREDICT_CONCURRENCY
                # capacity, or warming): none of them will free within the
                # 0.5-4s backoff schedule, so remaining attempts are pure
                # added latency. Transport failures deliberately do NOT
                # take this exit: a lone replica's connection blip still
                # recovers transparently via the full-set fallback.
                raise _unavailable('All ready replicas are at capacity. '
                                   f'Last error: {response_or_exception}.')
            # TODO(tian): Fail fast for errors like 404 not found.
            if retry_cnt >= self._max_retries:
                if isinstance(response_or_exception, fastapi.HTTPException):
                    raise response_or_exception
                exception = common_utils.remove_color(
                    common_utils.format_exception(response_or_exception,
                                                  use_bracket=True))
                raise fastapi.HTTPException(
                    # 500 means internal server error.
                    status_code=500,
                    detail=f'Max retries {self._max_retries} exceeded. '
                    f'Last error encountered: {exception}. Please use '
                    '"sky serve logs [SERVICE_NAME] --load-balancer" '
                    'for more information.')
            current_backoff = backoff.current_backoff()
            logger.error(f'Retry in {current_backoff} seconds.')
            await asyncio.sleep(current_backoff)

    def run(self):
        # Register the readiness route BEFORE the catch-all proxy route so it
        # is matched first (Starlette matches in registration order) instead of
        # being proxied to a replica.
        self._app.add_api_route('/_lb/health', self._health, methods=['GET'])
        self._app.add_api_route('/{path:path}',
                                self._proxy_with_retries,
                                methods=['GET', 'POST', 'PUT', 'DELETE'])

        @self._app.on_event('startup')
        async def startup():
            # Configure logger
            uvicorn_access_logger = logging.getLogger('uvicorn.access')
            for handler in uvicorn_access_logger.handlers:
                handler.setFormatter(sky_logging.FORMATTER)

            # Register controller synchronization task
            asyncio.create_task(self._sync_with_controller())

        uvicorn_tls_kwargs = ({} if self._tls_credential is None else
                              self._tls_credential.dump_uvicorn_kwargs())

        protocol = 'https' if self._tls_credential is not None else 'http'

        logger.info('SkyServe Load Balancer started on '
                    f'{protocol}://0.0.0.0:{self._load_balancer_port}. '
                    f'PID: {os.getpid()}')

        # Drain gracefully on SIGTERM (rolling update): _DrainableServer
        # deregisters + fails readiness immediately, then exits only after a
        # grace period so in-flight requests finish and k8s has pulled us from
        # the Service.
        config = uvicorn.Config(self._app,
                                host='0.0.0.0',
                                port=self._load_balancer_port,
                                **uvicorn_tls_kwargs)
        server = _DrainableServer(config, on_drain=self._begin_draining)
        asyncio.run(server.serve_with_drain())


def run_load_balancer(
    controller_addr: str,
    load_balancer_port: int,
    load_balancing_policy_name: Optional[str] = None,
    tls_credential: Optional[serve_utils.TLSCredential] = None,
    target_qps_per_replica: Optional[Union[float, Dict[str, float]]] = None,
    stream_timeout_seconds: Optional[int] = None,
    retriable_status_codes: Optional[List[int]] = None,
    max_retries: Optional[int] = None,
    retry_initial_backoff_seconds: Optional[float] = None,
) -> None:
    """Run the load balancer.

    The routing spec (policy / target QPS / stream timeout) is fetched from the
    controller over the sync channel; the corresponding args here are only a
    bootstrap seed for the in-pod caller and default to None for the standalone
    launcher (see `SkyServeLoadBalancer.__init__`).

    Args:
        controller_addr: The address of the controller.
        load_balancer_port: The port where the load balancer listens to.
        load_balancing_policy_name: Seed load balancing policy name.
            Defaults to None.
        tls_credential:
            The TLS credentials for HTTPS endpoint. Defaults to None.
        target_qps_per_replica: Seed target QPS per replica for instance-aware
            load balancing. Can be a float or dict mapping GPU types to QPS.
            Defaults to None.
        stream_timeout_seconds: Seed timeout in seconds for proxied responses.
            Defaults to None.
    """
    load_balancer = SkyServeLoadBalancer(
        controller_url=controller_addr,
        load_balancer_port=load_balancer_port,
        load_balancing_policy_name=load_balancing_policy_name,
        tls_credential=tls_credential,
        target_qps_per_replica=target_qps_per_replica,
        stream_timeout_seconds=stream_timeout_seconds,
        retriable_status_codes=retriable_status_codes,
        max_retries=max_retries,
        retry_initial_backoff_seconds=retry_initial_backoff_seconds)
    load_balancer.run()


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build the standalone (external) load balancer CLI parser.

    The routing spec -- load-balancing policy, per-replica target QPS, and
    stream timeout -- is NOT a launch arg: the LB fetches it from the
    controller over the load_balancer_sync channel (see
    `SkyServeLoadBalancer._apply_routing_spec`), so `sky serve update` changes
    reach a running LB without a re-roll. Only the controller address, the
    listen port, and the TLS material stay CLI args: TLS is bound to uvicorn at
    launch and a private key must never stream over the sync channel, so it
    remains a launch/mounted-secret concern.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--controller-addr',
                        required=True,
                        help='The address of the controller.')
    parser.add_argument('--load-balancer-port',
                        type=int,
                        required=True,
                        help='The port where the load balancer listens to.')
    parser.add_argument(
        '--tls-keyfile',
        type=str,
        default=None,
        help='Path to the TLS private key file for the HTTPS endpoint. Must '
        'be given together with --tls-certfile.')
    parser.add_argument(
        '--tls-certfile',
        type=str,
        default=None,
        help='Path to the TLS certificate file for the HTTPS endpoint. Must '
        'be given together with --tls-keyfile.')
    return parser


def _resolve_launch_kwargs(parser: argparse.ArgumentParser,
                           args: argparse.Namespace) -> Dict[str, Any]:
    """Coerce parsed CLI args into `run_load_balancer` kwargs.

    Invalid combinations exit via `parser.error()`. Factored out of __main__
    so the TLS coercion is unit-testable without starting a server. The
    routing spec (policy / target QPS / stream timeout) is sync-fetched, so it
    is not resolved here -- the standalone launcher passes None and the values
    arrive on the first controller sync.
    """
    if (args.tls_keyfile is None) != (args.tls_certfile is None):
        parser.error('--tls-keyfile and --tls-certfile must be given together.')
    tls_credential: Optional[serve_utils.TLSCredential] = None
    if args.tls_keyfile is not None:
        tls_credential = serve_utils.TLSCredential(keyfile=args.tls_keyfile,
                                                   certfile=args.tls_certfile)

    return dict(
        controller_addr=args.controller_addr,
        load_balancer_port=args.load_balancer_port,
        tls_credential=tls_credential,
    )


if __name__ == '__main__':
    _parser = _build_argument_parser()
    run_load_balancer(**_resolve_launch_kwargs(_parser, _parser.parse_args()))
