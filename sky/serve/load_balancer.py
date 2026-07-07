"""LoadBalancer: Distribute any incoming request to all ready replicas."""
import asyncio
import logging
import os
import threading
import traceback
from typing import Dict, List, Optional, Set, Union

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
        stream_timeout_seconds: int = constants.DEFAULT_LB_STREAM_TIMEOUT,
        retriable_status_codes: Optional[List[int]] = None,
    ) -> None:
        """Initialize the load balancer.

        Args:
            controller_url: The URL of the controller.
            load_balancer_port: The port where the load balancer listens to.
            load_balancing_policy_name: The name of the load balancing policy
                to use. Defaults to None.
            tls_credentials: The TLS credentials for HTTPS endpoint. Defaults
                to None.
            target_qps_per_replica: Target QPS per replica for instance-aware
                load balancing. Can be a float or dict mapping GPU types to QPS.
                Defaults to None.
            stream_timeout_seconds: Timeout in seconds for proxied responses.
        """
        self._app = fastapi.FastAPI()
        self._controller_url: str = controller_url
        self._load_balancer_port: int = load_balancer_port
        # Use the registry to create the load balancing policy
        self._load_balancing_policy = lb_policies.LoadBalancingPolicy.make(
            load_balancing_policy_name)

        # Set accelerator QPS for instance-aware policies
        if (target_qps_per_replica and
                isinstance(target_qps_per_replica, dict) and
                isinstance(self._load_balancing_policy,
                           lb_policies.InstanceAwareLeastLoadPolicy)):
            self._load_balancing_policy.set_target_qps_per_accelerator(
                target_qps_per_replica)

        logger.info('Starting load balancer with policy '
                    f'{load_balancing_policy_name}.')
        self._request_aggregator: serve_utils.RequestsAggregator = (
            serve_utils.RequestTimestamp())
        self._tls_credential: Optional[serve_utils.TLSCredential] = (
            tls_credential)
        self._stream_timeout_seconds: int = stream_timeout_seconds
        # Replica responses with these statuses are re-routed like
        # transport failures (empty = never, the default). Safe only for
        # idempotent workloads and "not now" statuses (503/429): the body
        # is discarded before any byte reaches the client.
        self._retriable_status_codes = frozenset(retriable_status_codes or ())
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
        # Strong refs to in-progress drain-close tasks (see
        # _drain_and_close_client); a bare create_task result can be GCed.
        self._client_close_tasks: Set[asyncio.Task] = set()

    async def _sync_with_controller_once(self) -> None:
        ready_replica_urls = []
        replica_info = {}

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
                    ready_replica_urls = list(replica_info.keys())
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                logger.error(f'An error occurred when syncing with '
                             f'the controller: {e}'
                             f'\nTraceback: {traceback.format_exc()}')
            else:
                logger.info(f'Available Replica URLs: {ready_replica_urls}')
                with self._client_pool_lock:
                    self._load_balancing_policy.set_ready_replicas(
                        ready_replica_urls)
                    # Set replica info for instance-aware policies
                    if isinstance(self._load_balancing_policy,
                                  lb_policies.InstanceAwareLeastLoadPolicy):
                        self._load_balancing_policy.set_replica_info(
                            replica_info)
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
        backoff = common_utils.Backoff(initial_backoff=1)
        # SkyServe supports serving on Spot Instances. To avoid preemptions
        # during request handling, we add a retry here.
        retry_cnt = 0
        # URLs that already failed THIS request: without exclusion,
        # least-load retries deterministically re-select a
        # dead-but-not-yet-pruned replica on a busy fleet (it sits at
        # load 0 while every healthy replica carries traffic).
        failed_urls: Set[str] = set()
        while True:
            retry_cnt += 1
            with self._client_pool_lock:
                ready_replica_url = self._load_balancing_policy.select_replica(
                    request, exclude=failed_urls)
            if ready_replica_url is None:
                response_or_exception = fastapi.HTTPException(
                    # 503 means that the server is currently
                    # unable to handle the incoming requests.
                    status_code=503,
                    detail='No ready replicas. '
                    'Use "sky serve status [SERVICE_NAME]" '
                    'to check the replica status.')
            else:
                response_or_exception = await self._proxy_request_to(
                    ready_replica_url, request)
            if not isinstance(response_or_exception, Exception):
                return response_or_exception
            if ready_replica_url is not None:
                failed_urls.add(ready_replica_url)
            # When the user aborts the request during streaming, the request
            # will be disconnected. We do not need to retry for this case.
            if await request.is_disconnected():
                # 499 means a client terminates the connection
                # before the server is able to respond.
                return fastapi.responses.Response(status_code=499)
            # TODO(tian): Fail fast for errors like 404 not found.
            if retry_cnt == constants.LB_MAX_RETRY:
                if isinstance(response_or_exception, fastapi.HTTPException):
                    raise response_or_exception
                exception = common_utils.remove_color(
                    common_utils.format_exception(response_or_exception,
                                                  use_bracket=True))
                raise fastapi.HTTPException(
                    # 500 means internal server error.
                    status_code=500,
                    detail=f'Max retries {constants.LB_MAX_RETRY} exceeded. '
                    f'Last error encountered: {exception}. Please use '
                    '"sky serve logs [SERVICE_NAME] --load-balancer" '
                    'for more information.')
            current_backoff = backoff.current_backoff()
            logger.error(f'Retry in {current_backoff} seconds.')
            await asyncio.sleep(current_backoff)

    def run(self):
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

        uvicorn.run(self._app,
                    host='0.0.0.0',
                    port=self._load_balancer_port,
                    **uvicorn_tls_kwargs)


def run_load_balancer(
    controller_addr: str,
    load_balancer_port: int,
    load_balancing_policy_name: Optional[str] = None,
    tls_credential: Optional[serve_utils.TLSCredential] = None,
    target_qps_per_replica: Optional[Union[float, Dict[str, float]]] = None,
    stream_timeout_seconds: int = constants.DEFAULT_LB_STREAM_TIMEOUT,
    retriable_status_codes: Optional[List[int]] = None,
) -> None:
    """Run the load balancer.

    Args:
        controller_addr: The address of the controller.
        load_balancer_port: The port where the load balancer listens to.
        policy_name: The name of the load balancing policy to use.
        Defaults to None.
        tls_credential:
            The TLS credentials for HTTPS endpoint. Defaults to None.
        target_qps_per_replica: Target QPS per replica for instance-aware
            load balancing. Can be a float or dict mapping GPU types to QPS.
            Defaults to None.
        stream_timeout_seconds: Timeout in seconds for proxied responses.
    """
    load_balancer = SkyServeLoadBalancer(
        controller_url=controller_addr,
        load_balancer_port=load_balancer_port,
        load_balancing_policy_name=load_balancing_policy_name,
        tls_credential=tls_credential,
        target_qps_per_replica=target_qps_per_replica,
        stream_timeout_seconds=stream_timeout_seconds,
        retriable_status_codes=retriable_status_codes)
    load_balancer.run()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--controller-addr',
                        required=True,
                        default='127.0.0.1',
                        help='The address of the controller.')
    parser.add_argument('--load-balancer-port',
                        type=int,
                        required=True,
                        default=8890,
                        help='The port where the load balancer listens to.')
    available_policies = list(lb_policies.LB_POLICIES.keys())
    parser.add_argument(
        '--load-balancing-policy',
        choices=available_policies,
        default=lb_policies.DEFAULT_LB_POLICY,
        help=f'The load balancing policy to use. Available policies: '
        f'{", ".join(available_policies)}.')
    args = parser.parse_args()
    run_load_balancer(args.controller_addr,
                      args.load_balancer_port,
                      args.load_balancing_policy,
                      tls_credential=None,
                      target_qps_per_replica=None)
