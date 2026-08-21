"""SkyServe status execution and response projection."""

from typing import Any

from sky import backends
from sky import exceptions
from sky.backends import backend_utils
from sky.serve import lb_k8s
from sky.serve import runner as serve_runner
from sky.serve import serve_history
from sky.serve import serve_rpc_utils
from sky.serve import serve_state
from sky.serve import serve_utils
from sky.utils import controller_utils
from sky.utils import subprocess_utils
from sky.utils import ux_utils


def _external_service_endpoint_url(service_name: str,
                                   resource_scope: str | None = None
                                  ) -> str | None:
    """Return the HTTP-only provider LB endpoint, or None if unavailable."""
    if resource_scope is None:
        # Preserve the legacy call shape for NULL-scope rows and embedders
        # whose endpoint hook predates incarnation-scoped LB names.
        socket_endpoint = lb_k8s.lb_service_endpoint_or_none(service_name)
    else:
        socket_endpoint = lb_k8s.lb_service_endpoint_or_none(
            service_name, resource_scope)
    if socket_endpoint is None:
        return None
    # TLS terminates at the platform ingress. The per-service LB Deployment
    # and Service are deliberately HTTP-only, including for legacy rows whose
    # persisted tls_encrypted bit predates the external-only topology.
    return f'http://{socket_endpoint}'


class _DefaultServiceStatusRunner:
    """Default implementation — gRPC with codegen + run_on_head fallback.

    Registered lazily by ``sky.serve.runner.current()``. Plugins override
    by calling ``sky.serve.runner.register()`` with their own
    implementation (e.g. an in-process runner for consolidation mode).
    """

    def get_service_status(
        self,
        *,
        handle: 'backends.CloudVmRayResourceHandle',
        service_names: list[str] | None,
        pool: bool,
        summary_only: bool = False,
        include_target_num_replicas: bool | None = None,
        metadata_only: bool = False,
    ) -> list[dict[str, Any]]:
        noun = 'pool' if pool else 'service'
        # Existing Serve v9 skylets parse but ignore the new optional proto
        # field and would return the full replica inventory. Metadata requests
        # therefore use the compatibility codegen path, which is built from
        # the slim snapshot primitive already present in Serve v9.
        use_legacy = metadata_only or not handle.is_grpc_enabled_with_flag

        service_records: list[dict[str, Any]] = []
        if not use_legacy:
            try:
                service_records = serve_rpc_utils.RpcRunner.get_service_status(
                    handle,
                    service_names,
                    pool,
                    summary_only=summary_only,
                    metadata_only=metadata_only,
                    include_target_num_replicas=include_target_num_replicas)
            except exceptions.SkyletMethodNotImplementedError:
                use_legacy = True

        if use_legacy:
            backend = backend_utils.get_backend_from_handle(handle)
            assert isinstance(backend, backends.CloudVmRayBackend)

            code = serve_utils.ServeCodeGen.get_service_status(
                service_names,
                pool=pool,
                summary_only=summary_only,
                metadata_only=metadata_only,
                include_target_num_replicas=include_target_num_replicas)
            returncode, serve_status_payload, stderr = backend.run_on_head(
                handle,
                code,
                require_outputs=True,
                stream_logs=False,
                separate_stderr=True)

            try:
                subprocess_utils.handle_returncode(returncode,
                                                   code,
                                                   f'Failed to fetch {noun}s',
                                                   stderr,
                                                   stream_logs=True)
            except exceptions.CommandError as e:
                raise RuntimeError(e.error_msg) from e

            service_records = serve_utils.load_service_status(
                serve_status_payload)

        return service_records


def status(
    service_names: str | list[str] | None = None,
    pool: bool = False,
    summary_only: bool = False,
    include_target_num_replicas: bool | None = None,
    history_hours: int | None = None,
    metadata_only: bool = False,
    include_endpoints: bool = False,
    authorized_owner_user_id: str | None = None,
) -> list[dict[str, Any]]:
    """Gets statuses of services or pools.

    summary_only skips per-replica info (returns replica_status_counts
    instead). `include_target_num_replicas` can opt summaries back into
    autoscaler target fetches; when omitted, full status keeps target counts
    while summary-only requests stay on the cheap DB-only path.
    """
    noun = 'pool' if pool else 'service'
    if summary_only and metadata_only:
        raise ValueError(
            'summary_only and metadata_only are mutually exclusive.')
    if metadata_only and (include_target_num_replicas or
                          history_hours is not None or include_endpoints):
        raise ValueError(
            'metadata_only cannot include target replicas, history, '
            'or endpoints.')
    if service_names is not None:
        if isinstance(service_names, str):
            service_names = [service_names]
    if authorized_owner_user_id is not None:
        service_names = serve_state.get_glob_service_names(
            service_names, pool=pool, owner_user_id=authorized_owner_user_id)
    if history_hours is not None:
        if pool:
            raise ValueError('Status history is only supported for services.')
        if service_names is None or len(service_names) != 1:
            raise ValueError('Status history requires exactly one service.')
    if service_names == []:
        return []

    try:
        backend_utils.check_network_connection()
    except exceptions.NetworkError as e:
        with ux_utils.print_exception_no_traceback():
            raise RuntimeError(f'Failed to refresh {noun}s status '
                               'due to network error.') from e

    controller_type = controller_utils.get_controller_for_pool(pool)
    handle = backend_utils.is_controller_accessible(
        controller=controller_type,
        stopped_message=controller_type.value.default_hint_if_non_existent.
        replace('service', noun))

    assert isinstance(handle, backends.CloudVmRayResourceHandle)

    runner_kwargs: dict[str, Any] = dict(
        handle=handle,
        service_names=service_names,
        pool=pool,
        summary_only=summary_only,
        include_target_num_replicas=include_target_num_replicas)
    if metadata_only:
        # Keep the standard call shape compatible with third-party runners
        # compiled against Serve v9. The new keyword is only required for the
        # new projection and is guarded by the API/Serve version bump.
        runner_kwargs['metadata_only'] = True
    service_records = serve_runner.current().get_service_status(**runner_kwargs)
    if authorized_owner_user_id is not None:
        # The controller call can outlive a delete/recreate of the same service
        # name.  Re-read exact current ownership before returning enrichment so
        # a successor owned by another tenant is omitted even if it appeared
        # after the pre-controller wildcard expansion.
        current_owned_names = set(
            serve_state.get_service_names_owned_by_user_id(
                authorized_owner_user_id))
        service_records = [
            record for record in service_records
            if record.get('name') in current_owned_names
        ]

    # Keep summary-only requests on the cheap DB-only path: callers that opt
    # out of replica detail should not pay an extra per-service Kubernetes
    # read just to hydrate endpoint strings.
    for service_record in service_records:
        service_record['endpoint'] = None
        # Pool doesn't have an endpoint.
        if pool or metadata_only or (summary_only and not include_endpoints):
            continue
        if service_record['load_balancer_port'] is not None:
            # load_balancer_port remains the registration sentinel exposed by
            # the status API. It is deliberately not a routing input: the
            # external per-service Kubernetes Service is the only supported
            # endpoint, and an unavailable external runtime stays unavailable.
            service_record['endpoint'] = _external_service_endpoint_url(
                service_record['name'], service_record.get('resource_scope'))

    if history_hours is not None and service_records:
        service_records[0]['replica_status_history'] = (
            serve_history.get_status_history(
                service_records[0]['name'],
                hours=history_hours,
                expected_service_hash=service_records[0].get('hash')))

    return service_records


# These names historically lived in sky.serve.server.impl. Keep their module
# identity stable for introspection and serialization while impl remains the
# public compatibility facade.
_external_service_endpoint_url.__module__ = 'sky.serve.server.impl'
_DefaultServiceStatusRunner.__module__ = 'sky.serve.server.impl'
status.__module__ = 'sky.serve.server.impl'

# Public implementation names let the compatibility facade and registry avoid
# reaching through the new module's private surface. The objects retain their
# historical names above so serialized references continue to resolve through
# sky.serve.server.impl.
external_service_endpoint_url = _external_service_endpoint_url
DefaultServiceStatusRunner = _DefaultServiceStatusRunner
