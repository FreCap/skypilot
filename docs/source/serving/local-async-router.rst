.. _skyserve-local-async-router:

Local async worker router
=========================

Some replicas run several independent model workers on one machine, such as
one process per GPU.  SkyServe includes a local router that exposes those
workers as one capacity-aware replica endpoint.  The router is independent of
the serving framework and worker launch mechanism.

Worker contract
---------------

Every worker must expose the same async POST route and readiness GET route.
The async route accepts these actions:

* ``async_capacity`` returns nonnegative integer ``running_count`` and
  ``predict_concurrency`` fields.
* ``async_predict`` accepts a request and returns its ``request_id``.
* ``async_status`` and ``async_cancel`` address a request by ``request_id``.

A worker must include newly accepted work in ``running_count`` before it
acknowledges ``async_predict``.  This lets the router reconcile its temporary
slot reservation with the worker's next capacity sample.

Starting the router
-------------------

Start workers on loopback ports, then run the router with the Python
interpreter recorded by the SkyPilot runtime.  For contiguous ports, use
``--upstream-count``:

.. code-block:: bash

   # Start four framework-specific workers on 8081 through 8084 first.
   SKY_RUNTIME_ROOT="${SKY_RUNTIME_DIR:-$HOME}"
   SKY_RUNTIME_PYTHON="$(cat "$SKY_RUNTIME_ROOT/.sky/python_path")"
   cd "$HOME"
   exec env -u PYTHONPATH "$SKY_RUNTIME_PYTHON" \
     -m sky.serve.local_async_router \
     --port 8080 \
     --upstream-port-start 8081 \
     --upstream-count 4 \
     --async-path /async \
     --readiness-path /ready

For non-contiguous workers, repeat ``--upstream`` with HTTP or HTTPS base URLs
instead.  Routes and upstreams are always supplied by the service; the router
does not contain model-specific paths or startup logic.

Routing guarantees
------------------

The router:

* sums worker capacities for ``async_capacity`` responses;
* reserves a free worker slot atomically before forwarding ``async_predict``;
* retries a different worker only after explicit rejection (429 by default,
  configurable with ``--retriable-status-code``);
* never replays a timeout, connection loss, or other ambiguous response;
* remembers request ownership for status and cancellation; and
* reports the replica ready when at least one worker is ready.

Concurrent capacity requests share one probe wave. Readiness and ownership
recovery return as soon as a worker gives a definitive answer instead of
waiting for unrelated slow workers. The proxy also strips hop-by-hop headers,
preserves repeated end-to-end response headers, and bounds request bodies to 1
MiB by default (configurable with ``--client-max-size-mib``).

Protocol scope
--------------

This is a purpose-built, buffered proxy for the JSON async worker contract,
not a general replacement for nginx. It does not proxy arbitrary routes,
websockets, or streaming request and response bodies. Keeping that surface
narrow lets the router make conservative retry and request-ownership guarantees
for long-running asynchronous inference.

The router is part of the SkyPilot runtime, so a service does not need to
install or package a separate proxy.  Worker processes still own execution,
result persistence, and recovery semantics.
