# API server core middleware decomposition

_Created: 2026-07-21_

## Problem

`sky/server/server.py` is the stable API application entrypoint, but it also
implements four cross-cutting HTTP middleware policies. Request identity,
security headers, graceful shutdown, and client/server version negotiation are
maintained between unrelated application lifespan and route-registration code.
This makes middleware changes contend with route work and obscures the boundary
between application composition and request/response policy.

## Goals

Move the complete core middleware policy leaf behind the existing
`sky.server.server` facade without changing middleware order, route behavior,
public imports, class identity, request state, response headers, shutdown
semantics, or version context propagation. Add no wrapper call, registry,
abstract base class, or dependency-injection layer.

## Background

The application already composes independently owned middleware from
`sky.server.auth.middleware`, `sky.server.dashboard`, and
`sky.server.metrics`. The four remaining classes share FastAPI and Starlette's
middleware contract, but they do not own application lifecycle or routes.

The responsibilities are:

| Responsibility | Callers | Dependencies | State owned | Failure modes | Performance sensitivity | Change cadence |
| --- | --- | --- | --- | --- | --- | --- |
| Request identity | Every HTTP route and SDK caller | request repository ID generation, Starlette request state | Per-request ID and response header | Missing request ID or mismatched header | One call and one header per request | Request tracing |
| Security headers | Every HTTP response and browser dashboard | CSP nonce state and response headers | No persistent state | Broken browser assets, weakened CSP, missing headers | One string projection per response | Browser security policy |
| Graceful shutdown | All new non-API requests during shutdown | process shutdown state | No state beyond the shared shutdown flag | New work admitted during shutdown or control APIs blocked | One flag read and path check per request | Process lifecycle |
| API version negotiation | All versioned clients | version compatibility parser and context variables | Per-request version context and response headers | Incompatible clients admitted or compatible clients rejected | One compatibility check per request | Client/server compatibility |
| Application composition | Uvicorn, plugins, routers, and middleware stack | FastAPI app, lifespan, auth and dashboard modules | App and middleware ordering | Duplicate initialization or changed wrapping order | Import and startup sensitive | Server topology |
| API routes | CLI, SDK, dashboard, and controllers | executor, persistence, transport, and domain modules | Request scheduling and streaming state | Route-specific protocol failures | Endpoint-specific | Product APIs |

The first four responsibilities have a stable middleware seam and different
callers, dependencies, failure modes, and reasons to change from application
composition and product routes.

## Solution

Create `sky/server/core_middleware.py` containing the four existing classes
with their implementations unchanged. `sky/server/server.py` imports that
module, exposes direct aliases at the historical names, restores the historical
`__module__` identity, and registers the same class objects in the same order.
The server module remains the stable Uvicorn and import facade.

Characterization tests pin request-ID propagation, graceful-shutdown routing,
version negotiation and response headers, security headers, direct alias
identity, historical module identity, and the order of the four classes in the
application middleware stack.

## Alternatives considered

Leaving the classes in `server.py` avoids a file, but retains a mixed
composition and policy boundary in a high-churn 2,475-line module. Extracting
each class separately would over-break a cohesive middleware family. Moving the
application factory or lifecycle at the same time would combine stateful
startup work with this low-state structural extraction. Wrapping extracted
classes in compatibility subclasses would add behavior and alter class
identity, so direct aliases are used instead.

## Test and rollout plan

Run the focused middleware and server registration tests, the complete API
server unit-test directory, the event-loop latency characterization, targeted
formatting and static analysis, compile checks, and `git diff --check`. Compare
class ASTs before and after extraction and measure cold import time on the base
and head. The unit-test workflow has no pull-request path filter and runs both
`tests/unit_tests` and `tests/test_ssh_proxy_lag.py`.

This is a structural change with no data or configuration migration. Rollback
is the inverse move back into `server.py`.
