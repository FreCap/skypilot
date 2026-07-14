"""Characterization tests for API server dashboard registration."""

import inspect

from fastapi.testclient import TestClient

from sky.server import server


def test_dashboard_routes_preserve_registration_and_callable_identity():
    registered_routes = []
    for route in server.app.routes:
        original_router = getattr(route, 'original_router', None)
        if original_router is None:
            registered_routes.append(route)
        else:
            registered_routes.extend(original_router.routes)
    routes = [
        route for route in registered_routes
        if getattr(route, 'path', None) in ('/dashboard/{full_path:path}', '/')
    ]

    assert [(route.path, route.methods) for route in routes] == [
        ('/dashboard/{full_path:path}', {'GET'}),
        ('/', {'GET'}),
    ]
    assert routes[0].endpoint is server.serve_dashboard
    assert routes[1].endpoint is server.root
    assert server.serve_dashboard.__module__ == 'sky.server.server'
    assert server.root.__module__ == 'sky.server.server'
    assert str(inspect.signature(server.serve_dashboard)) == (
        '(request: starlette.requests.Request, full_path: str)')
    assert str(inspect.signature(server.root)) == '()'

    with TestClient(server.app) as client:
        response = client.get('/', follow_redirects=False)
        missing_asset = client.get('/dashboard/_next/missing.js')
    assert response.status_code == 307
    assert response.headers['location'] == '/dashboard/'
    assert missing_asset.status_code == 404
    assert missing_asset.headers['cache-control'] == 'no-store'


def test_dashboard_middleware_preserves_relative_order_and_identity():
    middleware_classes = [item.cls for item in server.app.user_middleware]
    dashboard_middleware = [
        cls for cls in middleware_classes if cls.__name__ in {
            'CacheControlStaticMiddleware',
            'PathCleanMiddleware',
            'InternalDashboardPrefixMiddleware',
        }
    ]

    # Starlette stores middleware in reverse add_middleware() order.
    assert dashboard_middleware == [
        server.CacheControlStaticMiddleware,
        server.PathCleanMiddleware,
        server.InternalDashboardPrefixMiddleware,
    ]
    assert all(
        cls.__module__ == 'sky.server.server' for cls in dashboard_middleware)
