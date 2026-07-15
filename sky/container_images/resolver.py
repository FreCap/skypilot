"""Pure placement resolver for digest-pinned container image routes."""

from collections.abc import Iterable

from sky.container_images import models


class ImageRouteUnavailableError(ValueError):
    """No safe, ready registry route exists for a placement."""


def _to_resolved(route: models.ImageRoute) -> models.ResolvedContainerImage:
    if route.auth_strategy is None:
        raise ImageRouteUnavailableError(
            f'Image route {route.target_id!r} has no runtime pull-auth '
            'strategy.')
    return models.ResolvedContainerImage(
        image_id=route.image_id,
        location_id=route.location_id,
        reference=route.reference,
        target_id=route.target_id,
        distribution=route.distribution,
        profile_revision=route.profile_revision,
        policy_fingerprint=route.policy_fingerprint,
        digest=route.digest,
        auth_strategy=route.auth_strategy,
    )


def resolve(
    placement: models.Placement,
    routes: Iterable[models.ImageRoute],
    locality: models.Locality,
) -> models.ResolvedContainerImage:
    """Selects a pull route without network or infrastructure side effects.

    A regional route matches both provider and region.  Canonical fallback is
    usable only when its route declares an auth strategy for this placement.
    The resulting reference remains pinned for the lifetime of the launch.
    """
    route_list = list(routes)
    ready_for_state = [
        route for route in route_list
        if route.state == models.ImageLocationState.READY
    ]
    ready = [
        route for route in ready_for_state
        if models.platforms_support_runtime(route.platforms, placement.platform)
    ]
    if ready_for_state and not ready:
        raise ImageRouteUnavailableError(
            'Verified container image routes do not support the selected '
            f'runtime platform {placement.platform!r}.')
    local = [
        route for route in ready
        if route.provider.lower() == placement.locality_provider.lower() and
        route.region == placement.locality_region and
        route.auth_strategy is not None
    ]
    canonical = next((route for route in ready
                      if route.canonical and route.auth_strategy is not None),
                     None)

    if locality == models.Locality.CANONICAL:
        if canonical is None:
            raise ImageRouteUnavailableError(
                'Canonical image route is not ready or has no safe pull-auth '
                'strategy for the selected registry locality.')
        return _to_resolved(canonical)

    if local:
        # An in-region canonical is already local and satisfies require without
        # a wasteful duplicate target. Prefer an explicit regional target when
        # both exist, then sort by ID for deterministic resolution.
        return _to_resolved(
            sorted(local, key=lambda r: (r.canonical, r.target_id))[0])

    if locality == models.Locality.REQUIRE:
        raise ImageRouteUnavailableError(
            'Container image locality is required, but no verified local '
            'route is ready for the selected registry locality.')

    if canonical is None:
        raise ImageRouteUnavailableError(
            'No verified regional route is ready and the canonical route has '
            'no safe pull-auth strategy for the selected registry locality.')
    return _to_resolved(canonical)


# TODO(fcapponi): Add an explicit short-lived cross-cloud token adapter before
# allowing private ECR or GAR canonical fallback onto a different provider.
