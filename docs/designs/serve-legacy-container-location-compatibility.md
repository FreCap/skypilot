# Serve Legacy Container Location Compatibility

## Problem

Managed container image support normalizes the deprecated
`image_id: docker:...` form into an internal `ContainerImage`. Serve spot
placement then copies that internal legacy object through
`Location.container_image`, which re-enters the public `container_image`
validation boundary and raises. This breaks autoscaling, reserved-capacity
polling, and load-balancer synchronization for existing services after an API
server upgrade.

The same release also rejects an AWS-only logical-replica service with
`reserved_capacity_fill` because the Kubernetes fill-shape validation treats
an empty set of YAML Kubernetes candidates as a malformed non-one-GPU set.
Reserved capacity can be supplied by the broker, so the empty set is valid.

Native container selectors are also copied into persisted replica overrides as
Python `ContainerImage` objects. PostgreSQL JSON serialization rejects those
objects before the launch record can be inserted.

## Behavior contract

- A legacy Docker image keeps its legacy direct-pull provenance through every
  Serve location override and resource copy.
- A native `container_image` remains a native selector and continues to cross
  the public validation boundary in its JSON-safe YAML representation.
- Kubernetes fill candidates for logical replicas must each request exactly
  one GPU.
- An AWS-only task may enable reserved fill because its zero-cost candidates
  can be supplied outside the submitted YAML.

## Alternatives

Converting legacy identities into native selectors during location copying was
rejected because it changes mutable-tag compatibility and managed-distribution
policy. Disabling reserved fill was used only as an incident mitigation and is
not the intended steady state.

## Implementation and tests

1. Encode an internally normalized legacy Docker identity back through the
   legacy `image_id.docker` override when constructing a Serve location.
2. Serialize native container selectors in location overrides to the same
   scalar or dictionary form accepted by task YAML.
3. Apply the one-GPU logical fill check only when submitted Kubernetes fill
   shapes exist.
4. Add regression coverage for legacy location resource copies, JSON-safe
   native location overrides, AWS-only reserved fill, and non-one-GPU
   Kubernetes fill rejection.

## Rollout

Run the targeted Serve unit tests and formatting checks, merge the hotfix,
publish the next patch release, and deploy it to the API server. Verify that
the affected controller stops logging legacy container identity exceptions,
the load balancer becomes ready, the queued native-image service update is
processed, and new replicas remain L4-based. Re-enable the configured reserved
fill weight after the compatibility release is live.
