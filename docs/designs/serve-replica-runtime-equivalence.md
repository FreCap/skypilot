# Serve Replica Runtime Equivalence

## Problem

SkyServe can relabel a ready replica during a service update when the update
changes only service policy. The comparison currently treats generated source
provenance, such as `_metadata.git_commit`, as executable configuration. A
target-only update from a new Git commit therefore rolls healthy backends even
though their running process is unchanged.

The mismatch and success logs also include complete task dictionaries. Those
dictionaries can contain resolved service secrets and must never be written to
controller logs.

## Behavior contract

A prior replica is runtime-equivalent to the new version only when all fields
that affect its running process are equal after normalization:

- setup, run, environment variables, and secrets are equal;
- resources are equal after the existing `any_of` order normalization;
- file mounts are explicitly empty, and volumes remain equal;
- the prior and new versions use the same logical or physical replica unit;
- service and pool policy fields remain excluded because the controller and
  load balancer apply them independently;
- generated source provenance (`_metadata.git_commit`) and an empty generated
  storage-scope identity are excluded because they do not affect execution.

Changes to secrets, images, commands, resources, mounts, volumes, or other
task metadata still require replacement capacity. Relabel decisions log only
replica and version identifiers, never task dictionaries or secret values.

## Alternatives

Comparing only the image is too weak because command, environment, secret,
resource, and mounted-data changes can alter a process without changing the
image. Comparing the full serialized task is too strict because generated
provenance changes on every source commit.

## Rollout

Ship the normalization and redacted logging in the control plane. Existing
replicas are not mutated until a later service update invokes the comparison.
No database migration or API change is required.

## Test plan

- Verify a Git-metadata-only update relabels a ready replica.
- Verify a secret change does not relabel the replica.
- Verify controller logs for both successful and rejected comparisons do not
  contain secret values.
- Run the focused replica-manager unit tests and formatter.
