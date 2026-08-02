# Resources pickle serialization boundary

## Context

`sky/resources.py` is 3,381 lines. Most of the file is the public
`Resources` value object and its validation, placement, comparison, YAML, and
copy behavior. Its 295-line pickle family is different: `__getstate__()`
projects a wire-compatible builtin state, while `__setstate__()` migrates 36
historical state versions before restoring the object.

Line count alone does not justify an extraction. This proposal is limited to
the pickle codec because it has a complete, stable seam and a distinct reason
to change. The public class, method names, pickle identity, and validation
entrypoint remain in `sky.resources`.

## Responsibility map

### Resource specification and validation

- Callers: task parsing, optimizer, provisioning, backends, Jobs, Serve, CLI,
  SDK, and public `sky.Resources` users.
- Dependencies: cloud catalogs, provider feature checks, config schemas,
  accelerator normalization, storage and image models, and task policy.
- State: the current immutable resource request and its cached presentation.
- Failure modes: accepting an invalid request, rejecting a valid provider
  configuration, changing placement compatibility, or mutating a copied
  resource.
- Performance sensitivity: construction, copy, matching, and optimizer loops.
- Change cadence: resource fields, provider capabilities, and placement policy.

### YAML and public presentation

- Callers: task YAML parsing and emission, CLI output, API serialization, and
  debugging.
- Dependencies: public configuration aliases, schema rules, redaction, and
  stable user-facing formatting.
- State: no independent state; reads and projects the current resource.
- Failure modes: YAML incompatibility, secret leakage, help/output drift, or
  loss of round-trip fidelity.
- Performance sensitivity: task parsing and CLI output, but no remote I/O.
- Change cadence: public configuration and presentation compatibility.

### Pickle wire projection and historical migration

- Callers: Python `pickle`, stored cluster/resource handles, API server/client
  exchange, and direct compatibility tests.
- Dependencies: historical field versions, container-image codecs, Docker
  login configuration, legacy Kubernetes context lookup, accelerator names,
  ports, disk tiers, autostop hooks, and the facade validation method.
- State: a copied builtin state dictionary during serialization and one
  caller-supplied historical state dictionary during restoration.
- Failure modes: old handles becoming unreadable, importing new model classes
  on old clients, lost image or credential provenance, wrong Kubernetes
  context migration, changed exception text, or altered pickle identity.
- Performance sensitivity: no extra imports, copies, Kubernetes lookups, or
  validation calls; one dictionary copy remains the hot operation.
- Change cadence: backward-compatibility version bumps and durable model
  migrations, independently of placement algorithms.

## Decision

Extract the pickle implementation into a plain private helper module,
`sky/resources_serialization.py`. Keep `Resources.__getstate__()` and
`Resources.__setstate__()` as the stable facade and delegate directly.

The facade passes its current version, default disk size, Docker image helper,
and hook normalizer at call time. This preserves historical monkeypatch paths
and avoids importing `sky.resources` from the helper, so no circular import or
parallel public model is introduced. The helper continues to call the
instance's existing credential validator after projecting or restoring state.

## Alternatives considered

- Keep the method bodies in `Resources`: lowest carrying cost, but leaves a
  large compatibility ledger mixed with construction, validation, placement,
  YAML, and presentation. The 36-version migration is already a complete seam
  and changes for a materially different reason.
- Extract validation or YAML parsing too: rejected. Those paths share more of
  the current resource model and public configuration behavior, so this would
  exceed one bounded responsibility per run.
- Introduce a serializer class, protocol, registry, or versioned migration
  hierarchy: rejected because there is one implementation and no runtime
  strategy variation. Plain functions are sufficient.
- Move `Resources` itself: rejected because it would change a high-fan-in
  public and pickled identity.

## Behavior and compatibility contract

- `sky.Resources` and `sky.resources.Resources` keep their class identity.
- `Resources.__getstate__` and `Resources.__setstate__` keep their names,
  signatures, module, qualified name, and pickle dispatch behavior.
- Current pickle payloads and all historical version branches remain
  behaviorally identical, including exception text and state mutation.
- `sky.resources.kubernetes_utils`, `_normalize_hook_entry`, and
  `_maybe_add_docker_prefix_to_image_id` remain effective patch seams.
- Container image, Docker credential, hook, region, accelerator, port, disk,
  and job-recovery migrations preserve their ordering.
- Serialization retains one state dictionary copy and restoration adds no
  network, database, filesystem, Kubernetes, or validation operation.

## Milestones and rollback

1. Add facade, current-round-trip, earliest-state, Kubernetes-context,
   Kubernetes-image, and legacy-hook characterization tests and run them on
   the exact base.
2. Move only the two method implementations behind the historical facade.
3. Prove normalized AST equivalence for the moved logic, public identity,
   import behavior, operation counts, focused and component tests, static
   checks, and representative timing.
4. Roll back by restoring the two method bodies and deleting the helper. No
   stored data, schema, configuration, or external API migration is required.

## Changed-path-to-test matrix

| Changed path | Responsibility | Verification |
| --- | --- | --- |
| `sky/resources.py` | Public facade and dependency routing | Serialization contract, full resource tests, import identity, mypy, Pylint |
| `sky/resources_serialization.py` | Pickle projection and migration | Serialization contract, legacy image/job recovery tests, AST and operation-count checks |
| `tests/unit_tests/test_resources_serialization_contract.py` | Compatibility characterization | Run directly on exact base and extracted head |
| This design | Canonical contract | Formatter and diff checks |

## CI and performance plan

The pull-request unit-test workflow has no production path filter for these
files. The new contract and `tests/unit_tests/test_resources.py` map to the
Unit Tests job; optimizer, Jobs/API, failover, and compatibility jobs exercise
downstream resource consumers. The final PR must inspect the live workflow
mapping and exact-head check rollup.

Measure alternating fresh-process `import sky.resources` timings and repeated
current-resource pickle round trips against the exact base. Structural checks
must also prove one dictionary copy, one credential-validation call at each
boundary, and no additional context lookup or other I/O operation.
