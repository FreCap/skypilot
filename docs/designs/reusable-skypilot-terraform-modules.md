# Reusable SkyPilot Terraform modules

- **Status:** Accepted after adversarial review
- **Last updated:** 2026-08-05
- **Authoritative repository:** `boltz-bio/skypilot`, branch `improvements`

## Context

SkyPilot deployment Terraform currently lives in `boltz-bio/boltz-platform`.
That makes generally useful control-plane and spoke-workspace-pool building
blocks depend on an application repository, encourages consumers to copy them,
and prevents a SkyPilot release from carrying the infrastructure contract needed
to deploy it.

The modules move to SkyPilot's existing `infra/terraform/modules` tree. The
first consumer remains `boltz-platform`, but the module APIs and documentation
must not encode deployment-specific Boltz accounts, hostnames, images,
repositories, workspaces, or environment topology. The canonical
`boltz-bio/skypilot` distribution URL and repository-authority instruction are
intentional exceptions.

For this fork, `boltz-bio/skypilot:improvements` is the sole source of truth.
Modules consumed across repositories are pinned to an immutable commit from
that branch. Upstream `skypilot-org/skypilot` is not an implementation,
comparison, or history source unless a user explicitly requests it.

## Goals

- Publish the four currently deployed modules from stable, reusable paths:
  - `infra/terraform/modules/skypilot-control-plane`
  - `infra/terraform/modules/skypilot-spoke-workspace-pool-aws-vm`
  - `infra/terraform/modules/skypilot-spoke-workspace-pool-eks`
  - `infra/terraform/modules/skypilot-spoke-workspace-pool-rbac`
- Preserve the existing `boltz-platform` Terraform resource addresses and
  effective provider configuration and defaults while changing the module
  package source.
- Remove deployment-specific examples and assumptions from module code and
  documentation.
- Make IAM roles compatible with organization-managed permissions boundaries
  and AWS partitions without changing the commercial-AWS default behavior.
- Make the published modules provider-neutral child modules; provider
  configuration belongs to the root caller.
- Document prerequisites, inputs, outputs, upgrade behavior, rollback, and an
  immutable Git source example for every module.
- Exercise every published module with formatting, initialization, validation,
  and mocked Terraform contract tests in CI.

## Non-goals

- Provision an EKS cluster, VPC, PostgreSQL database, External Secrets Operator,
  ingress controller outside the chart, or cloud credentials.
- Publish these modules through the Terraform Registry.
- Introduce a Terragrunt API in the SkyPilot repository.
- Generalize the EKS-specific control-plane module to arbitrary Kubernetes
  distributions.
- Change live infrastructure, run `terraform apply`, migrate Terraform state,
  or rename existing Terraform resource labels.
- Move the separate GCP VM-pool module from `boltz-platform`; it is not part of
  the four-module AWS/Kubernetes package being transferred.

## Public contract

### Package and source contract

The module directories above are the public package paths. “Spoke workspace
pool” is package terminology for spoke-side infrastructure where workspace
workloads execute. It does not imply a one-to-one binding: one pool can serve
multiple workspaces, and one workspace can target more than one pool. Existing
Terraform labels, public inputs, persisted names, and SkyPilot `pool` vocabulary
remain unchanged. “Spoke” describes the logical workload side of the topology;
these modules do not require a physically separate account or cluster. A spoke
workspace pool is infrastructure packaging and is unrelated to a managed Job
Pool operated through `sky jobs pool`.

This package-directory rename happens before the draft module stack merges or
becomes a supported consumer contract. It does not rename module/resource labels
or create Terraform `moved` blocks. Existing immutable commits that expose the
earlier paths remain valid for callers pinned to those commits; the draft
`boltz-platform` migration is repinned atomically to a commit containing the new
paths. No moving-branch source or compatibility symlink is introduced.

Cross-repository callers use an immutable Git source:

```hcl
source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-spoke-workspace-pool-aws-vm?ref=<full-commit-sha>"
```

The EKS pool module's relative
`../skypilot-spoke-workspace-pool-rbac` dependency is part of the package
contract. Callers must use Git's `//subdirectory` syntax so Terraform downloads
the repository package and the sibling module remains available.

Moving a caller between a local and remote source does not change Terraform
addresses. Resource and nested-module labels therefore remain unchanged in
this release. No `moved` blocks are required for the `boltz-platform`
migration.

The published modules contain provider requirements but no provider
configuration blocks. A normal Terraform caller inherits or explicitly passes
`aws`, `kubernetes`, `helm`, and `time` providers. A Terragrunt caller generates
the root provider configuration next to the downloaded module. The
`boltz-platform` migration moves the existing EKS exec-auth configuration into
generated root files while retaining the same default provider addresses.

The supported language floor is Terraform or OpenTofu `>= 1.5.0`. SkyPilot CI
tests Terraform 1.14.8. The first consumer additionally tests OpenTofu 1.11.5
through Terragrunt. Provider floors remain AWS `>= 6.24.0`, Helm `>= 3.0`,
Kubernetes `>= 2.20`, and Time `>= 0.9`; the three committed consumer lock
files retain their exact selected versions and hashes.

### `skypilot-control-plane`

This module installs a pinned SkyPilot Helm chart onto an existing EKS cluster.
It creates the namespace when requested, the API-server EKS Pod Identity role,
optional External Secrets resources and read policy, the Helm release, and a
PostgreSQL-backed configuration seed job.

The caller supplies:

- the existing EKS cluster, AWS region and account;
- an exact chart version;
- a pre-created PostgreSQL connection secret;
- a helper container image containing Python, SQLAlchemy, PyYAML, and the
  PostgreSQL SQLAlchemy driver used by the connection string;
- an API-server image only when overriding the chart default;
- any optional OAuth, AWS credentials-file, catalog mirror, GCP WIF, Azure,
  ingress, request-store, or additional Helm configuration.

When GCP or Azure login initialization is enabled, the helper image must also
contain Bash and `gcloud` or `az`, respectively. Existing callers that pass only
`api_server_image` retain that image as the helper-image fallback. The image
reference should be immutable.

The module remains PostgreSQL-only for central API-server state and durable
configuration. API request envelopes have a separate, typed `request_store`
contract. It explicitly renders the chart-compatible SQLite backend, disabled
built-in execution-quiescence enforcement, and durable cutover-gate path by
default. A caller may select PostgreSQL after the chart's fresh-schema bootstrap
or documented one-way cutover has completed. Execution-quiescence enforcement
is valid only with PostgreSQL. `requestStore` is module-owned and cannot also be
set through `extra_helm_values`; callers migrating an existing escape-hatch
block move the same values to `request_store` in one plan so effective Helm
values do not change.

Supported secret integrations accept Kubernetes secret names or cloud secret
identifiers rather than secret payloads. That guarantee does not cover escape
hatches: callers must never put credentials in `config_extra`,
`extra_helm_values`, or `api_server_extra_envs`, because those values can enter
Terraform state and, for configuration, a ConfigMap. A private ECR chart login
also places a short-lived sensitive authorization-token data source in
Terraform state.

Configuration seeding is a durable database mutation. IaC values recursively
override persisted mappings, lists and scalars replace, and `workspaces` is
replaced wholesale. Retired Serve-key pruning is opt-in and one-way. A changed
script, desired configuration, pruning mode, or helper image creates a new seed
generation; after the transaction completes, the module restarts and waits for
the API deployment.

The execution host for that reconciliation needs authenticated `aws` and
`kubectl`, Bash, `mktemp`, and network paths to the EKS API and registries. The
host cluster needs the EKS Pod Identity agent. Enabled External Secret features
require the ESO CRDs and named ClusterSecretStore. Namespace and required
secrets are bootstrapped in a separate first stage; module-managed namespaced
objects explicitly depend on the namespace.

`aws_account_id` is an expected-account guard and is checked against the active
AWS identity; the region is likewise checked against the active provider.
The API-server role name is configurable and defaults to the current
`skypilot-api-${host_cluster_name}` value. The resulting name is validated
against IAM's 64-character limit.

### `skypilot-spoke-workspace-pool-aws-vm`

This module prepares a spoke AWS account in which SkyPilot workspace VMs run by
creating the provisioner role, VM role and instance profile, optional Session
Manager access, optional SkyServe controller permissions, and explicitly
supplied dataset and KMS grants. It does not create a VM during Terraform apply;
SkyPilot launches VMs into the prepared account later.

Role names retain their existing defaults but are caller-configurable. A
nullable permissions-boundary ARN is accepted and attached to every role the
module creates. IAM and managed-policy ARNs derive from the active AWS
partition. The EC2 Spot service-linked role ARN keeps its documented global
`spot.amazonaws.com` service path in every partition while its ARN prefix
follows the active partition; it must not be synthesized from the partition DNS
suffix.

The controller role ARN is a required, nonempty input, while the existing
counted provisioner addresses remain unchanged. The module intentionally grants
broad EC2 provisioning actions on `*`, access to `skypilot-*` storage buckets,
permission to create the EC2 Spot service-linked role, and optional Session
Manager access. Dataset grants and extra policy inputs can broaden that surface
and must be reviewed by the caller.

### `skypilot-spoke-workspace-pool-eks`

This module connects workspace workloads to an existing spoke EKS cluster, maps
a control-plane IAM principal through an EKS access entry, creates one RBAC
partition per namespace, and optionally creates Pod Identity associations,
exact-priority admission policies, static FSx volumes, and a SkyServe probe
ingress rule. It does not provision the EKS cluster.

The controller role ARN is a required, nonempty input.

Partition namespaces, groups, claim names, and role names are durable identity
keys. Changing them can replace infrastructure and is documented as an explicit
migration, not a routine update.

Partitions are workload credential and storage partitions, not independent
controller or tenant trust boundaries. One controller principal receives every
partition group. A namespace without a module-created Pod Identity association
has no identity from this module, but another pre-existing service-account
association can still provide credentials, and this module does not enforce the
SkyPilot workspace's namespace choice. Callers must audit other associations
and pin workspace namespaces.

The priority policy does not inject or default a PriorityClass. It denies a pod
unless the pod explicitly names the configured class. The cluster must support
`admissionregistration.k8s.io/v1` ValidatingAdmissionPolicy.

Probe ingress rejects public CIDRs unless the caller explicitly opts in. Its
description is configurable with a generic default; `boltz-platform` supplies
the exact legacy description during migration so the persisted rule does not
change. EKS access-entry mode, the Pod Identity agent, CSI drivers, storage
classes, VPC CNI routing, and service availability remain caller
prerequisites.

### `skypilot-spoke-workspace-pool-rbac`

This cloud-neutral module creates the namespace when requested, the workload
service account, read-only cluster visibility, namespaced workload lifecycle
permissions, and bindings to caller-supplied Kubernetes subjects.

The caller owns provider configuration and identity mapping. The module never
creates cloud IAM.

The module grants cluster-wide read access to nodes, pods, and RuntimeClasses,
plus namespaced pod, exec, port-forward, service, event, and optional PVC
permissions. Its ClusterRole and ClusterRoleBinding names are cluster-wide;
callers must choose names that do not collide with another module instance.
Changing namespace or service-account ownership flags after adoption requires a
state-aware migration.

## Architecture and invariants

```text
existing EKS host
  └── skypilot-control-plane
        ├── Helm release + PostgreSQL config seed
        └── API-server Pod Identity role
              ├── assumes skypilot-spoke-workspace-pool-aws-vm provisioner role
              └── maps into skypilot-spoke-workspace-pool-eks access entry
                    └── skypilot-spoke-workspace-pool-rbac[partition namespace]
```

- Child resource and module labels are preserved from the deployed modules.
- Existing `count` and `for_each` shapes, partition keys, and volume keys are
  preserved.
- The package-directory rename does not rename internal Terraform labels,
  variables, outputs, physical defaults, tags, service accounts, or namespaces.
- Defaults produce the same commercial-AWS names, policies, tags, and Helm
  values as the local modules they replace.
- Request-store defaults retain the chart's SQLite compatibility behavior;
  PostgreSQL selection is always explicit and cannot be inferred from the
  required central-database connection secret.
- Optional permissions boundaries default to `null`, preserving existing plans.
- Supported named-secret integrations do not ingest long-lived secret payloads.
- The control-plane chart and every cross-repository module source are pinned;
  no moving branch or prerelease lookup is accepted by production callers.
- Namespace and persistent-volume resources retain their existing ownership and
  `Retain` behavior.
- Provider credentials are ambient or short-lived `aws eks get-token` results;
  the EKS token is not persisted in Terraform state.
- Caller-owned objects are explicit: the control-plane module can attach an
  inline read policy to a named ESO role, and the EKS module can mutate a named
  node security group. Those ownership edges and their broad IAM/RBAC surfaces
  are documented.
- Inputs interpolated into local commands and physical identifiers are
  validated for their AWS, Kubernetes, Helm, or GCP grammar. Cross-field checks
  require coherent OAuth/ESO settings, a nonempty EKS partition set, valid FSx
  driver/mount-name combinations, non-public probe sources absent an explicit
  opt-in, and a permissions boundary in the active account and partition.

## Implementation phases

### Phase 1: publish modules in SkyPilot

1. Add the four modules and their operator documentation.
2. Move Kubernetes and Helm provider configuration out of the published
   modules and into caller-owned root configuration.
3. Remove deployment-specific prose and examples.
4. Add helper-image separation, a configurable API role name, optional
   permissions boundaries, partition-aware IAM/FSx values, validations, and
   tests without renaming resource labels.
5. Expand `infra/terraform/README.md` into a module catalog.
6. Add the authoritative-fork agent rule and align the formatter baseline with
   `origin/improvements`.

### Phase 2: migrate `boltz-platform`

1. Centralize one full 40-hex commit SHA from the Phase 1 pull request for
   review. Before Phase 2 can merge or apply, Phase 1 must merge and the pin
   must be updated to a commit reachable from `origin/improvements`.
2. Generate the existing EKS exec-auth providers only where used: Kubernetes
   and Helm in the control-plane root, Kubernetes in the EKS-pool root, and no
   additional provider in the AWS-VM root. Change all three live callers to the
   corresponding remote module source.
3. Synchronize and retain the local module copies temporarily so a source
   rollback remains valid with caller-owned providers. Preserve the legacy
   underscore directory layout and the EKS module's valid
   `../skypilot_pool_rbac` sibling path; initialize and validate every retained
   local source as well as every remote source.
4. Split tests: generic seed/module tests live in SkyPilot; Rainier pin,
   paid-capacity, auth-ring, PVC, pruning, and image assertions remain in
   `boltz-platform` at an environment-owned path. Update pre-commit, CI, the GCP
   pool README, and runbook references.
5. Keep all three committed environment `.terraform.lock.hcl` files byte-for-byte
   unchanged.
6. Run Terragrunt/OpenTofu source initialization, validation, provider
   comparison, and read-only old-source versus new-source plans. The old-source
   baseline is the untouched module tree from pre-Phase-2 `origin/main`, not the
   synchronized compatibility copies on the migration branch. Every plan must
   show no managed-resource change before an operator applies it. The published
   root modules intentionally add useful outputs, so a first plan may contain
   output-only state additions and return detailed exit code 2; those are
   acceptable only when plan JSON confirms an empty managed-resource action
   set.

### Phase 3: remove compatibility copies

Open a stacked draft that deletes the four unused local module directories and
updates stale local-path documentation. It must not merge until Phase 2 is
merged, applied, and operationally verified in all three Terraform states.

## Deployment and rollback

No infrastructure is applied by these pull requests.

Phase 2 relocates source and provider configuration without changing provider
addresses. An operator rolls it out with the ordinary Terragrunt/OpenTofu
plan/apply workflow for each existing unit. Because resource labels, nested
module paths, provider addresses, physical values, and lock files are unchanged,
OpenTofu should refresh existing objects at their current addresses and propose
no managed-resource changes. New root outputs may be recorded in state.

Before Phase 3 merges, rollback is a source change back to the synchronized
local path. After Phase 3 merges, rollback is a Git revert of Phase 3 followed
by that source change. The production pin always names a commit reachable from
`improvements`, so deleting the feature branch cannot remove it.

If any consumer plan proposes replacement, deletion, an address move, IAM
policy broadening, namespace recreation, or persistent-volume recreation, stop
the rollout. Correct the module contract first; do not approve the plan or add
ad hoc state commands.

Changing `request_store.backend` from SQLite to PostgreSQL is an operational
one-way boundary, not an ordinary Terraform rollback. Existing installations
must complete and verify the chart's cutover gate before applying that input.
After cutover, rollback retains PostgreSQL, the enforcement setting, and the
same gate path while reverting other release changes; it must never restore
SQLite merely by omitting the module input.

## Verification

Required before opening the pull requests:

- `terraform fmt -check -recursive infra/terraform`
- `terraform init`, `terraform validate`, and
  `terraform test -test-directory=terraform-tests` for every new module
- the control-plane seed-script unit suite
- a guard that every module directory has a Terraform test suite
- commercial AWS, GovCloud, and China mocked contract tests for IAM, Secrets
  Manager, S3, SSM, EC2, managed-policy, service-linked-role, service-principal,
  and FSx DNS partition values; correct syntax does not promise each service is
  available in every region or partition
- automated checks for unchanged resource/module labels, `count`/`for_each`
  shapes and identity keys, full 40-hex source pins, and unchanged consumer lock
  files
- Terragrunt/OpenTofu 1.11.5 source initialization and validation for all three
  `boltz-platform` callers
- old-source and new-source plan JSON comparison where the required backend and
  cloud credentials are available
- repository searches proving no deployment-specific account, hostname,
  repository, workspace, or image remains in the reusable module package
- a stale-reference gate proving published module code, the module catalog and
  READMEs, Terraform tests, and CI contain none of the old hyphenated package
  paths `skypilot-pool-aws-vm`, `skypilot-pool-eks`,
  `skypilot-pool-rbac`, or the old nested source
  `../skypilot-pool-rbac`; the temporary underscore-form `boltz-platform`
  rollback directories and `../skypilot_pool_rbac` source are the only
  pre-Phase-3 exception and are validated separately

The SkyPilot stale-reference gate is:

```bash
if rg -n 'skypilot-pool-(aws-vm|eks|rbac)|\.\./skypilot-pool-rbac' \
    infra/terraform .github/workflows; then
  exit 1
fi
```

Manual verification for the eventual rollout:

1. Plan the control-plane unit and verify zero resource changes.
2. Plan the EKS pool and verify the access entry, every
   `module.rbac[namespace]`, admission object, FSx PV/PVC, Pod Identity
   association, and security-group rule remain at their existing addresses.
3. Plan the AWS VM pool and verify both IAM roles, the instance profile,
   policies, and attachments remain in place.
4. Apply only after those checks, then verify the SkyPilot API health endpoint,
   workspace/cloud checks, one EKS launch/down, and one EC2 launch/down.

## Verification evidence

The pre-rename Phase 1 implementation was verified on 2026-07-31 with Terraform
1.14.8:

- `terraform fmt -check -recursive infra/terraform` passed.
- `terraform init -backend=false -test-directory=terraform-tests`,
  `terraform validate`, and
  `terraform test -test-directory=terraform-tests` passed for all eight
  modules under `infra/terraform/modules` (155 tests total; the four modules in
  this design contributed 46 tests).
- `uv run --no-project --with 'PyYAML>=6,<7' --with 'SQLAlchemy>=2,<3'
  python -m unittest -v test_seed_config.py` passed all 24 tests.
- An independent comparison against untouched `boltz-platform@origin/main`
  confirmed identical managed-resource/module labels, collection identity
  shapes, commercial-AWS effective values, and seed-script bytes. The seed
  script SHA-256 is
  `b87765a0f58db47b8cade97a8ebf6224e7558b31ba105478e3493f4615a1ca74`.
- The pre-rename design passed adversarial review after the Spot service-name
  and output-only plan contracts were clarified.

The rename-specific verification also completed on 2026-07-31:

- Terraform 1.14.8 formatting, initialization, validation, and all 155 tests
  passed across the eight published module roots. The four modules in this
  design contributed 9 control-plane, 11 AWS VM, 16 EKS, and 10 RBAC tests.
- Terraform-docs 0.20.0 checks passed for all four modules, and no module-root
  dependency lockfiles remain in the package tree.
- The seed suite passed all 24 tests and retained the SHA-256 recorded above.
- The stale-reference gate, actionlint 1.7.7, and working-tree/index diff checks
  passed.
- An independent comparison against both pre-rename SkyPilot commit
  `fe2938b71cd0559199909d5a31147cd93fad8c5d` and untouched
  `boltz-platform@origin/main` confirmed that the only executable rename change
  is the EKS sibling module source. Managed resource/module labels,
  `count`/`for_each` expressions, variable defaults, outputs, tags, physical
  names, commercial-AWS effective values, and seed bytes are unchanged.
- The rename contract passed adversarial review before implementation and again
  against the completed verification evidence.

Phase 2 commands and PR descriptions must record their exact results. No live
plan or apply evidence is claimed until the corresponding command has run
successfully.

The typed request-store contract was verified on 2026-08-05 with Terraform
1.15.8: formatting and all 16 control-plane mocked contract/validation tests
passed. Coverage includes explicit default rendering, PostgreSQL field mapping,
invalid backend and gate inputs, enforcement with SQLite, and rejection of a
conflicting `extra_helm_values.requestStore` block.

## Open gates

- Phase 2 may use a pushed Phase 1 SHA for draft review, but cannot merge or
  apply until Phase 1 is merged and the pin is reachable from
  `origin/improvements`.
- Phase 3 remains a stacked draft until Phase 2 is merged, applied, and verified
  against all three states.
- Any intentional resource-label change requires updating this design first and
  adding a reviewed address migration; none is currently planned.
