# Reusable SkyPilot Terraform modules

- **Status:** Accepted after adversarial review
- **Last updated:** 2026-07-31
- **Authoritative repository:** `boltz-bio/skypilot`, branch `improvements`

## Context

SkyPilot deployment Terraform currently lives in `boltz-bio/boltz-platform`.
That makes generally useful control-plane and compute-pool building blocks depend
on an application repository, encourages consumers to copy them, and prevents a
SkyPilot release from carrying the infrastructure contract needed to deploy it.

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
  - `infra/terraform/modules/skypilot-pool-aws-vm`
  - `infra/terraform/modules/skypilot-pool-eks`
  - `infra/terraform/modules/skypilot-pool-rbac`
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

The module directories above are the public package paths. Cross-repository
callers use an immutable Git source:

```hcl
source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-pool-aws-vm?ref=<full-commit-sha>"
```

The EKS pool module's relative `../skypilot-pool-rbac` dependency is part of the
package contract. Callers must use Git's `//subdirectory` syntax so Terraform
downloads the repository package and the sibling module remains available.

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
  ingress, or additional Helm configuration.

When GCP or Azure login initialization is enabled, the helper image must also
contain Bash and `gcloud` or `az`, respectively. Existing callers that pass only
`api_server_image` retain that image as the helper-image fallback. The image
reference should be immutable.

The module remains PostgreSQL-only. Supported secret integrations accept
Kubernetes secret names or cloud secret identifiers rather than secret payloads.
That guarantee does not cover escape hatches: callers must never put credentials
in `config_extra`, `extra_helm_values`, or `api_server_extra_envs`, because those
values can enter Terraform state and, for configuration, a ConfigMap. A private
ECR chart login also places a short-lived sensitive authorization-token data
source in Terraform state.

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

### `skypilot-pool-aws-vm`

This module registers an AWS account for direct EC2 workloads by creating the
provisioner role, VM role and instance profile, optional Session Manager access,
optional SkyServe controller permissions, and explicitly supplied dataset and
KMS grants.

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

### `skypilot-pool-eks`

This module registers an existing EKS cluster, maps a control-plane IAM
principal through an EKS access entry, creates one RBAC partition per namespace,
and optionally creates Pod Identity associations, exact-priority admission
policies, static FSx volumes, and a SkyServe probe ingress rule.

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

### `skypilot-pool-rbac`

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
              ├── assumes skypilot-pool-aws-vm provisioner role
              └── maps into skypilot-pool-eks access entry
                    └── skypilot-pool-rbac[partition namespace]
```

- Child resource and module labels are preserved from the deployed modules.
- Existing `count` and `for_each` shapes, partition keys, and volume keys are
  preserved.
- Defaults produce the same commercial-AWS names, policies, tags, and Helm
  values as the local modules they replace.
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

Phase 1 was verified on 2026-07-31 with Terraform 1.14.8:

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
- The exact design passed adversarial review after the Spot service-name and
  output-only plan contracts were clarified.

Phase 2 commands and PR descriptions must record their exact results. No live
plan or apply evidence is claimed until the corresponding command has run
successfully.

## Open gates

- Phase 2 may use a pushed Phase 1 SHA for draft review, but cannot merge or
  apply until Phase 1 is merged and the pin is reachable from
  `origin/improvements`.
- Phase 3 remains a stacked draft until Phase 2 is merged, applied, and verified
  against all three states.
- Any intentional resource-label change requires updating this design first and
  adding a reviewed address migration; none is currently planned.
