# Managed image extension for a dedicated SkyPilot AWS account

This example composes the regional ECR distribution module and the separate
worker identity module for two AWS regions. It assumes the dedicated account
already has the SkyPilot control plane, external PostgreSQL, EKS/OIDC provider,
runtime roles, and networking. It does not create a VPC, EKS cluster, or the API
server. Add more aliased-provider module calls for more regions. Terraform
creates every managed-image repository and IAM boundary before the profile is
activated.

1. Read the stable catalog authority from the Images Readiness API or Dashboard.
2. Copy `terraform.tfvars.example`, set the profile hashes from the matching
   SkyPilot configuration revision, list the exact EC2/EKS pull roles, and keep
   the qualification repository generation at its backward-compatible default
   of `0` for the initial deployment.
3. Run `terraform init`, `terraform plan`, and `terraform apply`.
4. Create an immutable Kubernetes ConfigMap from
   `qualification_config_map_data` using the digest-derived
   `qualification_config_map_name`. The `helm_image_worker_values` output wires
   that exact name into
   `imageCopyWorker.qualificationManifestConfigMapName`.
5. Apply `aws-image-canary-account` once in each compute account. This example
   creates it for the dedicated account. Import the existing service-linked
   role when the account has already used EC2 Spot.
6. Apply `aws-image-canary-target` in each compute account/region, pass the
   account module output plus every customer-managed AMI KMS key, list EC2
   passable roles and profiles separately from EKS inspect-only node profiles,
   and provide its role ARN through `canary_target_role_arns` before enabling
   automatic canaries.
7. For a fresh installation with no older image-worker binary or durable
   qualification state, apply `helm_image_worker_values` together with the
   external PostgreSQL connection secret only after qualification is ready for
   steady-state workers. For an existing installation's first protocol-2
   upgrade, that all-on output is not a safe upgrade shortcut. Follow the
   canonical staged rollout below.

Create and verify the immutable handoff before enabling the copy worker:

```bash
NAMESPACE=skypilot
NAME="$(terraform output -raw qualification_config_map_name)"
terraform output -json qualification_config_map_data |
  jq --arg name "$NAME" --arg namespace "$NAMESPACE" \
    '{apiVersion:"v1",kind:"ConfigMap",metadata:{name:$name,namespace:$namespace},immutable:true,data:.}' |
  kubectl apply -f -
test "$(kubectl get configmap "$NAME" -n "$NAMESPACE" -o jsonpath='{.immutable}')" = true
```

Because the name is derived from the complete handoff content, any manifest
change produces a new name. Kubernetes rejects an in-place data change under an
existing immutable name.

For the first protocol-2 upgrade, pin the exact target image and use
`helm upgrade --reuse-values` for every phase:

1. Establish `Q`, then `T0`, with
   `imageCopyWorker.enabled=false`,
   `imageLifecycleWorker.enabled=false`, and
   `imageCanaryWorker.enabled=false`.
2. Run copy-only with `imageCopyWorker.enabled=true`,
   `imageCopyWorker.replicaCount=1`,
   `imageLifecycleWorker.enabled=false`,
   `imageCanaryWorker.enabled=true`, and
   `imageCanaryWorker.replicaCount=0`.
3. Add lifecycle with `imageLifecycleWorker.enabled=true` and
   `imageLifecycleWorker.replicaCount=1`; the target chart renders its
   Deployment with `Recreate`. Keep the canary worker at zero.
4. After zero unrelated `PENDING` and zero `RUNNING` canaries are proved and
   only the intended target is enqueued, set
   `imageCanaryWorker.replicaCount=1` and
   `imageCanaryWorker.maxInFlight=1` through that proof's verified terminal
   state. Restore reviewed steady-state concurrency only afterward.

Use a new immutable qualification-manifest ConfigMap name for the target
generation, and verify each phase before applying the next one. The complete
quiescence, migration, generation handoff, rollback, and database checks remain
authoritative in
`docs/designs/managed-container-image-distribution.md`.

If lifecycle qualification quarantines a repository, roll forward without
destroying it. Add a new integer to `qualification_repository_generations`,
keep generation `0` and every previously applied generation, and select the new
highest value with `active_qualification_repository_generation`. The active
value must never move backward to a quarantined generation. The example applies
the same retained set and active generation in both regions. Review
`qualification_repositories_by_region` after apply, then use that same active
generation in the new SkyPilot profile revision. Terraform's `prevent_destroy`
guard intentionally rejects removing any retained repository from state.

Keep every image worker disabled throughout that Terraform apply. The active
generation must be the only qualification repository with the full copy,
lifecycle, and runtime-pull allow policy. Every inactive retained generation
must have a wildcard-principal, deny-only repository policy covering all ECR
image data-plane actions. That deny must exclude `ecr:DescribeRepositories`,
`ecr:GetRepositoryPolicy`, `ecr:ListTagsForResource`, and repository, policy,
and tag management actions so Terraform can still refresh and reconcile the
retained resource. Because AWS policy updates are not atomic across resources,
do not create the handoff ConfigMap, ingest its profile revision, or enable a
worker until the complete apply succeeds and exact repository-policy readback
confirms that state in every configured region. A partial apply is a hard stop:
leave workers off and reapply until it converges.

Qualification remains asynchronous. Applying Terraform does not block a model
deployment and does not claim that runtime principals have passed their
canaries. Until a profile is fully qualified, opted-in managed placement fails
closed while direct OCI workspaces retain their existing behavior.

Repository creation is automatic at Terraform apply time. Image copying is not
eager: the canonical publication is explicit, and regional copies are created
just in time from durable workload demand or an explicit prepare operation.
