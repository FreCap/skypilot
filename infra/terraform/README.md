# SkyPilot Terraform modules

Reusable Terraform modules for deploying and operating SkyPilot infrastructure.
Consume a module locally from this repository or pin a full commit from the
authoritative fork:

```hcl
module "pool" {
  source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-pool-aws-vm?ref=<full-commit-sha>"
  # ...
}
```

Provider configuration and backend state belong to the root caller. Module
directories do not commit dependency lock files; executable root configurations
do.

## Module catalog

| Module | Purpose |
| --- | --- |
| [`skypilot-control-plane`](modules/skypilot-control-plane) | Install a PostgreSQL-backed SkyPilot control plane on an existing EKS cluster |
| [`skypilot-pool-aws-vm`](modules/skypilot-pool-aws-vm) | Register an AWS account for direct EC2 workloads |
| [`skypilot-pool-eks`](modules/skypilot-pool-eks) | Register an existing EKS cluster with namespace, identity, priority, storage, and probe partitions |
| [`skypilot-pool-rbac`](modules/skypilot-pool-rbac) | Create cloud-neutral Kubernetes RBAC for a SkyPilot pool |
| [`aws-image-distribution`](modules/aws-image-distribution) | Create managed-image ECR shard rings and target roles |
| [`aws-image-worker-identity`](modules/aws-image-worker-identity) | Create worker identities for managed image operations |
| [`aws-image-canary-account`](modules/aws-image-canary-account) | Bootstrap account-global image-canary prerequisites |
| [`aws-image-canary-target`](modules/aws-image-canary-target) | Create region-specific image qualification targets |

Every module documents prerequisites, security boundaries, inputs, outputs,
upgrade risks, and validation commands in its own README. Run:

```bash
terraform fmt -check -recursive infra/terraform
terraform -chdir=infra/terraform/modules/<module> init \
  -test-directory=terraform-tests
terraform -chdir=infra/terraform/modules/<module> validate
terraform -chdir=infra/terraform/modules/<module> test \
  -test-directory=terraform-tests
```

## Managed container image infrastructure

These modules provision the AWS infrastructure required by the managed
container image distribution design. They are an extension for an existing
SkyPilot control plane in a dedicated AWS account, not a complete VPC, EKS, or
SkyPilot account bootstrap.

The normal installation flow is:

1. Provision the SkyPilot API server, PostgreSQL database, EKS cluster, OIDC
   provider, runtime node roles, and networking through the organization's
   existing platform Terraform.
2. Apply `aws-image-distribution` once per target region. It creates the fixed
   ECR shard rings, qualification repositories, policies, and bounded copy and
   lifecycle roles ahead of activation.
3. Apply `aws-image-worker-identity` in the worker cluster account. It creates
   separate IRSA identities for copy, lifecycle, and runtime-canary workers.
4. Apply `aws-image-canary-account` once in every compute account. It creates
   the account-global EC2 Spot service-linked role used by every region.
5. Apply `aws-image-canary-target` in every compute account and region that will
   be qualified. Pass the account bootstrap output, any customer-managed AMI
   KMS keys, exact EC2 passable roles and profiles, and separate EKS
   inspect-only node profiles. Then pass the exact target role ARNs to the
   worker identity module.
6. Import the generated, secret-free qualification manifest and deploy the
   workers. Qualification and just-in-time regional copies run asynchronously.

Terraform never copies every image to every region. It pre-creates bounded
repository capacity and identities. SkyPilot materializes an immutable digest
only after an explicit prepare request or observed workload demand. Cloudflare
R2 and other S3-compatible stores are supported by the disabled builder
prototype for build inputs and artifacts, but they are not OCI runtime
registries and cannot replace ECR in the AWS runtime path.

See `examples/aws-dedicated-skypilot-account` for composition and the module
READMEs for exact prerequisites.
