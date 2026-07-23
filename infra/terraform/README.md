# Managed container image infrastructure

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
   be qualified. Pass the account bootstrap output and any customer-managed AMI
   KMS keys, then pass the exact target role ARNs to the worker identity module.
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
