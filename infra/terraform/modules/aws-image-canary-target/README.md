# AWS image canary target

This module creates the compute-account role used only by the managed-image
canary worker. It limits EC2 launches to explicit AMIs, subnets, security
groups, runtime roles, and instance profiles, and limits EKS identity checks to
explicit cluster ARNs. Temporary instances must carry the exact SkyPilot
catalog tag. Spot requests carry the same operation tags, and only matching
catalog-tagged instances and requests can be terminated or cancelled.

Configure `ami_arns`, `subnet_arns`, and at least one exact
`canary_instance_type` for EC2 qualification. The launch policy constrains all
three, requires catalog and operation tags, and permits `iam:PassRole` only to
EC2. An EKS-only deployment may leave the EC2 inputs empty and provide
`eks_cluster_arns`; it still lists the node roles and instance profiles that can
be inspected.

Before configuring any EC2 target, apply `aws-image-canary-account` once per
compute account. EC2 targets require that module's
`spot_service_linked_role_arn`, so Terraform orders the account bootstrap before
regional grants and a fresh account cannot accidentally activate with a missing
Spot prerequisite. If a qualified AMI or snapshot uses a customer-managed KMS
key, list every regional key in
`spot_customer_managed_kms_key_arns`; omitting one intentionally makes the Spot
canary fail closed instead of weakening the key policy. AWS-managed EBS keys do
not need a grant and must not be listed.

Use one target module instance per compute account and region. Add its
`role_arn` to `aws-image-worker-identity.canary_target_role_arns`, and use
`binding` as the profile's `canary_launch` access binding. EKS kubeconfig
authentication and the namespace-scoped Pod RBAC are cluster resources and
remain explicit inputs to the Helm deployment; this module does not grant
cluster-admin access.

Run the module contract tests with:

```bash
terraform test -test-directory=terraform-tests
```
