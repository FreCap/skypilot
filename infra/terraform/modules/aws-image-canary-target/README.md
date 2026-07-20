# AWS image canary target

This module creates the compute-account role used only by the managed-image
canary worker. It limits EC2 launches to explicit AMIs, subnets, security
groups, runtime roles, and instance profiles, and limits EKS identity checks to
explicit cluster ARNs. Temporary instances must carry the exact SkyPilot
catalog tag, and only matching tagged instances can be read or terminated.

Configure both `ami_arns` and `subnet_arns` for EC2 qualification. An EKS-only
deployment may leave both empty and provide `eks_cluster_arns`; it still lists
the node roles and instance profiles that can be inspected.

Use one module instance per compute account and region. Add its `role_arn` to
`aws-image-worker-identity.canary_target_role_arns`, and use `binding` as the
profile's `canary_launch` access binding. EKS kubeconfig authentication and the
namespace-scoped Pod RBAC are cluster resources and remain explicit inputs to
the Helm deployment; this module does not grant cluster-admin access.
