# AWS image canary target

This module creates the compute-account role used only by the managed-image
canary worker. It limits EC2 launches to explicit AMIs, subnets, security
groups, runtime roles, and instance profiles, and limits EKS identity checks to
explicit cluster ARNs. Temporary instances must carry the exact SkyPilot
catalog tag, and only matching tagged instances can be read or terminated.

Configure `ami_arns`, `subnet_arns`, and at least one exact
`canary_instance_type` for EC2 qualification. The launch policy constrains all
three plus the runtime instance profile, requires catalog and operation tags on
the created instance and EBS volumes, and permits `iam:PassRole` only to EC2.
AMI policy resources must use EC2's accountless authorization form,
`arn:<partition>:ec2:<region>::image/<ami-id>`, including for private AMIs.
AWS does not expose those request tags or the instance type while authorizing
the implicit primary network interface. Its separate `RunInstances` statement
is therefore limited to the exact configured subnets. It cannot create a
network interface independently, and the complete launch must still satisfy the
exact AMI, security-group, instance-type, instance-profile, and tagged-resource
statements. An EKS-only deployment may leave the EC2 inputs empty and provide
`eks_cluster_arns`; it still lists the node roles and instance profiles that can
be inspected.

Use one module instance per compute account and region. Add its `role_arn` to
`aws-image-worker-identity.canary_target_role_arns`, and use `binding` as the
profile's `canary_launch` access binding. EKS kubeconfig authentication and the
namespace-scoped Pod RBAC are cluster resources and remain explicit inputs to
the Helm deployment; this module does not grant cluster-admin access.
