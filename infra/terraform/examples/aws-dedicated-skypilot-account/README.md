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
   SkyPilot configuration revision, and list the exact EC2/EKS pull roles.
3. Run `terraform init`, `terraform plan`, and `terraform apply`.
4. Put `qualification_config_map_data` into a Kubernetes ConfigMap and set
   `imageCopyWorker.qualificationManifestConfigMapName` to its name.
5. Apply `aws-image-canary-account` once in each compute account. This example
   creates it for the dedicated account. Import the existing service-linked
   role when the account has already used EC2 Spot.
6. Apply `aws-image-canary-target` in each compute account/region, pass the
   account module output plus every customer-managed AMI KMS key, and provide
   its role ARN through `canary_target_role_arns` before enabling automatic
   canaries.
7. Apply `helm_image_worker_values`, together with the external PostgreSQL
   connection secret, when upgrading the SkyPilot Helm release.

Qualification remains asynchronous. Applying Terraform does not block a model
deployment and does not claim that runtime principals have passed their
canaries. Until a profile is fully qualified, opted-in managed placement fails
closed while direct OCI workspaces retain their existing behavior.

Repository creation is automatic at Terraform apply time. Image copying is not
eager: the canonical publication is explicit, and regional copies are created
just in time from durable workload demand or an explicit prepare operation.
