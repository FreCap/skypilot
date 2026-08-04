# SkyPilot AWS direct-VM spoke workspace pool

Prepares a spoke AWS account as a SkyPilot workspace pool for direct VMs (EC2).
The module is pure IAM: it does not create a VM, VPC, subnet, security group,
EKS cluster, or SkyPilot control plane. SkyPilot launches workspace VMs into the
prepared account later. A pool may serve multiple workspaces; the name does not
imply a one-to-one workspace binding. “Spoke” is logical and does not require a
separate AWS account. This infrastructure package is unrelated to a managed Job
Pool operated through `sky jobs pool`.

The module creates:

- a provisioner role trusted by the SkyPilot control-plane role;
- a VM role and instance profile attached to instances launched by SkyPilot;
- optional Session Manager permissions;
- optional permissions for an in-account SkyServe controller; and
- caller-supplied managed policies, inline policy JSON, and S3/KMS dataset
  grants for the VM role.

## Requirements

- Terraform or OpenTofu 1.5 or newer.
- AWS provider 6.24 or newer, configured by the root module.
- An existing SkyPilot control-plane IAM role in the same AWS partition.
- Permission to manage IAM roles, inline policies, managed-policy attachments,
  and instance profiles in the target account.

The default IAM policies intentionally grant broad EC2 provisioning actions on
`*`, access to `skypilot-*` storage buckets, and permission to create the EC2
Spot service-linked role. Review the rendered policies before applying them.
Extra policies and dataset grants can broaden that surface.

## Usage

Pin cross-repository consumers to an immutable commit:

```hcl
module "skypilot_spoke_workspace_pool_aws_vm" {
  source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-spoke-workspace-pool-aws-vm?ref=<full-commit-sha>"

  controller_role_arn     = "arn:aws:iam::111122223333:role/skypilot-api"
  permissions_boundary_arn = "arn:aws:iam::444455556666:policy/platform-boundary"

  vm_dataset_grants = [{
    bucket_arn  = "arn:aws:s3:::example-training-data"
    kms_key_arn = "arn:aws:kms:us-east-1:444455556666:key/00000000-0000-4000-8000-000000000001"
  }]

  tags = {
    Environment = "production"
  }
}
```

Apply the control plane first, pass its API-server role ARN as
`controller_role_arn`, and add the resulting `provisioner_role_arn` to the
control-plane role's `sts:AssumeRole` policy. Configure a standard AWS profile
for the matching SkyPilot workspace or workspaces:

```ini
[profile compute]
role_arn = arn:aws:iam::444455556666:role/skypilot-provisioner
credential_source = EcsContainer
region = us-east-1
```

If `external_id` is set, the profile or credential process must send that same
external ID. For a controller that may also run on an EC2 VM, use a
`credential_process` that works with both the control-plane workload's ambient
credentials and EC2 instance metadata.

## Session Manager and SkyServe

`enable_ssm = true` attaches `AmazonSSMManagedInstanceCore` to the VM role and
allows the provisioner to start `AWS-StartSSHSession` sessions. Instances still
need outbound HTTPS connectivity to the SSM services, through NAT or VPC
endpoints. SkyPilot workspace configuration must select its SSM connection
path and, for cross-account use, the correct AWS profile.

`enable_serve_controller = true` additionally lets the VM role pass its own
instance profile, manage SSM sessions to replicas, and assume the provisioner
role. It also adds the VM role to the provisioner trust policy. Enable this only
when a SkyServe controller launched in the target account must create replicas
there with its instance-metadata credentials.

## AWS partitions

IAM, EC2, S3, SSM, service principal, and AWS-managed policy ARNs are derived
from the active AWS partition. Input role, policy, bucket, KMS key, and
permissions-boundary ARNs are checked against that partition. The module
supports commercial AWS, AWS GovCloud (US), and AWS China when the configured
provider and supplied ARNs agree.

The EC2 Spot service-linked role keeps AWS's global `spot.amazonaws.com`
service name and path in every partition; only the ARN prefix changes.

The permissions boundary, when supplied, must be an IAM managed policy in the
target account. It is attached to both roles created by the module.

## Upgrade and rollback

The resource labels and `count` shapes match the original deployed module:
`aws_iam_role.provisioner[0]` and its dependent counted resources keep their
existing addresses. Moving an unchanged caller from a local source to this Git
source therefore requires no `moved` blocks. Confirm with a saved plan before
applying.

Changing either role name replaces IAM resources. Removing or changing a
permissions boundary may be prohibited by organization policy. To roll back a
source migration, restore the previous module source at the same resource
addresses and run another plan; do not remove resources from state.

## Module reference

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.5.0 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 6.24.0 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_aws"></a> [aws](#provider\_aws) | >= 6.24.0 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [aws_iam_instance_profile.vm](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_instance_profile) | resource |
| [aws_iam_role.provisioner](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role.vm](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role) | resource |
| [aws_iam_role_policy.provisioner](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [aws_iam_role_policy.vm](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [aws_iam_role_policy.vm_assume_provisioner](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [aws_iam_role_policy.vm_extra_inline](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [aws_iam_role_policy.vm_serve_replica](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy) | resource |
| [aws_iam_role_policy_attachment.vm_extra](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_iam_role_policy_attachment.vm_ssm](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_role_policy_attachment) | resource |
| [aws_caller_identity.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/caller_identity) | data source |
| [aws_iam_policy_document.provisioner](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.provisioner_assume](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.vm](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.vm_assume](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.vm_assume_provisioner](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_iam_policy_document.vm_serve_replica](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/iam_policy_document) | data source |
| [aws_partition.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/partition) | data source |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_controller_role_arn"></a> [controller\_role\_arn](#input\_controller\_role\_arn) | Exact IAM role ARN used by the SkyPilot control plane. The role is trusted<br/>to assume the provisioner role created in this account. | `string` | n/a | yes |
| <a name="input_enable_serve_controller"></a> [enable\_serve\_controller](#input\_enable\_serve\_controller) | Grant the VM role the additional IAM needed by an in-account SkyServe<br/>controller. The role can pass its own instance profile, manage SSM sessions<br/>to replicas, and assume the provisioner role; the provisioner trust also<br/>includes the VM role. Leave false when only the external control plane<br/>provisions this account. | `bool` | `false` | no |
| <a name="input_enable_ssm"></a> [enable\_ssm](#input\_enable\_ssm) | Attach AmazonSSMManagedInstanceCore to the VM role and allow the<br/>provisioner to start Session Manager SSH sessions. SkyPilot must separately<br/>be configured to use SSM or an SSM-based ssh\_proxy\_command. Instances need<br/>outbound HTTPS access to the SSM endpoints through NAT or VPC endpoints. | `bool` | `true` | no |
| <a name="input_external_id"></a> [external\_id](#input\_external\_id) | Optional ExternalId required on the AssumeRole (defense in depth for the cross-account trust). Null = no ExternalId. | `string` | `null` | no |
| <a name="input_instance_profile_name"></a> [instance\_profile\_name](#input\_instance\_profile\_name) | Name of the instance profile/role SkyPilot attaches to launched VMs. SkyPilot expects skypilot-v1 by default. | `string` | `"skypilot-v1"` | no |
| <a name="input_permissions_boundary_arn"></a> [permissions\_boundary\_arn](#input\_permissions\_boundary\_arn) | Optional organization-managed IAM permissions boundary attached to both roles created by this module. | `string` | `null` | no |
| <a name="input_provisioner_role_name"></a> [provisioner\_role\_name](#input\_provisioner\_role\_name) | Name of the provisioner role the control plane assumes to launch EC2 here. | `string` | `"skypilot-provisioner"` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Tags applied to IAM resources. | `map(string)` | `{}` | no |
| <a name="input_vm_dataset_grants"></a> [vm\_dataset\_grants](#input\_vm\_dataset\_grants) | S3 datasets the launched VMs may read/write in-job. Each grants ListBucket/Get/Put (+ multipart) on the bucket; set kms\_key\_arn for SSE-KMS buckets to add Decrypt/GenerateDataKey. Cross-account buckets also need the matching bucket and key policies in the owning account. | <pre>list(object({<br/>    bucket_arn  = string<br/>    kms_key_arn = optional(string)<br/>  }))</pre> | `[]` | no |
| <a name="input_vm_role_extra_policy_arns"></a> [vm\_role\_extra\_policy\_arns](#input\_vm\_role\_extra\_policy\_arns) | Extra managed policy ARNs to attach to the launched-VM role (e.g. S3 read access for datasets). | `list(string)` | `[]` | no |
| <a name="input_vm_role_extra_policy_json"></a> [vm\_role\_extra\_policy\_json](#input\_vm\_role\_extra\_policy\_json) | Inline IAM policy JSON attached to the launched-VM role, for resource-scoped<br/>grants that don't fit a managed-policy ARN — e.g. cross-account ECR pull of the<br/>model image plus model-weights S3 read. Null = none. | `string` | `null` | no |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_instance_profile_arn"></a> [instance\_profile\_arn](#output\_instance\_profile\_arn) | ARN of the instance profile attached to launched VMs. |
| <a name="output_instance_profile_name"></a> [instance\_profile\_name](#output\_instance\_profile\_name) | Name of the instance profile attached to launched VMs (skypilot-v1). |
| <a name="output_provisioner_role_arn"></a> [provisioner\_role\_arn](#output\_provisioner\_role\_arn) | ARN of the role the control plane assumes to launch EC2 here. Use it in the<br/>API server's ~/.aws/config profile (role\_arn=...) and grant the control-plane<br/>Pod Identity role sts:AssumeRole on it. |
| <a name="output_provisioner_role_name"></a> [provisioner\_role\_name](#output\_provisioner\_role\_name) | Name of the role the SkyPilot control plane assumes to manage EC2 resources. |
| <a name="output_vm_role_arn"></a> [vm\_role\_arn](#output\_vm\_role\_arn) | ARN of the IAM role attached to launched VMs through the instance profile. |
| <a name="output_vm_role_name"></a> [vm\_role\_name](#output\_vm\_role\_name) | Name of the IAM role attached to launched VMs through the instance profile. |
<!-- END_TF_DOCS -->
