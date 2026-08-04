# Register an AWS account as a SkyPilot direct-VM (EC2) backend.

variable "controller_role_arn" {
  description = <<-EOT
    Exact IAM role ARN used by the SkyPilot control plane. The role is trusted
    to assume the provisioner role created in this account.
  EOT
  type        = string
  nullable    = false

  validation {
    condition = var.controller_role_arn != null && (
      length(var.controller_role_arn) <= 2048 &&
      can(regex(
        "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,64}$",
        var.controller_role_arn,
      ))
    )
    error_message = "controller_role_arn must be a nonempty, exact IAM role ARN with an AWS-valid path and 1-64 character terminal name."
  }
}

variable "external_id" {
  description = "Optional ExternalId required on the AssumeRole (defense in depth for the cross-account trust). Null = no ExternalId."
  type        = string
  default     = null

  validation {
    condition = var.external_id == null || (
      length(var.external_id) >= 2 &&
      length(var.external_id) <= 1224 &&
      can(regex("^[A-Za-z0-9_+=,.@:./-]+$", var.external_id))
    )
    error_message = "external_id must be null or 2-1224 characters from the AWS STS external-ID character set."
  }
}

variable "provisioner_role_name" {
  description = "Name of the provisioner role the control plane assumes to launch EC2 here."
  type        = string
  default     = "skypilot-provisioner"

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.provisioner_role_name))
    error_message = "provisioner_role_name must be a valid IAM role name of 1-64 characters."
  }
}

variable "instance_profile_name" {
  description = "Name of the instance profile/role SkyPilot attaches to launched VMs. SkyPilot expects skypilot-v1 by default."
  type        = string
  default     = "skypilot-v1"

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.instance_profile_name))
    error_message = "instance_profile_name must be a valid IAM role and instance-profile name of 1-64 characters."
  }
}

variable "permissions_boundary_arn" {
  description = "Optional organization-managed IAM permissions boundary attached to both roles created by this module."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.permissions_boundary_arn == null || (
      length(var.permissions_boundary_arn) <= 2048 &&
      can(regex(
        "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:policy/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,128}$",
        var.permissions_boundary_arn,
      ))
    )
    error_message = "permissions_boundary_arn must be an exact IAM managed-policy ARN with an AWS-valid path and 1-128 character terminal name."
  }
}

variable "vm_role_extra_policy_arns" {
  description = "Extra managed policy ARNs to attach to the launched-VM role (e.g. S3 read access for datasets)."
  type        = list(string)
  default     = []

  validation {
    condition = alltrue([
      for arn in var.vm_role_extra_policy_arns :
      length(arn) <= 2048 &&
      can(regex(
        "^arn:aws(-[a-z0-9]+)*:iam::(aws|[0-9]{12}):policy/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,128}$",
        arn,
      ))
    ])
    error_message = "vm_role_extra_policy_arns must contain exact IAM managed-policy ARNs."
  }
}

variable "vm_role_extra_policy_json" {
  description = <<-EOT
    Inline IAM policy JSON attached to the launched-VM role, for resource-scoped
    grants that don't fit a managed-policy ARN — e.g. cross-account ECR pull of the
    model image plus model-weights S3 read. Null = none.
  EOT
  type        = string
  default     = null

  validation {
    condition = var.vm_role_extra_policy_json == null || (
      trimspace(var.vm_role_extra_policy_json) != "" &&
      can(jsondecode(var.vm_role_extra_policy_json))
    )
    error_message = "vm_role_extra_policy_json must be null or valid, nonempty JSON."
  }
}

variable "enable_serve_controller" {
  description = <<-EOT
    Grant the VM role the additional IAM needed by an in-account SkyServe
    controller. The role can pass its own instance profile, manage SSM sessions
    to replicas, and assume the provisioner role; the provisioner trust also
    includes the VM role. Leave false when only the external control plane
    provisions this account.
  EOT
  type        = bool
  default     = false
}

variable "vm_dataset_grants" {
  description = "S3 datasets the launched VMs may read/write in-job. Each grants ListBucket/Get/Put (+ multipart) on the bucket; set kms_key_arn for SSE-KMS buckets to add Decrypt/GenerateDataKey. Cross-account buckets also need the matching bucket and key policies in the owning account."
  type = list(object({
    bucket_arn  = string
    kms_key_arn = optional(string)
  }))
  default = []

  validation {
    condition = alltrue([
      for grant in var.vm_dataset_grants :
      can(regex(
        "^arn:aws(-[a-z0-9]+)*:s3:::[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$",
        grant.bucket_arn,
      )) &&
      (
        grant.kms_key_arn == null ||
        can(regex(
          "^arn:aws(-[a-z0-9]+)*:kms:[a-z0-9-]+:[0-9]{12}:key/(mrk-[0-9a-f]{32}|[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
          grant.kms_key_arn,
        ))
      )
    ])
    error_message = "Each dataset grant must use an exact S3 bucket ARN and, when set, an exact KMS key ARN."
  }
}

variable "enable_ssm" {
  description = <<-EOT
    Attach AmazonSSMManagedInstanceCore to the VM role and allow the
    provisioner to start Session Manager SSH sessions. SkyPilot must separately
    be configured to use SSM or an SSM-based ssh_proxy_command. Instances need
    outbound HTTPS access to the SSM endpoints through NAT or VPC endpoints.
  EOT
  type        = bool
  default     = true
}

variable "tags" {
  description = "Tags applied to IAM resources."
  type        = map(string)
  default     = {}
}
