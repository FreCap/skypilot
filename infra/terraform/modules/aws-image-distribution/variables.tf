variable "catalog_authority" {
  description = "Stable UUID returned by the SkyPilot Images readiness API."
  type        = string

  validation {
    condition     = can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", var.catalog_authority))
    error_message = "catalog_authority must be a lowercase UUID."
  }
}

variable "catalog_authority_base32" {
  description = "Lowercase, unpadded base32 encoding of the catalog authority UUID."
  type        = string

  validation {
    condition     = can(regex("^[a-z2-7]{26}$", var.catalog_authority_base32))
    error_message = "catalog_authority_base32 must be the 26-character unpadded UUID encoding."
  }
}

variable "realm" {
  description = "Stable operator-selected registry realm."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{0,31}$", var.realm))
    error_message = "realm must be a lowercase identifier of at most 32 characters."
  }
}

variable "profile" {
  description = "SkyPilot managed registry profile name."
  type        = string
}

variable "registry_account_id" {
  description = "Dedicated AWS account in which this module is applied."
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.registry_account_id))
    error_message = "registry_account_id must be a 12-digit AWS account ID."
  }
}

variable "region" {
  description = "Region of the AWS provider passed to this module."
  type        = string
}

variable "repository_prefix" {
  description = "Root ECR repository path owned by this SkyPilot catalog."
  type        = string
  default     = "skypilot/images"

  validation {
    condition     = can(regex("^[a-z0-9]+(?:[._/-][a-z0-9]+)*$", var.repository_prefix))
    error_message = "repository_prefix must be a valid lowercase ECR repository path."
  }
}

variable "qualification_repository_generations" {
  description = "Retained qualification repository generations. Generation 0 is the legacy fixed path and must remain present."
  type        = set(number)
  default     = [0]

  validation {
    condition = (
      length(var.qualification_repository_generations) >= 1 &&
      alltrue([
        for generation in var.qualification_repository_generations :
        generation >= 0 && generation <= 255 && floor(generation) == generation
      ])
    )
    error_message = "qualification_repository_generations must contain unique integer generations from 0 through 255."
  }
}

variable "active_qualification_repository_generation" {
  description = "Highest retained qualification repository generation emitted in the active Terraform handoff."
  type        = number
  default     = 0

  validation {
    condition = (
      var.active_qualification_repository_generation >= 0 &&
      var.active_qualification_repository_generation <= 255 &&
      floor(var.active_qualification_repository_generation) == var.active_qualification_repository_generation
    )
    error_message = "active_qualification_repository_generation must be an integer from 0 through 255."
  }
}

variable "workspaces" {
  description = "Complete bounded workspace set provisioned in this region."
  type        = set(string)

  validation {
    condition     = length(var.workspaces) > 0 && length(var.workspaces) <= 256 && alltrue([for workspace in var.workspaces : length(trimspace(workspace)) > 0])
    error_message = "workspaces must contain between 1 and 256 nonempty names."
  }
}

variable "targets" {
  description = "Profile targets in this provider region, including canonical when applicable."
  type = map(object({
    canonical                    = bool
    shard_count                  = number
    max_manifests_per_shard      = number
    max_declared_bytes_per_shard = number
    max_in_flight                = number
    runtime_pull_principal_arns  = set(string)
  }))

  validation {
    condition = length(var.targets) == 1 && alltrue([
      for name, target in var.targets :
      can(regex("^[a-z0-9][a-z0-9-]{0,62}$", name)) &&
      target.shard_count >= 1 && target.shard_count <= 256 &&
      target.max_manifests_per_shard >= 1 &&
      target.max_declared_bytes_per_shard >= 1 &&
      target.max_in_flight >= 1 && target.max_in_flight <= 1024 &&
      length(target.runtime_pull_principal_arns) <= 100 &&
      alltrue([
        for arn in target.runtime_pull_principal_arns :
        can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$", arn))
      ])
    ])
    error_message = "Provide exactly one regional target with a valid name, bounded fixed shards, positive ceilings, and at most 100 exact IAM role pull principals."
  }
}

variable "copy_worker_base_role_arns" {
  description = "Worker base roles allowed to assume the regional copy target role."
  type        = set(string)
  default     = []

  validation {
    condition = length(var.copy_worker_base_role_arns) <= 64 && alltrue([
      for arn in var.copy_worker_base_role_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$", arn))
    ])
    error_message = "copy_worker_base_role_arns must contain at most 64 exact IAM role ARNs with AWS-valid paths and 1-64 character terminal names."
  }
}

variable "lifecycle_worker_base_role_arns" {
  description = "Worker base roles allowed to assume the regional lifecycle target role."
  type        = set(string)
  default     = []

  validation {
    condition = length(var.lifecycle_worker_base_role_arns) <= 64 && alltrue([
      for arn in var.lifecycle_worker_base_role_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$", arn))
    ])
    error_message = "lifecycle_worker_base_role_arns must contain at most 64 exact IAM role ARNs with AWS-valid paths and 1-64 character terminal names."
  }
}

variable "applied_role_trust_policy_quota" {
  description = "Applied IAM role trust-policy character quota in the registry account."
  type        = number
  default     = 2048

  validation {
    condition = (
      var.applied_role_trust_policy_quota >= 2048 &&
      var.applied_role_trust_policy_quota <= 8192 &&
      floor(var.applied_role_trust_policy_quota) == var.applied_role_trust_policy_quota
    )
    error_message = "applied_role_trust_policy_quota must be an integer between 2048 and 8192."
  }
}

variable "copy_target_role_name" {
  description = "Deterministic copy target role name for this region."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.copy_target_role_name))
    error_message = "copy_target_role_name must be a valid 1-64 character IAM role name."
  }
}

variable "lifecycle_target_role_name" {
  description = "Deterministic lifecycle target role name for this region."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.lifecycle_target_role_name))
    error_message = "lifecycle_target_role_name must be a valid 1-64 character IAM role name."
  }
}

variable "permissions_boundary_arn" {
  description = "Optional organization-managed boundary attached to both registry target roles instead of the module-managed boundaries."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.permissions_boundary_arn == null || can(regex(
      "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:policy/.+$",
      var.permissions_boundary_arn,
    ))
    error_message = "permissions_boundary_arn must be null or an exact IAM customer-managed policy ARN."
  }
}

variable "worker_assume_role_external_id" {
  description = "Optional 2-1224 character AWS STS external ID required by registry target roles."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.worker_assume_role_external_id == null ? true : (
      length(var.worker_assume_role_external_id) >= 2 &&
      length(var.worker_assume_role_external_id) <= 1224 &&
      can(regex("^[A-Za-z0-9_+=,.@:/-]+$", var.worker_assume_role_external_id))
    )
    error_message = "worker_assume_role_external_id must be null or 2-1224 characters from the AWS STS allowed set: letters, digits, _+=,.@:/-."
  }
}

variable "encryption_type" {
  description = "ECR encryption type."
  type        = string
  default     = "AES256"

  validation {
    condition     = contains(["AES256", "KMS"], var.encryption_type)
    error_message = "encryption_type must be AES256 or KMS."
  }
}

variable "kms_key_arn" {
  description = "Regional KMS key ARN when encryption_type is KMS."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.kms_key_arn == null || can(regex(
      "^arn:aws(-[a-z0-9]+)*:kms:[a-z0-9]+(-[a-z0-9]+)+-[0-9]+:[0-9]{12}:key/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|mrk-[0-9a-f]{32})$",
      var.kms_key_arn,
    ))
    error_message = "kms_key_arn must be an exact regional KMS key ARN using a lowercase UUID or mrk- identifier."
  }
}

variable "scan_on_push" {
  description = "Whether ECR basic scanning runs when a manifest is pushed."
  type        = bool
  default     = false
}

variable "quota_headroom" {
  description = "Image slots kept outside SkyPilot admission per repository."
  type        = number
  default     = 10000
}

variable "applied_images_per_repository_quota" {
  description = "Verified applied ECR images-per-repository quota, or null when the attester must discover it."
  type        = number
  default     = null
  nullable    = true

  validation {
    condition     = var.applied_images_per_repository_quota == null || var.applied_images_per_repository_quota >= 1
    error_message = "applied_images_per_repository_quota must be null or positive."
  }
}

variable "applied_ecr_api_rate_per_second" {
  description = "Verified conservative shared ECR API rate used by copy and lifecycle workers."
  type        = number
  default     = 10

  validation {
    condition     = var.applied_ecr_api_rate_per_second >= 1 && var.applied_ecr_api_rate_per_second <= 1000000
    error_message = "applied_ecr_api_rate_per_second must be between 1 and 1000000."
  }
}

variable "ecr_api_burst" {
  description = "Verified ECR API token burst, bounded by one worker grant batch."
  type        = number
  default     = 10

  validation {
    condition     = var.ecr_api_burst >= 1 && var.ecr_api_burst <= 64
    error_message = "ecr_api_burst must be between 1 and 64."
  }
}

variable "max_repository_policy_bytes" {
  description = "Fail closed before exceeding the provider repository-policy limit."
  type        = number
  default     = 10240
}

variable "tags" {
  description = "Additional ownership tags."
  type        = map(string)
  default     = {}
}
