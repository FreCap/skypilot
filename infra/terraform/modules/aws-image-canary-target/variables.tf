variable "role_name" {
  description = "Name of the compute-account role assumed only by the image canary worker."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,64}$", var.role_name))
    error_message = "role_name must be a valid IAM role name."
  }
}

variable "canary_worker_role_arns" {
  description = "Exact control-plane worker base roles trusted to assume this role."
  type        = set(string)

  validation {
    condition = (
      length(var.canary_worker_role_arns) > 0 &&
      length(var.canary_worker_role_arns) <= 64 &&
      alltrue([
        for arn in var.canary_worker_role_arns :
        can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$", arn))
      ])
    )
    error_message = "canary_worker_role_arns must contain 1-64 exact IAM role ARNs with AWS-valid paths and 1-64 character terminal names."
  }
}

variable "applied_role_trust_policy_quota" {
  description = "Applied IAM role trust-policy character quota in the target account."
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

variable "catalog_authority" {
  description = "Exact SkyPilot catalog UUID required on every temporary resource tag."
  type        = string
}

variable "ec2_runtime_role_arns" {
  description = "Exact EC2 runtime roles that the canary authority may pass to RunInstances. Never include EKS node roles."
  type        = set(string)
  default     = []

  validation {
    condition = length(var.ec2_runtime_role_arns) <= 64 && alltrue([
      for arn in var.ec2_runtime_role_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$", arn))
    ])
    error_message = "ec2_runtime_role_arns must contain at most 64 exact IAM role ARNs with AWS-valid paths and 1-64 character terminal names."
  }
}

variable "ec2_instance_profile_arns" {
  description = "Exact EC2 instance profiles allowed on canary RunInstances requests and IAM inspection."
  type        = set(string)
  default     = []

  validation {
    condition = length(var.ec2_instance_profile_arns) <= 64 && alltrue([
      for arn in var.ec2_instance_profile_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:instance-profile/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,128}$", arn))
    ])
    error_message = "ec2_instance_profile_arns must contain at most 64 exact IAM instance-profile ARNs with AWS-valid paths and 1-128 character terminal names."
  }
}

variable "eks_node_instance_profile_arns" {
  description = "Exact EKS node instance profiles that the canary authority may inspect but never pass."
  type        = set(string)
  default     = []

  validation {
    condition = length(var.eks_node_instance_profile_arns) <= 64 && alltrue([
      for arn in var.eks_node_instance_profile_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:instance-profile/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,128}$", arn))
    ])
    error_message = "eks_node_instance_profile_arns must contain at most 64 exact IAM instance-profile ARNs with AWS-valid paths and 1-128 character terminal names."
  }
}

variable "ami_arns" {
  description = "Exact accountless EC2 image authorization ARNs allowed for canary launches."
  type        = set(string)

  default = []

  validation {
    condition = length(var.ami_arns) <= 64 && alltrue([
      for arn in var.ami_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:ec2:[a-z0-9]+(-[a-z0-9]+)+-[0-9]+::image/ami-([0-9a-f]{8}|[0-9a-f]{17})$", arn))
    ])
    error_message = "ami_arns must contain at most 64 exact accountless regional AMI authorization ARNs with 8- or 17-character lowercase hexadecimal IDs."
  }
}

variable "subnet_arns" {
  description = "Exact subnets allowed for EC2 canary launches."
  type        = set(string)

  default = []

  validation {
    condition = length(var.subnet_arns) <= 64 && alltrue([
      for arn in var.subnet_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:ec2:[a-z0-9]+(-[a-z0-9]+)+-[0-9]+:[0-9]{12}:subnet/subnet-([0-9a-f]{8}|[0-9a-f]{17})$", arn))
    ])
    error_message = "subnet_arns must contain at most 64 exact regional subnet ARNs with 8- or 17-character lowercase hexadecimal IDs."
  }
}

variable "security_group_arns" {
  description = "Exact security groups allowed for EC2 canary launches."
  type        = set(string)
  default     = []

  validation {
    condition = length(var.security_group_arns) <= 64 && alltrue([
      for arn in var.security_group_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:ec2:[a-z0-9]+(-[a-z0-9]+)+-[0-9]+:[0-9]{12}:security-group/sg-([0-9a-f]{8}|[0-9a-f]{17})$", arn))
    ])
    error_message = "security_group_arns must contain at most 64 exact regional security-group ARNs with 8- or 17-character lowercase hexadecimal IDs."
  }
}

variable "canary_instance_types" {
  description = "Exact EC2 instance types allowed for image pull canaries."
  type        = set(string)
  default     = []

  validation {
    condition     = length(var.canary_instance_types) <= 32 && alltrue([for item in var.canary_instance_types : can(regex("^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$", item))])
    error_message = "canary_instance_types must contain at most 32 valid EC2 instance type names."
  }
}

variable "spot_service_linked_role_arn" {
  description = "AWSServiceRoleForEC2Spot ARN from the account bootstrap module. Required for every EC2 target and unused by EKS-only targets."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.spot_service_linked_role_arn == null || can(regex("^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/aws-service-role/spot\\.amazonaws\\.com/AWSServiceRoleForEC2Spot$", var.spot_service_linked_role_arn))
    error_message = "spot_service_linked_role_arn must identify AWSServiceRoleForEC2Spot."
  }
}

variable "spot_customer_managed_kms_key_arns" {
  description = "Customer-managed regional KMS keys encrypting qualified Spot AMIs or snapshots. Keys may be owned by another account in the same partition and region. The module grants the EC2 Spot service-linked role launch access."
  type        = set(string)
  default     = []

  validation {
    condition     = length(var.spot_customer_managed_kms_key_arns) <= 64 && alltrue([for arn in var.spot_customer_managed_kms_key_arns : can(regex("^arn:aws(-[a-z0-9]+)*:kms:[a-z0-9]+(-[a-z0-9]+)+-[0-9]+:[0-9]{12}:key/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|mrk-[0-9a-f]{32})$", arn))])
    error_message = "spot_customer_managed_kms_key_arns must contain at most 64 exact regional KMS key ARNs using a lowercase UUID or mrk- identifier."
  }
}

variable "eks_cluster_arns" {
  description = "Exact EKS clusters whose identity the canary worker may verify."
  type        = set(string)
  default     = []

  validation {
    condition = length(var.eks_cluster_arns) <= 64 && alltrue([
      for arn in var.eks_cluster_arns :
      can(regex("^arn:aws(-[a-z0-9]+)*:eks:[a-z0-9]+(-[a-z0-9]+)+-[0-9]+:[0-9]{12}:cluster/[A-Za-z0-9][A-Za-z0-9_-]{0,99}$", arn))
    ])
    error_message = "eks_cluster_arns must contain at most 64 exact regional EKS cluster ARNs with AWS-valid 1-100 character names."
  }
}

variable "external_id" {
  description = "Optional 2-1224 character AWS STS external ID required when the worker assumes this role."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.external_id == null ? true : (
      length(var.external_id) >= 2 &&
      length(var.external_id) <= 1224 &&
      can(regex("^[A-Za-z0-9_+=,.@:/-]+$", var.external_id))
    )
    error_message = "external_id must be null or 2-1224 characters from the AWS STS allowed set: letters, digits, _+=,.@:/-."
  }
}

variable "permissions_boundary_arn" {
  description = "Optional organization-managed permissions boundary."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.permissions_boundary_arn == null || (
      can(regex(
        "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:policy/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,128}$",
        var.permissions_boundary_arn,
      )) &&
      can(regex(
        "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:policy/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,128}$",
        var.permissions_boundary_arn,
      ))
    )
    error_message = "permissions_boundary_arn must be an exact IAM managed-policy ARN with an AWS-valid path and 1-128 character terminal name."
  }
}

variable "tags" {
  description = "Additional tags for the role."
  type        = map(string)
  default     = {}
}
