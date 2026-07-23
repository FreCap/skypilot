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
    condition     = length(var.canary_worker_role_arns) > 0
    error_message = "At least one canary worker role ARN is required."
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
    condition     = length(var.ec2_runtime_role_arns) <= 64 && alltrue([for arn in var.ec2_runtime_role_arns : can(regex("^arn:[^:]+:iam::[0-9]{12}:role/.+$", arn))])
    error_message = "ec2_runtime_role_arns must contain at most 64 IAM role ARNs."
  }
}

variable "ec2_instance_profile_arns" {
  description = "Exact EC2 instance profiles allowed on canary RunInstances requests and IAM inspection."
  type        = set(string)
  default     = []

  validation {
    condition     = length(var.ec2_instance_profile_arns) <= 64 && alltrue([for arn in var.ec2_instance_profile_arns : can(regex("^arn:[^:]+:iam::[0-9]{12}:instance-profile/.+$", arn))])
    error_message = "ec2_instance_profile_arns must contain at most 64 IAM instance-profile ARNs."
  }
}

variable "eks_node_instance_profile_arns" {
  description = "Exact EKS node instance profiles that the canary authority may inspect but never pass."
  type        = set(string)
  default     = []

  validation {
    condition     = length(var.eks_node_instance_profile_arns) <= 64 && alltrue([for arn in var.eks_node_instance_profile_arns : can(regex("^arn:[^:]+:iam::[0-9]{12}:instance-profile/.+$", arn))])
    error_message = "eks_node_instance_profile_arns must contain at most 64 IAM instance-profile ARNs."
  }
}

variable "ami_arns" {
  description = "Exact regional AMI ARNs allowed for EC2 canary launches."
  type        = set(string)

  default = []
}

variable "subnet_arns" {
  description = "Exact subnets allowed for EC2 canary launches."
  type        = set(string)

  default = []
}

variable "security_group_arns" {
  description = "Exact security groups allowed for EC2 canary launches."
  type        = set(string)
  default     = []
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
    condition     = var.spot_service_linked_role_arn == null || can(regex("^arn:[^:]+:iam::[0-9]{12}:role/aws-service-role/spot\\.amazonaws\\.com/AWSServiceRoleForEC2Spot$", var.spot_service_linked_role_arn))
    error_message = "spot_service_linked_role_arn must identify AWSServiceRoleForEC2Spot."
  }
}

variable "spot_customer_managed_kms_key_arns" {
  description = "Customer-managed regional KMS keys encrypting qualified Spot AMIs or snapshots. The module grants the EC2 Spot service-linked role launch access."
  type        = set(string)
  default     = []

  validation {
    condition     = length(var.spot_customer_managed_kms_key_arns) <= 64 && alltrue([for arn in var.spot_customer_managed_kms_key_arns : can(regex("^arn:[^:]+:kms:[^:]+:[0-9]{12}:key/[0-9A-Za-z-]+$", arn))])
    error_message = "spot_customer_managed_kms_key_arns must contain at most 64 KMS key ARNs."
  }
}

variable "eks_cluster_arns" {
  description = "Exact EKS clusters whose identity the canary worker may verify."
  type        = set(string)
  default     = []
}

variable "external_id" {
  description = "Optional external ID required when the worker assumes this role."
  type        = string
  default     = null
  nullable    = true
}

variable "permissions_boundary_arn" {
  description = "Optional organization-managed permissions boundary."
  type        = string
  default     = null
  nullable    = true
}

variable "tags" {
  description = "Additional tags for the role."
  type        = map(string)
  default     = {}
}
