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

variable "runtime_role_arns" {
  description = "Exact EC2 or EKS node roles whose instance profiles may be inspected and passed."
  type        = set(string)

  validation {
    condition     = length(var.runtime_role_arns) > 0
    error_message = "At least one qualified runtime role ARN is required."
  }
}

variable "instance_profile_arns" {
  description = "Exact instance profiles used by EC2 canaries or EKS nodes."
  type        = set(string)

  validation {
    condition     = length(var.instance_profile_arns) > 0
    error_message = "At least one qualified instance profile ARN is required."
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
