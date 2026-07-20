variable "name_prefix" {
  description = "Prefix for the three independently permissioned worker base IAM roles."
  type        = string

  validation {
    condition     = can(regex("^[A-Za-z0-9+=,.@_-]{1,48}$", var.name_prefix))
    error_message = "name_prefix must be a valid IAM role-name prefix of at most 48 characters."
  }
}

variable "oidc_provider_arn" {
  description = "IAM OIDC provider ARN for the Kubernetes control-plane cluster."
  type        = string
}

variable "oidc_issuer_url" {
  description = "OIDC issuer URL for the Kubernetes control-plane cluster."
  type        = string
}

variable "kubernetes_namespace" {
  description = "Namespace in which the SkyPilot Helm release runs."
  type        = string
  default     = "skypilot"
}

variable "copy_service_account" {
  description = "Kubernetes service account used only by the image copy worker."
  type        = string
  default     = "skypilot-image-copy-worker"
}

variable "lifecycle_service_account" {
  description = "Kubernetes service account used only by the image lifecycle worker."
  type        = string
  default     = "skypilot-image-lifecycle-worker"
}

variable "canary_service_account" {
  description = "Kubernetes service account used only by the image canary worker."
  type        = string
  default     = "skypilot-image-canary-worker"
}

variable "copy_target_role_arns" {
  description = "Exact registry-account roles that the copy worker may assume."
  type        = set(string)
  default     = []
}

variable "lifecycle_target_role_arns" {
  description = "Exact registry-account roles that the lifecycle worker may assume."
  type        = set(string)
  default     = []
}

variable "canary_target_role_arns" {
  description = "Exact compute-account roles that the canary worker may assume."
  type        = set(string)
  default     = []
}

variable "permissions_boundary_arn" {
  description = "Optional organization-managed boundary for worker base roles."
  type        = string
  default     = null
  nullable    = true
}

variable "tags" {
  description = "Additional tags for all IAM resources."
  type        = map(string)
  default     = {}
}
