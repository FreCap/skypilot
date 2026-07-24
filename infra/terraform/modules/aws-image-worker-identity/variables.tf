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

  validation {
    condition = (
      length(var.oidc_provider_arn) <= 320 &&
      can(regex(
        "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:oidc-provider/([a-z0-9]|[a-z0-9][a-z0-9.-]{0,251}[a-z0-9])(/([A-Za-z0-9._~!&'()+,;=:@-]|%[0-9A-Fa-f]{2})+)*$",
        var.oidc_provider_arn,
      ))
    )
    error_message = "oidc_provider_arn must be an exact IAM OIDC-provider ARN with a bounded lowercase DNS authority, nonempty path segments, and valid percent encoding."
  }
}

variable "oidc_issuer_url" {
  description = "OIDC issuer URL for the Kubernetes control-plane cluster."
  type        = string

  validation {
    condition = (
      length(var.oidc_issuer_url) <= 255 &&
      can(regex(
        "^https://([a-z0-9]|[a-z0-9][a-z0-9.-]{0,251}[a-z0-9])(/([A-Za-z0-9._~!&'()+,;=:@-]|%[0-9A-Fa-f]{2})+)*$",
        var.oidc_issuer_url,
      )) &&
      try(alltrue([
        for label in split(".", split("/", trimprefix(var.oidc_issuer_url, "https://"))[0]) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ]), false)
    )
    error_message = "oidc_issuer_url must be a canonical HTTPS URL of at most 255 characters with a lowercase DNS authority, no port/query/fragment/trailing slash, nonempty path segments, and valid percent encoding."
  }
}

variable "kubernetes_namespace" {
  description = "Namespace in which the SkyPilot Helm release runs."
  type        = string
  default     = "skypilot"

  validation {
    condition = (
      length(var.kubernetes_namespace) >= 1 &&
      length(var.kubernetes_namespace) <= 63 &&
      can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", var.kubernetes_namespace))
    )
    error_message = "kubernetes_namespace must be a Kubernetes DNS-1123 label of at most 63 characters."
  }
}

variable "copy_service_account" {
  description = "Kubernetes service account used only by the image copy worker."
  type        = string
  default     = "skypilot-image-copy-worker"

  validation {
    condition = (
      length(var.copy_service_account) >= 1 &&
      length(var.copy_service_account) <= 253 &&
      alltrue([
        for label in split(".", var.copy_service_account) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "copy_service_account must be a Kubernetes DNS-1123 subdomain of at most 253 characters with labels of at most 63 characters."
  }
}

variable "lifecycle_service_account" {
  description = "Kubernetes service account used only by the image lifecycle worker."
  type        = string
  default     = "skypilot-image-lifecycle-worker"

  validation {
    condition = (
      length(var.lifecycle_service_account) >= 1 &&
      length(var.lifecycle_service_account) <= 253 &&
      alltrue([
        for label in split(".", var.lifecycle_service_account) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "lifecycle_service_account must be a Kubernetes DNS-1123 subdomain of at most 253 characters with labels of at most 63 characters."
  }
}

variable "canary_service_account" {
  description = "Kubernetes service account used only by the image canary worker."
  type        = string
  default     = "skypilot-image-canary-worker"

  validation {
    condition = (
      length(var.canary_service_account) >= 1 &&
      length(var.canary_service_account) <= 253 &&
      alltrue([
        for label in split(".", var.canary_service_account) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "canary_service_account must be a Kubernetes DNS-1123 subdomain of at most 253 characters with labels of at most 63 characters."
  }
}

variable "copy_target_role_arns" {
  description = "Exact registry-account roles that the copy worker may assume."
  type        = set(string)
  default     = []

  validation {
    condition = (
      length(var.copy_target_role_arns) <= 64 &&
      length(jsonencode(sort(tolist(var.copy_target_role_arns)))) <= 8000 &&
      alltrue([
        for arn in var.copy_target_role_arns :
        can(regex(
          "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,64}$",
          arn,
        )) &&
        can(regex(
          "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$",
          arn,
        ))
      ])
    )
    error_message = "copy_target_role_arns must contain at most 64 exact IAM role ARNs, fit within the bounded inline-policy budget, and use AWS-valid paths and 1-64 character terminal names."
  }
}

variable "lifecycle_target_role_arns" {
  description = "Exact registry-account roles that the lifecycle worker may assume."
  type        = set(string)
  default     = []

  validation {
    condition = (
      length(var.lifecycle_target_role_arns) <= 64 &&
      length(jsonencode(sort(tolist(var.lifecycle_target_role_arns)))) <= 8000 &&
      alltrue([
        for arn in var.lifecycle_target_role_arns :
        can(regex(
          "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,64}$",
          arn,
        )) &&
        can(regex(
          "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$",
          arn,
        ))
      ])
    )
    error_message = "lifecycle_target_role_arns must contain at most 64 exact IAM role ARNs, fit within the bounded inline-policy budget, and use AWS-valid paths and 1-64 character terminal names."
  }
}

variable "canary_target_role_arns" {
  description = "Exact compute-account roles that the canary worker may assume."
  type        = set(string)
  default     = []

  validation {
    condition = (
      length(var.canary_target_role_arns) <= 64 &&
      length(jsonencode(sort(tolist(var.canary_target_role_arns)))) <= 8000 &&
      alltrue([
        for arn in var.canary_target_role_arns :
        can(regex(
          "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_-]+/)*[A-Za-z0-9+=,.@_-]{1,64}$",
          arn,
        )) &&
        can(regex(
          "^arn:aws(-[a-z0-9]+)*:iam::[0-9]{12}:role/([A-Za-z0-9+=,.@_/-]{1,510}/)?[A-Za-z0-9+=,.@_-]{1,64}$",
          arn,
        ))
      ])
    )
    error_message = "canary_target_role_arns must contain at most 64 exact IAM role ARNs, fit within the bounded inline-policy budget, and use AWS-valid paths and 1-64 character terminal names."
  }
}

variable "permissions_boundary_arn" {
  description = "Optional organization-managed boundary for worker base roles."
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
  description = "Additional tags for all IAM resources."
  type        = map(string)
  default     = {}
}
