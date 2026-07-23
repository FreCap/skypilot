variable "registry_account_id" {
  type = string
}

variable "home_region" {
  type    = string
  default = "us-east-1"
}

variable "secondary_region" {
  type    = string
  default = "us-west-2"
}

variable "catalog_authority" {
  type = string
}

variable "catalog_authority_base32" {
  type = string
}

variable "realm" {
  type    = string
  default = "default"
}

variable "profile" {
  type    = string
  default = "aws-managed"
}

variable "profile_revision" {
  type = number
}

variable "profile_config_hash" {
  type = string
}

variable "physical_manifest_hash" {
  type = string
}

variable "qualification_generated_at" {
  description = "Explicit Unix timestamp used to make manifest generation reproducible."
  type        = number
}

variable "workspaces" {
  type    = set(string)
  default = ["default"]
}

variable "eks_oidc_provider_arn" {
  type = string
}

variable "eks_oidc_issuer_url" {
  type = string
}

variable "kubernetes_namespace" {
  type    = string
  default = "skypilot"
}

variable "home_runtime_pull_principal_arns" {
  type    = set(string)
  default = []
}

variable "secondary_runtime_pull_principal_arns" {
  type    = set(string)
  default = []
}

variable "canary_target_role_arns" {
  description = "Exact EC2/EKS compute-account canary roles trusted by the canary worker."
  type        = set(string)
  default     = []
}

variable "applied_images_per_repository_quota" {
  type     = number
  default  = null
  nullable = true
}

variable "tags" {
  type    = map(string)
  default = {}
}
