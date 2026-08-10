# Cloud-agnostic Kubernetes RBAC for a SkyPilot spoke workspace pool.
#
# This is pure Kubernetes — identical for an EKS or GKE pool — so the cloud-
# specific identity wiring (e.g. an EKS access entry, or GCP IAM for a future GKE
# pool) lives in the calling EKS workspace-pool module, which passes a
# `kubernetes` provider already pointed at the target cluster plus the RBAC
# subject(s) that represent the control plane's identity there.

variable "name" {
  description = "Name for the RBAC objects (ClusterRole/Role/bindings)."
  type        = string
  default     = "skypilot-pool"

  validation {
    condition = (
      length(var.name) >= 1 &&
      length(var.name) <= 253 &&
      alltrue([
        for label in split(".", var.name) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "name must be a Kubernetes DNS-1123 subdomain of at most 253 characters."
  }
}

variable "namespace" {
  description = "Dedicated namespace SkyPilot launches workloads into. NOT a shared application namespace."
  type        = string
  default     = "skypilot-pool"

  validation {
    condition = (
      length(var.namespace) >= 1 &&
      length(var.namespace) <= 63 &&
      can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", var.namespace))
    )
    error_message = "namespace must be a Kubernetes DNS-1123 label of at most 63 characters."
  }
}

variable "manage_namespace" {
  description = "Create the namespace. Set false if it is provisioned elsewhere."
  type        = bool
  default     = true
}

variable "subjects" {
  description = <<-EOT
    RBAC subjects that represent the SkyPilot control plane's identity on this
    cluster. EKS pools pass a Group (populated by an access entry); GKE pools pass
    a User equal to the controller's GCP service-account email.
  EOT
  type = list(object({
    kind      = string
    name      = string
    api_group = optional(string, "rbac.authorization.k8s.io")
  }))

  validation {
    condition = length(var.subjects) > 0 && alltrue([
      for subject in var.subjects :
      contains(["Group", "User"], subject.kind) &&
      length(trimspace(subject.name)) >= 1 &&
      length(subject.name) <= 253 &&
      subject.api_group == "rbac.authorization.k8s.io"
    ])
    error_message = "subjects must contain at least one unique User or Group with a nonempty name and api_group rbac.authorization.k8s.io."
  }

  validation {
    condition = length(distinct([
      for subject in var.subjects :
      "${subject.kind}\u0000${subject.name}\u0000${subject.api_group}"
    ])) == length(var.subjects)
    error_message = "subjects must not contain duplicate kind/name/api_group tuples."
  }
}

variable "labels" {
  description = "Extra labels applied to the RBAC objects."
  type        = map(string)
  default     = {}
}

variable "service_account_name" {
  description = "ServiceAccount created in the pool namespace for SkyPilot pods (matches the control plane's kubernetes.pod_config serviceAccountName)."
  type        = string
  default     = "skypilot-pool-sa"

  validation {
    condition = (
      length(var.service_account_name) >= 1 &&
      length(var.service_account_name) <= 253 &&
      alltrue([
        for label in split(".", var.service_account_name) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "service_account_name must be a Kubernetes DNS-1123 subdomain of at most 253 characters."
  }
}

variable "allow_pvc_read" {
  description = <<-EOT
    Grant read (get/list) on persistentvolumeclaims in the namespace. Required when
    a tier mounts pre-existing PVCs (e.g. FSx): before creating the pod SkyPilot GETs
    each referenced claim to check its phase. Read-only — the PVCs are Terraform-
    provisioned, so SkyPilot never creates/deletes them. Leave false for tiers with
    no volumes (nothing to read).
  EOT
  type        = bool
  default     = false
}

variable "kueue" {
  description = <<-EOT
    Optional Kueue objects that the SkyPilot control-plane subjects must
    preflight before launching a Pod. When set, grant only exact-name `get` on
    the namespaced LocalQueue, cluster-scoped ClusterQueue, and this Namespace,
    plus exact `GET /apis` and `GET /apis/` for served-version discovery. The
    workload ServiceAccount receives no Kueue permission.
  EOT
  type = object({
    local_queue_name   = string
    cluster_queue_name = string
  })
  default = null

  validation {
    condition = var.kueue == null ? true : (
      length(var.kueue.local_queue_name) >= 1 &&
      length(var.kueue.local_queue_name) <= 253 &&
      alltrue([
        for label in split(".", var.kueue.local_queue_name) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "kueue.local_queue_name must be a Kubernetes DNS-1123 subdomain of at most 253 characters."
  }

  validation {
    condition = var.kueue == null ? true : (
      length(var.kueue.cluster_queue_name) >= 1 &&
      length(var.kueue.cluster_queue_name) <= 253 &&
      alltrue([
        for label in split(".", var.kueue.cluster_queue_name) :
        length(label) >= 1 &&
        length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "kueue.cluster_queue_name must be a Kubernetes DNS-1123 subdomain of at most 253 characters."
  }
}
