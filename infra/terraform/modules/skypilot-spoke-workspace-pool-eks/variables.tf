variable "aws_region" {
  description = "Region of the existing EKS cluster."
  type        = string

  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.aws_region))
    error_message = "aws_region must be a valid AWS region name."
  }
}

variable "eks_cluster_name" {
  description = "Name of the existing EKS cluster to register as a SkyPilot pool."
  type        = string

  validation {
    condition = (
      length(var.eks_cluster_name) >= 1 &&
      length(var.eks_cluster_name) <= 100 &&
      can(regex("^[0-9A-Za-z][0-9A-Za-z_-]*$", var.eks_cluster_name))
    )
    error_message = "eks_cluster_name must be a valid 1-100 character EKS cluster name."
  }
}

variable "controller_role_arn" {
  description = <<-EOT
    IAM role ARN used by the SkyPilot control plane. The module maps this
    principal to every partition's RBAC group through one EKS access entry.
    Cross-account roles are supported within the active AWS partition.
  EOT
  type        = string

  validation {
    condition = (
      trimspace(var.controller_role_arn) != "" &&
      can(regex("^arn:[a-z0-9-]+:iam::[0-9]{12}:role/[0-9A-Za-z+=,.@_/-]+$", var.controller_role_arn))
    )
    error_message = "controller_role_arn must be a nonempty IAM role ARN."
  }
}

variable "partitions" {
  description = <<-EOT
    Workload partitions to register. Each item creates namespaced RBAC and can
    optionally create a Pod Identity association, an exact-priority admission
    policy, and static FSx PV/PVC pairs.

    A partition is a workload credential and storage partition, not an
    independent tenant boundary. The same controller principal receives every
    configured group. Pin each SkyPilot workspace to its intended namespace and
    audit pre-existing service-account associations and namespaced resources.

    Durable identity keys are namespace, group, priority-class name, FSx claim
    name, and the derived RBAC resource names. Change them only with a reviewed
    Terraform state and workload migration.
  EOT
  type = list(object({
    namespace                    = string
    group                        = optional(string)
    manage_namespace             = optional(bool, true)
    pod_identity_role_arn        = optional(string, "")
    pod_identity_service_account = optional(string, "skypilot-pool-sa")

    priority_class = optional(object({
      value = number
      name  = optional(string)
    }))

    fsx_volumes = optional(list(object({
      claim_name    = string
      volume_handle = string
      storage_class = string
      capacity      = string
      driver        = optional(string, "fsx.csi.aws.com")
      mountname     = optional(string)
    })), [])
  }))

  validation {
    condition     = length(var.partitions) > 0
    error_message = "partitions must contain at least one partition."
  }

  validation {
    condition = alltrue([
      for partition in var.partitions :
      can(regex(
        "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$",
        partition.namespace,
      )) && length(partition.namespace) <= 63
    ])
    error_message = "Each partition namespace must be a valid, nonempty Kubernetes DNS label of at most 63 characters."
  }

  validation {
    condition = alltrue([
      for partition in var.partitions :
      trimspace(coalesce(partition.group, partition.namespace)) != ""
    ])
    error_message = "Each partition group must be nonempty."
  }

  validation {
    condition = (
      length(var.partitions) ==
      length(distinct([for partition in var.partitions : partition.namespace]))
    )
    error_message = "Each partition must have a unique namespace."
  }

  validation {
    condition = (
      length(var.partitions) ==
      length(distinct([
        for partition in var.partitions :
        coalesce(partition.group, partition.namespace)
      ]))
    )
    error_message = "Each partition must have a unique group."
  }

  validation {
    condition = alltrue([
      for partition in var.partitions :
      can(regex(
        "^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$",
        partition.pod_identity_service_account,
      )) && length(partition.pod_identity_service_account) <= 253
    ])
    error_message = "Each pod_identity_service_account must be a valid Kubernetes DNS subdomain of at most 253 characters."
  }

  validation {
    condition = alltrue(flatten([
      for partition in var.partitions : [
        for volume in partition.fsx_volumes :
        (
          volume.driver == "fsx.csi.aws.com" ?
          trimspace(coalesce(volume.mountname, " ")) != "" :
          volume.driver == "fsx.openzfs.csi.aws.com" ?
          trimspace(coalesce(volume.mountname, " ")) == "" :
          false
        )
      ]
    ]))
    error_message = "FSx volumes must use fsx.csi.aws.com with a nonempty mountname, or fsx.openzfs.csi.aws.com without a mountname."
  }

  validation {
    condition = alltrue([
      for partition in var.partitions :
      length(partition.fsx_volumes) == length(distinct([
        for volume in partition.fsx_volumes : volume.claim_name
      ]))
    ])
    error_message = "FSx claim_name values must be unique within each partition namespace."
  }

  validation {
    condition = alltrue(flatten([
      for partition in var.partitions : [
        for volume in partition.fsx_volumes :
        trimspace(volume.claim_name) != "" &&
        trimspace(volume.volume_handle) != "" &&
        trimspace(volume.storage_class) != "" &&
        trimspace(volume.capacity) != ""
      ]
    ]))
    error_message = "Each FSx volume must have nonempty claim_name, volume_handle, storage_class, and capacity values."
  }

  validation {
    condition = alltrue([
      for partition in var.partitions :
      (
        trimspace(partition.pod_identity_role_arn) == "" ||
        can(regex(
          "^arn:[a-z0-9-]+:iam::[0-9]{12}:role/[0-9A-Za-z+=,.@_/-]+$",
          partition.pod_identity_role_arn,
        ))
      )
    ])
    error_message = "Each nonempty pod_identity_role_arn must be an IAM role ARN."
  }
}

variable "aws_profile" {
  description = <<-EOT
    Optional AWS CLI profile exposed through local.exec_env for Terragrunt
    callers that generate an aws eks get-token Kubernetes provider in the
    downloaded module directory. Ordinary Terraform callers may leave this
    null and pass their own configured providers.
  EOT
  type        = string
  default     = null
}

variable "tags" {
  description = "Tags to merge onto AWS resources."
  type        = map(string)
  default     = {}
}

variable "cluster_api_ingress_cidrs" {
  description = <<-EOT
    IPv4 CIDRs from which the SkyPilot control plane may reach the existing
    EKS cluster's private API endpoint. The module adds one TCP/443 rule to the
    EKS-managed cluster security group. The default creates no rule, and public
    /0 sources are rejected.
  EOT
  type        = list(string)
  default     = []

  validation {
    condition = (
      length(var.cluster_api_ingress_cidrs) ==
      length(distinct(var.cluster_api_ingress_cidrs)) &&
      alltrue([
        for cidr in var.cluster_api_ingress_cidrs :
        can(cidrnetmask(cidr)) &&
        try(tonumber(split("/", cidr)[1]) > 0, false)
      ])
    )
    error_message = "cluster_api_ingress_cidrs must contain unique, valid IPv4 CIDRs and must not contain a /0 source."
  }
}

variable "serve_probe_ingress" {
  description = <<-EOT
    Optional one-port ingress rule on a caller-owned node security group for a
    SkyServe control-plane prober. The default creates no rule. The source CIDR
    cannot be 0.0.0.0/0 unless allow_public_cidr is explicitly true.
  EOT
  type = object({
    node_security_group_id = string
    control_plane_cidr     = string
    port                   = number
    description            = optional(string, "SkyServe probe traffic from the control plane")
    allow_public_cidr      = optional(bool, false)
  })
  default = null

  validation {
    condition = var.serve_probe_ingress == null ? true : (
      can(cidrnetmask(var.serve_probe_ingress.control_plane_cidr)) ?
      (
        var.serve_probe_ingress.allow_public_cidr ||
        try(tonumber(split("/", var.serve_probe_ingress.control_plane_cidr)[1]) > 0, false)
      ) :
      false
    )
    error_message = "serve_probe_ingress.control_plane_cidr must be a valid IPv4 CIDR; any /0 source requires allow_public_cidr = true."
  }

  validation {
    condition = var.serve_probe_ingress == null ? true : (
      var.serve_probe_ingress.port >= 1 &&
      var.serve_probe_ingress.port <= 65535
    )
    error_message = "serve_probe_ingress.port must be between 1 and 65535."
  }

  validation {
    condition = var.serve_probe_ingress == null ? true : (
      can(regex("^sg-([0-9a-f]{8}|[0-9a-f]{17})$", var.serve_probe_ingress.node_security_group_id))
    )
    error_message = "serve_probe_ingress.node_security_group_id must be an AWS security-group ID."
  }

  validation {
    condition = var.serve_probe_ingress == null ? true : (
      trimspace(var.serve_probe_ingress.description) != "" &&
      length(var.serve_probe_ingress.description) <= 255
    )
    error_message = "serve_probe_ingress.description must be nonempty and at most 255 characters."
  }
}
