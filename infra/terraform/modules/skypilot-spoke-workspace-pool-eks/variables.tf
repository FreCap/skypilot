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

variable "reserved_fill_reclaim_audit" {
  description = <<-EOT
    Optional least-privilege attestation boundary for a deployment-owned
    reserved-fill reclaim policy. When configured, the module creates one
    spoke-account IAM role trusted only by the control-plane role carrying the
    exact EKS Pod Identity source tags, maps that role through a separate EKS
    access entry, and binds it to read-only Kubernetes rules for the reviewed
    partition, Kueue topology, scheduler, provider-owned flavors, and Nodes.

    partition_namespace must name one configured partition with a priority
    contract. local_queue_name and inference_cluster_queue_name are the
    explicit read-only audit targets, so this identity can be staged before
    that partition enables Kueue. Once partition.kueue is configured, its two
    queue names must exactly equal these audit targets. There is no derived or
    compatibility fallback between the contracts. The module still derives
    the Namespace, ServiceAccount, and Pod PriorityClass from the partition.
    External operator-owned ClusterQueues and other objects are listed
    explicitly and remain exact-name reads. The IAM
    role and Kubernetes group are deterministically unique to the target
    cluster and partition. The caller must separately grant AssumeRole and
    TagSession on the returned role ARN. Node inventory is necessarily a
    cluster-wide list because Kubernetes RBAC cannot restrict list by labels.
  EOT
  type = object({
    partition_namespace          = string
    local_queue_name             = string
    inference_cluster_queue_name = string

    source_identity = object({
      eks_cluster_arn = string
      namespace       = string
      service_account = string
    })

    external_cluster_queue_names   = set(string)
    workload_priority_class_names  = set(string)
    resource_flavor_names          = set(string)
    scheduler_namespace            = string
    scheduler_deployment_name      = string
    kueue_namespace                = string
    kueue_deployment_name          = string
    kueue_config_map_name          = string
    admission_policy_names         = set(string)
    admission_policy_binding_names = set(string)
    validating_webhook_names       = set(string)
    mutating_webhook_names         = set(string)
  })
  default = null

  validation {
    condition = var.reserved_fill_reclaim_audit == null ? true : (
      length(var.reserved_fill_reclaim_audit.local_queue_name) >= 1 &&
      length(var.reserved_fill_reclaim_audit.local_queue_name) <= 253 &&
      alltrue([
        for label in split(".", var.reserved_fill_reclaim_audit.local_queue_name) :
        length(label) >= 1 && length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ]) &&
      length(var.reserved_fill_reclaim_audit.inference_cluster_queue_name) >= 1 &&
      length(var.reserved_fill_reclaim_audit.inference_cluster_queue_name) <= 63 &&
      can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", var.reserved_fill_reclaim_audit.inference_cluster_queue_name))
    )
    error_message = "reserved_fill_reclaim_audit.local_queue_name must be a Kubernetes DNS-1123 subdomain and inference_cluster_queue_name must be a DNS-1123 label of at most 63 characters."
  }

  validation {
    condition = var.reserved_fill_reclaim_audit == null ? true : (
      can(regex("^arn:[a-z0-9-]+:eks:[a-z0-9-]+:[0-9]{12}:cluster/[0-9A-Za-z][0-9A-Za-z_-]*$", var.reserved_fill_reclaim_audit.source_identity.eks_cluster_arn)) &&
      can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", var.reserved_fill_reclaim_audit.source_identity.namespace)) &&
      length(var.reserved_fill_reclaim_audit.source_identity.namespace) <= 63 &&
      can(regex("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$", var.reserved_fill_reclaim_audit.source_identity.service_account)) &&
      length(var.reserved_fill_reclaim_audit.source_identity.service_account) <= 253 &&
      alltrue([
        for label in split(".", var.reserved_fill_reclaim_audit.source_identity.service_account) :
        length(label) >= 1 && length(label) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
      ])
    )
    error_message = "reserved_fill_reclaim_audit source_identity must contain an exact EKS cluster ARN and valid namespace/service-account names."
  }

  validation {
    condition = var.reserved_fill_reclaim_audit == null ? true : alltrue([
      length(var.reserved_fill_reclaim_audit.external_cluster_queue_names) > 0,
      length(var.reserved_fill_reclaim_audit.workload_priority_class_names) > 0,
      length(var.reserved_fill_reclaim_audit.resource_flavor_names) > 0,
      length(var.reserved_fill_reclaim_audit.admission_policy_names) > 0,
      length(var.reserved_fill_reclaim_audit.admission_policy_binding_names) > 0,
      length(var.reserved_fill_reclaim_audit.validating_webhook_names) > 0,
      length(var.reserved_fill_reclaim_audit.mutating_webhook_names) > 0,
      alltrue([
        for value in concat(
          [
            var.reserved_fill_reclaim_audit.partition_namespace,
            var.reserved_fill_reclaim_audit.scheduler_namespace,
            var.reserved_fill_reclaim_audit.scheduler_deployment_name,
            var.reserved_fill_reclaim_audit.kueue_namespace,
            var.reserved_fill_reclaim_audit.kueue_deployment_name,
            var.reserved_fill_reclaim_audit.kueue_config_map_name,
          ],
          tolist(var.reserved_fill_reclaim_audit.external_cluster_queue_names),
          tolist(var.reserved_fill_reclaim_audit.workload_priority_class_names),
          tolist(var.reserved_fill_reclaim_audit.resource_flavor_names),
          tolist(var.reserved_fill_reclaim_audit.admission_policy_names),
          tolist(var.reserved_fill_reclaim_audit.admission_policy_binding_names),
          tolist(var.reserved_fill_reclaim_audit.validating_webhook_names),
          tolist(var.reserved_fill_reclaim_audit.mutating_webhook_names),
          ) : (
          value == trimspace(value) &&
          length(value) <= 253 &&
          can(regex("^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$", value)) &&
          alltrue([
            for label in split(".", value) :
            length(label) >= 1 && length(label) <= 63 &&
            can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
          ])
        )
      ]),
    ])
    error_message = "reserved_fill_reclaim_audit exact object inventories must be nonempty canonical Kubernetes DNS names."
  }
}

variable "partitions" {
  description = <<-EOT
    Workload partitions to register. Each item creates namespaced RBAC and can
    optionally create a Pod Identity association, an exact-priority admission
    policy, a Kueue LocalQueue, and static FSx PV/PVC pairs.

    A partition is a workload credential and storage partition, not an
    independent tenant boundary. The same controller principal receives every
    configured group. Pin each SkyPilot workspace to its intended namespace and
    audit pre-existing service-account associations and namespaced resources.

    Durable identity keys are namespace, group, priority-class name, Kueue
    LocalQueue and ClusterQueue names, FSx claim name, and the derived RBAC
    resource names. Change them only with a reviewed Terraform state and
    workload migration.

    Each Kueue ClusterQueue name must be one DNS-1123 label of at most 63
    characters. Strict SkyPilot admission requires Kueue's
    AssignQueueLabelsForPods feature to publish that name on admitted Pods;
    dotted DNS subdomains and other non-label names cannot be published.
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

    kueue = optional(object({
      local_queue_name   = optional(string, "default")
      cluster_queue_name = string
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
    condition = alltrue([
      for partition in var.partitions :
      partition.kueue == null ? true : (
        length(partition.kueue.local_queue_name) >= 1 &&
        length(partition.kueue.local_queue_name) <= 253 &&
        alltrue([
          for label in split(".", partition.kueue.local_queue_name) :
          length(label) >= 1 &&
          length(label) <= 63 &&
          can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", label))
        ])
      )
    ])
    error_message = "Each kueue.local_queue_name must be a Kubernetes DNS-1123 subdomain of at most 253 characters."
  }

  validation {
    condition = alltrue([
      for partition in var.partitions :
      partition.kueue == null ? true : (
        length(partition.kueue.cluster_queue_name) >= 1 &&
        length(partition.kueue.cluster_queue_name) <= 63 &&
        can(regex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?$", partition.kueue.cluster_queue_name))
      )
    ])
    error_message = "Each kueue.cluster_queue_name must be a Kubernetes DNS-1123 label of at most 63 characters so Kueue AssignQueueLabelsForPods can publish it."
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

variable "allow_cluster_security_group_node_ingress" {
  description = <<-EOT
    Explicitly allow cluster_api_ingress_cidrs to add TCP/443 ingress to the
    EKS-managed cluster security group. EKS also attaches that security group
    to managed-node interfaces by default, so this opt-in acknowledges that
    the same source CIDRs can reach TCP/443 listeners on those nodes. The
    default fails closed whenever cluster_api_ingress_cidrs is nonempty.
  EOT
  type        = bool
  default     = false
}

variable "cluster_api_ingress_cidrs" {
  description = <<-EOT
    IPv4 CIDRs from which the SkyPilot control plane may reach the existing
    EKS cluster's private API endpoint. The module adds one TCP/443 rule to the
    EKS-managed cluster security group. EKS attaches that group to managed-node
    interfaces by default, so allow_cluster_security_group_node_ingress must be
    true before the rule can plan. The default creates no rule. Public /0
    sources and CIDRs that become duplicates after AWS canonicalization are
    rejected.
  EOT
  type        = list(string)
  default     = []

  validation {
    condition = (
      length(var.cluster_api_ingress_cidrs) ==
      length(distinct([
        for cidr in var.cluster_api_ingress_cidrs :
        try(cidrsubnet(cidr, 0, 0), cidr)
      ])) &&
      alltrue([
        for cidr in var.cluster_api_ingress_cidrs :
        can(cidrnetmask(cidr)) &&
        try(tonumber(split("/", cidr)[1]) > 0, false)
      ])
    )
    error_message = "cluster_api_ingress_cidrs must contain canonically unique, valid IPv4 CIDRs and must not contain a /0 source."
  }
}

variable "serve_probe_ingress" {
  description = <<-EOT
    Optional ingress rules on a caller-owned node security group for the
    SkyServe control plane. The default creates no rule. The source CIDR
    cannot be 0.0.0.0/0 unless allow_public_cidr is explicitly true.

    `port` is the replica serving port the prober and load balancer reach.

    `additional_ports` covers the rest of the control plane's Pod-IP traffic.
    Provisioning a Kubernetes replica SSHes to its Pod IP, so a spoke whose
    security group grants only the serving port accepts the probe but can
    never finish a launch: the Pod runs, the container reports ready, and the
    replica stays PROVISIONING until it is culled. Measured on a spoke built
    from this module alone -- TCP/8080 answered while TCP/22 was dropped, and
    every replica stalled. The sibling spoke worked only because an unrelated
    legacy rule ("SSH access from admin subnets") happened to cover the same
    control-plane CIDR, so the dependency was never represented here.

    `additional_port_descriptions` pins the AWS description of one additional
    port, keyed by port number as a string. Its only purpose is adopting a
    pre-existing rule: AWS treats (protocol, ports, CIDR) as the rule identity
    and rejects a duplicate, so a spoke that already carries the grant must
    import it rather than create it, and the import only plans clean if the
    description matches byte for byte. Leave it unset for new rules.
  EOT
  type = object({
    node_security_group_id       = string
    control_plane_cidr           = string
    port                         = number
    additional_ports             = optional(set(number), [])
    additional_port_descriptions = optional(map(string), {})
    description                  = optional(string, "SkyServe probe traffic from the control plane")
    allow_public_cidr            = optional(bool, false)
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
    condition = var.serve_probe_ingress == null ? true : alltrue([
      for p in var.serve_probe_ingress.additional_ports : p >= 1 && p <= 65535
    ])
    error_message = "serve_probe_ingress.additional_ports must be between 1 and 65535."
  }

  validation {
    # A description for a port that is not granted is silently dead config,
    # and the likeliest form of that typo is adopting the wrong port.
    condition = var.serve_probe_ingress == null ? true : alltrue([
      for k in keys(var.serve_probe_ingress.additional_port_descriptions) :
      contains([for p in var.serve_probe_ingress.additional_ports : tostring(p)], k)
    ])
    error_message = "Every serve_probe_ingress.additional_port_descriptions key must be a granted additional port."
  }

  validation {
    # A duplicate would make two rules claim the same port/CIDR pair on a
    # caller-owned group; AWS rejects the second and the apply half-fails.
    condition = var.serve_probe_ingress == null ? true : !contains(
      var.serve_probe_ingress.additional_ports, var.serve_probe_ingress.port
    )
    error_message = "serve_probe_ingress.additional_ports must not repeat serve_probe_ingress.port."
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
