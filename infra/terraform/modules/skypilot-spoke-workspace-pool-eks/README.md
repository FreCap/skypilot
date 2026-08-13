# SkyPilot EKS spoke workspace pool

Connect workspace workloads to an existing spoke Amazon EKS cluster. The module
maps one SkyPilot control-plane IAM role to namespaced Kubernetes RBAC and can
optionally create Pod Identity associations, exact-priority admission policies,
fail-closed Kueue LocalQueue contracts, static FSx volumes, private EKS API
ingress, and a one-port SkyServe probe ingress rule. It does not provision the
cluster. A pool may serve multiple workspaces, and a workspace may target more
than one pool. “Spoke” is logical and does not require a separate cluster. This
infrastructure package is unrelated to a managed Job Pool operated through
`sky jobs pool`.

This is a provider-neutral child module. It declares provider requirements but
does not configure credentials or cluster authentication.

## Prerequisites

- Terraform or OpenTofu 1.5 or newer.
- An existing EKS cluster whose authentication mode supports EKS access
  entries.
- AWS credentials that can read the cluster and manage its access entries, Pod
  Identity associations, and any requested security-group rule.
- A configured Kubernetes provider authorized to manage cluster-scoped RBAC,
  namespaces, PriorityClasses, admission policies, PVs, and PVCs.
- The EKS Pod Identity agent when a partition supplies
  `pod_identity_role_arn`.
- A compatible CSI driver and StorageClass for each FSx volume.
- Kubernetes support for
  `admissionregistration.k8s.io/v1` ValidatingAdmissionPolicy when
  `priority_class` or `kueue` is enabled.
- An installed Kueue API serving `kueue.x-k8s.io/v1beta2`, plus an
  operator-owned ClusterQueue, before `kueue` is enabled.
- Network routing from the control plane to workload pod IPs before enabling
  `serve_probe_ingress`.
- Private network routing from each source CIDR to the EKS endpoint before
  setting `cluster_api_ingress_cidrs`.

Service availability can differ across AWS commercial, GovCloud, and China
partitions. Partition-correct ARN and DNS construction does not imply that EKS,
Pod Identity, or a selected FSx driver is available in every region.

## Usage

Pin cross-repository consumers to an immutable commit:

```hcl
module "spoke_workspace_pool" {
  source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-spoke-workspace-pool-eks?ref=<full-commit-sha>"

  providers = {
    aws        = aws
    kubernetes = kubernetes.pool
  }

  aws_region          = "us-east-2"
  eks_cluster_name    = "gpu-pool"
  controller_role_arn = "arn:aws:iam::123456789012:role/skypilot-api"

  cluster_api_ingress_cidrs = ["10.20.0.0/16"]

  partitions = [
    {
      namespace                    = "skypilot-training"
      group                        = "skypilot-training"
      pod_identity_service_account = "skypilot-pool-sa"
      pod_identity_role_arn        = "arn:aws:iam::210987654321:role/skypilot-training"

      fsx_volumes = [{
        claim_name    = "training-data"
        volume_handle = "fs-0123456789abcdef0"
        storage_class = "fsx-lustre"
        capacity      = "1200Gi"
        mountname     = "abcd1234"
      }]
    },
    {
      namespace = "skypilot-inference"
      group     = "skypilot-inference"

      priority_class = {
        value = -1000
      }

      kueue = {
        cluster_queue_name = "inference-borrower"
      }
    },
  ]
}
```

The Git `//subdirectory` syntax is required: the EKS module depends on the
sibling `../skypilot-spoke-workspace-pool-rbac` module, so Terraform must
download the repository package rather than only one directory.

Configure the Kubernetes provider in the root module. For example:

```hcl
data "aws_eks_cluster" "pool" {
  name = "gpu-pool"
}

provider "kubernetes" {
  alias                  = "pool"
  host                   = data.aws_eks_cluster.pool.endpoint
  cluster_ca_certificate = base64decode(
    data.aws_eks_cluster.pool.certificate_authority[0].data
  )

  exec {
    api_version = "client.authentication.k8s.io/v1beta1"
    command     = "aws"
    args        = ["eks", "get-token", "--cluster-name", "gpu-pool", "--region", "us-east-2"]
  }
}
```

## Partition behavior

Every partition creates an instance of `module.rbac` keyed by its namespace.
`manage_namespace` defaults to `true`; set it to `false` only when the namespace
already exists and is managed elsewhere. A nonempty Pod Identity role creates
an association for the configured service account. An empty role creates no
association.

These partitions separate module-created credentials and storage; they are not
independent controller or tenant trust boundaries:

- one controller principal is mapped to every configured group;
- another pre-existing service-account association can still provide AWS
  credentials;
- this module does not force a SkyPilot workspace to choose a particular
  namespace; and
- other namespaced resources can remain accessible to workloads.

Audit existing associations and resources, and pin each workspace to its
intended namespace.

`priority_class` creates a PriorityClass plus a validating admission policy and
binding. The policy does not inject a class. It denies a pod unless the pod
explicitly supplies the exact configured class.

`kueue` is one fail-closed partition contract, not a feature toggle.
`local_queue_name` defaults to the literal `default`. On Kueue 0.18 this lets
the plain-Pod mutating webhook automatically add the queue label to otherwise
unlabelled Pods before the validating policy runs. The policy requires both the
exact queue label and the `managed=true` value normally added by Kueue, so an
ordinary client that omitted Kueue metadata is either auto-fixed or denied if
the webhook/defaulting path is unavailable. Kubernetes labels are writable and
are not proof against a namespace writer deliberately forging both values;
strict SkyPilot removes a supplied managed label, adds the scheduling gate, and
attests the create response. An explicit non-default name retains fail-closed
validation but intentionally gives up automatic queue-label defaulting.

`cluster_queue_name` must be one DNS-1123 label of at most 63 characters, even
though the ClusterQueue API itself accepts a DNS subdomain. Strict SkyPilot
admission requires Kueue 0.18's `AssignQueueLabelsForPods` feature to publish
the exact ClusterQueue name on each admitted Pod. Kueue cannot publish dotted,
overlength, uppercase, underscore-containing, or otherwise non-label names, so
the module rejects them before creating the LocalQueue.

When set, the module:

- reads the explicitly named, operator-owned ClusterQueue;
- proves that its namespace selector is either empty or exactly
  `kubernetes.io/metadata.name=<partition namespace>`;
- creates the namespaced LocalQueue and waits for `Active=True`;
- grants the SkyPilot control-plane subjects only exact-name `get` on that
  LocalQueue, its ClusterQueue, and the partition Namespace, plus exact
  `GET /apis` and `GET /apis/` for served-version discovery; and
- creates a fail-closed ValidatingAdmissionPolicy binding that rejects Pod
  creates and updates unless Kueue admitted them through that exact queue.

Kueue 0.18 can report a LocalQueue active even when the ClusterQueue selector
rejects its namespace, because the scheduler evaluates that selector later.
The explicit selector proof is therefore part of readiness. More complex
selectors are rejected conservatively, even if they happen to match external
labels. The module never creates or mutates ClusterQueue quota, cohort,
ResourceFlavor, preemption, or topology policy and has no TAS-specific branch.
Omitting `kueue` performs no Kueue API read and creates no queue, admission rule,
or additional RBAC.

The server-owned SkyPilot workspace must separately use the same
`local_queue_name`; this module cannot safely wire a control-plane stack back to
its spoke output without creating a dependency cycle. Use a SkyPilot release
containing the expanded LocalQueue-to-ClusterQueue drift preflight from this
change (1.1.1204 is only the minimum release for the original strict
LocalQueue check). Forgetting the SkyPilot queue setting is still safe for the
literal `default`: Kueue's defaulting webhook adds it and the admission policy
fails closed if that mutation does not happen, although explicit SkyPilot
configuration remains required for multi-Pod gang metadata and synchronous
attestation. No service YAML resource field or WorkloadPriorityClass is
required when the Pod already uses the module-enforced Kubernetes
PriorityClass: Kueue derives workload priority from that class when no Kueue
priority-class label is supplied.

Before adding `kueue` to an existing partition, stop new unmanaged placement
and drain its unlabeled Pods. The admission binding covers updates as well as
creates, so enabling it while legacy Pods still require mutation is an
intentional apply-time safety failure, not a migration mechanism. An existing
LocalQueue must be imported rather than silently taken over.
An imported LocalQueue must already reference the intended ClusterQueue; do not
retarget it in place. Drain and replace it so a stale pre-update `Active=True`
condition cannot make an infrastructure apply appear reconciled. SkyPilot also
checks the condition generation at every launch and fails closed during drift.

Each FSx entry creates a static `Retain` PV and a namespaced PVC. Lustre uses
`fsx.csi.aws.com` and requires `mountname`; OpenZFS uses
`fsx.openzfs.csi.aws.com` and rejects `mountname`. The endpoint is derived as
`<filesystem-id>.fsx.<region>.<AWS partition DNS suffix>`.

`cluster_api_ingress_cidrs` adds TCP/443 ingress to the EKS-managed cluster
security group used by private endpoint interfaces. It rejects public `/0`
sources. Configure routing and private DNS separately; this rule alone does not
make a private endpoint reachable.

`serve_probe_ingress` mutates a caller-owned security group. It grants TCP
ports from one IPv4 CIDR and rejects `0.0.0.0/0` unless
`allow_public_cidr = true` is explicitly set. Review that ownership edge before
enabling the rule.

`port` is the replica serving port the prober and load balancer reach.
`additional_ports` covers the rest of the control plane's Pod-IP traffic.
Launching a Kubernetes replica SSHes to its Pod IP, so a spoke that grants only
the serving port will accept the probe and still never finish a launch: the Pod
runs, the container reports ready, and the replica stays PROVISIONING until it
is culled. Grant `22` unless the control plane reaches Pods another way.
Omitting `additional_ports` creates no extra rule, so existing spokes are
unchanged.

## State and upgrades

Namespace, group, priority-class name, LocalQueue and ClusterQueue names, claim
name, role ARN, and the derived RBAC names are durable identity inputs. Changing
one can replace or orphan infrastructure and should be handled as an explicit
state and workload migration.

The module preserves the resource labels, `module.rbac` label, `count` and
`for_each` shapes, and namespace/claim identity keys from the original
deployment module. Moving an existing caller from the legacy local source to
this Git source therefore requires no Terraform `moved` blocks when inputs and
provider addresses stay unchanged. Stop if a migration plan proposes a
replacement, deletion, namespace recreation, or PV recreation.

## Module reference

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
| ---- | ------- |
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.5.0 |
| <a name="requirement_aws"></a> [aws](#requirement\_aws) | >= 6.24.0 |
| <a name="requirement_kubernetes"></a> [kubernetes](#requirement\_kubernetes) | >= 2.20 |

## Providers

| Name | Version |
| ---- | ------- |
| <a name="provider_aws"></a> [aws](#provider\_aws) | 6.58.0 |
| <a name="provider_kubernetes"></a> [kubernetes](#provider\_kubernetes) | 3.2.1 |

## Modules

| Name | Source | Version |
| ---- | ------ | ------- |
| <a name="module_rbac"></a> [rbac](#module\_rbac) | ../skypilot-spoke-workspace-pool-rbac | n/a |

## Resources

| Name | Type |
| ---- | ---- |
| [aws_eks_access_entry.pool](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/eks_access_entry) | resource |
| [aws_eks_pod_identity_association.pool_sa](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/eks_pod_identity_association) | resource |
| [aws_security_group_rule.cluster_api_from_control_plane](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.serve_control_plane_additional](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [aws_security_group_rule.serve_probe_from_control_plane](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/security_group_rule) | resource |
| [kubernetes_manifest.partition_kueue_binding](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_manifest.partition_kueue_policy](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_manifest.partition_local_queue](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_manifest.partition_priority_binding](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_manifest.partition_priority_policy](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/manifest) | resource |
| [kubernetes_persistent_volume_claim_v1.fsx](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/persistent_volume_claim_v1) | resource |
| [kubernetes_persistent_volume_v1.fsx](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/persistent_volume_v1) | resource |
| [kubernetes_priority_class_v1.partition](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/priority_class_v1) | resource |
| [aws_eks_cluster.target](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/eks_cluster) | data source |
| [aws_partition.current](https://registry.terraform.io/providers/hashicorp/aws/latest/docs/data-sources/partition) | data source |
| [kubernetes_resource.partition_cluster_queue](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/data-sources/resource) | data source |

## Inputs

| Name | Description | Type | Default | Required |
| ---- | ----------- | ---- | ------- | :------: |
| <a name="input_aws_profile"></a> [aws\_profile](#input\_aws\_profile) | Optional AWS CLI profile exposed through local.exec\_env for Terragrunt<br/>callers that generate an aws eks get-token Kubernetes provider in the<br/>downloaded module directory. Ordinary Terraform callers may leave this<br/>null and pass their own configured providers. | `string` | `null` | no |
| <a name="input_aws_region"></a> [aws\_region](#input\_aws\_region) | Region of the existing EKS cluster. | `string` | n/a | yes |
| <a name="input_cluster_api_ingress_cidrs"></a> [cluster\_api\_ingress\_cidrs](#input\_cluster\_api\_ingress\_cidrs) | IPv4 CIDRs from which the SkyPilot control plane may reach the existing<br/>EKS cluster's private API endpoint. The module adds one TCP/443 rule to the<br/>EKS-managed cluster security group. The default creates no rule, and public<br/>/0 sources are rejected. | `list(string)` | `[]` | no |
| <a name="input_controller_role_arn"></a> [controller\_role\_arn](#input\_controller\_role\_arn) | IAM role ARN used by the SkyPilot control plane. The module maps this<br/>principal to every partition's RBAC group through one EKS access entry.<br/>Cross-account roles are supported within the active AWS partition. | `string` | n/a | yes |
| <a name="input_eks_cluster_name"></a> [eks\_cluster\_name](#input\_eks\_cluster\_name) | Name of the existing EKS cluster to register as a SkyPilot pool. | `string` | n/a | yes |
| <a name="input_partitions"></a> [partitions](#input\_partitions) | Workload partitions to register. Each item creates namespaced RBAC and can<br/>optionally create a Pod Identity association, an exact-priority admission<br/>policy, a Kueue LocalQueue, and static FSx PV/PVC pairs.<br/><br/>A partition is a workload credential and storage partition, not an<br/>independent tenant boundary. The same controller principal receives every<br/>configured group. Pin each SkyPilot workspace to its intended namespace and<br/>audit pre-existing service-account associations and namespaced resources.<br/><br/>Durable identity keys are namespace, group, priority-class name, Kueue<br/>LocalQueue and ClusterQueue names, FSx claim name, and the derived RBAC<br/>resource names. Change them only with a reviewed Terraform state and<br/>workload migration.<br/><br/>Each Kueue ClusterQueue name must be one DNS-1123 label of at most 63<br/>characters. Strict SkyPilot admission requires Kueue's<br/>AssignQueueLabelsForPods feature to publish that name on admitted Pods;<br/>dotted DNS subdomains and other non-label names cannot be published. | <pre>list(object({<br/>    namespace                    = string<br/>    group                        = optional(string)<br/>    manage_namespace             = optional(bool, true)<br/>    pod_identity_role_arn        = optional(string, "")<br/>    pod_identity_service_account = optional(string, "skypilot-pool-sa")<br/><br/>    priority_class = optional(object({<br/>      value = number<br/>      name  = optional(string)<br/>    }))<br/><br/>    kueue = optional(object({<br/>      local_queue_name   = optional(string, "default")<br/>      cluster_queue_name = string<br/>    }))<br/><br/>    fsx_volumes = optional(list(object({<br/>      claim_name    = string<br/>      volume_handle = string<br/>      storage_class = string<br/>      capacity      = string<br/>      driver        = optional(string, "fsx.csi.aws.com")<br/>      mountname     = optional(string)<br/>    })), [])<br/>  }))</pre> | n/a | yes |
| <a name="input_serve_probe_ingress"></a> [serve\_probe\_ingress](#input\_serve\_probe\_ingress) | Optional ingress rules on a caller-owned node security group for the<br/>SkyServe control plane. The default creates no rule. The source CIDR<br/>cannot be 0.0.0.0/0 unless allow\_public\_cidr is explicitly true.<br/><br/>`port` is the replica serving port the prober and load balancer reach.<br/><br/>`additional_ports` covers the rest of the control plane's Pod-IP traffic.<br/>Provisioning a Kubernetes replica SSHes to its Pod IP, so a spoke whose<br/>security group grants only the serving port accepts the probe but can<br/>never finish a launch: the Pod runs, the container reports ready, and the<br/>replica stays PROVISIONING until it is culled. Measured on a spoke built<br/>from this module alone -- TCP/8080 answered while TCP/22 was dropped, and<br/>every replica stalled. The sibling spoke worked only because an unrelated<br/>legacy rule ("SSH access from admin subnets") happened to cover the same<br/>control-plane CIDR, so the dependency was never represented here.<br/><br/>`additional_port_descriptions` pins the AWS description of one additional<br/>port, keyed by port number as a string. Its only purpose is adopting a<br/>pre-existing rule: AWS treats (protocol, ports, CIDR) as the rule identity<br/>and rejects a duplicate, so a spoke that already carries the grant must<br/>import it rather than create it, and the import only plans clean if the<br/>description matches byte for byte. Leave it unset for new rules. | <pre>object({<br/>    node_security_group_id       = string<br/>    control_plane_cidr           = string<br/>    port                         = number<br/>    additional_ports             = optional(set(number), [])<br/>    additional_port_descriptions = optional(map(string), {})<br/>    description                  = optional(string, "SkyServe probe traffic from the control plane")<br/>    allow_public_cidr            = optional(bool, false)<br/>  })</pre> | `null` | no |
| <a name="input_tags"></a> [tags](#input\_tags) | Tags to merge onto AWS resources. | `map(string)` | `{}` | no |

## Outputs

| Name | Description |
| ---- | ----------- |
| <a name="output_access_entry_id"></a> [access\_entry\_id](#output\_access\_entry\_id) | ID of the EKS access entry that maps the controller principal. |
| <a name="output_fsx_claims"></a> [fsx\_claims](#output\_fsx\_claims) | Static FSx volume identity keys to namespaced PVC names. |
| <a name="output_kueue_local_queues"></a> [kueue\_local\_queues](#output\_kueue\_local\_queues) | Active Kueue LocalQueues created for configured partitions. |
| <a name="output_partition_service_accounts"></a> [partition\_service\_accounts](#output\_partition\_service\_accounts) | Partition namespace to workload service-account name mapping. |
| <a name="output_partitions"></a> [partitions](#output\_partitions) | Partition namespace to RBAC group mapping. |
| <a name="output_priority_classes"></a> [priority\_classes](#output\_priority\_classes) | Partition namespace to enforced PriorityClass name mapping. |
<!-- END_TF_DOCS -->
