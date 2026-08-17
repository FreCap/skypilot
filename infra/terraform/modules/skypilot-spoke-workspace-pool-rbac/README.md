# Kubernetes RBAC for SkyPilot spoke workspace pools

Creates the Kubernetes service account and least-privilege RBAC used by a
SkyPilot spoke workspace pool. This module is cloud-neutral: the caller
configures the Kubernetes provider and maps its cloud or human identity to one
or more Kubernetes `User` or `Group` subjects. A pool may serve multiple
workspaces; this module does not create a one-to-one workspace binding. “Spoke”
is logical, and this infrastructure package is unrelated to a managed Job Pool
operated through `sky jobs pool`.

The module grants:

- cluster-wide `get`/`list` access to nodes and pods;
- cluster-wide `list` access to RuntimeClasses;
- `get` access to only the `kube-system` Namespace object, whose UID lets
  protocol-v2 reserved fill deduplicate context aliases and fence retargeting;
- namespaced pod lifecycle, exec, and port-forward permissions;
- namespaced service lifecycle and event-list permissions;
- exact-name `get` on the configured workload ServiceAccount for server-owned
  worker projection and reserved-fill provider attestation;
- optional exact-name `get` on one Kueue LocalQueue, ClusterQueue, and
  partition Namespace, plus exact `GET /apis` and `GET /apis/`, for
  control-plane preflight;
- optional read-only access to persistent volume claims; and
- a separate teardown-only role bound to the pool ServiceAccount, so a node can
  delete itself when its idle timer fires.

It does not create cloud IAM, an identity mapping, a cluster, storage, or
SkyPilot configuration.

## Requirements

- Terraform or OpenTofu 1.5 or newer.
- Kubernetes provider 2.20 or newer, configured by the root module.
- Provider credentials authorized to manage namespaces, service accounts,
  roles, role bindings, cluster roles, and cluster role bindings.

ClusterRole and ClusterRoleBinding names are cluster-wide. Use a unique `name`
for every module instance in a cluster.

## Usage

Pin cross-repository consumers to an immutable commit:

```hcl
module "skypilot_spoke_workspace_pool_rbac" {
  source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-spoke-workspace-pool-rbac?ref=<full-commit-sha>"

  name                 = "skypilot-gpu-pool"
  namespace            = "skypilot-gpu"
  service_account_name = "skypilot-pool-sa"
  allow_pvc_read       = true
  kueue = {
    local_queue_name   = "default"
    cluster_queue_name = "inference-borrower"
  }

  subjects = [{
    kind = "Group"
    name = "skypilot:gpu-pool"
  }]

  labels = {
    "app.kubernetes.io/environment" = "production"
  }
}
```

Set `manage_namespace = false` only when the namespace already exists and is
managed by another Terraform resource or system. The module still creates the
service account and namespaced RBAC there.

## Security and ownership

The cluster-wide pod read permission is needed by SkyPilot's real-time
GPU-availability view. The namespaced role is the workload isolation boundary.
This module does not enforce which namespace a SkyPilot workspace chooses; the
control-plane configuration must pin workloads to the intended namespace.
The physical-cluster identity grant cannot be expressed with a namespaced
Role because Namespace objects are cluster-scoped. It grants only the named
`kube-system` object's non-secret metadata: no namespace list/watch, mutation,
or read of another Namespace is allowed.

`subjects` accepts `User` and `Group` subjects. Service-account subjects are not
accepted because Kubernetes role-binding service-account subjects also require
an explicit namespace, which is not part of this module's public subject type.

When `kueue` is non-null, the namespaced control-plane Role receives only `get`
on the exact `localqueues.kueue.x-k8s.io` object. The ClusterRole receives only
exact-name `get` on the referenced ClusterQueue and partition Namespace, which
lets runtime preflight evaluate current queue policy without any list or
mutation permission. It also receives non-resource `GET /apis` and
`GET /apis/` so version discovery remains explicit across supported Kubernetes
clients on clusters that do not grant the usual discovery role. The workload
ServiceAccount's self-teardown Role receives no Kueue permission. Omitting the
input creates no Kueue RBAC and is backward compatible.

The namespaced control-plane Role can also `get` only the configured workload
ServiceAccount. It cannot list, watch, create, update, patch, or delete
ServiceAccounts, and the workload ServiceAccount's self-teardown Role receives
no ServiceAccount API permission.

`cluster_queue_name` must be one DNS-1123 label of at most 63 characters, even
though the ClusterQueue API itself accepts a DNS subdomain. Strict SkyPilot
admission requires Kueue 0.18's `AssignQueueLabelsForPods` feature to publish
the exact ClusterQueue name on each admitted Pod; dotted and other non-label
names are therefore rejected before any RBAC is created.

### Node self-teardown

Everything bound to `subjects` is the *control plane's* identity. One SkyPilot
operation does not run there: the idle-timer teardown behind
`sky launch -i N --down` runs inside the node, as the pool ServiceAccount. With
no binding for that ServiceAccount it fails with 403 every 60s forever, and
because storing an autostop config needs no RBAC the API server still reports a
healthy `AUTOSTOP Nm (down)` while the cluster runs indefinitely.

The module therefore always creates a separate, teardown-shaped Role (pods
`get`/`list`/`delete`, services `get`/`list`/`delete`/`deletecollection`, events
`create`, deployments `list`) bound to the pool ServiceAccount. It is separate
from the control-plane Role on purpose: that one carries pods `create`,
`pods/exec` and `pods/portforward`, which would let a workload pod start pods and
exec into its neighbours.

This briefly shipped as an opt-in `allow_self_teardown` defaulting to `false`,
because Kubernetes RBAC cannot scope a verb to "pods this cluster owns" — pod
names are dynamic and verbs take no label selector — so in a namespace shared by
several users one SkyPilot workload can delete another's SkyPilot pods. That
reservation was misplaced. A pool namespace is not a tenant boundary and this
module never claimed it was; the caller's `partitions` documentation says so
outright, and the control-plane subjects already hold pods `create`/`exec`/
`portforward` in the same namespace. A pool without the grant cannot honour
`--down` at all, and nothing surfaces that until a cluster has been idling for
hours. A knob whose only correct value is `true` is a footgun, so it was
removed. Callers that set `allow_self_teardown` must drop the argument.

## Upgrade and rollback

The resource labels and permissions match the original deployed module.
Changing `name`, `namespace`, `service_account_name`, or `manage_namespace`
changes durable Kubernetes identities or ownership and requires a state-aware
migration. Moving an unchanged caller from a local source to this Git source
does not change Terraform addresses and requires no `moved` blocks.

Before applying an upgrade, save and review a plan. To roll back a source-only
migration, restore the previous source at the same addresses. Do not delete
namespace resources from state without first deciding which system owns the
live namespace.

## Module reference

<!-- BEGIN_TF_DOCS -->
## Requirements

| Name | Version |
| ---- | ------- |
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.5.0 |
| <a name="requirement_kubernetes"></a> [kubernetes](#requirement\_kubernetes) | >= 2.20 |

## Providers

| Name | Version |
| ---- | ------- |
| <a name="provider_kubernetes"></a> [kubernetes](#provider\_kubernetes) | 3.2.1 |

## Modules

No modules.

## Resources

| Name | Type |
| ---- | ---- |
| [kubernetes_cluster_role_binding_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/cluster_role_binding_v1) | resource |
| [kubernetes_cluster_role_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/cluster_role_v1) | resource |
| [kubernetes_namespace_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/namespace_v1) | resource |
| [kubernetes_role_binding_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role_binding_v1) | resource |
| [kubernetes_role_binding_v1.pool_sa_self_teardown](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role_binding_v1) | resource |
| [kubernetes_role_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role_v1) | resource |
| [kubernetes_role_v1.pool_sa_self_teardown](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role_v1) | resource |
| [kubernetes_service_account_v1.pool_sa](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/service_account_v1) | resource |

## Inputs

| Name | Description | Type | Default | Required |
| ---- | ----------- | ---- | ------- | :------: |
| <a name="input_allow_pvc_read"></a> [allow\_pvc\_read](#input\_allow\_pvc\_read) | Grant read (get/list) on persistentvolumeclaims in the namespace. Required when<br/>a tier mounts pre-existing PVCs (e.g. FSx): before creating the pod SkyPilot GETs<br/>each referenced claim to check its phase. Read-only — the PVCs are Terraform-<br/>provisioned, so SkyPilot never creates/deletes them. Leave false for tiers with<br/>no volumes (nothing to read). | `bool` | `false` | no |
| <a name="input_kueue"></a> [kueue](#input\_kueue) | Optional Kueue objects that the SkyPilot control-plane subjects must<br/>preflight before launching a Pod. When set, grant only exact-name `get` on<br/>the namespaced LocalQueue, cluster-scoped ClusterQueue, and this Namespace,<br/>plus exact `GET /apis` and `GET /apis/` for served-version discovery. The<br/>workload ServiceAccount receives no Kueue permission.<br/><br/>cluster\_queue\_name must be one DNS-1123 label of at most 63 characters.<br/>Strict SkyPilot admission requires Kueue's AssignQueueLabelsForPods<br/>feature to publish that name on admitted Pods; dotted DNS subdomains and<br/>other non-label names cannot be published. | <pre>object({<br/>    local_queue_name   = string<br/>    cluster_queue_name = string<br/>  })</pre> | `null` | no |
| <a name="input_labels"></a> [labels](#input\_labels) | Extra labels applied to the RBAC objects. | `map(string)` | `{}` | no |
| <a name="input_manage_namespace"></a> [manage\_namespace](#input\_manage\_namespace) | Create the namespace. Set false if it is provisioned elsewhere. | `bool` | `true` | no |
| <a name="input_name"></a> [name](#input\_name) | Name for the RBAC objects (ClusterRole/Role/bindings). | `string` | `"skypilot-pool"` | no |
| <a name="input_namespace"></a> [namespace](#input\_namespace) | Dedicated namespace SkyPilot launches workloads into. NOT a shared application namespace. | `string` | `"skypilot-pool"` | no |
| <a name="input_service_account_name"></a> [service\_account\_name](#input\_service\_account\_name) | ServiceAccount created in the pool namespace for SkyPilot pods (matches the control plane's kubernetes.pod\_config serviceAccountName). | `string` | `"skypilot-pool-sa"` | no |
| <a name="input_subjects"></a> [subjects](#input\_subjects) | RBAC subjects that represent the SkyPilot control plane's identity on this<br/>cluster. EKS pools pass a Group (populated by an access entry); GKE pools pass<br/>a User equal to the controller's GCP service-account email. | <pre>list(object({<br/>    kind      = string<br/>    name      = string<br/>    api_group = optional(string, "rbac.authorization.k8s.io")<br/>  }))</pre> | n/a | yes |

## Outputs

| Name | Description |
| ---- | ----------- |
| <a name="output_cluster_role_name"></a> [cluster\_role\_name](#output\_cluster\_role\_name) | Name shared by the cluster role and its binding. |
| <a name="output_kueue"></a> [kueue](#output\_kueue) | Exact Kueue LocalQueue and ClusterQueue names readable by the control-plane subjects, or null. |
| <a name="output_namespace"></a> [namespace](#output\_namespace) | Namespace SkyPilot launches pool workloads into. |
| <a name="output_role_name"></a> [role\_name](#output\_role\_name) | Name shared by the namespaced role and its binding. |
| <a name="output_self_teardown_role_name"></a> [self\_teardown\_role\_name](#output\_self\_teardown\_role\_name) | Name shared by the pod ServiceAccount's self-teardown role and its binding<br/>-- the grant that lets a node honour `sky launch -i N --down`. |
| <a name="output_service_account_name"></a> [service\_account\_name](#output\_service\_account\_name) | Name of the service account created for SkyPilot workloads. |
<!-- END_TF_DOCS -->
