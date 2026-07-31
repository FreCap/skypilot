# SkyPilot pool RBAC

Creates the Kubernetes service account and least-privilege RBAC used by a
SkyPilot compute pool. This module is cloud-neutral: the caller configures the
Kubernetes provider and maps its cloud or human identity to one or more
Kubernetes `User` or `Group` subjects.

The module grants:

- cluster-wide `get`/`list` access to nodes and pods;
- cluster-wide `list` access to RuntimeClasses;
- namespaced pod lifecycle, exec, and port-forward permissions;
- namespaced service lifecycle and event-list permissions; and
- optional read-only access to persistent volume claims.

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
module "skypilot_pool_rbac" {
  source = "git::https://github.com/boltz-bio/skypilot.git//infra/terraform/modules/skypilot-pool-rbac?ref=<full-commit-sha>"

  name                 = "skypilot-gpu-pool"
  namespace            = "skypilot-gpu"
  service_account_name = "skypilot-pool-sa"
  allow_pvc_read       = true

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

`subjects` accepts `User` and `Group` subjects. Service-account subjects are not
accepted because Kubernetes role-binding service-account subjects also require
an explicit namespace, which is not part of this module's public subject type.

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
|------|---------|
| <a name="requirement_terraform"></a> [terraform](#requirement\_terraform) | >= 1.5.0 |
| <a name="requirement_kubernetes"></a> [kubernetes](#requirement\_kubernetes) | >= 2.20 |

## Providers

| Name | Version |
|------|---------|
| <a name="provider_kubernetes"></a> [kubernetes](#provider\_kubernetes) | >= 2.20 |

## Modules

No modules.

## Resources

| Name | Type |
|------|------|
| [kubernetes_cluster_role_binding_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/cluster_role_binding_v1) | resource |
| [kubernetes_cluster_role_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/cluster_role_v1) | resource |
| [kubernetes_namespace_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/namespace_v1) | resource |
| [kubernetes_role_binding_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role_binding_v1) | resource |
| [kubernetes_role_v1.pool](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/role_v1) | resource |
| [kubernetes_service_account_v1.pool_sa](https://registry.terraform.io/providers/hashicorp/kubernetes/latest/docs/resources/service_account_v1) | resource |

## Inputs

| Name | Description | Type | Default | Required |
|------|-------------|------|---------|:--------:|
| <a name="input_allow_pvc_read"></a> [allow\_pvc\_read](#input\_allow\_pvc\_read) | Grant read (get/list) on persistentvolumeclaims in the namespace. Required when<br/>a tier mounts pre-existing PVCs (e.g. FSx): before creating the pod SkyPilot GETs<br/>each referenced claim to check its phase. Read-only — the PVCs are Terraform-<br/>provisioned, so SkyPilot never creates/deletes them. Leave false for tiers with<br/>no volumes (nothing to read). | `bool` | `false` | no |
| <a name="input_labels"></a> [labels](#input\_labels) | Extra labels applied to the RBAC objects. | `map(string)` | `{}` | no |
| <a name="input_manage_namespace"></a> [manage\_namespace](#input\_manage\_namespace) | Create the namespace. Set false if it is provisioned elsewhere. | `bool` | `true` | no |
| <a name="input_name"></a> [name](#input\_name) | Name for the RBAC objects (ClusterRole/Role/bindings). | `string` | `"skypilot-pool"` | no |
| <a name="input_namespace"></a> [namespace](#input\_namespace) | Dedicated namespace SkyPilot launches workloads into. NOT a shared application namespace. | `string` | `"skypilot-pool"` | no |
| <a name="input_service_account_name"></a> [service\_account\_name](#input\_service\_account\_name) | ServiceAccount created in the pool namespace for SkyPilot pods (matches the control plane's kubernetes.pod\_config serviceAccountName). | `string` | `"skypilot-pool-sa"` | no |
| <a name="input_subjects"></a> [subjects](#input\_subjects) | RBAC subjects that represent the SkyPilot control plane's identity on this<br/>cluster. EKS pools pass a Group (populated by an access entry); GKE pools pass<br/>a User equal to the controller's GCP service-account email. | <pre>list(object({<br/>    kind      = string<br/>    name      = string<br/>    api_group = optional(string, "rbac.authorization.k8s.io")<br/>  }))</pre> | n/a | yes |

## Outputs

| Name | Description |
|------|-------------|
| <a name="output_cluster_role_name"></a> [cluster\_role\_name](#output\_cluster\_role\_name) | Name shared by the cluster role and its binding. |
| <a name="output_namespace"></a> [namespace](#output\_namespace) | Namespace SkyPilot launches pool workloads into. |
| <a name="output_role_name"></a> [role\_name](#output\_role\_name) | Name shared by the namespaced role and its binding. |
| <a name="output_service_account_name"></a> [service\_account\_name](#output\_service\_account\_name) | Name of the service account created for SkyPilot workloads. |
<!-- END_TF_DOCS -->
