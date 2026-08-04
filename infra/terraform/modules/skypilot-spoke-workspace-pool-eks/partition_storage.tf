# Static FSx PV and PVC resources are keyed by namespace/claim name. The
# persistent volumes retain their data when released. Creating a PVC here does
# not prevent workloads from using other PVCs already present in the namespace.

locals {
  fsx_volumes = merge([
    for partition in var.partitions : {
      for volume in partition.fsx_volumes :
      "${partition.namespace}/${volume.claim_name}" => merge(volume, {
        namespace = partition.namespace
        dnsname   = "${volume.volume_handle}.fsx.${var.aws_region}.${data.aws_partition.current.dns_suffix}"
      })
    }
  ]...)
}

resource "kubernetes_persistent_volume_v1" "fsx" {
  for_each = local.fsx_volumes

  metadata {
    name   = "${each.value.claim_name}-${each.value.namespace}"
    labels = { "app.kubernetes.io/managed-by" = "Terraform" }
  }

  spec {
    capacity                         = { storage = each.value.capacity }
    access_modes                     = ["ReadWriteMany"]
    persistent_volume_reclaim_policy = "Retain"
    storage_class_name               = each.value.storage_class
    volume_mode                      = "Filesystem"
    mount_options                    = each.value.driver == "fsx.csi.aws.com" ? ["flock"] : []

    persistent_volume_source {
      csi {
        driver        = each.value.driver
        volume_handle = each.value.volume_handle
        volume_attributes = each.value.driver == "fsx.openzfs.csi.aws.com" ? {
          DNSName      = each.value.dnsname
          ResourceType = "filesystem"
          } : {
          dnsname   = each.value.dnsname
          mountname = each.value.mountname
        }
      }
    }
  }
}

resource "kubernetes_persistent_volume_claim_v1" "fsx" {
  for_each = local.fsx_volumes

  metadata {
    name      = each.value.claim_name
    namespace = each.value.namespace
    labels    = { "app.kubernetes.io/managed-by" = "Terraform" }
  }

  spec {
    access_modes       = ["ReadWriteMany"]
    storage_class_name = each.value.storage_class
    volume_name        = kubernetes_persistent_volume_v1.fsx[each.key].metadata[0].name

    resources {
      requests = { storage = each.value.capacity }
    }
  }

  depends_on = [module.rbac]
}
