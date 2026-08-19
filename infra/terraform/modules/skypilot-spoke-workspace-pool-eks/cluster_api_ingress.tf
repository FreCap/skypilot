# Private EKS endpoints attach the EKS-managed cluster security group to their
# network interfaces. EKS also attaches that group to managed-node interfaces
# by default, so callers must explicitly accept the shared TCP/443 exposure.
resource "aws_security_group_rule" "cluster_api_from_control_plane" {
  count = length(var.cluster_api_ingress_cidrs) > 0 ? 1 : 0

  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = var.cluster_api_ingress_cidrs
  description       = "SkyPilot control plane access to the private EKS API"
  security_group_id = data.aws_eks_cluster.target.vpc_config[0].cluster_security_group_id

  lifecycle {
    precondition {
      condition     = var.allow_cluster_security_group_node_ingress
      error_message = "allow_cluster_security_group_node_ingress must be true when cluster_api_ingress_cidrs is set because EKS also attaches the cluster security group to managed-node interfaces by default."
    }
  }
}
