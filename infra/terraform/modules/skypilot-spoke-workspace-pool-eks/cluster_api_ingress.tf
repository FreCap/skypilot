# Private EKS endpoints attach the EKS-managed cluster security group to their
# network interfaces. Keep this access rule with the spoke-pool attachment so
# an existing workload cluster does not need scheduler-specific module inputs.
resource "aws_security_group_rule" "cluster_api_from_control_plane" {
  count = length(var.cluster_api_ingress_cidrs) > 0 ? 1 : 0

  type              = "ingress"
  from_port         = 443
  to_port           = 443
  protocol          = "tcp"
  cidr_blocks       = var.cluster_api_ingress_cidrs
  description       = "SkyPilot control plane access to the private EKS API"
  security_group_id = data.aws_eks_cluster.target.vpc_config[0].cluster_security_group_id
}
