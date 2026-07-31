# Optional ingress for a SkyServe prober that reaches replica pod IPs through
# the node security group. This mutates a caller-owned security group and grants
# exactly one TCP port from one CIDR.
resource "aws_security_group_rule" "serve_probe_from_control_plane" {
  count = var.serve_probe_ingress == null ? 0 : 1

  type              = "ingress"
  security_group_id = var.serve_probe_ingress.node_security_group_id
  from_port         = var.serve_probe_ingress.port
  to_port           = var.serve_probe_ingress.port
  protocol          = "tcp"
  cidr_blocks       = [var.serve_probe_ingress.control_plane_cidr]
  description       = var.serve_probe_ingress.description
}
