# AWS image worker identity

This module creates three independent IRSA roles for the managed image copy,
lifecycle, and runtime-canary workers. Each role is bound to one exact
Kubernetes service account and may assume only its explicitly listed target
roles. The canary worker cannot assume a registry copy or lifecycle role.

The module creates no long-lived credentials. Pass its three role annotations
to `imageCopyWorker.serviceAccount.annotations`,
`imageLifecycleWorker.serviceAccount.annotations`, and
`imageCanaryWorker.serviceAccount.annotations` in the SkyPilot Helm chart.
The target role names should be deterministic inputs, which avoids a Terraform
dependency cycle between worker identity, registry policy, and compute-canary
modules.

Planning fails closed unless:

- the OIDC provider ARN exactly matches the HTTPS issuer authority and path in
  the active AWS account and partition;
- every target is an exact, bounded IAM role ARN in the active partition;
- an optional permissions boundary is an exact, bounded managed-policy ARN in
  the active account and partition; and
- the namespace and service-account names satisfy Kubernetes DNS limits.

Target roles may be in other accounts within the same AWS partition. Each
worker target set is capped at 64 entries and at a conservative serialized
policy budget so Terraform rejects a configuration that cannot fit in the
worker's inline policy.
