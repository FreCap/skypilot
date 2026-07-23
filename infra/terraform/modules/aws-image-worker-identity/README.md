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
