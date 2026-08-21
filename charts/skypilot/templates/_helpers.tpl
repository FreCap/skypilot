{{- define "skypilot.checkResources" -}}
{{- $cpu := .Values.apiService.resources.requests.cpu | default "0" -}}
{{- $memory := .Values.apiService.resources.requests.memory | default "0" -}}

{{- /* Convert CPU to numeric value */ -}}
{{- $cpuNum := 0.0 -}}
{{- if kindIs "string" $cpu -}}
  {{- if hasSuffix "m" $cpu -}}
    {{- $cpuNum = float64 (divf (trimSuffix "m" $cpu | atoi) 1000) -}}
  {{- else -}}
    {{- $cpuNum = float64 ($cpu | atoi) -}}
  {{- end -}}
{{- else -}}
  {{- $cpuNum = float64 $cpu -}}
{{- end -}}

{{- /* Convert memory to Gi */ -}}
{{- $memNum := 0.0 -}}
{{- if hasSuffix "Gi" $memory -}}
  {{- $memNum = float64 (trimSuffix "Gi" $memory | atoi) -}}
{{- else if hasSuffix "Mi" $memory -}}
  {{- $memNum = float64 (divf (trimSuffix "Mi" $memory | atoi) 1024) -}}
{{- else if hasSuffix "G" $memory -}}
  {{- $memNum = float64 ($memory | trimSuffix "G" | atoi) -}}
{{- else if hasSuffix "M" $memory -}}
  {{- $memNum = float64 (divf (trimSuffix "M" $memory | atoi) 1024) -}}
{{- end -}}

{{- if or (lt $cpuNum 4.0) (lt $memNum 8.0) -}}
{{/* TODO(aylei): add a reference to the tuning guide once complete */}}
  {{- fail "Error\nDeploying a SkyPilot API server requires at least 4 CPU cores and 8 GiB memory. You can either:\n1. Change `--set apiService.resources.requests.cpu` and `--set apiService.resources.requests.memory` to meet the requirements or unset them to use defaults\n2. add `--set apiService.skipResourceCheck=true` in command args to bypass this check (not recommended for production)\nto resolve this issue and then try again." -}}
{{- end -}}

{{- end -}}

{{/*
Fail before rendering a Pod whose configurable environment, volume, or mount
inputs collide with each other or with chart-owned fields.
*/}}
{{- define "skypilot.validatePodExtras" -}}
{{- $guardedEphemeralProfile := default false .guardedEphemeralProfile -}}
{{- $seenEnvs := dict -}}
{{- range (default (list) .reservedEnvNames) -}}
{{- $_ := set $seenEnvs . true -}}
{{- end -}}
{{- range $envList := (default (list) .envLists) -}}
{{- range (default (list) $envList) -}}
{{- $name := default "" .name -}}
{{- if empty $name -}}
{{- fail "extraEnvs entries must have a nonempty name" -}}
{{- end -}}
{{- if hasKey $seenEnvs $name -}}
{{- if eq $name "SKYPILOT_STATE_DB_MIGRATION_MODE" -}}
{{- fail "SKYPILOT_STATE_DB_MIGRATION_MODE is managed by databaseMigration and cannot be set through extraEnvs" -}}
{{- else -}}
{{- fail (printf "environment variable %q is duplicated or reserved by the chart" $name) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $seenEnvs $name true -}}
{{- end -}}
{{- end -}}

{{- $seenVolumes := dict -}}
{{- range (default (list) .reservedVolumeNames) -}}
{{- $_ := set $seenVolumes . true -}}
{{- end -}}
{{- range $volumeList := (default (list) .volumeLists) -}}
{{- range (default (list) $volumeList) -}}
{{- $name := default "" .name -}}
{{- if empty $name -}}
{{- fail "extraVolumes entries must have a nonempty name" -}}
{{- end -}}
{{- if hasKey $seenVolumes $name -}}
{{- fail (printf "volume name %q is duplicated or reserved by the chart" $name) -}}
{{- end -}}
{{- if and $guardedEphemeralProfile (hasKey . "persistentVolumeClaim") -}}
{{- fail (printf "extraVolumes persistentVolumeClaim %q is not supported in guarded HA" $name) -}}
{{- end -}}
{{- if and $guardedEphemeralProfile (hasKey . "emptyDir") -}}
{{- fail (printf "extraVolumes emptyDir %q is not supported in guarded HA; use a chart-owned bounded volume or a read-only projected source" $name) -}}
{{- end -}}
{{- if $guardedEphemeralProfile -}}
{{- $allowedVolumeKeys := list "name" "secret" "configMap" "projected" "downwardAPI" -}}
{{- range $key, $_ := . -}}
{{- if not (has $key $allowedVolumeKeys) -}}
{{- fail (printf "extraVolumes volume %q uses unsupported source %q in guarded HA" $name $key) -}}
{{- end -}}
{{- end -}}
{{- if not (or (hasKey . "secret") (hasKey . "configMap") (hasKey . "projected") (hasKey . "downwardAPI")) -}}
{{- fail (printf "extraVolumes volume %q must use a read-only projected Kubernetes source in guarded HA" $name) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $seenVolumes $name true -}}
{{- end -}}
{{- end -}}

{{- $seenMountNames := dict -}}
{{- range (default (list) .reservedVolumeNames) -}}
{{- $_ := set $seenMountNames . true -}}
{{- end -}}
{{- $seenMountPaths := dict -}}
{{- range (default (list) .reservedMountPaths) -}}
{{- $_ := set $seenMountPaths . true -}}
{{- end -}}
{{- range $mountList := (default (list) .mountLists) -}}
{{- range (default (list) $mountList) -}}
{{- $name := default "" .name -}}
{{- $path := default "" .mountPath -}}
{{- if or (empty $name) (empty $path) -}}
{{- fail "extraVolumeMounts entries must have nonempty name and mountPath fields" -}}
{{- end -}}
{{- if hasKey $seenMountNames $name -}}
{{- fail (printf "volume mount name %q is duplicated or reserved by the chart" $name) -}}
{{- end -}}
{{- if hasKey $seenMountPaths $path -}}
{{- fail (printf "volume mount path %q is duplicated or reserved by the chart" $path) -}}
{{- end -}}
{{- $_ := set $seenMountNames $name true -}}
{{- $_ := set $seenMountPaths $path true -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Require per-container node-ephemeral accounting for a guarded HA role. */}}
{{- define "skypilot.requireRoleEphemeralStorage" -}}
{{- $role := required "role is required" .role -}}
{{- $chartInjectsBudget := default false .chartInjectsBudget -}}
{{- $resources := default (dict) .resources -}}
{{- $requests := default (dict) (get $resources "requests") -}}
{{- $limits := default (dict) (get $resources "limits") -}}
{{- if and (not $chartInjectsBudget) (empty (get $requests "ephemeral-storage")) -}}
{{- fail (printf "%s resources.requests.ephemeral-storage is required in guarded HA" $role) -}}
{{- end -}}
{{- if and (not $chartInjectsBudget) (empty (get $limits "ephemeral-storage")) -}}
{{- fail (printf "%s resources.limits.ephemeral-storage is required in guarded HA" $role) -}}
{{- end -}}
{{- if and (not (empty (get $requests "ephemeral-storage"))) (ne (toString (get $requests "ephemeral-storage")) "6Gi") -}}
{{- fail (printf "%s resources.requests.ephemeral-storage must use the guarded HA chart-owned value 6Gi" $role) -}}
{{- end -}}
{{- if and (not (empty (get $limits "ephemeral-storage"))) (ne (toString (get $limits "ephemeral-storage")) "8Gi") -}}
{{- fail (printf "%s resources.limits.ephemeral-storage must use the guarded HA chart-owned value 8Gi" $role) -}}
{{- end -}}
{{- $ephemeralStorage := default (dict) .ephemeralStorage -}}
{{- $expectedLimits := dict "credentialSizeLimit" "64Mi" "logsSizeLimit" "1Gi" "runtimeSizeLimit" "256Mi" "sshSizeLimit" "64Mi" "stateSizeLimit" "4Gi" -}}
{{- range $name, $expected := $expectedLimits -}}
{{- if ne (toString (get $ephemeralStorage $name)) $expected -}}
{{- fail (printf "storage.ephemeral.%s must use the guarded HA chart-owned value %s" $name $expected) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Type-check and serialize the operator-owned annotations that SkyPilot merges
into each controller-generated inference Service. Kubernetes key syntax and
SkyPilot ownership conflicts have one semantic validator in sky/serve/lb_k8s.py.
*/}}
{{- define "skypilot.externalLBServiceAnnotationsJson" -}}
{{- $externalLB := .Values.serve.externalLoadBalancer -}}
{{- $annotations := get $externalLB "serviceAnnotations" -}}
{{- if not (kindIs "map" $annotations) -}}
{{- fail "serve.externalLoadBalancer.serviceAnnotations must be an object whose keys and values are strings" -}}
{{- end -}}
{{- range $key, $value := $annotations -}}
{{- if not (kindIs "string" $key) -}}
{{- fail "serve.externalLoadBalancer.serviceAnnotations keys must be strings" -}}
{{- end -}}
{{- if not (kindIs "string" $value) -}}
{{- fail (printf "serve.externalLoadBalancer.serviceAnnotations[%q] must be a string" $key) -}}
{{- end -}}
{{- end -}}
{{- toJson $annotations -}}
{{- end -}}

{{/*
Resolve the image name, overriding the registry when global.imageRegistry is set.
Usage: {{ include "common.image" (dict "root" . "image" "repo/name:tag") }}
*/}}
{{- define "common.image" -}}
{{- $image := default "" .image -}}
{{- $registry := default "" .root.Values.global.imageRegistry -}}
{{- if $registry -}}
  {{- $imagePath := trimPrefix "/" $image -}}
  {{- $parts := splitList "/" $imagePath -}}
  {{- if gt (len $parts) 1 -}}
    {{- $first := index $parts 0 -}}
    {{- if or (contains "." $first) (contains ":" $first) (eq $first "localhost") -}}
      {{- $imagePath = join "/" (slice $parts 1) -}}
    {{- end -}}
  {{- end -}}
  {{- printf "%s/%s" (trimSuffix "/" $registry) $imagePath -}}
{{- else -}}
  {{- $image -}}
{{- end -}}
{{- end -}}

{{/*
Pod scheduling aligned with apiService (nodeSelector, affinity, tolerations).
Applied to oauth2-proxy and bundled redis so they schedule like the API server
when umbrella charts set apiService.nodeSelector / tolerations / affinity.
*/}}
{{- define "skypilot.apiPodScheduling" -}}
{{- with .Values.apiService.nodeSelector }}
      nodeSelector:
        {{- toYaml . | nindent 8 }}
{{- end }}
{{- with .Values.apiService.affinity }}
      affinity:
        {{- toYaml . | nindent 8 }}
{{- end }}
{{- with .Values.apiService.tolerations }}
      tolerations:
        {{- toYaml . | nindent 8 }}
{{- end }}
{{- end -}}


{{/*
Check for apiService.config during upgrade and display warning
*/}}
{{- define "skypilot.checkUpgradeConfig" -}}
{{- if and .Release.IsUpgrade .Values.apiService.config -}}
WARNING: apiService.config is set during an upgrade operation, which will be IGNORED.

To update your SkyPilot config, follow the instructions in the upgrade guide:
https://docs.skypilot.co/en/latest/reference/api-server/api-server-admin-deploy.html#setting-the-skypilot-config
{{- end -}}
{{- end -}}

{{/*
Compute full release name with optional fullnameOverride.
*/}}
{{- define "skypilot.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/*
Return the effective request-store contract. The absent-parent branch exists
for exactly one Helm 3.19.1 --reuse-values cleanup revision: a final
requestStore: null deletes the captured legacy map before Serve047 restores the
canonical PostgreSQL-only map and removes this branch. Fail closed to
PostgreSQL with execution quiescence enforced; never reinterpret absence as
SQLite.
*/}}
{{- define "skypilot.effectiveRequestStore" -}}
{{- if hasKey .Values "requestStore" -}}
{{- toJson .Values.requestStore -}}
{{- else -}}
{{- toJson (dict "backend" "postgres" "enforceBuiltinExecutionQuiescenceBackends" true "cutoverGatePath" "/root/.sky/api-request-cutover.json") -}}
{{- end -}}
{{- end -}}

{{/*
Return the state PVC selected by this release. An external claim is
infrastructure-owned and must exist before any workload rollout begins.
*/}}
{{- define "skypilot.storageClaimName" -}}
{{- default (printf "%s-state" (include "skypilot.fullname" .)) (get .Values.storage "existingClaim") -}}
{{- end -}}

{{/*
Create the name of the service account to use
*/}}
{{- define "skypilot.serviceAccountName" -}}
{{- if .Values.rbac.serviceAccountName -}}
{{ .Values.rbac.serviceAccountName }}
{{- else -}}
{{ include "skypilot.fullname" . }}-api-sa
{{- end -}}
{{- end -}}

{{/* Managed image workers use intentionally separate workload identities. */}}
{{- define "skypilot.imageCopyWorkerServiceAccountName" -}}
{{- if .Values.imageCopyWorker.serviceAccount.name -}}
{{ .Values.imageCopyWorker.serviceAccount.name }}
{{- else -}}
{{ include "skypilot.fullname" . }}-image-copy-worker
{{- end -}}
{{- end -}}

{{/* Length-safe, identity-stable RBAC name for one source credential. */}}
{{- define "skypilot.imageSourceRbacName" -}}
{{- $base := include "skypilot.fullname" .root | trunc 42 | trimSuffix "-" -}}
{{- $identity := printf "%s/%s" .namespace .name -}}
{{- $hash := sha256sum $identity | trunc 12 -}}
{{- printf "%s-img-src-%s" $base $hash | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "skypilot.imageLifecycleWorkerServiceAccountName" -}}
{{- if .Values.imageLifecycleWorker.serviceAccount.name -}}
{{ .Values.imageLifecycleWorker.serviceAccount.name }}
{{- else -}}
{{ include "skypilot.fullname" . }}-image-lifecycle-worker
{{- end -}}
{{- end -}}

{{- define "skypilot.imageCanaryWorkerServiceAccountName" -}}
{{- if .Values.imageCanaryWorker.serviceAccount.name -}}
{{ .Values.imageCanaryWorker.serviceAccount.name }}
{{- else -}}
{{ include "skypilot.fullname" . }}-image-canary-worker
{{- end -}}
{{- end -}}

{{/*
Create the namespace if not exist
*/}}
{{- define "skypilot.ensureNamespace" -}}
{{ if not (lookup "v1" "Namespace" "" .) }}
apiVersion: v1
kind: Namespace
metadata:
  name: {{ . }}
  annotations:
    {{/* Keep the namespace when uninstalling the chart, so that the deployed sky resources (if any) can still work even if the API server get uninstalled */ -}}
    helm.sh/resource-policy: keep
{{ end -}}
{{- end -}}

{{/* Whether to enable basic auth */}}
{{- define "skypilot.enableBasicAuthInAPIServer" -}}
{{- if and (not (index .Values.ingress "oauth2-proxy" "enabled")) .Values.apiService.enableUserManagement -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{/* Get initial basic auth secret name */}}
{{- define "skypilot.initialBasicAuthSecretName" -}}
{{- if .Values.apiService.initialBasicAuthSecret -}}
{{ .Values.apiService.initialBasicAuthSecret }}
{{- else if .Values.apiService.initialBasicAuthCredentials -}}
{{ printf "%s-initial-basic-auth" (include "skypilot.fullname" .) }}
{{- else -}}
{{- /* Return empty string */ -}}
{{ "" }}
{{- end -}}
{{- end -}}

{{/* API server start arguments */}}
{{- define "skypilot.apiArgs" -}}
--deploy{{ if .Values.apiService.metrics.enabled }} --metrics --metrics-port {{ .Values.apiService.metrics.port }}{{ end }}{{ if include "skypilot.enableBasicAuthInAPIServer" . | trim | eq "true" }} --enable-basic-auth{{ end }}
{{- end -}}

{{- define "skypilot.oauth2ProxyURL" -}}
http://{{ include "skypilot.fullname" . }}-oauth2-proxy:4180
{{- end -}}

{{- define "skypilot.ingressBasicAuthEnabled" -}}
{{- if and .Values.ingress.enabled (or .Values.ingress.authSecret .Values.ingress.authCredentials) -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "skypilot.ingressOAuthEnabled" -}}
{{- if and .Values.ingress.enabled (index .Values.ingress "oauth2-proxy" "enabled") -}}
true
{{- else -}}
false
{{- end -}}
{{- end -}}

{{- define "skypilot.serviceAccountAuthEnabled" -}}
{{- if include "skypilot.ingressBasicAuthEnabled" . | trim | eq "true" -}}
false
{{- else if and .Values.auth .Values.auth.serviceAccount (ne .Values.auth.serviceAccount.enabled nil) -}}
{{- .Values.auth.serviceAccount.enabled -}}
{{- else -}}
{{- .Values.apiService.enableServiceAccounts -}}
{{- end -}}
{{- end -}}

{{/* Validate the oauth config */}}
{{- define "skypilot.validateOAuthConfig" -}}
{{- $authOAuthEnabled := .Values.auth.oauth.enabled -}}
{{- $ingressBasicAuthEnabled := include "skypilot.ingressBasicAuthEnabled" . | trim | eq "true" -}}
{{- $ingressOAuthEnabled := include "skypilot.ingressOAuthEnabled" . | trim | eq "true" -}}

{{- if and $authOAuthEnabled $ingressBasicAuthEnabled -}}
  {{- fail "Error\nauth.oauth.enabled cannot be used together with ingress basic authentication (ingress.authSecret or ingress.authCredentials). These authentication methods are mutually exclusive. Please:\n1. Disable auth.oauth.enabled, OR\n2. Remove ingress.authSecret and ingress.authCredentials\nThen try again." -}}
{{- end -}}

{{- if and $authOAuthEnabled $ingressOAuthEnabled -}}
  {{- fail "Error\nauth.oauth.enabled cannot be used together with ingress OAuth2 proxy authentication (ingress.oauth2-proxy.enabled). These authentication methods are mutually exclusive. Please:\n1. Disable auth.oauth.enabled, OR\n2. Set ingress.oauth2-proxy.enabled to false\nThen try again." -}}
{{- end -}}
{{- end -}}

{{/* Validate the external proxy config */}}
{{- define "skypilot.validateExternalProxyConfig" -}}
{{- $externalProxyEnabled := .Values.auth.externalProxy.enabled -}}
{{- $authOAuthEnabled := .Values.auth.oauth.enabled -}}
{{- $ingressOAuthEnabled := include "skypilot.ingressOAuthEnabled" . | trim | eq "true" -}}

{{- if and $externalProxyEnabled $authOAuthEnabled -}}
  {{- fail "Error\nauth.externalProxy.enabled cannot be used together with auth.oauth.enabled. These authentication methods are mutually exclusive. Please:\n1. Disable auth.externalProxy.enabled, OR\n2. Set auth.oauth.enabled to false\nThen try again." -}}
{{- end -}}

{{- if and $externalProxyEnabled $ingressOAuthEnabled -}}
  {{- fail "Error\nauth.externalProxy.enabled cannot be used together with ingress.oauth2-proxy.enabled. These authentication methods are mutually exclusive. Please:\n1. Disable auth.externalProxy.enabled, OR\n2. Set ingress.oauth2-proxy.enabled to false\nThen try again." -}}
{{- end -}}
{{- end -}}
