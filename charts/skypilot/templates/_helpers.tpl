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
Return the state PVC selected by this release. An external claim is
infrastructure-owned and must exist before any workload rollout begins.
*/}}
{{- define "skypilot.storageClaimName" -}}
{{- default (printf "%s-state" (include "skypilot.fullname" .)) (get .Values.storage "existingClaim") -}}
{{- end -}}

{{/*
Stable, release-scoped name for a proposed authority cohort's pre-apply
manifest.  Do not use skypilot.fullname here: fullnameOverride is mutable,
while namespace + Helm release name is the durable release anchor.
*/}}
{{- define "skypilot.resourceActionAuthorityPreflightManifestName" -}}
{{- $identity := printf "%s\n%s\n%s" .namespace .helmReleaseName .cohortSuffix -}}
{{- printf "skypilot-ra-preflight-%s" (sha256sum $identity | trunc 40) -}}
{{- end -}}

{{/* Exact read-only path consumed by the migration preflight parser. */}}
{{- define "skypilot.resourceActionAuthorityPreflightManifestPath" -}}
{{- printf "/etc/skypilot/resource-action-authority/release-preflight/%s/manifest.json" .cohortSuffix -}}
{{- end -}}

{{/*
Validate and resolve the optional immutable API-role qualification policy.
An empty exact triple is the only unconfigured state; any partial or drifted
reference fails before Helm emits a workload.  A missing key is the sole
backward-compatible old-release state and resolves to that exact empty triple;
an explicit null or non-object remains invalid.
*/}}
{{- define "skypilot.resourceActionQualificationPolicy" -}}
{{- $resourceActions := dict -}}
{{- if hasKey .Values "resourceActions" -}}
{{- $resourceActions = get .Values "resourceActions" -}}
{{- if not (kindIs "map" $resourceActions) -}}
{{- fail "resourceActions must be an object" -}}
{{- end -}}
{{- end -}}
{{- $policy := dict "repoPath" "" "byteSize" 0 "sha256" "" -}}
{{- if hasKey $resourceActions "qualificationPolicy" -}}
{{- $policy = get $resourceActions "qualificationPolicy" -}}
{{- if not (kindIs "map" $policy) -}}
{{- fail "resourceActions.qualificationPolicy must be an object" -}}
{{- end -}}
{{- end -}}
{{- if or (ne (len $policy) 3) (not (hasKey $policy "repoPath")) (not (hasKey $policy "byteSize")) (not (hasKey $policy "sha256")) -}}
{{- fail "resourceActions.qualificationPolicy must contain exactly repoPath, byteSize, and sha256" -}}
{{- end -}}
{{- $repoPath := get $policy "repoPath" -}}
{{- $byteSize := get $policy "byteSize" -}}
{{- $sha256 := get $policy "sha256" -}}
{{- if not (kindIs "string" $repoPath) -}}
{{- fail "resourceActions.qualificationPolicy.repoPath must be text" -}}
{{- end -}}
{{- if not (kindIs "string" $sha256) -}}
{{- fail "resourceActions.qualificationPolicy.sha256 must be text" -}}
{{- end -}}
{{- $byteSizeKind := kindOf $byteSize -}}
{{- if not (or (eq $byteSizeKind "int") (eq $byteSizeKind "int64") (eq $byteSizeKind "float64")) -}}
{{- fail "resourceActions.qualificationPolicy.byteSize must be an integer" -}}
{{- end -}}
{{- if ne (float64 $byteSize) (floor (float64 $byteSize)) -}}
{{- fail "resourceActions.qualificationPolicy.byteSize must be an integer" -}}
{{- end -}}
{{- $configured := or (ne $repoPath "") (ne (int $byteSize) 0) (ne $sha256 "") -}}
{{- if not $configured -}}
{{- toJson (dict "configured" false) -}}
{{- else -}}
{{- if not (regexMatch "^charts/skypilot/files/resource-action-qualification-policies/[A-Za-z0-9_-][A-Za-z0-9_.-]*(?:/[A-Za-z0-9_-][A-Za-z0-9_.-]*)*\\.json$" $repoPath) -}}
{{- fail "resourceActions.qualificationPolicy.repoPath must name a normalized JSON file under charts/skypilot/files/resource-action-qualification-policies/" -}}
{{- end -}}
{{- if or (lt (int $byteSize) 1) (gt (int $byteSize) 65536) -}}
{{- fail "resourceActions.qualificationPolicy.byteSize must be between 1 and 65536" -}}
{{- end -}}
{{- if not (regexMatch "^[0-9a-f]{64}$" $sha256) -}}
{{- fail "resourceActions.qualificationPolicy.sha256 must be lowercase SHA-256 hex" -}}
{{- end -}}
{{- $contents := .Files.Get (trimPrefix "charts/skypilot/" $repoPath) -}}
{{- if empty $contents -}}
{{- fail "resourceActions.qualificationPolicy is not packaged by this chart" -}}
{{- end -}}
{{- if ne (len $contents) (int $byteSize) -}}
{{- fail "resourceActions.qualificationPolicy.byteSize does not match packaged bytes" -}}
{{- end -}}
{{- if ne (sha256sum $contents) $sha256 -}}
{{- fail "resourceActions.qualificationPolicy.sha256 does not match packaged bytes" -}}
{{- end -}}
{{- $fullName := include "skypilot.fullname" . -}}
{{- $identity := printf "%s\n%s\n%s" .Release.Namespace $fullName $sha256 -}}
{{- $configMapName := printf "skypilot-ra-policy-%s" (sha256sum $identity | trunc 40) -}}
{{- toJson (dict "configured" true "repoPath" $repoPath "byteSize" (int $byteSize) "sha256" $sha256 "configMapName" $configMapName) -}}
{{- end -}}
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
