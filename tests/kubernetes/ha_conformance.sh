#!/usr/bin/env bash
#
# Exercise a guarded SkyPilot HA release under continuous in-cluster traffic.
# The script rolls image A to image B, deletes every original role pod, rolls
# back to A, and upgrades to B again. It never reads or prints token material;
# the canary consumes an existing Kubernetes Secret directly.

set -euo pipefail

CONTEXT="${SKYPILOT_HA_CONTEXT:?set SKYPILOT_HA_CONTEXT}"
NAMESPACE="${SKYPILOT_HA_NAMESPACE:?set SKYPILOT_HA_NAMESPACE}"
RELEASE="${SKYPILOT_HA_RELEASE:?set SKYPILOT_HA_RELEASE}"
IMAGE_B="${SKYPILOT_HA_IMAGE_B:?set SKYPILOT_HA_IMAGE_B}"
TOKEN_SECRET="${SKYPILOT_HA_TOKEN_SECRET:?set SKYPILOT_HA_TOKEN_SECRET}"
TOKEN_KEY="${SKYPILOT_HA_TOKEN_KEY:-token}"
CHART="${SKYPILOT_HA_CHART:-charts/skypilot}"
TIMEOUT="${SKYPILOT_HA_TIMEOUT:-30m}"
EXPECTED_CONFIRM="${CONTEXT}/${NAMESPACE}/${RELEASE}"
CONFIRM="${SKYPILOT_HA_CONFIRM:-}"

for command_name in awk curl helm jq kubectl sed; do
  command -v "${command_name}" >/dev/null || {
    echo "missing required command: ${command_name}" >&2
    exit 1
  }
done

if [[ "${CONFIRM}" != "${EXPECTED_CONFIRM}" ]]; then
  echo "refusing destructive HA checks: set SKYPILOT_HA_CONFIRM=${EXPECTED_CONFIRM}" >&2
  exit 1
fi
if [[ "${NAMESPACE}" == "default" || "${NAMESPACE}" == "test" ]]; then
  echo "refusing to run in protected namespace ${NAMESPACE}" >&2
  exit 1
fi
if [[ ! -d "${CHART}" ]]; then
  echo "chart directory does not exist: ${CHART}" >&2
  exit 1
fi

kubectl --context "${CONTEXT}" get namespace "${NAMESPACE}" >/dev/null
kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get secret \
  "${TOKEN_SECRET}" -o json | jq -e \
  --arg key "${TOKEN_KEY}" '.data[$key] != null' >/dev/null

release_values="$(
  helm get values "${RELEASE}" --kube-context "${CONTEXT}" \
    --namespace "${NAMESPACE}" -o json
)"
jq -e '
  .apiService.highAvailability.enabled == true and
  .requestStore.backend == "postgres" and
  .apiService.dbConnectionSecretName != null
' <<<"${release_values}" >/dev/null || {
  echo "release is not a guarded PostgreSQL HA deployment" >&2
  exit 1
}
DRAIN_SECONDS="$(
  jq -r '.apiService.highAvailability.readinessDrainSeconds // 20' \
    <<<"${release_values}"
)"
if [[ ! "${DRAIN_SECONDS}" =~ ^[0-9]+$ || "${DRAIN_SECONDS}" -lt 1 ]]; then
  echo "release has an invalid readiness drain interval: ${DRAIN_SECONDS}" >&2
  exit 1
fi

REVISION_A="$(
  helm status "${RELEASE}" --kube-context "${CONTEXT}" \
    --namespace "${NAMESPACE}" -o json | jq -r '.version'
)"
IMAGE_A="$(jq -r '.apiService.image' <<<"${release_values}")"
if [[ -z "${IMAGE_A}" || "${IMAGE_A}" == "null" ]]; then
  echo "release has no explicit apiService.image" >&2
  exit 1
fi
if [[ "${IMAGE_A}" == "${IMAGE_B}" ]]; then
  echo "image A and image B must differ" >&2
  exit 1
fi

CANARY_NAME="${RELEASE:0:39}-ha-conformance"

cleanup_canary() {
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" delete deployment \
    "${CANARY_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" delete pdb \
    "${CANARY_NAME}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
}
trap cleanup_canary EXIT
cleanup_canary

kubectl --context "${CONTEXT}" -n "${NAMESPACE}" apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${CANARY_NAME}
spec:
  replicas: 1
  selector:
    matchLabels:
      app: ${CANARY_NAME}
  template:
    metadata:
      labels:
        app: ${CANARY_NAME}
      annotations:
        karpenter.sh/do-not-disrupt: "true"
    spec:
      automountServiceAccountToken: false
      containers:
      - name: canary
        image: curlimages/curl:8.12.1
        env:
        - name: CANARY_TOKEN
          valueFrom:
            secretKeyRef:
              name: ${TOKEN_SECRET}
              key: ${TOKEN_KEY}
        command:
        - /bin/sh
        - -c
        - |
          set -eu
          token="\$(printf '%s' "\${CANARY_TOKEN}" | tr -d '\r\n')"
          base_url="http://${RELEASE}-api-service"
          health_loop() {
            while true; do
              if ! code="\$(curl --silent --show-error --output /dev/null \
                --write-out '%{http_code}' --max-time 5 \
                "\${base_url}/api/health")"; then
                code=000
              fi
              echo "CANARY health \${code} \$(date -u +%Y-%m-%dT%H:%M:%SZ)"
              sleep 0.1
            done
          }
          durable_loop() {
            headers="/tmp/status-headers"
            while true; do
              : >"\${headers}"
              if ! submit_code="\$(curl --silent --show-error \
                --dump-header "\${headers}" --output /dev/null \
                --write-out '%{http_code}' --max-time 10 \
                --header "Authorization: Bearer \${token}" \
                --header "Content-Type: application/json" \
                --data '{"include_credentials":false,"summary_response":true}' \
                "\${base_url}/status")"; then
                submit_code=000
              fi
              request_id="\$(awk '
                tolower(\$1) == "x-skypilot-request-id:" {print \$2}
              ' "\${headers}" | tail -n 1 | tr -d '\r\n')"
              echo "CANARY submit \${submit_code} \${request_id:-missing}"
              if [ "\${submit_code}" = 200 ] && [ -n "\${request_id}" ]; then
                if ! get_code="\$(curl --silent --show-error \
                  --output /dev/null --write-out '%{http_code}' --max-time 30 \
                  --header "Authorization: Bearer \${token}" \
                  "\${base_url}/api/get?request_id=\${request_id}")"; then
                  get_code=000
                fi
                echo "CANARY get \${get_code} \${request_id}"
                if ! stream_code="\$(curl --silent --show-error \
                  --output /dev/null --write-out '%{http_code}' --max-time 30 \
                  --header "Authorization: Bearer \${token}" \
                  "\${base_url}/api/stream?request_id=\${request_id}&follow=false&format=plain")"; then
                  stream_code=000
                fi
                echo "CANARY stream \${stream_code} \${request_id}"
              fi
              sleep 1
            done
          }
          health_loop &
          health_pid=\$!
          durable_loop &
          durable_pid=\$!
          trap 'kill "\${health_pid}" "\${durable_pid}" 2>/dev/null || true' \
            INT TERM EXIT
          wait
        resources:
          requests:
            cpu: 20m
            memory: 32Mi
          limits:
            cpu: 200m
            memory: 96Mi
---
apiVersion: policy/v1
kind: PodDisruptionBudget
metadata:
  name: ${CANARY_NAME}
spec:
  minAvailable: 1
  selector:
    matchLabels:
      app: ${CANARY_NAME}
EOF

kubectl --context "${CONTEXT}" -n "${NAMESPACE}" rollout status \
  "deployment/${CANARY_NAME}" --timeout="${TIMEOUT}"
CANARY_UID="$(
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pods \
    -l "app=${CANARY_NAME}" -o jsonpath='{.items[0].metadata.uid}'
)"

canary_logs() {
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" logs \
    "deployment/${CANARY_NAME}" --all-containers
}

assert_canary() {
  local phase="$1"
  local current_uid restart_count
  read -r current_uid restart_count < <(
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pods \
      -l "app=${CANARY_NAME}" -o json | jq -r '
      if (.items | length) == 1 then
        [.items[0].metadata.uid,
         (.items[0].status.containerStatuses[0].restartCount // 0)] | @tsv
      else
        ["missing", -1] | @tsv
      end
    '
  )
  if [[ "${current_uid}" != "${CANARY_UID}" || "${restart_count}" != 0 ]]; then
    echo "canary identity changed during ${phase}: uid=${current_uid} restarts=${restart_count}" >&2
    return 1
  fi
  local logs
  logs="$(canary_logs)"
  local bad
  bad="$(
    awk '
      $1 == "CANARY" &&
      ($3 != "200" || ($2 == "submit" && $4 == "missing")) {print}
    ' <<<"${logs}" | head -20
  )"
  if [[ -n "${bad}" ]]; then
    echo "canary failure during ${phase}:" >&2
    echo "${bad}" >&2
    return 1
  fi
  local health_count submit_count get_count stream_count
  health_count="$(awk '$1 == "CANARY" && $2 == "health" {count++} END {print count+0}' <<<"${logs}")"
  submit_count="$(awk '$1 == "CANARY" && $2 == "submit" {count++} END {print count+0}' <<<"${logs}")"
  get_count="$(awk '$1 == "CANARY" && $2 == "get" {count++} END {print count+0}' <<<"${logs}")"
  stream_count="$(awk '$1 == "CANARY" && $2 == "stream" {count++} END {print count+0}' <<<"${logs}")"
  if ((health_count < 20 || submit_count < 2 || get_count < 2 ||
       stream_count < 2)); then
    echo "insufficient canary samples during ${phase}: health=${health_count} submit=${submit_count} get=${get_count} stream=${stream_count}" >&2
    return 1
  fi
  echo "canary ${phase}: health=${health_count} submit=${submit_count} get=${get_count} stream=${stream_count}, failures=0"
}

wait_roles() {
  local deployment
  for deployment in \
    "${RELEASE}-api-server" \
    "${RELEASE}-executor" \
    "${RELEASE}-controller"; do
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" rollout status \
      "deployment/${deployment}" --timeout="${TIMEOUT}"
  done
}

verify_stable_ha() {
  local deployment desired available
  for deployment in \
    "${RELEASE}-api-server" \
    "${RELEASE}-executor" \
    "${RELEASE}-controller"; do
    read -r desired available < <(
      kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get deployment \
        "${deployment}" -o json | jq -r \
        '[.spec.replicas, (.status.availableReplicas // 0)] | @tsv'
    )
    if [[ "${desired}" != "${available}" || "${available}" -lt 2 ]]; then
      echo "deployment ${deployment} is not stably redundant: desired=${desired} available=${available}" >&2
      return 1
    fi
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get deployment \
      "${deployment}" -o json | jq -e '
      .spec.template.spec.topologySpreadConstraints
      | any(.topologyKey == "kubernetes.io/hostname")
    ' >/dev/null
  done

  local pdb healthy desired_healthy allowed
  for pdb in "${RELEASE}-api" "${RELEASE}-executor" "${RELEASE}-controller"; do
    read -r healthy desired_healthy allowed < <(
      kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pdb "${pdb}" \
        -o json | jq -r \
        '[(.status.currentHealthy // 0), (.status.desiredHealthy // 0), (.status.disruptionsAllowed // 0)] | @tsv'
    )
    if ((healthy < desired_healthy || allowed < 1)); then
      echo "PDB ${pdb} is not healthy: current=${healthy} desired=${desired_healthy} allowed=${allowed}" >&2
      return 1
    fi
  done
}

record_state() {
  local phase="$1"
  echo "STATE ${phase}"
  helm status "${RELEASE}" --kube-context "${CONTEXT}" \
    --namespace "${NAMESPACE}" -o json | jq \
    '{revision: .version, status: .info.status, updated: .info.last_deployed}'
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get deployments,pdb -o wide
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pods \
    -o json | jq -r --arg release "${RELEASE}" '
    .items[]
    | (.metadata.labels.app // "") as $app
    | select($app | startswith($release + "-api")
      or startswith($release + "-executor")
      or startswith($release + "-controller"))
    | [.metadata.name, .metadata.uid, .metadata.creationTimestamp,
       .spec.containers[0].image,
       (.status.containerStatuses[0].imageID // "pending")]
    | @tsv
  '
}

verify_migration_hook() {
  local revision="$1"
  local expected_image="$2"
  local job
  job="$(
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get jobs \
      -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=database-migration,skypilot.co/helm-revision=${revision}" \
      -o json | jq -r '.items | if length == 1 then .[0].metadata.name else "" end'
  )"
  if [[ -z "${job}" ]]; then
    echo "expected exactly one migration hook Job for revision ${revision}" >&2
    return 1
  fi
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" wait \
    --for=condition=complete "job/${job}" --timeout="${TIMEOUT}"
  kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get job "${job}" \
    -o json | jq -e --arg image "${expected_image}" '
    .status.succeeded == 1 and
    (.status.failed // 0) == 0 and
    .spec.template.spec.containers[0].image == $image
  ' >/dev/null

  local completed_at first_target_pod
  completed_at="$(
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get job "${job}" \
      -o jsonpath='{.status.completionTime}'
  )"
  first_target_pod="$(
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pods -o json |
      jq -r --arg release "${RELEASE}" --arg image "${expected_image}" '
      [.items[]
       | (.metadata.labels.app // "") as $app
       | select($app | startswith($release + "-api")
         or startswith($release + "-executor")
         or startswith($release + "-controller"))
       | select(.metadata.deletionTimestamp == null)
       | select(.spec.containers[0].image == $image)
       | .metadata.creationTimestamp]
      | sort
      | .[0] // ""
    '
  )"
  if [[ -z "${completed_at}" || -z "${first_target_pod}" ||
        "${completed_at}" > "${first_target_pod}" ]]; then
    echo "migration hook did not complete before target-image pod creation: hook=${completed_at} pod=${first_target_pod}" >&2
    return 1
  fi
  echo "migration revision ${revision}: job=${job} completed=${completed_at} first_target_pod=${first_target_pod}"
}

verify_role_image() {
  local expected_image="$1"
  local deployment role actual pod_count mismatches expected_digest
  expected_digest=""
  if [[ "${expected_image}" == *@sha256:* ]]; then
    expected_digest="${expected_image##*@}"
  fi
  for role in api executor controller; do
    if [[ "${role}" == api ]]; then
      deployment="${RELEASE}-api-server"
    else
      deployment="${RELEASE}-${role}"
    fi
    actual="$(
      kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get deployment \
        "${deployment}" -o jsonpath='{.spec.template.spec.containers[0].image}'
    )"
    if [[ "${actual}" != "${expected_image}" ]]; then
      echo "deployment ${deployment} image mismatch: ${actual}" >&2
      return 1
    fi
    read -r pod_count mismatches < <(
      kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pods \
        -l "app=${RELEASE}-${role}" -o json | jq -r \
        --arg image "${expected_image}" --arg digest "${expected_digest}" '
        [.items[] | select(.metadata.deletionTimestamp == null)] as $pods
        | [
            ($pods | length),
            ([$pods[]
              | select(
                  .spec.containers[0].image != $image or
                  (.status.containerStatuses[0].ready // false) != true or
                  (.status.containerStatuses[0].restartCount // 0) != 0 or
                  ($digest != "" and
                   ((.status.containerStatuses[0].imageID // "")
                    | endswith($digest) | not))
                )] | length)
          ]
        | @tsv
      '
    )
    if ((pod_count < 2 || mismatches != 0)); then
      echo "role ${role} has non-ready, restarted, or wrong-image pods: count=${pod_count} mismatches=${mismatches}" >&2
      return 1
    fi
  done
}

drain_row_state() {
  local instance_id="$1"
  local query_pod output
  query_pod="$(
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pods \
      -l "app=${RELEASE}-api" -o json | jq -r '
      [.items[]
       | select(.metadata.deletionTimestamp == null)
       | select(
           any(.status.conditions[]?;
               .type == "Ready" and .status == "True"))
       | .metadata.name]
      | sort
      | .[0] // ""
    '
  )"
  if [[ -z "${query_pod}" ]]; then
    echo "WAIT"
    return
  fi
  output="$(
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" exec "${query_pod}" -- \
      python -c '
import sys
import uuid

import sqlalchemy

from sky.server.requests import postgres as request_postgres

with request_postgres.initialize_and_get_db().connect() as connection:
    row = connection.execute(
        sqlalchemy.select(
            request_postgres.SERVER_INSTANCES.c.ready,
            request_postgres.SERVER_INSTANCES.c.draining_at,
            request_postgres.SERVER_INSTANCES.c.health_detail,
        ).where(
            request_postgres.SERVER_INSTANCES.c.instance_id
            == uuid.UUID(sys.argv[1])
        )
    ).mappings().one_or_none()
phase = None if row is None else (row["health_detail"] or {}).get("phase")
print(
    "DRAINED"
    if row is not None
    and not row["ready"]
    and row["draining_at"] is not None
    and phase == "draining"
    else "WAIT"
)
' "${instance_id}" 2>/dev/null || true
  )"
  tail -n 1 <<<"${output}"
}

delete_original_role_pods() {
  local role="$1"
  local selector="app=${RELEASE}-${role}"
  local pods pod
  pods="$(
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pods \
      -l "${selector}" -o json | jq -r \
      '[.items[] | select(.metadata.deletionTimestamp == null)]
       | sort_by(.metadata.name)[] | .metadata.name'
  )"
  if [[ "$(wc -w <<<"${pods}" | tr -d ' ')" -lt 2 ]]; then
    echo "role ${role} does not have two pods to delete" >&2
    return 1
  fi
  for pod in ${pods}; do
    local pod_ip instance_id probe_path probe_port
    read -r pod_ip instance_id probe_path probe_port < <(
      kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pod "${pod}" \
        -o json | jq -r '
        .spec.containers[0] as $container
        | $container.readinessProbe.httpGet.port as $probe_port
        | [
            .status.podIP,
            .metadata.uid,
            $container.readinessProbe.httpGet.path,
            (if ($probe_port | type) == "number"
             then $probe_port
             else ([$container.ports[]
                    | select(.name == $probe_port)
                    | .containerPort][0])
             end)
          ]
        | @tsv
      '
    )
    if [[ -z "${pod_ip}" || -z "${instance_id}" ||
          -z "${probe_path}" || -z "${probe_port}" ]]; then
      echo "could not resolve readiness endpoint for ${pod}" >&2
      return 1
    fi
    local probe_host="${pod_ip}"
    if [[ "${probe_host}" == *:* ]]; then
      probe_host="[${probe_host}]"
    fi

    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" delete pod "${pod}" \
      --wait=false
    local endpoint_seen=false
    local lease_seen=false
    local pod_condition_seen=false
    local probe_code="missing"
    local row_state="WAIT"
    local ready="True"
    local drain_deadline=$((SECONDS + DRAIN_SECONDS))
    while kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pod "${pod}" \
      >/dev/null 2>&1; do
      if [[ "${endpoint_seen}" != true ]]; then
        probe_code="$(
          kubectl --context "${CONTEXT}" -n "${NAMESPACE}" exec \
            "deployment/${CANARY_NAME}" -- \
            curl --silent --output /dev/null --write-out '%{http_code}' \
              --max-time 2 \
              "http://${probe_host}:${probe_port}${probe_path}" \
              2>/dev/null || true
        )"
        if [[ "${probe_code}" == "503" ]]; then
          endpoint_seen=true
        fi
      fi
      if [[ "${lease_seen}" != true ]]; then
        row_state="$(drain_row_state "${instance_id}")"
        if [[ "${row_state}" == "DRAINED" ]]; then
          lease_seen=true
        fi
      fi
      ready="$(
        kubectl --context "${CONTEXT}" -n "${NAMESPACE}" get pod "${pod}" \
          -o json | jq -r '
          [.status.conditions[]
           | select(.type == "Ready")
           | .status][0] // "False"
        '
      )"
      if [[ "${ready}" == "False" ]]; then
        pod_condition_seen=true
      fi
      if [[ "${endpoint_seen}" == true && "${lease_seen}" == true &&
            "${pod_condition_seen}" == true ]]; then
        break
      fi
      if ((SECONDS >= drain_deadline)); then
        break
      fi
      sleep 1
    done
    if [[ "${endpoint_seen}" != true || "${lease_seen}" != true ||
          "${pod_condition_seen}" != true ]]; then
      echo "pod ${pod} did not complete readiness-first drain: endpoint=${probe_code} lease=${row_state} pod_ready=${ready}" >&2
      return 1
    fi
    echo "drain ${pod}: endpoint=503 lease=draining pod_ready=False"
    kubectl --context "${CONTEXT}" -n "${NAMESPACE}" wait \
      --for=delete "pod/${pod}" --timeout="${TIMEOUT}"
    wait_roles
    sleep 5
    assert_canary "${role}-pod-${pod}"
  done
}

upgrade_to_b() {
  helm upgrade "${RELEASE}" "${CHART}" --kube-context "${CONTEXT}" \
    --namespace "${NAMESPACE}" --reuse-values \
    --set-string "apiService.image=${IMAGE_B}" \
    --wait --timeout "${TIMEOUT}"
}

sleep 15
assert_canary baseline
record_state image-a

upgrade_to_b
REVISION_B="$(
  helm status "${RELEASE}" --kube-context "${CONTEXT}" \
    --namespace "${NAMESPACE}" -o json | jq -r '.version'
)"
verify_migration_hook "${REVISION_B}" "${IMAGE_B}"
wait_roles
verify_stable_ha
verify_role_image "${IMAGE_B}"
sleep 15
assert_canary image-a-to-b
record_state image-b

delete_original_role_pods api
delete_original_role_pods executor
delete_original_role_pods controller
verify_stable_ha

helm rollback "${RELEASE}" "${REVISION_A}" --kube-context "${CONTEXT}" \
  --namespace "${NAMESPACE}" --wait --timeout "${TIMEOUT}"
wait_roles
verify_role_image "${IMAGE_A}"
sleep 15
assert_canary rollback-to-image-a
record_state rollback-a

upgrade_to_b
REVISION_FINAL="$(
  helm status "${RELEASE}" --kube-context "${CONTEXT}" \
    --namespace "${NAMESPACE}" -o json | jq -r '.version'
)"
verify_migration_hook "${REVISION_FINAL}" "${IMAGE_B}"
wait_roles
verify_stable_ha
verify_role_image "${IMAGE_B}"
sleep 15
assert_canary final-image-b
record_state final-b

kubectl --context "${CONTEXT}" -n "${NAMESPACE}" delete jobs \
  -l "app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=database-migration" \
  --wait=true

echo "CONFORMANCE PASS: A=${IMAGE_A} B=${IMAGE_B} revisions=${REVISION_A},${REVISION_B},${REVISION_FINAL}"
