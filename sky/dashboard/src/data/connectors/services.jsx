import { apiClient } from './client';
import { CLUSTER_NOT_UP_ERROR } from '@/data/connectors/constants';

// Normalize a raw replica_info entry from the /serve/status response.
// The REST encoder (`encode_serve_status`) serializes replica statuses to
// their plain string values (sky/serve/serve_state.py ReplicaStatus) and
// leaves the pickled `handle` as an opaque blob, which we ignore.
export function normalizeReplica(replica) {
  const rawHourlyCost = replica.hourly_cost;
  const hourlyCost =
    rawHourlyCost === null || rawHourlyCost === undefined
      ? null
      : Number(rawHourlyCost);
  const rawPlannedCapacity = Number(replica.planned_capacity);
  const plannedCapacity =
    Number.isInteger(rawPlannedCapacity) && rawPlannedCapacity > 0
      ? rawPlannedCapacity
      : 1;
  const rawReadyAt = replica.ready_at;
  const readyAt =
    rawReadyAt === null || rawReadyAt === undefined ? null : Number(rawReadyAt);
  const rawTimeToReadySeconds = replica.time_to_ready_seconds;
  const timeToReadySeconds =
    rawTimeToReadySeconds === null || rawTimeToReadySeconds === undefined
      ? null
      : Number(rawTimeToReadySeconds);
  return {
    id: replica.replica_id,
    status: replica.status,
    version: replica.version,
    endpoint: replica.endpoint || null,
    is_spot: replica.is_spot,
    launched_at: replica.launched_at || null,
    ready_at: Number.isFinite(readyAt) ? readyAt : null,
    timeToReadySeconds: Number.isFinite(timeToReadySeconds)
      ? timeToReadySeconds
      : null,
    cloud: replica.cloud || null,
    region: replica.region || null,
    infra: replica.infra || null,
    resources_str: replica.resources_str || null,
    resources_str_full:
      replica.resources_str_full || replica.resources_str || null,
    plannedCapacity,
    hourlyCost: Number.isFinite(hourlyCost) ? hourlyCost : null,
    hourlyCostExclusionReason: replica.hourly_cost_exclusion_reason || null,
  };
}

// Replica statuses treated as failed, mirroring
// sky/serve/serve_state.py ReplicaStatus.failed_statuses(). The CLI's
// REPLICAS column (`_get_replicas` in sky/serve/serve_utils.py) excludes
// these from the ready/total denominator; keep the dashboard consistent.
const FAILED_REPLICA_STATUSES = new Set([
  'FAILED',
  'FAILED_CLEANUP',
  'FAILED_INITIAL_DELAY',
  'FAILED_PROBING',
  'FAILED_PROVISION',
  'UNKNOWN',
]);

// Normalize a raw service record from the /serve/status response into the
// shape consumed by the services pages. Statuses arrive as plain strings
// (sky/serve/serve_state.py ServiceStatus values).
export function normalizeService(record) {
  const replicaInfo = Array.isArray(record.replica_info)
    ? record.replica_info
    : [];
  const replicas = replicaInfo.map(normalizeReplica);
  // Summary responses (summary_only=true) carry a cheap status histogram
  // instead of per-replica entries; compute the same aggregates from it so
  // list/header views render identically on either response shape.
  const counts =
    !replicaInfo.length && record.replica_status_counts
      ? record.replica_status_counts
      : null;
  let physicalReplicasReady;
  let physicalReplicasFailed;
  let physicalReplicasTotalRaw;
  if (counts) {
    physicalReplicasReady = counts['READY'] || 0;
    physicalReplicasFailed = Object.entries(counts)
      .filter(([status]) => FAILED_REPLICA_STATUSES.has(status))
      .reduce((acc, [, n]) => acc + n, 0);
    physicalReplicasTotalRaw = Object.values(counts).reduce(
      (acc, n) => acc + n,
      0
    );
  } else {
    physicalReplicasReady = replicas.filter((r) => r.status === 'READY').length;
    physicalReplicasFailed = replicas.filter((r) =>
      FAILED_REPLICA_STATUSES.has(r.status)
    ).length;
    physicalReplicasTotalRaw = replicas.length;
  }
  const physicalReplicasTotal =
    physicalReplicasTotalRaw - physicalReplicasFailed;

  const usesLogicalReplicas = ['logical', 'logical_slot'].includes(
    record.replica_unit
  );
  const capacityFor = (replica) =>
    usesLogicalReplicas ? replica.plannedCapacity : 1;
  const computedReplicasReady = replicas
    .filter((replica) => replica.status === 'READY')
    .reduce((total, replica) => total + capacityFor(replica), 0);
  const computedReplicasFailed = replicas
    .filter((replica) => FAILED_REPLICA_STATUSES.has(replica.status))
    .reduce((total, replica) => total + capacityFor(replica), 0);
  const computedReplicasTotal =
    replicas.reduce((total, replica) => total + capacityFor(replica), 0) -
    computedReplicasFailed;
  const hasAuthoritativeCapacityCounts = [
    record.ready_replicas,
    record.total_replicas,
    record.failed_replicas,
  ].every((value) => Number.isInteger(value) && value >= 0);
  const replicasReady = hasAuthoritativeCapacityCounts
    ? record.ready_replicas
    : replicas.length
      ? computedReplicasReady
      : physicalReplicasReady;
  const replicasTotal = hasAuthoritativeCapacityCounts
    ? record.total_replicas
    : replicas.length
      ? computedReplicasTotal
      : physicalReplicasTotal;
  const replicasFailed = hasAuthoritativeCapacityCounts
    ? record.failed_replicas
    : replicas.length
      ? computedReplicasFailed
      : physicalReplicasFailed;
  const physicalCountFields = [
    record.physical_ready_replicas,
    record.physical_total_replicas,
    record.physical_failed_replicas,
  ];
  const hasAuthoritativePhysicalCounts = physicalCountFields.every(
    (value) => Number.isInteger(value) && value >= 0
  );

  const pricedReplicas = replicas.filter(
    (replica) => replica.hourlyCost !== null
  );
  const estimatedHourlyCost = pricedReplicas.length
    ? pricedReplicas.reduce((total, replica) => total + replica.hourlyCost, 0)
    : null;
  const spotHourlyCost = pricedReplicas
    .filter((replica) => replica.is_spot)
    .reduce((total, replica) => total + replica.hourlyCost, 0);
  const onDemandHourlyCost = pricedReplicas
    .filter((replica) => !replica.is_spot)
    .reduce((total, replica) => total + replica.hourlyCost, 0);
  const hourlyCostExcludedReplicaCount = replicas.filter(
    (replica) => replica.hourlyCostExclusionReason
  ).length;
  const rawRequestRate = record.requests_per_second;
  const requestRate =
    rawRequestRate === null || rawRequestRate === undefined
      ? null
      : Number(rawRequestRate);
  const normalizedRequestRate = Number.isFinite(requestRate)
    ? requestRate
    : null;
  const costPerThousandRequests =
    estimatedHourlyCost !== null &&
    hourlyCostExcludedReplicaCount === 0 &&
    normalizedRequestRate > 0
      ? (estimatedHourlyCost * 1000) / (normalizedRequestRate * 3600)
      : null;

  return {
    name: record.name,
    status: record.status,
    // Epoch timestamp of when the service first became ready (see
    // serve_state.set_service_uptime); the UI renders `now - uptime`.
    uptime: record.uptime ?? null,
    endpoint: record.endpoint || null,
    replicasReady,
    replicasTotal,
    replicasFailed,
    replicaUnit: usesLogicalReplicas ? 'logical' : 'physical',
    physicalReplicasReady: hasAuthoritativePhysicalCounts
      ? record.physical_ready_replicas
      : physicalReplicasReady,
    physicalReplicasTotal: hasAuthoritativePhysicalCounts
      ? record.physical_total_replicas
      : physicalReplicasTotal,
    physicalReplicasFailed: hasAuthoritativePhysicalCounts
      ? record.physical_failed_replicas
      : physicalReplicasFailed,
    // True when this record came from a summary_only response: the
    // per-replica list is intentionally absent, not empty.
    summaryOnly: Boolean(counts),
    targetReplicas: record.target_num_replicas ?? null,
    policy: record.policy || null,
    loadBalancingPolicy: record.load_balancing_policy || null,
    requestedResources: record.requested_resources_str || null,
    activeVersions: record.active_versions || [],
    version: record.version ?? null,
    tlsEncrypted: Boolean(record.tls_encrypted),
    // User-facing task YAML, redacted server-side (`service_yaml` in
    // _get_service_status); absent on old servers and empty when the
    // controller could not read the stored YAML.
    serviceYaml: record.service_yaml || null,
    estimatedHourlyCost,
    spotHourlyCost,
    onDemandHourlyCost,
    pricedReplicaCount: pricedReplicas.length,
    hourlyCostExcludedReplicaCount,
    recentRequestCount: record.recent_request_count ?? null,
    requestWindowSeconds: record.request_window_seconds ?? null,
    requestRate: normalizedRequestRate,
    inFlightRequests: record.in_flight_requests ?? null,
    requestQueueDepth: record.request_queue_depth ?? null,
    rejectedRequests: record.rejected_requests ?? null,
    requestStatsAgeSeconds: record.request_stats_age_seconds ?? null,
    costPerThousandRequests,
    replicas,
  };
}

export async function getServices(options = {}) {
  // summaryOnly asks the server to skip per-replica serialization and
  // return a status histogram instead — at fleet scale (hundreds of
  // replicas) the full query takes tens of seconds while the summary is
  // near-instant, so list views and page headers should always use it.
  // serviceNames narrows the query to specific services (null = all).
  const {
    serviceNames = null,
    summaryOnly = false,
    includeTargetReplicas,
  } = options;
  try {
    const requestBody = {
      service_names: serviceNames,
      summary_only: summaryOnly,
    };
    if (includeTargetReplicas !== undefined) {
      requestBody.include_target_num_replicas = includeTargetReplicas;
    }
    const response = await apiClient.post(`/serve/status`, requestBody);
    if (!response.ok) {
      const msg = `Initial API request to get services failed with status ${response.status}`;
      throw new Error(msg);
    }
    const id = response.headers.get('X-Skypilot-Request-ID');
    if (!id) {
      const msg = 'No request ID received from server for getting services';
      throw new Error(msg);
    }
    const fetchedData = await apiClient.get(`/api/get?request_id=${id}`);
    let errorMessage = fetchedData.statusText;
    if (fetchedData.status === 500) {
      try {
        const data = await fetchedData.json();
        if (data.detail && data.detail.error) {
          try {
            const error = JSON.parse(data.detail.error);
            if (error.type && error.type === CLUSTER_NOT_UP_ERROR) {
              // The serve controller is not up (e.g. no services have ever
              // been launched); treat as "no services".
              return { services: [], controllerStopped: true };
            } else {
              errorMessage = error.message || String(data.detail.error);
            }
          } catch (jsonError) {
            console.error(
              'Error parsing JSON from data.detail.error:',
              jsonError
            );
            errorMessage = String(data.detail.error);
          }
        }
      } catch (dataError) {
        console.error('Error parsing response JSON:', dataError);
        errorMessage = String(dataError);
      }
    }

    if (!fetchedData.ok) {
      const msg = `API request to get services result failed with status ${fetchedData.status}, error: ${errorMessage}`;
      throw new Error(msg);
    }

    const data = await fetchedData.json();
    const serviceData = data.return_value ? JSON.parse(data.return_value) : [];
    const services = (Array.isArray(serviceData) ? serviceData : []).map(
      normalizeService
    );
    return { services, controllerStopped: false };
  } catch (error) {
    console.error('Error fetching services:', error);
    throw error;
  }
}
