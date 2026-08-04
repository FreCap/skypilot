import { apiClient } from './client';
import {
  API_VERSION_HEADER,
  CLUSTER_NOT_UP_ERROR,
  SERVE_DASHBOARD_DIRECT_READS_API_VERSION,
  SERVE_DASHBOARD_REPLICA_READS_API_VERSION,
} from '@/data/connectors/constants';

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
  const rawCreatedAt = replica.created_at;
  const createdAt =
    rawCreatedAt === null || rawCreatedAt === undefined
      ? null
      : Number(rawCreatedAt);
  return {
    id: replica.replica_id,
    status: replica.status,
    version: replica.version,
    endpoint: replica.endpoint || null,
    is_spot: replica.is_spot,
    launched_at: replica.launched_at || null,
    createdAt: Number.isFinite(createdAt) ? createdAt : null,
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

// Rows in these states are durable intent or completed lifecycle history, not
// current provider billability. Keep every other status conservative: stopping,
// cleanup-failed, unknown, and future statuses may still have live resources.
const NON_BILLABLE_COST_STATUSES = new Set([
  'PENDING',
  'FAILED',
  'FAILED_INITIAL_DELAY',
  'FAILED_PROBING',
  'FAILED_PROVISION',
  'PREEMPTED',
]);

const HISTORY_COUNT_FIELDS = [
  ['ready_count', 'readyCount'],
  ['provisioning_count', 'provisioningCount'],
  ['not_ready_count', 'notReadyCount'],
  ['errored_count', 'erroredCount'],
  ['preempted_count', 'preemptedCount'],
  ['stopping_count', 'stoppingCount'],
  ['total_count', 'totalCount'],
];

const OPTIONAL_HISTORY_COUNT_FIELDS = [
  ['ready_reserved_count', 'readyReservedCount'],
  ['logical_ready_count', 'logicalReadyCount'],
  ['logical_ready_reserved_count', 'logicalReadyReservedCount'],
  ['logical_provisioning_count', 'logicalProvisioningCount'],
  ['logical_not_ready_count', 'logicalNotReadyCount'],
  ['logical_errored_count', 'logicalErroredCount'],
  ['logical_preempted_count', 'logicalPreemptedCount'],
  ['logical_stopping_count', 'logicalStoppingCount'],
  ['logical_total_count', 'logicalTotalCount'],
];

const ACCELERATOR_HISTORY_FIELDS = [
  ['min_replicas', 'minReplicas'],
  ['demand_target', 'demandTarget'],
  ['ready_capacity', 'readyCapacity'],
  ['provisioning_capacity', 'provisioningCapacity'],
  ['total_capacity', 'totalCapacity'],
  ['zero_cost_ready_capacity', 'zeroCostReadyCapacity'],
  ['fill_target', 'fillTarget'],
  ['free_reserved_slots', 'freeReservedSlots'],
];

const OPTIONAL_ACCELERATOR_HISTORY_FIELDS = [
  ['warm_retention_target', 'warmRetentionTarget'],
  ['cold_launch_authority', 'coldLaunchAuthority'],
];

const PREDICTION_TIME_OUTCOMES = ['succeeded', 'failed'];

export function normalizeAcceleratorBreakdown(value) {
  if (!value || typeof value !== 'object' || value.version !== 1) return null;
  const cards = value.configured_accelerators;
  if (
    !Array.isArray(cards) ||
    cards.length === 0 ||
    cards.length > 8 ||
    cards.some((card) => typeof card !== 'string' || !card) ||
    new Set(cards.map((card) => card.toLowerCase())).size !== cards.length
  ) {
    return null;
  }
  const normalized = { configuredAccelerators: [...cards] };
  // `version` is the long-lived accelerator-breakdown/LB compatibility
  // schema. Capacity interpretation evolves independently so old samples stay
  // readable without relabeling their broader provisioning counts.
  if (
    value.capacity_semantics_version !== undefined &&
    value.capacity_semantics_version !== null
  ) {
    const capacitySemanticsVersion = Number(value.capacity_semantics_version);
    if (
      !Number.isInteger(capacitySemanticsVersion) ||
      capacitySemanticsVersion < 1
    ) {
      return null;
    }
    normalized.capacitySemanticsVersion = capacitySemanticsVersion;
  }
  for (const [source, target] of ACCELERATOR_HISTORY_FIELDS) {
    const raw = value[source];
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
    const counts = {};
    for (const card of cards) {
      const count = Number(raw[card]);
      if (!Number.isInteger(count) || count < 0) return null;
      counts[card] = count;
    }
    normalized[target] = counts;
  }
  for (const [source, target] of OPTIONAL_ACCELERATOR_HISTORY_FIELDS) {
    const raw = value[source];
    if (raw === undefined) {
      continue;
    }
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return null;
    const counts = {};
    for (const card of cards) {
      const count = Number(raw[card]);
      if (!Number.isInteger(count) || count < 0) return null;
      counts[card] = count;
    }
    normalized[target] = counts;
  }
  return normalized;
}

export function normalizeReplicaHistory(history) {
  if (!history || typeof history !== 'object') return null;
  const available = history.available !== false;
  const samples = Array.isArray(history.samples)
    ? history.samples
        .map((sample) => {
          const timestamp = Number(sample.timestamp);
          const version = Number(sample.version);
          if (!Number.isFinite(timestamp) || !Number.isInteger(version)) {
            return null;
          }
          const normalized = {
            timestamp,
            observedAt: Number(sample.observed_at) || timestamp,
            version,
          };
          HISTORY_COUNT_FIELDS.forEach(([source, target]) => {
            const value = Number(sample[source]);
            normalized[target] =
              Number.isInteger(value) && value >= 0 ? value : 0;
          });
          OPTIONAL_HISTORY_COUNT_FIELDS.forEach(([source, target]) => {
            const rawValue = sample[source];
            const value = Number(rawValue);
            normalized[target] =
              rawValue !== null &&
              rawValue !== undefined &&
              Number.isInteger(value) &&
              value >= 0
                ? value
                : null;
          });
          return normalized;
        })
        .filter(Boolean)
    : [];
  const requestSamples = Array.isArray(history.request_samples)
    ? history.request_samples
        .map((sample) => {
          const timestamp = Number(sample.timestamp);
          const requestCount = Number(sample.request_count);
          if (
            !Number.isFinite(timestamp) ||
            !Number.isInteger(requestCount) ||
            requestCount < 0
          ) {
            return null;
          }
          const rejectedCount = Number(sample.rejected_count);
          return {
            timestamp,
            requestCount,
            rejectedCount:
              Object.prototype.hasOwnProperty.call(sample, 'rejected_count') &&
              sample.rejected_count !== null &&
              sample.rejected_count !== undefined &&
              Number.isInteger(rejectedCount) &&
              rejectedCount >= 0
                ? rejectedCount
                : null,
          };
        })
        .filter(Boolean)
    : [];
  const predictionTimeHistogramVersion = Number(
    history.prediction_time_histogram_version
  );
  const predictionTimeBucketUpperBoundsSeconds = Array.isArray(
    history.prediction_time_bucket_upper_bounds_seconds
  )
    ? history.prediction_time_bucket_upper_bounds_seconds.map(Number)
    : [];
  const predictionTimeHistogramSupported =
    predictionTimeHistogramVersion === 1 &&
    predictionTimeBucketUpperBoundsSeconds.length > 0 &&
    predictionTimeBucketUpperBoundsSeconds.every(
      (value, index) =>
        Number.isFinite(value) &&
        value > 0 &&
        (index === 0 ||
          value > predictionTimeBucketUpperBoundsSeconds[index - 1])
    );
  const predictionTimeBucketCount =
    predictionTimeBucketUpperBoundsSeconds.length + 1;
  const predictionTimeSamples =
    predictionTimeHistogramSupported &&
    Array.isArray(history.prediction_time_samples)
      ? history.prediction_time_samples
          .map((sample) => {
            const timestamp = Number(sample.timestamp);
            const rawCounts = sample.outcome_counts;
            if (
              !Number.isFinite(timestamp) ||
              !rawCounts ||
              typeof rawCounts !== 'object' ||
              Array.isArray(rawCounts)
            ) {
              return null;
            }
            const outcomeCounts = {};
            for (const outcome of PREDICTION_TIME_OUTCOMES) {
              if (!Object.prototype.hasOwnProperty.call(rawCounts, outcome)) {
                continue;
              }
              const counts = rawCounts[outcome];
              if (
                !Array.isArray(counts) ||
                counts.length !== predictionTimeBucketCount
              ) {
                return null;
              }
              const normalizedCounts = counts.map(Number);
              if (
                normalizedCounts.some(
                  (count) => !Number.isInteger(count) || count < 0
                )
              ) {
                return null;
              }
              outcomeCounts[outcome] = normalizedCounts;
            }
            return Object.keys(outcomeCounts).length
              ? { timestamp, outcomeCounts }
              : null;
          })
          .filter(Boolean)
      : [];
  const autoscalerSamples = Array.isArray(history.autoscaler_samples)
    ? history.autoscaler_samples
        .map((sample) => {
          const timestamp = Number(sample.timestamp);
          const observedAt = Number(sample.observed_at);
          const version = Number(sample.version);
          const requiredCounts = [
            sample.demand_target,
            sample.capacity_target,
            sample.ready_capacity,
            sample.provisioning_capacity,
            sample.total_capacity,
          ].map(Number);
          if (
            !Number.isFinite(timestamp) ||
            !Number.isFinite(observedAt) ||
            !Number.isInteger(version) ||
            version < 1 ||
            !['physical_backend', 'logical_slot'].includes(
              sample.replica_unit
            ) ||
            requiredCounts.some(
              (value) => !Number.isInteger(value) || value < 0
            )
          ) {
            return null;
          }
          const [
            demandTarget,
            capacityTarget,
            readyCapacity,
            provisioningCapacity,
            totalCapacity,
          ] = requiredCounts;
          if (capacityTarget < demandTarget) return null;
          const optionalCount = (value) => {
            if (value === null || value === undefined) return null;
            const count = Number(value);
            return Number.isInteger(count) && count >= 0 ? count : null;
          };
          return {
            timestamp,
            observedAt,
            controllerSessionId:
              typeof sample.controller_session_id === 'string'
                ? sample.controller_session_id
                : null,
            version,
            replicaUnit: sample.replica_unit,
            demandTarget,
            capacityTarget,
            readyCapacity,
            provisioningCapacity,
            totalCapacity,
            peakInFlight: optionalCount(sample.peak_in_flight),
            peakQueueDepth: optionalCount(sample.peak_queue_depth),
            acceleratorBreakdown: normalizeAcceleratorBreakdown(
              sample.accelerator_breakdown
            ),
          };
        })
        .filter(Boolean)
    : [];
  const requestsLastHour = Number(history.requests_last_hour);
  return {
    available,
    reason: history.reason || null,
    serviceHash: history.service_hash || null,
    bucketSeconds: Number(history.bucket_seconds) || 60,
    retentionHours: Number(history.retention_hours) || 72,
    windowStart: Number(history.window_start) || null,
    windowEnd: Number(history.window_end) || null,
    samples,
    requestSamples,
    predictionTimeHistogramVersion: predictionTimeHistogramSupported
      ? predictionTimeHistogramVersion
      : null,
    predictionTimeBucketUpperBoundsSeconds: predictionTimeHistogramSupported
      ? predictionTimeBucketUpperBoundsSeconds
      : [],
    predictionTimeSamples,
    autoscalerSamples,
    rejectionHistoryAvailable: history.rejection_history_available === true,
    requestWindowSeconds:
      Number(history.request_window_seconds) > 0
        ? Number(history.request_window_seconds)
        : null,
    requestsLastHour:
      available && Number.isInteger(requestsLastHour) && requestsLastHour >= 0
        ? requestsLastHour
        : null,
  };
}

function normalizeAcceleratorCountMap(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return {};
  return Object.fromEntries(
    Object.entries(value).flatMap(([card, rawCount]) => {
      const count = Number(rawCount);
      return typeof card === 'string' && Number.isInteger(count) && count >= 0
        ? [[card, count]]
        : [];
    })
  );
}

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
  const metadataOnly = Boolean(record.metadata_only);
  const replicaStatusCounts = counts ? { ...counts } : null;
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
  const replicaCountsPending = metadataOnly && !counts && !replicas.length;

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

  const costTrackedReplicas = replicas.filter(
    (replica) => !NON_BILLABLE_COST_STATUSES.has(replica.status)
  );
  const pricedReplicas = costTrackedReplicas.filter(
    (replica) => replica.hourlyCost !== null
  );
  const excludedReplicas = costTrackedReplicas.filter(
    (replica) => replica.hourlyCostExclusionReason
  );
  const hourlyCostExclusionReasons = {};
  excludedReplicas.forEach((replica) => {
    const reason = replica.hourlyCostExclusionReason;
    hourlyCostExclusionReasons[reason] =
      (hourlyCostExclusionReasons[reason] || 0) + 1;
  });
  const knownHourlyCost = pricedReplicas.reduce(
    (total, replica) => total + replica.hourlyCost,
    0
  );
  const estimatedHourlyCost = pricedReplicas.length ? knownHourlyCost : null;
  const spotHourlyCost = pricedReplicas
    .filter((replica) => replica.is_spot)
    .reduce((total, replica) => total + replica.hourlyCost, 0);
  const onDemandHourlyCost = pricedReplicas
    .filter((replica) => !replica.is_spot)
    .reduce((total, replica) => total + replica.hourlyCost, 0);
  const hourlyCostExcludedReplicaCount = excludedReplicas.length;
  const rawRequestRate = record.requests_per_second;
  const requestRate =
    rawRequestRate === null || rawRequestRate === undefined
      ? null
      : Number(rawRequestRate);
  const normalizedRequestRate = Number.isFinite(requestRate)
    ? requestRate
    : null;
  const costPerThousandRequests =
    pricedReplicas.length > 0 && normalizedRequestRate > 0
      ? // Lower bound when replicas with unknown prices are excluded: known
        // cloud spend divided by all requests served by the fleet.
        (knownHourlyCost * 1000) / (normalizedRequestRate * 3600)
      : null;
  const acceleratorMaps = {
    hardFloor: normalizeAcceleratorCountMap(record.min_replicas_by_accelerator),
    demandTarget: normalizeAcceleratorCountMap(
      record.demand_target_by_accelerator ??
        record.target_num_replicas_by_accelerator
    ),
    warmRetentionTarget: normalizeAcceleratorCountMap(
      record.warm_retention_target_by_accelerator
    ),
    coldLaunchAuthority: normalizeAcceleratorCountMap(
      record.cold_launch_authority_by_accelerator
    ),
    ready: normalizeAcceleratorCountMap(record.ready_replicas_by_accelerator),
    provisioning: normalizeAcceleratorCountMap(
      record.provisioning_replicas_by_accelerator
    ),
    total: normalizeAcceleratorCountMap(record.total_replicas_by_accelerator),
    zeroCostReady: normalizeAcceleratorCountMap(
      record.zero_cost_ready_replicas_by_accelerator
    ),
    fillTarget: normalizeAcceleratorCountMap(record.fill_target_by_accelerator),
    freeReserved: normalizeAcceleratorCountMap(
      record.free_reserved_slots_by_accelerator
    ),
  };
  const acceleratorCards = [];
  const seenAccelerators = new Set();
  Object.values(acceleratorMaps).forEach((counts) => {
    Object.keys(counts).forEach((card) => {
      const normalized = card.toLowerCase();
      if (seenAccelerators.has(normalized) || normalized === 'unknown') return;
      seenAccelerators.add(normalized);
      acceleratorCards.push(card);
    });
  });
  const acceleratorCapacity = acceleratorCards.map((card) => ({
    card,
    ready: acceleratorMaps.ready[card] || 0,
    provisioning: acceleratorMaps.provisioning[card] || 0,
    total: acceleratorMaps.total[card] || 0,
    demandTarget: acceleratorMaps.demandTarget[card] || 0,
    warmRetentionTarget: Object.hasOwn(
      acceleratorMaps.warmRetentionTarget,
      card
    )
      ? acceleratorMaps.warmRetentionTarget[card]
      : null,
    coldLaunchAuthority: Object.hasOwn(
      acceleratorMaps.coldLaunchAuthority,
      card
    )
      ? acceleratorMaps.coldLaunchAuthority[card]
      : null,
    hardFloor: acceleratorMaps.hardFloor[card] || 0,
    zeroCostReady: acceleratorMaps.zeroCostReady[card] || 0,
    fillTarget: Object.hasOwn(acceleratorMaps.fillTarget, card)
      ? acceleratorMaps.fillTarget[card]
      : null,
    freeReserved: Object.hasOwn(acceleratorMaps.freeReserved, card)
      ? acceleratorMaps.freeReserved[card]
      : null,
  }));

  return {
    name: record.name,
    serviceHash: record.hash || null,
    status: record.status,
    // Epoch timestamp of when the service first became ready (see
    // serve_state.set_service_uptime); the UI renders `now - uptime`.
    uptime: record.uptime ?? null,
    endpoint: record.endpoint || null,
    replicasReady: replicaCountsPending ? null : replicasReady,
    replicasTotal: replicaCountsPending ? null : replicasTotal,
    replicasFailed: replicaCountsPending ? null : replicasFailed,
    replicaUnit: usesLogicalReplicas ? 'logical' : 'physical',
    physicalReplicasReady: replicaCountsPending
      ? null
      : hasAuthoritativePhysicalCounts
        ? record.physical_ready_replicas
        : physicalReplicasReady,
    physicalReplicasTotal: replicaCountsPending
      ? null
      : hasAuthoritativePhysicalCounts
        ? record.physical_total_replicas
        : physicalReplicasTotal,
    physicalReplicasFailed: replicaCountsPending
      ? null
      : hasAuthoritativePhysicalCounts
        ? record.physical_failed_replicas
        : physicalReplicasFailed,
    // True when this record came from a summary_only response: the
    // per-replica list is intentionally absent, not empty.
    summaryOnly: Boolean(counts),
    // The metadata projection intentionally omits all replica-derived fields.
    // Keep that distinct from a real zero-replica summary so the UI can show
    // placeholders until the aggregate response arrives.
    metadataOnly,
    replicaStatusCounts,
    targetReplicas: record.target_num_replicas ?? null,
    acceleratorCapacity,
    fillTarget: record.fill_target ?? null,
    freeReservedSlots: record.fill_free_slots ?? null,
    policy: record.policy || null,
    loadBalancingPolicy: record.load_balancing_policy || null,
    requestedResources: record.requested_resources_str || null,
    activeVersions: record.active_versions || [],
    version: record.version ?? null,
    electedVersion: record.elected_version ?? record.version ?? null,
    tlsEncrypted: Boolean(record.tls_encrypted),
    // User-facing task YAML, redacted server-side (`service_yaml` in
    // _get_service_status); absent on old servers and empty when the
    // controller could not read the stored YAML.
    serviceYaml: record.service_yaml || null,
    estimatedHourlyCost,
    spotHourlyCost,
    onDemandHourlyCost,
    costTrackedReplicaCount: costTrackedReplicas.length,
    pricedReplicaCount: pricedReplicas.length,
    hourlyCostExcludedReplicaCount,
    hourlyCostExclusionReasons,
    recentRequestCount: record.recent_request_count ?? null,
    requestWindowSeconds: record.request_window_seconds ?? null,
    requestRate: normalizedRequestRate,
    inFlightRequests: record.in_flight_requests ?? null,
    requestQueueDepth: record.request_queue_depth ?? null,
    rejectedRequests: record.rejected_requests ?? null,
    requestStatsAgeSeconds: record.request_stats_age_seconds ?? null,
    costPerThousandRequests,
    replicaHistory: normalizeReplicaHistory(record.replica_status_history),
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
    metadataOnly = false,
    includeTargetReplicas,
    historyHours,
    includeEndpoints,
  } = options;
  try {
    const requestBody = {
      service_names: serviceNames,
      summary_only: summaryOnly,
    };
    if (metadataOnly) {
      requestBody.metadata_only = true;
    }
    if (includeTargetReplicas !== undefined) {
      requestBody.include_target_num_replicas = includeTargetReplicas;
    }
    if (historyHours !== undefined) {
      requestBody.history_hours = historyHours;
    }
    if (includeEndpoints !== undefined) {
      requestBody.include_endpoints = includeEndpoints;
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

const SERVICE_HISTORY_SECTIONS = [
  'requests',
  'replicas',
  'prediction',
  'autoscaler',
];

export async function getServiceHistory({
  serviceName,
  serviceHash,
  hours = 1,
  sections = SERVICE_HISTORY_SECTIONS,
}) {
  const params = new URLSearchParams({
    hours: String(hours),
    expected_service_hash: serviceHash,
  });
  sections.forEach((section) => params.append('section', section));
  const response = await apiClient.get(
    `/serve/${encodeURIComponent(serviceName)}/history?${params.toString()}`
  );
  const serverApiVersion = Number(response.headers?.get?.(API_VERSION_HEADER));
  if (response.status === 404) {
    const legacyFallback =
      !Number.isInteger(serverApiVersion) ||
      serverApiVersion < SERVE_DASHBOARD_DIRECT_READS_API_VERSION;
    return {
      available: false,
      reason: legacyFallback ? 'unsupported' : 'not_found',
      legacyFallback,
    };
  }
  if (response.status === 409) {
    const error = new Error('The service incarnation changed.');
    error.code = 'SERVICE_HASH_MISMATCH';
    throw error;
  }
  if (!response.ok) {
    throw new Error(
      `Service history request failed with status ${response.status}`
    );
  }
  const history = normalizeReplicaHistory(await response.json());
  if (!history) {
    throw new Error('Service history response was malformed');
  }
  return {
    ...history,
    legacyFallback: history?.reason === 'non_consolidated',
  };
}

const PAST_ATTEMPT_REPLICA_STATUSES = new Set([
  'FAILED',
  'FAILED_INITIAL_DELAY',
  'FAILED_PROBING',
  'FAILED_PROVISION',
]);

function countReplicaStatuses(counts, statuses) {
  return Object.entries(counts || {}).reduce(
    (total, [status, count]) =>
      statuses.has(status) ? total + Number(count || 0) : total,
    0
  );
}

function totalReplicaCounts(counts) {
  return Object.values(counts || {}).reduce(
    (total, count) => total + Number(count || 0),
    0
  );
}

// Normalize the compact persisted projection returned by
// GET /serve/replica-summaries. Physical row counts and logical planned
// capacity stay separate so callers cannot accidentally label one as the
// other.
export function normalizeServiceReplicaSummary(summary) {
  if (!summary || typeof summary !== 'object') return null;
  const name = summary.service_name;
  const serviceHash = summary.service_hash;
  if (typeof name !== 'string' || !name || typeof serviceHash !== 'string') {
    return null;
  }
  const replicaStatusCounts = summary.replica_status_counts || {};
  const replicaCapacityCounts = summary.replica_capacity_counts || {};
  const usesLogicalReplicas = ['logical', 'logical_slot'].includes(
    summary.replica_unit
  );
  const physicalReplicasReady = Number(replicaStatusCounts.READY || 0);
  const physicalReplicasFailed = countReplicaStatuses(
    replicaStatusCounts,
    FAILED_REPLICA_STATUSES
  );
  const physicalReplicasTotal =
    totalReplicaCounts(replicaStatusCounts) - physicalReplicasFailed;
  const logicalReplicasReady = Number(replicaCapacityCounts.READY || 0);
  const logicalReplicasFailed = countReplicaStatuses(
    replicaCapacityCounts,
    FAILED_REPLICA_STATUSES
  );
  const logicalReplicasTotal =
    totalReplicaCounts(replicaCapacityCounts) - logicalReplicasFailed;
  const currentOrUncertainCount = Number(summary.current_or_uncertain_count);
  const pastAttemptCount = Number(summary.past_attempt_count);
  return {
    name,
    serviceHash,
    replicaUnit: usesLogicalReplicas ? 'logical' : 'physical',
    replicaStatusCounts: { ...replicaStatusCounts },
    replicaCapacityCounts: { ...replicaCapacityCounts },
    replicasReady: usesLogicalReplicas
      ? logicalReplicasReady
      : physicalReplicasReady,
    replicasTotal: usesLogicalReplicas
      ? logicalReplicasTotal
      : physicalReplicasTotal,
    replicasFailed: usesLogicalReplicas
      ? logicalReplicasFailed
      : physicalReplicasFailed,
    physicalReplicasReady,
    physicalReplicasTotal,
    physicalReplicasFailed,
    currentOrUncertainCount: Number.isInteger(currentOrUncertainCount)
      ? currentOrUncertainCount
      : 0,
    pastAttemptCount: Number.isInteger(pastAttemptCount)
      ? pastAttemptCount
      : countReplicaStatuses(
          replicaStatusCounts,
          PAST_ATTEMPT_REPLICA_STATUSES
        ),
  };
}

function directReadFallback(
  response,
  minimumVersion = SERVE_DASHBOARD_DIRECT_READS_API_VERSION
) {
  const serverApiVersion = Number(response.headers?.get?.(API_VERSION_HEADER));
  return (
    !Number.isInteger(serverApiVersion) || serverApiVersion < minimumVersion
  );
}

export async function getServiceReplicaSummaries({ serviceNames = null } = {}) {
  const params = new URLSearchParams();
  (serviceNames || []).forEach((name) => params.append('service_name', name));
  const query = params.toString();
  const response = await apiClient.get(
    `/serve/replica-summaries${query ? `?${query}` : ''}`
  );
  if (response.status === 404) {
    const legacyFallback = directReadFallback(
      response,
      SERVE_DASHBOARD_REPLICA_READS_API_VERSION
    );
    return {
      available: false,
      reason: legacyFallback ? 'unsupported' : 'not_found',
      legacyFallback,
      summaries: [],
    };
  }
  if (response.status === 409) {
    const error = new Error('The service incarnation changed.');
    error.code = 'SERVICE_HASH_MISMATCH';
    throw error;
  }
  if (!response.ok) {
    throw new Error(
      `Service replica summary request failed with status ${response.status}`
    );
  }
  const payload = await response.json();
  if (!payload || typeof payload !== 'object') {
    throw new Error('Service replica summary response was malformed');
  }
  const available = payload.available !== false;
  const observedAt = finiteOrNull(payload.observed_at);
  const summaries = (
    Array.isArray(payload.summaries)
      ? payload.summaries
      : Array.isArray(payload.services)
        ? payload.services
        : []
  )
    .map(normalizeServiceReplicaSummary)
    .filter(Boolean)
    .map((summary) => ({ ...summary, observedAt }));
  return {
    available,
    reason: payload.reason || null,
    legacyFallback: !available && payload.reason === 'non_consolidated',
    observedAt,
    summaries,
  };
}

export async function getServiceReplicas({
  serviceName,
  serviceHash,
  scope,
  limit = 50,
  cursor = null,
}) {
  const params = new URLSearchParams({
    scope,
    limit: String(limit),
    expected_service_hash: serviceHash,
  });
  if (cursor) params.set('cursor', cursor);
  const response = await apiClient.get(
    `/serve/${encodeURIComponent(serviceName)}/replicas?${params.toString()}`
  );
  if (response.status === 404) {
    const legacyFallback = directReadFallback(
      response,
      SERVE_DASHBOARD_REPLICA_READS_API_VERSION
    );
    return {
      available: false,
      reason: legacyFallback ? 'unsupported' : 'not_found',
      legacyFallback,
      replicas: [],
      total: 0,
      nextCursor: null,
    };
  }
  if (response.status === 409) {
    const error = new Error('The service incarnation changed.');
    error.code = 'SERVICE_HASH_MISMATCH';
    throw error;
  }
  if (!response.ok) {
    throw new Error(
      `Service replicas request failed with status ${response.status}`
    );
  }
  const payload = await response.json();
  if (!payload || typeof payload !== 'object') {
    throw new Error('Service replicas response was malformed');
  }
  const available = payload.available !== false;
  const total = Number(payload.total);
  return {
    available,
    reason: payload.reason || null,
    legacyFallback: !available && payload.reason === 'non_consolidated',
    serviceName: payload.service_name || serviceName,
    serviceHash: payload.service_hash || serviceHash,
    replicaUnit: ['logical', 'logical_slot'].includes(payload.replica_unit)
      ? 'logical'
      : 'physical',
    scope: payload.scope || scope,
    observedAt: finiteOrNull(payload.observed_at),
    total: Number.isInteger(total) && total >= 0 ? total : 0,
    nextCursor: payload.next_cursor || null,
    replicas: (Array.isArray(payload.replicas) ? payload.replicas : []).map(
      (replica) => ({ ...normalizeReplica(replica), directProjection: true })
    ),
  };
}

function finiteOrNull(value) {
  if (value === null || value === undefined) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

export function normalizeServicePlacement(payload) {
  const placer = payload?.placer_state || {};
  const capacity = payload?.capacity_hints || {};
  const history = payload?.history || {};
  return {
    serviceName: payload?.service_name || null,
    placerState: {
      available: placer.available !== false,
      reason: placer.reason || null,
      enabled: placer.enabled === true,
      retrySeconds: finiteOrNull(placer.retry_seconds),
      observedAt: finiteOrNull(placer.observed_at),
      statusSemantics: placer.status_semantics || null,
      truncated: placer.truncated === true,
      locations: Array.isArray(placer.locations)
        ? placer.locations.map((location) => ({
            cloud: location.cloud || 'Unknown',
            region: location.region || null,
            zone: location.zone || null,
            instanceType: location.instance_type || null,
            accelerators: location.accelerators || null,
            useSpot: location.use_spot === true,
            storedStatus: location.stored_status || null,
            effectiveStatus: location.effective_status || null,
            benchReason: location.bench_reason || null,
            probeEligible: location.probe_eligible === true,
            benchedAt: finiteOrNull(location.benched_at),
            nextProbeAt: finiteOrNull(location.next_probe_at),
            cachedHourlyCost: finiteOrNull(location.cached_hourly_cost),
            paidAdmission: location.paid_admission
              ? {
                  state: location.paid_admission.state || null,
                  poolRemaining: finiteOrNull(
                    location.paid_admission.pool_remaining
                  ),
                  serviceRemaining: finiteOrNull(
                    location.paid_admission.service_remaining
                  ),
                  cooldownUntil: finiteOrNull(
                    location.paid_admission.cooldown_until
                  ),
                }
              : null,
          }))
        : [],
    },
    capacityHints: {
      available: capacity.available !== false,
      reason: capacity.reason || null,
      truncated: capacity.truncated === true,
      hints: Array.isArray(capacity.hints)
        ? capacity.hints.map((hint) => ({
            kind: hint.kind || 'capacity',
            cloud: hint.cloud || null,
            region: hint.region || null,
            zone: hint.zone || null,
            instanceType: hint.instance_type || null,
            accelerators: hint.accelerators || null,
            numNodes: finiteOrNull(hint.num_nodes),
            observedAt: finiteOrNull(hint.observed_at),
            expiresAt: finiteOrNull(hint.expires_at),
          }))
        : [],
    },
    history: {
      available: history.available !== false,
      reason: history.reason || null,
      retentionHours: finiteOrNull(history.retention_hours) || 24,
      windowStart: finiteOrNull(history.window_start),
      windowEnd: finiteOrNull(history.window_end),
      outcomeCounts: history.outcome_counts || {},
      nextCursor: history.next_cursor || null,
      events: Array.isArray(history.events)
        ? history.events.map((event) => ({
            eventId: event.event_id,
            requestId: event.request_id,
            replicaId: event.replica_id,
            clusterName: event.cluster_name,
            attemptOrdinal: event.attempt_ordinal,
            observedAt: finiteOrNull(event.observed_at),
            outcome: event.outcome,
            provider: event.provider,
            region: event.region,
            zone: event.zone,
            instanceType: event.instance_type,
            accelerators: event.accelerators || null,
            useSpot: event.use_spot === true,
            numNodes: finiteOrNull(event.num_nodes),
            hourlyPrice: finiteOrNull(event.hourly_price),
            priceSource: event.price_source || null,
            errorCode: event.error_code || null,
            errorSummary: event.error_summary || null,
          }))
        : [],
    },
  };
}

export async function getServicePlacement({
  serviceName,
  hours = 24,
  limit = 50,
  cursor = null,
}) {
  const response = await apiClient.post('/serve/placement', {
    service_name: serviceName,
    hours,
    limit,
    cursor,
  });
  if (!response.ok) {
    const error = new Error(
      `Failed to request service placement (${response.status})`
    );
    error.status = response.status;
    throw error;
  }
  const requestId = response.headers.get('X-Skypilot-Request-ID');
  if (!requestId) {
    throw new Error('No request ID received for service placement');
  }
  const result = await apiClient.get(`/api/get?request_id=${requestId}`);
  if (!result.ok) {
    const error = new Error(
      `Failed to fetch service placement (${result.status})`
    );
    error.status = result.status;
    throw error;
  }
  const envelope = await result.json();
  const payload = envelope.return_value
    ? JSON.parse(envelope.return_value)
    : {};
  return normalizeServicePlacement(payload);
}

async function parseImmediateResponse(response, fallback) {
  if (response.ok) return response.json();
  let detail;
  try {
    const payload = await response.json();
    detail = payload?.detail;
  } catch (_) {
    // Use the status-based fallback below.
  }
  const error = new Error(detail || `${fallback} (${response.status})`);
  error.status = response.status;
  throw error;
}

export async function getServiceVersions(serviceName) {
  const response = await apiClient.get(
    `/serve/${encodeURIComponent(serviceName)}/versions`
  );
  return parseImmediateResponse(response, 'Failed to fetch service versions');
}

export async function electServiceVersion(serviceName, version) {
  return apiClient.fetch(
    `/serve/${encodeURIComponent(serviceName)}/versions/elect`,
    { version }
  );
}
