// Mock the shared API client so we can exercise the connector's
// normalization logic without hitting the network.
jest.mock('@/data/connectors/client', () => ({
  __esModule: true,
  apiClient: {
    fetch: jest.fn(),
    post: jest.fn(),
    get: jest.fn(),
  },
}));

import { apiClient } from '@/data/connectors/client';
import {
  electServiceVersion,
  getServiceHistory,
  getServicePricing,
  getServiceReplicaSummaries,
  getServiceReplicas,
  getServicePlacement,
  getServiceVersions,
  getServices,
  normalizeAcceleratorBreakdown,
  normalizeReplicaHistory,
  normalizeService,
  normalizeServiceReplicaSummary,
  normalizeServicePlacement,
  normalizeReplica,
  normalizeServicePricing,
} from '@/data/connectors/services';

const REQUEST_ID = 'req-123';

describe('normalizeAcceleratorBreakdown capacity semantics', () => {
  const legacyBreakdown = {
    version: 1,
    configured_accelerators: ['L4'],
    min_replicas: { L4: 1 },
    demand_target: { L4: 2 },
    ready_capacity: { L4: 1 },
    provisioning_capacity: { L4: 1 },
    total_capacity: { L4: 2 },
    zero_cost_ready_capacity: { L4: 0 },
    fill_target: { L4: 0 },
    free_reserved_slots: { L4: 0 },
  };

  it('keeps schema version 1 while optionally exposing capacity semantics v2', () => {
    expect(normalizeAcceleratorBreakdown(legacyBreakdown)).not.toHaveProperty(
      'capacitySemanticsVersion'
    );
    expect(
      normalizeAcceleratorBreakdown({
        ...legacyBreakdown,
        capacity_semantics_version: 2,
      })
    ).toHaveProperty('capacitySemanticsVersion', 2);
    expect(
      normalizeAcceleratorBreakdown({
        ...legacyBreakdown,
        version: 2,
        capacity_semantics_version: 2,
      })
    ).toBeNull();
  });
});

function mockDispatchResponse() {
  return {
    ok: true,
    status: 200,
    headers: { get: (name) => (name ? REQUEST_ID : null) },
  };
}

function mockResultResponse(returnValue) {
  return {
    ok: true,
    status: 200,
    json: async () => ({ return_value: JSON.stringify(returnValue) }),
  };
}

// A raw service record as returned by POST /serve/status after the
// server-side `encode_serve_status` (statuses are plain enum-value strings).
function rawServiceRecord(overrides = {}) {
  return {
    name: 'boltz-l4-fleet',
    hash: 'service-hash-a',
    status: 'READY',
    uptime: 1751600000,
    endpoint: 'http://10.0.0.1:30001',
    policy: 'autoscaling(min=1,max=4)',
    requested_resources_str: '1x[L4:1]',
    load_balancing_policy: 'round_robin',
    tls_encrypted: false,
    active_versions: [2],
    version: 2,
    target_num_replicas: 2,
    recent_request_count: 30,
    request_window_seconds: 60,
    requests_per_second: 0.5,
    in_flight_requests: 2,
    request_queue_depth: 1,
    rejected_requests: 3,
    replica_info: [
      {
        replica_id: 1,
        name: 'boltz-l4-fleet-1',
        status: 'READY',
        version: 2,
        endpoint: 'http://10.0.0.2:8000',
        is_spot: false,
        launched_at: 1751590000,
        ready_at: 1751590125,
        time_to_ready_seconds: 180,
        cloud: 'AWS',
        region: 'us-east-1',
        infra: 'AWS (us-east-1)',
        resources_str: '1x(gpus=L4:1)',
        resources_str_full: '1x(gpus=L4:1, cpus=4, mem=16)',
        hourly_cost: 1.25,
        hourly_cost_exclusion_reason: null,
        handle: 'opaque-encoded-handle',
      },
    ],
    ...overrides,
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('getServices', () => {
  it('fetches /serve/status for all services and normalizes records', async () => {
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([rawServiceRecord()]));

    const { services, controllerStopped } = await getServices();

    expect(apiClient.post).toHaveBeenCalledWith('/serve/status', {
      service_names: null,
      summary_only: false,
    });
    expect(apiClient.get).toHaveBeenCalledWith(
      `/api/get?request_id=${REQUEST_ID}`
    );

    expect(controllerStopped).toBe(false);
    expect(services).toHaveLength(1);
    const service = services[0];
    expect(service).toMatchObject({
      name: 'boltz-l4-fleet',
      serviceHash: 'service-hash-a',
      status: 'READY',
      uptime: 1751600000,
      endpoint: 'http://10.0.0.1:30001',
      replicasReady: 1,
      replicasTotal: 1,
      targetReplicas: 2,
      policy: 'autoscaling(min=1,max=4)',
      loadBalancingPolicy: 'round_robin',
      requestedResources: '1x[L4:1]',
      activeVersions: [2],
      estimatedHourlyCost: 1.25,
      requestRate: 0.5,
      recentRequestCount: 30,
      inFlightRequests: 2,
      requestQueueDepth: 1,
      rejectedRequests: 3,
      costPerThousandRequests: 0.6944444444444444,
    });
    expect(service.replicas).toHaveLength(1);
    expect(service.replicas[0]).toMatchObject({
      id: 1,
      status: 'READY',
      version: 2,
      endpoint: 'http://10.0.0.2:8000',
      launched_at: 1751590000,
      ready_at: 1751590125,
      timeToReadySeconds: 180,
      region: 'us-east-1',
      resources_str: '1x(gpus=L4:1)',
      resources_str_full: '1x(gpus=L4:1, cpus=4, mem=16)',
      plannedCapacity: 1,
      hourlyCost: 1.25,
    });
    // The pickled handle blob must not leak into the normalized replica.
    expect(service.replicas[0].handle).toBeUndefined();
  });

  it('returns an empty list when there are no services', async () => {
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([]));

    const result = await getServices();

    expect(result).toEqual({ services: [], controllerStopped: false });
  });

  it('counts ready replicas with mixed replica statuses, excluding failed ones from the total like `sky serve status`', async () => {
    // Mirrors _get_replicas in sky/serve/serve_utils.py: READY counts
    // toward ready, and statuses in ReplicaStatus.failed_statuses()
    // (here FAILED_PROBING) are excluded from the total.
    const record = rawServiceRecord({
      status: 'REPLICA_INIT',
      replica_info: [
        { replica_id: 1, status: 'READY' },
        { replica_id: 2, status: 'PROVISIONING' },
        { replica_id: 3, status: 'READY' },
        { replica_id: 4, status: 'FAILED_PROBING' },
        { replica_id: 5, status: 'NOT_READY' },
      ],
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices();

    expect(services[0].replicasReady).toBe(2);
    expect(services[0].replicasTotal).toBe(4);
    expect(services[0].replicasFailed).toBe(1);
    // The full replica list still contains every row, failed included.
    expect(services[0].replicas.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
  });

  it('shows 2/3 when one of four replicas is FAILED (CLI-consistent denominator)', async () => {
    const record = rawServiceRecord({
      replica_info: [
        { replica_id: 1, status: 'READY' },
        { replica_id: 2, status: 'READY' },
        { replica_id: 3, status: 'NOT_READY' },
        { replica_id: 4, status: 'FAILED' },
      ],
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices();

    expect(services[0].replicasReady).toBe(2);
    expect(services[0].replicasTotal).toBe(3);
    expect(services[0].replicasFailed).toBe(1);
  });

  it('uses authoritative logical capacity while retaining physical backend counts', async () => {
    const record = rawServiceRecord({
      replica_unit: 'logical_slot',
      ready_replicas: 8,
      total_replicas: 12,
      failed_replicas: 4,
      physical_ready_replicas: 1,
      physical_total_replicas: 2,
      physical_failed_replicas: 1,
      observed_ready_replicas: 20,
      observed_ready_replicas_fresh: false,
      request_stats_age_seconds: 700,
      replica_info: [
        { replica_id: 1, status: 'READY', planned_capacity: 8 },
        { replica_id: 2, status: 'PROVISIONING', planned_capacity: 4 },
        { replica_id: 3, status: 'FAILED_PROVISION', planned_capacity: 4 },
      ],
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices();

    expect(services[0]).toMatchObject({
      replicaUnit: 'logical',
      replicasReady: 8,
      replicasTotal: 12,
      replicasFailed: 4,
      physicalReplicasReady: 1,
      physicalReplicasTotal: 2,
      physicalReplicasFailed: 1,
      observedReadyReplicas: 20,
      observedReadyReplicasFresh: false,
      requestStatsAgeSeconds: 700,
    });
    expect(
      services[0].replicas.map((replica) => replica.plannedCapacity)
    ).toEqual([8, 4, 4]);
  });

  it('sums per-backend widths for a logical full response without aggregates', () => {
    const service = normalizeService(
      rawServiceRecord({
        replica_unit: 'logical',
        replica_info: [
          { replica_id: 1, status: 'READY', planned_capacity: 8 },
          { replica_id: 2, status: 'PROVISIONING', planned_capacity: 4 },
          { replica_id: 3, status: 'FAILED_PROBING', planned_capacity: 2 },
        ],
      })
    );

    expect(service).toMatchObject({
      replicaUnit: 'logical',
      replicasReady: 8,
      replicasTotal: 12,
      replicasFailed: 2,
      physicalReplicasReady: 1,
      physicalReplicasTotal: 2,
      physicalReplicasFailed: 1,
    });
  });

  it('does not guess observation freshness for an older server', () => {
    const service = normalizeService(
      rawServiceRecord({
        replica_unit: 'logical',
        observed_ready_replicas: 8,
      })
    );

    expect(service.observedReadyReplicas).toBe(8);
    expect(service.observedReadyReplicasFresh).toBeNull();
  });

  it('excludes every failed-class replica status from the total', async () => {
    // The full ReplicaStatus.failed_statuses() set from
    // sky/serve/serve_state.py.
    const failedStatuses = [
      'FAILED',
      'FAILED_CLEANUP',
      'FAILED_INITIAL_DELAY',
      'FAILED_PROBING',
      'FAILED_PROVISION',
      'UNKNOWN',
    ];
    const record = rawServiceRecord({
      replica_info: [
        { replica_id: 1, status: 'READY' },
        ...failedStatuses.map((status, i) => ({
          replica_id: i + 2,
          status,
        })),
      ],
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices();

    expect(services[0].replicasReady).toBe(1);
    expect(services[0].replicasTotal).toBe(1);
    expect(services[0].replicasFailed).toBe(failedStatuses.length);
  });

  it('passes service_names and summary_only through to /serve/status', async () => {
    // Summary responses have no replica_info; the server sends
    // replica_status_counts instead (SERVE_VERSION >= 6).
    const record = rawServiceRecord({
      replica_info: undefined,
      replica_status_counts: {
        READY: 3,
        PROVISIONING: 2,
        FAILED_PROBING: 1,
      },
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices({
      serviceNames: ['boltz-l4-fleet'],
      summaryOnly: true,
    });

    expect(apiClient.post).toHaveBeenCalledWith('/serve/status', {
      service_names: ['boltz-l4-fleet'],
      summary_only: true,
    });
    expect(services[0].summaryOnly).toBe(true);
    expect(services[0].replicas).toEqual([]);
    expect(services[0].replicasReady).toBe(3);
    // Failed-class statuses are excluded from the total, matching the
    // replica_info-based computation.
    expect(services[0].replicasTotal).toBe(5);
    expect(services[0].replicasFailed).toBe(1);
  });

  it('keeps metadata-only replica fields pending instead of inventing zeroes', async () => {
    const record = rawServiceRecord({
      metadata_only: true,
      endpoint: null,
      replica_info: undefined,
      replica_status_counts: undefined,
      target_num_replicas: undefined,
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices({ metadataOnly: true });

    expect(apiClient.post).toHaveBeenCalledWith('/serve/status', {
      service_names: null,
      summary_only: false,
      metadata_only: true,
    });
    expect(services[0]).toMatchObject({
      name: 'boltz-l4-fleet',
      status: 'READY',
      metadataOnly: true,
      summaryOnly: false,
      replicasReady: null,
      replicasTotal: null,
      replicasFailed: null,
      replicaStatusCounts: null,
    });
  });

  it('opts summary requests into deferred endpoint hydration', async () => {
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(
      mockResultResponse([
        rawServiceRecord({
          replica_info: undefined,
          replica_status_counts: { READY: 1 },
        }),
      ])
    );

    await getServices({ summaryOnly: true, includeEndpoints: true });

    expect(apiClient.post).toHaveBeenCalledWith('/serve/status', {
      service_names: null,
      summary_only: true,
      include_endpoints: true,
    });
  });

  it('requests and normalizes aggregate replica history', async () => {
    const record = rawServiceRecord({
      replica_status_history: {
        available: true,
        bucket_seconds: 60,
        retention_hours: 72,
        window_start: 1751590000,
        window_end: 1751633200,
        rejection_history_available: true,
        request_samples: [
          {
            timestamp: 1751633160,
            request_count: 9,
            rejected_count: 2,
          },
        ],
        prediction_time_histogram_version: 1,
        prediction_time_bucket_upper_bounds_seconds: [0.1, 1, 10],
        prediction_time_samples: [
          {
            timestamp: 1751633160,
            outcome_counts: {
              succeeded: [1, 2, 3, 4],
              failed: [0, 0, 1, 0],
            },
          },
        ],
        autoscaler_samples: [
          {
            timestamp: 1751633160,
            observed_at: 1751633175,
            controller_session_id: 'a'.repeat(32),
            version: 2,
            replica_unit: 'physical_backend',
            demand_target: 4,
            capacity_target: 8,
            ready_capacity: 6,
            provisioning_capacity: 2,
            total_capacity: 9,
            peak_in_flight: 5,
            peak_queue_depth: 3,
            accelerator_breakdown: {
              version: 1,
              capacity_semantics_version: 2,
              configured_accelerators: ['A100', 'A100-80GB'],
              min_replicas: { A100: 1, 'A100-80GB': 0 },
              demand_target: { A100: 3, 'A100-80GB': 1 },
              warm_retention_target: { A100: 2, 'A100-80GB': 0 },
              cold_launch_authority: { A100: 0, 'A100-80GB': 1 },
              ready_capacity: { A100: 4, 'A100-80GB': 2 },
              provisioning_capacity: { A100: 1, 'A100-80GB': 1 },
              total_capacity: { A100: 6, 'A100-80GB': 3 },
              zero_cost_ready_capacity: { A100: 2, 'A100-80GB': 1 },
              fill_target: { A100: 5, 'A100-80GB': 0 },
              free_reserved_slots: { A100: 1, 'A100-80GB': 0 },
            },
          },
        ],
        samples: [
          {
            timestamp: 1751633160,
            observed_at: 1751633170,
            version: 2,
            ready_count: 3,
            ready_reserved_count: 1,
            provisioning_count: 1,
            not_ready_count: 0,
            errored_count: 2,
            preempted_count: 0,
            stopping_count: 1,
            total_count: 7,
            logical_ready_count: 24,
            logical_ready_reserved_count: 8,
            logical_provisioning_count: 8,
            logical_not_ready_count: 0,
            logical_errored_count: 16,
            logical_preempted_count: 0,
            logical_stopping_count: 8,
            logical_total_count: 56,
          },
        ],
      },
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices({
      serviceNames: ['boltz-l4-fleet'],
      summaryOnly: true,
      historyHours: 12,
    });

    expect(apiClient.post).toHaveBeenCalledWith('/serve/status', {
      service_names: ['boltz-l4-fleet'],
      summary_only: true,
      history_hours: 12,
    });
    expect(services[0].replicaHistory).toMatchObject({
      available: true,
      bucketSeconds: 60,
      retentionHours: 72,
      samples: [
        {
          timestamp: 1751633160,
          version: 2,
          readyCount: 3,
          readyReservedCount: 1,
          provisioningCount: 1,
          erroredCount: 2,
          stoppingCount: 1,
          totalCount: 7,
          logicalReadyCount: 24,
          logicalReadyReservedCount: 8,
          logicalProvisioningCount: 8,
          logicalErroredCount: 16,
          logicalStoppingCount: 8,
          logicalTotalCount: 56,
        },
      ],
      requestSamples: [
        { timestamp: 1751633160, requestCount: 9, rejectedCount: 2 },
      ],
      predictionTimeHistogramVersion: 1,
      predictionTimeBucketUpperBoundsSeconds: [0.1, 1, 10],
      predictionTimeSamples: [
        {
          timestamp: 1751633160,
          outcomeCounts: {
            succeeded: [1, 2, 3, 4],
            failed: [0, 0, 1, 0],
          },
        },
      ],
      rejectionHistoryAvailable: true,
      autoscalerSamples: [
        {
          timestamp: 1751633160,
          observedAt: 1751633175,
          controllerSessionId: 'a'.repeat(32),
          version: 2,
          replicaUnit: 'physical_backend',
          demandTarget: 4,
          capacityTarget: 8,
          readyCapacity: 6,
          provisioningCapacity: 2,
          totalCapacity: 9,
          peakInFlight: 5,
          peakQueueDepth: 3,
          acceleratorBreakdown: {
            capacitySemanticsVersion: 2,
            configuredAccelerators: ['A100', 'A100-80GB'],
            minReplicas: { A100: 1, 'A100-80GB': 0 },
            demandTarget: { A100: 3, 'A100-80GB': 1 },
            warmRetentionTarget: { A100: 2, 'A100-80GB': 0 },
            coldLaunchAuthority: { A100: 0, 'A100-80GB': 1 },
            readyCapacity: { A100: 4, 'A100-80GB': 2 },
            provisioningCapacity: { A100: 1, 'A100-80GB': 1 },
            totalCapacity: { A100: 6, 'A100-80GB': 3 },
            zeroCostReadyCapacity: { A100: 2, 'A100-80GB': 1 },
            fillTarget: { A100: 5, 'A100-80GB': 0 },
            freeReservedSlots: { A100: 1, 'A100-80GB': 0 },
          },
        },
      ],
    });
  });

  it('keeps logical summary aggregates distinct from the physical status histogram', async () => {
    const record = rawServiceRecord({
      replica_unit: 'logical_slot',
      ready_replicas: 8,
      total_replicas: 12,
      failed_replicas: 4,
      physical_ready_replicas: 1,
      physical_total_replicas: 2,
      physical_failed_replicas: 1,
      replica_info: undefined,
      replica_status_counts: {
        READY: 1,
        PROVISIONING: 1,
        FAILED_PROBING: 1,
      },
    });
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse([record]));

    const { services } = await getServices({ summaryOnly: true });

    expect(services[0]).toMatchObject({
      summaryOnly: true,
      replicasReady: 8,
      replicasTotal: 12,
      replicasFailed: 4,
      physicalReplicasReady: 1,
      physicalReplicasTotal: 2,
      physicalReplicasFailed: 1,
    });
  });

  it('passes include_target_num_replicas through when requested', async () => {
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(
      mockResultResponse([
        rawServiceRecord({
          target_num_replicas: 7,
        }),
      ])
    );

    const { services } = await getServices({
      serviceNames: ['boltz-l4-fleet'],
      summaryOnly: true,
      includeTargetReplicas: true,
    });

    expect(apiClient.post).toHaveBeenCalledWith('/serve/status', {
      service_names: ['boltz-l4-fleet'],
      summary_only: true,
      include_target_num_replicas: true,
    });
    expect(services[0].targetReplicas).toBe(7);
  });

  it('reports controllerStopped when the serve controller is not up', async () => {
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue({
      ok: false,
      status: 500,
      statusText: 'Internal Server Error',
      json: async () => ({
        detail: {
          error: JSON.stringify({
            type: 'ClusterNotUpError',
            message: 'controller is down',
          }),
        },
      }),
    });

    const result = await getServices();

    expect(result).toEqual({ services: [], controllerStopped: true });
  });
});

describe('getServiceHistory', () => {
  it('requests one bounded range and normalizes the direct response', async () => {
    apiClient.get.mockResolvedValue({
      ok: true,
      status: 200,
      headers: { get: () => '66' },
      json: async () => ({
        available: true,
        service_hash: 'hash/a',
        bucket_seconds: 60,
        retention_hours: 72,
        window_start: 0,
        window_end: 3600,
        samples: [],
        request_samples: [],
        prediction_time_samples: [],
        autoscaler_samples: [],
      }),
    });

    const history = await getServiceHistory({
      serviceName: 'boltz/l4',
      serviceHash: 'hash/a',
      hours: 1,
    });

    expect(apiClient.get).toHaveBeenCalledWith(
      '/serve/boltz%2Fl4/history?hours=1&expected_service_hash=hash%2Fa&section=requests&section=replicas&section=prediction&section=autoscaler'
    );
    expect(history).toMatchObject({
      available: true,
      serviceHash: 'hash/a',
      legacyFallback: false,
    });
  });

  it('uses legacy fallback only when a 404 comes from an older server', async () => {
    apiClient.get.mockResolvedValueOnce({
      ok: false,
      status: 404,
      headers: { get: () => '65' },
    });
    await expect(
      getServiceHistory({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        hours: 1,
      })
    ).resolves.toMatchObject({
      available: false,
      reason: 'unsupported',
      legacyFallback: true,
    });

    apiClient.get.mockResolvedValueOnce({
      ok: false,
      status: 404,
      headers: { get: () => '66' },
    });
    await expect(
      getServiceHistory({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        hours: 1,
      })
    ).resolves.toMatchObject({
      available: false,
      reason: 'not_found',
      legacyFallback: false,
    });
  });

  it('surfaces a service-incarnation conflict distinctly', async () => {
    apiClient.get.mockResolvedValue({
      ok: false,
      status: 409,
      headers: { get: () => '66' },
    });

    await expect(
      getServiceHistory({
        serviceName: 'svc',
        serviceHash: 'old-hash',
        hours: 12,
      })
    ).rejects.toMatchObject({ code: 'SERVICE_HASH_MISMATCH' });
  });

  it('rejects a malformed successful response', async () => {
    apiClient.get.mockResolvedValue({
      ok: true,
      status: 200,
      headers: { get: () => '66' },
      json: async () => null,
    });

    await expect(
      getServiceHistory({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        hours: 1,
      })
    ).rejects.toThrow('Service history response was malformed');
  });
});

describe('direct replica projections', () => {
  function directResponse(payload, { status = 200, apiVersion = '66' } = {}) {
    return {
      ok: status >= 200 && status < 300,
      status,
      headers: { get: () => apiVersion },
      json: async () => payload,
    };
  }

  it('normalizes physical rows separately from logical capacity', () => {
    expect(
      normalizeServiceReplicaSummary({
        service_name: 'svc',
        service_hash: 'hash-a',
        service_status: 'READY',
        service_uptime: 1751600000,
        service_policy: 'autoscaling(min=0,max=8)',
        requested_resources_str: '1x[L4:4]',
        replica_unit: 'logical_slot',
        replica_status_counts: {
          READY: 1,
          PROVISIONING: 1,
          FAILED_PROVISION: 2,
        },
        replica_capacity_counts: {
          READY: 8,
          PROVISIONING: 4,
          FAILED_PROVISION: 12,
        },
        current_or_uncertain_count: 2,
        past_attempt_count: 2,
      })
    ).toMatchObject({
      name: 'svc',
      serviceHash: 'hash-a',
      persistedMetadataLoaded: true,
      status: 'READY',
      uptime: 1751600000,
      policy: 'autoscaling(min=0,max=8)',
      requestedResources: '1x[L4:4]',
      replicaUnit: 'logical',
      replicasReady: 8,
      replicasTotal: 12,
      replicasFailed: 12,
      physicalReplicasReady: 1,
      physicalReplicasTotal: 2,
      physicalReplicasFailed: 2,
      currentOrUncertainCount: 2,
      pastAttemptCount: 2,
    });
  });

  it('requests batched summaries with repeated service names', async () => {
    apiClient.get.mockResolvedValue(
      directResponse({
        available: true,
        service_metadata_included: true,
        observed_at: 123,
        summaries: [
          {
            service_name: 'svc-a',
            service_hash: 'hash-a',
            service_status: 'READY',
            service_uptime: 1751600000,
            service_policy: 'autoscaling(min=0,max=1)',
            requested_resources_str: '1x[L4:1]',
            replica_unit: 'physical',
            replica_status_counts: { READY: 1 },
            replica_capacity_counts: { READY: 1 },
            current_or_uncertain_count: 1,
            past_attempt_count: 0,
          },
        ],
      })
    );

    const result = await getServiceReplicaSummaries({
      serviceNames: ['svc-a', 'svc-b'],
    });

    expect(apiClient.get).toHaveBeenCalledWith(
      '/serve/replica-summaries?service_name=svc-a&service_name=svc-b'
    );
    expect(result.summaries[0]).toMatchObject({
      name: 'svc-a',
      serviceHash: 'hash-a',
      persistedMetadataLoaded: true,
      observedAt: 123,
      replicasReady: 1,
    });
    expect(result.serviceMetadataIncluded).toBe(true);
  });

  it('capability-gates summary 404s and topology fallback', async () => {
    apiClient.get.mockResolvedValueOnce(
      directResponse({}, { status: 404, apiVersion: '66' })
    );
    await expect(getServiceReplicaSummaries()).resolves.toMatchObject({
      reason: 'unsupported',
      legacyFallback: true,
    });

    apiClient.get.mockResolvedValueOnce(
      directResponse({}, { status: 404, apiVersion: '67' })
    );
    await expect(getServiceReplicaSummaries()).resolves.toMatchObject({
      reason: 'not_found',
      legacyFallback: false,
    });

    apiClient.get.mockResolvedValueOnce(
      directResponse({ available: false, reason: 'non_consolidated' })
    );
    await expect(getServiceReplicaSummaries()).resolves.toMatchObject({
      reason: 'non_consolidated',
      legacyFallback: true,
    });
  });

  it('requests a bounded replica page and marks omitted enrichment', async () => {
    apiClient.get.mockResolvedValue(
      directResponse({
        available: true,
        service_name: 'svc',
        service_hash: 'hash-a',
        scope: 'current_or_uncertain',
        replica_unit: 'physical',
        observed_at: 200,
        total: 2,
        next_cursor: 'cursor-2',
        replicas: [
          {
            replica_id: 7,
            pricing_fingerprint: 'fp-7',
            status: 'FAILED_CLEANUP',
            version: 3,
            created_at: 190,
          },
        ],
      })
    );

    const result = await getServiceReplicas({
      serviceName: 'svc/name',
      serviceHash: 'hash-a',
      scope: 'current_or_uncertain',
      limit: 50,
      cursor: 'cursor-1',
    });

    expect(apiClient.get).toHaveBeenCalledWith(
      '/serve/svc%2Fname/replicas?scope=current_or_uncertain&limit=50&expected_service_hash=hash-a&cursor=cursor-1'
    );
    expect(result).toMatchObject({
      available: true,
      serviceHash: 'hash-a',
      total: 2,
      nextCursor: 'cursor-2',
      replicas: [
        {
          id: 7,
          pricingFingerprint: 'fp-7',
          status: 'FAILED_CLEANUP',
          createdAt: 190,
          directProjection: true,
          launched_at: null,
        },
      ],
    });
  });

  it('surfaces page hash conflicts and only falls back on old-server 404s', async () => {
    apiClient.get.mockResolvedValueOnce(
      directResponse({}, { status: 409, apiVersion: '66' })
    );
    await expect(
      getServiceReplicas({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        scope: 'past_attempts',
      })
    ).rejects.toMatchObject({ code: 'SERVICE_HASH_MISMATCH' });

    apiClient.get.mockResolvedValueOnce(
      directResponse({}, { status: 404, apiVersion: '66' })
    );
    await expect(
      getServiceReplicas({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        scope: 'past_attempts',
      })
    ).resolves.toMatchObject({
      reason: 'unsupported',
      legacyFallback: true,
    });

    apiClient.get.mockResolvedValueOnce(
      directResponse({}, { status: 404, apiVersion: '67' })
    );
    await expect(
      getServiceReplicas({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        scope: 'past_attempts',
      })
    ).resolves.toMatchObject({
      reason: 'not_found',
      legacyFallback: false,
    });

    apiClient.get.mockResolvedValueOnce(
      directResponse({ available: false, reason: 'non_consolidated' })
    );
    await expect(
      getServiceReplicas({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        scope: 'past_attempts',
      })
    ).resolves.toMatchObject({
      reason: 'non_consolidated',
      legacyFallback: true,
    });
  });
});

describe('persisted service pricing', () => {
  function pricingResponse(payload, { status = 200, apiVersion = '71' } = {}) {
    return {
      ok: status >= 200 && status < 300,
      status,
      headers: { get: () => apiVersion },
      json: async () => payload,
    };
  }

  function aggregatePayload(overrides = {}) {
    return {
      available: true,
      service_name: 'svc/name',
      service_hash: 'hash/a',
      observed_at: 123,
      price_basis: 'version_catalog',
      aggregate: {
        available: true,
        unavailable_reason: null,
        coverage: 'partial',
        known_hourly_cost: 1.5,
        spot_hourly_cost: 0.5,
        non_spot_hourly_cost: 1,
        tracked_replica_count: 3,
        priced_replica_count: 2,
        excluded_replica_count: 1,
        exclusion_reasons: { missing_version_catalog: 1 },
      },
      replicas: [],
      ...overrides,
    };
  }

  it('requests and normalizes an aggregate-only version-catalog projection', async () => {
    apiClient.get.mockResolvedValue(pricingResponse(aggregatePayload()));

    const result = await getServicePricing({
      serviceName: 'svc/name',
      serviceHash: 'hash/a',
    });

    expect(apiClient.get).toHaveBeenCalledWith(
      '/serve/svc%2Fname/pricing?expected_service_hash=hash%2Fa'
    );
    expect(result).toMatchObject({
      available: true,
      serviceName: 'svc/name',
      serviceHash: 'hash/a',
      priceBasis: 'version_catalog',
      replicas: [],
      aggregate: {
        available: true,
        coverage: 'partial',
        estimatedHourlyCost: 1.5,
        spotHourlyCost: 0.5,
        nonSpotHourlyCost: 1,
        costTrackedReplicaCount: 3,
        pricedReplicaCount: 2,
        hourlyCostExcludedReplicaCount: 1,
        hourlyCostExclusionReasons: { missing_version_catalog: 1 },
      },
    });
  });

  it.each([
    [
      'empty',
      {
        known_hourly_cost: 0,
        spot_hourly_cost: 0,
        non_spot_hourly_cost: 0,
        tracked_replica_count: 0,
        priced_replica_count: 0,
        excluded_replica_count: 0,
        exclusion_reasons: {},
      },
    ],
    [
      'complete',
      {
        known_hourly_cost: 0,
        spot_hourly_cost: 0,
        non_spot_hourly_cost: 0,
        tracked_replica_count: 2,
        priced_replica_count: 2,
        excluded_replica_count: 0,
        exclusion_reasons: {},
      },
    ],
    [
      'none',
      {
        known_hourly_cost: null,
        spot_hourly_cost: null,
        non_spot_hourly_cost: null,
        tracked_replica_count: 2,
        priced_replica_count: 0,
        excluded_replica_count: 2,
        exclusion_reasons: { missing_version_catalog: 2 },
      },
    ],
  ])(
    'preserves %s coverage instead of conflating it with zero',
    (coverage, values) => {
      const normalized = normalizeServicePricing(
        aggregatePayload({
          aggregate: {
            available: true,
            unavailable_reason: null,
            coverage,
            ...values,
          },
        })
      );

      expect(normalized.aggregate).toMatchObject({
        coverage,
        estimatedHourlyCost: values.known_hourly_cost,
        costTrackedReplicaCount: values.tracked_replica_count,
      });
    }
  );

  it('keeps aggregate oversize unavailable without manufacturing totals', () => {
    const normalized = normalizeServicePricing(
      aggregatePayload({
        aggregate: {
          available: false,
          unavailable_reason: 'projection_too_large',
          coverage: null,
          known_hourly_cost: null,
          spot_hourly_cost: null,
          non_spot_hourly_cost: null,
          tracked_replica_count: null,
          priced_replica_count: null,
          excluded_replica_count: null,
          exclusion_reasons: null,
        },
      })
    );

    expect(normalized.aggregate).toEqual({
      available: false,
      unavailableReason: 'projection_too_large',
      coverage: null,
      estimatedHourlyCost: null,
      spotHourlyCost: null,
      nonSpotHourlyCost: null,
      costTrackedReplicaCount: null,
      pricedReplicaCount: null,
      hourlyCostExcludedReplicaCount: null,
      hourlyCostExclusionReasons: null,
    });
  });

  it('deduplicates row IDs and settles priced, excluded, and absent rows', async () => {
    apiClient.get.mockResolvedValue(
      pricingResponse(
        aggregatePayload({
          aggregate: null,
          replicas: [
            {
              replica_id: 7,
              pricing_fingerprint: 'fp-7',
              hourly_cost: 0,
              price_source: 'zero_cost_provenance',
              hourly_cost_exclusion_reason: null,
            },
            {
              replica_id: 8,
              pricing_fingerprint: 'fp-8',
              hourly_cost: null,
              price_source: null,
              hourly_cost_exclusion_reason: 'missing_version_catalog',
            },
            {
              replica_id: 9,
              pricing_fingerprint: null,
              hourly_cost: null,
              price_source: null,
              hourly_cost_exclusion_reason: 'not_current_or_uncertain',
            },
          ],
        })
      )
    );

    const result = await getServicePricing({
      serviceName: 'svc/name',
      serviceHash: 'hash/a',
      replicaIds: [7, 8, 7, 9],
    });

    expect(apiClient.get).toHaveBeenCalledWith(
      '/serve/svc%2Fname/pricing?expected_service_hash=hash%2Fa&replica_id=7&replica_id=8&replica_id=9'
    );
    expect(result.aggregate).toBeNull();
    expect(result.replicas).toEqual([
      {
        id: 7,
        pricingFingerprint: 'fp-7',
        hourlyCost: 0,
        priceSource: 'zero_cost_provenance',
        hourlyCostExclusionReason: null,
      },
      {
        id: 8,
        pricingFingerprint: 'fp-8',
        hourlyCost: null,
        priceSource: null,
        hourlyCostExclusionReason: 'missing_version_catalog',
      },
      {
        id: 9,
        pricingFingerprint: null,
        hourlyCost: null,
        priceSource: null,
        hourlyCostExclusionReason: 'not_current_or_uncertain',
      },
    ]);
  });

  it('rejects raw oversize and non-positive IDs before contacting the server', async () => {
    await expect(
      getServicePricing({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        replicaIds: Array(101).fill(1),
      })
    ).rejects.toThrow('at most 100');
    await expect(
      getServicePricing({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        replicaIds: [0],
      })
    ).rejects.toThrow('positive integers');
    expect(apiClient.get).not.toHaveBeenCalled();
  });

  it('rejects overlapping modes, incomplete settlement, and unsafe fingerprints', async () => {
    apiClient.get.mockResolvedValueOnce(
      pricingResponse(
        aggregatePayload({
          replicas: [
            {
              replica_id: 7,
              pricing_fingerprint: 'fp-7',
              hourly_cost: 1,
              price_source: 'version_catalog',
              hourly_cost_exclusion_reason: null,
            },
          ],
        })
      )
    );
    await expect(
      getServicePricing({ serviceName: 'svc/name', serviceHash: 'hash/a' })
    ).rejects.toThrow('response mode was malformed');

    apiClient.get.mockResolvedValueOnce(
      pricingResponse(
        aggregatePayload({
          aggregate: null,
          replicas: [
            {
              replica_id: 7,
              pricing_fingerprint: 'fp-7',
              hourly_cost: 1,
              price_source: 'version_catalog',
              hourly_cost_exclusion_reason: null,
            },
          ],
        })
      )
    );
    await expect(
      getServicePricing({
        serviceName: 'svc/name',
        serviceHash: 'hash/a',
        replicaIds: [7, 8],
      })
    ).rejects.toThrow('did not settle every replica');

    expect(
      normalizeServicePricing(
        aggregatePayload({
          aggregate: null,
          replicas: [
            {
              replica_id: 7,
              pricing_fingerprint: null,
              hourly_cost: 1,
              price_source: 'version_catalog',
              hourly_cost_exclusion_reason: null,
            },
          ],
        })
      )
    ).toBeNull();
  });

  it('capability-gates pricing 404s without restoring full status', async () => {
    apiClient.get.mockResolvedValueOnce(
      pricingResponse({}, { status: 404, apiVersion: '70' })
    );
    await expect(
      getServicePricing({ serviceName: 'svc', serviceHash: 'hash-a' })
    ).resolves.toMatchObject({
      available: false,
      reason: 'unsupported',
      legacyFallback: false,
    });

    apiClient.get.mockResolvedValueOnce(
      pricingResponse({}, { status: 404, apiVersion: '71' })
    );
    await expect(
      getServicePricing({ serviceName: 'svc', serviceHash: 'hash-a' })
    ).resolves.toMatchObject({
      available: false,
      reason: 'not_found',
      legacyFallback: false,
    });
  });
});

describe('service version administration', () => {
  it('fetches the immutable version history', async () => {
    const history = {
      service_name: 'boltz-l4-fleet',
      elected_version: 3,
      active_versions: [2, 3],
      versions: [{ version: 3, elected: true, active: true }],
    };
    apiClient.get.mockResolvedValue({
      ok: true,
      json: async () => history,
    });

    await expect(getServiceVersions('boltz/l4')).resolves.toEqual(history);
    expect(apiClient.get).toHaveBeenCalledWith('/serve/boltz%2Fl4/versions');
  });

  it('elects a stored version through the queued update path', async () => {
    apiClient.fetch.mockResolvedValue([]);

    await electServiceVersion('boltz-l4-fleet', 2);

    expect(apiClient.fetch).toHaveBeenCalledWith(
      '/serve/boltz-l4-fleet/versions/elect',
      { version: 2 }
    );
  });
});

describe('normalizeReplicaHistory', () => {
  it('drops malformed samples and defaults invalid counts to zero', () => {
    const history = normalizeReplicaHistory({
      available: true,
      samples: [
        { timestamp: '100', version: 1, ready_count: '2' },
        { timestamp: 'bad', version: 2, ready_count: 3 },
      ],
      request_samples: [
        { timestamp: '120', request_count: '7' },
        { timestamp: '180', request_count: 2, rejected_count: null },
        { timestamp: 'bad', request_count: 8 },
      ],
      autoscaler_samples: [
        {
          timestamp: '120',
          observed_at: '125',
          controller_session_id: 'a'.repeat(32),
          version: 2,
          replica_unit: 'logical_slot',
          demand_target: 3,
          capacity_target: 4,
          ready_capacity: 2,
          provisioning_capacity: 1,
          total_capacity: 4,
          peak_in_flight: 7,
          peak_queue_depth: null,
          accelerator_breakdown: {
            version: 1,
            configured_accelerators: ['A100', 'A100-80GB'],
            min_replicas: { A100: 1, 'A100-80GB': 0 },
            demand_target: { A100: 3, 'A100-80GB': 1 },
            ready_capacity: { A100: 2, 'A100-80GB': 0 },
            provisioning_capacity: { A100: 1, 'A100-80GB': 0 },
            total_capacity: { A100: 3, 'A100-80GB': 1 },
            zero_cost_ready_capacity: { A100: 1, 'A100-80GB': 0 },
            fill_target: { A100: 4, 'A100-80GB': 0 },
            free_reserved_slots: { A100: 1, 'A100-80GB': 0 },
          },
        },
        {
          timestamp: 'bad',
          observed_at: 125,
          version: 2,
          replica_unit: 'logical_slot',
        },
      ],
      request_window_seconds: 3600,
      requests_last_hour: 7,
    });
    expect(history.samples).toEqual([
      expect.objectContaining({
        timestamp: 100,
        version: 1,
        readyCount: 2,
        erroredCount: 0,
        totalCount: 0,
        readyReservedCount: null,
        logicalReadyCount: null,
        logicalReadyReservedCount: null,
        logicalTotalCount: null,
      }),
    ]);
    expect(history.requestSamples).toEqual([
      { timestamp: 120, requestCount: 7, rejectedCount: null },
      { timestamp: 180, requestCount: 2, rejectedCount: null },
    ]);
    expect(history.autoscalerSamples).toEqual([
      {
        timestamp: 120,
        observedAt: 125,
        controllerSessionId: 'a'.repeat(32),
        version: 2,
        replicaUnit: 'logical_slot',
        demandTarget: 3,
        capacityTarget: 4,
        readyCapacity: 2,
        provisioningCapacity: 1,
        totalCapacity: 4,
        peakInFlight: 7,
        peakQueueDepth: null,
        acceleratorBreakdown: {
          configuredAccelerators: ['A100', 'A100-80GB'],
          minReplicas: { A100: 1, 'A100-80GB': 0 },
          demandTarget: { A100: 3, 'A100-80GB': 1 },
          readyCapacity: { A100: 2, 'A100-80GB': 0 },
          provisioningCapacity: { A100: 1, 'A100-80GB': 0 },
          totalCapacity: { A100: 3, 'A100-80GB': 1 },
          zeroCostReadyCapacity: { A100: 1, 'A100-80GB': 0 },
          fillTarget: { A100: 4, 'A100-80GB': 0 },
          freeReservedSlots: { A100: 1, 'A100-80GB': 0 },
        },
      },
    ]);
    expect(history.requestWindowSeconds).toBe(3600);
    expect(history.requestsLastHour).toBe(7);
  });

  it('does not fabricate zero requests when history is unavailable', () => {
    const history = normalizeReplicaHistory({
      available: false,
      request_window_seconds: 3600,
      requests_last_hour: 0,
      samples: [],
      request_samples: [],
    });

    expect(history.available).toBe(false);
    expect(history.requestsLastHour).toBeNull();
  });
});

describe('normalizeService / normalizeReplica', () => {
  it('keeps A100 and A100-80GB as separate exact-card capacity rows', () => {
    const service = normalizeService(
      rawServiceRecord({
        min_replicas_by_accelerator: { A100: 1, 'A100-80GB': 2 },
        target_num_replicas_by_accelerator: { A100: 3, 'A100-80GB': 4 },
        warm_retention_target_by_accelerator: { A100: 2, 'A100-80GB': 1 },
        cold_launch_authority_by_accelerator: { A100: 0, 'A100-80GB': 3 },
        ready_replicas_by_accelerator: { A100: 2, 'A100-80GB': 1 },
        provisioning_replicas_by_accelerator: {
          A100: 1,
          'A100-80GB': 3,
        },
        total_replicas_by_accelerator: { A100: 3, 'A100-80GB': 4 },
        zero_cost_ready_replicas_by_accelerator: {
          A100: 1,
          'A100-80GB': 0,
        },
        fill_target: 5,
        fill_free_slots: 2,
      })
    );

    expect(service.acceleratorCapacity).toEqual([
      expect.objectContaining({
        card: 'A100',
        ready: 2,
        provisioning: 1,
        demandTarget: 3,
        warmRetentionTarget: 2,
        coldLaunchAuthority: 0,
        hardFloor: 1,
      }),
      expect.objectContaining({
        card: 'A100-80GB',
        ready: 1,
        provisioning: 3,
        demandTarget: 4,
        warmRetentionTarget: 1,
        coldLaunchAuthority: 3,
        hardFloor: 2,
      }),
    ]);
    expect(service.fillTarget).toBe(5);
    expect(service.freeReservedSlots).toBe(2);
  });

  it('combines spot and on-demand replica costs and tracks exclusions', () => {
    const service = normalizeService(
      rawServiceRecord({
        requests_per_second: 0.5,
        replica_info: [
          {
            replica_id: 1,
            status: 'READY',
            is_spot: true,
            hourly_cost: 1.5,
          },
          {
            replica_id: 2,
            status: 'READY',
            is_spot: false,
            hourly_cost: 4,
          },
          {
            replica_id: 3,
            status: 'READY',
            hourly_cost: null,
            hourly_cost_exclusion_reason: 'kubernetes',
          },
        ],
      })
    );

    expect(service.estimatedHourlyCost).toBe(5.5);
    expect(service.spotHourlyCost).toBe(1.5);
    expect(service.onDemandHourlyCost).toBe(4);
    expect(service.pricedReplicaCount).toBe(2);
    expect(service.hourlyCostExcludedReplicaCount).toBe(1);
    expect(service.hourlyCostExclusionReasons).toEqual({ kubernetes: 1 });
    // Report measurable cloud spend while keeping the excluded capacity
    // explicit in the UI.
    expect(service.costPerThousandRequests).toBe(3.0555555555555554);
  });

  it('does not price an all-Kubernetes fleet without cost data', () => {
    const service = normalizeService(
      rawServiceRecord({
        requests_per_second: 0.5,
        replica_info: [
          {
            replica_id: 1,
            status: 'READY',
            hourly_cost: null,
            hourly_cost_exclusion_reason: 'kubernetes',
          },
        ],
      })
    );

    expect(service.estimatedHourlyCost).toBeNull();
    expect(service.costPerThousandRequests).toBeNull();
    expect(service.hourlyCostExclusionReasons).toEqual({ kubernetes: 1 });
  });

  it('prices current billability risk without charging historical rows', () => {
    const statuses = [
      ['READY', 1],
      ['SHUTTING_DOWN', 2],
      ['FAILED_CLEANUP', 3],
      ['UNKNOWN', 4],
      ['PENDING', 5],
      ['FAILED', 6],
      ['FAILED_INITIAL_DELAY', 7],
      ['FAILED_PROBING', 8],
      ['FAILED_PROVISION', 9],
      ['PREEMPTED', 10],
    ];
    const service = normalizeService(
      rawServiceRecord({
        replica_info: statuses.map(([status, replicaId]) => ({
          replica_id: replicaId,
          status,
          hourly_cost: replicaId,
        })),
      })
    );

    expect(service.estimatedHourlyCost).toBe(10);
    expect(service.costTrackedReplicaCount).toBe(4);
    expect(service.pricedReplicaCount).toBe(4);
    expect(service.hourlyCostExcludedReplicaCount).toBe(0);
  });

  it('does not report unpriced historical rows as current exclusions', () => {
    const service = normalizeService(
      rawServiceRecord({
        replica_info: [
          {
            replica_id: 1,
            status: 'FAILED',
            hourly_cost: null,
            hourly_cost_exclusion_reason: 'kubernetes',
          },
          {
            replica_id: 2,
            status: 'SHUTTING_DOWN',
            hourly_cost: null,
            hourly_cost_exclusion_reason: 'kubernetes',
          },
        ],
      })
    );

    expect(service.costTrackedReplicaCount).toBe(1);
    expect(service.hourlyCostExcludedReplicaCount).toBe(1);
    expect(service.hourlyCostExclusionReasons).toEqual({ kubernetes: 1 });
  });

  it('handles a service with no replica_info', () => {
    const service = normalizeService(
      rawServiceRecord({
        replica_info: undefined,
        endpoint: null,
        uptime: null,
      })
    );

    expect(service.replicas).toEqual([]);
    expect(service.replicasReady).toBe(0);
    expect(service.replicasTotal).toBe(0);
    expect(service.endpoint).toBeNull();
    expect(service.uptime).toBeNull();
  });

  it('falls back to the short resources string when the full one is missing', () => {
    const replica = normalizeReplica({
      replica_id: 7,
      status: 'STARTING',
      resources_str: '1x(gpus=L4:1)',
    });

    expect(replica.id).toBe(7);
    expect(replica.resources_str_full).toBe('1x(gpus=L4:1)');
    expect(replica.endpoint).toBeNull();
    expect(replica.launched_at).toBeNull();
    expect(replica.ready_at).toBeNull();
    expect(replica.timeToReadySeconds).toBeNull();
  });
});

describe('service placement', () => {
  const rawPlacement = {
    service_name: 'svc',
    placer_state: {
      available: true,
      enabled: true,
      retry_seconds: 600,
      status_semantics: 'Eligibility is not live inventory.',
      locations: [
        {
          cloud: 'AWS',
          region: 'us-east-1',
          zone: 'us-east-1a',
          instance_type: 'g6.xlarge',
          accelerators: { L4: 1 },
          use_spot: true,
          stored_status: 'PREEMPTED',
          effective_status: 'ACTIVE',
          bench_reason: 'quota',
          probe_eligible: true,
          benched_at: 1000,
          next_probe_at: 1600,
          paid_admission: {
            state: 'cooldown',
            pool_remaining: 0,
            service_remaining: 12,
            cooldown_until: 1700,
          },
        },
      ],
    },
    capacity_hints: {
      available: true,
      hints: [
        {
          kind: 'capacity',
          cloud: 'aws',
          region: 'us-east-1',
          zone: 'us-east-1a',
          instance_type: 'g6.4xlarge',
          accelerators: 'L4:1',
          num_nodes: 2,
          expires_at: 1120,
        },
      ],
    },
    history: {
      available: true,
      retention_hours: 24,
      outcome_counts: { capacity_failed: 1 },
      next_cursor: 'cursor-a',
      events: [
        {
          event_id: 'event-a',
          request_id: 'request-a',
          cluster_name: 'svc-1',
          attempt_ordinal: 0,
          observed_at: 1000,
          outcome: 'capacity_failed',
          provider: 'AWS',
          region: 'us-east-1',
          zone: 'us-east-1a',
          instance_type: 'g6.4xlarge',
          hourly_price: 0.25,
        },
      ],
    },
  };

  it('normalizes retry, cache, and history fields', () => {
    const placement = normalizeServicePlacement(rawPlacement);

    expect(placement.serviceName).toBe('svc');
    expect(placement.placerState.locations[0]).toMatchObject({
      cloud: 'AWS',
      instanceType: 'g6.xlarge',
      benchReason: 'quota',
      probeEligible: true,
      storedStatus: 'PREEMPTED',
      effectiveStatus: 'ACTIVE',
      nextProbeAt: 1600,
      paidAdmission: {
        state: 'cooldown',
        poolRemaining: 0,
        serviceRemaining: 12,
        cooldownUntil: 1700,
      },
    });
    expect(placement.placerState.statusSemantics).toBe(
      'Eligibility is not live inventory.'
    );
    expect(placement.capacityHints.hints[0]).toMatchObject({
      kind: 'capacity',
      cloud: 'aws',
      instanceType: 'g6.4xlarge',
      accelerators: 'L4:1',
      numNodes: 2,
    });
    expect(placement.history.events[0]).toMatchObject({
      eventId: 'event-a',
      attemptOrdinal: 0,
      hourlyPrice: 0.25,
    });
  });

  it('dispatches one bounded service placement request', async () => {
    apiClient.post.mockResolvedValue(mockDispatchResponse());
    apiClient.get.mockResolvedValue(mockResultResponse(rawPlacement));

    const placement = await getServicePlacement({
      serviceName: 'svc',
      hours: 12,
      limit: 10,
      cursor: 'cursor-a',
    });

    expect(apiClient.post).toHaveBeenCalledWith('/serve/placement', {
      service_name: 'svc',
      hours: 12,
      limit: 10,
      cursor: 'cursor-a',
    });
    expect(apiClient.get).toHaveBeenCalledWith(
      `/api/get?request_id=${REQUEST_ID}`
    );
    expect(placement.history.events).toHaveLength(1);
  });
});
