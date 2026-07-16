// Mock the shared API client so we can exercise the connector's
// normalization logic without hitting the network.
jest.mock('@/data/connectors/client', () => ({
  __esModule: true,
  apiClient: {
    post: jest.fn(),
    get: jest.fn(),
  },
}));

import { apiClient } from '@/data/connectors/client';
import {
  getServices,
  normalizeReplicaHistory,
  normalizeService,
  normalizeReplica,
} from '@/data/connectors/services';

const REQUEST_ID = 'req-123';

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

  it('requests and normalizes aggregate replica history', async () => {
    const record = rawServiceRecord({
      replica_status_history: {
        available: true,
        bucket_seconds: 60,
        retention_hours: 72,
        window_start: 1751590000,
        window_end: 1751633200,
        samples: [
          {
            timestamp: 1751633160,
            observed_at: 1751633170,
            version: 2,
            ready_count: 3,
            provisioning_count: 1,
            not_ready_count: 0,
            errored_count: 2,
            preempted_count: 0,
            stopping_count: 1,
            total_count: 7,
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
          provisioningCount: 1,
          erroredCount: 2,
          stoppingCount: 1,
          totalCount: 7,
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
        { timestamp: 'bad', request_count: 8 },
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
      }),
    ]);
    expect(history.requestSamples).toEqual([
      { timestamp: 120, requestCount: 7 },
    ]);
    expect(history.requestWindowSeconds).toBe(3600);
    expect(history.requestsLastHour).toBe(7);
  });
});

describe('normalizeService / normalizeReplica', () => {
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
    // A partial fleet price must not produce an understated per-request cost.
    expect(service.costPerThousandRequests).toBeNull();
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
