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
    replica_info: [
      {
        replica_id: 1,
        name: 'boltz-l4-fleet-1',
        status: 'READY',
        version: 2,
        endpoint: 'http://10.0.0.2:8000',
        is_spot: false,
        launched_at: 1751590000,
        cloud: 'AWS',
        region: 'us-east-1',
        infra: 'AWS (us-east-1)',
        resources_str: '1x(gpus=L4:1)',
        resources_str_full: '1x(gpus=L4:1, cpus=4, mem=16)',
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
    });
    expect(service.replicas).toHaveLength(1);
    expect(service.replicas[0]).toMatchObject({
      id: 1,
      status: 'READY',
      version: 2,
      endpoint: 'http://10.0.0.2:8000',
      launched_at: 1751590000,
      region: 'us-east-1',
      resources_str: '1x(gpus=L4:1)',
      resources_str_full: '1x(gpus=L4:1, cpus=4, mem=16)',
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

  it('counts ready replicas with mixed replica statuses', async () => {
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
    expect(services[0].replicasTotal).toBe(5);
    expect(services[0].replicas.map((r) => r.id)).toEqual([1, 2, 3, 4, 5]);
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

describe('normalizeService / normalizeReplica', () => {
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
  });
});
