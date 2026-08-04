jest.mock('@/data/connectors/client', () => ({
  __esModule: true,
  apiClient: {
    post: jest.fn(),
    get: jest.fn(),
  },
}));

jest.mock('@/data/connectors/workspaces', () => ({
  __esModule: true,
  getWorkspaces: jest.fn(),
  getEnabledCloudsBatch: jest.fn(),
}));

jest.mock('@/data/connectors/clusters', () => ({
  __esModule: true,
  getClusters: jest.fn(),
}));

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
  },
}));

import { apiClient } from '@/data/connectors/client';
import {
  getContextGPUData,
  getWorkspaceInfrastructure,
} from '@/data/connectors/infra';
import { getContextGPUData as getContextGPUDataDirect } from '@/data/connectors/infra-kubernetes';
import { getClusters } from '@/data/connectors/clusters';
import {
  getEnabledCloudsBatch,
  getWorkspaces,
} from '@/data/connectors/workspaces';
import dashboardCache from '@/lib/cache';

function dispatchResponse(requestId) {
  return {
    ok: true,
    status: 200,
    headers: {
      get: (name) => (name === 'X-Skypilot-Request-ID' ? requestId : null),
    },
  };
}

function resultResponse(nodeInfoDict) {
  return {
    ok: true,
    status: 200,
    json: async () => ({
      return_value: JSON.stringify({ node_info_dict: nodeInfoDict }),
    }),
  };
}

beforeEach(() => {
  jest.clearAllMocks();
  jest.spyOn(console, 'log').mockImplementation(() => {});
  jest.spyOn(console, 'warn').mockImplementation(() => {});
  jest.spyOn(console, 'error').mockImplementation(() => {});
});

afterEach(() => {
  jest.restoreAllMocks();
});

describe('Kubernetes infrastructure gateway characterization', () => {
  it('preserves the historical infra facade as a direct export', () => {
    expect(getContextGPUData).toBe(getContextGPUDataDirect);
  });

  it('sums preemptible accelerators across the nodes of a GPU type', async () => {
    apiClient.post.mockResolvedValue(dispatchResponse('node-request'));
    apiClient.get.mockResolvedValue(
      resultResponse({
        'node-a': {
          name: 'node-a',
          accelerator_type: 'A100',
          total: { accelerator_count: 8 },
          free: { accelerators_available: 0 },
          is_ready: true,
          accelerators_preemptible: 5,
          preemptible_breakdown: { 'inference-low (-1000)': 5 },
        },
        'node-b': {
          name: 'node-b',
          accelerator_type: 'A100',
          total: { accelerator_count: 8 },
          free: { accelerators_available: 1 },
          is_ready: true,
          accelerators_preemptible: 3,
          preemptible_breakdown: {
            'inference-low (-1000)': 2,
            'drill (-500)': 1,
          },
        },
        // Not ready: its capacity is reported whole as `gpu_not_ready`, so its
        // preemptible count must not also land in the used split.
        'node-c': {
          name: 'node-c',
          accelerator_type: 'A100',
          total: { accelerator_count: 8 },
          free: { accelerators_available: 0 },
          is_ready: false,
          accelerators_preemptible: 8,
          preemptible_breakdown: { 'inference-low (-1000)': 8 },
        },
      })
    );

    const { perContextGPUs } = await getContextGPUData('prod');
    expect(perContextGPUs).toHaveLength(1);
    expect(perContextGPUs[0].gpu_preemptible).toBe(8);
    expect(perContextGPUs[0].gpu_preemptible_breakdown).toEqual({
      'inference-low (-1000)': 7,
      'drill (-500)': 1,
    });
    expect(perContextGPUs[0].gpu_not_ready).toBe(8);
  });

  it('submits and polls once while preserving node readiness projection', async () => {
    apiClient.post.mockResolvedValue(dispatchResponse('node-request'));
    apiClient.get.mockResolvedValue(
      resultResponse({
        'node-b': {
          name: 'node-b',
          accelerator_type: 'L4',
          total: { accelerator_count: 4 },
          free: { accelerators_available: 2 },
          is_ready: true,
          is_cordoned: false,
          taints: [{ key: 'dedicated', tolerated: false }],
          ip_address: '10.0.0.2',
          cpu_count: 16,
          memory_gb: 64,
          cpu_free: 8,
          memory_free_gb: 32,
        },
        'node-a': {
          name: 'node-a',
          accelerator_type: 'A100',
          total: { accelerator_count: 2 },
          free: { accelerators_available: 1 },
          is_ready: false,
        },
      })
    );

    await expect(getContextGPUData('prod')).resolves.toEqual({
      context: 'prod',
      perContextGPUs: [
        {
          gpu_name: 'L4',
          gpu_requestable_qty_per_node: 4,
          gpu_total: 4,
          gpu_free: 2,
          gpu_not_ready: 4,
          context: 'prod',
          gpu_preemptible: 0,
          gpu_preemptible_breakdown: {},
        },
        {
          gpu_name: 'A100',
          gpu_requestable_qty_per_node: 2,
          gpu_total: 2,
          gpu_free: 1,
          gpu_not_ready: 2,
          context: 'prod',
          gpu_preemptible: 0,
          gpu_preemptible_breakdown: {},
        },
      ],
      perNodeGPUs: [
        {
          node_name: 'node-b',
          gpu_name: 'L4',
          gpu_total: 4,
          gpu_free: 2,
          is_ready: true,
          is_cordoned: false,
          taints: [{ key: 'dedicated', tolerated: false }],
          context: 'prod',
          ip_address: '10.0.0.2',
          cpu_count: 16,
          memory_gb: 64,
          cpu_free: 8,
          memory_free_gb: 32,
          gpu_preemptible: null,
          gpu_preemptible_breakdown: null,
        },
        {
          node_name: 'node-a',
          gpu_name: 'A100',
          gpu_total: 2,
          gpu_free: 1,
          is_ready: false,
          is_cordoned: false,
          taints: [],
          context: 'prod',
          ip_address: null,
          cpu_count: null,
          memory_gb: null,
          cpu_free: null,
          memory_free_gb: null,
          gpu_preemptible: null,
          gpu_preemptible_breakdown: null,
        },
      ],
      error: null,
    });

    expect(apiClient.post).toHaveBeenCalledTimes(1);
    expect(apiClient.post).toHaveBeenCalledWith('/kubernetes_node_info', {
      context: 'prod',
    });
    expect(apiClient.get).toHaveBeenCalledTimes(1);
    expect(apiClient.get).toHaveBeenCalledWith(
      '/api/get?request_id=node-request'
    );
  });

  it('contains transport failures as an empty per-context result', async () => {
    apiClient.post.mockResolvedValue({
      ok: true,
      status: 200,
      headers: { get: () => null },
    });

    await expect(getContextGPUData('offline')).resolves.toEqual({
      context: 'offline',
      perContextGPUs: [],
      perNodeGPUs: [],
      error: 'No request ID received from server for kubernetes node info',
    });
    expect(apiClient.post).toHaveBeenCalledTimes(1);
    expect(apiClient.get).not.toHaveBeenCalled();
  });

  it('keeps successful contexts when legacy workspace aggregation partially fails', async () => {
    dashboardCache.get.mockImplementation((fn) => {
      if (fn === getWorkspaces) {
        return Promise.resolve({ primary: { owner: 'test' } });
      }
      if (fn === getEnabledCloudsBatch) {
        return Promise.resolve({
          primary: ['kubernetes/zeta', 'kubernetes/alpha'],
        });
      }
      if (fn === getClusters) {
        return Promise.resolve([]);
      }
      throw new Error(`Unexpected cache function: ${fn.name}`);
    });
    apiClient.post.mockImplementation((_path, { context }) => {
      if (context === 'zeta') {
        return Promise.reject(new Error('zeta unavailable'));
      }
      return Promise.resolve(dispatchResponse(`${context}-request`));
    });
    apiClient.get.mockResolvedValue(
      resultResponse({
        'node-a': {
          name: 'node-a',
          accelerator_type: 'L4',
          total: { accelerator_count: 4 },
          free: { accelerators_available: 3 },
          is_ready: true,
        },
      })
    );

    await expect(getWorkspaceInfrastructure()).resolves.toEqual({
      workspaces: {
        primary: {
          config: { owner: 'test' },
          clouds: ['kubernetes/zeta', 'kubernetes/alpha'],
          contexts: ['zeta', 'alpha'],
        },
      },
      allContextNames: ['alpha', 'zeta'],
      allGPUs: [
        {
          gpu_total: 4,
          gpu_free: 3,
          gpu_not_ready: 0,
          gpu_name: 'L4',
          gpu_preemptible: 0,
          gpu_preemptible_breakdown: {},
        },
      ],
      perContextGPUs: [
        {
          gpu_name: 'L4',
          gpu_requestable_qty_per_node: 4,
          gpu_total: 4,
          gpu_free: 3,
          gpu_not_ready: 0,
          context: 'alpha',
          gpu_preemptible: 0,
          gpu_preemptible_breakdown: {},
        },
      ],
      perNodeGPUs: [
        {
          node_name: 'node-a',
          gpu_name: 'L4',
          gpu_total: 4,
          gpu_free: 3,
          ip_address: null,
          context: 'alpha',
          cpu_count: null,
          memory_gb: null,
          cpu_free: null,
          memory_free_gb: null,
          is_ready: true,
          is_cordoned: false,
          taints: [],
          gpu_preemptible: null,
          gpu_preemptible_breakdown: null,
        },
      ],
      contextStats: {},
      contextWorkspaceMap: {
        zeta: ['primary'],
        alpha: ['primary'],
      },
      contextErrors: { zeta: 'zeta unavailable' },
    });

    expect(apiClient.post).toHaveBeenCalledTimes(2);
    expect(apiClient.get).toHaveBeenCalledTimes(1);
  });
});
