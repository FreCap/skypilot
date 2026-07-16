jest.mock('@/data/connectors/client', () => ({
  __esModule: true,
  apiClient: {
    post: jest.fn(),
    get: jest.fn(),
  },
}));

import { apiClient } from '@/data/connectors/client';
import { getSlurmInfrastructure } from '@/data/connectors/infra';
import { getSlurmInfrastructure as getSlurmInfrastructureDirect } from '@/data/connectors/infra-slurm';

function dispatchResponse(requestId) {
  return {
    ok: true,
    status: 200,
    headers: {
      get: (name) => (name === 'X-Skypilot-Request-ID' ? requestId : null),
    },
  };
}

function resultResponse(returnValue) {
  return {
    ok: true,
    status: 200,
    json: async () => ({ return_value: JSON.stringify(returnValue) }),
  };
}

beforeEach(() => {
  jest.clearAllMocks();
});

describe('getSlurmInfrastructure', () => {
  it('preserves the historical infra facade as a direct export', () => {
    expect(getSlurmInfrastructure).toBe(getSlurmInfrastructureDirect);
  });

  it('fetches both Slurm inventories in parallel and projects stable sorted rows', async () => {
    const clusterRows = [
      [
        'zeta',
        [
          ['L4', [1, 2], 4, 3],
          ['A100', [1], 2, 1],
        ],
      ],
      ['alpha', [['L4', [1], 8, 5]]],
    ];
    const nodeRows = [
      {
        node_name: 'node-b',
        slurm_cluster_name: 'zeta',
        partition: null,
        gpu_type: null,
        total_gpus: null,
        free_gpus: null,
      },
      {
        node_name: 'node-a',
        slurm_cluster_name: 'alpha',
        partition: 'gpu',
        gpu_type: 'L4',
        total_gpus: 8,
        free_gpus: 5,
      },
    ];

    apiClient.post.mockImplementation((path) => {
      if (path === '/slurm_gpu_availability') {
        return Promise.resolve(dispatchResponse('cluster-request'));
      }
      if (path === '/slurm_node_info') {
        return Promise.resolve(dispatchResponse('node-request'));
      }
      throw new Error(`Unexpected path: ${path}`);
    });
    apiClient.get.mockImplementation((path) => {
      if (path === '/api/get?request_id=cluster-request') {
        return Promise.resolve(resultResponse(clusterRows));
      }
      if (path === '/api/get?request_id=node-request') {
        return Promise.resolve(resultResponse(nodeRows));
      }
      throw new Error(`Unexpected path: ${path}`);
    });

    await expect(getSlurmInfrastructure()).resolves.toEqual({
      allSlurmGPUs: [
        { gpu_name: 'A100', gpu_total: 2, gpu_free: 1 },
        { gpu_name: 'L4', gpu_total: 12, gpu_free: 8 },
      ],
      perClusterSlurmGPUs: [
        {
          cluster: 'alpha',
          gpu_name: 'L4',
          gpu_requestable_qty_per_node: '1',
          gpu_total: 8,
          gpu_free: 5,
        },
        {
          cluster: 'zeta',
          gpu_name: 'A100',
          gpu_requestable_qty_per_node: '1',
          gpu_total: 2,
          gpu_free: 1,
        },
        {
          cluster: 'zeta',
          gpu_name: 'L4',
          gpu_requestable_qty_per_node: '1, 2',
          gpu_total: 4,
          gpu_free: 3,
        },
      ],
      perNodeSlurmGPUs: [
        {
          cluster: 'alpha',
          node_name: 'node-a',
          partition: 'gpu',
          gpu_name: 'L4',
          gpu_total: 8,
          gpu_free: 5,
        },
        {
          cluster: 'zeta',
          node_name: 'node-b',
          partition: 'default',
          gpu_name: '-',
          gpu_total: 0,
          gpu_free: 0,
        },
      ],
    });
    expect(apiClient.post).toHaveBeenCalledTimes(2);
    expect(apiClient.post).toHaveBeenCalledWith('/slurm_gpu_availability', {});
    expect(apiClient.post).toHaveBeenCalledWith('/slurm_node_info', {});
    expect(apiClient.get).toHaveBeenCalledTimes(2);
  });

  it('keeps node inventory when the cluster inventory request fails', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    apiClient.post.mockImplementation((path) => {
      if (path === '/slurm_gpu_availability') {
        return Promise.resolve({ ok: false, status: 503 });
      }
      return Promise.resolve(dispatchResponse('node-request'));
    });
    apiClient.get.mockResolvedValue(
      resultResponse([
        {
          node_name: 'node-a',
          slurm_cluster_name: 'alpha',
          partition: 'gpu',
          gpu_type: 'L4',
          total_gpus: 4,
          free_gpus: 2,
        },
      ])
    );

    await expect(getSlurmInfrastructure()).resolves.toEqual({
      allSlurmGPUs: [],
      perClusterSlurmGPUs: [],
      perNodeSlurmGPUs: [
        {
          cluster: 'alpha',
          node_name: 'node-a',
          partition: 'gpu',
          gpu_name: 'L4',
          gpu_total: 4,
          gpu_free: 2,
        },
      ],
    });
    expect(apiClient.post).toHaveBeenCalledTimes(2);
    expect(apiClient.get).toHaveBeenCalledTimes(1);
  });
});
