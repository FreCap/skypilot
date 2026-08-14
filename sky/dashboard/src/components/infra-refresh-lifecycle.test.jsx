import { act, render, screen, waitFor } from '@testing-library/react';

import { GPUs } from '@/components/infra';
import {
  getContextGPUData,
  getWorkspaceContexts,
} from '@/data/connectors/infra';

jest.mock('next/router', () => ({
  useRouter: () => ({
    push: jest.fn(),
    isReady: true,
    query: {},
  }),
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/plugins/PluginSlot', () => ({
  PluginSlot: () => null,
}));

jest.mock('@/plugins/PluginWrapperSlot', () => ({
  PluginWrapperSlot: ({ children }) => children,
}));

jest.mock('@/plugins/PluginProvider', () => ({
  useAllDataProviders: () => [],
  usePluginComponents: () => [],
}));

jest.mock('@/lib/cache-preloader', () => ({
  __esModule: true,
  default: {
    preloadForPage: jest.fn().mockResolvedValue(undefined),
  },
}));

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn((fn, args = []) => fn(...args)),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
  },
}));

jest.mock('@/data/connectors/infra', () => ({
  getWorkspaceInfrastructure: jest.fn().mockResolvedValue({}),
  getWorkspaceContexts: jest.fn(),
  getContextGPUData: jest.fn(),
  getInfraSummary: jest.fn(),
  getCloudInfrastructure: jest.fn().mockResolvedValue({}),
  getEnabledCloudsList: jest.fn().mockResolvedValue({
    clouds: [],
    totalClouds: 0,
  }),
  getContextJobs: jest.fn().mockResolvedValue({}),
  getContextClusters: jest.fn().mockResolvedValue({}),
  getSlurmInfrastructure: jest.fn().mockResolvedValue({
    allSlurmGPUs: [],
    perClusterSlurmGPUs: [],
    perNodeSlurmGPUs: [],
  }),
}));

jest.mock('@/data/connectors/workspaces', () => ({
  runSkyCheck: jest.fn().mockResolvedValue(undefined),
  getWorkspaces: jest.fn().mockResolvedValue({}),
  getEnabledCloudsBatch: jest.fn().mockResolvedValue({}),
}));

jest.mock('@/data/connectors/clusters', () => ({
  getClusters: jest.fn().mockResolvedValue([]),
}));

jest.mock('@/data/connectors/jobs', () => ({
  getManagedJobs: jest.fn().mockResolvedValue({ jobs: [] }),
}));

jest.mock('@/data/connectors/ssh-node-pools', () => ({
  getSSHNodePools: jest.fn().mockResolvedValue({}),
  updateSSHNodePools: jest.fn(),
  deleteSSHNodePool: jest.fn(),
  deploySSHNodePool: jest.fn(),
}));

function deferred() {
  let resolve;
  const promise = new Promise((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

describe('infrastructure refresh lifecycle', () => {
  it('coalesces overlapping manual refreshes before the context fanout duplicates', async () => {
    const gpuData = {
      perContextGPUs: [],
      perNodeGPUs: [],
      error: null,
    };
    getWorkspaceContexts.mockResolvedValue({
      workspaces: { default: { clouds: ['kubernetes/ctx'] } },
      allContextNames: ['ctx'],
      contextWorkspaceMap: { ctx: ['default'] },
    });
    getContextGPUData.mockResolvedValueOnce(gpuData);

    render(<GPUs />);
    await waitFor(() => {
      expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
    });

    const first = deferred();
    const second = deferred();
    getContextGPUData
      .mockImplementationOnce(() => first.promise)
      .mockImplementationOnce(() => second.promise);

    act(() => {
      window.dispatchEvent(new Event('skydashboard:infra:refresh'));
      window.dispatchEvent(new Event('skydashboard:infra:refresh'));
    });
    await waitFor(() => {
      expect(getContextGPUData).toHaveBeenCalledTimes(2);
      expect(screen.getByText('Loading...')).toBeVisible();
    });

    await act(async () => {
      first.resolve(gpuData);
      second.resolve(gpuData);
      await first.promise;
      await second.promise;
    });
    await waitFor(() => {
      expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
    });
  });
});
