import { act, render, screen, waitFor } from '@testing-library/react';

import { GPUs } from '@/components/infra';
import { getClusters } from '@/data/connectors/clusters';
import { getManagedJobs } from '@/data/connectors/jobs';
import {
  getCloudInfrastructure,
  getContextGPUData,
  getEnabledCloudsList,
  getSlurmInfrastructure,
  getWorkspaceInfrastructure,
  getWorkspaceContexts,
} from '@/data/connectors/infra';
import {
  getEnabledCloudsBatch,
  runSkyCheck,
} from '@/data/connectors/workspaces';
import {
  deploySSHNodePool,
  getSSHNodePools,
} from '@/data/connectors/ssh-node-pools';
import dashboardCache from '@/lib/cache';
import cachePreloader from '@/lib/cache-preloader';

const router = {
  isReady: true,
  query: {},
  asPath: '/infra',
  push: jest.fn(),
};
let lastSshNodePoolDetailsProps = null;

jest.mock('next/router', () => ({
  useRouter: () => router,
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/lib/analytics', () => ({
  trackInfraAction: jest.fn(),
}));

jest.mock('@/lib/config', () => ({
  REFRESH_INTERVALS: {
    REFRESH_INTERVAL: 1000,
  },
}));

jest.mock('@/lib/cache-preloader', () => ({
  __esModule: true,
  default: {
    preloadForPage: jest.fn(),
  },
}));

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
  },
}));

jest.mock('@/data/connectors/infra', () => ({
  getWorkspaceInfrastructure: jest.fn(),
  getWorkspaceContexts: jest.fn(),
  getContextGPUData: jest.fn(),
  getCloudInfrastructure: jest.fn(),
  getEnabledCloudsList: jest.fn(),
  getContextJobs: jest.fn(async () => ({})),
  getContextClusters: jest.fn(async () => ({})),
  getSlurmInfrastructure: jest.fn(),
}));

jest.mock('@/data/connectors/workspaces', () => ({
  runSkyCheck: jest.fn(),
  getWorkspaces: jest.fn(),
  getEnabledCloudsBatch: jest.fn(),
}));

jest.mock('@/data/connectors/clusters', () => ({
  getClusters: jest.fn(),
}));

jest.mock('@/data/connectors/jobs', () => ({
  getManagedJobs: jest.fn(),
}));

jest.mock('@/data/connectors/ssh-node-pools', () => ({
  getSSHNodePools: jest.fn(),
  updateSSHNodePools: jest.fn(),
  deleteSSHNodePool: jest.fn(),
  deploySSHNodePool: jest.fn(),
}));

jest.mock('@/data/connectors/client', () => ({
  apiClient: {
    get: jest.fn(),
    fetch: jest.fn(),
  },
}));

jest.mock('@/plugins/PluginSlot', () => ({
  PluginSlot: ({ fallback = null }) => fallback,
}));

jest.mock('@/plugins/PluginWrapperSlot', () => ({
  PluginWrapperSlot: ({ children }) => children,
}));

jest.mock('@/plugins/PluginProvider', () => ({
  useAllDataProviders: () => [],
  usePluginComponents: () => [],
}));

jest.mock('@/components/infra-context-details', () => ({
  ContextDetails: ({ contextName }) => <div>{contextName}</div>,
}));

jest.mock('@/components/infra-section', () => ({
  InfrastructureSection: ({ title, contexts = [], isLoading }) => (
    <section data-testid={`infra-${title.toLowerCase()}`}>
      <h2>{title}</h2>
      <div>{isLoading ? 'loading' : contexts.join(',') || 'empty'}</div>
    </section>
  ),
  SkeletonBadge: () => <span>loading</span>,
}));

jest.mock('@/components/ssh-node-pool-modal', () => ({
  SSHNodePoolModal: () => null,
}));

jest.mock('@/components/ssh-node-pool-details', () => ({
  SSHNodePoolDetails: (props) => {
    lastSshNodePoolDetailsProps = props;
    return <div>{props.poolName}</div>;
  },
}));

jest.mock('@/components/ui/select', () => ({
  Select: ({ children }) => <div>{children}</div>,
  SelectContent: ({ children }) => <div>{children}</div>,
  SelectItem: ({ children, value }) => <div data-value={value}>{children}</div>,
  SelectTrigger: ({ children }) => <div>{children}</div>,
  SelectValue: () => <span>All Workspaces</span>,
}));

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function contextsPayload(name) {
  return {
    workspaces: {},
    allContextNames: [name],
    contextWorkspaceMap: {},
  };
}

function cacheCallsFor(fetcher) {
  return dashboardCache.get.mock.calls.filter(([candidate]) => {
    return candidate === fetcher;
  });
}

function installStableFetches() {
  cachePreloader.preloadForPage.mockResolvedValue(undefined);
  runSkyCheck.mockResolvedValue(undefined);
  getWorkspaceContexts.mockResolvedValue(contextsPayload('manual-context'));
  getContextGPUData.mockResolvedValue({
    perContextGPUs: [],
    perNodeGPUs: [],
  });
  dashboardCache.get.mockImplementation((fetcher, args) => {
    if (fetcher === getWorkspaceContexts) {
      return Promise.resolve(contextsPayload('initial-context'));
    }
    if (fetcher === getEnabledCloudsList) {
      return Promise.resolve({ clouds: [], totalClouds: 0 });
    }
    if (fetcher === getManagedJobs) {
      return Promise.resolve({ jobs: [] });
    }
    if (fetcher === getClusters) {
      return Promise.resolve([]);
    }
    if (fetcher === getSlurmInfrastructure) {
      return Promise.resolve({
        allSlurmGPUs: [],
        perClusterSlurmGPUs: [],
        perNodeSlurmGPUs: [],
      });
    }
    if (fetcher === getSSHNodePools) {
      return Promise.resolve({});
    }
    if (fetcher === getEnabledCloudsBatch) {
      return Promise.resolve({});
    }
    if (
      fetcher === getCloudInfrastructure &&
      JSON.stringify(args) === '[false]'
    ) {
      return Promise.resolve({ clouds: [], totalClouds: 0 });
    }
    throw new Error(`Unexpected cache fetcher: ${fetcher?.name || 'unknown'}`);
  });
}

describe('Infra page refresh lifecycle', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    installStableFetches();
    router.query = {};
    router.asPath = '/infra';
    router.push.mockReset();
    lastSshNodePoolDetailsProps = null;
    Object.defineProperty(window.document, 'visibilityState', {
      configurable: true,
      value: 'visible',
    });
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('preserves the due boundary when visibility returns before the next refresh', async () => {
    jest.useFakeTimers();

    const { unmount } = render(<GPUs />);
    await screen.findByText('initial-context');
    await act(async () => {
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(1);

    Object.defineProperty(window.document, 'visibilityState', {
      configurable: true,
      value: 'hidden',
    });
    await act(async () => {
      jest.advanceTimersByTime(999);
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(1);

    Object.defineProperty(window.document, 'visibilityState', {
      configurable: true,
      value: 'visible',
    });
    await act(async () => {
      window.document.dispatchEvent(new Event('visibilitychange'));
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(1);

    await act(async () => {
      jest.advanceTimersByTime(1);
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(2);

    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(3);

    unmount();
    window.document.dispatchEvent(new Event('visibilitychange'));
    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(3);
  });

  it('keeps the newest manual refresh when an older background refresh resolves later', async () => {
    jest.useFakeTimers();
    const backgroundContexts = deferred();
    const manualContexts = deferred();

    let cachedContextCalls = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaceContexts) {
        cachedContextCalls += 1;
        if (cachedContextCalls === 1) {
          return Promise.resolve(contextsPayload('initial-context'));
        }
        return backgroundContexts.promise;
      }
      if (fetcher === getEnabledCloudsList) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      if (fetcher === getManagedJobs) {
        return Promise.resolve({ jobs: [] });
      }
      if (fetcher === getClusters) {
        return Promise.resolve([]);
      }
      if (fetcher === getSlurmInfrastructure) {
        return Promise.resolve({
          allSlurmGPUs: [],
          perClusterSlurmGPUs: [],
          perNodeSlurmGPUs: [],
        });
      }
      if (fetcher === getSSHNodePools) {
        return Promise.resolve({});
      }
      if (fetcher === getEnabledCloudsBatch) {
        return Promise.resolve({});
      }
      if (
        fetcher === getCloudInfrastructure &&
        JSON.stringify(args) === '[false]'
      ) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      throw new Error(
        `Unexpected cache fetcher: ${fetcher?.name || 'unknown'}`
      );
    });
    getWorkspaceContexts.mockReturnValue(manualContexts.promise);

    render(<GPUs />);

    await screen.findByText('initial-context');

    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(2);

    await act(async () => {
      window.dispatchEvent(new Event('skydashboard:infra:refresh'));
      await Promise.resolve();
    });
    expect(getWorkspaceContexts).toHaveBeenCalledTimes(1);

    await act(async () => {
      manualContexts.resolve(contextsPayload('manual-context'));
      await manualContexts.promise;
    });
    await screen.findByText('manual-context');

    await act(async () => {
      backgroundContexts.resolve(contextsPayload('stale-context'));
      await backgroundContexts.promise;
    });

    expect(screen.getByText('manual-context')).toBeInTheDocument();
    expect(screen.queryByText('stale-context')).not.toBeInTheDocument();
  });

  it('does not let a stale background failure clear a newer manual refresh', async () => {
    jest.useFakeTimers();
    const backgroundContexts = deferred();
    const manualContexts = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);

    let cachedContextCalls = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaceContexts) {
        cachedContextCalls += 1;
        if (cachedContextCalls === 1) {
          return Promise.resolve(contextsPayload('initial-context'));
        }
        return backgroundContexts.promise;
      }
      if (fetcher === getEnabledCloudsList) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      if (fetcher === getManagedJobs) {
        return Promise.resolve({ jobs: [] });
      }
      if (fetcher === getClusters) {
        return Promise.resolve([]);
      }
      if (fetcher === getSlurmInfrastructure) {
        return Promise.resolve({
          allSlurmGPUs: [],
          perClusterSlurmGPUs: [],
          perNodeSlurmGPUs: [],
        });
      }
      if (fetcher === getSSHNodePools) {
        return Promise.resolve({});
      }
      if (fetcher === getEnabledCloudsBatch) {
        return Promise.resolve({});
      }
      if (
        fetcher === getCloudInfrastructure &&
        JSON.stringify(args) === '[false]'
      ) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      throw new Error(
        `Unexpected cache fetcher: ${fetcher?.name || 'unknown'}`
      );
    });
    getWorkspaceContexts.mockReturnValue(manualContexts.promise);

    render(<GPUs />);

    await screen.findByText('initial-context');
    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });

    await act(async () => {
      window.dispatchEvent(new Event('skydashboard:infra:refresh'));
      await Promise.resolve();
    });
    await act(async () => {
      manualContexts.resolve(contextsPayload('manual-context'));
      await manualContexts.promise;
    });
    await screen.findByText('manual-context');

    await act(async () => {
      backgroundContexts.reject(new Error('stale infra failure'));
      await Promise.allSettled([backgroundContexts.promise]);
    });

    expect(screen.getByText('manual-context')).toBeInTheDocument();
    expect(consoleError).not.toHaveBeenCalledWith(
      'Error in fetchKubernetesData:',
      expect.objectContaining({ message: 'stale infra failure' })
    );
    consoleError.mockRestore();
  });

  it('coalesces overlapping background polls onto one in-flight refresh', async () => {
    jest.useFakeTimers();
    const backgroundContexts = deferred();

    let cachedContextCalls = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaceContexts) {
        cachedContextCalls += 1;
        if (cachedContextCalls === 1) {
          return Promise.resolve(contextsPayload('initial-context'));
        }
        return backgroundContexts.promise;
      }
      if (fetcher === getEnabledCloudsList) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      if (fetcher === getManagedJobs) {
        return Promise.resolve({ jobs: [] });
      }
      if (fetcher === getClusters) {
        return Promise.resolve([]);
      }
      if (fetcher === getSlurmInfrastructure) {
        return Promise.resolve({
          allSlurmGPUs: [],
          perClusterSlurmGPUs: [],
          perNodeSlurmGPUs: [],
        });
      }
      if (fetcher === getSSHNodePools) {
        return Promise.resolve({});
      }
      if (fetcher === getEnabledCloudsBatch) {
        return Promise.resolve({});
      }
      if (
        fetcher === getCloudInfrastructure &&
        JSON.stringify(args) === '[false]'
      ) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      throw new Error(
        `Unexpected cache fetcher: ${fetcher?.name || 'unknown'}`
      );
    });

    render(<GPUs />);

    await screen.findByText('initial-context');

    await act(async () => {
      jest.advanceTimersByTime(1000);
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(2);

    await act(async () => {
      jest.advanceTimersByTime(2000);
      await Promise.resolve();
    });
    expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(2);

    await act(async () => {
      backgroundContexts.resolve(contextsPayload('background-context'));
      await backgroundContexts.promise;
    });
    await screen.findByText('background-context');
  });

  it('revokes request ownership on unmount before the context fanout starts', async () => {
    const initialContexts = deferred();
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaceContexts) {
        return initialContexts.promise;
      }
      if (fetcher === getEnabledCloudsList) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      if (fetcher === getManagedJobs) {
        return Promise.resolve({ jobs: [] });
      }
      if (fetcher === getClusters) {
        return Promise.resolve([]);
      }
      if (fetcher === getSlurmInfrastructure) {
        return Promise.resolve({
          allSlurmGPUs: [],
          perClusterSlurmGPUs: [],
          perNodeSlurmGPUs: [],
        });
      }
      if (fetcher === getSSHNodePools) {
        return Promise.resolve({});
      }
      if (fetcher === getEnabledCloudsBatch) {
        return Promise.resolve({});
      }
      if (
        fetcher === getCloudInfrastructure &&
        JSON.stringify(args) === '[false]'
      ) {
        return Promise.resolve({ clouds: [], totalClouds: 0 });
      }
      throw new Error(
        `Unexpected cache fetcher: ${fetcher?.name || 'unknown'}`
      );
    });

    const { unmount } = render(<GPUs />);
    await waitFor(() =>
      expect(cacheCallsFor(getWorkspaceContexts)).toHaveLength(1)
    );

    unmount();
    await act(async () => {
      initialContexts.resolve(contextsPayload('late-context'));
      await initialContexts.promise;
    });

    expect(getContextGPUData).not.toHaveBeenCalled();
  });

  it('passes a deploy handler that preserves the queued request id', async () => {
    router.query = { context: 'ssh-gpu-pool' };
    router.asPath = '/infra/ssh-gpu-pool';
    deploySSHNodePool.mockResolvedValue({ request_id: 'deploy-123' });

    render(<GPUs />);

    await waitFor(() =>
      expect(lastSshNodePoolDetailsProps?.poolName).toBe('gpu-pool')
    );

    const result =
      await lastSshNodePoolDetailsProps.handleDeploySSHPool('gpu-pool');

    expect(result).toEqual({ request_id: 'deploy-123' });
    expect(deploySSHNodePool).toHaveBeenCalledTimes(1);
    expect(deploySSHNodePool).toHaveBeenCalledWith('gpu-pool');
  });
});
