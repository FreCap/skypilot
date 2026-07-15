import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import { Workspaces } from '@/components/workspaces';
import { getClusters } from '@/data/connectors/clusters';
import { getManagedJobs } from '@/data/connectors/jobs';
import {
  getEnabledCloudsBatch,
  getWorkspaces,
} from '@/data/connectors/workspaces';
import { apiClient } from '@/data/connectors/client';
import cachePreloader from '@/lib/cache-preloader';
import dashboardCache from '@/lib/cache';

jest.mock('next/router', () => ({
  useRouter: () => ({ push: jest.fn() }),
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/lib/analytics', () => ({
  trackWorkspaceAction: jest.fn(),
}));

jest.mock('@/lib/cache-preloader', () => ({
  __esModule: true,
  default: { preloadForPage: jest.fn() },
}));

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
  },
}));

jest.mock('@/data/connectors/workspaces', () => ({
  getWorkspaces: jest.fn(),
  getEnabledCloudsBatch: jest.fn(),
  deleteWorkspace: jest.fn(),
}));

jest.mock('@/data/connectors/clusters', () => ({
  getClusters: jest.fn(),
}));

jest.mock('@/data/connectors/jobs', () => ({
  getManagedJobs: jest.fn(),
}));

jest.mock('@/data/connectors/client', () => ({
  apiClient: {
    fetch: jest.fn().mockResolvedValue({}),
    get: jest.fn(),
  },
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

function callsFor(fetcher) {
  return dashboardCache.get.mock.calls.filter(([candidate]) => {
    return candidate === fetcher;
  });
}

function installSuccessfulFetches() {
  dashboardCache.get.mockImplementation((fetcher) => {
    if (fetcher === getWorkspaces) return Promise.resolve({ alpha: {} });
    if (fetcher === getEnabledCloudsBatch) {
      return Promise.resolve({ alpha: ['aws'] });
    }
    if (fetcher === getClusters) {
      return Promise.resolve([{ workspace: 'alpha', status: 'RUNNING' }]);
    }
    if (fetcher === getManagedJobs) {
      return Promise.resolve({
        jobs: [{ workspace: 'alpha', status: 'RUNNING' }],
      });
    }
    throw new Error('Unexpected cache fetcher');
  });
}

describe('Workspaces request lifecycle', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    cachePreloader.preloadForPage.mockResolvedValue(undefined);
  });

  it('runs exactly one aggregation sweep on mount', async () => {
    installSuccessfulFetches();

    render(<Workspaces />);

    await screen.findByText('alpha');
    await act(async () => {
      await Promise.resolve();
    });

    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(1);
    expect(callsFor(getWorkspaces)).toHaveLength(1);
    expect(callsFor(getEnabledCloudsBatch)).toHaveLength(1);
    expect(callsFor(getClusters)).toHaveLength(1);
    expect(callsFor(getManagedJobs)).toHaveLength(1);
  });

  it('does not start a request when preload finishes after unmount', async () => {
    const preload = deferred();
    cachePreloader.preloadForPage.mockReturnValue(preload.promise);
    installSuccessfulFetches();

    const { unmount } = render(<Workspaces />);
    unmount();

    await act(async () => {
      preload.resolve();
      await preload.promise;
    });

    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('stops an in-flight request after unmount', async () => {
    const workspaces = deferred();
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getWorkspaces) return workspaces.promise;
      throw new Error('Unmounted request continued to the next stage');
    });

    const { unmount } = render(<Workspaces />);
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(1));
    unmount();

    await act(async () => {
      workspaces.resolve({ alpha: {} });
      await workspaces.promise;
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });

  it('ignores an in-flight request failure after unmount', async () => {
    const workspaces = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getWorkspaces) return workspaces.promise;
      throw new Error('Unmounted request continued to the next stage');
    });

    const { unmount } = render(<Workspaces />);
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(1));
    unmount();

    await act(async () => {
      workspaces.reject(new Error('stale workspace failure'));
      await Promise.allSettled([workspaces.promise]);
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(consoleError).not.toHaveBeenCalled();
    consoleError.mockRestore();
  });

  it('keeps the newest refresh when overlapping requests settle out of order', async () => {
    const olderRefresh = deferred();
    const newerRefresh = deferred();
    let workspaceCall = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaces) {
        workspaceCall += 1;
        if (workspaceCall === 1) return Promise.resolve({ alpha: {} });
        if (workspaceCall === 2) return olderRefresh.promise;
        if (workspaceCall === 3) return newerRefresh.promise;
      }
      if (fetcher === getEnabledCloudsBatch) {
        const names = args[0];
        return Promise.resolve(
          Object.fromEntries(names.map((name) => [name, ['aws']]))
        );
      }
      if (fetcher === getClusters) return Promise.resolve([]);
      if (fetcher === getManagedJobs) return Promise.resolve({ jobs: [] });
      throw new Error('Unexpected cache fetcher');
    });

    render(<Workspaces />);
    await screen.findByText('alpha');

    fireEvent.keyDown(window, { key: 'r', ctrlKey: true });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(2));
    fireEvent.keyDown(window, { key: 'r', ctrlKey: true });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(3));

    await act(async () => {
      newerRefresh.resolve({ gamma: {} });
      await newerRefresh.promise;
    });
    await screen.findByText('gamma');

    await act(async () => {
      olderRefresh.resolve({ beta: {} });
      await olderRefresh.promise;
    });

    expect(screen.getByText('gamma')).toBeInTheDocument();
    expect(screen.queryByText('beta')).not.toBeInTheDocument();
    expect(callsFor(getWorkspaces)).toHaveLength(3);
  });

  it('clears loading state when the current manual refresh fails', async () => {
    installSuccessfulFetches();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);

    render(<Workspaces />);
    await screen.findByText('alpha');

    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getWorkspaces) {
        return Promise.reject(new Error('workspace refresh failed'));
      }
      throw new Error('Failed refresh continued to the next stage');
    });
    apiClient.fetch.mockResolvedValueOnce({});

    fireEvent.keyDown(window, { key: 'r', ctrlKey: true });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(2));
    await waitFor(() => {
      expect(screen.getByText('Refresh').closest('button')).toBeEnabled();
    });
    expect(screen.queryAllByText('Loading...')).toHaveLength(0);
    expect(consoleError).toHaveBeenCalledWith(
      'Error fetching workspace data:',
      expect.any(Error)
    );
    consoleError.mockRestore();
  });
});
