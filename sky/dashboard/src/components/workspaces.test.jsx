import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';

import { Workspaces } from '@/components/workspaces';
import { getClusters } from '@/data/connectors/clusters';
import { getManagedJobs } from '@/data/connectors/jobs';
import {
  deleteWorkspace,
  getEnabledCloudsBatch,
  getWorkspaces,
} from '@/data/connectors/workspaces';
import { apiClient, getCurrentUserRole } from '@/data/connectors/client';
import cachePreloader from '@/lib/cache-preloader';
import dashboardCache from '@/lib/cache';
import { REFRESH_INTERVALS } from '@/lib/config';

const mockRouterPush = jest.fn();

jest.mock('next/router', () => ({
  useRouter: () => ({ push: mockRouterPush }),
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
  getCurrentUserRole: jest.fn(),
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

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
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
    getCurrentUserRole.mockResolvedValue({
      role: 'admin',
      name: 'Admin',
      id: 'admin-id',
    });
  });

  afterEach(() => {
    jest.useRealTimers();
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

  it('preserves workspace table filtering, labels, and protected actions', async () => {
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getWorkspaces) {
        return Promise.resolve({ default: { private: true }, beta: {} });
      }
      if (fetcher === getEnabledCloudsBatch) {
        return Promise.resolve({ default: ['kubernetes'], beta: ['aws'] });
      }
      if (fetcher === getClusters) {
        return Promise.resolve([
          { workspace: 'default', status: 'RUNNING' },
          { workspace: 'beta', status: 'RUNNING' },
        ]);
      }
      if (fetcher === getManagedJobs) {
        return Promise.resolve({
          jobs: [{ workspace: 'beta', status: 'RUNNING' }],
        });
      }
      throw new Error('Unexpected cache fetcher');
    });

    render(<Workspaces />);

    await screen.findByRole('button', { name: 'default' });
    expect(getCurrentUserRole).not.toHaveBeenCalled();
    expect(screen.getByText('Private')).toBeInTheDocument();
    expect(screen.getByText('Public')).toBeInTheDocument();
    expect(screen.getByRole('link', { name: 'Kubernetes' })).toHaveAttribute(
      'href',
      '/infra'
    );
    expect(screen.getByTitle('Cannot delete default workspace')).toBeDisabled();
    expect(screen.getByTitle('Delete workspace')).toBeEnabled();

    fireEvent.change(screen.getByPlaceholderText('Filter workspaces'), {
      target: { value: 'kubernetes' },
    });
    expect(screen.getByRole('button', { name: 'default' })).toBeInTheDocument();
    expect(
      screen.queryByRole('button', { name: 'beta' })
    ).not.toBeInTheDocument();

    fireEvent.click(screen.getByTitle('Clear search'));
    const betaButton = screen.getByRole('button', { name: 'beta' });
    const betaRow = betaButton.closest('tr');
    expect(betaRow).not.toBeNull();
    const betaActions = within(betaRow).getAllByRole('button');
    fireEvent.click(betaActions[1]);
    expect(mockRouterPush).toHaveBeenCalledWith({
      pathname: '/clusters',
      query: { workspace: 'beta' },
    });
    fireEvent.click(betaActions[2]);
    expect(mockRouterPush).toHaveBeenCalledWith({
      pathname: '/jobs',
      query: { workspace: 'beta' },
    });
    fireEvent.click(betaButton);
    await waitFor(() => {
      expect(mockRouterPush).toHaveBeenCalledWith('/workspaces/beta');
    });
    expect(getCurrentUserRole).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('columnheader', { name: 'Workspace ↑' }));
    expect(
      screen.getByRole('columnheader', { name: 'Workspace ↓' })
    ).toBeInTheDocument();

    fireEvent.click(screen.getByTitle('Delete workspace'));
    expect(await screen.findByText('Delete Workspace')).toBeInTheDocument();
    expect(getCurrentUserRole).toHaveBeenCalledTimes(2);
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

  it('reuses one in-flight interval refresh instead of starting a second sweep', async () => {
    jest.useFakeTimers();
    const intervalRefresh = deferred();
    let workspaceCall = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaces) {
        workspaceCall += 1;
        if (workspaceCall === 1) return Promise.resolve({ alpha: {} });
        if (workspaceCall >= 2) return intervalRefresh.promise;
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

    dashboardCache.get.mockClear();

    await act(async () => {
      jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
      await Promise.resolve();
    });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(1));

    await act(async () => {
      jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
      await Promise.resolve();
    });
    expect(callsFor(getWorkspaces)).toHaveLength(1);

    await act(async () => {
      intervalRefresh.resolve({ alpha: {} });
      await intervalRefresh.promise;
    });
  });

  it('supersedes an in-flight interval sweep after deleting a workspace', async () => {
    jest.useFakeTimers();
    const intervalRefresh = deferred();
    const postDeleteRefresh = deferred();
    let workspaceCall = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaces) {
        workspaceCall += 1;
        if (workspaceCall === 1) return Promise.resolve({ alpha: {} });
        if (workspaceCall === 2) return intervalRefresh.promise;
        return postDeleteRefresh.promise;
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
    deleteWorkspace.mockResolvedValue(undefined);

    render(<Workspaces />);
    await screen.findByText('alpha');

    await act(async () => {
      jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
      await Promise.resolve();
    });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(2));

    fireEvent.click(screen.getByTitle('Delete workspace'));
    await screen.findByText('Delete Workspace');
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await waitFor(() => expect(deleteWorkspace).toHaveBeenCalledWith('alpha'));
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(3));

    await act(async () => {
      postDeleteRefresh.resolve({});
      intervalRefresh.resolve({ alpha: {} });
      await Promise.all([postDeleteRefresh.promise, intervalRefresh.promise]);
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });

    await waitFor(() =>
      expect(screen.queryByText('alpha')).not.toBeInTheDocument()
    );
  });

  it('keeps manual refresh ownership when an interval tick fires', async () => {
    jest.useFakeTimers();
    const manualRefresh = deferred();
    let workspaceCall = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaces) {
        workspaceCall += 1;
        if (workspaceCall === 1) return Promise.resolve({ alpha: {} });
        if (workspaceCall >= 2) return manualRefresh.promise;
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
    apiClient.fetch.mockResolvedValue({});

    render(<Workspaces />);
    await screen.findByText('alpha');

    fireEvent.keyDown(window, { key: 'r', ctrlKey: true });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(2));

    const refreshButton = screen.getByText('Refresh').closest('button');
    expect(refreshButton).toBeDisabled();

    await act(async () => {
      jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
      await Promise.resolve();
    });
    expect(callsFor(getWorkspaces)).toHaveLength(2);
    expect(refreshButton).toBeDisabled();

    await act(async () => {
      manualRefresh.resolve({ alpha: {} });
      await manualRefresh.promise;
    });

    await waitFor(() => expect(refreshButton).toBeEnabled());
  });

  it('does not let an interval tick supersede a pending manual health check', async () => {
    jest.useFakeTimers();
    const healthCheck = deferred();
    installSuccessfulFetches();
    apiClient.fetch.mockReturnValue(healthCheck.promise);

    render(<Workspaces />);
    await screen.findByText('alpha');
    dashboardCache.get.mockClear();

    fireEvent.keyDown(window, { key: 'r', ctrlKey: true });
    await waitFor(() => expect(apiClient.fetch).toHaveBeenCalledTimes(1));
    const refreshButton = screen.getByText('Refresh').closest('button');
    expect(refreshButton).toBeDisabled();

    await act(async () => {
      jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
      await Promise.resolve();
    });
    expect(callsFor(getWorkspaces)).toHaveLength(0);
    expect(refreshButton).toBeDisabled();

    await act(async () => {
      healthCheck.resolve({});
      await healthCheck.promise;
    });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(1));
    await waitFor(() => expect(refreshButton).toBeEnabled());

    dashboardCache.get.mockClear();
    await act(async () => {
      jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
      await Promise.resolve();
    });
    await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(1));
  });

  it('refreshes immediately on visibility restore and skips the adjacent timer boundary', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    installSuccessfulFetches();
    setDocumentVisibility('hidden');

    const { unmount } = render(<Workspaces />);
    let mounted = true;

    try {
      await screen.findByText('alpha');
      dashboardCache.get.mockClear();

      await act(async () => {
        jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL - 1);
        await Promise.resolve();
      });
      expect(callsFor(getWorkspaces)).toHaveLength(0);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(1));

      await act(async () => {
        jest.advanceTimersByTime(1);
        await Promise.resolve();
      });
      expect(callsFor(getWorkspaces)).toHaveLength(1);

      unmount();
      mounted = false;
      dashboardCache.get.mockClear();
      window.document.dispatchEvent(new Event('visibilitychange'));
      await act(async () => {
        jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
        await Promise.resolve();
      });
      expect(callsFor(getWorkspaces)).toHaveLength(0);
    } finally {
      if (mounted) {
        unmount();
      }
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('fences a pre-hide automatic sweep when visibility restore starts a fresh read', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const staleRefresh = deferred();
    const visibleRefresh = deferred();
    let workspaceCall = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaces) {
        workspaceCall += 1;
        if (workspaceCall === 1) return Promise.resolve({ alpha: {} });
        if (workspaceCall === 2) return staleRefresh.promise;
        if (workspaceCall === 3) return visibleRefresh.promise;
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
    setDocumentVisibility('visible');

    const { unmount } = render(<Workspaces />);
    let mounted = true;

    try {
      await screen.findByText('alpha');

      await act(async () => {
        jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
        await Promise.resolve();
      });
      await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(2));

      setDocumentVisibility('hidden');
      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(3));

      await act(async () => {
        staleRefresh.resolve({ beta: {} });
        await staleRefresh.promise;
      });
      expect(screen.getByText('alpha')).toBeInTheDocument();
      expect(screen.queryByText('beta')).not.toBeInTheDocument();

      await act(async () => {
        visibleRefresh.resolve({ gamma: {} });
        await visibleRefresh.promise;
      });
      await screen.findByText('gamma');
      expect(screen.queryByText('beta')).not.toBeInTheDocument();
    } finally {
      if (mounted) {
        unmount();
      }
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('keeps manual refresh ownership when visibility returns', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const healthCheck = deferred();
    const manualRefresh = deferred();
    let workspaceCall = 0;
    dashboardCache.get.mockImplementation((fetcher, args) => {
      if (fetcher === getWorkspaces) {
        workspaceCall += 1;
        if (workspaceCall === 1) return Promise.resolve({ alpha: {} });
        if (workspaceCall >= 2) return manualRefresh.promise;
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
    apiClient.fetch.mockReturnValue(healthCheck.promise);
    setDocumentVisibility('hidden');

    const { unmount } = render(<Workspaces />);
    let mounted = true;

    try {
      await screen.findByText('alpha');
      fireEvent.keyDown(window, { key: 'r', ctrlKey: true });
      await waitFor(() => expect(apiClient.fetch).toHaveBeenCalledTimes(1));
      expect(screen.getByText('Refresh').closest('button')).toBeDisabled();

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(callsFor(getWorkspaces)).toHaveLength(1);

      await act(async () => {
        healthCheck.resolve({});
        await healthCheck.promise;
      });
      await waitFor(() => expect(callsFor(getWorkspaces)).toHaveLength(2));

      await act(async () => {
        manualRefresh.resolve({ alpha: {} });
        await manualRefresh.promise;
      });
      await waitFor(() =>
        expect(screen.getByText('Refresh').closest('button')).toBeEnabled()
      );
    } finally {
      if (mounted) {
        unmount();
      }
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('clears loading state when the current manual refresh fails', async () => {
    jest.useFakeTimers();
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

    dashboardCache.get.mockClear();
    await act(async () => {
      jest.advanceTimersByTime(REFRESH_INTERVALS.REFRESH_INTERVAL);
      await Promise.resolve();
    });
    expect(callsFor(getWorkspaces)).toHaveLength(1);
    consoleError.mockRestore();
  });
});
