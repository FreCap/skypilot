import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import {
  Clusters,
  Status2Actions,
  enabledActions,
  handleVSCodeConnection,
} from '@/components/clusters';
import {
  Status2Actions as ExtractedStatus2Actions,
  enabledActions as extractedEnabledActions,
  handleVSCodeConnection as extractedHandleVSCodeConnection,
} from '@/components/cluster-actions';
import {
  getClusters,
  getClusterHistory,
  useClusterData,
} from '@/data/connectors/clusters';
import { getWorkspaces } from '@/data/connectors/workspaces';
import dashboardCache from '@/lib/cache';
import cachePreloader from '@/lib/cache-preloader';
import { trackClusterAction } from '@/lib/analytics';

const actualCachePreloader = jest.requireActual(
  '@/lib/cache-preloader'
).default;

const router = {
  isReady: true,
  pathname: '/clusters',
  query: {},
  replace: jest.fn(),
};

jest.mock('next/router', () => ({
  useRouter: () => router,
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/lib/analytics', () => ({
  trackClusterAction: jest.fn(),
  trackFilterUsed: jest.fn(),
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
    setPreloader: jest.fn(),
  },
}));

jest.mock('@/data/connectors/clusters', () => ({
  getClusters: jest.fn(),
  getClusterHistory: jest.fn(),
  useClusterData: jest.fn(),
}));

jest.mock('@/data/connectors/workspaces', () => ({
  getWorkspaces: jest.fn(),
}));

jest.mock('@/plugins/PluginProvider', () => ({
  useTableColumns: () => [],
}));

jest.mock('@/plugins/PluginSlot', () => ({
  PluginSlot: ({ fallback }) => fallback,
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

const refreshClusters = jest.fn();

describe('Clusters preload lifecycle', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    router.query = {};
    cachePreloader.preloadForPage.mockResolvedValue(undefined);
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getClusters) return Promise.resolve([]);
      if (fetcher === getWorkspaces) return Promise.resolve({});
      throw new Error('Unexpected cache fetcher');
    });
    useClusterData.mockReturnValue({
      data: [],
      allData: [],
      total: 0,
      page: 1,
      limit: 10,
      totalPages: 1,
      hasNext: false,
      hasPrev: false,
      setPage: jest.fn(),
      setLimit: jest.fn(),
      loading: false,
      refresh: refreshClusters,
      isServerPagination: false,
    });
  });

  it('uses the preloader as the only owner of foreground cache reads', async () => {
    render(<Clusters />);

    await screen.findByText(/Updated just now/);

    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(1);
    expect(cachePreloader.preloadForPage).toHaveBeenCalledWith('clusters');
    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('invalidates and fetches each cluster preload key exactly once when forced', async () => {
    await actualCachePreloader.preloadForPage('clusters', {
      force: true,
      backgroundPreload: false,
    });

    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getClusters, []);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getWorkspaces, []);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenCalledWith(getClusters, []);
    expect(dashboardCache.get).toHaveBeenCalledWith(getWorkspaces, []);
  });

  it('does not continue a preload that finishes after unmount', async () => {
    const preload = deferred();
    cachePreloader.preloadForPage.mockReturnValue(preload.promise);
    const { unmount } = render(<Clusters />);

    unmount();
    await act(async () => {
      preload.resolve();
      await preload.promise;
    });

    expect(dashboardCache.get).not.toHaveBeenCalled();
    expect(refreshClusters).not.toHaveBeenCalled();
  });

  it('finishes the current load when the preloader fails', async () => {
    cachePreloader.preloadForPage.mockRejectedValue(
      new Error('preload unavailable')
    );
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);

    render(<Clusters />);

    await screen.findByText(/Updated just now/);
    expect(dashboardCache.get).not.toHaveBeenCalled();
    expect(consoleError).toHaveBeenCalledWith(
      'Error preloading clusters data:',
      expect.objectContaining({ message: 'preload unavailable' })
    );
    consoleError.mockRestore();
  });

  it('lets a manual refresh supersede the initial preload', async () => {
    const initialPreload = deferred();
    const refreshPreload = deferred();
    cachePreloader.preloadForPage
      .mockReturnValueOnce(initialPreload.promise)
      .mockReturnValueOnce(refreshPreload.promise);
    render(<Clusters />);

    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(
      2,
      'clusters',
      { force: true }
    );
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();

    await act(async () => {
      initialPreload.resolve();
      await initialPreload.promise;
    });
    expect(dashboardCache.get).not.toHaveBeenCalled();
    expect(refreshClusters).not.toHaveBeenCalled();

    await act(async () => {
      refreshPreload.resolve();
      await refreshPreload.promise;
    });
    await waitFor(() => expect(refreshClusters).toHaveBeenCalledTimes(1));
    expect(screen.getByText(/Updated just now/)).toBeInTheDocument();
  });

  it('invalidates the visible history cache key', async () => {
    router.query = { history: 'true', historyDays: '5' };
    render(<Clusters />);
    await screen.findByText(/Updated just now/);

    await act(async () => {
      fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
      await Promise.resolve();
    });

    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getClusterHistory, [
      null,
      5,
    ]);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
  });
});

describe('Cluster actions', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('enables connection actions only for running clusters', () => {
    expect(enabledActions('RUNNING')).toEqual(['connect', 'VSCode']);
    expect(enabledActions('STOPPED')).toEqual([]);
  });

  it('preserves the clusters module exports as direct aliases', () => {
    expect(Status2Actions).toBe(ExtractedStatus2Actions);
    expect(enabledActions).toBe(extractedEnabledActions);
    expect(handleVSCodeConnection).toBe(extractedHandleVSCodeConnection);
  });

  it('dispatches running-cluster actions with analytics', () => {
    const onOpenSSHModal = jest.fn();
    const onOpenVSCodeModal = jest.fn();

    render(
      <Status2Actions
        withLabel
        cluster="cluster-a"
        status="RUNNING"
        onOpenSSHModal={onOpenSSHModal}
        onOpenVSCodeModal={onOpenVSCodeModal}
      />
    );

    fireEvent.click(screen.getByRole('button', { name: 'Connect' }));
    fireEvent.click(screen.getByRole('button', { name: 'VSCode' }));

    expect(onOpenSSHModal).toHaveBeenCalledWith('cluster-a');
    expect(onOpenVSCodeModal).toHaveBeenCalledWith('cluster-a');
    expect(trackClusterAction).toHaveBeenNthCalledWith(1, 'connect', {
      status: 'RUNNING',
    });
    expect(trackClusterAction).toHaveBeenNthCalledWith(2, 'VSCode', {
      status: 'RUNNING',
    });
  });

  it('renders disabled actions without clickable controls', () => {
    render(<Status2Actions withLabel cluster="cluster-a" status="STOPPED" />);

    expect(screen.queryAllByRole('button')).toHaveLength(0);
    expect(screen.getByTitle('connect')).toHaveTextContent('Connect');
    expect(screen.getByTitle('VSCode')).toHaveTextContent('VSCode');
  });

  it('falls back to the SSH URI when no modal callback is provided', () => {
    const open = jest.spyOn(window, 'open').mockImplementation(() => null);

    render(<Status2Actions withLabel cluster="cluster-a" status="RUNNING" />);
    fireEvent.click(screen.getByRole('button', { name: 'Connect' }));

    expect(open).toHaveBeenCalledWith('ssh://cluster-a');
    open.mockRestore();
  });

  it('keeps direct VS Code dispatch as a no-op without a callback', () => {
    const onOpenVSCodeModal = jest.fn();

    handleVSCodeConnection('cluster-a', onOpenVSCodeModal);
    handleVSCodeConnection('cluster-b');

    expect(onOpenVSCodeModal).toHaveBeenCalledWith('cluster-a');
  });
});
