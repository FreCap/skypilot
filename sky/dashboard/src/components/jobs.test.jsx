import {
  act,
  fireEvent,
  render,
  renderHook,
  screen,
  within,
  waitFor,
} from '@testing-library/react';
import {
  ManagedJobs,
  ManagedJobsTable,
  useManagedJobsPageData,
  filterJobsByName,
  filterJobsByPool,
  filterJobsByUser,
  filterJobsByWorkspace,
  getAggregatedStatus,
  statusGroups,
} from '@/components/jobs';
import * as jobDomain from '@/components/job-domain';
import * as jobsFacade from '@/components/jobs';
import { REFRESH_INTERVAL } from '@/components/utils';
import dashboardCache from '@/lib/cache';
import cachePreloader from '@/lib/cache-preloader';
import jobsCacheManager from '@/lib/jobs-cache-manager';
import { getPoolStatus } from '@/data/connectors/jobs';
import { getCurrentUserInfo } from '@/data/connectors/client';
import { getUsers } from '@/data/connectors/users';
import { getWorkspaces } from '@/data/connectors/workspaces';

jest.mock('next/router', () => {
  const router = {
    isReady: true,
    query: {},
    push: jest.fn(),
    replace: jest.fn(),
  };
  return { useRouter: () => router };
});

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
  },
}));

jest.mock('@/lib/cache-preloader', () => ({
  __esModule: true,
  default: {
    preloadForPage: jest.fn(),
  },
}));

jest.mock('@/lib/jobs-cache-manager', () => ({
  __esModule: true,
  default: {
    getPaginatedJobs: jest.fn(),
    prefetchNextPage: jest.fn(),
    invalidateCache: jest.fn(),
  },
}));

jest.mock('@/data/connectors/client', () => ({
  apiClient: jest.fn(),
  getCurrentUserInfo: jest.fn(),
}));

beforeEach(() => {
  jest.clearAllMocks();
  cachePreloader.preloadForPage.mockResolvedValue(undefined);
  dashboardCache.get.mockImplementation((fetcher) => {
    if (fetcher === getPoolStatus) {
      return Promise.resolve({ pools: [] });
    }
    return Promise.resolve([]);
  });
  jobsCacheManager.getPaginatedJobs.mockResolvedValue({
    jobs: [],
    pools: [],
    total: 0,
    totalNoFilter: 0,
    statusCounts: {},
    controllerStopped: false,
    hasNext: false,
  });
  getCurrentUserInfo.mockResolvedValue({ id: 'local', name: 'local' });
});

const deferred = () => {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
};

const countCacheReads = (fetcher) =>
  dashboardCache.get.mock.calls.filter(([fn]) => fn === fetcher).length;

const setDocumentVisibility = (value) => {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
};

describe('managed jobs page initialization', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it('performs one preload sweep for one mount', async () => {
    render(<ManagedJobs />);

    await screen.findByText(/Updated just now/);
    await act(async () => {
      await Promise.resolve();
    });

    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(1);
    expect(cachePreloader.preloadForPage).toHaveBeenCalledWith('jobs');
    expect(
      dashboardCache.get.mock.calls.filter(
        ([fetcher]) => fetcher === getPoolStatus
      )
    ).toHaveLength(1);
  });

  it('renders the pool row contract from the pool snapshot', async () => {
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getPoolStatus) {
        return Promise.resolve({
          pools: [
            {
              name: 'training-pool',
              jobCounts: { RUNNING: 2 },
              replica_info: [
                { status: 'READY', cloud: 'AWS', region: 'us-east-1' },
                { status: 'STOPPED', cloud: 'AWS', region: 'us-east-1' },
              ],
              target_num_replicas: 3,
              requested_resources_str: '1x A100',
            },
          ],
        });
      }
      return Promise.resolve([]);
    });

    render(<ManagedJobs />);

    const poolLink = await screen.findByRole('link', {
      name: 'training-pool',
    });
    const poolRow = poolLink.closest('tr');
    expect(poolLink).toHaveAttribute('href', '/jobs/pools/training-pool');
    expect(within(poolRow).getByText('1 (target: 3)')).toBeInTheDocument();
    expect(within(poolRow).getByText('1x A100')).toBeInTheDocument();
    expect(within(poolRow).getByText('RUNNING')).toBeInTheDocument();
    expect(within(poolRow).getByText('AWS (1 region)')).toBeInTheDocument();
    expect(
      within(poolRow).getByRole('link', { name: 'See all jobs' })
    ).toHaveAttribute('href', expect.stringContaining('pool'));
  });

  it('does not start the pool request when preload finishes after unmount', async () => {
    const preload = deferred();
    cachePreloader.preloadForPage.mockReturnValue(preload.promise);
    const { unmount } = renderHook(() => useManagedJobsPageData());

    unmount();
    await act(async () => {
      preload.resolve();
      await preload.promise;
    });

    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('continues to the pool snapshot when preload fails', async () => {
    cachePreloader.preloadForPage.mockRejectedValue(
      new Error('preload unavailable')
    );
    dashboardCache.get.mockResolvedValue({ pools: [{ name: 'pool-a' }] });
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const { result } = renderHook(() => useManagedJobsPageData());

    await waitFor(() => {
      expect(result.current.poolsData).toEqual([{ name: 'pool-a' }]);
    });
    expect(result.current.preloadingComplete).toBe(true);
    expect(result.current.poolsLoading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(consoleError).toHaveBeenCalledWith(
      'Error preloading jobs data:',
      expect.objectContaining({ message: 'preload unavailable' })
    );
    consoleError.mockRestore();
  });

  it('lets the latest refresh own the pool snapshot and child fanout', async () => {
    const initialPools = deferred();
    const refreshedPools = deferred();
    dashboardCache.get
      .mockReturnValueOnce(initialPools.promise)
      .mockReturnValueOnce(refreshedPools.promise);
    const { result } = renderHook(() => useManagedJobsPageData());

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    const refreshJobs = jest.fn();
    result.current.jobsRefreshRef.current = refreshJobs;

    let refreshPromise;
    await act(async () => {
      refreshPromise = result.current.handleRefresh();
      await Promise.resolve();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(refreshJobs).toHaveBeenCalledTimes(1);

    await act(async () => {
      refreshedPools.resolve({ pools: [{ name: 'fresh-pool' }] });
      await Promise.all([refreshedPools.promise, refreshPromise]);
    });
    expect(result.current.poolsData).toEqual([{ name: 'fresh-pool' }]);
    expect(result.current.poolsLoading).toBe(false);

    await act(async () => {
      initialPools.resolve({ pools: [{ name: 'stale-pool' }] });
      await initialPools.promise;
    });
    expect(result.current.poolsData).toEqual([{ name: 'fresh-pool' }]);
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(1, 'jobs');
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(2, 'jobs', {
      force: true,
    });
    expect(jobsCacheManager.invalidateCache).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getPoolStatus, [{}]);
  });

  it('ignores a stale request failure after a refresh succeeds', async () => {
    const initialPools = deferred();
    const refreshedPools = deferred();
    dashboardCache.get
      .mockReturnValueOnce(initialPools.promise)
      .mockReturnValueOnce(refreshedPools.promise);
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const { result } = renderHook(() => useManagedJobsPageData());

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    let refreshPromise;
    await act(async () => {
      refreshPromise = result.current.handleRefresh();
      await Promise.resolve();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    await act(async () => {
      refreshedPools.resolve({ pools: [{ name: 'fresh-pool' }] });
      await Promise.all([refreshedPools.promise, refreshPromise]);
    });
    await act(async () => {
      initialPools.reject(new Error('stale pool failure'));
      await expect(initialPools.promise).rejects.toThrow('stale pool failure');
    });

    expect(result.current.poolsData).toEqual([{ name: 'fresh-pool' }]);
    expect(consoleError).not.toHaveBeenCalledWith(
      'Error fetching data:',
      expect.objectContaining({ message: 'stale pool failure' })
    );
    consoleError.mockRestore();
  });

  it('keeps the visible pools table on the latest manual refresh', async () => {
    const initialPoolRequestA = deferred();
    const initialPoolRequestB = deferred();
    const refreshedPools = deferred();
    let poolRequestCount = 0;
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher !== getPoolStatus) return Promise.resolve([]);
      poolRequestCount += 1;
      if (poolRequestCount === 1) return initialPoolRequestA.promise;
      return refreshedPools.promise;
    });
    render(<ManagedJobs />);

    await waitFor(() => expect(poolRequestCount).toBe(1));
    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    await waitFor(() => expect(refreshButton).toBeEnabled());
    fireEvent.click(refreshButton);
    await waitFor(() => expect(poolRequestCount).toBe(2));

    await act(async () => {
      refreshedPools.resolve({
        pools: [{ name: 'fresh-pool', replica_info: [] }],
      });
      await refreshedPools.promise;
    });
    await screen.findAllByText('fresh-pool');

    await act(async () => {
      initialPoolRequestA.resolve({
        pools: [{ name: 'stale-pool', replica_info: [] }],
      });
      await initialPoolRequestA.promise;
    });

    await waitFor(() => {
      expect(screen.queryByText('stale-pool')).not.toBeInTheDocument();
    });
    expect(screen.getAllByText('fresh-pool')).not.toHaveLength(0);
  });

  it('coalesces concurrent forced refresh sweeps into one preload and child fanout', async () => {
    const forcedPreload = deferred();
    const forcedPools = deferred();
    cachePreloader.preloadForPage
      .mockResolvedValueOnce(undefined)
      .mockImplementationOnce(() => forcedPreload.promise);
    dashboardCache.get
      .mockResolvedValueOnce({ pools: [{ name: 'initial-pool' }] })
      .mockImplementationOnce(() => forcedPools.promise);

    const { result } = renderHook(() => useManagedJobsPageData());

    await waitFor(() => {
      expect(result.current.poolsData).toEqual([{ name: 'initial-pool' }]);
    });
    const refreshJobs = jest.fn();
    result.current.jobsRefreshRef.current = refreshJobs;

    await act(async () => {
      result.current.handleRefresh();
      result.current.handleRefresh();
      await Promise.resolve();
    });

    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(2);
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(2, 'jobs', {
      force: true,
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(refreshJobs).not.toHaveBeenCalled();

    await act(async () => {
      forcedPreload.resolve();
      await Promise.resolve();
    });

    expect(refreshJobs).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      forcedPools.resolve({ pools: [{ name: 'fresh-pool' }] });
      await forcedPools.promise;
    });

    expect(result.current.poolsData).toEqual([{ name: 'fresh-pool' }]);
  });

  it('releases forced refresh ownership after a failed pool snapshot', async () => {
    const firstForcedPools = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({ pools: [{ name: 'initial-pool' }] })
      .mockImplementationOnce(() => firstForcedPools.promise)
      .mockResolvedValueOnce({ pools: [{ name: 'recovered-pool' }] });
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const { result } = renderHook(() => useManagedJobsPageData());

    await waitFor(() => {
      expect(result.current.poolsData).toEqual([{ name: 'initial-pool' }]);
    });

    await act(async () => {
      result.current.handleRefresh();
      result.current.handleRefresh();
      await Promise.resolve();
    });

    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(2, 'jobs', {
      force: true,
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      firstForcedPools.reject(new Error('pool snapshot unavailable'));
      await firstForcedPools.promise.catch(() => {});
    });

    await act(async () => {
      result.current.handleRefresh();
      await Promise.resolve();
    });

    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(3, 'jobs', {
      force: true,
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current.poolsData).toEqual([{ name: 'recovered-pool' }]);
    expect(consoleError).toHaveBeenCalledWith(
      'Error fetching data:',
      expect.objectContaining({ message: 'pool snapshot unavailable' })
    );
    consoleError.mockRestore();
  });

  it('lets manual refresh supersede a pending interval poll and blocks overlapping polls', async () => {
    jest.useFakeTimers();
    const automaticPools = deferred();
    const manualPools = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({ pools: [{ name: 'initial-pool' }] })
      .mockImplementationOnce(() => automaticPools.promise)
      .mockImplementationOnce(() => manualPools.promise)
      .mockResolvedValueOnce({ pools: [{ name: 'post-manual-pool' }] });

    const { result, unmount } = renderHook(() => useManagedJobsPageData());

    await waitFor(() => {
      expect(result.current.poolsData).toEqual([{ name: 'initial-pool' }]);
    });

    act(() => {
      jest.advanceTimersByTime(REFRESH_INTERVAL);
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      result.current.handleRefresh();
      await Promise.resolve();
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    act(() => {
      jest.advanceTimersByTime(REFRESH_INTERVAL * 2);
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      manualPools.resolve({ pools: [{ name: 'manual-pool' }] });
      await manualPools.promise;
    });
    expect(result.current.poolsData).toEqual([{ name: 'manual-pool' }]);

    await act(async () => {
      automaticPools.resolve({ pools: [{ name: 'stale-pool' }] });
      await automaticPools.promise;
    });
    expect(result.current.poolsData).toEqual([{ name: 'manual-pool' }]);

    act(() => {
      jest.advanceTimersByTime(REFRESH_INTERVAL);
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));
    await waitFor(() => {
      expect(result.current.poolsData).toEqual([{ name: 'post-manual-pool' }]);
    });

    unmount();
    jest.useRealTimers();
  });

  it('refreshes pools immediately when the page becomes visible again', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('hidden');
    dashboardCache.get
      .mockResolvedValueOnce({ pools: [{ name: 'initial-pool' }] })
      .mockResolvedValueOnce({ pools: [{ name: 'visible-pool' }] });

    const { result, unmount } = renderHook(() => useManagedJobsPageData());
    let mounted = true;

    try {
      await waitFor(() => {
        expect(result.current.poolsData).toEqual([{ name: 'initial-pool' }]);
      });
      dashboardCache.get.mockClear();

      act(() => {
        jest.advanceTimersByTime(REFRESH_INTERVAL * 2 - 1);
      });
      expect(dashboardCache.get).not.toHaveBeenCalled();

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });

      expect(dashboardCache.get).toHaveBeenCalledTimes(1);
      await waitFor(() => {
        expect(result.current.poolsData).toEqual([{ name: 'visible-pool' }]);
      });

      act(() => {
        jest.advanceTimersByTime(1);
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      unmount();
      mounted = false;

      window.document.dispatchEvent(new Event('visibilitychange'));
      act(() => {
        jest.advanceTimersByTime(REFRESH_INTERVAL);
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);
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
      jest.useRealTimers();
    }
  });
});

describe('managed jobs automatic refresh', () => {
  afterEach(() => {
    jest.useRealTimers();
  });

  it('issues one queue fetch for one filter change', async () => {
    getCurrentUserInfo.mockResolvedValue({ id: 'alice-id', name: 'alice' });
    jobsCacheManager.getPaginatedJobs.mockResolvedValue({
      jobs: [
        {
          id: 1,
          task_id: 0,
          task_job_id: '1-0',
          name: 'baseline-job',
          user: 'alice',
          user_hash: 'alice-id',
          status: 'RUNNING',
        },
      ],
      total: 1,
      totalNoFilter: 1,
      statusCounts: { RUNNING: 1 },
      controllerStopped: false,
      hasNext: false,
    });

    const props = {
      refreshInterval: 5000,
      setLoading: jest.fn(),
      refreshDataRef: { current: null },
      filters: [],
      onUserFilter: jest.fn(),
      onRefresh: jest.fn(),
      poolsData: [],
      poolsLoading: false,
      setValueList: jest.fn(),
      preloadingComplete: true,
      lastFetchedTime: null,
    };

    const { rerender } = render(<ManagedJobsTable {...props} />);

    await screen.findByText('baseline-job');
    await act(async () => {
      for (let i = 0; i < 5; i += 1) {
        await Promise.resolve();
      }
    });

    jobsCacheManager.getPaginatedJobs.mockClear();

    rerender(
      <ManagedJobsTable
        {...props}
        filters={[{ property: 'name', value: 'alpha' }]}
      />
    );

    await waitFor(() =>
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalled()
    );
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(1);
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledWith(
      expect.objectContaining({
        allUsers: true,
        nameMatch: 'alpha',
        page: 1,
        limit: 10,
        userMatch: 'alice',
      })
    );
  });

  it('resets to page 1 before fetching filtered jobs', async () => {
    const originalUrl = window.location.href;
    window.history.replaceState(
      null,
      '',
      'http://localhost/jobs?page=2&pageSize=10'
    );
    getCurrentUserInfo.mockResolvedValue({ id: 'alice-id', name: 'alice' });
    jobsCacheManager.getPaginatedJobs.mockResolvedValue({
      jobs: [
        {
          id: 11,
          task_id: 0,
          task_job_id: '11-0',
          name: 'paged-job',
          user: 'alice',
          user_hash: 'alice-id',
          status: 'RUNNING',
        },
      ],
      total: 25,
      totalNoFilter: 25,
      statusCounts: { RUNNING: 1 },
      controllerStopped: false,
      hasNext: false,
    });

    const props = {
      refreshInterval: 5000,
      setLoading: jest.fn(),
      refreshDataRef: { current: null },
      filters: [],
      onUserFilter: jest.fn(),
      onRefresh: jest.fn(),
      poolsData: [],
      poolsLoading: false,
      setValueList: jest.fn(),
      preloadingComplete: true,
      lastFetchedTime: null,
    };

    try {
      const { rerender } = render(<ManagedJobsTable {...props} />);

      await screen.findByText('paged-job');
      await act(async () => {
        for (let i = 0; i < 5; i += 1) {
          await Promise.resolve();
        }
      });

      jobsCacheManager.getPaginatedJobs.mockClear();

      rerender(
        <ManagedJobsTable
          {...props}
          filters={[{ property: 'name', value: 'alpha' }]}
        />
      );

      await waitFor(() =>
        expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalled()
      );
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(1);
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledWith(
        expect.objectContaining({
          nameMatch: 'alpha',
          page: 1,
        })
      );
    } finally {
      window.history.replaceState(null, '', originalUrl);
    }
  });

  it('issues one queue fetch for one ownership scope change', async () => {
    getCurrentUserInfo.mockResolvedValue({ id: 'alice-id', name: 'alice' });
    jobsCacheManager.getPaginatedJobs.mockResolvedValue({
      jobs: [
        {
          id: 1,
          task_id: 0,
          task_job_id: '1-0',
          name: 'scope-job',
          user: 'alice',
          user_hash: 'alice-id',
          status: 'RUNNING',
        },
      ],
      total: 1,
      totalNoFilter: 1,
      statusCounts: { RUNNING: 1 },
      controllerStopped: false,
      hasNext: false,
    });

    const props = {
      refreshInterval: 5000,
      setLoading: jest.fn(),
      refreshDataRef: { current: null },
      filters: [],
      onUserFilter: jest.fn(),
      onRefresh: jest.fn(),
      poolsData: [],
      poolsLoading: false,
      setValueList: jest.fn(),
      preloadingComplete: true,
      lastFetchedTime: null,
    };

    render(<ManagedJobsTable {...props} />);

    await screen.findByText('scope-job');
    await act(async () => {
      for (let i = 0; i < 5; i += 1) {
        await Promise.resolve();
      }
    });

    fireEvent.click(screen.getByRole('button', { name: /RUNNING/i }));
    await waitFor(() =>
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalled()
    );

    jobsCacheManager.getPaginatedJobs.mockClear();

    fireEvent.click(screen.getByRole('tab', { name: 'All Jobs' }));

    await waitFor(() =>
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalled()
    );
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(1);
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledWith(
      expect.objectContaining({
        allUsers: true,
        page: 1,
        userMatch: undefined,
      })
    );
  });

  it('resets to page 1 before fetching after an ownership scope change', async () => {
    const originalUrl = window.location.href;
    window.history.replaceState(
      null,
      '',
      'http://localhost/jobs?page=2&pageSize=10'
    );
    getCurrentUserInfo.mockResolvedValue({ id: 'alice-id', name: 'alice' });
    jobsCacheManager.getPaginatedJobs.mockResolvedValue({
      jobs: [
        {
          id: 11,
          task_id: 0,
          task_job_id: '11-0',
          name: 'paged-scope-job',
          user: 'alice',
          user_hash: 'alice-id',
          status: 'RUNNING',
        },
      ],
      total: 25,
      totalNoFilter: 25,
      statusCounts: { RUNNING: 1 },
      controllerStopped: false,
      hasNext: false,
    });

    const props = {
      refreshInterval: 5000,
      setLoading: jest.fn(),
      refreshDataRef: { current: null },
      filters: [],
      onUserFilter: jest.fn(),
      onRefresh: jest.fn(),
      poolsData: [],
      poolsLoading: false,
      setValueList: jest.fn(),
      preloadingComplete: true,
      lastFetchedTime: null,
    };

    try {
      render(<ManagedJobsTable {...props} />);

      await screen.findByText('paged-scope-job');
      await act(async () => {
        for (let i = 0; i < 5; i += 1) {
          await Promise.resolve();
        }
      });

      jobsCacheManager.getPaginatedJobs.mockClear();

      fireEvent.click(screen.getByRole('tab', { name: 'All Jobs' }));

      await waitFor(() =>
        expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalled()
      );
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(1);
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledWith(
        expect.objectContaining({
          page: 1,
          userMatch: undefined,
        })
      );
    } finally {
      window.history.replaceState(null, '', originalUrl);
    }
  });

  it('serializes background polls while manual refresh remains live', async () => {
    jest.useFakeTimers();
    getCurrentUserInfo.mockResolvedValue({ id: 'alice-id', name: 'alice' });
    const runningBatchResponse = {
      jobs: [
        {
          id: 1,
          task_id: 0,
          task_job_id: '1-0',
          name: 'batch-job',
          user: 'alice',
          user_hash: 'alice-id',
          status: 'RUNNING',
          batch_total_batches: 10,
          batch_completed_batches: 1,
        },
      ],
      total: 1,
      totalNoFilter: 1,
      statusCounts: { RUNNING: 1 },
      controllerStopped: false,
      hasNext: false,
    };
    jobsCacheManager.getPaginatedJobs.mockResolvedValue(runningBatchResponse);
    const refreshDataRef = { current: null };
    const { unmount } = render(
      <ManagedJobsTable
        refreshInterval={5000}
        setLoading={jest.fn()}
        refreshDataRef={refreshDataRef}
        filters={[]}
        onUserFilter={jest.fn()}
        onRefresh={jest.fn()}
        poolsData={[]}
        poolsLoading={false}
        setValueList={jest.fn()}
        preloadingComplete={true}
        lastFetchedTime={null}
      />
    );

    await screen.findByText('batch-job');
    await act(async () => {
      for (let i = 0; i < 5; i++) await Promise.resolve();
    });
    jobsCacheManager.getPaginatedJobs.mockClear();
    jobsCacheManager.invalidateCache.mockClear();

    const pendingPoll = deferred();
    jobsCacheManager.getPaginatedJobs.mockReturnValue(pendingPoll.promise);
    act(() => {
      jest.advanceTimersByTime(1000);
    });
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(1);
    expect(jobsCacheManager.invalidateCache).toHaveBeenCalledTimes(1);

    let manualRefresh;
    act(() => {
      manualRefresh = refreshDataRef.current({ includeStatus: false });
    });
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(2);

    act(() => {
      jest.advanceTimersByTime(2000);
    });
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(2);
    expect(jobsCacheManager.invalidateCache).toHaveBeenCalledTimes(1);

    await act(async () => {
      pendingPoll.resolve(runningBatchResponse);
      await Promise.all([pendingPoll.promise, manualRefresh]);
    });
    act(() => {
      jest.advanceTimersByTime(1000);
    });
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(3);
    expect(jobsCacheManager.invalidateCache).toHaveBeenCalledTimes(2);

    unmount();
    act(() => {
      jest.advanceTimersByTime(5000);
    });
    expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(3);
  });

  it('refreshes jobs immediately when the page becomes visible again', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('hidden');
    getCurrentUserInfo.mockResolvedValue({ id: 'alice-id', name: 'alice' });
    jobsCacheManager.getPaginatedJobs
      .mockResolvedValueOnce({
        jobs: [
          {
            id: 1,
            task_id: 0,
            task_job_id: '1-0',
            name: 'initial-job',
            user: 'alice',
            user_hash: 'alice-id',
            status: 'RUNNING',
          },
        ],
        total: 1,
        totalNoFilter: 1,
        statusCounts: { RUNNING: 1 },
        controllerStopped: false,
        hasNext: false,
      })
      .mockResolvedValueOnce({
        jobs: [
          {
            id: 1,
            task_id: 0,
            task_job_id: '1-0',
            name: 'visible-job',
            user: 'alice',
            user_hash: 'alice-id',
            status: 'SUCCEEDED',
          },
        ],
        total: 1,
        totalNoFilter: 1,
        statusCounts: { SUCCEEDED: 1 },
        controllerStopped: false,
        hasNext: false,
      });

    const { unmount } = render(
      <ManagedJobsTable
        refreshInterval={5000}
        setLoading={jest.fn()}
        refreshDataRef={{ current: null }}
        filters={[]}
        onUserFilter={jest.fn()}
        onRefresh={jest.fn()}
        poolsData={[]}
        poolsLoading={false}
        setValueList={jest.fn()}
        preloadingComplete={true}
        lastFetchedTime={null}
      />
    );
    let mounted = true;

    try {
      await screen.findByText('initial-job');
      jobsCacheManager.getPaginatedJobs.mockClear();

      act(() => {
        jest.advanceTimersByTime(10000);
      });
      expect(jobsCacheManager.getPaginatedJobs).not.toHaveBeenCalled();

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });

      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(1);
      await screen.findByText('visible-job');

      unmount();
      mounted = false;
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
      jest.useRealTimers();
    }
  });

  it('reuses the cached filter directory across background polls', async () => {
    jest.useFakeTimers();
    getCurrentUserInfo.mockResolvedValue({ id: 'alice-id', name: 'alice' });
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getUsers) {
        return Promise.resolve([{ username: 'alice' }]);
      }
      if (fetcher === getWorkspaces) {
        return Promise.resolve({ default: {} });
      }
      return Promise.resolve([]);
    });
    const runningBatchResponse = {
      jobs: [
        {
          id: 1,
          task_id: 0,
          task_job_id: '1-0',
          name: 'batch-job',
          user: 'alice',
          user_hash: 'alice-id',
          status: 'RUNNING',
          batch_total_batches: 10,
          batch_completed_batches: 1,
        },
      ],
      total: 1,
      totalNoFilter: 1,
      statusCounts: { RUNNING: 1 },
      controllerStopped: false,
      hasNext: false,
    };
    jobsCacheManager.getPaginatedJobs.mockResolvedValue(runningBatchResponse);

    render(
      <ManagedJobsTable
        refreshInterval={5000}
        setLoading={jest.fn()}
        refreshDataRef={{ current: null }}
        filters={[]}
        onUserFilter={jest.fn()}
        onRefresh={jest.fn()}
        poolsData={[]}
        poolsLoading={false}
        setValueList={jest.fn()}
        preloadingComplete={true}
        lastFetchedTime={null}
      />
    );

    await screen.findByText('batch-job');
    await act(async () => {
      for (let i = 0; i < 5; i += 1) {
        await Promise.resolve();
      }
    });

    const initialUserReads = countCacheReads(getUsers);
    const initialWorkspaceReads = countCacheReads(getWorkspaces);
    jobsCacheManager.getPaginatedJobs.mockClear();
    jobsCacheManager.invalidateCache.mockClear();

    act(() => {
      jest.advanceTimersByTime(1000);
    });

    await waitFor(() =>
      expect(jobsCacheManager.getPaginatedJobs).toHaveBeenCalledTimes(1)
    );
    expect(jobsCacheManager.invalidateCache).toHaveBeenCalledTimes(1);
    expect(countCacheReads(getUsers)).toBe(initialUserReads);
    expect(countCacheReads(getWorkspaces)).toBe(initialWorkspaceReads);
  });

  it('ignores stale filter directory completions after a newer preload', async () => {
    const firstUsers = deferred();
    const firstWorkspaces = deferred();
    const secondUsers = deferred();
    const secondWorkspaces = deferred();
    let userRequests = 0;
    let workspaceRequests = 0;
    dashboardCache.get.mockImplementation((fetcher) => {
      if (fetcher === getUsers) {
        userRequests += 1;
        return userRequests === 1 ? firstUsers.promise : secondUsers.promise;
      }
      if (fetcher === getWorkspaces) {
        workspaceRequests += 1;
        return workspaceRequests === 1
          ? firstWorkspaces.promise
          : secondWorkspaces.promise;
      }
      return Promise.resolve([]);
    });
    const setValueList = jest.fn();
    const props = {
      refreshInterval: 5000,
      setLoading: jest.fn(),
      refreshDataRef: { current: null },
      filters: [],
      onUserFilter: jest.fn(),
      onRefresh: jest.fn(),
      poolsData: [],
      poolsLoading: false,
      setValueList,
      lastFetchedTime: null,
    };

    const { rerender } = render(
      <ManagedJobsTable {...props} preloadingComplete={true} />
    );

    await waitFor(() => expect(countCacheReads(getUsers)).toBe(1));
    await waitFor(() => expect(countCacheReads(getWorkspaces)).toBe(1));

    rerender(<ManagedJobsTable {...props} preloadingComplete={false} />);
    rerender(<ManagedJobsTable {...props} preloadingComplete={true} />);

    await waitFor(() => expect(countCacheReads(getUsers)).toBe(2));
    await waitFor(() => expect(countCacheReads(getWorkspaces)).toBe(2));

    await act(async () => {
      secondUsers.resolve([{ username: 'fresh-user' }]);
      secondWorkspaces.resolve({ beta: {} });
      await Promise.all([secondUsers.promise, secondWorkspaces.promise]);
    });

    await waitFor(() =>
      expect(setValueList).toHaveBeenLastCalledWith(
        expect.objectContaining({
          user: ['fresh-user'],
          workspace: ['beta'],
        })
      )
    );

    await act(async () => {
      firstUsers.resolve([{ username: 'stale-user' }]);
      firstWorkspaces.resolve({ alpha: {} });
      await Promise.all([firstUsers.promise, firstWorkspaces.promise]);
    });

    expect(setValueList).toHaveBeenLastCalledWith(
      expect.objectContaining({
        user: ['fresh-user'],
        workspace: ['beta'],
      })
    );
  });
});

describe('job domain helpers', () => {
  it('preserves direct identities through the historical jobs facade', () => {
    expect(jobsFacade.statusGroups).toBe(jobDomain.statusGroups);
    expect(jobsFacade.getAggregatedStatus).toBe(jobDomain.getAggregatedStatus);
    expect(jobsFacade.filterJobsByName).toBe(jobDomain.filterJobsByName);
    expect(jobsFacade.filterJobsByWorkspace).toBe(
      jobDomain.filterJobsByWorkspace
    );
    expect(jobsFacade.filterJobsByUser).toBe(jobDomain.filterJobsByUser);
    expect(jobsFacade.filterJobsByPool).toBe(jobDomain.filterJobsByPool);
  });

  it('classifies active and finished statuses', () => {
    expect(statusGroups).toEqual({
      active: [
        'PENDING',
        'RUNNING',
        'RECOVERING',
        'SUBMITTED',
        'STARTING',
        'CANCELLING',
      ],
      finished: [
        'SUCCEEDED',
        'FAILED',
        'CANCELLED',
        'FAILED_SETUP',
        'FAILED_PRECHECKS',
        'FAILED_NO_RESOURCE',
        'FAILED_CONTROLLER',
      ],
    });
  });

  it('aggregates status while ignoring auxiliary job-group tasks', () => {
    expect(getAggregatedStatus()).toBe('PENDING');
    expect(getAggregatedStatus([])).toBe('PENDING');
    expect(getAggregatedStatus([{ status: 'RUNNING' }])).toBe('RUNNING');
    expect(
      getAggregatedStatus([
        { status: 'RUNNING', is_primary_in_job_group: null },
        { status: 'FAILED', is_primary_in_job_group: undefined },
      ])
    ).toBe('FAILED');
    expect(
      getAggregatedStatus([
        { status: 'RUNNING', is_primary_in_job_group: true },
        { status: 'FAILED', is_primary_in_job_group: false },
      ])
    ).toBe('RUNNING');
    expect(
      getAggregatedStatus([
        { status: 'RUNNING', is_primary_in_job_group: false },
        { status: 'FAILED', is_primary_in_job_group: false },
      ])
    ).toBe('FAILED');
    expect(
      getAggregatedStatus([{ status: 'UNKNOWN' }, { status: 'SUCCEEDED' }])
    ).toBe('SUCCEEDED');
  });

  it('filters names case-insensitively and preserves passthrough identity', () => {
    const jobs = [{ name: 'Alpha Train' }, { name: 'beta' }, {}];

    expect(filterJobsByName(jobs, '')).toBe(jobs);
    expect(filterJobsByName(jobs, '  ALPHA ')).toEqual([jobs[0]]);
  });

  it('filters workspaces with the historical default workspace behavior', () => {
    const jobs = [{ workspace: 'Research' }, { workspace: 'default' }, {}];

    expect(filterJobsByWorkspace(jobs, 'ALL_WORKSPACES')).toBe(jobs);
    expect(filterJobsByWorkspace(jobs, 'research')).toEqual([jobs[0]]);
    expect(filterJobsByWorkspace(jobs, 'DEFAULT')).toEqual([jobs[1], jobs[2]]);
  });

  it('prefers user hashes when filtering users', () => {
    const jobs = [{ user: 'alice', user_hash: 'hash-alice' }, { user: 'bob' }];

    expect(filterJobsByUser(jobs, 'ALL_USERS')).toBe(jobs);
    expect(filterJobsByUser(jobs, 'hash-alice')).toEqual([jobs[0]]);
    expect(filterJobsByUser(jobs, 'alice')).toEqual([]);
    expect(filterJobsByUser(jobs, 'bob')).toEqual([jobs[1]]);
  });

  it('filters pools case-insensitively and preserves passthrough identity', () => {
    const jobs = [{ pool: 'GPU-Train' }, { pool: 'cpu' }, {}];

    expect(filterJobsByPool(jobs, '  ')).toBe(jobs);
    expect(filterJobsByPool(jobs, ' gpu ')).toEqual([jobs[0]]);
  });
});
