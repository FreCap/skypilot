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
import dashboardCache from '@/lib/cache';
import cachePreloader from '@/lib/cache-preloader';
import jobsCacheManager from '@/lib/jobs-cache-manager';
import { getPoolStatus } from '@/data/connectors/jobs';
import { getCurrentUserInfo } from '@/data/connectors/client';

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

describe('managed jobs page initialization', () => {
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
    ).toHaveLength(2); // One page snapshot plus the PoolsTable's own read.
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
    const refreshPools = jest.fn();
    result.current.jobsRefreshRef.current = refreshJobs;
    result.current.poolsRefreshRef.current = refreshPools;

    act(() => result.current.handleRefresh());
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(refreshJobs).toHaveBeenCalledTimes(1);
    expect(refreshPools).toHaveBeenCalledTimes(1);

    await act(async () => {
      refreshedPools.resolve({ pools: [{ name: 'fresh-pool' }] });
      await refreshedPools.promise;
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
    act(() => result.current.handleRefresh());
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    await act(async () => {
      refreshedPools.resolve({ pools: [{ name: 'fresh-pool' }] });
      await refreshedPools.promise;
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
      if (poolRequestCount === 2) return initialPoolRequestB.promise;
      return refreshedPools.promise;
    });
    render(<ManagedJobs />);

    await waitFor(() => expect(poolRequestCount).toBe(2));
    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    await waitFor(() => expect(refreshButton).toBeEnabled());
    fireEvent.click(refreshButton);
    await waitFor(() => expect(poolRequestCount).toBe(4));

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
      initialPoolRequestB.resolve({
        pools: [{ name: 'stale-pool', replica_info: [] }],
      });
      await Promise.all([
        initialPoolRequestA.promise,
        initialPoolRequestB.promise,
      ]);
    });

    await waitFor(() => {
      expect(screen.queryByText('stale-pool')).not.toBeInTheDocument();
    });
    expect(screen.getAllByText('fresh-pool')).not.toHaveLength(0);
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
