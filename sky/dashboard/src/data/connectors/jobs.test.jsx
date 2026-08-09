import { act, renderHook, waitFor } from '@testing-library/react';

// Mock the shared dashboard cache so we can observe get/invalidate calls
// without hitting the network.
jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
    setPreloader: jest.fn(),
    getCached: jest.fn(),
    clear: jest.fn(),
  },
}));

jest.mock('@/data/connectors/client', () => ({
  apiClient: {
    get: jest.fn(),
    post: jest.fn(),
  },
  getCurrentUserInfo: jest.fn(),
}));

import dashboardCache from '@/lib/cache';
import { apiClient } from '@/data/connectors/client';
import {
  getManagedJobs,
  getPoolStatus,
  useManagedJobPools,
  useSingleManagedJob,
} from '@/data/connectors/jobs';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('getPoolStatus request scope', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    apiClient.post.mockResolvedValue({
      ok: true,
      headers: { get: () => 'pool-status-request' },
    });
    apiClient.get.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ return_value: '[]' }),
    });
  });

  it('sends only the requested pool names to the backend', async () => {
    await getPoolStatus({ poolNames: ['pool-a'] });

    expect(apiClient.post).toHaveBeenCalledTimes(1);
    expect(apiClient.post).toHaveBeenCalledWith('/jobs/pool_status', {
      pool_names: ['pool-a'],
    });
  });

  it('keeps the all-pools request as the default', async () => {
    await getPoolStatus();

    expect(apiClient.post).toHaveBeenCalledTimes(1);
    expect(apiClient.post).toHaveBeenCalledWith('/jobs/pool_status', {
      pool_names: null,
    });
  });

  it('skips the active-jobs query when only pool identity is needed', async () => {
    apiClient.get.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ return_value: '[{"name":"pool-a"}]' }),
    });

    const result = await getPoolStatus({
      poolNames: ['pool-a'],
      includeJobCounts: false,
    });

    expect(result).toEqual({
      pools: [{ name: 'pool-a', jobCounts: {} }],
      controllerStopped: false,
    });
    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('uses backend job status counts without fetching managed jobs again', async () => {
    apiClient.get.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        return_value:
          '[{"name":"pool-a","job_status_counts":{"RUNNING":2,"PENDING":1}}]',
      }),
    });

    const result = await getPoolStatus({ poolNames: ['pool-a'] });

    expect(result).toEqual({
      pools: [
        {
          name: 'pool-a',
          job_status_counts: { RUNNING: 2, PENDING: 1 },
          jobCounts: { RUNNING: 2, PENDING: 1 },
        },
      ],
      controllerStopped: false,
    });
    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('keeps pool counts on the pool-status payload contract', async () => {
    apiClient.get.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        return_value:
          '[{"name":"pool-a"},{"name":"pool-b","job_status_counts":{"RUNNING":1}}]',
      }),
    });

    const result = await getPoolStatus({ poolNames: ['pool-a', 'pool-b'] });

    expect(dashboardCache.get).not.toHaveBeenCalled();
    expect(result).toEqual({
      pools: [
        { name: 'pool-a', jobCounts: {} },
        {
          name: 'pool-b',
          job_status_counts: { RUNNING: 1 },
          jobCounts: { RUNNING: 1 },
        },
      ],
      controllerStopped: false,
    });
  });
});

describe('useManagedJobPools request ownership', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('skips pool status for loading and non-pool jobs', async () => {
    const { result, rerender } = renderHook(
      ({ jobs, jobId }) => useManagedJobPools(jobs, jobId),
      { initialProps: { jobs: null, jobId: '42' } }
    );

    expect(result.current).toEqual([]);
    expect(dashboardCache.get).not.toHaveBeenCalled();

    rerender({ jobs: [{ id: 42, pool: null }], jobId: '42' });

    expect(result.current).toEqual([]);
    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('fetches the current job unique pools once across unrelated refreshes', async () => {
    dashboardCache.get.mockResolvedValue({
      pools: [{ name: 'pool-a' }, { name: 'pool-b' }],
    });
    const initialJobs = [
      { id: 42, pool: 'pool-b', status: 'PENDING' },
      { id: 42, pool: 'pool-a', status: 'PENDING' },
      { id: 42, pool: 'pool-b', status: 'PENDING' },
      { id: 43, pool: 'pool-z', status: 'RUNNING' },
    ];
    const { result, rerender } = renderHook(
      ({ jobs }) => useManagedJobPools(jobs, '42'),
      { initialProps: { jobs: initialJobs } }
    );

    await waitFor(() => expect(result.current).toHaveLength(2));
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenCalledWith(getPoolStatus, [
      {
        poolNames: ['pool-a', 'pool-b'],
        includeJobCounts: false,
      },
    ]);

    rerender({
      jobs: initialJobs.map((job) => ({ ...job, status: 'RUNNING' })).reverse(),
    });

    expect(result.current).toEqual([{ name: 'pool-a' }, { name: 'pool-b' }]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });

  it('drops a stale pool result after the job pool set changes', async () => {
    const poolA = deferred();
    const poolB = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => poolA.promise)
      .mockImplementationOnce(() => poolB.promise);
    const { result, rerender } = renderHook(
      ({ jobs }) => useManagedJobPools(jobs, '42'),
      { initialProps: { jobs: [{ id: 42, pool: 'pool-a' }] } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    rerender({ jobs: [{ id: 42, pool: 'pool-b' }] });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(result.current).toEqual([]);

    await act(async () => {
      poolB.resolve({ pools: [{ name: 'pool-b' }] });
      await poolB.promise;
    });
    expect(result.current).toEqual([{ name: 'pool-b' }]);

    await act(async () => {
      poolA.resolve({ pools: [{ name: 'pool-a' }] });
      await poolA.promise;
    });
    expect(result.current).toEqual([{ name: 'pool-b' }]);
  });

  it('ignores a stale failure after the current pool request succeeds', async () => {
    const poolA = deferred();
    const poolB = deferred();
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    dashboardCache.get
      .mockImplementationOnce(() => poolA.promise)
      .mockImplementationOnce(() => poolB.promise);
    const { result, rerender } = renderHook(
      ({ jobs }) => useManagedJobPools(jobs, '42'),
      { initialProps: { jobs: [{ id: 42, pool: 'pool-a' }] } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    rerender({ jobs: [{ id: 42, pool: 'pool-b' }] });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      poolB.resolve({ pools: [{ name: 'pool-b' }] });
      await poolB.promise;
    });
    await act(async () => {
      poolA.reject(new Error('stale pool failure'));
      await poolA.promise.catch(() => {});
    });

    expect(result.current).toEqual([{ name: 'pool-b' }]);
    expect(errorSpy).not.toHaveBeenCalled();
    errorSpy.mockRestore();
  });

  it('publishes an empty snapshot when the current pool request fails', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    dashboardCache.get.mockRejectedValue(new Error('pool status unavailable'));
    const { result } = renderHook(() =>
      useManagedJobPools([{ id: 42, pool: 'pool-a' }], '42')
    );

    await waitFor(() => expect(errorSpy).toHaveBeenCalledTimes(1));
    expect(result.current).toEqual([]);
    expect(errorSpy).toHaveBeenCalledWith(
      'Error fetching pools data:',
      expect.objectContaining({ message: 'pool status unavailable' })
    );
    errorSpy.mockRestore();
  });
});

describe('useSingleManagedJob refresh ownership', () => {
  const jobId = '56164';
  const expectedArgs = [{ allUsers: true, allFields: true, jobIDs: [jobId] }];

  beforeEach(() => {
    jest.clearAllMocks();
    dashboardCache.get.mockResolvedValue({
      jobs: [{ id: Number(jobId) }],
      controllerStopped: false,
    });
  });

  it('does not invalidate the cache on the initial load', async () => {
    renderHook(() => useSingleManagedJob(jobId));

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
  });

  it('owns the forced refresh until its exact-key read settles', async () => {
    const refreshedRequest = deferred();
    const { result } = renderHook(() => useSingleManagedJob(jobId));

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
    dashboardCache.get.mockImplementationOnce(() => refreshedRequest.promise);

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshJobData();
    });

    expect(result.current.loading).toBe(true);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(
      getManagedJobs,
      expectedArgs
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenLastCalledWith(
      getManagedJobs,
      expectedArgs
    );

    await act(async () => {
      refreshedRequest.resolve({
        jobs: [{ id: Number(jobId), status: 'RUNNING' }],
        controllerStopped: false,
      });
      await refreshPromise;
    });

    expect(result.current.loading).toBe(false);
    expect(result.current.jobData.jobs[0].status).toBe('RUNNING');
  });

  it('keeps the visible job snapshot when a refresh fails', async () => {
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});
    const { result } = renderHook(() => useSingleManagedJob(jobId));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.jobData.jobs[0].id).toBe(Number(jobId));

    dashboardCache.get.mockRejectedValueOnce(new Error('refresh failed'));

    await act(async () => {
      await result.current.refreshJobData();
    });

    expect(result.current.loading).toBe(false);
    expect(result.current.jobData.jobs[0].id).toBe(Number(jobId));
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(errorSpy).toHaveBeenCalledWith(
      'Error fetching single managed job data:',
      expect.objectContaining({ message: 'refresh failed' })
    );
    errorSpy.mockRestore();
  });

  it('coalesces concurrent forced refreshes for the same job', async () => {
    const refreshedRequest = deferred();
    const { result } = renderHook(() => useSingleManagedJob(jobId));

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    dashboardCache.get.mockImplementationOnce(() => refreshedRequest.promise);

    let firstRefresh;
    let secondRefresh;
    act(() => {
      firstRefresh = result.current.refreshJobData();
      secondRefresh = result.current.refreshJobData();
    });

    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      refreshedRequest.resolve({ jobs: [], controllerStopped: false });
      await Promise.all([firstRefresh, secondRefresh]);
    });
  });

  it('ignores an earlier job response that resolves after navigation', async () => {
    const firstRequest = deferred();
    const secondRequest = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => firstRequest.promise)
      .mockImplementationOnce(() => secondRequest.promise);

    const { result, rerender } = renderHook(
      ({ id }) => useSingleManagedJob(id),
      { initialProps: { id: '56164' } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    rerender({ id: '56165' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      secondRequest.resolve({
        jobs: [{ id: 56165, status: 'RUNNING' }],
        controllerStopped: false,
      });
      await secondRequest.promise;
    });
    expect(result.current.jobData.jobs[0].id).toBe(56165);

    await act(async () => {
      firstRequest.resolve({
        jobs: [{ id: 56164, status: 'SUCCEEDED' }],
        controllerStopped: false,
      });
      await firstRequest.promise;
    });

    expect(result.current.jobData.jobs[0].id).toBe(56165);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('hides data owned by the previous route on the first navigation render', async () => {
    const secondRequest = deferred();
    const routeSnapshots = [];
    dashboardCache.get
      .mockResolvedValueOnce({
        jobs: [{ id: 56164, status: 'SUCCEEDED' }],
        controllerStopped: false,
      })
      .mockImplementationOnce(() => secondRequest.promise);

    const { result, rerender } = renderHook(
      ({ id }) => {
        const details = useSingleManagedJob(id);
        routeSnapshots.push({
          id,
          jobData: details.jobData,
          loading: details.loading,
        });
        return details;
      },
      { initialProps: { id: '56164' } }
    );

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.jobData.jobs[0].id).toBe(56164);

    routeSnapshots.length = 0;
    rerender({ id: '56165' });

    expect(routeSnapshots[0]).toEqual({
      id: '56165',
      jobData: null,
      loading: true,
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      secondRequest.resolve({
        jobs: [{ id: 56165, status: 'RUNNING' }],
        controllerStopped: false,
      });
      await secondRequest.promise;
    });

    expect(result.current.jobData.jobs[0].id).toBe(56165);
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('does not reuse an old refresh after leaving and returning to a job', async () => {
    const oldRefresh = deferred();
    const newRefresh = deferred();
    const { result, rerender } = renderHook(
      ({ id }) => useSingleManagedJob(id),
      { initialProps: { id: '56164' } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    dashboardCache.get.mockImplementationOnce(() => oldRefresh.promise);
    let oldRefreshPromise;
    act(() => {
      oldRefreshPromise = result.current.refreshJobData();
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    dashboardCache.get.mockResolvedValueOnce({
      jobs: [{ id: 56165, status: 'RUNNING' }],
      controllerStopped: false,
    });
    rerender({ id: '56165' });
    await waitFor(() => expect(result.current.jobData.jobs[0].id).toBe(56165));

    dashboardCache.get.mockResolvedValueOnce({
      jobs: [{ id: 56164, status: 'RUNNING' }],
      controllerStopped: false,
    });
    rerender({ id: '56164' });
    await waitFor(() => expect(result.current.jobData.jobs[0].id).toBe(56164));

    dashboardCache.get.mockImplementationOnce(() => newRefresh.promise);
    let newRefreshPromise;
    act(() => {
      newRefreshPromise = result.current.refreshJobData();
    });

    expect(newRefreshPromise).not.toBe(oldRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenCalledTimes(5);

    await act(async () => {
      newRefresh.resolve({
        jobs: [{ id: 56164, status: 'SUCCEEDED' }],
        controllerStopped: false,
      });
      await newRefreshPromise;
    });
    expect(result.current.jobData.jobs[0].status).toBe('SUCCEEDED');

    await act(async () => {
      oldRefresh.resolve({
        jobs: [{ id: 56164, status: 'FAILED' }],
        controllerStopped: false,
      });
      await oldRefreshPromise;
    });
    expect(result.current.jobData.jobs[0].status).toBe('SUCCEEDED');
  });

  it('reuses the in-flight initial load when no current-route data is visible', async () => {
    const initialRequest = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    dashboardCache.get.mockImplementationOnce(() => initialRequest.promise);

    const { result } = renderHook(() => useSingleManagedJob('56164'));

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshJobData();
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();

    await act(async () => {
      initialRequest.resolve({
        jobs: [{ id: 56164, status: 'RUNNING' }],
        controllerStopped: false,
      });
      await refreshPromise;
    });

    expect(result.current.loading).toBe(false);
    expect(result.current.jobData.jobs[0].status).toBe('RUNNING');
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    consoleError.mockRestore();
  });
});
