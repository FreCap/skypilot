import { act, renderHook, waitFor } from '@testing-library/react';

jest.mock('@/data/connectors/toast', () => ({
  showToast: jest.fn(),
}));

jest.mock('@/plugins/dataEnhancement', () => ({
  applyEnhancements: jest.fn((data) => data),
}));

jest.mock('@/lib/jobs-cache-manager', () => ({
  __esModule: true,
  default: {
    invalidateJobsCache: jest.fn(),
  },
}));

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

import dashboardCache from '@/lib/cache';
import { getManagedJobs, useSingleManagedJob } from '@/data/connectors/jobs';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function detailArgs(jobId) {
  return [{ allUsers: true, allFields: true, jobIDs: [jobId] }];
}

describe('useSingleManagedJob refresh ownership', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  it('coalesces a manual refresh with an in-flight initial load', async () => {
    const initialLoad = deferred();
    dashboardCache.get.mockImplementationOnce(() => initialLoad.promise);

    const { result } = renderHook(() => useSingleManagedJob('42'));

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshJobData();
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();

    await act(async () => {
      initialLoad.resolve({
        jobs: [{ id: '42', status: 'RUNNING' }],
        controllerStopped: false,
      });
      await refreshPromise;
    });

    expect(result.current.jobData.jobs).toEqual([
      { id: '42', status: 'RUNNING' },
    ]);
    expect(result.current.loading).toBe(false);
  });

  it('coalesces repeated manual refreshes while a forced refresh is in flight', async () => {
    dashboardCache.get.mockResolvedValueOnce({
      jobs: [{ id: '42', status: 'INIT' }],
      controllerStopped: false,
    });
    const refreshed = deferred();

    const { result } = renderHook(() => useSingleManagedJob('42'));

    await waitFor(() =>
      expect(result.current.jobData.jobs).toEqual([
        { id: '42', status: 'INIT' },
      ])
    );

    dashboardCache.get.mockImplementationOnce(() => refreshed.promise);

    let firstRefresh;
    let secondRefresh;
    act(() => {
      firstRefresh = result.current.refreshJobData();
      secondRefresh = result.current.refreshJobData();
    });

    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(
      getManagedJobs,
      detailArgs('42')
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      refreshed.resolve({
        jobs: [{ id: '42', status: 'SUCCEEDED' }],
        controllerStopped: false,
      });
      await Promise.all([firstRefresh, secondRefresh]);
    });

    expect(result.current.jobData.jobs).toEqual([
      { id: '42', status: 'SUCCEEDED' },
    ]);
  });

  it('reuses the new route load when refresh is clicked before the route settles', async () => {
    dashboardCache.get.mockResolvedValueOnce({
      jobs: [{ id: '41', status: 'SUCCEEDED' }],
      controllerStopped: false,
    });
    const nextRoute = deferred();

    const { result, rerender } = renderHook(
      ({ jobId }) => useSingleManagedJob(jobId),
      { initialProps: { jobId: '41' } }
    );

    await waitFor(() =>
      expect(result.current.jobData.jobs).toEqual([
        { id: '41', status: 'SUCCEEDED' },
      ])
    );

    dashboardCache.get.mockImplementationOnce(() => nextRoute.promise);
    rerender({ jobId: '42' });

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshJobData();
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();

    await act(async () => {
      nextRoute.resolve({
        jobs: [{ id: '42', status: 'RUNNING' }],
        controllerStopped: false,
      });
      await refreshPromise;
    });

    expect(result.current.jobData.jobs).toEqual([
      { id: '42', status: 'RUNNING' },
    ]);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
  });
});
