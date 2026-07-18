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
});

describe('useSingleManagedJob manual-refresh cache invalidation', () => {
  const jobId = '56164';
  const expectedArgs = [{ allUsers: true, allFields: true, jobIDs: [jobId] }];

  beforeEach(() => {
    jest.clearAllMocks();
    dashboardCache.get.mockResolvedValue({
      jobs: [{ id: Number(jobId) }],
      controllerStopped: false,
    });
  });

  it('does not invalidate the cache on the initial load (refreshTrigger = 0)', async () => {
    renderHook(() => useSingleManagedJob(jobId, 0));

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
  });

  it('invalidates the cached entry before refetching when refreshTrigger increments', async () => {
    const { rerender } = renderHook(
      ({ trigger }) => useSingleManagedJob(jobId, trigger),
      { initialProps: { trigger: 0 } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();

    // Simulate clicking the detail-page Refresh button.
    rerender({ trigger: 1 });

    await waitFor(() =>
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1)
    );
    // Must target the same function + args the fetch uses, otherwise the wrong
    // cache key is cleared and the refresh stays stale.
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(
      getManagedJobs,
      expectedArgs
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(dashboardCache.get).toHaveBeenLastCalledWith(
      getManagedJobs,
      expectedArgs
    );
  });

  it('does not invalidate when navigating to a new job while refreshTrigger stays elevated', async () => {
    // The parent keeps refreshTrigger state across jobId changes, so after a
    // refresh the trigger remains > 0. Navigating to a different job must NOT
    // invalidate the new job's cache on its initial load.
    const { rerender } = renderHook(
      ({ id, trigger }) => useSingleManagedJob(id, trigger),
      { initialProps: { id: jobId, trigger: 1 } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    jest.clearAllMocks();
    dashboardCache.get.mockResolvedValue({
      jobs: [],
      controllerStopped: false,
    });

    // Navigate to a different job; trigger is unchanged (no manual refresh).
    rerender({ id: '56165', trigger: 1 });

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
  });

  it('ignores an earlier job response that resolves after navigation', async () => {
    const firstRequest = deferred();
    const secondRequest = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => firstRequest.promise)
      .mockImplementationOnce(() => secondRequest.promise);

    const { result, rerender } = renderHook(
      ({ id }) => useSingleManagedJob(id, 0),
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

  it('keeps a refreshed request loading when the superseded request fails', async () => {
    const initialRequest = deferred();
    const refreshedRequest = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    dashboardCache.get
      .mockImplementationOnce(() => initialRequest.promise)
      .mockImplementationOnce(() => refreshedRequest.promise);

    const { result, rerender } = renderHook(
      ({ trigger }) => useSingleManagedJob('56164', trigger),
      { initialProps: { trigger: 0 } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    rerender({ trigger: 1 });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      initialRequest.reject(new Error('superseded request failed'));
      await initialRequest.promise.catch(() => {});
    });

    expect(result.current.loading).toBe(true);
    expect(result.current.jobData).toBeNull();

    await act(async () => {
      refreshedRequest.resolve({
        jobs: [{ id: 56164, status: 'RUNNING' }],
        controllerStopped: false,
      });
      await refreshedRequest.promise;
    });

    expect(result.current.loading).toBe(false);
    expect(result.current.jobData.jobs[0].status).toBe('RUNNING');
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    consoleError.mockRestore();
  });
});
