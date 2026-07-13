import { act, renderHook, waitFor } from '@testing-library/react';

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
import {
  getClusterJobs,
  getClusters,
  useClusterDetails,
} from '@/data/connectors/clusters';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('useClusterDetails request ownership', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('does not publish or continue a cluster request superseded by navigation', async () => {
    const firstCluster = deferred();
    const secondCluster = deferred();
    const secondJobs = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => firstCluster.promise)
      .mockImplementationOnce(() => secondCluster.promise)
      .mockImplementationOnce(() => secondJobs.promise);

    const { result, rerender } = renderHook(
      ({ cluster }) => useClusterDetails({ cluster }),
      { initialProps: { cluster: 'cluster-a' } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    rerender({ cluster: 'cluster-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      secondCluster.resolve([{ name: 'cluster-b', workspace: 'workspace-b' }]);
      await secondCluster.promise;
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    await act(async () => {
      secondJobs.resolve([{ id: 2, cluster: 'cluster-b' }]);
      await secondJobs.promise;
    });
    expect(result.current.clusterData.name).toBe('cluster-b');
    expect(result.current.clusterJobData).toEqual([
      { id: 2, cluster: 'cluster-b' },
    ]);

    await act(async () => {
      firstCluster.resolve([{ name: 'cluster-a', workspace: 'workspace-a' }]);
      await firstCluster.promise;
    });

    expect(result.current.clusterData.name).toBe('cluster-b');
    expect(result.current.clusterJobData).toEqual([
      { id: 2, cluster: 'cluster-b' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(3, getClusterJobs, [
      { clusterName: 'cluster-b', workspace: 'workspace-b' },
    ]);
  });

  it('hides data owned by the previous route while the new cluster loads', async () => {
    const secondCluster = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockImplementationOnce(() => secondCluster.promise);

    const { result, rerender } = renderHook(
      ({ cluster }) => useClusterDetails({ cluster }),
      { initialProps: { cluster: 'cluster-a' } }
    );
    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));
    expect(result.current.clusterData.name).toBe('cluster-a');

    rerender({ cluster: 'cluster-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    expect(result.current.clusterData).toBeNull();
    expect(result.current.clusterJobData).toBeNull();
    expect(result.current.clusterDetailsLoading).toBe(true);
    expect(result.current.clusterJobsLoading).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('keeps a newer refresh loading when the superseded request fails', async () => {
    const initialCluster = deferred();
    const refreshedCluster = deferred();
    const refreshedJobs = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    dashboardCache.get
      .mockImplementationOnce(() => initialCluster.promise)
      .mockImplementationOnce(() => refreshedCluster.promise)
      .mockImplementationOnce(() => refreshedJobs.promise);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    let refreshPromise;
    await act(async () => {
      refreshPromise = result.current.refreshData();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      initialCluster.reject(new Error('superseded cluster request failed'));
      await initialCluster.promise.catch(() => {});
    });
    expect(result.current.clusterDetailsLoading).toBe(true);

    await act(async () => {
      refreshedCluster.resolve([
        { name: 'cluster-a', workspace: 'workspace-a' },
      ]);
      await refreshedCluster.promise;
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));
    expect(result.current.clusterJobsLoading).toBe(true);

    await act(async () => {
      refreshedJobs.resolve([{ id: 1, cluster: 'cluster-a' }]);
      await refreshPromise;
    });

    expect(result.current.clusterDetailsLoading).toBe(false);
    expect(result.current.clusterJobsLoading).toBe(false);
    expect(result.current.clusterData.name).toBe('cluster-a');
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
    consoleError.mockRestore();
  });

  it('does not launch the dependent job request after unmount', async () => {
    const clusterRequest = deferred();
    dashboardCache.get.mockImplementationOnce(() => clusterRequest.promise);

    const { unmount } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    unmount();

    await act(async () => {
      clusterRequest.resolve([{ name: 'cluster-a', workspace: 'workspace-a' }]);
      await clusterRequest.promise;
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });

  it('keeps a job-only refresh newer than the full request chain', async () => {
    const initialJobs = deferred();
    const refreshedJobs = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockImplementationOnce(() => initialJobs.promise)
      .mockImplementationOnce(() => refreshedJobs.promise);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    let refreshPromise;
    await act(async () => {
      refreshPromise = result.current.refreshClusterJobsOnly();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    await act(async () => {
      refreshedJobs.resolve([{ id: 2, status: 'RUNNING' }]);
      await refreshPromise;
    });
    expect(result.current.clusterJobData).toEqual([
      { id: 2, status: 'RUNNING' },
    ]);

    await act(async () => {
      initialJobs.resolve([{ id: 1, status: 'SUCCEEDED' }]);
      await initialJobs.promise;
    });
    expect(result.current.clusterJobData).toEqual([
      { id: 2, status: 'RUNNING' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('uses one cluster read and one workspace-scoped job read per chain', async () => {
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }]);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );

    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(1, getClusters, [
      { clusterNames: ['cluster-a'] },
    ]);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(2, getClusterJobs, [
      { clusterName: 'cluster-a', workspace: 'workspace-a' },
    ]);
  });
});
