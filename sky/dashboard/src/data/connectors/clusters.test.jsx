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
  useClusterData,
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
    const routeSnapshots = [];
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockImplementationOnce(() => secondCluster.promise);

    const { result, rerender } = renderHook(
      ({ cluster }) => {
        const details = useClusterDetails({ cluster });
        routeSnapshots.push({
          cluster,
          clusterData: details.clusterData,
          clusterJobData: details.clusterJobData,
          loading: details.loading,
          clusterDetailsLoading: details.clusterDetailsLoading,
          clusterJobsLoading: details.clusterJobsLoading,
        });
        return details;
      },
      { initialProps: { cluster: 'cluster-a' } }
    );
    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));
    expect(result.current.clusterData.name).toBe('cluster-a');

    routeSnapshots.length = 0;
    rerender({ cluster: 'cluster-b' });

    // The first render for the new route is the commit boundary. Cleanup in a
    // later effect is too late: the page could paint cluster A's actions,
    // jobs, and external links under cluster B's URL.
    expect(routeSnapshots[0]).toEqual({
      cluster: 'cluster-b',
      clusterData: null,
      clusterJobData: null,
      loading: true,
      clusterDetailsLoading: true,
      clusterJobsLoading: true,
    });

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

  it('does not restart the jobs spinner or issue a superseded job read', async () => {
    const slowRefreshCluster = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, status: 'SUCCEEDED' }])
      .mockImplementationOnce(() => slowRefreshCluster.promise)
      .mockResolvedValueOnce([{ id: 2, status: 'RUNNING' }])
      .mockResolvedValue([{ id: 3, status: 'STALE' }]);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );
    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));

    // Full refresh whose cluster read hangs.
    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshData();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    // A newer job-only refresh completes while the chain is pending.
    await act(async () => {
      await result.current.refreshClusterJobsOnly();
    });
    expect(result.current.clusterJobData).toEqual([
      { id: 2, status: 'RUNNING' },
    ]);
    expect(result.current.clusterJobsLoading).toBe(false);

    // The superseded chain resolving must not corrupt newer state.
    await act(async () => {
      slowRefreshCluster.resolve([
        { name: 'cluster-a', workspace: 'workspace-a' },
      ]);
      await refreshPromise;
    });

    expect(result.current.clusterJobsLoading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
    expect(result.current.clusterJobData).toEqual([
      { id: 2, status: 'RUNNING' },
    ]);
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

  it('coalesces duplicate full refreshes and releases ownership after success', async () => {
    const refreshedCluster = deferred();
    const refreshedJobs = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockImplementationOnce(() => refreshedCluster.promise)
      .mockImplementationOnce(() => refreshedJobs.promise);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );
    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));

    let firstRefreshPromise;
    let duplicateRefreshPromise;
    act(() => {
      firstRefreshPromise = result.current.refreshData();
      duplicateRefreshPromise = result.current.refreshData();
    });

    expect(duplicateRefreshPromise).toBe(firstRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getClusters, [
      { clusterNames: ['cluster-a'] },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      refreshedCluster.resolve([
        { name: 'cluster-a', workspace: 'workspace-a', status: 'RUNNING' },
      ]);
      await refreshedCluster.promise;
    });

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidate).toHaveBeenNthCalledWith(
      2,
      getClusterJobs,
      [{ clusterName: 'cluster-a', workspace: 'workspace-a' }]
    );

    await act(async () => {
      refreshedJobs.resolve([{ id: 2, cluster: 'cluster-a' }]);
      await Promise.all([firstRefreshPromise, duplicateRefreshPromise]);
    });

    expect(result.current.clusterData.status).toBe('RUNNING');
    expect(result.current.clusterJobData).toEqual([
      { id: 2, cluster: 'cluster-a' },
    ]);
    expect(result.current.clusterDetailsLoading).toBe(false);
    expect(result.current.clusterJobsLoading).toBe(false);

    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 3, cluster: 'cluster-a' }]);
    let laterRefreshPromise;
    act(() => {
      laterRefreshPromise = result.current.refreshData();
    });
    expect(laterRefreshPromise).not.toBe(firstRefreshPromise);

    await act(async () => {
      await laterRefreshPromise;
    });

    expect(result.current.clusterJobData).toEqual([
      { id: 3, cluster: 'cluster-a' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(6);
  });

  it('releases full refresh ownership after a failed refresh', async () => {
    const failedCluster = deferred();
    jest.spyOn(console, 'error').mockImplementation(() => {});
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockImplementationOnce(() => failedCluster.promise);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );
    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));

    let firstRefreshPromise;
    let duplicateRefreshPromise;
    act(() => {
      firstRefreshPromise = result.current.refreshData();
      duplicateRefreshPromise = result.current.refreshData();
    });

    expect(duplicateRefreshPromise).toBe(firstRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      failedCluster.reject(new Error('refresh cluster failed'));
      await Promise.all([firstRefreshPromise, duplicateRefreshPromise]);
    });

    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 4, cluster: 'cluster-a' }]);
    let recoveredRefreshPromise;
    act(() => {
      recoveredRefreshPromise = result.current.refreshData();
    });

    expect(recoveredRefreshPromise).not.toBe(firstRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);

    await act(async () => {
      await recoveredRefreshPromise;
    });

    expect(result.current.clusterJobData).toEqual([
      { id: 4, cluster: 'cluster-a' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(5);
  });

  it('coalesces duplicate job-only refreshes and releases ownership after success', async () => {
    const refreshedJobs = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockImplementationOnce(() => refreshedJobs.promise);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );
    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));

    let firstRefreshPromise;
    let duplicateRefreshPromise;
    act(() => {
      firstRefreshPromise = result.current.refreshClusterJobsOnly();
      duplicateRefreshPromise = result.current.refreshClusterJobsOnly();
    });

    expect(duplicateRefreshPromise).toBe(firstRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getClusterJobs, [
      { clusterName: 'cluster-a', workspace: 'workspace-a' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      refreshedJobs.resolve([{ id: 2, cluster: 'cluster-a' }]);
      await Promise.all([firstRefreshPromise, duplicateRefreshPromise]);
    });

    expect(result.current.clusterJobData).toEqual([
      { id: 2, cluster: 'cluster-a' },
    ]);
    expect(result.current.clusterJobsLoading).toBe(false);

    dashboardCache.get.mockResolvedValueOnce([{ id: 3, cluster: 'cluster-a' }]);
    let laterRefreshPromise;
    act(() => {
      laterRefreshPromise = result.current.refreshClusterJobsOnly();
    });
    expect(laterRefreshPromise).not.toBe(firstRefreshPromise);

    await act(async () => {
      await laterRefreshPromise;
    });

    expect(result.current.clusterJobData).toEqual([
      { id: 3, cluster: 'cluster-a' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
  });

  it('clears stale cluster state when a same-route refresh confirms the cluster is gone', async () => {
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockResolvedValueOnce([]);

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );

    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));
    expect(result.current.clusterData.name).toBe('cluster-a');
    expect(result.current.clusterJobData).toEqual([
      { id: 1, cluster: 'cluster-a' },
    ]);

    await act(async () => {
      await result.current.refreshData();
    });

    expect(result.current.clusterData).toBeNull();
    expect(result.current.clusterJobData).toBeNull();
    expect(result.current.clusterDetailsLoading).toBe(false);
    expect(result.current.clusterJobsLoading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(3, getClusters, [
      { clusterNames: ['cluster-a'] },
    ]);
    expect(consoleError).toHaveBeenCalledWith(
      'No cluster data found for cluster:',
      'cluster-a'
    );
    consoleError.mockRestore();
  });

  it('preserves the current cluster state when the refresh cluster read fails', async () => {
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'cluster-a', workspace: 'workspace-a' }])
      .mockResolvedValueOnce([{ id: 1, cluster: 'cluster-a' }])
      .mockRejectedValueOnce(new Error('cluster read failed'));

    const { result } = renderHook(() =>
      useClusterDetails({ cluster: 'cluster-a' })
    );

    await waitFor(() => expect(result.current.clusterJobsLoading).toBe(false));
    expect(result.current.clusterData.name).toBe('cluster-a');
    expect(result.current.clusterJobData).toEqual([
      { id: 1, cluster: 'cluster-a' },
    ]);

    await act(async () => {
      await result.current.refreshData();
    });

    expect(result.current.clusterData.name).toBe('cluster-a');
    expect(result.current.clusterJobData).toEqual([
      { id: 1, cluster: 'cluster-a' },
    ]);
    expect(result.current.clusterDetailsLoading).toBe(false);
    expect(result.current.clusterJobsLoading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
    expect(consoleError).toHaveBeenCalledWith(
      'Error fetching cluster data:',
      expect.objectContaining({ message: 'cluster read failed' })
    );
    consoleError.mockRestore();
  });
});

describe('useClusterData request ownership', () => {
  const stableOptions = {
    sortConfig: { key: null, direction: 'ascending' },
    filters: [],
  };

  beforeEach(() => {
    jest.clearAllMocks();
    delete window.__skyPaginationFetch;
  });

  afterEach(() => {
    jest.useRealTimers();
    delete window.__skyPaginationFetch;
    jest.restoreAllMocks();
  });

  it('coalesces manual refreshes and overdue ticks for one context', async () => {
    jest.useFakeTimers();
    const initialRequest = deferred();
    dashboardCache.get.mockImplementation(() => initialRequest.promise);

    const { result } = renderHook(() =>
      useClusterData({ ...stableOptions, refreshInterval: 1000 })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    let firstRefresh;
    let secondRefresh;
    act(() => {
      firstRefresh = result.current.refresh();
      secondRefresh = result.current.refresh();
      jest.advanceTimersByTime(3000);
    });

    expect(firstRefresh).toBe(secondRefresh);
    // The first explicit refresh deliberately supersedes the automatic mount
    // load. Its duplicate and all overdue interval ticks reuse that owner.
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialRequest.resolve([{ cluster: 'cluster-a' }]);
      await firstRefresh;
    });
    expect(result.current.data).toEqual([
      { cluster: 'cluster-a', isHistorical: false },
    ]);
  });

  it('reacquires refresh ownership after the current request fails', async () => {
    jest.useFakeTimers();
    const failedRequest = deferred();
    const recoveredRequest = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    dashboardCache.get
      .mockImplementationOnce(() => failedRequest.promise)
      .mockImplementationOnce(() => recoveredRequest.promise);

    const { result } = renderHook(() =>
      useClusterData({ ...stableOptions, refreshInterval: 1000 })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    await act(async () => {
      failedRequest.reject(new Error('cluster list unavailable'));
      await failedRequest.promise.catch(() => {});
    });
    expect(result.current.loading).toBe(false);

    act(() => jest.advanceTimersByTime(1000));
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      recoveredRequest.resolve([{ cluster: 'cluster-b' }]);
      await recoveredRequest.promise;
    });
    expect(result.current.data).toEqual([
      { cluster: 'cluster-b', isHistorical: false },
    ]);
    expect(consoleError).toHaveBeenCalledTimes(1);
  });

  it('does not let an old completion clear the new page owner', async () => {
    jest.useFakeTimers();
    const firstPage = deferred();
    const secondPage = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => firstPage.promise)
      .mockImplementationOnce(() => secondPage.promise);

    const { result } = renderHook(() =>
      useClusterData({ ...stableOptions, refreshInterval: 1000 })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    act(() => result.current.setPage(2));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      firstPage.resolve([{ cluster: 'stale-page-1' }]);
      await firstPage.promise;
    });

    act(() => jest.advanceTimersByTime(1000));
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      secondPage.resolve([{ cluster: 'fresh-page-2' }]);
      await secondPage.promise;
    });
    for (let i = 0; i < 4; i += 1) {
      await act(async () => {
        await Promise.resolve();
      });
    }
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('keeps the latest server page and suppresses a stale prefetch', async () => {
    const firstPage = deferred();
    const secondPage = deferred();
    window.__skyPaginationFetch = jest.fn();
    dashboardCache.get
      .mockImplementationOnce(() => firstPage.promise)
      .mockImplementationOnce(() => secondPage.promise)
      .mockResolvedValue({ items: [] });

    const { result } = renderHook(() => useClusterData(stableOptions));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    act(() => result.current.setPage(2));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      secondPage.resolve({
        items: [{ cluster: 'fresh-page-2' }],
        total: 11,
        totalPages: 2,
        hasNext: false,
        hasPrev: true,
      });
      await secondPage.promise;
    });
    expect(result.current.data).toEqual([{ cluster: 'fresh-page-2' }]);
    expect(result.current.loading).toBe(false);

    await act(async () => {
      firstPage.resolve({
        items: [{ cluster: 'stale-page-1' }],
        total: 20,
        totalPages: 2,
        hasNext: true,
        hasPrev: false,
      });
      await firstPage.promise;
    });

    expect(result.current.data).toEqual([{ cluster: 'fresh-page-2' }]);
    expect(result.current.page).toBe(2);
    expect(result.current.total).toBe(11);
    expect(result.current.hasPrev).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('keeps the latest server refresh loading when an older request fails', async () => {
    const initialRequest = deferred();
    const refreshedRequest = deferred();
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    window.__skyPaginationFetch = jest.fn();
    dashboardCache.get
      .mockImplementationOnce(() => initialRequest.promise)
      .mockImplementationOnce(() => refreshedRequest.promise);

    const { result } = renderHook(() => useClusterData(stableOptions));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refresh();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      initialRequest.reject(new Error('superseded request failed'));
      await initialRequest.promise.catch(() => {});
    });
    expect(result.current.loading).toBe(true);
    expect(result.current.error).toBeNull();
    expect(consoleError).not.toHaveBeenCalled();

    await act(async () => {
      refreshedRequest.resolve({
        items: [{ cluster: 'fresh' }],
        total: 1,
      });
      await refreshPromise;
    });
    expect(result.current.data).toEqual([{ cluster: 'fresh' }]);
    expect(result.current.loading).toBe(false);
  });

  it('keeps the latest client refresh when an older request resolves last', async () => {
    const initialRequest = deferred();
    const refreshedRequest = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => initialRequest.promise)
      .mockImplementationOnce(() => refreshedRequest.promise);

    const { result } = renderHook(() => useClusterData(stableOptions));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refresh();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      refreshedRequest.resolve([{ cluster: 'fresh' }]);
      await refreshPromise;
    });
    expect(result.current.data).toEqual([
      { cluster: 'fresh', isHistorical: false },
    ]);

    await act(async () => {
      initialRequest.resolve([{ cluster: 'stale' }]);
      await initialRequest.promise;
    });
    expect(result.current.data).toEqual([
      { cluster: 'fresh', isHistorical: false },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('does not prefetch after the hook unmounts', async () => {
    const initialRequest = deferred();
    window.__skyPaginationFetch = jest.fn();
    dashboardCache.get
      .mockImplementationOnce(() => initialRequest.promise)
      .mockResolvedValue({ items: [] });

    const { unmount } = renderHook(() => useClusterData(stableOptions));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    unmount();

    await act(async () => {
      initialRequest.resolve({
        items: [{ cluster: 'stale' }],
        total: 20,
        totalPages: 2,
        hasNext: true,
      });
      await initialRequest.promise;
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });

  it('keeps one foreground read and one prefetch for the current page', async () => {
    window.__skyPaginationFetch = jest.fn();
    dashboardCache.get
      .mockResolvedValueOnce({
        items: [{ cluster: 'page-1' }],
        total: 20,
        totalPages: 2,
        hasNext: true,
      })
      .mockResolvedValueOnce({ items: [{ cluster: 'page-2' }] });

    const { result } = renderHook(() => useClusterData(stableOptions));
    await waitFor(() => expect(result.current.loading).toBe(false));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    expect(dashboardCache.get).toHaveBeenNthCalledWith(
      2,
      window.__skyPaginationFetch,
      [
        expect.objectContaining({
          page: 2,
          limit: 10,
        }),
      ],
      { ttl: 30000 }
    );
  });
});
