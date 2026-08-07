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
import { useHistoricalClusterLookup } from '@/pages/clusters/[cluster]';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('useHistoricalClusterLookup stale-response fencing', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('keeps history unsettled until the current route lookup finishes', async () => {
    const pendingLookup = deferred();

    dashboardCache.get.mockImplementationOnce(() => pendingLookup.promise);

    const { result } = renderHook(() =>
      useHistoricalClusterLookup({
        cluster: 'cluster-a',
        clusterData: null,
        clusterDetailsLoading: false,
      })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(result.current.historyLoading).toBe(true);
    expect(result.current.historySettled).toBe(false);

    await act(async () => {
      pendingLookup.resolve([]);
      await Promise.resolve();
    });

    expect(result.current.historyData).toBeNull();
    expect(result.current.isHistoricalCluster).toBe(false);
    expect(result.current.historyLoading).toBe(false);
    expect(result.current.historySettled).toBe(true);
  });

  it('drops stale history results from a previous route target', async () => {
    const firstLookup = deferred();
    const secondLookup = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => firstLookup.promise)
      .mockImplementationOnce(() => secondLookup.promise);

    const { result, rerender } = renderHook(
      ({ cluster, clusterData, clusterDetailsLoading }) =>
        useHistoricalClusterLookup({
          cluster,
          clusterData,
          clusterDetailsLoading,
        }),
      {
        initialProps: {
          cluster: 'cluster-a',
          clusterData: null,
          clusterDetailsLoading: false,
        },
      }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    rerender({
      cluster: 'cluster-b',
      clusterData: null,
      clusterDetailsLoading: false,
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      secondLookup.resolve([
        { cluster: 'cluster-b', cluster_hash: 'cluster-b-hash' },
      ]);
      await Promise.resolve();
    });
    expect(result.current.historyData.cluster).toBe('cluster-b');
    expect(result.current.isHistoricalCluster).toBe(true);
    expect(result.current.historyLoading).toBe(false);
    expect(result.current.historySettled).toBe(true);

    await act(async () => {
      firstLookup.resolve([
        { cluster: 'cluster-a', cluster_hash: 'cluster-a-hash' },
      ]);
      await Promise.resolve();
    });

    expect(result.current.historyData.cluster).toBe('cluster-b');
    expect(result.current.isHistoricalCluster).toBe(true);
    expect(result.current.historyLoading).toBe(false);
    expect(result.current.historySettled).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('clears fallback state once active cluster data appears', async () => {
    const pendingLookup = deferred();

    dashboardCache.get.mockImplementationOnce(() => pendingLookup.promise);

    const { result, rerender } = renderHook(
      ({ cluster, clusterData, clusterDetailsLoading }) =>
        useHistoricalClusterLookup({
          cluster,
          clusterData,
          clusterDetailsLoading,
        }),
      {
        initialProps: {
          cluster: 'cluster-a',
          clusterData: null,
          clusterDetailsLoading: false,
        },
      }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(result.current.historyLoading).toBe(true);
    expect(result.current.historySettled).toBe(false);

    rerender({
      cluster: 'cluster-a',
      clusterData: { cluster: 'cluster-a', status: 'RUNNING' },
      clusterDetailsLoading: false,
    });

    expect(result.current.historyData).toBeNull();
    expect(result.current.isHistoricalCluster).toBe(false);
    expect(result.current.historyLoading).toBe(false);
    expect(result.current.historySettled).toBe(true);

    await act(async () => {
      pendingLookup.resolve([
        { cluster: 'cluster-a', cluster_hash: 'cluster-a-hash' },
      ]);
      await Promise.resolve();
    });

    expect(result.current.historyData).toBeNull();
    expect(result.current.isHistoricalCluster).toBe(false);
    expect(result.current.historyLoading).toBe(false);
    expect(result.current.historySettled).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });
});
