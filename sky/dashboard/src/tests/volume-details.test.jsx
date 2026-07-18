import { act, renderHook, waitFor } from '@testing-library/react';

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
  },
}));

import dashboardCache from '@/lib/cache';
import { getVolumes } from '@/data/connectors/volumes';
import { useVolumeDetails } from '@/pages/volumes/[volume]';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('useVolumeDetails request ownership', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('hides data owned by the previous route while the new target loads', async () => {
    const volumeB = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'volume-a', status: 'READY' }])
      .mockImplementationOnce(() => volumeB.promise);

    const { result, rerender } = renderHook(
      ({ volumeName }) => useVolumeDetails({ volumeName }),
      { initialProps: { volumeName: 'volume-a' } }
    );
    await waitFor(() =>
      expect(result.current.volumeData?.name).toBe('volume-a')
    );

    rerender({ volumeName: 'volume-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    expect(result.current.volumeData).toBeNull();
    expect(result.current.loading).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(1, getVolumes, [
      { name: 'volume-a' },
    ]);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(2, getVolumes, [
      { name: 'volume-b' },
    ]);
  });

  it('drops a stale result from the previous route target', async () => {
    const volumeA = deferred();
    const volumeB = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => volumeA.promise)
      .mockImplementationOnce(() => volumeB.promise);

    const { result, rerender } = renderHook(
      ({ volumeName }) => useVolumeDetails({ volumeName }),
      { initialProps: { volumeName: 'volume-a' } }
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    rerender({ volumeName: 'volume-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      volumeB.resolve([{ name: 'volume-b', status: 'READY' }]);
      await volumeB.promise;
    });
    expect(result.current.volumeData.name).toBe('volume-b');

    await act(async () => {
      volumeA.resolve([{ name: 'volume-a', status: 'READY' }]);
      await volumeA.promise;
    });

    expect(result.current.volumeData.name).toBe('volume-b');
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('keeps fresh data when a previous route request fails late', async () => {
    const volumeA = deferred();
    const volumeB = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => volumeA.promise)
      .mockImplementationOnce(() => volumeB.promise);
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    const { result, rerender } = renderHook(
      ({ volumeName }) => useVolumeDetails({ volumeName }),
      { initialProps: { volumeName: 'volume-a' } }
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    rerender({ volumeName: 'volume-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      volumeB.resolve([{ name: 'volume-b', status: 'READY' }]);
      await volumeB.promise;
    });
    await act(async () => {
      volumeA.reject(new Error('stale failure'));
      await expect(volumeA.promise).rejects.toThrow('stale failure');
    });

    expect(result.current.volumeData.name).toBe('volume-b');
    expect(result.current.loading).toBe(false);
    expect(errorSpy).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('lets the newest manual refresh own data and loading state', async () => {
    const initial = deferred();
    const olderRefresh = deferred();
    const newerRefresh = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => initial.promise)
      .mockImplementationOnce(() => olderRefresh.promise)
      .mockImplementationOnce(() => newerRefresh.promise);

    const { result } = renderHook(() =>
      useVolumeDetails({ volumeName: 'volume-a' })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    await act(async () => {
      initial.resolve([{ name: 'volume-a', status: 'INITIAL' }]);
      await initial.promise;
    });

    let olderRefreshPromise;
    act(() => {
      olderRefreshPromise = result.current.refreshData();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    let newerRefreshPromise;
    act(() => {
      newerRefreshPromise = result.current.refreshData();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    await act(async () => {
      newerRefresh.resolve([{ name: 'volume-a', status: 'NEW' }]);
      await newerRefreshPromise;
    });
    expect(result.current.volumeData.status).toBe('NEW');
    expect(result.current.loading).toBe(false);

    await act(async () => {
      olderRefresh.resolve([{ name: 'volume-a', status: 'OLD' }]);
      await olderRefreshPromise;
    });

    expect(result.current.volumeData.status).toBe('NEW');
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.invalidateFunction).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidateFunction).toHaveBeenCalledWith(getVolumes);
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });
});
