import {
  act,
  render,
  renderHook,
  screen,
  waitFor,
} from '@testing-library/react';

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
  },
}));

const mockUseRouter = jest.fn();

jest.mock('next/router', () => ({
  useRouter: () => mockUseRouter(),
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

import dashboardCache from '@/lib/cache';
import { getVolumes } from '@/data/connectors/volumes';
import VolumeDetailsPage, { useVolumeDetails } from '@/pages/volumes/[volume]';

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
    jest.resetAllMocks();
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

  it('coalesces a manual refresh with an in-flight route load for the new volume', async () => {
    const volumeB = deferred();
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'volume-a', status: 'READY-A' }])
      .mockImplementationOnce(() => volumeB.promise)
      .mockResolvedValueOnce([{ name: 'volume-b', status: 'DUPLICATE' }]);

    const { result, rerender } = renderHook(
      ({ volumeName }) => useVolumeDetails({ volumeName }),
      { initialProps: { volumeName: 'volume-a' } }
    );
    await waitFor(() =>
      expect(result.current.volumeData?.status).toBe('READY-A')
    );

    rerender({ volumeName: 'volume-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshData();
    });

    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      volumeB.resolve([{ name: 'volume-b', status: 'READY-B' }]);
      await refreshPromise;
    });

    expect(result.current.volumeData?.status).toBe('READY-B');
    expect(result.current.loading).toBe(false);
  });

  it('scopes duplicate manual refresh invalidation to the current volume key', async () => {
    const initial = deferred();
    const refresh = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => initial.promise)
      .mockImplementationOnce(() => refresh.promise);

    const { result } = renderHook(() =>
      useVolumeDetails({ volumeName: 'volume-a' })
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    await act(async () => {
      initial.resolve([{ name: 'volume-a', status: 'INITIAL' }]);
      await initial.promise;
    });

    let firstRefreshPromise;
    let duplicateRefreshPromise;
    act(() => {
      firstRefreshPromise = result.current.refreshData();
      duplicateRefreshPromise = result.current.refreshData();
    });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    expect(duplicateRefreshPromise).toBe(firstRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getVolumes, [
      { name: 'volume-a' },
    ]);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();

    await act(async () => {
      refresh.resolve([{ name: 'volume-a', status: 'NEW' }]);
      await Promise.all([firstRefreshPromise, duplicateRefreshPromise]);
    });
    expect(result.current.volumeData.status).toBe('NEW');
    expect(result.current.loading).toBe(false);

    dashboardCache.get.mockResolvedValueOnce([
      { name: 'volume-a', status: 'NEWER' },
    ]);
    let laterRefreshPromise;
    act(() => {
      laterRefreshPromise = result.current.refreshData();
    });
    await act(async () => {
      await laterRefreshPromise;
    });

    expect(result.current.volumeData.status).toBe('NEWER');
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidate).toHaveBeenNthCalledWith(2, getVolumes, [
      { name: 'volume-a' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('releases refresh ownership after a failed refresh', async () => {
    const failedRefresh = deferred();
    jest.spyOn(console, 'error').mockImplementation(() => {});
    dashboardCache.get
      .mockResolvedValueOnce([{ name: 'volume-a', status: 'INITIAL' }])
      .mockImplementationOnce(() => failedRefresh.promise);

    const { result } = renderHook(() =>
      useVolumeDetails({ volumeName: 'volume-a' })
    );
    await waitFor(() =>
      expect(result.current.volumeData?.status).toBe('INITIAL')
    );

    let firstRefreshPromise;
    let duplicateRefreshPromise;
    act(() => {
      firstRefreshPromise = result.current.refreshData();
      duplicateRefreshPromise = result.current.refreshData();
    });

    expect(duplicateRefreshPromise).toBe(firstRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getVolumes, [
      { name: 'volume-a' },
    ]);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      failedRefresh.reject(new Error('refresh failed'));
      await Promise.all([firstRefreshPromise, duplicateRefreshPromise]);
    });

    dashboardCache.get.mockResolvedValueOnce([
      { name: 'volume-a', status: 'RECOVERED' },
    ]);
    let recoveredRefreshPromise;
    act(() => {
      recoveredRefreshPromise = result.current.refreshData();
    });

    expect(recoveredRefreshPromise).not.toBe(firstRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidate).toHaveBeenNthCalledWith(2, getVolumes, [
      { name: 'volume-a' },
    ]);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();

    await act(async () => {
      await recoveredRefreshPromise;
    });

    expect(result.current.volumeData.status).toBe('RECOVERED');
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('does not reuse an old refresh owner after leaving and returning to a volume', async () => {
    const oldRefresh = deferred();
    const newRefresh = deferred();
    dashboardCache.get.mockResolvedValueOnce([
      { name: 'volume-a', status: 'volume-a-initial' },
    ]);

    const { result, rerender } = renderHook(
      ({ volumeName }) => useVolumeDetails({ volumeName }),
      { initialProps: { volumeName: 'volume-a' } }
    );
    await waitFor(() =>
      expect(result.current.volumeData?.status).toBe('volume-a-initial')
    );

    dashboardCache.get.mockImplementationOnce(() => oldRefresh.promise);
    let oldRefreshPromise;
    act(() => {
      oldRefreshPromise = result.current.refreshData();
    });

    dashboardCache.get.mockResolvedValueOnce([
      { name: 'volume-b', status: 'volume-b-full' },
    ]);
    rerender({ volumeName: 'volume-b' });
    await waitFor(() =>
      expect(result.current.volumeData?.status).toBe('volume-b-full')
    );

    dashboardCache.get.mockResolvedValueOnce([
      { name: 'volume-a', status: 'volume-a-return' },
    ]);
    rerender({ volumeName: 'volume-a' });
    await waitFor(() =>
      expect(result.current.volumeData?.status).toBe('volume-a-return')
    );

    dashboardCache.get.mockImplementationOnce(() => newRefresh.promise);
    let newRefreshPromise;
    act(() => {
      newRefreshPromise = result.current.refreshData();
    });

    expect(newRefreshPromise).not.toBe(oldRefreshPromise);
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidate).toHaveBeenNthCalledWith(1, getVolumes, [
      { name: 'volume-a' },
    ]);
    expect(dashboardCache.invalidate).toHaveBeenNthCalledWith(2, getVolumes, [
      { name: 'volume-a' },
    ]);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();

    await act(async () => {
      newRefresh.resolve([{ name: 'volume-a', status: 'volume-a-new' }]);
      await newRefreshPromise;
    });
    expect(result.current.volumeData.status).toBe('volume-a-new');

    await act(async () => {
      oldRefresh.resolve([{ name: 'volume-a', status: 'volume-a-old' }]);
      await oldRefreshPromise;
    });

    expect(result.current.volumeData.status).toBe('volume-a-new');
  });
});

describe('VolumeDetails route ownership rendering', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  it('keeps the page in loading state instead of flashing not found on a route change', async () => {
    const nextVolume = deferred();
    const routerState = {
      isReady: true,
      query: { volume: 'volume-a' },
    };
    mockUseRouter.mockImplementation(() => routerState);

    dashboardCache.get.mockResolvedValueOnce([
      { name: 'volume-a', status: 'READY', user_name: 'alice' },
    ]);

    const { rerender } = render(<VolumeDetailsPage />);

    await waitFor(() =>
      expect(screen.getAllByText('volume-a')).not.toHaveLength(0)
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    dashboardCache.get.mockImplementationOnce(() => nextVolume.promise);
    routerState.query = { volume: 'volume-b' };
    rerender(<VolumeDetailsPage />);

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    expect(screen.getByText('Loading volume details...')).toBeInTheDocument();
    expect(screen.queryByText('Volume not found.')).not.toBeInTheDocument();
    expect(screen.queryByText('volume-a')).not.toBeInTheDocument();

    await act(async () => {
      nextVolume.resolve([
        { name: 'volume-b', status: 'READY', user_name: 'bob' },
      ]);
      await nextVolume.promise;
    });

    expect(screen.getAllByText('volume-b')).not.toHaveLength(0);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });
});
