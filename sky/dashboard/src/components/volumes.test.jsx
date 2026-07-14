import React from 'react';
import { act, render, screen } from '@testing-library/react';

jest.mock('next/router', () => ({
  useRouter: () => ({
    isReady: true,
    pathname: '/volumes',
    query: {},
    replace: jest.fn(),
  }),
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/plugins/PluginProvider', () => ({
  usePluginComponents: () => [],
  useTableColumns: () => [],
}));

jest.mock('@/lib/analytics', () => ({
  trackVolumeAction: jest.fn(),
}));

jest.mock('@/lib/cache-preloader', () => ({
  __esModule: true,
  default: {
    preloadForPage: jest.fn(),
  },
}));

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
  },
}));

import { Volumes, VolumesTable } from '@/components/volumes';
import dashboardCache from '@/lib/cache';
import cachePreloader from '@/lib/cache-preloader';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function volume(name) {
  return {
    name,
    status: 'READY',
    size: 100,
    type: 'k8s-pvc',
  };
}

async function flushPromises(rounds = 6) {
  for (let i = 0; i < rounds; i += 1) {
    await act(async () => {
      await Promise.resolve();
    });
  }
}

describe('Volumes request ownership', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
    cachePreloader.preloadForPage.mockResolvedValue();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('keeps the latest interval request in control when an older success finishes', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise);

    render(<Volumes />);
    await flushPromises();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(30000);
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      oldRequest.resolve([volume('stale-volume')]);
      await oldRequest.promise;
    });

    expect(screen.queryByText('stale-volume')).not.toBeInTheDocument();
    expect(screen.getAllByText('Loading...').length).toBeGreaterThan(0);

    await act(async () => {
      currentRequest.resolve([volume('current-volume')]);
      await currentRequest.promise;
    });

    expect(screen.getByText('current-volume')).toBeInTheDocument();
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('does not let an older failure erase a newer interval result', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise);

    render(<Volumes />);
    await flushPromises();

    await act(async () => {
      jest.advanceTimersByTime(30000);
      currentRequest.resolve([volume('current-volume')]);
      await currentRequest.promise;
    });
    expect(screen.getByText('current-volume')).toBeInTheDocument();

    await act(async () => {
      oldRequest.reject(new Error('stale failure'));
      await oldRequest.promise.catch(() => {});
    });

    expect(screen.getByText('current-volume')).toBeInTheDocument();
    expect(consoleError).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    consoleError.mockRestore();
  });

  it('invalidates request ownership when the table unmounts', async () => {
    const pendingRequest = deferred();
    const setLoading = jest.fn();
    const onDataChange = jest.fn();
    const refreshDataRef = { current: null };
    dashboardCache.get.mockReturnValueOnce(pendingRequest.promise);

    const { unmount } = render(
      <VolumesTable
        refreshInterval={30000}
        setLoading={setLoading}
        refreshDataRef={refreshDataRef}
        onDeleteVolume={jest.fn()}
        onDataChange={onDataChange}
        preloadingComplete={true}
      />
    );
    expect(setLoading).toHaveBeenCalledWith(true);
    expect(refreshDataRef.current).toEqual(expect.any(Function));
    setLoading.mockClear();

    unmount();
    await act(async () => {
      pendingRequest.resolve([volume('late-volume')]);
      await pendingRequest.promise;
    });

    expect(onDataChange).not.toHaveBeenCalled();
    expect(setLoading).not.toHaveBeenCalled();
    expect(refreshDataRef.current).toBeNull();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });
});
