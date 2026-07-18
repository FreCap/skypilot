import React from 'react';
import { act, render, screen } from '@testing-library/react';

import { PoolsTable } from '@/components/pools-table';
import { getPoolStatus } from '@/data/connectors/jobs';
import dashboardCache from '@/lib/cache';

jest.mock('@/data/connectors/jobs', () => ({
  getPoolStatus: jest.fn(),
}));

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function poolResponse(name) {
  return {
    pools: [
      {
        name,
        jobCounts: {},
        replica_info: [],
        target_num_replicas: 0,
        requested_resources_str: '1x A100',
      },
    ],
  };
}

async function flushPromises() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

describe('PoolsTable refresh lifecycle', () => {
  beforeEach(() => {
    jest.useFakeTimers();
    dashboardCache.clear();
    getPoolStatus.mockReset();
    Object.defineProperty(window.document, 'visibilityState', {
      configurable: true,
      value: 'visible',
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
    jest.useRealTimers();
    dashboardCache.clear();
  });

  it('publishes each automatic refresh and coalesces overdue timer ticks', async () => {
    getPoolStatus.mockResolvedValueOnce(poolResponse('pool-a'));
    const getSpy = jest.spyOn(dashboardCache, 'get');
    const invalidateSpy = jest.spyOn(dashboardCache, 'invalidate');
    const { unmount } = render(
      <PoolsTable
        refreshInterval={30_000}
        setLoading={jest.fn()}
        refreshDataRef={{ current: null }}
      />
    );
    await flushPromises();
    expect(screen.getByRole('link', { name: 'pool-a' })).toBeInTheDocument();

    getSpy.mockClear();
    invalidateSpy.mockClear();
    const refresh = deferred();
    getPoolStatus.mockReturnValueOnce(refresh.promise);

    act(() => {
      jest.advanceTimersByTime(30_000);
    });
    act(() => {
      jest.advanceTimersByTime(60_000);
    });

    expect(getSpy).toHaveBeenCalledTimes(1);
    expect(invalidateSpy).toHaveBeenCalledTimes(1);
    expect(getPoolStatus).toHaveBeenCalledTimes(2);

    await act(async () => {
      refresh.resolve(poolResponse('pool-b'));
      await refresh.promise;
    });
    expect(screen.getByRole('link', { name: 'pool-b' })).toBeInTheDocument();
    expect(
      screen.queryByRole('link', { name: 'pool-a' })
    ).not.toBeInTheDocument();

    unmount();
  });

  it('lets a manual refresh supersede a pending poll and revokes its ref on unmount', async () => {
    getPoolStatus.mockResolvedValueOnce(poolResponse('pool-a'));
    const refreshDataRef = { current: null };
    const { unmount } = render(
      <PoolsTable
        refreshInterval={30_000}
        setLoading={jest.fn()}
        refreshDataRef={refreshDataRef}
      />
    );
    await flushPromises();

    const automatic = deferred();
    const manual = deferred();
    getPoolStatus
      .mockReturnValueOnce(automatic.promise)
      .mockReturnValueOnce(manual.promise);
    act(() => {
      jest.advanceTimersByTime(30_000);
    });

    dashboardCache.invalidate(getPoolStatus, [{}]);
    let manualRefresh;
    act(() => {
      manualRefresh = refreshDataRef.current();
    });
    expect(getPoolStatus).toHaveBeenCalledTimes(3);

    await act(async () => {
      manual.resolve(poolResponse('pool-manual'));
      await Promise.all([manual.promise, manualRefresh]);
    });
    expect(
      screen.getByRole('link', { name: 'pool-manual' })
    ).toBeInTheDocument();

    await act(async () => {
      automatic.resolve(poolResponse('pool-stale'));
      await automatic.promise;
    });
    expect(
      screen.getByRole('link', { name: 'pool-manual' })
    ).toBeInTheDocument();
    expect(
      screen.queryByRole('link', { name: 'pool-stale' })
    ).not.toBeInTheDocument();

    unmount();
    expect(refreshDataRef.current).toBeNull();
    act(() => {
      jest.advanceTimersByTime(60_000);
    });
    expect(getPoolStatus).toHaveBeenCalledTimes(3);
  });

  it('does not let a timer tick invalidate a pending manual refresh', async () => {
    getPoolStatus.mockResolvedValueOnce(poolResponse('pool-a'));
    const refreshDataRef = { current: null };
    const { unmount } = render(
      <PoolsTable
        refreshInterval={30_000}
        setLoading={jest.fn()}
        refreshDataRef={refreshDataRef}
      />
    );
    await flushPromises();

    const manual = deferred();
    getPoolStatus.mockReturnValueOnce(manual.promise);
    dashboardCache.invalidate(getPoolStatus, [{}]);
    let manualRefresh;
    act(() => {
      manualRefresh = refreshDataRef.current();
      jest.advanceTimersByTime(90_000);
    });

    expect(getPoolStatus).toHaveBeenCalledTimes(2);
    await act(async () => {
      manual.resolve(poolResponse('pool-manual'));
      await Promise.all([manual.promise, manualRefresh]);
    });
    expect(
      screen.getByRole('link', { name: 'pool-manual' })
    ).toBeInTheDocument();

    unmount();
  });

  it('keeps the last snapshot after a failed poll and retries next interval', async () => {
    getPoolStatus.mockResolvedValueOnce(poolResponse('pool-a'));
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    const { unmount } = render(
      <PoolsTable
        refreshInterval={30_000}
        setLoading={jest.fn()}
        refreshDataRef={{ current: null }}
      />
    );
    await flushPromises();

    getPoolStatus.mockRejectedValueOnce(new Error('poll unavailable'));
    await act(async () => {
      jest.advanceTimersByTime(30_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByRole('link', { name: 'pool-a' })).toBeInTheDocument();

    getPoolStatus.mockResolvedValueOnce(poolResponse('pool-b'));
    await act(async () => {
      jest.advanceTimersByTime(30_000);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByRole('link', { name: 'pool-b' })).toBeInTheDocument();
    expect(getPoolStatus).toHaveBeenCalledTimes(3);
    expect(consoleError).toHaveBeenCalledWith(
      'Error fetching pools data:',
      expect.any(Error)
    );

    unmount();
  });
});
