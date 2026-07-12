import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

const router = {
  isReady: true,
  query: { pool: 'pool-a' },
};

jest.mock('next/router', () => ({
  useRouter: () => router,
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
import PoolDetailPage from '@/pages/jobs/pools/[pool]';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function pool(name, resources) {
  return {
    name,
    requested_resources_str: resources,
    replica_info: [],
  };
}

describe('PoolDetailPage request ownership', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    router.query.pool = 'pool-a';
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('drops a stale result from the previous pool route', async () => {
    const poolA = deferred();
    const poolB = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => poolA.promise)
      .mockImplementationOnce(() => poolB.promise);

    const view = render(<PoolDetailPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    router.query.pool = 'pool-b';
    view.rerender(<PoolDetailPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      poolB.resolve({ pools: [pool('pool-b', 'fresh-b')] });
      await poolB.promise;
    });
    expect(screen.getByText('fresh-b')).toBeInTheDocument();

    await act(async () => {
      poolA.resolve({ pools: [pool('pool-a', 'stale-a')] });
      await poolA.promise;
    });

    expect(screen.getByText('fresh-b')).toBeInTheDocument();
    expect(screen.queryByText('stale-a')).not.toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('ignores a stale failure after the new pool route succeeds', async () => {
    const poolA = deferred();
    const poolB = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => poolA.promise)
      .mockImplementationOnce(() => poolB.promise);
    const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => {});

    const view = render(<PoolDetailPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    router.query.pool = 'pool-b';
    view.rerender(<PoolDetailPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      poolB.resolve({ pools: [pool('pool-b', 'fresh-b')] });
      await poolB.promise;
    });
    await act(async () => {
      poolA.reject(new Error('stale failure'));
      await expect(poolA.promise).rejects.toThrow('stale failure');
    });

    expect(screen.getByText('fresh-b')).toBeInTheDocument();
    expect(screen.queryByText(/stale failure/)).not.toBeInTheDocument();
    expect(errorSpy).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('lets a route change supersede an in-flight retry', async () => {
    const initial = deferred();
    const retry = deferred();
    const poolB = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => initial.promise)
      .mockImplementationOnce(() => retry.promise)
      .mockImplementationOnce(() => poolB.promise);
    jest.spyOn(console, 'error').mockImplementation(() => {});

    const view = render(<PoolDetailPage />);
    await act(async () => {
      initial.reject(new Error('load failed'));
      await expect(initial.promise).rejects.toThrow('load failed');
    });
    await screen.findByText('Failed to fetch pool data: load failed');

    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    router.query.pool = 'pool-b';
    view.rerender(<PoolDetailPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    await act(async () => {
      poolB.resolve({ pools: [pool('pool-b', 'fresh-b')] });
      await poolB.promise;
    });
    await act(async () => {
      retry.resolve({ pools: [pool('pool-a', 'stale-retry')] });
      await retry.promise;
    });

    expect(screen.getByText('fresh-b')).toBeInTheDocument();
    expect(screen.queryByText('stale-retry')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Refresh' })).not.toBeDisabled();
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });
});
