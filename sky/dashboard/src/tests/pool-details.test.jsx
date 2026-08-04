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

const mockInfraBadgeRenders = [];
jest.mock('@/components/utils', () => {
  const actual = jest.requireActual('@/components/utils');
  return {
    ...actual,
    InfraBadges: (props) => {
      mockInfraBadgeRenders.push(props);
      return null;
    },
  };
});

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
import { getPoolStatus } from '@/data/connectors/jobs';
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
    mockInfraBadgeRenders.length = 0;
    router.query.pool = 'pool-a';
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('hides the previous pool on the first route render', async () => {
    const poolB = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({ pools: [pool('pool-a', 'owned-a')] })
      .mockImplementationOnce(() => poolB.promise);

    const view = render(<PoolDetailPage />);
    await screen.findByText('owned-a');
    expect(mockInfraBadgeRenders).toHaveLength(1);

    mockInfraBadgeRenders.length = 0;
    router.query.pool = 'pool-b';
    view.rerender(<PoolDetailPage />);

    // Child renders capture the commit boundary before route effects run.
    // Pool A must not render actions or details once the URL belongs to B.
    expect(mockInfraBadgeRenders).toHaveLength(0);
    expect(screen.queryByText('owned-a')).not.toBeInTheDocument();
    expect(screen.getByText('Loading pool details...')).toBeInTheDocument();

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(dashboardCache.get).toHaveBeenNthCalledWith(2, getPoolStatus, [
      { poolNames: ['pool-b'] },
    ]);
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();

    await act(async () => {
      poolB.resolve({ pools: [pool('pool-b', 'owned-b')] });
      await poolB.promise;
    });
    expect(screen.getByText('owned-b')).toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('drops a stale result from the previous pool route', async () => {
    const poolA = deferred();
    const poolB = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => poolA.promise)
      .mockImplementationOnce(() => poolB.promise);

    const view = render(<PoolDetailPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(dashboardCache.get).toHaveBeenNthCalledWith(1, getPoolStatus, [
      { poolNames: ['pool-a'] },
    ]);

    router.query.pool = 'pool-b';
    view.rerender(<PoolDetailPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(dashboardCache.get).toHaveBeenNthCalledWith(2, getPoolStatus, [
      { poolNames: ['pool-b'] },
    ]);

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
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getPoolStatus, [
      { poolNames: ['pool-a'] },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('coalesces duplicate refreshes and releases ownership after success', async () => {
    const refresh = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({ pools: [pool('pool-a', 'initial')] })
      .mockImplementationOnce(() => refresh.promise);

    render(<PoolDetailPage />);
    await screen.findByText('initial');

    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    act(() => {
      refreshButton.click();
      refreshButton.click();
    });

    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getPoolStatus, [
      { poolNames: ['pool-a'] },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenLastCalledWith(getPoolStatus, [
      { poolNames: ['pool-a'] },
    ]);
    expect(refreshButton).toBeDisabled();

    await act(async () => {
      refresh.resolve({ pools: [pool('pool-a', 'fresh')] });
      await refresh.promise;
    });
    expect(await screen.findByText('fresh')).toBeInTheDocument();

    dashboardCache.get.mockResolvedValueOnce({
      pools: [pool('pool-a', 'newer')],
    });
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));

    expect(await screen.findByText('newer')).toBeInTheDocument();
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('coalesces duplicate retries and releases ownership after failure', async () => {
    const initial = deferred();
    const retry = deferred();
    dashboardCache.get
      .mockImplementationOnce(() => initial.promise)
      .mockImplementationOnce(() => retry.promise);
    jest.spyOn(console, 'error').mockImplementation(() => {});

    render(<PoolDetailPage />);
    await act(async () => {
      initial.reject(new Error('load failed'));
      await expect(initial.promise).rejects.toThrow('load failed');
    });
    expect(
      await screen.findByText('Failed to fetch pool data: load failed')
    ).toBeInTheDocument();

    const retryButton = screen.getByRole('button', { name: 'Retry' });
    act(() => {
      retryButton.click();
      retryButton.click();
    });

    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(retryButton).toBeDisabled();
    expect(
      screen.getByText('Failed to fetch pool data: load failed')
    ).toBeInTheDocument();

    await act(async () => {
      retry.reject(new Error('retry failed'));
      await expect(retry.promise).rejects.toThrow('retry failed');
    });
    expect(
      await screen.findByText('Failed to fetch pool data: retry failed')
    ).toBeInTheDocument();

    dashboardCache.get.mockResolvedValueOnce({
      pools: [pool('pool-a', 'recovered')],
    });
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));

    expect(await screen.findByText('recovered')).toBeInTheDocument();
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });
});
