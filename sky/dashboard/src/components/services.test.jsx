import React from 'react';
import { render, act, screen } from '@testing-library/react';

// Mock the shared dashboard cache so we can count fetch invocations
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

import dashboardCache from '@/lib/cache';
import { Services, ServicesTable } from '@/components/services';

const SERVICES_RESPONSE = {
  services: [
    {
      name: 'boltz-l4-fleet',
      status: 'READY',
      uptime: 1751600000,
      endpoint: 'http://10.0.0.1:30001',
      replicasReady: 1,
      replicasTotal: 1,
      replicasFailed: 0,
      policy: 'autoscaling(min=1,max=4)',
      requestedResources: '1x[L4:1]',
      replicas: [],
    },
  ],
  controllerStopped: false,
};

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function responseFor(name) {
  return {
    ...SERVICES_RESPONSE,
    services: [{ ...SERVICES_RESPONSE.services[0], name }],
  };
}

// Flush the pending fetch promise chain and any state updates it causes.
// Repeated rounds give an unstable-callback fetch loop (fetch -> state
// update -> new callback identity -> effect re-run -> fetch ...) the
// chance to manifest as extra dashboardCache.get calls.
async function flushFetches(rounds = 4) {
  for (let i = 0; i < rounds; i += 1) {
    await act(async () => {
      await Promise.resolve();
    });
  }
}

function StatefulServicesTable({ refreshDataRef }) {
  const [loading, setLoading] = React.useState(false);
  return (
    <ServicesTable
      refreshInterval={30000}
      loading={loading}
      setLoading={setLoading}
      refreshDataRef={refreshDataRef}
    />
  );
}

describe('Services fetch wiring', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
    dashboardCache.get.mockResolvedValue(SERVICES_RESPONSE);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('fetches exactly once on mount despite rerenders from fetch-driven state updates', async () => {
    render(<Services />);

    await flushFetches();

    // The fetch updates loading state and the last-fetched timestamp in
    // the parent; those rerenders must NOT retrigger the fetch effect.
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });

  it('fetches again only when the refresh interval elapses', async () => {
    render(<Services />);
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    // Just under the 30s interval: no new fetch (the 10s ticks of the
    // last-updated timestamp rerender the parent along the way).
    await act(async () => {
      jest.advanceTimersByTime(29000);
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    // Crossing the interval triggers exactly one more fetch.
    await act(async () => {
      jest.advanceTimersByTime(2000);
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('coalesces interval ticks while the current request is pending', async () => {
    const pendingRequest = deferred();
    dashboardCache.get.mockReturnValue(pendingRequest.promise);

    render(<Services />);
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(90000);
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    await act(async () => {
      pendingRequest.resolve(SERVICES_RESPONSE);
      await pendingRequest.promise;
    });
  });

  it('lets a manual refresh supersede an older request', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise);

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    await act(async () => {
      refreshDataRef.current();
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      oldRequest.resolve(responseFor('stale-service'));
      await oldRequest.promise;
    });

    expect(screen.queryByText('stale-service')).not.toBeInTheDocument();
    expect(screen.getAllByText('Loading...').length).toBeGreaterThan(0);

    await act(async () => {
      currentRequest.resolve(responseFor('current-service'));
      await currentRequest.promise;
    });

    expect(screen.getByText('current-service')).toBeInTheDocument();
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('does not let an older failure erase a newer manual refresh', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    const refreshDataRef = { current: null };
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise);

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    await act(async () => {
      refreshDataRef.current();
      currentRequest.resolve(responseFor('current-service'));
      await currentRequest.promise;
    });
    expect(screen.getByText('current-service')).toBeInTheDocument();

    await act(async () => {
      oldRequest.reject(new Error('stale failure'));
      await oldRequest.promise.catch(() => {});
    });

    expect(screen.getByText('current-service')).toBeInTheDocument();
    expect(consoleError).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    consoleError.mockRestore();
  });

  it('invalidates request ownership when the table unmounts', async () => {
    const pendingRequest = deferred();
    const setLoading = jest.fn();
    const onFetched = jest.fn();
    dashboardCache.get.mockReturnValueOnce(pendingRequest.promise);

    const { unmount } = render(
      <ServicesTable
        refreshInterval={30000}
        loading={false}
        setLoading={setLoading}
        refreshDataRef={{ current: null }}
        onFetched={onFetched}
      />
    );
    expect(setLoading).toHaveBeenCalledWith(true);
    setLoading.mockClear();

    unmount();
    await act(async () => {
      pendingRequest.resolve(SERVICES_RESPONSE);
      await pendingRequest.promise;
    });

    expect(onFetched).not.toHaveBeenCalled();
    expect(setLoading).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });
});
