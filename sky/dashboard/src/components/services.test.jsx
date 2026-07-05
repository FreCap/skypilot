import React from 'react';
import { render, act } from '@testing-library/react';

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
import { Services } from '@/components/services';

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
});
