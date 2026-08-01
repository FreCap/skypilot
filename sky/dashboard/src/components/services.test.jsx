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
import {
  Services,
  ServicesTable,
  getPastAttemptCount,
  getServiceOperationalState,
} from '@/components/services';
import { getServices } from '@/data/connectors/services';

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

const setDocumentVisibility = (value) => {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
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

  it('fetches exactly one metadata and one summary phase on mount', async () => {
    render(<Services />);

    await flushFetches();

    // The fetch updates loading state and the last-fetched timestamp in
    // the parent; those rerenders must NOT retrigger the fetch effect.
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    expect(dashboardCache.get.mock.calls).toEqual([
      [getServices, [{ metadataOnly: true }]],
      [getServices, [{ summaryOnly: true, includeEndpoints: true }]],
    ]);
  });

  it('renders metadata immediately and replaces placeholders when the summary lands', async () => {
    const summary = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            uptime: null,
            endpoint: null,
            policy: null,
            requestedResources: null,
            metadataOnly: true,
            replicasReady: null,
            replicasTotal: null,
            replicasFailed: null,
            replicaStatusCounts: null,
          },
        ],
        controllerStopped: false,
      })
      .mockReturnValueOnce(summary.promise);

    render(<Services />);
    await flushFetches();

    expect(screen.getByText('boltz-l4-fleet')).toBeInTheDocument();
    expect(screen.getByText('Serving')).toBeInTheDocument();
    expect(screen.getAllByText('Loading...')).toHaveLength(6);

    await act(async () => {
      summary.resolve({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            metadataOnly: false,
            summaryOnly: true,
            targetReplicas: 1,
            replicaStatusCounts: {
              READY: 1,
              FAILED_PROVISION: 199,
              FAILED_INITIAL_DELAY: 64,
              FAILED_PROBING: 20,
              FAILED: 38,
            },
          },
        ],
        controllerStopped: false,
      });
      await summary.promise;
    });

    expect(screen.getByText('Healthy')).toBeInTheDocument();
    expect(screen.getByText('321 past attempts')).toBeInTheDocument();
    expect(screen.getByText('http://10.0.0.1:30001')).toBeInTheDocument();
  });

  it('settles metadata placeholders as unavailable when enrichment fails', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            uptime: null,
            endpoint: null,
            policy: null,
            requestedResources: null,
            metadataOnly: true,
            replicasReady: null,
            replicasTotal: null,
          },
        ],
        controllerStopped: false,
      })
      .mockRejectedValueOnce(new Error('summary unavailable'));

    render(<Services />);
    await flushFetches();

    expect(screen.getByText('boltz-l4-fleet')).toBeInTheDocument();
    expect(screen.getByText('Serving')).toBeInTheDocument();
    expect(screen.getAllByText('Unavailable')).toHaveLength(5);
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
    consoleError.mockRestore();
  });

  it('fetches again only when the refresh interval elapses', async () => {
    render(<Services />);
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    // Just under the 30s interval: no new fetch (the 10s ticks of the
    // last-updated timestamp rerender the parent along the way).
    await act(async () => {
      jest.advanceTimersByTime(29000);
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    // Crossing the interval triggers exactly one more fetch.
    await act(async () => {
      jest.advanceTimersByTime(2000);
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
  });

  it('refreshes once immediately when the page becomes visible again', async () => {
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('hidden');
    const { unmount } = render(<Services />);
    let mounted = true;

    try {
      await flushFetches();
      dashboardCache.get.mockClear();
      dashboardCache.invalidate.mockClear();

      await act(async () => {
        jest.advanceTimersByTime(60000 - 1);
      });
      expect(dashboardCache.get).not.toHaveBeenCalled();

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });

      expect(dashboardCache.invalidate.mock.calls).toEqual([
        [getServices, [{ summaryOnly: true, includeEndpoints: true }]],
        [getServices, [{ metadataOnly: true }]],
      ]);
      await flushFetches();
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(1);
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      unmount();
      mounted = false;
      window.document.dispatchEvent(new Event('visibilitychange'));
      await act(async () => {
        jest.advanceTimersByTime(30000);
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    } finally {
      if (mounted) {
        unmount();
      }
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('fences a pre-hide request when visibility restore starts a fresh read', async () => {
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const oldRequest = deferred();
    const visibleRequest = deferred();
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(visibleRequest.promise)
      .mockResolvedValue(SERVICES_RESPONSE);
    setDocumentVisibility('hidden');
    const { unmount } = render(<Services />);

    try {
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        oldRequest.resolve(responseFor('stale-service'));
        await oldRequest.promise;
      });
      expect(screen.queryByText('stale-service')).not.toBeInTheDocument();

      await act(async () => {
        visibleRequest.resolve(responseFor('visible-service'));
        await visibleRequest.promise;
      });
      expect(screen.getByText('visible-service')).toBeInTheDocument();
      await flushFetches();
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
    } finally {
      unmount();
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('reuses a manual refresh when the page becomes visible', async () => {
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const initialRequest = deferred();
    const manualRequest = deferred();
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockReturnValueOnce(initialRequest.promise)
      .mockReturnValueOnce(manualRequest.promise)
      .mockResolvedValue(SERVICES_RESPONSE);
    setDocumentVisibility('hidden');
    const { unmount } = render(
      <StatefulServicesTable refreshDataRef={refreshDataRef} />
    );

    try {
      await act(async () => {
        refreshDataRef.current();
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);
      dashboardCache.invalidate.mockClear();

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });

      expect(dashboardCache.invalidate).not.toHaveBeenCalled();
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        manualRequest.resolve(responseFor('manual-service'));
        await manualRequest.promise;
      });
      expect(screen.getByText('manual-service')).toBeInTheDocument();
    } finally {
      unmount();
      initialRequest.resolve(SERVICES_RESPONSE);
      await initialRequest.promise;
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
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
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('lets a manual refresh supersede an older request', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise)
      .mockResolvedValue(SERVICES_RESPONSE);

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    await act(async () => {
      refreshDataRef.current();
    });
    await flushFetches();
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
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('does not let an older failure erase a newer manual refresh', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    const refreshDataRef = { current: null };
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise)
      .mockResolvedValue(SERVICES_RESPONSE);

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
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
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

describe('service operational semantics', () => {
  it('treats retained terminal attempts as history, not a live failure', () => {
    const service = {
      status: 'READY',
      replicasReady: 1,
      targetReplicas: 1,
      replicaStatusCounts: {
        READY: 1,
        FAILED_PROVISION: 199,
        FAILED_INITIAL_DELAY: 64,
        FAILED_PROBING: 20,
        FAILED: 38,
      },
      replicas: [],
    };

    expect(getPastAttemptCount(service)).toBe(321);
    expect(getServiceOperationalState(service)).toMatchObject({
      label: 'Healthy',
      tone: 'success',
      detail:
        '1/1 target replicas are ready. 321 past attempts were replaced automatically. No action is required.',
    });
  });

  it('reserves needs-attention wording for actionable states', () => {
    expect(
      getServiceOperationalState({
        status: 'FAILED_CLEANUP',
        replicasReady: null,
        targetReplicas: null,
        replicaStatusCounts: null,
        replicas: [],
        metadataOnly: true,
      }).label
    ).toBe('Cleanup needs verification');
    expect(
      getServiceOperationalState({
        status: 'READY',
        replicasReady: 1,
        targetReplicas: 2,
        replicaStatusCounts: { READY: 1, PROVISIONING: 1 },
        replicas: [],
      }).label
    ).toBe('Scaling automatically');
    expect(
      getServiceOperationalState({
        status: 'CONTROLLER_FAILED',
        replicasReady: 0,
        targetReplicas: 1,
        replicaStatusCounts: {},
        replicas: [],
      }).label
    ).toBe('Needs attention');
    expect(
      getServiceOperationalState({
        status: 'READY',
        replicasReady: 1,
        targetReplicas: 1,
        replicaStatusCounts: { READY: 1, FAILED_CLEANUP: 1 },
        replicas: [],
      }).label
    ).toBe('Cleanup needs verification');
    expect(
      getServiceOperationalState({
        status: 'READY',
        replicasReady: 1,
        targetReplicas: 1,
        replicaStatusCounts: { READY: 1, UNKNOWN: 1 },
        replicas: [],
      }).label
    ).toBe('Replica state needs verification');
  });
});
