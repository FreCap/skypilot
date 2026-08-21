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
import {
  getServiceReplicaSummaries,
  getServices,
} from '@/data/connectors/services';

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

function persistedResponseFor(name) {
  return {
    available: true,
    serviceMetadataIncluded: true,
    summaries: [
      {
        ...SERVICES_RESPONSE.services[0],
        name,
        serviceHash: `hash-${name}`,
        persistedMetadataLoaded: true,
        pastAttemptCount: 0,
      },
    ],
  };
}

const EMPTY_REPLICA_SUMMARIES = { available: true, summaries: [] };

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

function StatefulServicesTable({ refreshDataRef, onFetched }) {
  const [loading, setLoading] = React.useState(false);
  return (
    <ServicesTable
      refreshInterval={30000}
      loading={loading}
      setLoading={setLoading}
      refreshDataRef={refreshDataRef}
      onFetched={onFetched}
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

  it('fetches one metadata, live summary, and persisted replica phase on mount', async () => {
    render(<Services />);

    await flushFetches();

    // The fetch updates loading state and the last-fetched timestamp in
    // the parent; those rerenders must NOT retrigger the fetch effect.
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
    expect(dashboardCache.get.mock.calls).toEqual([
      [getServices, [{ metadataOnly: true }]],
      [getServices, [{ summaryOnly: true, includeEndpoints: true }]],
      [getServiceReplicaSummaries, [{}]],
    ]);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(
      getServiceReplicaSummaries,
      [{}]
    );
  });

  it('keeps legacy first paint pending until controller metadata settles', async () => {
    const metadata = deferred();
    const summary = deferred();
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockReturnValueOnce(metadata.promise)
      .mockReturnValueOnce(summary.promise)
      .mockResolvedValueOnce({
        available: false,
        reason: 'unsupported',
        legacyFallback: true,
        summaries: [],
      });

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    await flushFetches();

    expect(screen.getByText('Loading...')).toBeInTheDocument();
    expect(screen.queryByText('No services found.')).not.toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      refreshDataRef.current();
      await Promise.resolve();
    });
    // The old-server controller response remains the one compatibility owner;
    // refreshing cannot fence it or accumulate another transport request.
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      metadata.resolve(SERVICES_RESPONSE);
      summary.resolve(SERVICES_RESPONSE);
      await Promise.all([metadata.promise, summary.promise]);
    });
    await flushFetches();

    expect(screen.getByText('boltz-l4-fleet')).toBeInTheDocument();
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
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

  it('renders a summary that arrives before metadata without losing enrichment', async () => {
    const metadata = deferred();
    dashboardCache.get
      .mockReturnValueOnce(metadata.promise)
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            status: 'READY',
            summaryOnly: true,
            metadataOnly: false,
            targetReplicas: 1,
            replicaStatusCounts: { READY: 1 },
          },
        ],
        controllerStopped: false,
      });

    render(<Services />);
    await flushFetches();

    expect(screen.getByText('boltz-l4-fleet')).toBeInTheDocument();
    expect(screen.getByText('Healthy')).toBeInTheDocument();
    expect(screen.getByText('http://10.0.0.1:30001')).toBeInTheDocument();

    await act(async () => {
      metadata.resolve({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            status: 'READY',
            endpoint: null,
            metadataOnly: true,
            replicasReady: null,
            replicasTotal: null,
            replicasFailed: null,
            replicaStatusCounts: null,
          },
        ],
        controllerStopped: false,
      });
      await metadata.promise;
    });

    expect(screen.getByText('Healthy')).toBeInTheDocument();
    expect(screen.getByText('http://10.0.0.1:30001')).toBeInTheDocument();
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
  });

  it('paints direct persisted metadata before controller transport lands', async () => {
    const metadata = deferred();
    const liveSummary = deferred();
    const replicaSummary = deferred();
    dashboardCache.get
      .mockReturnValueOnce(metadata.promise)
      .mockReturnValueOnce(liveSummary.promise)
      .mockReturnValueOnce(replicaSummary.promise);

    render(<Services />);
    await act(async () => {
      replicaSummary.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [
          {
            name: 'boltz-l4-fleet',
            serviceHash: 'hash-a',
            persistedMetadataLoaded: true,
            status: 'READY',
            uptime: 1751600000,
            policy: 'autoscaling(min=1,max=4)',
            requestedResources: '1x[L4:1]',
            replicasReady: 9,
            replicasTotal: 9,
            pastAttemptCount: 99,
          },
        ],
      });
      await replicaSummary.promise;
    });

    expect(screen.getByText('boltz-l4-fleet')).toBeInTheDocument();
    expect(screen.getByText('Serving')).toBeInTheDocument();
    expect(screen.getByText('9/9')).toBeInTheDocument();
    expect(screen.getByText('99 past attempts')).toBeInTheDocument();
    expect(screen.getAllByText('Loading...')).toHaveLength(1);

    await act(async () => {
      metadata.resolve({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            metadataOnly: true,
            replicasReady: null,
            replicasTotal: null,
          },
        ],
      });
      await metadata.promise;
    });

    expect(screen.getByText('boltz-l4-fleet')).toBeInTheDocument();
    expect(screen.getByText('9/9')).toBeInTheDocument();
    expect(screen.getByText('99 past attempts')).toBeInTheDocument();

    await act(async () => {
      liveSummary.resolve({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            summaryOnly: true,
          },
        ],
      });
      await liveSummary.promise;
    });
  });

  it('lets an authoritative direct incarnation replace stale controller identity', async () => {
    const liveSummary = deferred();
    const replicaSummary = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            metadataOnly: true,
          },
        ],
      })
      .mockReturnValueOnce(liveSummary.promise)
      .mockReturnValueOnce(replicaSummary.promise);

    render(<Services />);
    await flushFetches();
    await act(async () => {
      replicaSummary.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [
          {
            name: 'boltz-l4-fleet',
            serviceHash: 'hash-b',
            persistedMetadataLoaded: true,
            status: 'READY',
            replicasReady: 9,
            replicasTotal: 9,
            pastAttemptCount: 90,
          },
        ],
      });
      await replicaSummary.promise;
    });

    expect(screen.getByText('9/9')).toBeInTheDocument();
    expect(screen.getByText('90 past attempts')).toBeInTheDocument();

    await act(async () => {
      liveSummary.resolve({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            summaryOnly: true,
          },
        ],
      });
      await liveSummary.promise;
    });

    expect(screen.getByText('9/9')).toBeInTheDocument();
    expect(screen.queryByText('1/1')).not.toBeInTheDocument();
  });

  it('does not attach hashless controller enrichment to modern identity', async () => {
    const liveSummary = deferred();
    const replicaSummary = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: null,
            endpoint: 'https://stale-hashless.example.test',
            metadataOnly: true,
          },
        ],
      })
      .mockReturnValueOnce(liveSummary.promise)
      .mockReturnValueOnce(replicaSummary.promise);

    render(<Services />);
    await act(async () => {
      replicaSummary.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [
          {
            name: 'boltz-l4-fleet',
            serviceHash: 'hash-current',
            persistedMetadataLoaded: true,
            status: 'READY',
            replicasReady: 9,
            replicasTotal: 9,
            pastAttemptCount: 0,
          },
        ],
      });
      await replicaSummary.promise;
    });

    expect(screen.getByText('9/9')).toBeInTheDocument();
    expect(
      screen.queryByText('https://stale-hashless.example.test')
    ).not.toBeInTheDocument();

    await act(async () => {
      liveSummary.resolve({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: null,
            endpoint: 'https://late-hashless.example.test',
            summaryOnly: true,
          },
        ],
      });
      await liveSummary.promise;
    });
    expect(screen.getByText('9/9')).toBeInTheDocument();
    expect(
      screen.queryByText('https://late-hashless.example.test')
    ).not.toBeInTheDocument();
  });

  it('does not resurrect a service absent from authoritative persisted identity', async () => {
    const liveSummary = deferred();
    dashboardCache.get
      .mockResolvedValueOnce(responseFor('stale-service'))
      .mockReturnValueOnce(liveSummary.promise)
      .mockResolvedValueOnce({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [],
      });

    render(<Services />);
    await flushFetches();
    expect(screen.queryByText('stale-service')).not.toBeInTheDocument();
    expect(
      screen.getByText('No services found. Launch one with `sky serve up`.')
    ).toBeInTheDocument();

    await act(async () => {
      liveSummary.resolve(responseFor('stale-service'));
      await liveSummary.promise;
    });

    expect(screen.queryByText('stale-service')).not.toBeInTheDocument();
    expect(
      screen.getByText('No services found. Launch one with `sky serve up`.')
    ).toBeInTheDocument();
  });

  it('rejects a mismatched direct summary after metadata arrives', async () => {
    const replicaSummary = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            metadataOnly: true,
            replicasReady: null,
            replicasTotal: null,
          },
        ],
      })
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            summaryOnly: true,
          },
        ],
      })
      .mockReturnValueOnce(replicaSummary.promise);

    render(<Services />);
    await flushFetches();
    await act(async () => {
      replicaSummary.resolve({
        available: true,
        summaries: [
          {
            name: 'boltz-l4-fleet',
            serviceHash: 'hash-b',
            replicasReady: 7,
            replicasTotal: 7,
            pastAttemptCount: 70,
          },
        ],
      });
      await replicaSummary.promise;
    });

    expect(screen.getByText('1/1')).toBeInTheDocument();
    expect(screen.queryByText('7/7')).not.toBeInTheDocument();
    expect(screen.queryByText('70 past attempts')).not.toBeInTheDocument();
  });

  it('lets a recreated controller identity discard prior direct fields', async () => {
    const liveSummary = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            metadataOnly: true,
          },
        ],
      })
      .mockReturnValueOnce(liveSummary.promise)
      .mockResolvedValueOnce({
        available: true,
        summaries: [
          {
            name: 'boltz-l4-fleet',
            serviceHash: 'hash-a',
            replicasReady: 8,
            replicasTotal: 8,
            pastAttemptCount: 80,
          },
        ],
      });
    render(<Services />);
    await flushFetches();
    expect(screen.getByText('80 past attempts')).toBeInTheDocument();

    await act(async () => {
      liveSummary.resolve({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-b',
            summaryOnly: true,
          },
        ],
      });
      await liveSummary.promise;
    });

    expect(screen.queryByText('80 past attempts')).not.toBeInTheDocument();
    expect(screen.getByText('1/1')).toBeInTheDocument();
  });

  it('drops direct-only fields when topology falls back', async () => {
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            metadataOnly: true,
          },
        ],
      })
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            summaryOnly: true,
            replicaStatusCounts: { READY: 1 },
          },
        ],
      })
      .mockResolvedValueOnce({
        available: true,
        summaries: [
          {
            name: 'boltz-l4-fleet',
            serviceHash: 'hash-a',
            replicasReady: 1,
            replicasTotal: 1,
            pastAttemptCount: 12,
          },
        ],
      });
    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    await flushFetches();
    expect(screen.getByText('12 past attempts')).toBeInTheDocument();

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            metadataOnly: true,
          },
        ],
      })
      .mockResolvedValueOnce({
        services: [
          {
            ...SERVICES_RESPONSE.services[0],
            serviceHash: 'hash-a',
            summaryOnly: true,
            replicaStatusCounts: { READY: 1 },
          },
        ],
      })
      .mockResolvedValueOnce({
        available: false,
        reason: 'non_consolidated',
        legacyFallback: true,
        summaries: [],
      });
    await act(async () => refreshDataRef.current());

    expect(screen.queryByText('12 past attempts')).not.toBeInTheDocument();
    expect(screen.getByText('1/1')).toBeInTheDocument();
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
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    // Just under the 30s interval: no new fetch (the 10s ticks of the
    // last-updated timestamp rerender the parent along the way).
    await act(async () => {
      jest.advanceTimersByTime(29000);
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    // Crossing the interval triggers exactly one more fetch.
    await act(async () => {
      jest.advanceTimersByTime(2000);
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(6);
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
        [getServiceReplicaSummaries, [{}]],
        [getServiceReplicaSummaries, [{}]],
      ]);
      await flushFetches();
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      await act(async () => {
        jest.advanceTimersByTime(1);
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      unmount();
      mounted = false;
      window.document.dispatchEvent(new Event('visibilitychange'));
      await act(async () => {
        jest.advanceTimersByTime(30000);
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
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
    const oldMetadata = deferred();
    const oldSummary = deferred();
    const oldReplicaSummary = deferred();
    const visibleReplicaSummary = deferred();
    dashboardCache.get
      .mockReturnValueOnce(oldMetadata.promise)
      .mockReturnValueOnce(oldSummary.promise)
      .mockReturnValueOnce(oldReplicaSummary.promise)
      .mockReturnValueOnce(visibleReplicaSummary.promise);
    setDocumentVisibility('hidden');
    const { unmount } = render(<Services />);

    try {
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
      await act(async () => {
        oldReplicaSummary.resolve(persistedResponseFor('initial-service'));
        await oldReplicaSummary.promise;
      });

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      // Controller enrichment keeps its own singleflight. Visibility fences
      // the old response and starts a fresh persisted read without piling up
      // a second controller request.
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);

      await act(async () => {
        oldMetadata.resolve(responseFor('stale-service'));
        oldSummary.resolve(responseFor('stale-service'));
        await Promise.all([
          oldMetadata.promise,
          oldSummary.promise,
          oldReplicaSummary.promise,
        ]);
      });
      expect(screen.queryByText('stale-service')).not.toBeInTheDocument();

      await act(async () => {
        visibleReplicaSummary.resolve(persistedResponseFor('visible-service'));
        await visibleReplicaSummary.promise;
      });
      expect(screen.getByText('visible-service')).toBeInTheDocument();
      await flushFetches();
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
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
    const initialMetadata = deferred();
    const initialSummary = deferred();
    const initialReplicaSummary = deferred();
    const manualReplicaSummary = deferred();
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockReturnValueOnce(initialMetadata.promise)
      .mockReturnValueOnce(initialSummary.promise)
      .mockReturnValueOnce(initialReplicaSummary.promise)
      .mockReturnValueOnce(manualReplicaSummary.promise);
    setDocumentVisibility('hidden');
    const { unmount } = render(
      <StatefulServicesTable refreshDataRef={refreshDataRef} />
    );

    try {
      await act(async () => {
        initialReplicaSummary.resolve(persistedResponseFor('initial-service'));
        await initialReplicaSummary.promise;
      });
      await act(async () => {
        refreshDataRef.current();
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
      dashboardCache.invalidate.mockClear();

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });

      expect(dashboardCache.invalidate).not.toHaveBeenCalled();
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);

      await act(async () => {
        manualReplicaSummary.resolve(persistedResponseFor('manual-service'));
        await manualReplicaSummary.promise;
      });
      expect(screen.getByText('manual-service')).toBeInTheDocument();
    } finally {
      unmount();
      initialMetadata.resolve(SERVICES_RESPONSE);
      initialSummary.resolve(SERVICES_RESPONSE);
      await Promise.all([
        initialMetadata.promise,
        initialSummary.promise,
        initialReplicaSummary.promise,
      ]);
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
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      jest.advanceTimersByTime(90000);
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      pendingRequest.resolve(SERVICES_RESPONSE);
      await pendingRequest.promise;
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
  });

  it('keeps manual persisted refresh live while controller enrichment is stalled', async () => {
    const oldMetadata = deferred();
    const oldSummary = deferred();
    const oldReplicaSummary = deferred();
    const currentReplicaSummary = deferred();
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockReturnValueOnce(oldMetadata.promise)
      .mockReturnValueOnce(oldSummary.promise)
      .mockReturnValueOnce(oldReplicaSummary.promise)
      .mockReturnValueOnce(currentReplicaSummary.promise);

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      oldReplicaSummary.resolve(persistedResponseFor('initial-service'));
      await oldReplicaSummary.promise;
    });
    await act(async () => {
      refreshDataRef.current();
    });
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);

    await act(async () => {
      oldMetadata.resolve(responseFor('stale-service'));
      oldSummary.resolve(responseFor('stale-service'));
      await Promise.all([
        oldMetadata.promise,
        oldSummary.promise,
        oldReplicaSummary.promise,
      ]);
    });

    expect(screen.queryByText('stale-service')).not.toBeInTheDocument();
    expect(screen.getAllByText('Loading...').length).toBeGreaterThan(0);

    await act(async () => {
      currentReplicaSummary.resolve(persistedResponseFor('current-service'));
      await currentReplicaSummary.promise;
    });

    expect(screen.getByText('current-service')).toBeInTheDocument();
    expect(screen.getAllByText('Loading...')).toHaveLength(1);
    expect(screen.getByLabelText('Endpoint')).toHaveTextContent('Loading...');
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
  });

  it('keeps proven direct capability live across a transient summary failure', async () => {
    const controllerPending = deferred();
    const refreshDataRef = { current: null };
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockReturnValueOnce(controllerPending.promise)
      .mockReturnValueOnce(controllerPending.promise)
      .mockResolvedValueOnce(persistedResponseFor('initial-service'));

    const { unmount } = render(
      <StatefulServicesTable refreshDataRef={refreshDataRef} />
    );
    let mounted = true;
    try {
      expect(await screen.findByText('initial-service')).toBeInTheDocument();

      dashboardCache.get.mockRejectedValueOnce(
        new Error('transient direct failure')
      );
      await act(async () => {
        await refreshDataRef.current();
      });
      expect(screen.getByText('initial-service')).toBeInTheDocument();

      dashboardCache.get.mockResolvedValueOnce(
        persistedResponseFor('recovered-service')
      );
      await act(async () => {
        await refreshDataRef.current();
      });

      expect(screen.getByText('recovered-service')).toBeInTheDocument();
      expect(screen.queryByText('initial-service')).not.toBeInTheDocument();
      expect(
        dashboardCache.get.mock.calls.filter(
          ([connector]) => connector === getServices
        )
      ).toHaveLength(2);

      unmount();
      mounted = false;
    } finally {
      if (mounted) unmount();
      controllerPending.resolve(SERVICES_RESPONSE);
      consoleError.mockRestore();
    }
  });

  it('does not present prior controller enrichment as freshly refreshed', async () => {
    const nextMetadata = deferred();
    const nextLiveSummary = deferred();
    const refreshDataRef = { current: null };
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    const currentControllerResponse = {
      ...SERVICES_RESPONSE,
      services: [
        {
          ...SERVICES_RESPONSE.services[0],
          serviceHash: 'hash-boltz-l4-fleet',
        },
      ],
    };
    dashboardCache.get
      .mockResolvedValueOnce(currentControllerResponse)
      .mockResolvedValueOnce(currentControllerResponse)
      .mockResolvedValueOnce(persistedResponseFor('boltz-l4-fleet'))
      .mockReturnValueOnce(nextMetadata.promise)
      .mockReturnValueOnce(nextLiveSummary.promise)
      .mockResolvedValueOnce(persistedResponseFor('boltz-l4-fleet'));

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    expect(
      await screen.findByText('http://10.0.0.1:30001')
    ).toBeInTheDocument();

    await act(async () => refreshDataRef.current());

    expect(screen.queryByText('http://10.0.0.1:30001')).not.toBeInTheDocument();
    expect(screen.getByLabelText('Endpoint')).toHaveTextContent('Loading...');
    expect(screen.getByText('1/1')).toBeInTheDocument();

    await act(async () => {
      nextMetadata.reject(new Error('controller unavailable'));
      nextLiveSummary.reject(new Error('controller unavailable'));
      await Promise.allSettled([nextMetadata.promise, nextLiveSummary.promise]);
    });

    expect(screen.getByLabelText('Endpoint')).toHaveTextContent('Unavailable');
    expect(screen.getByText('1/1')).toBeInTheDocument();
    consoleError.mockRestore();
  });

  it('does not let a late controller response relabel persisted freshness', async () => {
    const controllerMetadata = deferred();
    const controllerSummary = deferred();
    const refreshDataRef = { current: null };
    const onFetched = jest.fn();
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockReturnValueOnce(controllerMetadata.promise)
      .mockReturnValueOnce(controllerSummary.promise)
      .mockResolvedValueOnce({
        ...persistedResponseFor('boltz-l4-fleet'),
        observedAt: 1234,
      });

    try {
      render(
        <StatefulServicesTable
          refreshDataRef={refreshDataRef}
          onFetched={onFetched}
        />
      );
      expect(await screen.findByText('boltz-l4-fleet')).toBeInTheDocument();
      expect(onFetched).toHaveBeenCalledTimes(1);
      expect(onFetched).toHaveBeenLastCalledWith(new Date(1234 * 1000));

      dashboardCache.get.mockRejectedValueOnce(
        new Error('persisted refresh failed')
      );
      await act(async () => refreshDataRef.current());
      expect(onFetched).toHaveBeenCalledTimes(1);

      await act(async () => {
        controllerMetadata.resolve(SERVICES_RESPONSE);
        controllerSummary.resolve(SERVICES_RESPONSE);
        await Promise.all([
          controllerMetadata.promise,
          controllerSummary.promise,
        ]);
      });
      expect(onFetched).toHaveBeenCalledTimes(1);
      expect(onFetched).toHaveBeenLastCalledWith(new Date(1234 * 1000));
    } finally {
      consoleError.mockRestore();
    }
  });

  it('starts one compatibility successor after a rolling legacy response', async () => {
    const oldMetadata = deferred();
    const oldSummary = deferred();
    const refreshDataRef = { current: null };
    dashboardCache.get
      .mockReturnValueOnce(oldMetadata.promise)
      .mockReturnValueOnce(oldSummary.promise)
      .mockResolvedValueOnce(persistedResponseFor('modern-service'))
      .mockResolvedValueOnce({
        available: false,
        reason: 'unsupported',
        legacyFallback: true,
        summaries: [],
      })
      .mockResolvedValueOnce(responseFor('legacy-service'))
      .mockResolvedValueOnce(responseFor('legacy-service'));

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    expect(await screen.findByText('modern-service')).toBeInTheDocument();

    let refreshPromise;
    await act(async () => {
      refreshPromise = refreshDataRef.current();
      await Promise.resolve();
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);

    await act(async () => {
      oldMetadata.resolve(responseFor('stale-modern-service'));
      oldSummary.resolve(responseFor('stale-modern-service'));
      await refreshPromise;
    });

    expect(screen.getByText('legacy-service')).toBeInTheDocument();
    expect(screen.queryByText('stale-modern-service')).not.toBeInTheDocument();
    expect(
      dashboardCache.get.mock.calls.filter(
        ([connector]) => connector === getServices
      )
    ).toHaveLength(4);
    expect(dashboardCache.get).toHaveBeenCalledTimes(6);
  });

  it('does not let an older failure erase a newer manual refresh', async () => {
    const oldMetadata = deferred();
    const oldSummary = deferred();
    const oldReplicaSummary = deferred();
    const currentReplicaSummary = deferred();
    const refreshDataRef = { current: null };
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockReturnValueOnce(oldMetadata.promise)
      .mockReturnValueOnce(oldSummary.promise)
      .mockReturnValueOnce(oldReplicaSummary.promise)
      .mockReturnValueOnce(currentReplicaSummary.promise);

    render(<StatefulServicesTable refreshDataRef={refreshDataRef} />);
    await act(async () => {
      oldReplicaSummary.resolve(persistedResponseFor('initial-service'));
      await oldReplicaSummary.promise;
    });
    await act(async () => {
      refreshDataRef.current();
      currentReplicaSummary.resolve(persistedResponseFor('current-service'));
      await currentReplicaSummary.promise;
    });
    expect(screen.getByText('current-service')).toBeInTheDocument();

    await act(async () => {
      oldMetadata.reject(new Error('stale failure'));
      oldSummary.reject(new Error('stale failure'));
      await Promise.allSettled([
        oldMetadata.promise,
        oldSummary.promise,
        oldReplicaSummary.promise,
      ]);
    });

    expect(screen.getByText('current-service')).toBeInTheDocument();
    expect(consoleError).not.toHaveBeenCalled();
    await flushFetches();
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
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
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);
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

  it('does not call stale logical capacity healthy before routing is ready', () => {
    const state = getServiceOperationalState({
      status: 'REPLICA_INIT',
      replicaUnit: 'logical',
      replicasReady: 279,
      replicasTotal: 288,
      targetReplicas: 0,
      replicaStatusCounts: { READY: 279, PROVISIONING: 9 },
      replicas: [],
    });

    expect(state).toMatchObject({
      label: 'Routing unverified',
      tone: 'warning',
    });
    expect(state.detail).toContain(
      'Do not treat this snapshot as verified routable capacity.'
    );
  });
});
