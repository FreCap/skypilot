import {
  act,
  render,
  renderHook,
  screen,
  within,
  waitFor,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { formatFullTimestamp } from '@/components/utils';

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

jest.mock('@/data/connectors/services', () => {
  const actual = jest.requireActual('@/data/connectors/services');
  return {
    ...actual,
    getServiceHistory: jest.fn(),
    getServiceReplicaSummaries: jest.fn(),
    getServiceReplicas: jest.fn(),
  };
});

const mockUseRouter = jest.fn();

jest.mock('next/router', () => ({
  useRouter: () => mockUseRouter(),
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

jest.mock('@/components/serve-history', () => ({
  ServeHistorySection: () => <div data-testid="serve-history-section" />,
}));

jest.mock('@/components/service-version-history', () => ({
  ServiceVersionHistory: () => <div data-testid="service-version-history" />,
}));

jest.mock('@/components/service-placement', () => ({
  ServicePlacement: () => <div data-testid="service-placement" />,
}));

import dashboardCache from '@/lib/cache';
import {
  getServiceHistory,
  getServiceReplicaSummaries,
  getServiceReplicas,
  getServices,
} from '@/data/connectors/services';
import ServiceDetailsPage, {
  AcceleratorCapacityCard,
  getReplicaPlacementBreakdown,
  ReplicaPlacementCard,
  ReplicasCard,
  ServiceDetailCard,
  sortReplicas,
  useServiceDetails,
  useServiceHistory,
  useServiceReplicaData,
} from '@/pages/services/[service]';

describe('AcceleratorCapacityCard', () => {
  it('renders exact cards and separates demand floors from reserved supply', () => {
    render(
      <AcceleratorCapacityCard
        serviceData={{
          fillTarget: 4,
          freeReservedSlots: 2,
          acceleratorCapacity: [
            {
              card: 'A100',
              ready: 1,
              provisioning: 2,
              total: 3,
              demandTarget: 3,
              warmRetentionTarget: 2,
              coldLaunchAuthority: 0,
              hardFloor: 1,
              zeroCostReady: 1,
              fillTarget: null,
              freeReserved: null,
            },
            {
              card: 'A100-80GB',
              ready: 2,
              provisioning: 0,
              total: 2,
              demandTarget: 2,
              warmRetentionTarget: 1,
              coldLaunchAuthority: 1,
              hardFloor: 2,
              zeroCostReady: 0,
              fillTarget: null,
              freeReserved: null,
            },
          ],
        }}
      />
    );

    expect(screen.getByText('A100')).toBeInTheDocument();
    expect(screen.getByText('A100-80GB')).toBeInTheDocument();
    expect(screen.getByText('Aggregate fill target: 4')).toBeInTheDocument();
    expect(
      screen.getByText('Aggregate free reserved slots: 2')
    ).toBeInTheDocument();
    expect(screen.getByText('Committed / unready')).toBeInTheDocument();
    expect(screen.queryByText('Provisioning')).not.toBeInTheDocument();
  });
});

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function detailSummaryArgs(serviceName) {
  return [
    {
      serviceNames: [serviceName],
      metadataOnly: true,
    },
  ];
}

function detailFullArgs(serviceName) {
  return [
    {
      serviceNames: [serviceName],
      includeTargetReplicas: true,
    },
  ];
}

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
}

describe('useServiceDetails stale-response fencing', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    getServiceHistory.mockResolvedValue({
      available: true,
      serviceHash: 'hash-a',
      bucketSeconds: 60,
      windowStart: 0,
      windowEnd: 3600,
      samples: [],
      requestSamples: [],
      predictionTimeSamples: [],
      autoscalerSamples: [],
    });
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('skips the full service read when only summary data is requested', async () => {
    const unexpectedFull = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'summary-only', summaryOnly: true }],
      })
      .mockImplementationOnce(() => unexpectedFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc', loadFull: false })
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('summary-only')
    );

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(
      1,
      getServices,
      detailSummaryArgs('svc')
    );
    expect(result.current.replicasLoading).toBe(false);
  });

  it('keeps summary-only manual and polling refreshes off the full read', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'manual-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'poll-summary', summaryOnly: true }],
      });

    const { result, unmount } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc', loadFull: false })
    );
    let mounted = true;

    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(result.current.serviceData.status).toBe('initial-summary');
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      await act(async () => {
        await result.current.refreshData();
      });
      expect(result.current.serviceData.status).toBe('manual-summary');
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);
      expect(dashboardCache.invalidate.mock.calls).toEqual([
        [getServices, detailSummaryArgs('svc')],
      ]);

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      expect(result.current.serviceData.status).toBe('poll-summary');
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
      expect(dashboardCache.invalidate.mock.calls).toEqual([
        [getServices, detailSummaryArgs('svc')],
        [getServices, detailSummaryArgs('svc')],
      ]);

      unmount();
      mounted = false;
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
      jest.useRealTimers();
    }
  });

  it('coalesces concurrent forced refreshes for the same service', async () => {
    const refreshedSummary = deferred();
    const refreshedFull = deferred();
    const duplicateSummary = deferred();
    const duplicateFull = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'initial-full', replicas: [] }],
      });

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('initial-full')
    );

    dashboardCache.get
      .mockImplementationOnce(() => refreshedSummary.promise)
      .mockImplementationOnce(() => refreshedFull.promise)
      .mockImplementationOnce(() => duplicateSummary.promise)
      .mockImplementationOnce(() => duplicateFull.promise);

    let firstRefresh;
    let secondRefresh;
    act(() => {
      firstRefresh = result.current.refreshData();
      secondRefresh = result.current.refreshData();
    });

    expect(secondRefresh).toBe(firstRefresh);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
    expect(dashboardCache.invalidate.mock.calls).toEqual([
      [getServices, detailSummaryArgs('svc')],
      [getServices, detailFullArgs('svc')],
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      refreshedSummary.resolve({
        services: [{ name: 'svc', status: 'fresh-summary', summaryOnly: true }],
      });
      refreshedFull.resolve({
        services: [{ name: 'svc', status: 'fresh-full', replicas: ['new'] }],
      });
      duplicateSummary.resolve({
        services: [
          { name: 'svc', status: 'duplicate-summary', summaryOnly: true },
        ],
      });
      duplicateFull.resolve({
        services: [
          { name: 'svc', status: 'duplicate-full', replicas: ['old'] },
        ],
      });
      await Promise.all([firstRefresh, secondRefresh]);
    });

    expect(result.current.serviceData.status).toBe('fresh-full');
    expect(result.current.serviceData.replicas).toEqual(['new']);
  });

  it('releases refresh ownership after both service reads fail', async () => {
    const failedSummary = deferred();
    const failedFull = deferred();
    jest.spyOn(console, 'error').mockImplementation(() => {});
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'initial-full', replicas: [] }],
      });

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('initial-full')
    );

    dashboardCache.get
      .mockImplementationOnce(() => failedSummary.promise)
      .mockImplementationOnce(() => failedFull.promise);
    let failedRefresh;
    let duplicateRefresh;
    act(() => {
      failedRefresh = result.current.refreshData();
      duplicateRefresh = result.current.refreshData();
    });

    expect(duplicateRefresh).toBe(failedRefresh);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);

    await act(async () => {
      failedSummary.reject(new Error('summary unavailable'));
      failedFull.reject(new Error('replicas unavailable'));
      await Promise.all([failedRefresh, duplicateRefresh]);
    });

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'recovered-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'recovered-full', replicas: [] }],
      });

    let recoveredRefresh;
    act(() => {
      recoveredRefresh = result.current.refreshData();
    });

    expect(recoveredRefresh).not.toBe(failedRefresh);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(4);
    expect(dashboardCache.get).toHaveBeenCalledTimes(5);

    await act(async () => {
      await recoveredRefresh;
    });
    expect(result.current.serviceData.status).toBe('recovered-full');
  });

  it('coalesces a manual refresh with an in-flight initial load', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshData();
    });

    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      });
      await Promise.resolve();
    });
    expect(result.current.serviceData.status).toBe('initial-summary');
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialFull.resolve({
        services: [{ name: 'svc', status: 'initial-full', replicas: ['r1'] }],
      });
      await refreshPromise;
    });

    expect(result.current.serviceData.status).toBe('initial-full');
    expect(result.current.serviceData.replicas).toEqual(['r1']);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('unblocks the initial load as soon as metadata lands', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', metadataOnly: true },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-summary');
    expect(result.current.loading).toBe(false);
    expect(result.current.replicasLoading).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialFull.resolve({
        services: [
          {
            name: 'svc',
            status: 'initial-full',
            replicas: ['r1'],
            replicaHistory: { currentReadyReplicas: 1 },
          },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-full');
    expect(result.current.serviceData.replicas).toEqual(['r1']);
  });

  it('keeps metadata visible after an empty full-detail response', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', metadataOnly: true },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-summary');
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialFull.resolve({ services: [] });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-summary');
    expect(result.current.serviceData.enrichmentUnavailable).toBe(true);
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('marks deferred details unavailable when the full read fails', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();
    const consoleError = jest.spyOn(console, 'error').mockImplementation();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    await act(async () => {
      initialSummary.resolve({
        services: [{ name: 'svc', status: 'READY', metadataOnly: true }],
      });
      await Promise.resolve();
    });

    await act(async () => {
      initialFull.reject(new Error('replicas unavailable'));
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('READY');
    expect(result.current.serviceData.enrichmentUnavailable).toBe(true);
    expect(result.current.replicasLoading).toBe(false);
    consoleError.mockRestore();
  });

  it('renders full detail when the metadata request fails', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    jest.spyOn(console, 'error').mockImplementation(() => {});
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    await act(async () => {
      initialSummary.reject(new Error('metadata unavailable'));
      await Promise.resolve();
    });

    expect(result.current.serviceData).toBe(null);
    expect(result.current.loading).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialFull.resolve({
        services: [{ name: 'svc', status: 'initial-full', replicas: ['r1'] }],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-full');
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('keeps the initial load fenced while summary fails and full is pending', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();
    jest.spyOn(console, 'error').mockImplementation(() => {});

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    await act(async () => {
      initialSummary.reject(new Error('summary unavailable'));
      await Promise.resolve();
    });

    expect(result.current.loading).toBe(true);
    expect(result.current.replicasLoading).toBe(true);
    expect(result.current.serviceData).toBe(null);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialFull.resolve({
        services: [{ name: 'svc', status: 'initial-full', replicas: ['r1'] }],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-full');
    expect(result.current.serviceData.replicas).toEqual(['r1']);
    expect(result.current.loading).toBe(false);
    expect(result.current.replicasLoading).toBe(false);
  });

  it('lets a manual refresh supersede a pending full-detail read once summary data is visible', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();
    const refreshedSummary = deferred();
    const refreshedFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise)
      .mockImplementationOnce(() => refreshedSummary.promise)
      .mockImplementationOnce(() => refreshedFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      });
      await Promise.resolve();
    });
    expect(result.current.serviceData.status).toBe('initial-summary');
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    let firstRefresh;
    let duplicateRefresh;
    act(() => {
      firstRefresh = result.current.refreshData();
      duplicateRefresh = result.current.refreshData();
    });

    expect(duplicateRefresh).toBe(firstRefresh);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.invalidate.mock.calls).toEqual([
      [getServices, detailSummaryArgs('svc')],
      [getServices, detailFullArgs('svc')],
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      refreshedSummary.resolve({
        services: [
          { name: 'svc', status: 'refreshed-summary', summaryOnly: true },
        ],
      });
      refreshedFull.resolve({
        services: [{ name: 'svc', status: 'refreshed-full', replicas: ['r2'] }],
      });
      await firstRefresh;
    });

    expect(result.current.serviceData.status).toBe('refreshed-full');
    expect(result.current.serviceData.replicas).toEqual(['r2']);

    await act(async () => {
      initialFull.resolve({
        services: [
          { name: 'svc', status: 'stale-initial-full', replicas: ['r1'] },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('refreshed-full');
    expect(result.current.serviceData.replicas).toEqual(['r2']);
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
  });

  it('drops stale results from a previous service after the route target changes', async () => {
    const firstSummary = deferred();
    const secondSummary = deferred();
    const secondFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => firstSummary.promise)
      .mockImplementationOnce(() => secondSummary.promise)
      .mockImplementationOnce(() => secondFull.promise);

    const { result, rerender } = renderHook(
      ({ serviceName }) => useServiceDetails({ serviceName }),
      { initialProps: { serviceName: 'svc-a' } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));

    rerender({ serviceName: 'svc-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      secondSummary.resolve({
        services: [
          { name: 'svc-b', status: 'svc-b-summary', metadataOnly: true },
        ],
      });
      secondFull.resolve({
        services: [{ name: 'svc-b', status: 'svc-b-full', replicas: ['b'] }],
      });
      await Promise.all([secondSummary.promise, secondFull.promise]);
    });
    expect(result.current.serviceData.name).toBe('svc-b');

    await act(async () => {
      firstSummary.resolve({
        services: [
          { name: 'svc-a', status: 'svc-a-summary', summaryOnly: true },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.name).toBe('svc-b');
    expect(result.current.serviceData.status).toBe('svc-b-full');
  });

  it('coalesces a manual refresh for a new route while old service data is still visible', async () => {
    const nextSummary = deferred();
    const nextFull = deferred();

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc-a', status: 'svc-a-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc-a', status: 'svc-a-full', replicas: ['a0'] }],
      });

    const { result, rerender } = renderHook(
      ({ serviceName }) => useServiceDetails({ serviceName }),
      { initialProps: { serviceName: 'svc-a' } }
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('svc-a-full')
    );

    dashboardCache.get
      .mockImplementationOnce(() => nextSummary.promise)
      .mockImplementationOnce(() => nextFull.promise);
    rerender({ serviceName: 'svc-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    expect(result.current.serviceData.name).toBe('svc-a');
    expect(result.current.serviceData.status).toBe('svc-a-full');

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshData();
    });

    // The only visible data still belongs to the previous route target, so a
    // manual refresh should reuse the in-flight load for svc-b instead of
    // invalidating caches and starting a duplicate summary/full pair.
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      nextSummary.resolve({
        services: [
          { name: 'svc-b', status: 'svc-b-summary', summaryOnly: true },
        ],
      });
      nextFull.resolve({
        services: [{ name: 'svc-b', status: 'svc-b-full', replicas: ['b1'] }],
      });
      await refreshPromise;
    });

    expect(result.current.serviceData.name).toBe('svc-b');
    expect(result.current.serviceData.status).toBe('svc-b-full');
    expect(result.current.serviceData.replicas).toEqual(['b1']);
  });

  it('retries a failed new-route summary while the full read is pending', async () => {
    const nextSummary = deferred();
    const nextFull = deferred();
    const retriedSummary = deferred();
    const retriedFull = deferred();
    jest.spyOn(console, 'error').mockImplementation(() => {});

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc-a', status: 'svc-a-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc-a', status: 'svc-a-full', replicas: ['a0'] }],
      });

    const { result, rerender } = renderHook(
      ({ serviceName }) => useServiceDetails({ serviceName }),
      { initialProps: { serviceName: 'svc-a' } }
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('svc-a-full')
    );

    dashboardCache.get
      .mockImplementationOnce(() => nextSummary.promise)
      .mockImplementationOnce(() => nextFull.promise)
      .mockImplementationOnce(() => retriedSummary.promise)
      .mockImplementationOnce(() => retriedFull.promise);
    rerender({ serviceName: 'svc-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    await act(async () => {
      nextSummary.reject(new Error('summary unavailable'));
      await Promise.resolve();
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
    expect(result.current.serviceData.name).toBe('svc-a');
    expect(result.current.loading).toBe(true);

    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshData();
    });

    expect(dashboardCache.invalidate.mock.calls).toEqual([
      [getServices, detailSummaryArgs('svc-b')],
      [getServices, detailFullArgs('svc-b')],
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(5);

    await act(async () => {
      retriedSummary.resolve({
        services: [
          { name: 'svc-b', status: 'retried-summary', summaryOnly: true },
        ],
      });
      retriedFull.resolve({
        services: [{ name: 'svc-b', status: 'retried-full', replicas: ['b1'] }],
      });
      await refreshPromise;
    });

    await act(async () => {
      nextFull.resolve({
        services: [
          { name: 'svc-b', status: 'stale-first-full', replicas: ['b0'] },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('retried-full');
    expect(result.current.serviceData.replicas).toEqual(['b1']);
  });

  it('refreshes summary and replicas without overlapping at the polling cadence', async () => {
    jest.useFakeTimers();
    const refreshedSummary = deferred();
    const refreshedFull = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'initial-summary' }],
      })
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-full', replicas: ['old-replica'] },
        ],
      })
      .mockImplementationOnce(() => refreshedSummary.promise)
      .mockImplementationOnce(() => refreshedFull.promise);

    const { result, unmount } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );
    let mounted = true;

    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
      expect(dashboardCache.invalidate.mock.calls).toEqual([
        [getServices, detailSummaryArgs('svc')],
        [getServices, detailFullArgs('svc')],
      ]);

      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000 + 30 * 1000);
        await Promise.resolve();
      });

      // A slow metadata refresh must not accumulate new detail pairs at every
      // timer boundary.
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);

      await act(async () => {
        refreshedSummary.resolve({
          services: [
            {
              name: 'svc',
              status: 'fresh-summary',
              summaryOnly: true,
              replicaHistory: { currentReadyReplicas: 1 },
            },
          ],
        });
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
      expect(result.current.serviceData.status).toBe('fresh-summary');
      expect(result.current.serviceData.replicas).toEqual(['old-replica']);

      await act(async () => {
        refreshedFull.resolve({
          services: [
            { name: 'svc', status: 'fresh-full', replicas: ['new-replica'] },
          ],
        });
        await Promise.resolve();
      });
      expect(result.current.serviceData.status).toBe('fresh-full');
      expect(result.current.serviceData.replicas).toEqual(['new-replica']);

      dashboardCache.get
        .mockResolvedValueOnce({
          services: [{ name: 'svc', status: 'next-summary' }],
        })
        .mockResolvedValueOnce({
          services: [{ name: 'svc', status: 'next-full', replicas: [] }],
        });
      await act(async () => {
        jest.advanceTimersByTime(30 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(6);
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(4);

      unmount();
      mounted = false;
      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(6);
    } finally {
      if (mounted) {
        unmount();
      }
      jest.useRealTimers();
    }
  });

  it('refreshes immediately on visibility restore and skips the adjacent timer boundary', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('hidden');
    dashboardCache.get.mockResolvedValue({
      services: [{ name: 'svc', status: 'READY', replicas: [] }],
    });

    const { unmount } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );
    let mounted = true;

    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000 - 1);
        await Promise.resolve();
      });
      expect(dashboardCache.invalidate).not.toHaveBeenCalled();
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      dashboardCache.get.mockClear();
      dashboardCache.invalidate.mockClear();
      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });

      expect(dashboardCache.invalidate.mock.calls).toEqual([
        [getServices, detailSummaryArgs('svc')],
        [getServices, detailFullArgs('svc')],
      ]);
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(1);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      unmount();
      mounted = false;
      window.document.dispatchEvent(new Event('visibilitychange'));
      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
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
      jest.useRealTimers();
    }
  });

  it('fences a pre-hide poll when visibility restore starts a fresh read', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const pollSummary = deferred();
    const visibleSummary = deferred();
    const visibleFull = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'initial-full', replicas: ['r0'] }],
      })
      .mockImplementationOnce(() => pollSummary.promise)
      .mockImplementationOnce(() => visibleSummary.promise)
      .mockImplementationOnce(() => visibleFull.promise);
    setDocumentVisibility('visible');

    const { result, unmount } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );
    let mounted = true;

    try {
      await waitFor(() =>
        expect(result.current.serviceData.status).toBe('initial-full')
      );
      expect(result.current.serviceData.replicas).toEqual(['r0']);

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      setDocumentVisibility('hidden');
      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);

      await act(async () => {
        pollSummary.resolve({
          services: [
            { name: 'svc', status: 'stale-summary', summaryOnly: true },
          ],
        });
        await pollSummary.promise;
      });
      expect(result.current.serviceData.status).toBe('initial-full');
      expect(result.current.serviceData.replicas).toEqual(['r0']);

      await act(async () => {
        visibleSummary.resolve({
          services: [
            { name: 'svc', status: 'fresh-summary', summaryOnly: true },
          ],
        });
        visibleFull.resolve({
          services: [
            { name: 'svc', status: 'fresh-full', replicas: ['fresh'] },
          ],
        });
        await Promise.all([visibleSummary.promise, visibleFull.promise]);
      });
      expect(result.current.serviceData.status).toBe('fresh-full');
      expect(result.current.serviceData.replicas).toEqual(['fresh']);

      unmount();
      mounted = false;
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
      jest.useRealTimers();
    }
  });

  it('reuses a manual refresh when visibility returns', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const manualSummary = deferred();
    const manualFull = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'initial-full', replicas: ['r0'] }],
      })
      .mockImplementationOnce(() => manualSummary.promise)
      .mockImplementationOnce(() => manualFull.promise);
    setDocumentVisibility('hidden');

    const { result, unmount } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );
    let mounted = true;

    try {
      await waitFor(() =>
        expect(result.current.serviceData.status).toBe('initial-full')
      );

      let manualRefresh;
      act(() => {
        manualRefresh = result.current.refreshData();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);

      await act(async () => {
        manualSummary.resolve({
          services: [
            { name: 'svc', status: 'manual-summary', summaryOnly: true },
          ],
        });
        manualFull.resolve({
          services: [
            { name: 'svc', status: 'manual-full', replicas: ['manual'] },
          ],
        });
        await manualRefresh;
      });
      expect(result.current.serviceData.status).toBe('manual-full');
      expect(result.current.serviceData.replicas).toEqual(['manual']);

      unmount();
      mounted = false;
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
      jest.useRealTimers();
    }
  });

  it('does not start a periodic refresh while a manual refresh is in flight', async () => {
    jest.useFakeTimers();
    const refreshedSummary = deferred();
    const refreshedFull = deferred();
    let mounted = true;

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'READY' }],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'READY', replicas: [] }],
      });

    const { result, unmount } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      dashboardCache.get
        .mockImplementationOnce(() => refreshedSummary.promise)
        .mockImplementationOnce(() => refreshedFull.promise);

      let refreshPromise;
      act(() => {
        refreshPromise = result.current.refreshData();
      });

      expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });

      // The manual refresh owns both selected-service reads, so the timer must
      // not start a duplicate summary/full pair.
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);

      await act(async () => {
        refreshedSummary.resolve({
          services: [
            {
              name: 'svc',
              status: 'READY',
              replicaHistory: { currentReadyReplicas: 2 },
            },
          ],
        });
        refreshedFull.resolve({
          services: [{ name: 'svc', status: 'READY', replicas: ['r1'] }],
        });
        await refreshPromise;
      });

      dashboardCache.get
        .mockResolvedValueOnce({
          services: [
            {
              name: 'svc',
              replicaHistory: { currentReadyReplicas: 3 },
            },
          ],
        })
        .mockResolvedValueOnce({
          services: [{ name: 'svc', status: 'READY', replicas: ['r2'] }],
        });

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });

      expect(dashboardCache.get).toHaveBeenCalledTimes(6);
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(4);

      unmount();
      mounted = false;
    } finally {
      if (mounted) {
        unmount();
      }
      jest.useRealTimers();
    }
  });

  it('does not reuse an old refresh owner after leaving and returning to a service', async () => {
    const oldSummary = deferred();
    const newSummary = deferred();
    const newFull = deferred();

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc-a', status: 'svc-a-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc-a', status: 'svc-a-full', replicas: ['a0'] }],
      });

    const { result, rerender } = renderHook(
      ({ serviceName }) => useServiceDetails({ serviceName }),
      { initialProps: { serviceName: 'svc-a' } }
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('svc-a-full')
    );

    dashboardCache.get.mockImplementationOnce(() => oldSummary.promise);
    let oldRefreshPromise;
    act(() => {
      oldRefreshPromise = result.current.refreshData();
    });

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc-b', status: 'svc-b-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc-b', status: 'svc-b-full', replicas: ['b0'] }],
      });
    rerender({ serviceName: 'svc-b' });
    await waitFor(() => expect(result.current.serviceData.name).toBe('svc-b'));

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc-a', status: 'svc-a-return-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [
          { name: 'svc-a', status: 'svc-a-return-full', replicas: ['a1'] },
        ],
      });
    rerender({ serviceName: 'svc-a' });
    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('svc-a-return-full')
    );

    dashboardCache.get
      .mockImplementationOnce(() => newSummary.promise)
      .mockImplementationOnce(() => newFull.promise);
    let newRefreshPromise;
    act(() => {
      newRefreshPromise = result.current.refreshData();
    });

    expect(newRefreshPromise).not.toBe(oldRefreshPromise);
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(4);

    await act(async () => {
      newSummary.resolve({
        services: [
          { name: 'svc-a', status: 'svc-a-new-summary', summaryOnly: true },
        ],
      });
      newFull.resolve({
        services: [
          { name: 'svc-a', status: 'svc-a-new-full', replicas: ['a2'] },
        ],
      });
      await newRefreshPromise;
    });

    expect(result.current.serviceData.status).toBe('svc-a-new-full');

    await act(async () => {
      oldSummary.resolve({
        services: [
          { name: 'svc-a', status: 'svc-a-old-summary', summaryOnly: true },
        ],
      });
      await oldRefreshPromise;
    });

    expect(result.current.serviceData.status).toBe('svc-a-new-full');
    expect(result.current.serviceData.replicas).toEqual(['a2']);
  });

  it('scopes manual refresh invalidation to the current service detail keys', async () => {
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'initial-full', replicas: [] }],
      })
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'refreshed-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'refreshed-full', replicas: ['r1'] }],
      });

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('initial-full')
    );

    await act(async () => {
      await result.current.refreshData();
    });

    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.invalidate.mock.calls).toEqual([
      [getServices, detailSummaryArgs('svc')],
      [getServices, detailFullArgs('svc')],
    ]);
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
    expect(result.current.serviceData.status).toBe('refreshed-full');
  });
});

describe('useServiceHistory independent loading', () => {
  const directHistory = (serviceHash = 'hash-a') => ({
    available: true,
    serviceHash,
    bucketSeconds: 60,
    windowStart: 0,
    windowEnd: 3600,
    samples: [],
    requestSamples: [],
    predictionTimeSamples: [],
    autoscalerSamples: [],
  });

  beforeEach(() => {
    jest.resetAllMocks();
    getServiceHistory.mockResolvedValue(directHistory());
  });

  it('waits for the metadata hash, then loads only the initial hour', async () => {
    const { result, rerender, unmount } = renderHook(
      ({ serviceHash }) =>
        useServiceHistory({
          serviceName: 'svc',
          serviceHash,
        }),
      { initialProps: { serviceHash: null } }
    );

    expect(getServiceHistory).not.toHaveBeenCalled();
    expect(result.current.historyLoading).toBe(true);

    rerender({ serviceHash: 'hash-a' });
    await waitFor(() => expect(result.current.replicaHistory).not.toBeNull());

    expect(getServiceHistory).toHaveBeenCalledWith({
      serviceName: 'svc',
      serviceHash: 'hash-a',
      hours: 1,
    });
    expect(result.current.historyLoading).toBe(false);
    unmount();
  });

  it('fetches a larger selected range and reuses it for smaller presets', async () => {
    const { result, unmount } = renderHook(() =>
      useServiceHistory({ serviceName: 'svc', serviceHash: 'hash-a' })
    );
    await waitFor(() => expect(result.current.replicaHistory).not.toBeNull());

    await act(async () => {
      await result.current.loadHistoryHours(12);
    });
    expect(getServiceHistory).toHaveBeenLastCalledWith({
      serviceName: 'svc',
      serviceHash: 'hash-a',
      hours: 12,
    });
    expect(getServiceHistory).toHaveBeenCalledTimes(2);

    await act(async () => {
      await result.current.loadHistoryHours(1);
    });
    expect(getServiceHistory).toHaveBeenCalledTimes(2);
    unmount();
  });

  it('falls back to controller-backed status when direct reads are unavailable', async () => {
    getServiceHistory.mockResolvedValue({
      available: false,
      reason: 'non_consolidated',
      legacyFallback: true,
    });
    dashboardCache.get.mockResolvedValue({
      services: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          replicaHistory: directHistory(),
        },
      ],
    });

    const { result, unmount } = renderHook(() =>
      useServiceHistory({ serviceName: 'svc', serviceHash: 'hash-a' })
    );
    await waitFor(() => expect(result.current.replicaHistory).not.toBeNull());

    expect(dashboardCache.get).toHaveBeenCalledWith(getServices, [
      {
        serviceNames: ['svc'],
        summaryOnly: true,
        historyHours: 1,
      },
    ]);
    expect(result.current.replicaHistory.available).toBe(true);
    unmount();
  });

  it('uses controller-backed history for a landed legacy service without a hash', async () => {
    dashboardCache.get.mockResolvedValue({
      services: [
        {
          name: 'svc',
          serviceHash: null,
          replicaHistory: directHistory(null),
        },
      ],
    });

    const { result, unmount } = renderHook(() =>
      useServiceHistory({
        serviceName: 'svc',
        serviceHash: null,
        metadataReady: true,
      })
    );
    await waitFor(() => expect(result.current.replicaHistory).not.toBeNull());

    expect(getServiceHistory).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledWith(getServices, [
      {
        serviceNames: ['svc'],
        summaryOnly: true,
        historyHours: 1,
      },
    ]);
    expect(result.current.replicaHistory).toMatchObject({
      available: true,
      serviceHash: null,
    });
    expect(result.current.historyLoading).toBe(false);
    unmount();
  });

  it('drops a late history response from the previous service identity', async () => {
    const oldHistory = deferred();
    getServiceHistory
      .mockImplementationOnce(() => oldHistory.promise)
      .mockResolvedValueOnce(directHistory('hash-b'));
    const { result, rerender, unmount } = renderHook(
      ({ serviceName, serviceHash }) =>
        useServiceHistory({ serviceName, serviceHash }),
      {
        initialProps: { serviceName: 'svc-a', serviceHash: 'hash-a' },
      }
    );

    rerender({ serviceName: 'svc-b', serviceHash: 'hash-b' });
    await waitFor(() =>
      expect(result.current.replicaHistory?.serviceHash).toBe('hash-b')
    );

    await act(async () => {
      oldHistory.resolve(directHistory('hash-a'));
      await oldHistory.promise;
    });
    expect(result.current.replicaHistory.serviceHash).toBe('hash-b');
    unmount();
  });

  it('keeps last-good history visible when a refresh fails', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    const { result, unmount } = renderHook(() =>
      useServiceHistory({ serviceName: 'svc', serviceHash: 'hash-a' })
    );
    await waitFor(() => expect(result.current.replicaHistory).not.toBeNull());

    getServiceHistory.mockRejectedValueOnce(new Error('temporary failure'));
    await act(async () => {
      await result.current.refreshHistory();
    });

    expect(result.current.replicaHistory).toMatchObject({
      available: true,
      serviceHash: 'hash-a',
      refreshUnavailable: true,
    });
    expect(result.current.historyLoading).toBe(false);
    expect(consoleError).toHaveBeenCalledWith(
      'Failed to fetch service history:',
      expect.any(Error)
    );
    consoleError.mockRestore();
    unmount();
  });

  it('invalidates history when the service incarnation changes', async () => {
    const onServiceHashMismatch = jest.fn();
    const mismatch = new Error('service changed');
    mismatch.code = 'SERVICE_HASH_MISMATCH';
    getServiceHistory.mockRejectedValueOnce(mismatch);

    const { result, unmount } = renderHook(() =>
      useServiceHistory({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        onServiceHashMismatch,
      })
    );
    await waitFor(() =>
      expect(result.current.replicaHistory?.reason).toBe('service_changed')
    );

    expect(result.current.replicaHistory.available).toBe(false);
    expect(onServiceHashMismatch).toHaveBeenCalledTimes(1);
    unmount();
  });
});

describe('useServiceReplicaData bounded loading', () => {
  const directSummary = (overrides = {}) => ({
    available: true,
    summaries: [
      {
        name: 'svc',
        serviceHash: 'hash-a',
        replicaStatusCounts: { READY: 2, FAILED_PROVISION: 3 },
        replicasReady: 2,
        replicasTotal: 2,
        currentOrUncertainCount: 2,
        pastAttemptCount: 3,
        ...overrides,
      },
    ],
  });
  const directPage = (scope, overrides = {}) => ({
    available: true,
    serviceName: 'svc',
    serviceHash: 'hash-a',
    scope,
    total: 0,
    nextCursor: null,
    observedAt: 100,
    replicas: [],
    ...overrides,
  });

  beforeEach(() => {
    jest.resetAllMocks();
    dashboardCache.get.mockResolvedValue({
      services: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          status: 'READY',
          summaryOnly: true,
          replicas: [],
        },
      ],
    });
    getServiceReplicaSummaries.mockResolvedValue(directSummary());
    getServiceReplicas.mockResolvedValue(
      directPage('current_or_uncertain', {
        total: 2,
        replicas: [
          { id: 3, status: 'READY' },
          { id: 2, status: 'FAILED_CLEANUP' },
        ],
      })
    );
  });

  it('fans out bounded reads after the hash anchor and defers past attempts', async () => {
    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );

    await waitFor(() => expect(result.current.currentPage.total).toBe(2));
    expect(getServiceReplicaSummaries).toHaveBeenCalledWith({
      serviceNames: ['svc'],
    });
    expect(getServiceReplicas).toHaveBeenCalledTimes(1);
    expect(getServiceReplicas).toHaveBeenCalledWith(
      expect.objectContaining({
        scope: 'current_or_uncertain',
        limit: 50,
      })
    );
    expect(dashboardCache.get).not.toHaveBeenCalledWith(
      getServices,
      detailFullArgs('svc')
    );

    getServiceReplicas.mockResolvedValueOnce(
      directPage('past_attempts', {
        total: 3,
        nextCursor: 'past-2',
        replicas: [{ id: 10, status: 'FAILED_PROVISION' }],
      })
    );
    await act(async () => result.current.openPastAttempts());
    expect(getServiceReplicas).toHaveBeenLastCalledWith(
      expect.objectContaining({ scope: 'past_attempts', cursor: null })
    );
    expect(result.current.pastPage.replicas.map((row) => row.id)).toEqual([10]);
  });

  it('loads more explicitly and deduplicates replica IDs', async () => {
    getServiceReplicas
      .mockResolvedValueOnce(
        directPage('current_or_uncertain', {
          total: 3,
          nextCursor: 'current-2',
          replicas: [
            { id: 3, status: 'READY' },
            { id: 2, status: 'STARTING' },
          ],
        })
      )
      .mockResolvedValueOnce(
        directPage('current_or_uncertain', {
          total: 3,
          replicas: [
            { id: 2, status: 'STARTING' },
            { id: 1, status: 'PROVISIONING' },
          ],
        })
      );
    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );
    await waitFor(() =>
      expect(result.current.currentPage.nextCursor).toBe('current-2')
    );

    await act(async () => result.current.loadMoreCurrent());

    expect(result.current.currentPage.replicas.map((row) => row.id)).toEqual([
      3, 2, 1,
    ]);
    expect(getServiceReplicas).toHaveBeenLastCalledWith(
      expect.objectContaining({ cursor: 'current-2' })
    );
  });

  it('does not let load-more supersede an in-flight first-page refresh', async () => {
    getServiceReplicas.mockResolvedValueOnce(
      directPage('current_or_uncertain', {
        total: 3,
        nextCursor: 'current-2',
        replicas: [
          { id: 3, status: 'READY' },
          { id: 2, status: 'STARTING' },
        ],
      })
    );
    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );
    await waitFor(() =>
      expect(result.current.currentPage.nextCursor).toBe('current-2')
    );

    const refreshedPage = deferred();
    getServiceReplicas.mockReturnValueOnce(refreshedPage.promise);
    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshReplicas();
    });
    await waitFor(() => expect(result.current.currentPage.loading).toBe(true));
    const callsDuringRefresh = getServiceReplicas.mock.calls.length;

    await act(async () => result.current.loadMoreCurrent());
    expect(getServiceReplicas).toHaveBeenCalledTimes(callsDuringRefresh);

    await act(async () => {
      refreshedPage.resolve(
        directPage('current_or_uncertain', {
          total: 1,
          replicas: [{ id: 4, status: 'READY' }],
        })
      );
      await refreshedPage.promise;
      await refreshPromise;
    });
    expect(result.current.currentPage.replicas).toEqual([
      { id: 4, status: 'READY' },
    ]);
  });

  it('keeps last-good rows when a direct refresh fails', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );
    await waitFor(() => expect(result.current.currentPage.total).toBe(2));
    getServiceReplicas.mockRejectedValueOnce(new Error('temporarily down'));

    await act(async () => result.current.refreshReplicas());

    expect(result.current.currentPage.replicas.map((row) => row.id)).toEqual([
      3, 2,
    ]);
    expect(result.current.currentPage.refreshUnavailable).toBe(true);
    expect(result.current.currentPage.loading).toBe(false);
    consoleError.mockRestore();
  });

  it('treats a modern not-found page as an identity invalidation', async () => {
    const onServiceHashMismatch = jest.fn();
    getServiceReplicas.mockResolvedValueOnce({
      available: false,
      reason: 'not_found',
      legacyFallback: false,
      replicas: [],
      total: 0,
      nextCursor: null,
    });

    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
        onServiceHashMismatch,
      })
    );

    await waitFor(() => expect(onServiceHashMismatch).toHaveBeenCalledTimes(1));
    expect(result.current.currentPage.unavailable).toBe(true);
  });

  it('uses the full controller path for non-consolidated topology', async () => {
    getServiceReplicaSummaries.mockResolvedValueOnce({
      available: false,
      reason: 'non_consolidated',
      legacyFallback: true,
      summaries: [],
    });
    dashboardCache.get.mockImplementation((_connector, [options]) => {
      if (options.summaryOnly) {
        return Promise.resolve({
          services: [
            {
              name: 'svc',
              serviceHash: 'hash-a',
              status: 'READY',
              summaryOnly: true,
              replicas: [],
            },
          ],
        });
      }
      return Promise.resolve({
        services: [
          {
            name: 'svc',
            serviceHash: 'hash-a',
            status: 'READY',
            replicas: [
              { id: 1, status: 'READY' },
              { id: 2, status: 'FAILED_PROVISION' },
            ],
          },
        ],
      });
    });

    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );

    await waitFor(() => expect(result.current.legacyService).not.toBeNull());
    expect(result.current.currentPage.replicas).toEqual([
      { id: 1, status: 'READY' },
    ]);
    expect(result.current.pastPage.replicas).toEqual([
      { id: 2, status: 'FAILED_PROVISION' },
    ]);
    expect(dashboardCache.get).toHaveBeenCalledWith(
      getServices,
      detailFullArgs('svc')
    );
  });

  it('fences an older forced legacy response', async () => {
    const initialFull = deferred();
    const refreshedFull = deferred();
    let fullCall = 0;
    dashboardCache.get.mockImplementation((_connector, [options]) => {
      if (options.summaryOnly) {
        return Promise.resolve({
          services: [{ name: 'svc', status: 'READY', summaryOnly: true }],
        });
      }
      fullCall += 1;
      return fullCall === 1 ? initialFull.promise : refreshedFull.promise;
    });
    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: null,
        metadataReady: true,
      })
    );
    await waitFor(() => expect(fullCall).toBe(1));
    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshReplicas();
    });
    await waitFor(() => expect(fullCall).toBe(2));
    await act(async () => {
      refreshedFull.resolve({
        services: [
          {
            name: 'svc',
            status: 'FRESH',
            replicas: [{ id: 2, status: 'READY' }],
          },
        ],
      });
      await refreshedFull.promise;
      await refreshPromise;
    });
    await act(async () => {
      initialFull.resolve({
        services: [
          {
            name: 'svc',
            status: 'STALE',
            replicas: [{ id: 1, status: 'READY' }],
          },
        ],
      });
      await initialFull.promise;
    });

    expect(result.current.legacyService.status).toBe('FRESH');
    expect(result.current.currentPage.replicas).toEqual([
      { id: 2, status: 'READY' },
    ]);
  });

  it('deduplicates a replica that moves from current to past', async () => {
    getServiceReplicas
      .mockResolvedValueOnce(
        directPage('current_or_uncertain', {
          total: 1,
          replicas: [{ id: 5, status: 'SHUTTING_DOWN' }],
        })
      )
      .mockResolvedValueOnce(
        directPage('past_attempts', {
          total: 1,
          replicas: [{ id: 5, status: 'FAILED_PROVISION' }],
        })
      );
    const { result } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );
    await waitFor(() => expect(result.current.currentPage.total).toBe(1));
    await act(async () => result.current.openPastAttempts());

    expect(result.current.currentPage.replicas).toEqual([]);
    expect(result.current.currentPage.total).toBe(0);
    expect(result.current.pastPage.replicas).toEqual([
      { id: 5, status: 'FAILED_PROVISION' },
    ]);
  });

  it('drops a late page from the previous route generation', async () => {
    const serviceAPage = deferred();
    dashboardCache.get.mockImplementation((_connector, [options]) => {
      const name = options.serviceNames[0];
      return Promise.resolve({
        services: [
          {
            name,
            serviceHash: name === 'svc-a' ? 'hash-a' : 'hash-b',
            status: 'READY',
            summaryOnly: true,
            replicas: [],
          },
        ],
      });
    });
    getServiceReplicaSummaries
      .mockResolvedValueOnce(
        directSummary({ name: 'svc-a', serviceHash: 'hash-a' })
      )
      .mockResolvedValueOnce(
        directSummary({ name: 'svc-b', serviceHash: 'hash-b' })
      );
    getServiceReplicas
      .mockReturnValueOnce(serviceAPage.promise)
      .mockResolvedValueOnce({
        ...directPage('current_or_uncertain', {
          serviceName: 'svc-b',
          serviceHash: 'hash-b',
          total: 1,
          replicas: [{ id: 2, status: 'READY' }],
        }),
      });

    const { result, rerender } = renderHook(
      ({ serviceName, serviceHash }) =>
        useServiceReplicaData({
          serviceName,
          serviceHash,
          metadataReady: true,
        }),
      {
        initialProps: { serviceName: 'svc-a', serviceHash: 'hash-a' },
      }
    );
    rerender({ serviceName: 'svc-b', serviceHash: 'hash-b' });
    await waitFor(() => expect(result.current.currentPage.total).toBe(1));

    await act(async () => {
      serviceAPage.resolve(
        directPage('current_or_uncertain', {
          serviceName: 'svc-a',
          serviceHash: 'hash-a',
          total: 1,
          replicas: [{ id: 1, status: 'READY' }],
        })
      );
      await serviceAPage.promise;
    });

    expect(result.current.currentPage.replicas).toEqual([
      { id: 2, status: 'READY' },
    ]);
  });
});

describe('ServiceDetails route ownership rendering', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    getServiceHistory.mockResolvedValue({
      available: true,
      serviceHash: 'hash-a',
      bucketSeconds: 60,
      windowStart: 0,
      windowEnd: 3600,
      samples: [],
      requestSamples: [],
      predictionTimeSamples: [],
      autoscalerSamples: [],
    });
  });

  it('shows route loading instead of the previous service while a new route is in flight', async () => {
    const nextSummary = deferred();
    const routerState = {
      isReady: true,
      query: { service: 'svc-a' },
    };
    mockUseRouter.mockImplementation(() => routerState);

    dashboardCache.get.mockResolvedValueOnce({
      services: [
        { name: 'svc-a', status: 'READY', summaryOnly: true, replicas: [] },
      ],
    });

    const { rerender } = render(<ServiceDetailsPage />);

    await waitFor(() =>
      expect(screen.getAllByText('svc-a')).not.toHaveLength(0)
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    dashboardCache.get.mockImplementationOnce(() => nextSummary.promise);
    routerState.query = { service: 'svc-b' };
    rerender(<ServiceDetailsPage />);

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    expect(screen.getByText('Loading service details...')).toBeInTheDocument();
    expect(screen.queryByText('Service not found.')).not.toBeInTheDocument();
    expect(screen.queryByText('svc-a')).not.toBeInTheDocument();

    await act(async () => {
      nextSummary.resolve({
        services: [
          {
            name: 'svc-b',
            status: 'STARTING',
            summaryOnly: true,
            replicas: [],
          },
        ],
      });
      await nextSummary.promise;
    });

    expect(screen.getAllByText('svc-b')).not.toHaveLength(0);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('does not reuse a previous snapshot when returning through an A-B-A route cycle', async () => {
    const serviceBSummary = deferred();
    const freshServiceASummary = deferred();
    const routerState = {
      isReady: true,
      query: { service: 'svc-a' },
    };
    mockUseRouter.mockImplementation(() => routerState);

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          {
            name: 'svc-a',
            status: 'STALE-A',
            summaryOnly: true,
            replicas: [],
          },
        ],
      })
      .mockImplementationOnce(() => serviceBSummary.promise)
      .mockImplementationOnce(() => freshServiceASummary.promise);

    const { rerender } = render(<ServiceDetailsPage />);
    await waitFor(() =>
      expect(screen.getAllByText('STALE-A')).not.toHaveLength(0)
    );

    routerState.query = { service: 'svc-b' };
    rerender(<ServiceDetailsPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    routerState.query = { service: 'svc-a' };
    rerender(<ServiceDetailsPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));
    rerender(<ServiceDetailsPage />);

    expect(screen.getByText('Loading service details...')).toBeInTheDocument();
    expect(screen.queryAllByText('STALE-A')).toHaveLength(0);

    await act(async () => {
      freshServiceASummary.resolve({
        services: [
          {
            name: 'svc-a',
            status: 'FRESH-A',
            summaryOnly: true,
            replicas: [],
          },
        ],
      });
      await freshServiceASummary.promise;
    });

    expect(screen.getAllByText('FRESH-A')).not.toHaveLength(0);
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      serviceBSummary.resolve({
        services: [
          {
            name: 'svc-b',
            status: 'STALE-B',
            summaryOnly: true,
            replicas: [],
          },
        ],
      });
      await serviceBSummary.promise;
    });

    expect(screen.getAllByText('FRESH-A')).not.toHaveLength(0);
    expect(screen.queryAllByText('STALE-B')).toHaveLength(0);
  });

  it('refreshes summary ownership on a placement A-B-A route cycle', async () => {
    const serviceBSummary = deferred();
    const freshServiceASummary = deferred();
    const routerState = {
      isReady: true,
      query: { service: 'svc-a', tab: 'placement' },
    };
    mockUseRouter.mockImplementation(() => routerState);

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [{ name: 'svc-a', status: 'STALE-A', summaryOnly: true }],
      })
      .mockImplementationOnce(() => serviceBSummary.promise)
      .mockImplementationOnce(() => freshServiceASummary.promise);

    const { rerender } = render(<ServiceDetailsPage />);
    await waitFor(() =>
      expect(screen.getByTestId('service-placement')).toBeInTheDocument()
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    routerState.query = { service: 'svc-b', tab: 'placement' };
    rerender(<ServiceDetailsPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    routerState.query = { service: 'svc-a', tab: 'placement' };
    rerender(<ServiceDetailsPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));

    expect(screen.getByText('Loading service details...')).toBeInTheDocument();
    expect(screen.queryByTestId('service-placement')).not.toBeInTheDocument();

    await act(async () => {
      freshServiceASummary.resolve({
        services: [{ name: 'svc-a', status: 'FRESH-A', summaryOnly: true }],
      });
      await freshServiceASummary.promise;
    });

    expect(screen.getByTestId('service-placement')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Placement' })).toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(3);

    await act(async () => {
      serviceBSummary.resolve({
        services: [{ name: 'svc-b', status: 'STALE-B', summaryOnly: true }],
      });
      await serviceBSummary.promise;
    });

    expect(screen.getByTestId('service-placement')).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Placement' })).toBeInTheDocument();
  });

  it('keeps placement metadata-only and uses bounded reads on overview', async () => {
    const routerState = {
      isReady: true,
      query: { service: 'svc', tab: 'placement' },
    };
    mockUseRouter.mockImplementation(() => routerState);

    dashboardCache.get.mockResolvedValueOnce({
      services: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          status: 'READY',
          metadataOnly: true,
        },
      ],
    });
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          replicaStatusCounts: { READY: 1 },
          replicasReady: 1,
          replicasTotal: 1,
          currentOrUncertainCount: 1,
          pastAttemptCount: 0,
        },
      ],
    });
    getServiceReplicas.mockResolvedValue({
      available: true,
      serviceHash: 'hash-a',
      scope: 'current_or_uncertain',
      total: 1,
      nextCursor: null,
      replicas: [{ id: 1, status: 'READY' }],
    });

    const { rerender } = render(<ServiceDetailsPage />);

    await waitFor(() =>
      expect(screen.getByTestId('service-placement')).toBeInTheDocument()
    );

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenNthCalledWith(
      1,
      getServices,
      detailSummaryArgs('svc')
    );

    dashboardCache.get.mockResolvedValueOnce({
      services: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          status: 'READY',
          summaryOnly: true,
          replicas: [],
        },
      ],
    });
    routerState.query = { service: 'svc' };
    rerender(<ServiceDetailsPage />);

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(dashboardCache.get).toHaveBeenNthCalledWith(2, getServices, [
      {
        serviceNames: ['svc'],
        summaryOnly: true,
        includeTargetReplicas: true,
        includeEndpoints: true,
      },
    ]);
    expect(getServiceReplicaSummaries).toHaveBeenCalledWith({
      serviceNames: ['svc'],
    });
    expect(getServiceReplicas).toHaveBeenCalledWith(
      expect.objectContaining({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        scope: 'current_or_uncertain',
        limit: 50,
      })
    );
    expect(dashboardCache.get).not.toHaveBeenCalledWith(
      getServices,
      detailFullArgs('svc')
    );
  });

  it('keeps legacy full-status past attempts capped at 50 rows', async () => {
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'svc' },
    });
    const historical = Array.from({ length: 60 }, (_, index) => ({
      id: index + 1,
      status: 'FAILED_PROVISION',
      version: 1,
    }));
    const fullService = {
      name: 'svc',
      serviceHash: null,
      status: 'READY',
      metadataOnly: false,
      summaryOnly: false,
      replicasReady: 1,
      replicasTotal: 1,
      replicasFailed: 60,
      targetReplicas: 1,
      replicaStatusCounts: { READY: 1, FAILED_PROVISION: 60 },
      replicas: [{ id: 61, status: 'READY', version: 1 }, ...historical],
      acceleratorCapacity: [],
      activeVersions: [1],
      hourlyCostExclusionReasons: {},
    };
    dashboardCache.get.mockImplementation((_connector, [options]) => {
      if (options.metadataOnly) {
        return Promise.resolve({
          services: [
            {
              ...fullService,
              metadataOnly: true,
              replicasReady: null,
              replicasTotal: null,
              replicasFailed: null,
              replicaStatusCounts: null,
              replicas: [],
            },
          ],
        });
      }
      if (options.historyHours) {
        return Promise.resolve({
          services: [
            {
              name: 'svc',
              serviceHash: null,
              replicaHistory: {
                available: true,
                samples: [],
                requestSamples: [],
                predictionTimeSamples: [],
                autoscalerSamples: [],
              },
            },
          ],
        });
      }
      if (options.summaryOnly) {
        return Promise.resolve({
          services: [{ ...fullService, summaryOnly: true, replicas: [] }],
        });
      }
      return Promise.resolve({ services: [fullService] });
    });

    render(<ServiceDetailsPage />);

    const summary = await screen.findByText('Past attempts (60)');
    const details = summary.closest('details');
    expect(details).not.toHaveAttribute('open');
    expect(within(details).getAllByRole('row')).toHaveLength(51);
    expect(
      within(details).getByText('Showing the 50 most recent attempts.')
    ).toBeInTheDocument();
    expect(getServiceReplicaSummaries).not.toHaveBeenCalled();
    expect(getServiceReplicas).not.toHaveBeenCalled();
  });

  it('reuses an existing full snapshot when overview is revisited', async () => {
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'initial-full', replicas: ['r1'] }],
      });

    const { result, rerender } = renderHook(
      ({ loadFull }) => useServiceDetails({ serviceName: 'svc', loadFull }),
      { initialProps: { loadFull: true } }
    );

    await waitFor(() =>
      expect(result.current.serviceData.status).toBe('initial-full')
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      rerender({ loadFull: false });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-full');
    expect(result.current.serviceData.replicas).toEqual(['r1']);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      rerender({ loadFull: true });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-full');
    expect(result.current.serviceData.replicas).toEqual(['r1']);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });
});

describe('ServiceDetailCard cost and request estimates', () => {
  it('shows hourly cost, request activity, and compute cost per request', () => {
    render(
      <ServiceDetailCard
        requestHistory={{
          requestsLastHour: 1234,
          requestWindowSeconds: 3600,
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          uptime: null,
          replicasReady: 2,
          replicasTotal: 2,
          replicasFailed: 0,
          targetReplicas: 2,
          endpoint: null,
          policy: 'autoscaling',
          loadBalancingPolicy: 'round_robin',
          requestedResources: 'L4:1',
          activeVersions: [1],
          estimatedHourlyCost: 5.5,
          spotHourlyCost: 1.5,
          onDemandHourlyCost: 4,
          costTrackedReplicaCount: 2,
          hourlyCostExcludedReplicaCount: 0,
          requestRate: 0.5,
          recentRequestCount: 30,
          requestWindowSeconds: 60,
          inFlightRequests: 2,
          requestQueueDepth: 1,
          rejectedRequests: 3,
          requestStatsAgeSeconds: 4,
          costPerThousandRequests: 3.055555,
        }}
      />
    );

    expect(screen.getByText('$5.50/hr')).toBeTruthy();
    expect(screen.getByText('Estimated tracked compute cost')).toBeTruthy();
    expect(
      screen.getByText(
        'Spot $1.50/hr · On-demand $4.00/hr · 2 active, stopping, or cleanup-uncertain replicas · Current catalog, compute only, not a provider bill'
      )
    ).toBeTruthy();
    expect(screen.getByText('0.50 req/s')).toBeTruthy();
    expect(
      screen.getByText(
        '30 requests in 60s · 1,234 requests in last hour · 2 in flight · 1 queued · 3 rejected · activity report 4s old'
      )
    ).toBeTruthy();
    expect(screen.getByText('$3.0556')).toBeTruthy();
    expect(screen.getByText('Known cloud compute / 1K requests')).toBeTruthy();
  });

  it('shows known cloud cost while identifying excluded Kubernetes capacity', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          uptime: null,
          replicasReady: 120,
          replicasTotal: 120,
          replicasFailed: 0,
          targetReplicas: 120,
          endpoint: null,
          policy: 'autoscaling',
          loadBalancingPolicy: 'least_load',
          requestedResources: 'A100:1',
          activeVersions: [1],
          estimatedHourlyCost: null,
          spotHourlyCost: 0,
          onDemandHourlyCost: 0,
          costTrackedReplicaCount: 120,
          pricedReplicaCount: 0,
          hourlyCostExcludedReplicaCount: 120,
          hourlyCostExclusionReasons: { kubernetes: 120 },
          requestRate: 0.5,
          recentRequestCount: 30,
          requestWindowSeconds: 60,
          costPerThousandRequests: null,
        }}
      />
    );

    expect(screen.getByText('Unknown')).toBeTruthy();
    expect(
      screen.getByText(
        'No pricing available · 120 Kubernetes replicas excluded'
      )
    ).toBeTruthy();
  });

  it('distinguishes missing request activity from missing pricing', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 2,
          replicasTotal: 2,
          replicasFailed: 0,
          targetReplicas: 2,
          activeVersions: [1],
          estimatedHourlyCost: 1,
          spotHourlyCost: 1,
          onDemandHourlyCost: 0,
          costTrackedReplicaCount: 2,
          pricedReplicaCount: 1,
          hourlyCostExcludedReplicaCount: 1,
          hourlyCostExclusionReasons: { kubernetes: 1 },
          requestRate: 0,
          recentRequestCount: 0,
          requestWindowSeconds: 60,
          costPerThousandRequests: null,
        }}
      />
    );

    expect(
      screen.getByText('No recent request rate · 1 Kubernetes replica excluded')
    ).toBeTruthy();
  });

  it('labels logical replicas separately from physical backends', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          uptime: null,
          replicaUnit: 'logical',
          replicasReady: 8,
          replicasTotal: 12,
          replicasFailed: 4,
          physicalReplicasReady: 1,
          physicalReplicasTotal: 2,
          physicalReplicasFailed: 1,
          replicaStatusCounts: {
            READY: 1,
            PROVISIONING: 1,
            FAILED_PROBING: 1,
          },
          targetReplicas: 1,
          endpoint: null,
          policy: 'autoscaling',
          loadBalancingPolicy: 'instance_aware_least_load',
          requestedResources: 'L4:1',
          activeVersions: [2],
          estimatedHourlyCost: null,
          spotHourlyCost: 0,
          onDemandHourlyCost: 0,
          hourlyCostExcludedReplicaCount: 0,
          requestRate: null,
          recentRequestCount: null,
          requestWindowSeconds: null,
          inFlightRequests: null,
          requestQueueDepth: null,
          rejectedRequests: null,
          requestStatsAgeSeconds: null,
          costPerThousandRequests: null,
        }}
      />
    );

    expect(
      screen.getByText('Logical capacity (ready/non-failed)')
    ).toBeTruthy();
    expect(
      screen.getByText(/1\/2 physical backends \(ready\/non-failed\)/)
    ).toBeTruthy();
    expect(screen.getByText(/1 past attempt retained/)).toBeTruthy();
    expect(screen.queryByText(/failed or cleanup-uncertain/)).toBeNull();
    expect(screen.queryByText('Replicas (ready/non-failed)')).toBeNull();
  });

  it('does not show a request total when durable history is unavailable', () => {
    render(
      <ServiceDetailCard
        requestHistory={{
          available: false,
          requestsLastHour: 0,
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          uptime: null,
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          endpoint: null,
          policy: 'fixed',
          loadBalancingPolicy: 'round_robin',
          requestedResources: 'L4:1',
          activeVersions: [1],
          estimatedHourlyCost: null,
          spotHourlyCost: 0,
          onDemandHourlyCost: 0,
          hourlyCostExcludedReplicaCount: 0,
          requestRate: null,
          recentRequestCount: null,
          requestWindowSeconds: null,
          inFlightRequests: null,
          requestQueueDepth: null,
          rejectedRequests: null,
          requestStatsAgeSeconds: null,
          costPerThousandRequests: null,
        }}
      />
    );

    expect(screen.queryByText('0 requests in last hour')).toBeNull();
  });
});

describe('service replica placement breakdown', () => {
  const replicas = [
    {
      cloud: 'kubernetes',
      region: 'research-context',
      status: 'READY',
    },
    {
      cloud: 'Kubernetes',
      region: 'research-context',
      status: 'PROVISIONING',
    },
    {
      cloud: 'Kubernetes',
      region: 'research-context',
      status: 'PROVISIONING',
      launched_at: 100,
    },
    {
      cloud: 'Kubernetes',
      region: 'research-context',
      status: 'STARTING',
    },
    {
      cloud: 'Kubernetes',
      region: 'research-context',
      status: 'FAILED_CLEANUP',
    },
    {
      cloud: 'Kubernetes',
      region: 'research-context',
      status: 'FAILED_PROVISION',
    },
    { cloud: 'aws', region: 'us-east-1', status: 'PENDING' },
    { cloud: 'AWS', region: 'us-east-1', status: 'READY' },
    { cloud: 'GCP', region: 'us-central1', status: 'NOT_READY' },
    { cloud: 'GCP', region: 'us-central1', status: 'PREEMPTED' },
    { cloud: 'GCP', region: 'us-central1', status: 'FAILED_PROBING' },
    { cloud: null, region: null, status: 'SUSPENDED' },
  ];

  it('groups providers and regions into lifecycle counts', () => {
    expect(getReplicaPlacementBreakdown(replicas)).toEqual([
      {
        cloud: 'AWS',
        region: 'us-east-1',
        queuedIntent: 1,
        providerSetup: 0,
        initializingNotReady: 0,
        ready: 1,
        stopping: 0,
        cleanupUncertain: 0,
        historicalFailure: 0,
        other: 0,
        currentOrUncertain: 2,
        trackedAttempts: 2,
      },
      {
        cloud: 'GCP',
        region: 'us-central1',
        queuedIntent: 0,
        providerSetup: 0,
        initializingNotReady: 1,
        ready: 0,
        stopping: 1,
        cleanupUncertain: 0,
        historicalFailure: 1,
        other: 0,
        currentOrUncertain: 2,
        trackedAttempts: 3,
      },
      {
        cloud: 'Kubernetes',
        region: 'research-context',
        queuedIntent: 1,
        providerSetup: 1,
        initializingNotReady: 1,
        ready: 1,
        stopping: 0,
        cleanupUncertain: 1,
        historicalFailure: 1,
        other: 0,
        currentOrUncertain: 5,
        trackedAttempts: 6,
      },
      {
        cloud: 'Unknown',
        region: 'Pending placement',
        queuedIntent: 0,
        providerSetup: 0,
        initializingNotReady: 0,
        ready: 0,
        stopping: 0,
        cleanupUncertain: 0,
        historicalFailure: 0,
        other: 1,
        currentOrUncertain: 1,
        trackedAttempts: 1,
      },
    ]);
  });

  it('renders tracked attempts without presenting the total as machines', () => {
    render(<ReplicaPlacementCard replicas={replicas} loading={false} />);

    expect(screen.getByText('Replica attempts by placement')).toBeTruthy();
    expect(
      screen.getByText(/Queued intent and retained failure history/)
    ).toBeTruthy();
    expect(screen.getByText('Current / uncertain')).toBeTruthy();
    expect(screen.getByText('Tracked attempts')).toBeTruthy();
    const researchRow = screen.getByText('research-context').closest('tr');
    expect(
      within(researchRow)
        .getAllByRole('cell')
        .map((cell) => cell.textContent)
    ).toEqual([
      'Kubernetes',
      'research-context',
      '1',
      '1',
      '1',
      '1',
      '0',
      '1',
      '1',
      '0',
      '5',
      '6',
    ]);
    expect(screen.getByText('Pending placement')).toBeTruthy();
  });
});

describe('service replica table sorting', () => {
  const replicas = [
    {
      id: 10,
      status: 'READY',
      version: 2,
      resources_str: 'L4:1',
      hourlyCost: 2.5,
      region: 'us-west-2',
      endpoint: 'http://10.0.0.10:8000',
      ready_at: 110,
      timeToReadySeconds: 80,
      launched_at: 30,
    },
    {
      id: 2,
      status: 'PROVISIONING',
      version: 1,
      resources_str: 'H100:8',
      hourlyCost: null,
      region: 'us-east-1',
      endpoint: null,
      ready_at: null,
      timeToReadySeconds: null,
      launched_at: 10,
    },
    {
      id: 1,
      status: 'READY',
      version: 1,
      resources_str: 'L4:1',
      hourlyCost: 1.25,
      region: 'us-central1',
      endpoint: 'http://10.0.0.1:8000',
      ready_at: 40,
      timeToReadySeconds: 20,
      launched_at: 20,
    },
  ];

  it('sorts numerically and leaves missing values at the end', () => {
    expect(
      sortReplicas(replicas, {
        key: 'id',
        direction: 'ascending',
      }).map((replica) => replica.id)
    ).toEqual([1, 2, 10]);
    expect(
      sortReplicas(replicas, {
        key: 'hourlyCost',
        direction: 'descending',
      }).map((replica) => replica.id)
    ).toEqual([10, 1, 2]);
  });

  it('orders by ID initially and toggles a selected column', async () => {
    const user = userEvent.setup();
    render(<ReplicasCard replicas={replicas} loading={false} />);

    const rowIds = () =>
      screen
        .getAllByRole('row')
        .slice(1)
        .map((row) => within(row).getAllByRole('cell')[0].textContent);

    expect(rowIds()).toEqual(['1', '2', '10']);
    expect(screen.getByRole('columnheader', { name: 'ID' })).toHaveAttribute(
      'aria-sort',
      'ascending'
    );

    await user.click(screen.getByRole('button', { name: 'Launched' }));
    expect(rowIds()).toEqual(['2', '1', '10']);

    await user.click(screen.getByRole('button', { name: 'Launched' }));
    expect(rowIds()).toEqual(['10', '1', '2']);

    await user.click(screen.getByRole('button', { name: 'Ready in' }));
    expect(rowIds()).toEqual(['1', '10', '2']);

    await user.click(screen.getByRole('button', { name: 'Ready in' }));
    expect(rowIds()).toEqual(['10', '1', '2']);
  });

  it('keeps cleanup uncertainty current and collapses bounded retry history', () => {
    const historical = Array.from({ length: 60 }, (_, index) => ({
      id: index + 1,
      status: index % 2 ? 'FAILED_PROVISION' : 'FAILED_PROBING',
      version: 1,
    }));
    render(
      <ReplicasCard
        replicas={[
          ...historical,
          { id: 61, status: 'READY', version: 2 },
          { id: 62, status: 'FAILED_CLEANUP', version: 2 },
        ]}
        loading={false}
      />
    );

    expect(screen.getByText('FAILED_CLEANUP')).toBeInTheDocument();
    const summary = screen.getByText('Past attempts (60)');
    const details = summary.closest('details');
    expect(details).not.toHaveAttribute('open');
    expect(within(details).getAllByRole('row')).toHaveLength(51);
    expect(
      within(details).getByText('Showing the 50 most recent attempts.')
    ).toBeInTheDocument();
  });

  it('loads current and past pages explicitly with independent placeholders', async () => {
    const user = userEvent.setup();
    const onLoadMoreCurrent = jest.fn();
    const onOpenPast = jest.fn();
    const onLoadMorePast = jest.fn();
    const { rerender } = render(
      <ReplicasCard
        replicas={[{ id: 3, status: 'READY', version: 1 }]}
        loading={false}
        currentTotal={3}
        currentNextCursor="current-2"
        onLoadMoreCurrent={onLoadMoreCurrent}
        pastReplicas={[]}
        pastTotal={2}
        pastLoading
        pastNextCursor="past-2"
        onOpenPast={onOpenPast}
        onLoadMorePast={onLoadMorePast}
      />
    );

    expect(
      screen.getByText('Showing 1 of 3 current or uncertain')
    ).toBeVisible();
    await user.click(
      screen.getByRole('button', { name: 'Load more replicas' })
    );
    expect(onLoadMoreCurrent).toHaveBeenCalledTimes(1);
    expect(onOpenPast).not.toHaveBeenCalled();

    await user.click(screen.getByText('Past attempts (2)'));
    expect(onOpenPast).toHaveBeenCalledTimes(1);
    expect(screen.getByText('Past attempts are loading.')).toBeVisible();
    const pastLoadMore = screen.getByRole('button', {
      name: 'Load more past attempts',
    });
    expect(pastLoadMore).toBeDisabled();
    await user.click(pastLoadMore);
    expect(onLoadMorePast).not.toHaveBeenCalled();

    rerender(
      <ReplicasCard
        replicas={[{ id: 3, status: 'READY', version: 1 }]}
        loading={false}
        currentTotal={3}
        currentNextCursor="current-2"
        onLoadMoreCurrent={onLoadMoreCurrent}
        pastReplicas={[]}
        pastTotal={2}
        pastNextCursor="past-2"
        onOpenPast={onOpenPast}
        onLoadMorePast={onLoadMorePast}
      />
    );
    await user.click(
      screen.getByRole('button', { name: 'Load more past attempts' })
    );
    expect(onLoadMorePast).toHaveBeenCalledTimes(1);
  });

  it('labels fields omitted from the direct projection and shows creation time', () => {
    render(
      <ReplicasCard
        replicas={[
          {
            id: 7,
            status: 'FAILED_CLEANUP',
            version: 2,
            createdAt: 1751600000,
            directProjection: true,
          },
        ]}
        loading={false}
        currentTotal={1}
        pastReplicas={[]}
        pastTotal={0}
      />
    );

    expect(screen.getAllByText('Not loaded')).toHaveLength(4);
    expect(screen.getByRole('columnheader', { name: 'Created' })).toBeVisible();
    expect(
      screen.getByText(formatFullTimestamp(new Date(1751600000 * 1000)))
    ).toBeVisible();
  });
});
