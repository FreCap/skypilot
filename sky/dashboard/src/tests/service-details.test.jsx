import {
  act,
  render,
  renderHook,
  screen,
  within,
  waitFor,
} from '@testing-library/react';
import userEvent from '@testing-library/user-event';

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
import { getServices } from '@/data/connectors/services';
import ServiceDetailsPage, {
  AcceleratorCapacityCard,
  getReplicaPlacementBreakdown,
  ReplicaPlacementCard,
  ReplicasCard,
  ServiceDetailCard,
  sortReplicas,
  useServiceDetails,
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
      summaryOnly: true,
      includeTargetReplicas: true,
      historyHours: 24,
    },
  ];
}

function detailFullArgs(serviceName) {
  return [{ serviceNames: [serviceName] }];
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
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);

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
    expect(dashboardCache.get).toHaveBeenCalledTimes(6);

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

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    let refreshPromise;
    act(() => {
      refreshPromise = result.current.refreshData();
    });

    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
    expect(dashboardCache.invalidateFunction).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      });
      await Promise.resolve();
    });
    expect(result.current.serviceData.status).toBe('initial-summary');

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

  it('unblocks the initial load as soon as full detail lands', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

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
    expect(result.current.historyLoading).toBe(true);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      initialSummary.resolve({
        services: [
          {
            name: 'svc',
            status: 'initial-summary',
            summaryOnly: true,
            replicaHistory: { currentReadyReplicas: 1 },
          },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-full');
    expect(result.current.replicaHistory).toEqual({
      currentReadyReplicas: 1,
    });
    expect(result.current.historyLoading).toBe(false);
  });

  it('renders a late summary after an empty full-detail response', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      initialFull.resolve({ services: [] });
      await Promise.resolve();
    });

    expect(result.current.serviceData).toBe(null);
    expect(result.current.loading).toBe(true);

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-summary');
    expect(result.current.loading).toBe(false);
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('preserves an early summary after an empty full-detail response', async () => {
    const initialSummary = deferred();
    const initialFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => initialSummary.promise)
      .mockImplementationOnce(() => initialFull.promise);

    const { result } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc' })
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-summary');
    expect(result.current.loading).toBe(false);

    await act(async () => {
      initialFull.resolve({ services: [] });
      await Promise.resolve();
    });

    expect(result.current.serviceData.status).toBe('initial-summary');
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

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      initialSummary.reject(new Error('summary unavailable'));
      await Promise.resolve();
    });

    expect(result.current.loading).toBe(true);
    expect(result.current.historyLoading).toBe(false);
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

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    await act(async () => {
      initialSummary.resolve({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      });
      await Promise.resolve();
    });
    expect(result.current.serviceData.status).toBe('initial-summary');

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
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);

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
    const firstFull = deferred();
    const secondSummary = deferred();
    const secondFull = deferred();

    dashboardCache.get
      .mockImplementationOnce(() => firstSummary.promise)
      .mockImplementationOnce(() => firstFull.promise)
      .mockImplementationOnce(() => secondSummary.promise)
      .mockImplementationOnce(() => secondFull.promise);

    const { result, rerender } = renderHook(
      ({ serviceName }) => useServiceDetails({ serviceName }),
      { initialProps: { serviceName: 'svc-a' } }
    );

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));

    rerender({ serviceName: 'svc-b' });
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));

    await act(async () => {
      secondFull.resolve({
        services: [{ name: 'svc-b', status: 'svc-b-full', replicas: ['b'] }],
      });
      await Promise.resolve();
    });
    expect(result.current.serviceData.name).toBe('svc-b');

    await act(async () => {
      firstSummary.resolve({
        services: [
          { name: 'svc-a', status: 'svc-a-summary', summaryOnly: true },
        ],
      });
      firstFull.resolve({
        services: [{ name: 'svc-a', status: 'svc-a-full', replicas: ['a'] }],
      });
      secondSummary.resolve({
        services: [
          { name: 'svc-b', status: 'svc-b-summary', summaryOnly: true },
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
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));

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
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);

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
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));

    await act(async () => {
      nextSummary.reject(new Error('summary unavailable'));
      await Promise.resolve();
    });
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
    expect(dashboardCache.get).toHaveBeenCalledTimes(6);

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
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
      expect(dashboardCache.invalidate.mock.calls).toEqual([
        [getServices, detailSummaryArgs('svc')],
        [getServices, detailFullArgs('svc')],
      ]);

      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000 + 30 * 1000);
        await Promise.resolve();
      });

      // A slow full refresh must not accumulate a new summary/full pair at
      // every timer boundary.
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
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
    const pollFull = deferred();
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
      .mockImplementationOnce(() => pollFull.promise)
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
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);

      setDocumentVisibility('hidden');
      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(6);

      await act(async () => {
        pollSummary.resolve({
          services: [
            { name: 'svc', status: 'stale-summary', summaryOnly: true },
          ],
        });
        pollFull.resolve({
          services: [
            { name: 'svc', status: 'stale-full', replicas: ['stale'] },
          ],
        });
        await Promise.all([pollSummary.promise, pollFull.promise]);
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
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
      expect(dashboardCache.invalidate).toHaveBeenCalledTimes(2);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
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
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });

      // The manual refresh owns both selected-service reads, so the timer must
      // not start a duplicate summary/full pair.
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
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
    const oldFull = deferred();
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

    dashboardCache.get
      .mockImplementationOnce(() => oldSummary.promise)
      .mockImplementationOnce(() => oldFull.promise);
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
      oldFull.resolve({
        services: [
          { name: 'svc-a', status: 'svc-a-old-full', replicas: ['old'] },
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

describe('ServiceDetails route ownership rendering', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  it('shows route loading instead of the previous service while a new route is in flight', async () => {
    const nextSummary = deferred();
    const nextFull = deferred();
    const routerState = {
      isReady: true,
      query: { service: 'svc-a' },
    };
    mockUseRouter.mockImplementation(() => routerState);

    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc-a', status: 'READY', summaryOnly: true, replicas: [] },
        ],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc-a', status: 'READY', replicas: [] }],
      });

    const { rerender } = render(<ServiceDetailsPage />);

    await waitFor(() =>
      expect(screen.getAllByText('svc-a')).not.toHaveLength(0)
    );
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    dashboardCache.get
      .mockImplementationOnce(() => nextSummary.promise)
      .mockImplementationOnce(() => nextFull.promise);
    routerState.query = { service: 'svc-b' };
    rerender(<ServiceDetailsPage />);

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));

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
      nextFull.resolve({
        services: [{ name: 'svc-b', status: 'READY', replicas: [] }],
      });
      await Promise.all([nextSummary.promise, nextFull.promise]);
    });

    expect(screen.getAllByText('svc-b')).not.toHaveLength(0);
    expect(dashboardCache.get).toHaveBeenCalledTimes(4);
  });

  it('does not reuse a previous snapshot when returning through an A-B-A route cycle', async () => {
    const serviceBSummary = deferred();
    const serviceBFull = deferred();
    const freshServiceASummary = deferred();
    const freshServiceAFull = deferred();
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
      .mockResolvedValueOnce({
        services: [{ name: 'svc-a', status: 'STALE-A', replicas: [] }],
      })
      .mockImplementationOnce(() => serviceBSummary.promise)
      .mockImplementationOnce(() => serviceBFull.promise)
      .mockImplementationOnce(() => freshServiceASummary.promise)
      .mockImplementationOnce(() => freshServiceAFull.promise);

    const { rerender } = render(<ServiceDetailsPage />);
    await waitFor(() =>
      expect(screen.getByText('STALE-A')).toBeInTheDocument()
    );

    routerState.query = { service: 'svc-b' };
    rerender(<ServiceDetailsPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));

    routerState.query = { service: 'svc-a' };
    rerender(<ServiceDetailsPage />);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(6));
    rerender(<ServiceDetailsPage />);

    expect(screen.getByText('Loading service details...')).toBeInTheDocument();
    expect(screen.queryByText('STALE-A')).not.toBeInTheDocument();

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
      freshServiceAFull.resolve({
        services: [{ name: 'svc-a', status: 'FRESH-A', replicas: [] }],
      });
      await Promise.all([
        freshServiceASummary.promise,
        freshServiceAFull.promise,
      ]);
    });

    expect(screen.getByText('FRESH-A')).toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(6);

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
      serviceBFull.resolve({
        services: [{ name: 'svc-b', status: 'STALE-B', replicas: [] }],
      });
      await Promise.all([serviceBSummary.promise, serviceBFull.promise]);
    });

    expect(screen.getByText('FRESH-A')).toBeInTheDocument();
    expect(screen.queryByText('STALE-B')).not.toBeInTheDocument();
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

  it('keeps placement loads summary-only until overview needs replicas', async () => {
    const routerState = {
      isReady: true,
      query: { service: 'svc', tab: 'placement' },
    };
    mockUseRouter.mockImplementation(() => routerState);

    dashboardCache.get.mockResolvedValueOnce({
      services: [{ name: 'svc', status: 'READY', summaryOnly: true }],
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
      services: [{ name: 'svc', status: 'READY', replicas: [] }],
    });
    routerState.query = { service: 'svc' };
    rerender(<ServiceDetailsPage />);

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
    expect(dashboardCache.get).toHaveBeenNthCalledWith(
      2,
      getServices,
      detailFullArgs('svc')
    );
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
    expect(
      screen.getByText(/4 failed or cleanup-uncertain slots, including history/)
    ).toBeTruthy();
    expect(
      screen.getByText(
        /1 failed or cleanup-uncertain backend, including history/
      )
    ).toBeTruthy();
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
});
