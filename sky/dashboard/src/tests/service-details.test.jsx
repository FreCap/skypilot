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
    getServiceDemand: jest.fn(),
    getServiceHistory: jest.fn(),
    getServicePricing: jest.fn(),
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
  getServiceDemand,
  getServiceHistory,
  getServicePricing,
  getServiceReplicaSummaries,
  getServiceReplicas,
  getServices,
} from '@/data/connectors/services';
import ServiceDetailsPage, {
  AcceleratorCapacityCard,
  applyCurrentCapacityPlanHistory,
  currentCapacityPlanLocalDeadline,
  getReplicaPlacementBreakdown,
  ReplicaPlacementCard,
  ReplicasCard,
  ServiceDetailCard,
  getLatestTerminalObservationReportAgeSeconds,
  getTerminalPredictionObservationSummaryLastHour,
  getTerminalPredictionObservationsLastHour,
  sortReplicas,
  useServiceDetails,
  useServiceDemand,
  useServiceHistory,
  useServicePricing,
  useServiceReplicaData,
  useServiceSummaryBootstrap,
} from '@/pages/services/[service]';

function committedPlanHistory({
  demandTarget = 2,
  observedAt = 99,
  validUntil = 110,
  windowEnd = 100,
  receivedAt = 100,
} = {}) {
  return {
    available: true,
    serviceHash: 'hash-a',
    windowEnd,
    receivedAt,
    autoscalerProjectionMode: 'DURABLE_FEED',
    autoscalerProjectionModeMalformed: false,
    autoscalerLatestSampleMalformed: false,
    autoscalerSamples: [
      {
        timestamp: 60,
        observedAt,
        controllerSessionId: 'a'.repeat(32),
        version: 1,
        replicaUnit: 'physical_backend',
        demandTarget,
        capacityTarget: demandTarget,
        readyCapacity: 0,
        provisioningCapacity: 0,
        totalCapacity: 0,
        peakInFlight: demandTarget,
        peakQueueDepth: demandTarget,
        acceleratorBreakdown: {
          capacitySemanticsVersion: 2,
          configuredAccelerators: ['L4'],
          minReplicas: { L4: 0 },
          demandTarget: { L4: demandTarget },
          warmRetentionTarget: { L4: 0 },
          coldLaunchAuthority: { L4: demandTarget },
          readyCapacity: { L4: 0 },
          provisioningCapacity: { L4: 0 },
          totalCapacity: { L4: 0 },
          zeroCostReadyCapacity: { L4: 0 },
          fillTarget: { L4: 0 },
          freeReservedSlots: { L4: 0 },
          capacityPlan: {
            generation: 7,
            sha256: 'a'.repeat(64),
            validUntil,
          },
        },
      },
    ],
  };
}

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

  it('renders unavailable planned values as n/a without hiding realized capacity', () => {
    const serviceData = applyCurrentCapacityPlanHistory(
      {
        targetReplicas: 9,
        acceleratorCapacity: [
          {
            card: 'L4',
            ready: 1,
            provisioning: 2,
            total: 3,
            demandTarget: 9,
          },
        ],
      },
      { available: false, reason: 'provider_timeout' }
    );
    render(<AcceleratorCapacityCard serviceData={serviceData} />);

    expect(
      screen.getByText(/Current committed plan unavailable/)
    ).toBeVisible();
    const row = screen.getByText('L4').closest('tr');
    expect(within(row).getAllByText('n/a')).toHaveLength(7);
    expect(within(row).getByText('1')).toBeVisible();
    expect(within(row).getByText('2')).toBeVisible();
    expect(within(row).getByText('3')).toBeVisible();
  });
});

describe('current committed capacity-plan history', () => {
  const service = {
    name: 'svc',
    serviceHash: 'hash-a',
    version: 1,
    targetReplicas: 99,
    acceleratorCapacity: [{ card: 'L4', ready: 0, total: 0 }],
  };

  it('uses a fresh exact plan and preserves an authoritative zero target', () => {
    const history = committedPlanHistory({ demandTarget: 0 });
    expect(currentCapacityPlanLocalDeadline(history)).toBe(110);

    expect(
      applyCurrentCapacityPlanHistory(service, history, 100)
    ).toMatchObject({
      targetReplicas: 0,
      fillTarget: 0,
      freeReservedSlots: 0,
      capacityPlanSummary: { status: 'AVAILABLE' },
      acceleratorCapacity: [
        {
          card: 'L4',
          demandTarget: 0,
          warmRetentionTarget: 0,
          coldLaunchAuthority: 0,
        },
      ],
    });
  });

  it('retains a failed refresh only until the DB-relative lease expires', () => {
    const history = {
      ...committedPlanHistory(),
      refreshUnavailable: true,
    };
    expect(
      applyCurrentCapacityPlanHistory(service, history, 109).capacityPlanSummary
    ).toMatchObject({ status: 'AVAILABLE', refreshUnavailable: true });
    expect(
      applyCurrentCapacityPlanHistory(service, history, 110).capacityPlanSummary
        .status
    ).toBe('STALE');
  });

  it.each([
    [
      'unavailable',
      { available: false, reason: 'history_unavailable' },
      'UNAVAILABLE',
      100,
    ],
    [
      'malformed',
      {
        ...committedPlanHistory(),
        autoscalerLatestSampleMalformed: true,
      },
      'MALFORMED',
      100,
    ],
    ['stale', committedPlanHistory(), 'STALE', 111],
  ])(
    'fails %s plan state closed without fabricating zero',
    (_name, history, status, now) => {
      const result = applyCurrentCapacityPlanHistory(service, history, now);
      expect(result.targetReplicas).toBeNull();
      expect(result.capacityPlanSummary.status).toBe(status);
      expect(result.acceleratorCapacity[0].demandTarget).toBeNull();
      expect(result.acceleratorCapacity[0].coldLaunchAuthority).toBeNull();
    }
  );
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

function getDetailValue(label) {
  const heading = screen
    .getAllByText(label)
    .find(
      (element) =>
        element.tagName === 'DIV' && element.classList.contains('font-medium')
    );
  if (!heading?.nextElementSibling) {
    throw new Error(`No detail value found for ${label}`);
  }
  return heading.nextElementSibling;
}

describe('useServiceSummaryBootstrap controller-independent identity', () => {
  beforeEach(() => {
    jest.resetAllMocks();
  });

  it('loads the persisted service identity without controller transport', async () => {
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          persistedMetadataLoaded: true,
          status: 'READY',
        },
      ],
    });

    const { result } = renderHook(() =>
      useServiceSummaryBootstrap({ serviceName: 'svc' })
    );

    await waitFor(() =>
      expect(result.current.serviceSummary?.serviceHash).toBe('hash-a')
    );
    expect(getServiceReplicaSummaries).toHaveBeenCalledWith({
      serviceNames: ['svc'],
    });
    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('does not restore controller identity after a proven direct refresh fails', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    getServiceReplicaSummaries.mockResolvedValueOnce({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          status: 'READY',
        },
      ],
    });
    const { result } = renderHook(() => {
      const summary = useServiceSummaryBootstrap({ serviceName: 'svc' });
      const compatibility = useServiceDetails({
        serviceName: summary.legacyFallback ? 'svc' : null,
        loadFull: false,
      });
      return { ...summary, compatibility };
    });

    try {
      await waitFor(() =>
        expect(result.current.serviceSummary?.serviceHash).toBe('hash-a')
      );
      getServiceReplicaSummaries.mockRejectedValueOnce(
        new Error('direct transport unavailable')
      );

      await act(async () => result.current.refreshSummary());

      expect(result.current.legacyFallback).toBe(false);
      expect(result.current.serviceSummary).toMatchObject({
        serviceHash: 'hash-a',
        refreshUnavailable: true,
      });
      expect(dashboardCache.get).not.toHaveBeenCalled();
    } finally {
      consoleError.mockRestore();
    }
  });

  it('queues exactly one fresh summary after an in-flight request', async () => {
    const initial = deferred();
    const successor = deferred();
    getServiceReplicaSummaries
      .mockReturnValueOnce(initial.promise)
      .mockReturnValueOnce(successor.promise);

    const { result } = renderHook(() =>
      useServiceSummaryBootstrap({ serviceName: 'svc' })
    );
    await waitFor(() =>
      expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(1)
    );

    let firstRefresh;
    let secondRefresh;
    act(() => {
      firstRefresh = result.current.refreshSummary();
      secondRefresh = result.current.refreshSummary();
    });
    expect(firstRefresh).toBe(secondRefresh);
    expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(1);

    await act(async () => {
      initial.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [
          {
            name: 'svc',
            serviceHash: 'hash-before-boundary',
            persistedMetadataLoaded: true,
          },
        ],
      });
      await initial.promise;
    });
    await waitFor(() =>
      expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(2)
    );

    await act(async () => {
      successor.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [
          {
            name: 'svc',
            serviceHash: 'hash-after-boundary',
            persistedMetadataLoaded: true,
          },
        ],
      });
      await Promise.all([successor.promise, firstRefresh, secondRefresh]);
    });
    expect(result.current.serviceSummary?.serviceHash).toBe(
      'hash-after-boundary'
    );
  });

  it('does not resurrect a queued successor across an A-B-A route cycle', async () => {
    const staleServiceA = deferred();
    const staleServiceB = deferred();
    const currentServiceA = deferred();
    getServiceReplicaSummaries
      .mockReturnValueOnce(staleServiceA.promise)
      .mockReturnValueOnce(staleServiceB.promise)
      .mockReturnValueOnce(currentServiceA.promise);

    const { result, rerender } = renderHook(
      ({ serviceName }) => useServiceSummaryBootstrap({ serviceName }),
      { initialProps: { serviceName: 'svc-a' } }
    );
    await waitFor(() =>
      expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(1)
    );

    let queuedRefresh;
    act(() => {
      queuedRefresh = result.current.refreshSummary();
    });
    rerender({ serviceName: 'svc-b' });
    await waitFor(() =>
      expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(2)
    );
    rerender({ serviceName: 'svc-a' });
    await waitFor(() =>
      expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(3)
    );

    await act(async () => {
      staleServiceA.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [{ name: 'svc-a', serviceHash: 'stale-hash-a' }],
      });
      await staleServiceA.promise;
    });
    // The retired successor reuses the current route owner. It must not start
    // a fourth request or supersede the legitimate current A response.
    expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(3);

    await act(async () => {
      currentServiceA.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [{ name: 'svc-a', serviceHash: 'current-hash-a' }],
      });
      await Promise.all([currentServiceA.promise, queuedRefresh]);
    });
    expect(result.current.serviceSummary?.serviceHash).toBe('current-hash-a');
    expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(3);

    await act(async () => {
      staleServiceB.resolve({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [{ name: 'svc-b', serviceHash: 'stale-hash-b' }],
      });
      await staleServiceB.promise;
    });
    expect(result.current.serviceSummary?.serviceHash).toBe('current-hash-a');
  });
});

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
      expect(dashboardCache.get.mock.calls).toEqual([
        [getServices, detailSummaryArgs('svc')],
        [getServices, detailSummaryArgs('svc')],
        [getServices, detailSummaryArgs('svc')],
      ]);
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

  it('does not supersede a pending summary-only controller poll', async () => {
    jest.useFakeTimers();
    const pendingPoll = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [
          { name: 'svc', status: 'initial-summary', summaryOnly: true },
        ],
      })
      .mockReturnValueOnce(pendingPoll.promise);

    const { result, unmount } = renderHook(() =>
      useServiceDetails({ serviceName: 'svc', loadFull: false })
    );
    let mounted = true;
    try {
      await waitFor(() =>
        expect(result.current.serviceData.status).toBe('initial-summary')
      );

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      let manualRefresh;
      act(() => {
        manualRefresh = result.current.refreshData();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        pendingPoll.resolve({
          services: [
            { name: 'svc', status: 'poll-summary', summaryOnly: true },
          ],
        });
        await manualRefresh;
      });
      expect(result.current.serviceData.status).toBe('poll-summary');

      unmount();
      mounted = false;
    } finally {
      if (mounted) unmount();
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

  it('does not overlap persisted-history reads at the polling cadence', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const slowRefresh = deferred();
    setDocumentVisibility('visible');
    getServiceHistory
      .mockResolvedValueOnce(directHistory())
      .mockImplementation(() => slowRefresh.promise);

    const { result, unmount } = renderHook(() =>
      useServiceHistory({ serviceName: 'svc', serviceHash: 'hash-a' })
    );
    let mounted = true;

    try {
      await waitFor(() => expect(result.current.replicaHistory).not.toBeNull());

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      expect(getServiceHistory).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000);
        await Promise.resolve();
      });

      expect(getServiceHistory).toHaveBeenCalledTimes(2);
      unmount();
      mounted = false;
    } finally {
      if (mounted) unmount();
      slowRefresh.resolve(directHistory());
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

  it('supersedes a pre-hide history poll on visibility restoration', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const pollRefresh = deferred();
    const visibilityRefresh = deferred();
    setDocumentVisibility('visible');
    getServiceHistory
      .mockResolvedValueOnce(directHistory())
      .mockImplementationOnce(() => pollRefresh.promise)
      .mockImplementationOnce(() => visibilityRefresh.promise);

    const { result, unmount } = renderHook(() =>
      useServiceHistory({ serviceName: 'svc', serviceHash: 'hash-a' })
    );
    let mounted = true;

    try {
      await waitFor(() => expect(result.current.replicaHistory).not.toBeNull());
      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      expect(getServiceHistory).toHaveBeenCalledTimes(2);

      setDocumentVisibility('hidden');
      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(getServiceHistory).toHaveBeenCalledTimes(3);

      await act(async () => {
        visibilityRefresh.resolve(directHistory());
        await visibilityRefresh.promise;
      });
      unmount();
      mounted = false;
    } finally {
      if (mounted) unmount();
      pollRefresh.resolve(directHistory());
      visibilityRefresh.resolve(directHistory());
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
});

describe('useServiceDemand controller-independent loading', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    getServiceReplicaSummaries.mockResolvedValue({
      available: false,
      reason: 'unsupported',
      legacyFallback: true,
      summaries: [],
    });
    getServiceDemand.mockImplementation(({ serviceHash }) =>
      Promise.resolve({
        serviceName: 'svc',
        serviceHash,
        requestTelemetryState: 'fresh',
        requestTelemetryReason: 'complete',
        recentRequestCount: 9,
        requestWindowSeconds: 60,
        requestRate: 0.15,
        inFlightRequests: 2,
        requestQueueDepth: 1,
        rejectedRequests: 0,
        requestStatsAgeSeconds: 2,
      })
    );
  });

  it('waits for the service hash and reads demand directly', async () => {
    const { result, rerender, unmount } = renderHook(
      ({ serviceHash }) =>
        useServiceDemand({ serviceName: 'svc', serviceHash }),
      { initialProps: { serviceHash: null } }
    );

    expect(getServiceDemand).not.toHaveBeenCalled();
    rerender({ serviceHash: 'hash-a' });
    await waitFor(() =>
      expect(result.current.demandData?.recentRequestCount).toBe(9)
    );

    expect(getServiceDemand).toHaveBeenCalledWith({
      serviceName: 'svc',
      serviceHash: 'hash-a',
    });
    unmount();
  });

  it('marks last-good demand stale when a refresh fails', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    const { result, unmount } = renderHook(() =>
      useServiceDemand({ serviceName: 'svc', serviceHash: 'hash-a' })
    );
    await waitFor(() =>
      expect(result.current.demandData?.requestTelemetryState).toBe('fresh')
    );

    getServiceDemand.mockRejectedValueOnce(new Error('temporary failure'));
    await act(async () => {
      await result.current.refreshDemand();
    });

    expect(result.current.demandData).toMatchObject({
      requestTelemetryState: 'stale',
      requestTelemetryReason: 'dashboard_refresh_failed',
      recentRequestCount: 9,
      requestStatsAgeSeconds: null,
    });
    consoleError.mockRestore();
    unmount();
  });

  it('coalesces refreshes while a direct demand read is in flight', async () => {
    let resolveDemand;
    getServiceDemand.mockReturnValueOnce(
      new Promise((resolve) => {
        resolveDemand = resolve;
      })
    );
    const { result, unmount } = renderHook(() =>
      useServiceDemand({ serviceName: 'svc', serviceHash: 'hash-a' })
    );
    await waitFor(() => expect(getServiceDemand).toHaveBeenCalledTimes(1));

    const first = result.current.refreshDemand();
    const second = result.current.refreshDemand();

    expect(first).toBe(second);
    expect(getServiceDemand).toHaveBeenCalledTimes(1);
    await act(async () => {
      resolveDemand({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        requestTelemetryState: 'fresh',
        requestTelemetryReason: 'complete',
        inFlightRequests: 0,
      });
      await first;
    });
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
  const persistedSummary = (overrides = {}) =>
    directSummary(overrides).summaries[0];
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
        persistedReplicaSummary: persistedSummary(),
        metadataReady: true,
      })
    );

    await waitFor(() => expect(result.current.currentPage.total).toBe(2));
    expect(getServiceReplicaSummaries).not.toHaveBeenCalled();
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

  it('keeps one persisted page refresh owner across slow polling intervals', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');
    const pendingPage = deferred();
    getServiceReplicas.mockReturnValue(pendingPage.promise);

    const { unmount } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );

    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(getServiceReplicaSummaries).not.toHaveBeenCalled();
      expect(getServiceReplicas).toHaveBeenCalledTimes(1);

      await act(async () => {
        jest.advanceTimersByTime(3 * 60 * 1000);
        await Promise.resolve();
      });

      expect(getServiceReplicaSummaries).not.toHaveBeenCalled();
      expect(getServiceReplicas).toHaveBeenCalledTimes(1);
    } finally {
      unmount();
      pendingPage.resolve(
        directPage('current_or_uncertain', {
          total: 1,
          replicas: [{ id: 1, status: 'READY' }],
        })
      );
      jest.runOnlyPendingTimers();
      jest.useRealTimers();
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

  it('keeps controller enrichment singleflight outside persisted refresh ownership', async () => {
    jest.useFakeTimers();
    const controllerPending = deferred();
    dashboardCache.get.mockReturnValue(controllerPending.promise);
    const { result, unmount } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );
    let mounted = true;
    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      let manualRefresh;
      act(() => {
        manualRefresh = result.current.refreshReplicas();
      });
      await act(async () => {
        await manualRefresh;
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      unmount();
      mounted = false;
    } finally {
      if (mounted) unmount();
      controllerPending.resolve({ services: [] });
      jest.runOnlyPendingTimers();
      jest.useRealTimers();
    }
  });

  it('lets a manual replica refresh supersede automatic work and reuses its owner', async () => {
    const initialPage = deferred();
    const manualPage = deferred();
    getServiceReplicas
      .mockReturnValueOnce(initialPage.promise)
      .mockReturnValueOnce(manualPage.promise);

    const { result, unmount } = renderHook(() =>
      useServiceReplicaData({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
      })
    );
    await act(async () => {
      await Promise.resolve();
    });

    let firstManual;
    let secondManual;
    act(() => {
      firstManual = result.current.refreshReplicas();
      secondManual = result.current.refreshReplicas();
    });

    expect(firstManual).toBe(secondManual);
    expect(getServiceReplicaSummaries).not.toHaveBeenCalled();
    expect(getServiceReplicas).toHaveBeenCalledTimes(2);

    unmount();
    initialPage.resolve(directPage('current_or_uncertain'));
    manualPage.resolve(directPage('current_or_uncertain'));
    await Promise.allSettled([firstManual, initialPage.promise]);
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

  it('uses the full controller path for legacy topology', async () => {
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
        serviceHash: null,
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

function pricingAggregateResult(overrides = {}) {
  return {
    available: true,
    serviceName: 'svc',
    serviceHash: 'hash-a',
    observedAt: 100,
    priceBasis: 'version_catalog',
    aggregate: {
      available: true,
      unavailableReason: null,
      coverage: 'complete',
      estimatedHourlyCost: 1.25,
      spotHourlyCost: 0,
      nonSpotHourlyCost: 1.25,
      costTrackedReplicaCount: 1,
      pricedReplicaCount: 1,
      hourlyCostExcludedReplicaCount: 0,
      hourlyCostExclusionReasons: {},
    },
    replicas: [],
    legacyFallback: false,
    ...overrides,
  };
}

function pricingRowResult(replicas, overrides = {}) {
  return {
    available: true,
    serviceName: 'svc',
    serviceHash: 'hash-a',
    observedAt: 100,
    priceBasis: 'version_catalog',
    aggregate: null,
    replicas,
    legacyFallback: false,
    ...overrides,
  };
}

describe('useServicePricing independent enrichment', () => {
  const replica = {
    id: 7,
    status: 'READY',
    pricingFingerprint: 'fp-7',
  };
  const currentPage = {
    serviceHash: 'hash-a',
    replicas: [replica],
  };

  beforeEach(() => {
    jest.resetAllMocks();
  });

  it('fans out aggregate and fingerprint-fenced row pricing independently', async () => {
    getServicePricing.mockImplementation(({ replicaIds = [] }) => {
      if (replicaIds.length === 0) return pricingAggregateResult();
      return pricingRowResult([
        {
          id: 7,
          pricingFingerprint: 'fp-7',
          hourlyCost: 0,
          priceSource: 'zero_cost_provenance',
          hourlyCostExclusionReason: null,
        },
      ]);
    });

    const { result } = renderHook(() =>
      useServicePricing({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
        currentPage,
      })
    );

    await waitFor(() =>
      expect(result.current.aggregate?.estimatedHourlyCost).toBe(1.25)
    );
    await waitFor(() =>
      expect(result.current.getReplicaPricing(replica)).toMatchObject({
        state: 'available',
        hourlyCost: 0,
        priceSource: 'zero_cost_provenance',
      })
    );
    expect(getServicePricing).toHaveBeenCalledWith({
      serviceName: 'svc',
      serviceHash: 'hash-a',
    });
    expect(getServicePricing).toHaveBeenCalledWith({
      serviceName: 'svc',
      serviceHash: 'hash-a',
      replicaIds: [7],
    });
  });

  it('retries a row request interrupted by leaving and returning to Overview', async () => {
    const staleRow = deferred();
    let rowAttempt = 0;
    getServicePricing.mockImplementation(({ replicaIds = [] }) => {
      if (replicaIds.length === 0) return pricingAggregateResult();
      rowAttempt += 1;
      if (rowAttempt === 1) return staleRow.promise;
      return pricingRowResult([
        {
          id: 7,
          pricingFingerprint: 'fp-7',
          hourlyCost: 2,
          priceSource: 'version_catalog',
          hourlyCostExclusionReason: null,
        },
      ]);
    });

    const { result, rerender } = renderHook(
      ({ enabled }) =>
        useServicePricing({
          serviceName: 'svc',
          serviceHash: 'hash-a',
          metadataReady: true,
          currentPage,
          enabled,
        }),
      { initialProps: { enabled: true } }
    );

    await waitFor(() => expect(rowAttempt).toBe(1));
    expect(result.current.getReplicaPricing(replica)).toEqual({
      state: 'loading',
    });

    rerender({ enabled: false });
    rerender({ enabled: true });

    await waitFor(() => expect(rowAttempt).toBe(2));
    await waitFor(() =>
      expect(result.current.getReplicaPricing(replica)).toMatchObject({
        state: 'available',
        hourlyCost: 2,
      })
    );

    await act(async () => {
      staleRow.resolve(
        pricingRowResult([
          {
            id: 7,
            pricingFingerprint: 'fp-7',
            hourlyCost: 99,
            priceSource: 'version_catalog',
            hourlyCostExclusionReason: null,
          },
        ])
      );
      await staleRow.promise;
    });
    expect(result.current.getReplicaPricing(replica).hourlyCost).toBe(2);
  });

  it('does not merge a mismatched fingerprint and refreshes the bounded page', async () => {
    const onRefreshCurrentPage = jest.fn().mockResolvedValue();
    getServicePricing.mockImplementation(({ replicaIds = [] }) =>
      replicaIds.length === 0
        ? pricingAggregateResult()
        : pricingRowResult([
            {
              id: 7,
              pricingFingerprint: 'new-fingerprint',
              hourlyCost: 9,
              priceSource: 'version_catalog',
              hourlyCostExclusionReason: null,
            },
          ])
    );

    const { result } = renderHook(() =>
      useServicePricing({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
        currentPage,
        onRefreshCurrentPage,
      })
    );

    await waitFor(() => expect(onRefreshCurrentPage).toHaveBeenCalledTimes(1));
    expect(result.current.getReplicaPricing(replica)).toEqual({
      state: 'unavailable',
    });
  });

  it('settles a missing-ID result and immediately refreshes the current page', async () => {
    const onRefreshCurrentPage = jest.fn().mockResolvedValue();
    getServicePricing.mockImplementation(({ replicaIds = [] }) =>
      replicaIds.length === 0
        ? pricingAggregateResult()
        : pricingRowResult([
            {
              id: 7,
              pricingFingerprint: null,
              hourlyCost: null,
              priceSource: null,
              hourlyCostExclusionReason: 'not_current_or_uncertain',
            },
          ])
    );

    const { result } = renderHook(() =>
      useServicePricing({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
        currentPage,
        onRefreshCurrentPage,
      })
    );

    await waitFor(() => expect(onRefreshCurrentPage).toHaveBeenCalledTimes(1));
    expect(result.current.getReplicaPricing(replica)).toEqual({
      state: 'unavailable',
    });
  });

  it('retries negative rows but retains positive rows on pricing refresh', async () => {
    let rowAttempt = 0;
    getServicePricing.mockImplementation(({ replicaIds = [] }) => {
      if (replicaIds.length === 0) return pricingAggregateResult();
      rowAttempt += 1;
      return pricingRowResult([
        rowAttempt === 1
          ? {
              id: 7,
              pricingFingerprint: 'fp-7',
              hourlyCost: null,
              priceSource: null,
              hourlyCostExclusionReason: 'missing_version_catalog',
            }
          : {
              id: 7,
              pricingFingerprint: 'fp-7',
              hourlyCost: 2,
              priceSource: 'version_catalog',
              hourlyCostExclusionReason: null,
            },
      ]);
    });

    const { result } = renderHook(() =>
      useServicePricing({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
        currentPage,
      })
    );
    await waitFor(() =>
      expect(result.current.getReplicaPricing(replica).state).toBe('excluded')
    );

    await act(async () => result.current.refreshPricing());
    await waitFor(() =>
      expect(result.current.getReplicaPricing(replica)).toMatchObject({
        state: 'available',
        hourlyCost: 2,
      })
    );
    expect(rowAttempt).toBe(2);

    await act(async () => result.current.refreshPricing());
    expect(rowAttempt).toBe(2);
  });

  it('chunks only current-page missing IDs into bounded row requests', async () => {
    const replicas = Array.from({ length: 205 }, (_, index) => ({
      id: index + 1,
      pricingFingerprint: `fp-${index + 1}`,
    }));
    getServicePricing.mockImplementation(({ replicaIds = [] }) => {
      if (replicaIds.length === 0) return pricingAggregateResult();
      return pricingRowResult(
        replicaIds.map((id) => ({
          id,
          pricingFingerprint: `fp-${id}`,
          hourlyCost: 1,
          priceSource: 'version_catalog',
          hourlyCostExclusionReason: null,
        }))
      );
    });

    renderHook(() =>
      useServicePricing({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        metadataReady: true,
        currentPage: { serviceHash: 'hash-a', replicas },
      })
    );

    await waitFor(() =>
      expect(
        getServicePricing.mock.calls.filter(
          ([options]) => options.replicaIds?.length
        )
      ).toHaveLength(3)
    );
    expect(
      getServicePricing.mock.calls
        .filter(([options]) => options.replicaIds?.length)
        .map(([options]) => options.replicaIds.length)
    ).toEqual([100, 100, 5]);
  });

  it('fences a late aggregate from a previous service incarnation', async () => {
    const staleAggregate = deferred();
    getServicePricing.mockImplementation(({ serviceHash, replicaIds = [] }) => {
      if (replicaIds.length > 0) return pricingRowResult([]);
      if (serviceHash === 'hash-a') return staleAggregate.promise;
      return pricingAggregateResult({
        serviceName: 'svc-b',
        serviceHash: 'hash-b',
        aggregate: {
          ...pricingAggregateResult().aggregate,
          estimatedHourlyCost: 4,
          nonSpotHourlyCost: 4,
        },
      });
    });

    const { result, rerender } = renderHook(
      ({ serviceName, serviceHash }) =>
        useServicePricing({
          serviceName,
          serviceHash,
          metadataReady: true,
          currentPage: { serviceHash, replicas: [] },
        }),
      {
        initialProps: { serviceName: 'svc-a', serviceHash: 'hash-a' },
      }
    );
    rerender({ serviceName: 'svc-b', serviceHash: 'hash-b' });
    await waitFor(() =>
      expect(result.current.aggregate?.estimatedHourlyCost).toBe(4)
    );

    await act(async () => {
      staleAggregate.resolve(
        pricingAggregateResult({
          serviceName: 'svc-a',
          serviceHash: 'hash-a',
          aggregate: {
            ...pricingAggregateResult().aggregate,
            estimatedHourlyCost: 99,
            nonSpotHourlyCost: 99,
          },
        })
      );
      await staleAggregate.promise;
    });
    expect(result.current.aggregate.estimatedHourlyCost).toBe(4);
  });

  it('never renders pricing from an older same-name service hash', async () => {
    const pendingNewIncarnation = deferred();
    const observations = [];
    getServicePricing.mockImplementation(({ serviceHash, replicaIds = [] }) => {
      if (serviceHash === 'hash-b') return pendingNewIncarnation.promise;
      if (replicaIds.length === 0) return pricingAggregateResult();
      return pricingRowResult([
        {
          id: 7,
          pricingFingerprint: 'fp-7',
          hourlyCost: 2,
          priceSource: 'version_catalog',
          hourlyCostExclusionReason: null,
        },
      ]);
    });

    const { result, rerender } = renderHook(
      ({ serviceHash }) => {
        const pricing = useServicePricing({
          serviceName: 'svc',
          serviceHash,
          metadataReady: true,
          currentPage: { serviceHash, replicas: [replica] },
        });
        observations.push({
          serviceHash,
          aggregateCost: pricing.aggregate?.estimatedHourlyCost ?? null,
          rowCost: pricing.getReplicaPricing(replica).hourlyCost ?? null,
        });
        return pricing;
      },
      { initialProps: { serviceHash: 'hash-a' } }
    );

    await waitFor(() =>
      expect(result.current.aggregate?.estimatedHourlyCost).toBe(1.25)
    );
    await waitFor(() =>
      expect(result.current.getReplicaPricing(replica).hourlyCost).toBe(2)
    );

    observations.length = 0;
    rerender({ serviceHash: 'hash-b' });

    expect(observations.length).toBeGreaterThan(0);
    expect(observations).not.toContainEqual(
      expect.objectContaining({ aggregateCost: 1.25 })
    );
    expect(observations).not.toContainEqual(
      expect.objectContaining({ rowCost: 2 })
    );
    expect(result.current.aggregate).toBeNull();
    expect(result.current.getReplicaPricing(replica)).toEqual({
      state: 'loading',
    });
  });
});

describe('ServiceDetails route ownership rendering', () => {
  beforeEach(() => {
    jest.resetAllMocks();
    getServiceReplicaSummaries.mockResolvedValue({
      available: false,
      reason: 'unsupported',
      legacyFallback: true,
      summaries: [],
    });
    getServiceDemand.mockImplementation(({ serviceHash }) =>
      Promise.resolve({
        serviceName: 'svc',
        serviceHash,
        requestTelemetryState: 'unavailable',
        requestTelemetryReason: 'unsupported',
        legacyFallback: true,
      })
    );
    getServicePricing.mockResolvedValue(
      pricingAggregateResult({
        aggregate: {
          available: true,
          unavailableReason: null,
          coverage: 'empty',
          estimatedHourlyCost: 0,
          spotHourlyCost: 0,
          nonSpotHourlyCost: 0,
          costTrackedReplicaCount: 0,
          pricedReplicaCount: 0,
          hourlyCostExcludedReplicaCount: 0,
          hourlyCostExclusionReasons: {},
        },
      })
    );
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

  it('shows persisted request activity while controller enrichment is stalled', async () => {
    const controllerPending = deferred();
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    const now = Date.now() / 1000;
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'svc' },
    });
    dashboardCache.get.mockReturnValue(controllerPending.promise);
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          persistedMetadataLoaded: true,
          status: 'READY',
          uptime: null,
          policy: 'fixed',
          requestedResources: 'L4:1',
          replicaUnit: 'physical',
          replicaStatusCounts: {},
          replicasReady: null,
          replicasTotal: null,
          replicasFailed: 0,
          currentOrUncertainCount: 2,
          pastAttemptCount: 0,
        },
      ],
    });
    getServiceReplicas.mockResolvedValue({
      available: true,
      serviceName: 'svc',
      serviceHash: 'hash-a',
      scope: 'current_or_uncertain',
      total: 2,
      nextCursor: null,
      observedAt: 100,
      replicas: [],
    });
    getServiceDemand.mockResolvedValue({
      serviceName: 'svc',
      serviceHash: 'hash-a',
      requestTelemetryState: 'fresh',
      requestTelemetryReason: 'in_flight_incomplete',
      requestTelemetryBreakdownAvailable: true,
      requestRate: 0.5,
      recentRequestCount: 30,
      requestWindowSeconds: 60,
      inFlightRequests: null,
      confirmedInFlightRequests: 3,
      processingRequests: null,
      confirmedProcessingRequests: 2,
      httpInFlightRequests: 1,
      unknownInFlightReplicaCount: 1,
      requestQueueDepth: 1,
      rejectedRequests: 0,
      requestStatsAgeSeconds: 2,
    });
    const fullHistory = {
      ...committedPlanHistory({
        demandTarget: 240,
        observedAt: now - 1,
        validUntil: now + 30,
        windowEnd: now,
        receivedAt: now,
      }),
      available: true,
      serviceHash: 'hash-a',
      bucketSeconds: 60,
      windowStart: 0,
      windowEnd: now,
      samples: [],
      requestSamples: [],
      predictionTimeHistogramVersion: 1,
      predictionTimeLatestHourReportedAt: now - 2,
      predictionTimeBucketUpperBoundsSeconds: [1],
      predictionTimeSamples: [
        {
          timestamp: now,
          outcomeCounts: {
            succeeded: [2, 0],
            failed: [0, 1],
          },
        },
      ],
    };
    getServiceHistory.mockResolvedValue(fullHistory);

    try {
      render(<ServiceDetailsPage />);

      expect(
        await screen.findByText('2 confirmed async processing')
      ).toBeVisible();
      expect(screen.getByText('3 confirmed in flight')).toBeVisible();
      expect(screen.getByText('1 queued / unassigned')).toBeVisible();
      expect(await screen.findByText('(target: 240)')).toBeVisible();
      expect(screen.getByText('Loading replica health...')).toBeVisible();
      expect(screen.getByText('L4')).toBeVisible();
      expect(getDetailValue('Endpoint')).toHaveTextContent('Loading...');
      expect(
        screen.getByText(
          'complete reporter-set snapshot · unassigned, selecting, or retry-backoff work · 0.50 req/s recent · 30 recorded requests in 60s · 0 rejected · activity report 2s old'
        )
      ).toBeVisible();
      expect(
        getDetailValue('Compatibility terminal observations (last hour)')
      ).toHaveTextContent('3 recorded');
      expect(
        screen.getByText(
          '2 succeeded · 1 failed · latest terminal-observation report was 2s old at this history snapshot · load-balancer observations, not unique logical requests'
        )
      ).toBeVisible();

      await act(async () => {
        controllerPending.reject(new Error('provider route/status timeout'));
        await controllerPending.promise.catch(() => {});
      });
      await waitFor(() =>
        expect(getDetailValue('Endpoint')).toHaveTextContent('Unavailable')
      );
      expect(screen.getByText('(target: 240)')).toBeVisible();
      expect(screen.getByText('Replica health unavailable.')).toBeVisible();
      expect(screen.getByText('2 confirmed async processing')).toBeVisible();
      expect(screen.getByText('3 confirmed in flight')).toBeVisible();
      expect(screen.getByText('1 queued / unassigned')).toBeVisible();
      expect(getServiceDemand).toHaveBeenCalledWith({
        serviceName: 'svc',
        serviceHash: 'hash-a',
      });
      expect(dashboardCache.get).toHaveBeenCalled();
    } finally {
      consoleError.mockRestore();
    }
  });

  it('clears controller request counts when durable telemetry is unavailable', async () => {
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'svc' },
    });
    dashboardCache.get.mockResolvedValue({
      services: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          status: 'READY',
          endpoint: 'https://live.example.test',
          targetReplicas: 2,
          inFlightRequests: 2,
          confirmedInFlightRequests: 2,
          requestRate: 1,
          recentRequestCount: 60,
          requestWindowSeconds: 60,
          summaryOnly: true,
        },
      ],
    });
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          persistedMetadataLoaded: true,
          status: 'READY',
          policy: 'fixed',
          requestedResources: 'L4:1',
          replicaUnit: 'physical',
          replicaStatusCounts: { READY: 2 },
          replicasReady: 2,
          replicasTotal: 2,
          replicasFailed: 0,
          currentOrUncertainCount: 2,
          pastAttemptCount: 0,
        },
      ],
    });
    getServiceReplicas.mockResolvedValue({
      available: true,
      serviceName: 'svc',
      serviceHash: 'hash-a',
      scope: 'current_or_uncertain',
      total: 2,
      nextCursor: null,
      observedAt: 100,
      replicas: [],
    });
    getServiceDemand.mockResolvedValue({
      serviceName: 'svc',
      serviceHash: 'hash-a',
      requestTelemetryState: 'unavailable',
      requestTelemetryReason: 'no_current_reporters',
      requestRate: null,
      recentRequestCount: null,
      requestWindowSeconds: null,
      inFlightRequests: null,
      confirmedInFlightRequests: null,
      unknownInFlightReplicaCount: null,
      requestQueueDepth: null,
      rejectedRequests: null,
      legacyFallback: false,
    });

    render(<ServiceDetailsPage />);

    expect(await screen.findByText('2/2')).toBeVisible();
    await waitFor(() =>
      expect(screen.getByText('request telemetry unavailable')).toBeVisible()
    );
    expect(getDetailValue('Requests now')).toHaveTextContent('-');
    expect(
      screen.queryByText('2 tracked in flight (legacy aggregate)')
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText('2 confirmed tracked in flight (legacy aggregate)')
    ).not.toBeInTheDocument();
  });

  it('treats a modern persisted not-found response as authoritative', async () => {
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'missing-service' },
    });
    getServiceReplicaSummaries.mockResolvedValue({
      available: false,
      reason: 'not_found',
      legacyFallback: false,
      summaries: [],
    });
    dashboardCache.get.mockReturnValue(new Promise(() => {}));

    render(<ServiceDetailsPage />);

    expect(await screen.findByText('Service not found.')).toBeVisible();
    expect(dashboardCache.get).not.toHaveBeenCalled();
  });

  it('marks deferred controller enrichment unavailable after it fails', async () => {
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'svc' },
    });
    dashboardCache.get.mockRejectedValue(new Error('controller unavailable'));
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          persistedMetadataLoaded: true,
          status: 'READY',
          policy: 'fixed',
          requestedResources: 'L4:1',
          replicaUnit: 'physical',
          replicaStatusCounts: { READY: 2 },
          replicasReady: 2,
          replicasTotal: 2,
          replicasFailed: 0,
          currentOrUncertainCount: 2,
          pastAttemptCount: 0,
        },
      ],
    });
    getServiceReplicas.mockResolvedValue({
      available: true,
      serviceName: 'svc',
      serviceHash: 'hash-a',
      scope: 'current_or_uncertain',
      total: 2,
      nextCursor: null,
      observedAt: 100,
      replicas: [],
    });

    try {
      render(<ServiceDetailsPage />);

      expect(await screen.findByText('2/2')).toBeVisible();
      await waitFor(() =>
        expect(getDetailValue('Endpoint')).toHaveTextContent('Unavailable')
      );
    } finally {
      consoleError.mockRestore();
    }
  });

  it('keeps persisted lifecycle counts while accepting live endpoint enrichment', async () => {
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'svc' },
    });
    dashboardCache.get.mockResolvedValue({
      services: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          status: 'STARTING',
          endpoint: 'https://live.example.test',
          targetReplicas: 3,
          replicasReady: 1,
          replicasTotal: 1,
          replicaStatusCounts: { STARTING: 1 },
          summaryOnly: true,
        },
      ],
    });
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          persistedMetadataLoaded: true,
          status: 'READY',
          uptime: null,
          policy: 'fixed',
          requestedResources: 'L4:1',
          replicaUnit: 'physical',
          replicaStatusCounts: { READY: 2 },
          replicasReady: 2,
          replicasTotal: 2,
          replicasFailed: 0,
          currentOrUncertainCount: 2,
          pastAttemptCount: 0,
        },
      ],
    });
    getServiceReplicas.mockResolvedValue({
      available: true,
      serviceName: 'svc',
      serviceHash: 'hash-a',
      scope: 'current_or_uncertain',
      total: 2,
      nextCursor: null,
      observedAt: 100,
      replicas: [],
    });

    render(<ServiceDetailsPage />);

    expect(await screen.findByText('2/2')).toBeVisible();
    await waitFor(() => expect(screen.getByText('(target: 3)')).toBeVisible());
    expect(screen.getByText('https://live.example.test')).toBeVisible();
    expect(screen.queryByText('1/1')).not.toBeInTheDocument();
    expect(screen.getAllByText('READY').length).toBeGreaterThan(0);
    expect(screen.queryByText('STARTING')).not.toBeInTheDocument();
  });

  it('drops hashless controller enrichment for a modern service identity', async () => {
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'svc' },
    });
    dashboardCache.get.mockResolvedValue({
      services: [
        {
          name: 'svc',
          serviceHash: null,
          status: 'STARTING',
          endpoint: 'https://stale-hashless.example.test',
          targetReplicas: 99,
          replicasReady: 1,
          replicasTotal: 1,
          replicaStatusCounts: { STARTING: 1 },
          summaryOnly: true,
        },
      ],
    });
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          persistedMetadataLoaded: true,
          status: 'READY',
          policy: 'fixed',
          requestedResources: 'L4:1',
          replicaUnit: 'physical',
          replicaStatusCounts: { READY: 2 },
          replicasReady: 2,
          replicasTotal: 2,
          replicasFailed: 0,
          currentOrUncertainCount: 2,
          pastAttemptCount: 0,
        },
      ],
    });
    getServiceReplicas.mockResolvedValue({
      available: true,
      serviceName: 'svc',
      serviceHash: 'hash-a',
      scope: 'current_or_uncertain',
      total: 2,
      nextCursor: null,
      observedAt: 100,
      replicas: [],
    });

    render(<ServiceDetailsPage />);

    expect(await screen.findByText('2/2')).toBeVisible();
    expect(
      screen.queryByText('https://stale-hashless.example.test')
    ).not.toBeInTheDocument();
    expect(screen.queryByText('(target: 99)')).not.toBeInTheDocument();
    expect(getDetailValue('Endpoint')).toHaveTextContent('Loading...');
    expect(screen.queryByText('STARTING')).not.toBeInTheDocument();
  });

  it('does not retain controller identity after direct capability recovers', async () => {
    jest.useFakeTimers();
    mockUseRouter.mockReturnValue({
      isReady: true,
      query: { service: 'svc' },
    });
    getServiceReplicaSummaries
      .mockResolvedValueOnce({
        available: false,
        reason: 'unsupported',
        legacyFallback: true,
        summaries: [],
      })
      .mockResolvedValueOnce({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [],
      });
    dashboardCache.get.mockResolvedValue({
      services: [
        {
          name: 'svc',
          serviceHash: null,
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          replicas: [],
          summaryOnly: true,
        },
      ],
    });

    const { unmount } = render(<ServiceDetailsPage />);
    let mounted = true;
    try {
      expect(await screen.findByText('1/1')).toBeVisible();

      await act(async () => {
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      await waitFor(() =>
        expect(screen.getByText('Service not found.')).toBeVisible()
      );
      expect(screen.queryByText('1/1')).not.toBeInTheDocument();

      unmount();
      mounted = false;
    } finally {
      if (mounted) unmount();
      jest.useRealTimers();
    }
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
    getServiceReplicaSummaries.mockResolvedValue({
      available: true,
      serviceMetadataIncluded: true,
      summaries: [
        {
          name: 'svc',
          serviceHash: 'hash-a',
          persistedMetadataLoaded: true,
          status: 'READY',
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

    expect(dashboardCache.get).not.toHaveBeenCalled();

    routerState.query = { service: 'svc' };
    rerender(<ServiceDetailsPage />);

    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(1));
    expect(dashboardCache.get).toHaveBeenNthCalledWith(1, getServices, [
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
    expect(getServiceReplicaSummaries).toHaveBeenCalledTimes(1);
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

describe('terminal prediction observation summary', () => {
  it('reports the succeeded and failed observation breakdown', () => {
    expect(
      getTerminalPredictionObservationSummaryLastHour({
        available: true,
        bucketSeconds: 60,
        windowEnd: 7200,
        predictionTimeHistogramVersion: 1,
        predictionTimeBucketUpperBoundsSeconds: [1],
        predictionTimeSamples: [
          {
            timestamp: 7140,
            outcomeCounts: { succeeded: [2, 0], failed: [0, 1] },
          },
        ],
      })
    ).toEqual({ succeeded: 2, failed: 1, total: 3 });
  });

  it('sums only terminal outcomes in the latest 60 history buckets', () => {
    expect(
      getTerminalPredictionObservationsLastHour({
        available: true,
        bucketSeconds: 60,
        windowEnd: 7200,
        predictionTimeHistogramVersion: 1,
        predictionTimeBucketUpperBoundsSeconds: [1],
        predictionTimeSamples: [
          {
            timestamp: 3600,
            outcomeCounts: { succeeded: [9, 0] },
          },
          {
            timestamp: 7140,
            outcomeCounts: { succeeded: [2, 0], failed: [0, 1] },
          },
        ],
      })
    ).toBe(3);
  });

  it('keeps unsupported terminal history unknown instead of showing zero', () => {
    expect(
      getTerminalPredictionObservationsLastHour({
        available: true,
        windowEnd: 7200,
        predictionTimeSamples: [],
      })
    ).toBe(null);
  });

  it('derives reporter freshness from the server query boundary', () => {
    expect(
      getLatestTerminalObservationReportAgeSeconds({
        windowEnd: 7200,
        predictionTimeLatestHourReportedAt: 7197.5,
      })
    ).toBe(2.5);
    expect(
      getLatestTerminalObservationReportAgeSeconds({
        windowEnd: 7200,
        predictionTimeLatestHourReportedAt: 7201,
      })
    ).toBe(null);
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
    expect(
      screen.getByText('2 tracked in flight (legacy aggregate)')
    ).toBeTruthy();
    expect(
      screen.getByText(
        '0.50 req/s recent · 30 recorded requests in 60s · 1,234 recorded requests in last hour · 1 queued · 3 rejected · activity report 4s old'
      )
    ).toBeTruthy();
    expect(screen.getByText('$3.0556')).toBeTruthy();
    expect(screen.getByText('Known cloud compute / 1K requests')).toBeTruthy();
  });

  it('separates processing, HTTP in-flight, queue, and exact terminal telemetry', () => {
    const now = jest.spyOn(Date, 'now').mockReturnValue(200_000);
    const observedAt = 198;
    render(
      <ServiceDetailCard
        requestHistory={{ available: true, requestsLastHour: 10 }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 2,
          replicasTotal: 2,
          replicasFailed: 0,
          activeVersions: [1],
          hourlyCostExcludedReplicaCount: 0,
          requestTelemetryState: 'fresh',
          requestTelemetrySource: 'postgresql_lb_demand_reports',
          requestTelemetryBreakdownAvailable: true,
          requestTelemetryObservedAt: observedAt,
          requestStatsAgeSeconds: 2,
          requestRate: 0.5,
          recentRequestCount: 30,
          requestWindowSeconds: 60,
          processingRequests: 2,
          confirmedProcessingRequests: 2,
          httpInFlightRequests: 1,
          inFlightRequests: 3,
          confirmedInFlightRequests: 3,
          unknownInFlightReplicaCount: 0,
          requestQueueDepth: 4,
          rejectedRequests: 1,
          offeredArrivalTelemetryAvailable: true,
          uniqueJobArrivals60s: 18,
          uniqueJobArrivals300s: 25,
          headerlessArrivals60s: 2,
          headerlessArrivals300s: 5,
          offeredArrivalTrackingSaturated: true,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            observedAt: 197,
            stateCounts: {
              REJECTED_PRE_DISPATCH: 2,
              DISPATCH_MAY_HAVE_OCCURRED: 1,
              ACCEPTED: 3,
              AMBIGUOUS: 1,
              SUCCEEDED: 5,
              FAILED: 2,
              CANCELLED: 1,
              EXPIRED: 0,
            },
            operationalTerminalReceiptTotal: 8,
            operationalTerminalReceiptsByStatus: {
              SUCCEEDED: 5,
              FAILED: 2,
              CANCELLED: 1,
              EXPIRED: 0,
            },
          },
        }}
      />
    );

    expect(getDetailValue('Async processing now')).toHaveTextContent(
      '2 async processing'
    );
    expect(getDetailValue('Total in flight')).toHaveTextContent(
      '3 tracked in flight'
    );
    expect(getDetailValue('Queued now')).toHaveTextContent(
      '4 queued / unassigned'
    );
    expect(getDetailValue('Offered arrivals (60s)')).toHaveTextContent(
      '20 offered in 60s'
    );
    expect(getDetailValue('Offered arrivals (5m)')).toHaveTextContent(
      '30 offered in 5m'
    );
    expect(
      screen.getByText(/18 stable-ID \+ 2 headerless in 60s/)
    ).toHaveTextContent('tracking saturated; reported counts are lower bounds');
    expect(
      screen.getByText(/25 stable-ID \+ 5 headerless in 5m/)
    ).toHaveTextContent(
      'includes pre-admission attempts; stable IDs are deduplicated within each load-balancer window'
    );
    expect(screen.queryByText('Requests now')).toBeNull();
    expect(
      screen.getByText(
        'complete routed-backend occupancy · async backend occupancy only'
      )
    ).toBeVisible();
    expect(
      screen.getByText(
        /1 HTTP envelope \+ 2 confirmed async processing · conservative sum/
      )
    ).toBeVisible();
    const queueTelemetry = screen.getByText(/complete reporter-set snapshot/);
    expect(queueTelemetry).toHaveTextContent(
      'unassigned, selecting, or retry-backoff work'
    );
    expect(queueTelemetry).toHaveTextContent('activity report 2s old');
    expect(queueTelemetry).toHaveTextContent(
      `observed ${formatFullTimestamp(new Date(observedAt * 1000))}`
    );
    expect(queueTelemetry).toHaveTextContent(
      'source PostgreSQL load-balancer reports'
    );
    expect(
      getDetailValue('Succeeded / terminal (exact async)')
    ).toHaveTextContent('5 succeeded / 8 terminal, protocol-covered (partial)');
    expect(
      screen.getByText(/protocol-uncovered request count unknown/)
    ).toBeVisible();
    now.mockRestore();
  });

  it('labels split occupancy as partial without fabricating unknown work', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 5,
          replicasTotal: 5,
          replicasFailed: 0,
          activeVersions: [1],
          hourlyCostExcludedReplicaCount: 0,
          requestTelemetryState: 'fresh',
          requestTelemetryBreakdownAvailable: true,
          requestStatsAgeSeconds: 2,
          processingRequests: null,
          confirmedProcessingRequests: 0,
          httpInFlightRequests: 1,
          inFlightRequests: null,
          confirmedInFlightRequests: 1,
          unknownInFlightReplicaCount: 5,
          requestQueueDepth: 0,
        }}
      />
    );

    expect(getDetailValue('Async processing now')).toHaveTextContent(
      '0 confirmed async processing'
    );
    expect(getDetailValue('Total in flight')).toHaveTextContent(
      '1 confirmed in flight'
    );
    expect(getDetailValue('Queued now')).toHaveTextContent(
      '0 queued / unassigned'
    );
    expect(
      screen.getByText(
        /partial routed-backend occupancy; 5 backends with unknown occupancy/
      )
    ).toBeVisible();
    expect(
      screen.getByText(/partial lower bound; 5 backends with unknown occupancy/)
    ).toBeVisible();
    expect(screen.queryByText(/5 unobserved requests/)).toBeNull();
  });

  it('does not label synchronous HTTP work as async processing', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          activeVersions: [1],
          hourlyCostExcludedReplicaCount: 0,
          requestTelemetryState: 'fresh',
          requestTelemetryBreakdownAvailable: true,
          processingRequests: 0,
          confirmedProcessingRequests: 0,
          httpInFlightRequests: 1,
          inFlightRequests: 1,
          confirmedInFlightRequests: 1,
          unknownInFlightReplicaCount: 0,
          requestQueueDepth: 0,
        }}
      />
    );

    expect(getDetailValue('Async processing now')).toHaveTextContent(
      '0 async processing'
    );
    expect(getDetailValue('Total in flight')).toHaveTextContent(
      '1 tracked in flight'
    );
    expect(
      screen.getByText(/1 HTTP envelope \+ 0 confirmed async processing/)
    ).toBeVisible();
  });

  it('labels completion count freshness as unavailable on an old response', () => {
    render(
      <ServiceDetailCard
        requestHistory={{
          available: true,
          bucketSeconds: 60,
          windowEnd: 7200,
          predictionTimeHistogramVersion: 1,
          predictionTimeBucketUpperBoundsSeconds: [1],
          predictionTimeSamples: [
            {
              timestamp: 7140,
              outcomeCounts: { succeeded: [2, 0], failed: [0, 1] },
            },
          ],
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
        }}
      />
    );

    expect(
      getDetailValue('Compatibility terminal observations (last hour)')
    ).toHaveTextContent('3 recorded');
    expect(
      screen.getByText(
        '2 succeeded · 1 failed · latest terminal-observation report time unavailable · load-balancer observations, not unique logical requests'
      )
    ).toBeVisible();
  });

  it('labels an N-1 combined gauge as a legacy in-flight aggregate', () => {
    render(
      <ServiceDetailCard
        requestHistory={{
          available: true,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            stateCounts: {
              REJECTED_PRE_DISPATCH: 2,
              DISPATCH_MAY_HAVE_OCCURRED: 1,
              ACCEPTED: 3,
              AMBIGUOUS: 1,
              SUCCEEDED: 5,
              FAILED: 2,
              CANCELLED: 1,
              EXPIRED: 0,
            },
            operationalTerminalReceiptTotal: 8,
            operationalTerminalReceiptsByStatus: {
              SUCCEEDED: 5,
              FAILED: 2,
              CANCELLED: 1,
              EXPIRED: 0,
            },
          },
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
          inFlightRequests: 6,
        }}
      />
    );

    expect(getDetailValue('Requests now')).toHaveTextContent(
      '6 tracked in flight (legacy aggregate)'
    );
    expect(screen.queryByText('Async processing now')).toBeNull();
    expect(
      getDetailValue('Succeeded / terminal (exact async)')
    ).toHaveTextContent('5 succeeded / 8 terminal, protocol-covered (partial)');
    expect(
      screen.getByText(
        '2 rejected before dispatch · 3 accepted / dispatch-confirmed (not proven actively processing) · 1 dispatch may have occurred · 1 ambiguous · 5 succeeded · 2 failed · 1 cancelled · 0 expired · coverage partial; protocol-uncovered request count unknown'
      )
    ).toBeVisible();
  });

  it('keeps legacy processing telemetry when exact coverage is partial', () => {
    render(
      <ServiceDetailCard
        requestHistory={{
          available: true,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            stateCounts: {
              REJECTED_PRE_DISPATCH: 0,
              DISPATCH_MAY_HAVE_OCCURRED: 1,
              ACCEPTED: 2,
              AMBIGUOUS: 0,
              SUCCEEDED: 1,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
            operationalTerminalReceiptTotal: 1,
            operationalTerminalReceiptsByStatus: {
              SUCCEEDED: 1,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
          },
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
          inFlightRequests: 7,
        }}
      />
    );

    expect(getDetailValue('Requests now')).toHaveTextContent(
      '7 tracked in flight (legacy aggregate)'
    );
    expect(
      getDetailValue('Succeeded / terminal (exact async)')
    ).toHaveTextContent('1 succeeded / 1 terminal, protocol-covered (partial)');
    expect(
      screen.getByText(
        '0 rejected before dispatch · 2 accepted / dispatch-confirmed (not proven actively processing) · 1 dispatch may have occurred · 0 ambiguous · 1 succeeded · 0 failed · 0 cancelled · 0 expired · coverage partial; protocol-uncovered request count unknown'
      )
    ).toBeVisible();
  });

  it('marks a retained completion snapshot stale after refresh failure', () => {
    render(
      <ServiceDetailCard
        requestHistory={{
          available: true,
          refreshUnavailable: true,
          bucketSeconds: 60,
          windowEnd: 7200,
          predictionTimeHistogramVersion: 1,
          predictionTimeLatestHourReportedAt: 7198,
          predictionTimeBucketUpperBoundsSeconds: [1],
          predictionTimeSamples: [
            {
              timestamp: 7140,
              outcomeCounts: { succeeded: [1, 0] },
            },
          ],
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
        }}
      />
    );

    expect(
      screen.getByText(
        '1 succeeded · 0 failed · history refresh failed; showing the last persisted one-hour snapshot · load-balancer observations, not unique logical requests'
      )
    ).toBeVisible();
  });

  it('shows exact ledger snapshot age and retained-refresh failure', () => {
    const now = jest.spyOn(Date, 'now').mockReturnValue(200_000);
    render(
      <ServiceDetailCard
        requestHistory={{
          available: true,
          refreshUnavailable: true,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            observedAt: 100,
            stateCounts: {
              REJECTED_PRE_DISPATCH: 0,
              DISPATCH_MAY_HAVE_OCCURRED: 0,
              ACCEPTED: 0,
              AMBIGUOUS: 0,
              SUCCEEDED: 1,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
            operationalTerminalReceiptTotal: 1,
            operationalTerminalReceiptsByStatus: {
              SUCCEEDED: 1,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
          },
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
        }}
      />
    );

    expect(
      screen.getByText(
        'Exact ledger refresh failed; showing a snapshot observed 100s ago.'
      )
    ).toBeVisible();
    now.mockRestore();
  });

  it('prefers the direct PostgreSQL summary and keeps its freshness separate from stale load-balancer reports', () => {
    const now = jest.spyOn(Date, 'now').mockReturnValue(200_000);
    const stateCounts = {
      REJECTED_PRE_DISPATCH: 0,
      DISPATCH_MAY_HAVE_OCCURRED: 0,
      ACCEPTED: 2,
      AMBIGUOUS: 0,
      SUCCEEDED: 1,
      FAILED: 0,
      CANCELLED: 0,
      EXPIRED: 0,
    };
    const terminalByStatus = {
      SUCCEEDED: 1,
      FAILED: 0,
      CANCELLED: 0,
      EXPIRED: 0,
    };
    render(
      <ServiceDetailCard
        requestHistory={{
          available: true,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            observedAt: 50,
            stateCounts: { ...stateCounts, SUCCEEDED: 9 },
            operationalTerminalReceiptTotal: 9,
            operationalTerminalReceiptsByStatus: {
              ...terminalByStatus,
              SUCCEEDED: 9,
            },
          },
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
          requestTelemetryState: 'stale',
          requestTelemetryReason: 'reports_stale',
          requestTelemetrySource: 'postgresql_lb_demand_reports',
          requestQueueDepth: 3,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            observedAt: 100,
            stateCounts,
            operationalTerminalReceiptTotal: 1,
            operationalTerminalReceiptsByStatus: terminalByStatus,
          },
        }}
      />
    );

    expect(
      getDetailValue('Succeeded / terminal (exact async)')
    ).toHaveTextContent('1 succeeded / 1 terminal, protocol-covered (partial)');
    expect(
      screen.getByText('PostgreSQL exact ledger snapshot observed 100s ago.')
    ).toBeVisible();
    expect(
      screen.getByText(
        'last reported 3 queued · source PostgreSQL load-balancer reports · request telemetry stale'
      )
    ).toBeVisible();
    now.mockRestore();
  });

  it('marks a retained direct PostgreSQL summary stale after demand refresh failure', () => {
    const now = jest.spyOn(Date, 'now').mockReturnValue(200_000);
    render(
      <ServiceDetailCard
        requestHistory={{ available: true }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
          requestTelemetryState: 'stale',
          requestTelemetryReason: 'dashboard_refresh_failed',
          requestTelemetryRefreshUnavailable: true,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            observedAt: 100,
            stateCounts: {
              REJECTED_PRE_DISPATCH: 0,
              DISPATCH_MAY_HAVE_OCCURRED: 0,
              ACCEPTED: 0,
              AMBIGUOUS: 0,
              SUCCEEDED: 1,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
            operationalTerminalReceiptTotal: 1,
            operationalTerminalReceiptsByStatus: {
              SUCCEEDED: 1,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
          },
        }}
      />
    );

    expect(
      screen.getByText(
        'PostgreSQL exact ledger refresh failed; showing a snapshot observed 100s ago.'
      )
    ).toHaveClass('text-amber-700');
    now.mockRestore();
  });

  it('does not fall back to loading or history after the direct ledger read fails closed', () => {
    render(
      <ServiceDetailCard
        historyLoading
        requestHistory={{
          available: true,
          asyncRequestSummary: {
            available: true,
            coverage: 'partial',
            stateCounts: {
              REJECTED_PRE_DISPATCH: 0,
              DISPATCH_MAY_HAVE_OCCURRED: 0,
              ACCEPTED: 0,
              AMBIGUOUS: 0,
              SUCCEEDED: 9,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
            operationalTerminalReceiptTotal: 9,
            operationalTerminalReceiptsByStatus: {
              SUCCEEDED: 9,
              FAILED: 0,
              CANCELLED: 0,
              EXPIRED: 0,
            },
          },
        }}
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 1,
          replicasTotal: 1,
          replicasFailed: 0,
          targetReplicas: 1,
          activeVersions: [1],
          asyncRequestSummary: {
            available: false,
            source: 'postgresql_async_request_ledger',
            reason: 'schema_unavailable',
          },
        }}
      />
    );

    expect(
      getDetailValue('Succeeded / terminal (exact async)')
    ).toHaveTextContent('Unavailable');
    expect(
      screen.getByText(
        'PostgreSQL exact ledger unavailable: schema unavailable'
      )
    ).toBeVisible();
    expect(
      getDetailValue('Succeeded / terminal (exact async)')
    ).not.toHaveTextContent('9 terminal');
  });

  it('labels partial version-catalog totals as a known lower bound', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 3,
          replicasTotal: 3,
          replicasFailed: 0,
          targetReplicas: 3,
          activeVersions: [1, 2],
          priceBasis: 'version_catalog',
          pricingCoverage: 'partial',
          estimatedHourlyCost: 2,
          spotHourlyCost: 0.5,
          nonSpotHourlyCost: 1.5,
          costTrackedReplicaCount: 3,
          pricedReplicaCount: 2,
          hourlyCostExcludedReplicaCount: 1,
          hourlyCostExclusionReasons: { missing_version_catalog: 1 },
          requestRate: 1,
          costPerThousandRequests: 0.555555,
        }}
      />
    );

    expect(screen.getByText('$2.00+/hr')).toBeVisible();
    expect(
      screen.getByText(
        "Spot $0.50/hr · Non-Spot $1.50/hr · 1 unpriced replica · 3 active, stopping, or cleanup-uncertain replicas · Each replica version's deployment catalog; reserved $0 from persisted placement provenance; compute estimate, not a provider bill"
      )
    ).toBeVisible();
    expect(screen.getByText('$0.5556+')).toBeVisible();
    expect(
      screen.getByText(
        'Known lower bound at the recent request rate · 1 missing version catalog replica excluded'
      )
    ).toBeVisible();
  });

  it('renders empty version-catalog coverage as an explicit zero', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 0,
          replicasTotal: 0,
          replicasFailed: 0,
          targetReplicas: 0,
          activeVersions: [1],
          priceBasis: 'version_catalog',
          pricingCoverage: 'empty',
          estimatedHourlyCost: 0,
          spotHourlyCost: 0,
          nonSpotHourlyCost: 0,
          costTrackedReplicaCount: 0,
          pricedReplicaCount: 0,
          hourlyCostExcludedReplicaCount: 0,
          hourlyCostExclusionReasons: {},
          requestRate: 0,
          costPerThousandRequests: null,
        }}
      />
    );

    expect(screen.getByText('$0.00/hr')).toBeVisible();
    expect(
      screen.getByText(
        'No cost-tracked replicas; current tracked compute cost is $0.'
      )
    ).toBeVisible();
    expect(
      screen.getByText(
        "Each replica version's deployment catalog; reserved $0 from persisted placement provenance; compute estimate, not a provider bill"
      )
    ).toBeVisible();
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
      screen.getByText('Fleet-wide logical slots (ready/non-failed)')
    ).toBeTruthy();
    expect(
      screen.getByText(/1\/2 physical backends \(ready\/non-failed\)/)
    ).toBeTruthy();
    expect(screen.getByText(/1 past attempt retained/)).toBeTruthy();
    expect(screen.queryByText(/failed or cleanup-uncertain/)).toBeNull();
    expect(screen.queryByText('Replicas (ready/non-failed)')).toBeNull();
    expect(
      screen.getByText(/not a GPU count for the current cluster/)
    ).toBeTruthy();
  });

  it('separates a stale load-balancer observation from current replica state', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'boltz-l4-fleet',
          status: 'READY',
          uptime: null,
          replicaUnit: 'logical',
          replicasReady: 62,
          replicasTotal: 64,
          replicasFailed: 3911,
          physicalReplicasReady: 62,
          physicalReplicasTotal: 64,
          physicalReplicasFailed: 3911,
          observedReadyReplicas: 262,
          observedReadyReplicasFresh: false,
          requestStatsAgeSeconds: 700,
          replicaStatusCounts: { READY: 62, PROVISIONING: 2, UNKNOWN: 3828 },
          targetReplicas: 0,
          endpoint: null,
          policy: 'autoscaling',
          loadBalancingPolicy: 'instance_aware_least_load',
          requestedResources: 'H200:1',
          activeVersions: [58],
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
          costPerThousandRequests: null,
        }}
      />
    );

    expect(screen.getByText('62/64')).toBeTruthy();
    expect(
      screen.getByText(/last load-balancer observation \(262 logical slots\)/)
    ).toBeTruthy();
    expect(screen.getAllByText(/700s old/)).toHaveLength(2);
  });

  it('warns when controller-recorded logical slots are not routing-ready', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'boltz-l4-fleet',
          status: 'REPLICA_INIT',
          uptime: null,
          replicaUnit: 'logical',
          replicasReady: 279,
          replicasTotal: 288,
          replicasFailed: 0,
          physicalReplicasReady: 279,
          physicalReplicasTotal: 288,
          physicalReplicasFailed: 0,
          replicaStatusCounts: { READY: 279, PROVISIONING: 9 },
          targetReplicas: 0,
          endpoint: null,
          policy: 'autoscaling',
          loadBalancingPolicy: 'instance_aware_least_load',
          requestedResources: 'H200:1',
          activeVersions: [58],
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

    expect(screen.getByText('Routing unverified')).toBeTruthy();
    expect(
      screen.getByText(
        /These are controller-recorded slots, not confirmed live GPU endpoints/
      )
    ).toBeTruthy();
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
          requestRate: 0.5,
          recentRequestCount: 30,
          requestWindowSeconds: 60,
          inFlightRequests: 2,
          requestQueueDepth: 1,
          rejectedRequests: 0,
          requestStatsAgeSeconds: null,
          requestTelemetryState: 'stale',
          requestTelemetryReason: 'dashboard_refresh_failed',
          costPerThousandRequests: null,
        }}
      />
    );

    expect(screen.queryByText('0 recorded requests in last hour')).toBeNull();
    expect(
      screen.getByText('2 last reported tracked in flight (legacy aggregate)')
    ).toBeTruthy();
    expect(
      screen.getByText(
        'last reported 0.50 req/s · last reported 30 recorded requests in 60s · last reported 1 queued · last reported 0 rejected · request telemetry stale; showing last persisted snapshot'
      )
    ).toBeTruthy();
  });

  it('renders fresh zero as an observed request-processing count', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 0,
          replicasTotal: 0,
          replicasFailed: 0,
          activeVersions: [1],
          hourlyCostExcludedReplicaCount: 0,
          requestRate: 0,
          recentRequestCount: 0,
          requestWindowSeconds: 60,
          inFlightRequests: 0,
          requestQueueDepth: 0,
          rejectedRequests: 0,
          requestStatsAgeSeconds: 2,
          requestTelemetryState: 'fresh',
          zeroCostActuationStatus: 'available',
          zeroCostActuationMode: 'DURABLE_INTENT',
          zeroCostActuationEpoch: 3,
          pendingZeroCostActuationCount: 3,
          zeroCostActuationStateCounts: {
            GRANTED: 1,
            ACTUATING: 1,
            COMMITTED: 4,
            RETRYABLE: 1,
            TERMINAL: 2,
          },
          costPerThousandRequests: null,
        }}
      />
    );

    expect(screen.getByText('Requests now')).toBeTruthy();
    expect(
      screen.getByText('0 tracked in flight (legacy aggregate)')
    ).toBeTruthy();
    expect(
      screen.getByText(
        '0.00 req/s recent · 0 recorded requests in 60s · 0 queued · 0 rejected · activity report 2s old'
      )
    ).toBeTruthy();
    expect(screen.getByText('Reserved fill grants')).toBeTruthy();
    expect(screen.getByText('3 pending before replica rows')).toBeTruthy();
    expect(
      screen.getByText(
        'durable intent · epoch 3 · 1 granted · 1 actuating · 1 retryable · 4 committed · 2 terminal'
      )
    ).toBeTruthy();
    expect(screen.queryByText(/telemetry stale/)).toBeNull();
  });

  it('renders a proven lower bound when backend occupancy is incomplete', () => {
    render(
      <ServiceDetailCard
        serviceData={{
          name: 'svc',
          status: 'READY',
          replicasReady: 5,
          replicasTotal: 5,
          replicasFailed: 0,
          activeVersions: [1],
          hourlyCostExcludedReplicaCount: 0,
          requestRate: 0,
          recentRequestCount: 0,
          requestWindowSeconds: 60,
          inFlightRequests: null,
          confirmedInFlightRequests: 0,
          unknownInFlightReplicaCount: 5,
          requestQueueDepth: 0,
          rejectedRequests: 0,
          requestStatsAgeSeconds: 2,
          requestTelemetryState: 'fresh',
          requestTelemetryReason: 'in_flight_incomplete',
          costPerThousandRequests: null,
        }}
      />
    );

    expect(
      screen.getByText('0 confirmed tracked in flight (legacy aggregate)')
    ).toBeTruthy();
    expect(
      screen.getByText(
        '0.00 req/s recent · 0 recorded requests in 60s · 0 queued · 5 backends with unknown occupancy · 0 rejected · activity report 2s old'
      )
    ).toBeTruthy();
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

  it('labels reserved zero-cost current rows and never prices past attempts', async () => {
    const user = userEvent.setup();
    render(
      <ReplicasCard
        replicas={[
          {
            id: 7,
            status: 'READY',
            version: 2,
            directProjection: true,
            hourlyCost: 0,
            priceSource: 'zero_cost_provenance',
          },
        ]}
        loading={false}
        currentTotal={1}
        pastReplicas={[
          {
            id: 6,
            status: 'FAILED_PROVISION',
            version: 1,
            directProjection: true,
            hourlyCost: 8,
            priceSource: 'version_catalog',
          },
        ]}
        pastTotal={1}
      />
    );

    expect(screen.getByText('$0.00 reserved')).toBeVisible();
    await user.click(screen.getByText('Past attempts (1)'));
    expect(screen.getByText('Not available')).toBeVisible();
    expect(screen.queryByText('$8.00')).not.toBeInTheDocument();
  });
});

describe('committed capacity-plan lease clock', () => {
  it('fails the projection closed to STALE when the DB-relative lease passes before the next poll', async () => {
    jest.useFakeTimers();
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    const controllerPending = deferred();
    try {
      const now = Date.now() / 1000;
      mockUseRouter.mockReturnValue({
        isReady: true,
        query: { service: 'svc' },
      });
      dashboardCache.get.mockReturnValue(controllerPending.promise);
      getServiceReplicaSummaries.mockResolvedValue({
        available: true,
        serviceMetadataIncluded: true,
        summaries: [
          {
            name: 'svc',
            serviceHash: 'hash-a',
            persistedMetadataLoaded: true,
            status: 'READY',
            uptime: null,
            policy: 'fixed',
            requestedResources: 'L4:1',
            replicaUnit: 'physical',
            replicaStatusCounts: {},
            replicasReady: null,
            replicasTotal: null,
            replicasFailed: 0,
            currentOrUncertainCount: 2,
            pastAttemptCount: 0,
          },
        ],
      });
      getServiceReplicas.mockResolvedValue({
        available: true,
        serviceName: 'svc',
        serviceHash: 'hash-a',
        scope: 'current_or_uncertain',
        total: 2,
        nextCursor: null,
        observedAt: now,
        replicas: [],
      });
      getServiceDemand.mockResolvedValue({
        serviceName: 'svc',
        serviceHash: 'hash-a',
        requestTelemetryState: 'unavailable',
        requestTelemetryReason: 'unsupported',
        legacyFallback: true,
      });
      getServicePricing.mockResolvedValue(
        pricingAggregateResult({
          aggregate: {
            available: true,
            unavailableReason: null,
            coverage: 'empty',
            estimatedHourlyCost: 0,
            spotHourlyCost: 0,
            nonSpotHourlyCost: 0,
            costTrackedReplicaCount: 0,
            pricedReplicaCount: 0,
            hourlyCostExcludedReplicaCount: 0,
            hourlyCostExclusionReasons: {},
          },
        })
      );
      // The plan lease outlives the read by six seconds, shorter than the
      // ten-second history poll: expiry must be applied by the local lease
      // clock, not by the next fetch.
      getServiceHistory.mockResolvedValue({
        ...committedPlanHistory({
          demandTarget: 240,
          observedAt: now - 1,
          validUntil: now + 6,
          windowEnd: now,
          receivedAt: now,
        }),
        bucketSeconds: 60,
        windowStart: 0,
        samples: [],
        requestSamples: [],
        predictionTimeHistogramVersion: 1,
        predictionTimeLatestHourReportedAt: null,
        predictionTimeBucketUpperBoundsSeconds: [1],
        predictionTimeSamples: [],
      });

      render(<ServiceDetailsPage />);

      expect(await screen.findByText('(target: 240)')).toBeVisible();
      expect(screen.queryByText(/Current committed plan stale/)).toBeNull();

      await act(async () => {
        jest.advanceTimersByTime(4 * 1000);
      });
      expect(screen.getByText('(target: 240)')).toBeVisible();

      await act(async () => {
        jest.advanceTimersByTime(3 * 1000);
      });
      expect(
        screen.getByText(
          /Current committed plan stale: capacity_plan_expired\. Planned values are unavailable, not zero\./
        )
      ).toBeVisible();
      expect(screen.queryByText('(target: 240)')).toBeNull();
      expect(getServiceHistory).toHaveBeenCalledTimes(1);
    } finally {
      consoleError.mockRestore();
      jest.useRealTimers();
    }
  });
});
