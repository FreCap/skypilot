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

import dashboardCache from '@/lib/cache';
import { getServices } from '@/data/connectors/services';
import {
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

describe('useServiceDetails stale-response fencing', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('ignores an earlier refresh cycle that resolves after a manual refresh', async () => {
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
    expect(dashboardCache.get.mock.calls[0][1][0]).toMatchObject({
      historyHours: 24,
    });

    let refreshPromise;
    await act(async () => {
      refreshPromise = result.current.refreshData();
    });

    await waitFor(() =>
      expect(dashboardCache.invalidateFunction).toHaveBeenCalledWith(
        getServices
      )
    );
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(4));

    await act(async () => {
      refreshedSummary.resolve({
        services: [{ name: 'svc', status: 'fresh-summary', summaryOnly: true }],
      });
      await Promise.resolve();
    });
    expect(result.current.serviceData.status).toBe('fresh-summary');

    await act(async () => {
      initialFull.resolve({
        services: [{ name: 'svc', status: 'stale-full', replicas: ['old'] }],
      });
      await Promise.resolve();
    });
    expect(result.current.serviceData.status).toBe('fresh-summary');

    await act(async () => {
      refreshedFull.resolve({
        services: [{ name: 'svc', status: 'fresh-full', replicas: ['new'] }],
      });
      await refreshPromise;
    });

    expect(result.current.serviceData.status).toBe('fresh-full');
    expect(result.current.serviceData.replicas).toEqual(['new']);
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

  it('does not overlap history refreshes at the fixed polling cadence', async () => {
    jest.useFakeTimers();
    const historyRefresh = deferred();
    dashboardCache.get
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'READY' }],
      })
      .mockResolvedValueOnce({
        services: [{ name: 'svc', status: 'READY', replicas: [] }],
      })
      .mockImplementation(() => historyRefresh.promise);

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
        jest.advanceTimersByTime(60 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000 + 30 * 1000);
        await Promise.resolve();
      });

      // A slow history query must not accumulate one new request per timer
      // boundary. Apart from the initial summary and full fetches, only one
      // automatic history request may be pending.
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      await act(async () => {
        historyRefresh.resolve({
          services: [
            { name: 'svc', replicaHistory: { currentReadyReplicas: 1 } },
          ],
        });
        await Promise.resolve();
      });

      await act(async () => {
        jest.advanceTimersByTime(30 * 1000 - 1);
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      await act(async () => {
        jest.advanceTimersByTime(1);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);

      unmount();
      mounted = false;
      await act(async () => {
        jest.advanceTimersByTime(2 * 60 * 1000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(4);
    } finally {
      if (mounted) {
        unmount();
      }
      jest.useRealTimers();
    }
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
    expect(
      screen.getByText(
        'Spot $1.50/hr · On-demand $4.00/hr · Current catalog, compute only'
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

    expect(screen.getByText('Logical replicas (ready/total)')).toBeTruthy();
    expect(screen.getByText(/1\/2 physical backends ready/)).toBeTruthy();
    expect(screen.queryByText('Replicas (ready/total)')).toBeNull();
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
      status: 'STARTING',
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
        pending: 1,
        provisioning: 0,
        initializing: 0,
        ready: 1,
        notReady: 0,
        stopping: 0,
        error: 0,
        other: 0,
        total: 2,
      },
      {
        cloud: 'GCP',
        region: 'us-central1',
        pending: 0,
        provisioning: 0,
        initializing: 0,
        ready: 0,
        notReady: 1,
        stopping: 1,
        error: 1,
        other: 0,
        total: 3,
      },
      {
        cloud: 'Kubernetes',
        region: 'research-context',
        pending: 0,
        provisioning: 1,
        initializing: 1,
        ready: 1,
        notReady: 0,
        stopping: 0,
        error: 1,
        other: 0,
        total: 4,
      },
      {
        cloud: 'Unknown',
        region: 'Pending placement',
        pending: 0,
        provisioning: 0,
        initializing: 0,
        ready: 0,
        notReady: 0,
        stopping: 0,
        error: 0,
        other: 1,
        total: 1,
      },
    ]);
  });

  it('renders one row per provider and region after machines load', () => {
    render(<ReplicaPlacementCard replicas={replicas} loading={false} />);

    expect(screen.getByText('Machines by region')).toBeTruthy();
    const researchRow = screen.getByText('research-context').closest('tr');
    expect(
      within(researchRow)
        .getAllByRole('cell')
        .map((cell) => cell.textContent)
    ).toEqual([
      'Kubernetes',
      'research-context',
      '0',
      '1',
      '1',
      '1',
      '0',
      '0',
      '1',
      '0',
      '4',
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
