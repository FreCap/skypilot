import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  within,
  waitFor,
} from '@testing-library/react';

import {
  filterPlacementLocations,
  formatAccelerators,
  formatHourlyPrice,
  formatPlacementHourlyPrice,
  LOCATION_AVAILABILITY,
  locationAvailability,
  locationDisplayStatus,
  ServicePlacement,
} from './service-placement';
import { getServicePlacement } from '@/data/connectors/services';

jest.mock('@/data/connectors/services', () => ({
  getServicePlacement: jest.fn(),
}));

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, reject, resolve };
}

const ORDER_GENERATION_A = 'a'.repeat(64);
const ORDER_GENERATION_B = 'b'.repeat(64);

const placement = {
  serviceName: 'svc',
  placerState: {
    available: true,
    enabled: true,
    retrySeconds: 600,
    paginationVersion: 2,
    pageOffset: 0,
    costUnit: 'gpu_slot_hour',
    orderSemantics: 'catalog_normalized_cost_then_location_identity',
    orderGeneration: ORDER_GENERATION_A,
    locations: [
      {
        cloud: 'GCP',
        region: 'us-central1',
        zone: 'us-central1-a',
        instanceType: 'g2-standard-4',
        accelerators: { L4: 1 },
        useSpot: true,
        storedStatus: 'ACTIVE',
        effectiveStatus: 'ACTIVE',
        probeEligible: false,
        benchedAt: null,
        nextProbeAt: null,
        cachedHourlyCost: 0.35,
        normalizedHourlyCost: 0.35,
      },
      {
        cloud: 'AWS',
        region: 'us-east-1',
        zone: 'us-east-1a',
        instanceType: 'g6.xlarge',
        accelerators: { L4: 1 },
        useSpot: true,
        storedStatus: 'PREEMPTED',
        effectiveStatus: 'ACTIVE',
        benchReason: 'quota',
        probeEligible: true,
        benchedAt: 1000,
        nextProbeAt: 1600,
        cachedHourlyCost: 0.45,
        normalizedHourlyCost: 0.45,
        paidAdmission: {
          state: 'probe',
          poolRemaining: 1,
          serviceRemaining: 12,
        },
      },
      {
        cloud: 'AWS',
        region: 'us-east-1',
        zone: 'us-east-1c',
        instanceType: 'p5.48xlarge',
        accelerators: { H100: 1 },
        useSpot: true,
        storedStatus: 'PREEMPTED',
        effectiveStatus: 'PREEMPTED',
        probeEligible: false,
        benchedAt: 1000,
        nextProbeAt: 9999999999,
        cachedHourlyCost: 2.5,
        normalizedHourlyCost: 2.5,
      },
      {
        cloud: 'AWS',
        region: 'us-east-1',
        zone: 'us-east-1b',
        instanceType: 'g6.2xlarge',
        accelerators: { A100: 1 },
        useSpot: false,
        storedStatus: 'ACTIVE',
        effectiveStatus: 'ACTIVE',
        probeEligible: false,
        benchedAt: null,
        nextProbeAt: null,
        cachedHourlyCost: 3.25,
        normalizedHourlyCost: 3.25,
      },
    ],
    truncated: false,
    nextOffset: null,
    totalLocations: 4,
  },
  capacityHints: {
    available: true,
    hints: [
      {
        kind: 'capacity',
        cloud: 'gcp',
        region: 'asia-northeast3',
        zone: 'asia-northeast3-b',
        instanceType: 'g2-standard-4',
        accelerators: 'L4:1',
        numNodes: 1,
        expiresAt: 1120,
      },
    ],
    truncated: false,
  },
  history: {
    available: true,
    outcomeCounts: { capacity_failed: 1 },
    events: [],
    nextCursor: null,
  },
};

beforeEach(() => {
  jest.clearAllMocks();
  getServicePlacement.mockResolvedValue(placement);
});

it('formats shape and retry state', () => {
  expect(formatAccelerators({ A100: 2, L4: 1 })).toBe('A100:2, L4:1');
  expect(formatAccelerators({})).toBe('-');
  expect(formatHourlyPrice(1.25)).toBe('$1.2500/hr');
  expect(formatHourlyPrice(null)).toBe('Price unavailable');
  expect(formatPlacementHourlyPrice(0.125, 'gpu_slot_hour')).toBe(
    '$0.1250/GPU-hr'
  );
  expect(formatPlacementHourlyPrice(0.5, null)).toBe('$0.5000/ordering-unit');
  expect(
    locationDisplayStatus({
      storedStatus: 'PREEMPTED',
      probeEligible: true,
    })
  ).toBe('Probe eligible');
  expect(
    locationDisplayStatus({
      storedStatus: 'PREEMPTED',
      probeEligible: false,
    })
  ).toBe('Benched');
  expect(
    locationAvailability({ effectiveStatus: 'ACTIVE', useSpot: true })
  ).toBe(LOCATION_AVAILABILITY.AVAILABLE_SPOT);
  expect(
    locationAvailability({ effectiveStatus: 'ACTIVE', useSpot: false })
  ).toBe(LOCATION_AVAILABILITY.AVAILABLE_ON_DEMAND);
  expect(
    locationAvailability({ effectiveStatus: 'PREEMPTED', useSpot: true })
  ).toBe(LOCATION_AVAILABILITY.UNAVAILABLE);
});

it('filters candidates without changing the server placement order', () => {
  const filtered = filterPlacementLocations(placement.placerState.locations, {
    provider: 'all',
    region: 'all',
    card: 'L4',
    availability: LOCATION_AVAILABILITY.AVAILABLE_SPOT,
    maxPrice: '0.4',
  });
  expect(filtered).toHaveLength(1);
  expect(filtered[0]).toMatchObject({
    cloud: 'GCP',
    region: 'us-central1',
    normalizedHourlyCost: 0.35,
  });

  const normalizedCheaperThanMachine = {
    ...placement.placerState.locations[0],
    cachedHourlyCost: 0.8,
    normalizedHourlyCost: 0.2,
  };
  expect(
    filterPlacementLocations([normalizedCheaperThanMachine], {
      provider: 'all',
      region: 'all',
      card: 'all',
      availability: 'all',
      maxPrice: '0.4',
    })
  ).toEqual([normalizedCheaperThanMachine]);
});

it('does not claim cost ordering for an older controller response', async () => {
  getServicePlacement.mockResolvedValueOnce({
    ...placement,
    placerState: {
      ...placement.placerState,
      costUnit: null,
      orderSemantics: null,
      locations: placement.placerState.locations.map((location) => ({
        ...location,
        normalizedHourlyCost: null,
      })),
    },
  });

  render(<ServicePlacement serviceName="svc" />);

  expect(
    await screen.findByText(/does not report a normalized catalog-cost order/)
  ).toBeTruthy();
  expect(screen.getAllByText('Order price unavailable')).toHaveLength(4);
  expect(screen.getByText('Maximum machine price')).toBeTruthy();
});

it('loads once on mount and only refreshes manually', async () => {
  jest.useFakeTimers();
  try {
    render(<ServicePlacement serviceName="svc" />);

    expect(
      await screen.findByText('Candidate catalog — not launches')
    ).toBeTruthy();
    const l4Row = screen.getByRole('row', { name: /Zone: us-east-1a/ });
    expect(within(l4Row).getByText('L4:1')).toBeTruthy();
    expect(within(l4Row).getByText('$0.4500/GPU-hr')).toBeTruthy();
    expect(within(l4Row).getByText('$0.4500/machine-hr')).toBeTruthy();
    const a100Row = screen.getByRole('row', { name: /Zone: us-east-1b/ });
    expect(within(a100Row).getByText('A100:1')).toBeTruthy();
    expect(within(a100Row).getByText('$3.2500/GPU-hr')).toBeTruthy();
    const h100Row = screen.getByRole('row', { name: /Zone: us-east-1c/ });
    expect(within(h100Row).getByText('H100:1')).toBeTruthy();
    expect(within(h100Row).getByText('$2.5000/GPU-hr')).toBeTruthy();
    expect(
      screen.getByLabelText(
        /Eligibility: Eligible spot[\s\S]*Probe eligible since/
      )
    ).toBeTruthy();
    expect(
      screen.getByLabelText(/Eligibility: Ineligible[\s\S]*Next probe:/)
    ).toBeTruthy();
    expect(screen.getByText('Zonal capacity')).toBeTruthy();
    // The provider is shown so a hint is attributable once more than one
    // cloud can contribute suppression.
    expect(screen.getByText('gcp')).toBeTruthy();
    expect(screen.getByText('g2-standard-4 (L4:1)')).toBeTruthy();
    expect(screen.getByText(/exact instance demand/)).toBeTruthy();
    expect(
      screen.getByText(/Actual selection applies ACTIVE status/)
    ).toBeTruthy();
    expect(
      screen
        .getAllByRole('row')
        .filter((row) => row.hasAttribute('aria-label'))
        .map((row) => row.getAttribute('aria-label').split('\n')[0])
    ).toEqual([
      'Zone: us-central1-a',
      'Zone: us-east-1a',
      'Zone: us-east-1c',
      'Zone: us-east-1b',
    ]);
    expect(
      screen
        .getAllByRole('heading', { level: 3 })
        .map((heading) => heading.textContent)
    ).toEqual([
      'Actual placement attempts (24h)',
      'Candidate catalog — not launches',
      'Launch suppression',
    ]);
    expect(getServicePlacement).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(60 * 60 * 1000);
      await Promise.resolve();
    });
    expect(getServicePlacement).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    await waitFor(() => expect(getServicePlacement).toHaveBeenCalledTimes(2));
  } finally {
    jest.useRealTimers();
  }
});

it('filters the compact rows and clears all filters', async () => {
  render(<ServicePlacement serviceName="svc" />);
  expect(await screen.findByText('$3.2500/GPU-hr')).toBeTruthy();

  fireEvent.change(screen.getByLabelText('Provider filter'), {
    target: { value: 'GCP' },
  });
  expect(screen.queryByRole('row', { name: /Zone: us-east-1a/ })).toBeNull();
  expect(screen.getByRole('row', { name: /Zone: us-central1-a/ })).toBeTruthy();

  fireEvent.change(screen.getByLabelText('Provider filter'), {
    target: { value: 'all' },
  });
  fireEvent.change(screen.getByLabelText('Region filter'), {
    target: { value: 'us-central1' },
  });
  expect(screen.queryByRole('row', { name: /Zone: us-east-1a/ })).toBeNull();
  expect(screen.getByRole('row', { name: /Zone: us-central1-a/ })).toBeTruthy();

  fireEvent.change(screen.getByLabelText('Region filter'), {
    target: { value: 'all' },
  });
  fireEvent.change(screen.getByLabelText('Eligibility filter'), {
    target: { value: LOCATION_AVAILABILITY.AVAILABLE_ON_DEMAND },
  });
  expect(screen.getByText('$3.2500/GPU-hr')).toBeTruthy();
  expect(screen.queryByText('$0.4500/GPU-hr')).toBeNull();
  expect(screen.queryByText('$2.5000/GPU-hr')).toBeNull();

  fireEvent.change(screen.getByLabelText('Eligibility filter'), {
    target: { value: 'all' },
  });
  fireEvent.change(screen.getByLabelText('Card filter'), {
    target: { value: 'L4' },
  });
  fireEvent.change(screen.getByLabelText('Maximum price filter'), {
    target: { value: '0.4' },
  });
  expect(screen.getByText('$0.3500/GPU-hr')).toBeTruthy();
  expect(screen.queryByText('$0.4500/GPU-hr')).toBeNull();

  fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }));
  expect(screen.getByText('$0.4500/GPU-hr')).toBeTruthy();
  expect(screen.getByText('$3.2500/GPU-hr')).toBeTruthy();
  expect(screen.getByText('$2.5000/GPU-hr')).toBeTruthy();
});

it('exposes next-probe detail on the unavailable card hover target', async () => {
  render(<ServicePlacement serviceName="svc" />);
  const unavailable = await screen.findByLabelText(
    /Eligibility: Ineligible[\s\S]*Next probe:/
  );

  expect(unavailable).toHaveAttribute(
    'title',
    expect.stringMatching(/Next probe:/)
  );
});

it('ignores a stale response after the service route changes', async () => {
  const first = deferred();
  const second = deferred();
  getServicePlacement
    .mockImplementationOnce(() => first.promise)
    .mockImplementationOnce(() => second.promise);
  const { rerender } = render(<ServicePlacement serviceName="svc-a" />);
  rerender(<ServicePlacement serviceName="svc-b" />);

  await act(async () => {
    second.resolve({
      ...placement,
      serviceName: 'svc-b',
      placerState: {
        ...placement.placerState,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'svc-b-region',
          },
        ],
      },
    });
    await Promise.resolve();
  });
  expect(await screen.findByText('svc-b-region')).toBeTruthy();

  await act(async () => {
    first.resolve({
      ...placement,
      serviceName: 'svc-a',
      placerState: {
        ...placement.placerState,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'svc-a-region',
          },
        ],
      },
    });
    await Promise.resolve();
  });
  expect(screen.getByText('svc-b-region')).toBeTruthy();
  expect(screen.queryByText('svc-a-region')).toBeNull();
});

it('loads older decisions with the opaque cursor and appends them', async () => {
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      history: {
        ...placement.history,
        events: [
          {
            eventId: 'new-event',
            clusterName: 'svc-new',
            observedAt: 2000,
            outcome: 'succeeded',
            hourlyPrice: 0.1541,
            priceSource: 'catalog_at_decision',
          },
        ],
        nextCursor: 'older-cursor',
      },
    })
    .mockResolvedValueOnce({
      ...placement,
      history: {
        ...placement.history,
        events: [
          {
            eventId: 'old-event',
            clusterName: 'svc-old',
            observedAt: 1000,
            outcome: 'capacity_failed',
          },
        ],
        nextCursor: null,
      },
    });

  render(<ServicePlacement serviceName="svc" />);
  expect(await screen.findByText('svc-new')).toBeTruthy();
  expect(screen.getByText('$0.1541/machine-hr')).toBeTruthy();
  expect(screen.getByText('Catalog estimate at decision')).toBeTruthy();
  expect(screen.getByText(/not provider billing/)).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load older decisions' }));

  expect(await screen.findByText('svc-old')).toBeTruthy();
  expect(screen.getByText('svc-new')).toBeTruthy();
  expect(getServicePlacement).toHaveBeenLastCalledWith({
    serviceName: 'svc',
    cursor: 'older-cursor',
    locationOffset: 0,
  });
});

it('loads the next bounded location page and preserves the first page', async () => {
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'first-region',
          },
        ],
        nextOffset: 1,
        totalLocations: 2,
        truncated: true,
      },
    })
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        locations: [
          {
            ...placement.placerState.locations[1],
            region: 'second-region',
          },
        ],
        pageOffset: 1,
        nextOffset: null,
        totalLocations: 2,
        truncated: false,
      },
    });

  render(<ServicePlacement serviceName="svc" />);
  expect(await screen.findByText('first-region')).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load more locations' }));

  expect(await screen.findByText('second-region')).toBeTruthy();
  expect(screen.getByText('first-region')).toBeTruthy();
  expect(getServicePlacement).toHaveBeenLastCalledWith({
    serviceName: 'svc',
    cursor: null,
    locationOffset: 1,
    locationOrderGeneration: ORDER_GENERATION_A,
  });
});

it('reloads page zero instead of mixing different catalog orders', async () => {
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        paginationVersion: 1,
        orderSemantics: null,
        orderGeneration: null,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'legacy-first',
          },
        ],
        nextOffset: 1,
        totalLocations: 2,
        truncated: true,
      },
    })
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        pageOffset: 1,
        locations: [
          {
            ...placement.placerState.locations[1],
            region: 'must-not-append',
          },
        ],
        nextOffset: null,
        totalLocations: 2,
        truncated: false,
      },
    })
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'fresh-first',
          },
        ],
        nextOffset: 1,
        totalLocations: 2,
        truncated: true,
      },
    });

  render(<ServicePlacement serviceName="svc" />);
  expect(await screen.findByText('legacy-first')).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load more locations' }));

  expect(await screen.findByText('fresh-first')).toBeTruthy();
  expect(screen.queryByText('legacy-first')).toBeNull();
  expect(screen.queryByText('must-not-append')).toBeNull();
  expect(getServicePlacement).toHaveBeenCalledTimes(3);
  expect(getServicePlacement).toHaveBeenLastCalledWith({ serviceName: 'svc' });
});

it('reloads page zero when the server rejects a stale order token', async () => {
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'stale-first',
          },
        ],
        nextOffset: 1,
        totalLocations: 2,
        truncated: true,
      },
    })
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        available: false,
        reason: 'catalog_order_changed',
        orderGeneration: ORDER_GENERATION_B,
        locations: [],
      },
    })
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        orderGeneration: ORDER_GENERATION_B,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'current-first',
          },
        ],
        nextOffset: 1,
        totalLocations: 2,
        truncated: true,
      },
    });

  render(<ServicePlacement serviceName="svc" />);
  expect(await screen.findByText('stale-first')).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load more locations' }));

  expect(await screen.findByText('current-first')).toBeTruthy();
  expect(screen.queryByText('stale-first')).toBeNull();
  expect(getServicePlacement).toHaveBeenNthCalledWith(2, {
    serviceName: 'svc',
    cursor: null,
    locationOffset: 1,
    locationOrderGeneration: ORDER_GENERATION_A,
  });
  expect(getServicePlacement).toHaveBeenLastCalledWith({ serviceName: 'svc' });
});

it('preserves and retries a location page after a fail-soft response', async () => {
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      placerState: {
        ...placement.placerState,
        locations: [
          {
            ...placement.placerState.locations[0],
            region: 'first-region',
          },
        ],
        nextOffset: 1,
        totalLocations: 2,
        truncated: true,
      },
    })
    .mockResolvedValue({
      ...placement,
      placerState: {
        available: false,
        reason: 'controller_unavailable',
        locations: [],
        nextOffset: null,
      },
    });

  render(<ServicePlacement serviceName="svc" />);
  expect(await screen.findByText('first-region')).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load more locations' }));

  expect(
    await screen.findByText('Failed to load more placement locations.')
  ).toBeTruthy();
  expect(screen.getByText('first-region')).toBeTruthy();
  const retry = screen.getByRole('button', { name: 'Load more locations' });
  expect(retry).toBeEnabled();

  fireEvent.click(retry);
  await waitFor(() => expect(getServicePlacement).toHaveBeenCalledTimes(3));
  expect(getServicePlacement).toHaveBeenLastCalledWith({
    serviceName: 'svc',
    cursor: null,
    locationOffset: 1,
    locationOrderGeneration: ORDER_GENERATION_A,
  });
});

it('preserves and retries history after a fail-soft response', async () => {
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      history: {
        ...placement.history,
        events: [
          {
            eventId: 'new-event',
            clusterName: 'svc-new',
            observedAt: 2000,
            outcome: 'succeeded',
          },
        ],
        nextCursor: 'older-cursor',
      },
    })
    .mockResolvedValue({
      ...placement,
      history: {
        available: false,
        reason: 'database_unavailable',
        events: [],
        nextCursor: null,
      },
    });

  render(<ServicePlacement serviceName="svc" />);
  expect(await screen.findByText('svc-new')).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load older decisions' }));

  expect(
    await screen.findByText('Failed to load more placement history.')
  ).toBeTruthy();
  expect(screen.getByText('svc-new')).toBeTruthy();
  const retry = screen.getByRole('button', { name: 'Load older decisions' });
  expect(retry).toBeEnabled();

  fireEvent.click(retry);
  await waitFor(() => expect(getServicePlacement).toHaveBeenCalledTimes(3));
  expect(getServicePlacement).toHaveBeenLastCalledWith({
    serviceName: 'svc',
    cursor: 'older-cursor',
    locationOffset: 0,
  });
});

it('keeps refresh from superseding an in-flight history page', async () => {
  const older = deferred();
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      history: {
        ...placement.history,
        nextCursor: 'older-cursor',
      },
    })
    .mockImplementationOnce(() => older.promise);

  render(<ServicePlacement serviceName="svc" />);
  const loadOlder = await screen.findByRole('button', {
    name: 'Load older decisions',
  });
  fireEvent.click(loadOlder);

  const refresh = screen.getByRole('button', { name: 'Refresh' });
  expect(refresh).toBeDisabled();
  fireEvent.click(refresh);
  expect(getServicePlacement).toHaveBeenCalledTimes(2);

  await act(async () => {
    older.resolve({
      ...placement,
      history: { ...placement.history, nextCursor: null },
    });
    await older.promise;
  });
  expect(refresh).toBeEnabled();
});

it('keeps history pagination from superseding an in-flight refresh', async () => {
  const refresh = deferred();
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      history: {
        ...placement.history,
        nextCursor: 'older-cursor',
      },
    })
    .mockImplementationOnce(() => refresh.promise);

  render(<ServicePlacement serviceName="svc" />);
  const loadOlder = await screen.findByRole('button', {
    name: 'Load older decisions',
  });
  fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));

  expect(loadOlder).toBeDisabled();
  fireEvent.click(loadOlder);
  expect(getServicePlacement).toHaveBeenCalledTimes(2);

  await act(async () => {
    refresh.resolve({
      ...placement,
      history: { ...placement.history, nextCursor: 'older-cursor' },
    });
    await refresh.promise;
  });
  expect(loadOlder).toBeEnabled();
});

it('releases both controls after a placement request fails', async () => {
  const refresh = deferred();
  getServicePlacement
    .mockResolvedValueOnce({
      ...placement,
      history: {
        ...placement.history,
        nextCursor: 'older-cursor',
      },
    })
    .mockImplementationOnce(() => refresh.promise);

  render(<ServicePlacement serviceName="svc" />);
  const loadOlder = await screen.findByRole('button', {
    name: 'Load older decisions',
  });
  const refreshButton = screen.getByRole('button', { name: 'Refresh' });
  fireEvent.click(refreshButton);

  await act(async () => {
    refresh.reject(new Error('transient failure'));
    try {
      await refresh.promise;
    } catch (_) {
      // The component owns and renders this expected failure.
    }
  });

  expect(
    await screen.findByText('Failed to load placement data.')
  ).toBeTruthy();
  expect(refreshButton).toBeEnabled();
  expect(loadOlder).toBeEnabled();
});
