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
  groupPlacementLocations,
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

const placement = {
  serviceName: 'svc',
  placerState: {
    available: true,
    enabled: true,
    retrySeconds: 600,
    locations: [
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
        paidAdmission: {
          state: 'probe',
          poolRemaining: 1,
          serviceRemaining: 12,
        },
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
      },
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

it('groups one row per provider-region and filters card, availability, and price', () => {
  const groups = groupPlacementLocations(placement.placerState.locations);
  expect(groups).toHaveLength(2);
  expect(groups[0]).toMatchObject({
    provider: 'AWS',
    region: 'us-east-1',
  });
  expect(groups[0].available).toHaveLength(2);
  expect(groups[0].unavailable).toHaveLength(1);

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
    cachedHourlyCost: 0.35,
  });
});

it('loads once on mount and only refreshes manually', async () => {
  jest.useFakeTimers();
  try {
    render(<ServicePlacement serviceName="svc" />);

    expect(await screen.findByText('Service fallback locations')).toBeTruthy();
    const awsRow = screen
      .getByRole('cell', { name: /AWS us-east-1/ })
      .closest('tr');
    expect(within(awsRow).getByText('L4:1')).toBeTruthy();
    expect(within(awsRow).getByText('$0.4500/hr')).toBeTruthy();
    expect(within(awsRow).getByText('A100:1')).toBeTruthy();
    expect(within(awsRow).getByText('$3.2500/hr')).toBeTruthy();
    expect(within(awsRow).getByText('H100:1')).toBeTruthy();
    expect(within(awsRow).getByText('$2.5000/hr')).toBeTruthy();
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
  expect(await screen.findByText('$3.2500/hr')).toBeTruthy();

  fireEvent.change(screen.getByLabelText('Provider filter'), {
    target: { value: 'GCP' },
  });
  expect(screen.queryByRole('cell', { name: /AWS us-east-1/ })).toBeNull();
  expect(screen.getByRole('cell', { name: /GCP us-central1/ })).toBeTruthy();

  fireEvent.change(screen.getByLabelText('Provider filter'), {
    target: { value: 'all' },
  });
  fireEvent.change(screen.getByLabelText('Region filter'), {
    target: { value: 'us-central1' },
  });
  expect(screen.queryByRole('cell', { name: /AWS us-east-1/ })).toBeNull();
  expect(screen.getByRole('cell', { name: /GCP us-central1/ })).toBeTruthy();

  fireEvent.change(screen.getByLabelText('Region filter'), {
    target: { value: 'all' },
  });
  fireEvent.change(screen.getByLabelText('Eligibility filter'), {
    target: { value: LOCATION_AVAILABILITY.AVAILABLE_ON_DEMAND },
  });
  expect(screen.getByText('$3.2500/hr')).toBeTruthy();
  expect(screen.queryByText('$0.4500/hr')).toBeNull();
  expect(screen.queryByText('$2.5000/hr')).toBeNull();

  fireEvent.change(screen.getByLabelText('Eligibility filter'), {
    target: { value: 'all' },
  });
  fireEvent.change(screen.getByLabelText('Card filter'), {
    target: { value: 'L4' },
  });
  fireEvent.change(screen.getByLabelText('Maximum price filter'), {
    target: { value: '0.4' },
  });
  expect(screen.getByText('$0.3500/hr')).toBeTruthy();
  expect(screen.queryByText('$0.4500/hr')).toBeNull();

  fireEvent.click(screen.getByRole('button', { name: 'Clear filters' }));
  expect(screen.getByText('$0.4500/hr')).toBeTruthy();
  expect(screen.getByText('$3.2500/hr')).toBeTruthy();
  expect(screen.getByText('$2.5000/hr')).toBeTruthy();
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
  expect(
    await screen.findByRole('cell', { name: /AWS svc-b-region/ })
  ).toBeTruthy();

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
  expect(screen.getByRole('cell', { name: /AWS svc-b-region/ })).toBeTruthy();
  expect(screen.queryByRole('cell', { name: /AWS svc-a-region/ })).toBeNull();
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
  expect(
    await screen.findByRole('cell', { name: /AWS first-region/ })
  ).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load more locations' }));

  expect(
    await screen.findByRole('cell', { name: /AWS second-region/ })
  ).toBeTruthy();
  expect(screen.getByRole('cell', { name: /AWS first-region/ })).toBeTruthy();
  expect(getServicePlacement).toHaveBeenLastCalledWith({
    serviceName: 'svc',
    cursor: null,
    locationOffset: 1,
  });
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
  expect(
    await screen.findByRole('cell', { name: /AWS first-region/ })
  ).toBeTruthy();

  fireEvent.click(screen.getByRole('button', { name: 'Load more locations' }));

  expect(
    await screen.findByText('Failed to load more placement locations.')
  ).toBeTruthy();
  expect(screen.getByRole('cell', { name: /AWS first-region/ })).toBeTruthy();
  const retry = screen.getByRole('button', { name: 'Load more locations' });
  expect(retry).toBeEnabled();

  fireEvent.click(retry);
  await waitFor(() => expect(getServicePlacement).toHaveBeenCalledTimes(3));
  expect(getServicePlacement).toHaveBeenLastCalledWith({
    serviceName: 'svc',
    cursor: null,
    locationOffset: 1,
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
