import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import {
  formatAccelerators,
  locationDisplayStatus,
  ServicePlacement,
} from './service-placement';
import { getServicePlacement } from '@/data/connectors/services';

jest.mock('@/data/connectors/services', () => ({
  getServicePlacement: jest.fn(),
}));

function deferred() {
  let resolve;
  const promise = new Promise((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
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
        accelerators: { L4: 1 },
        useSpot: true,
        storedStatus: 'PREEMPTED',
        effectiveStatus: 'ACTIVE',
        probeEligible: true,
        benchedAt: 1000,
        nextProbeAt: 1600,
      },
    ],
    truncated: false,
  },
  capacityHints: {
    available: true,
    hints: [
      {
        kind: 'capacity',
        region: 'us-east-1',
        zone: 'us-east-1a',
        instanceType: 'g6.4xlarge',
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
});

it('loads once on mount and only refreshes manually', async () => {
  jest.useFakeTimers();
  try {
    render(<ServicePlacement serviceName="svc" />);

    expect(await screen.findByText('Service fallback locations')).toBeTruthy();
    expect(screen.getByText('Probe eligible')).toBeTruthy();
    expect(screen.getByText(/Stored PREEMPTED/)).toBeTruthy();
    expect(screen.getByText(/Benched/)).toBeTruthy();
    expect(screen.getByText('AZ capacity')).toBeTruthy();
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
  expect(await screen.findByText(/svc-b-region/)).toBeTruthy();

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
  expect(screen.getByText(/svc-b-region/)).toBeTruthy();
  expect(screen.queryByText(/svc-a-region/)).toBeNull();
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
  });
});
