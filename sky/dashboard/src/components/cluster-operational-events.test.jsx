import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import { ClusterOperationalEvents } from '@/components/cluster-operational-events';
import {
  getOperationalEvents,
  OperationalEventApiError,
  OPERATIONAL_EVENTS_UNAVAILABLE,
} from '@/data/connectors/events';

jest.mock('@/data/connectors/events', () => {
  const actual = jest.requireActual('@/data/connectors/events');
  return {
    ...actual,
    getOperationalEvents: jest.fn(),
  };
});

function event(id, overrides = {}) {
  return {
    id,
    occurred_at: '2026-07-30T12:00:00Z',
    kind: 'cluster.start',
    outcome: 'succeeded',
    actor: { id: 'alice-id', name: 'alice@example.com', type: 'sso' },
    request_id: `request-${id}`,
    message: 'Cluster start succeeded.',
    ...overrides,
  };
}

describe('ClusterOperationalEvents', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('renders event identity and safe outcome fields', async () => {
    getOperationalEvents.mockResolvedValue({
      items: [event('one')],
      next_cursor: null,
    });

    render(
      <ClusterOperationalEvents
        clusterHash="hash-1"
        clusterName="trainer"
        workspace="research"
      />
    );

    expect(
      screen.getByText('Loading operational history...')
    ).toBeInTheDocument();
    expect(await screen.findByText('alice@example.com')).toBeInTheDocument();
    expect(screen.getByText('start')).toBeInTheDocument();
    expect(screen.getByText('succeeded')).toBeInTheDocument();
    expect(screen.getByText('request-one')).toBeInTheDocument();
    expect(getOperationalEvents).toHaveBeenCalledWith(
      {
        clusterHash: 'hash-1',
        clusterName: 'trainer',
        workspace: 'research',
        limit: 20,
      },
      expect.any(AbortSignal)
    );
  });

  it('renders explicit empty and PostgreSQL-unavailable states', async () => {
    getOperationalEvents.mockResolvedValueOnce({
      items: [],
      next_cursor: null,
    });
    const view = render(
      <ClusterOperationalEvents clusterName="trainer" workspace="default" />
    );
    expect(
      await screen.findByText(
        'No operational events recorded for this cluster.'
      )
    ).toBeInTheDocument();

    getOperationalEvents.mockRejectedValueOnce(
      new OperationalEventApiError(OPERATIONAL_EVENTS_UNAVAILABLE, 503)
    );
    view.rerender(
      <ClusterOperationalEvents clusterName="trainer-2" workspace="default" />
    );
    expect(
      await screen.findByText(
        'Operational history requires a PostgreSQL-backed API server.'
      )
    ).toBeInTheDocument();
  });

  it('appends older pages through the server cursor', async () => {
    getOperationalEvents
      .mockResolvedValueOnce({
        items: [event('newer')],
        next_cursor: 'older-cursor',
      })
      .mockResolvedValueOnce({
        items: [event('older', { kind: 'cluster.launch' })],
        next_cursor: null,
      });

    render(
      <ClusterOperationalEvents
        clusterHash="hash-1"
        clusterName="trainer"
        workspace="research"
      />
    );
    await screen.findByText('request-newer');
    fireEvent.click(screen.getByRole('button', { name: 'Load more' }));
    expect(await screen.findByText('request-older')).toBeInTheDocument();
    expect(getOperationalEvents.mock.calls[1][0]).toEqual({
      clusterHash: 'hash-1',
      clusterName: 'trainer',
      workspace: 'research',
      limit: 20,
      cursor: 'older-cursor',
    });
  });

  it('ignores a stale response after cluster navigation', async () => {
    let resolveFirst;
    const first = new Promise((resolve) => {
      resolveFirst = resolve;
    });
    getOperationalEvents.mockReturnValueOnce(first).mockResolvedValueOnce({
      items: [event('second')],
      next_cursor: null,
    });
    const view = render(
      <ClusterOperationalEvents clusterHash="hash-1" clusterName="one" />
    );
    const firstSignal = getOperationalEvents.mock.calls[0][1];
    view.rerender(
      <ClusterOperationalEvents clusterHash="hash-2" clusterName="two" />
    );
    expect(firstSignal.aborted).toBe(true);
    expect(await screen.findByText('request-second')).toBeInTheDocument();

    await act(async () => {
      resolveFirst({ items: [event('stale')], next_cursor: null });
    });
    await waitFor(() => {
      expect(screen.queryByText('request-stale')).not.toBeInTheDocument();
    });
  });

  it('resets an in-flight load-more state after cluster navigation', async () => {
    let resolveLoadMore;
    const pendingLoadMore = new Promise((resolve) => {
      resolveLoadMore = resolve;
    });
    getOperationalEvents
      .mockResolvedValueOnce({
        items: [event('first')],
        next_cursor: 'first-cursor',
      })
      .mockReturnValueOnce(pendingLoadMore)
      .mockResolvedValueOnce({
        items: [event('second')],
        next_cursor: 'second-cursor',
      });

    const view = render(
      <ClusterOperationalEvents clusterHash="hash-1" clusterName="one" />
    );
    await screen.findByText('request-first');
    fireEvent.click(screen.getByRole('button', { name: 'Load more' }));
    expect(screen.getByRole('button', { name: 'Loading...' })).toBeDisabled();

    view.rerender(
      <ClusterOperationalEvents clusterHash="hash-2" clusterName="two" />
    );
    expect(await screen.findByText('request-second')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Load more' })).toBeEnabled();

    await act(async () => {
      resolveLoadMore({ items: [event('stale-more')], next_cursor: null });
    });
    expect(screen.queryByText('request-stale-more')).not.toBeInTheDocument();
  });
});
