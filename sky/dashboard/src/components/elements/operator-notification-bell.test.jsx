import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import {
  acknowledgeOperatorNotifications,
  getOperatorNotifications,
} from '@/data/connectors/operator-notifications';
import {
  OPERATOR_NOTIFICATION_POLL_MS,
  OperatorNotificationBell,
} from './operator-notification-bell';

jest.mock('@/data/connectors/operator-notifications', () => ({
  getOperatorNotifications: jest.fn(),
  acknowledgeOperatorNotifications: jest.fn(),
}));

const unread = {
  notifications: [
    {
      category: 'insufficient_quota',
      message: 'Insufficient AWS quota in us-east-1.',
      first_seen_at: 100,
      last_seen_at: 200,
      occurrence_count: 4,
      sequence: 7,
      unread: true,
    },
  ],
  unread_count: 1,
  latest_sequence: 7,
  last_seen_sequence: 0,
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

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
}

const empty = {
  notifications: [],
  unread_count: 0,
  latest_sequence: 0,
  last_seen_sequence: 0,
};

describe('OperatorNotificationBell', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    getOperatorNotifications.mockResolvedValue(unread);
    acknowledgeOperatorNotifications.mockResolvedValue({
      last_seen_sequence: 7,
    });
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('shows and acknowledges the unread subset when opened', async () => {
    render(<OperatorNotificationBell role="admin" />);

    const button = await screen.findByRole('button', {
      name: 'Operator notifications: 1 unread',
    });
    fireEvent.click(button);

    expect(screen.getByText('Insufficient Quota')).toBeInTheDocument();
    expect(
      screen.getByText('Insufficient AWS quota in us-east-1.')
    ).toBeInTheDocument();
    expect(screen.getByText(/4 occurrences/)).toBeInTheDocument();
    expect(
      screen.getByRole('link', { name: 'View the last 7 days' })
    ).toHaveAttribute('href', '/notifications');
    await waitFor(() =>
      expect(acknowledgeOperatorNotifications).toHaveBeenCalledWith(7)
    );
    await screen.findByRole('button', {
      name: 'Operator notifications: 0 unread',
    });
  });

  it('shows every unread category before acknowledging the global cursor', async () => {
    getOperatorNotifications.mockResolvedValue({
      notifications: Array.from({ length: 6 }, (_, index) => ({
        category: `category_${index + 1}`,
        message: `Actionable message ${index + 1}`,
        first_seen_at: 100 + index,
        last_seen_at: 200 + index,
        occurrence_count: 1,
        sequence: index + 1,
        unread: true,
      })),
      unread_count: 6,
      latest_sequence: 6,
      last_seen_sequence: 0,
    });
    acknowledgeOperatorNotifications.mockResolvedValue({
      last_seen_sequence: 6,
    });

    render(<OperatorNotificationBell role="admin" />);
    fireEvent.click(
      await screen.findByRole('button', {
        name: 'Operator notifications: 6 unread',
      })
    );

    expect(screen.getByText('Actionable message 6')).toBeInTheDocument();
    await waitFor(() =>
      expect(acknowledgeOperatorNotifications).toHaveBeenCalledWith(6)
    );
  });

  it('is hidden from non-admin users', () => {
    render(<OperatorNotificationBell role="user" />);
    expect(
      screen.queryByRole('button', { name: /Operator notifications/ })
    ).not.toBeInTheDocument();
    expect(getOperatorNotifications).not.toHaveBeenCalled();
  });

  it('coalesces overdue polls and retries after the owner fails', async () => {
    jest.useFakeTimers();
    const first = deferred();
    getOperatorNotifications
      .mockReset()
      .mockReturnValueOnce(first.promise)
      .mockResolvedValueOnce(unread);

    render(<OperatorNotificationBell role="admin" />);
    expect(getOperatorNotifications).toHaveBeenCalledTimes(1);

    act(() => {
      jest.advanceTimersByTime(OPERATOR_NOTIFICATION_POLL_MS * 3);
    });
    expect(getOperatorNotifications).toHaveBeenCalledTimes(1);

    await act(async () => {
      first.reject(new Error('poll unavailable'));
      await Promise.resolve();
    });
    await act(async () => {
      jest.advanceTimersByTime(OPERATOR_NOTIFICATION_POLL_MS);
      await Promise.resolve();
    });
    expect(getOperatorNotifications).toHaveBeenCalledTimes(2);
    await screen.findByRole('button', {
      name: 'Operator notifications: 1 unread',
    });
  });

  it('pauses hidden polls, catches up once, and resumes visible cadence', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');
    const { unmount } = render(<OperatorNotificationBell role="admin" />);
    let mounted = true;

    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(1);

      setDocumentVisibility('hidden');
      await act(async () => {
        jest.advanceTimersByTime(OPERATOR_NOTIFICATION_POLL_MS * 3 - 1000);
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(1);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(1000);
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(OPERATOR_NOTIFICATION_POLL_MS);
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(3);

      unmount();
      mounted = false;
      window.document.dispatchEvent(new Event('visibilitychange'));
      await act(async () => {
        jest.advanceTimersByTime(OPERATOR_NOTIFICATION_POLL_MS);
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(3);
    } finally {
      if (mounted) unmount();
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

  it('removes recurring refresh ownership after leaving the admin role', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');
    const { rerender, unmount } = render(
      <OperatorNotificationBell role="admin" />
    );

    try {
      await act(async () => {
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(1);

      rerender(<OperatorNotificationBell role="user" />);
      window.document.dispatchEvent(new Event('visibilitychange'));
      await act(async () => {
        jest.advanceTimersByTime(OPERATOR_NOTIFICATION_POLL_MS * 2);
        await Promise.resolve();
      });
      expect(getOperatorNotifications).toHaveBeenCalledTimes(1);
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

  it('fences a poll from an earlier admin role lifecycle', async () => {
    const stale = deferred();
    getOperatorNotifications
      .mockReset()
      .mockReturnValueOnce(stale.promise)
      .mockResolvedValueOnce(unread);
    const { rerender } = render(<OperatorNotificationBell role="admin" />);

    rerender(<OperatorNotificationBell role="user" />);
    rerender(<OperatorNotificationBell role="admin" />);
    await screen.findByRole('button', {
      name: 'Operator notifications: 1 unread',
    });

    await act(async () => {
      stale.resolve(empty);
      await stale.promise;
    });
    expect(
      screen.getByRole('button', {
        name: 'Operator notifications: 1 unread',
      })
    ).toBeInTheDocument();
  });

  it('fences an acknowledgement from an earlier admin lifecycle', async () => {
    const staleAcknowledgement = deferred();
    acknowledgeOperatorNotifications.mockReturnValueOnce(
      staleAcknowledgement.promise
    );
    const { rerender } = render(<OperatorNotificationBell role="admin" />);
    fireEvent.click(
      await screen.findByRole('button', {
        name: 'Operator notifications: 1 unread',
      })
    );

    rerender(<OperatorNotificationBell role="user" />);
    rerender(<OperatorNotificationBell role="admin" />);
    await waitFor(() =>
      expect(getOperatorNotifications).toHaveBeenCalledTimes(2)
    );
    await act(async () => {
      staleAcknowledgement.resolve({ last_seen_sequence: 7 });
      await staleAcknowledgement.promise;
    });

    expect(
      screen.getByRole('button', {
        name: 'Operator notifications: 1 unread',
      })
    ).toBeInTheDocument();
  });

  it('does not republish locally acknowledged notifications after a stale poll', async () => {
    jest.useFakeTimers();
    getOperatorNotifications
      .mockReset()
      .mockResolvedValueOnce(unread)
      .mockResolvedValueOnce(unread);

    render(<OperatorNotificationBell role="admin" />);
    fireEvent.click(
      await screen.findByRole('button', {
        name: 'Operator notifications: 1 unread',
      })
    );

    await waitFor(() =>
      expect(acknowledgeOperatorNotifications).toHaveBeenCalledWith(7)
    );
    await screen.findByRole('button', {
      name: 'Operator notifications: 0 unread',
    });

    await act(async () => {
      jest.advanceTimersByTime(OPERATOR_NOTIFICATION_POLL_MS);
      await Promise.resolve();
    });

    expect(getOperatorNotifications).toHaveBeenCalledTimes(2);
    expect(
      screen.getByRole('button', {
        name: 'Operator notifications: 0 unread',
      })
    ).toBeInTheDocument();
  });
});
