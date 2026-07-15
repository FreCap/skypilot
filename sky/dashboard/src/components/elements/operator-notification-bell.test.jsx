import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import {
  acknowledgeOperatorNotifications,
  getOperatorNotifications,
} from '@/data/connectors/operator-notifications';
import { OperatorNotificationBell } from './operator-notification-bell';

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

describe('OperatorNotificationBell', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    getOperatorNotifications.mockResolvedValue(unread);
    acknowledgeOperatorNotifications.mockResolvedValue({
      last_seen_sequence: 7,
    });
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

  it('is hidden from non-admin users', () => {
    render(<OperatorNotificationBell role="user" />);
    expect(
      screen.queryByRole('button', { name: /Operator notifications/ })
    ).not.toBeInTheDocument();
    expect(getOperatorNotifications).not.toHaveBeenCalled();
  });
});
