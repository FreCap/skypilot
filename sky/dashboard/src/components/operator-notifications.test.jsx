import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import { getCurrentUserRole } from '@/data/connectors/client';
import {
  acknowledgeOperatorNotifications,
  getOperatorNotifications,
} from '@/data/connectors/operator-notifications';
import { OperatorNotifications } from './operator-notifications';

jest.mock('@/data/connectors/client', () => ({
  getCurrentUserRole: jest.fn(),
}));
jest.mock('@/data/connectors/operator-notifications', () => ({
  getOperatorNotifications: jest.fn(),
  acknowledgeOperatorNotifications: jest.fn(),
}));

describe('OperatorNotifications', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    getCurrentUserRole.mockResolvedValue({ role: 'admin' });
    acknowledgeOperatorNotifications.mockResolvedValue({
      last_seen_sequence: 9,
    });
    getOperatorNotifications.mockResolvedValue({
      notifications: [
        {
          category: 'insufficient_quota',
          message: 'Insufficient AWS quota for g6.4xlarge in us-east-1.',
          first_seen_at: 100,
          last_seen_at: 200,
          occurrence_count: 12,
          sequence: 9,
          unread: true,
        },
      ],
      unread_count: 1,
      latest_sequence: 9,
      last_seen_sequence: 0,
    });
  });

  it('renders and acknowledges seven-day category history', async () => {
    render(<OperatorNotifications />);

    expect(
      await screen.findByRole('heading', { name: 'Operator notifications' })
    ).toBeInTheDocument();
    expect(screen.getByText('Insufficient Quota')).toBeInTheDocument();
    expect(
      screen.getByText('Insufficient AWS quota for g6.4xlarge in us-east-1.')
    ).toBeInTheDocument();
    expect(screen.getByText('12')).toBeInTheDocument();
    await waitFor(() =>
      expect(acknowledgeOperatorNotifications).toHaveBeenCalledWith(9)
    );

    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    await waitFor(() =>
      expect(getOperatorNotifications).toHaveBeenCalledTimes(2)
    );
  });

  it('does not fetch notification history for non-admin users', async () => {
    getCurrentUserRole.mockResolvedValue({ role: 'user' });
    render(<OperatorNotifications />);
    expect(
      await screen.findByText(
        'Admin access is required to view operator notifications.'
      )
    ).toBeInTheDocument();
    expect(getOperatorNotifications).not.toHaveBeenCalled();
  });

  it('renders a clear empty state without acknowledging', async () => {
    getOperatorNotifications.mockResolvedValue({
      notifications: [],
      unread_count: 0,
      latest_sequence: 0,
      last_seen_sequence: 0,
    });

    render(<OperatorNotifications />);
    expect(
      await screen.findByText(
        'No operator notifications were recorded in the last 7 days.'
      )
    ).toBeInTheDocument();
    expect(acknowledgeOperatorNotifications).not.toHaveBeenCalled();
  });
});
