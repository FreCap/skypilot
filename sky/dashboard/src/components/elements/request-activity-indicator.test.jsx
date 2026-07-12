import React from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';

import {
  resetRequestActivityForTests,
  trackDashboardRequest,
} from '@/lib/request-activity';
import { RequestActivityIndicator } from './request-activity-indicator';

describe('RequestActivityIndicator', () => {
  beforeEach(() => {
    resetRequestActivityForTests();
  });

  afterEach(() => {
    resetRequestActivityForTests();
  });

  it('shows live activity and five-minute browser history', async () => {
    let resolveRequest;
    const { unmount } = render(<RequestActivityIndicator />);
    expect(
      screen.getByRole('button', {
        name: 'Dashboard request activity: Idle',
      })
    ).toBeInTheDocument();

    let request;
    act(() => {
      request = trackDashboardRequest(
        () =>
          new Promise((resolve) => {
            resolveRequest = resolve;
          })
      );
    });

    expect(
      screen.getByRole('button', {
        name: 'Dashboard request activity: 1 active',
      })
    ).toBeInTheDocument();

    fireEvent.click(
      screen.getByRole('button', {
        name: 'Dashboard request activity: 1 active',
      })
    );
    expect(
      screen.getByText('Requests started per 5 minutes')
    ).toBeInTheDocument();
    expect(screen.getByText('1 request / hour')).toBeInTheDocument();
    expect(
      screen.getByText(/Best-effort API calls observed by this browser/)
    ).toBeInTheDocument();

    await act(async () => {
      resolveRequest('done');
      await request;
    });
    expect(
      screen.getByRole('button', {
        name: 'Dashboard request activity: Idle',
      })
    ).toBeInTheDocument();
    unmount();
  });
});
