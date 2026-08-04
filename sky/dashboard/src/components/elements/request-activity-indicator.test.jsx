import React from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';

jest.mock('@/lib/request-activity', () => {
  const actual = jest.requireActual('@/lib/request-activity');
  return {
    ...actual,
    refreshRequestActivity: jest.fn(actual.refreshRequestActivity),
  };
});

import {
  refreshRequestActivity,
  resetRequestActivityForTests,
  trackDashboardRequest,
} from '@/lib/request-activity';
import { RequestActivityIndicator } from './request-activity-indicator';

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
}

describe('RequestActivityIndicator', () => {
  beforeEach(() => {
    jest.clearAllMocks();
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

  it('pauses hidden bucket refreshes and resumes on the next bucket boundary', async () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-08-02T12:04:00Z'));
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');

    try {
      const { unmount } = render(<RequestActivityIndicator />);
      expect(jest.getTimerCount()).toBe(1);

      act(() => {
        setDocumentVisibility('hidden');
        window.document.dispatchEvent(new Event('visibilitychange'));
      });
      expect(jest.getTimerCount()).toBe(0);

      act(() => {
        jest.advanceTimersByTime(6 * 60 * 1000);
      });
      expect(refreshRequestActivity).not.toHaveBeenCalled();

      act(() => {
        setDocumentVisibility('visible');
        window.document.dispatchEvent(new Event('visibilitychange'));
      });
      expect(refreshRequestActivity).toHaveBeenCalledTimes(1);
      expect(jest.getTimerCount()).toBe(1);

      act(() => {
        jest.advanceTimersByTime(4 * 60 * 1000 + 59 * 1000);
      });
      expect(refreshRequestActivity).toHaveBeenCalledTimes(1);

      act(() => {
        jest.advanceTimersByTime(1000);
      });
      expect(refreshRequestActivity).toHaveBeenCalledTimes(2);

      unmount();
      expect(jest.getTimerCount()).toBe(0);
    } finally {
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

  it('does not refresh early when visibility returns before the next bucket boundary', () => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-08-02T12:04:00Z'));
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');

    try {
      render(<RequestActivityIndicator />);

      act(() => {
        setDocumentVisibility('hidden');
        window.document.dispatchEvent(new Event('visibilitychange'));
        jest.advanceTimersByTime(30 * 1000);
        setDocumentVisibility('visible');
        window.document.dispatchEvent(new Event('visibilitychange'));
      });
      expect(refreshRequestActivity).not.toHaveBeenCalled();
      expect(jest.getTimerCount()).toBe(1);

      act(() => {
        jest.advanceTimersByTime(29 * 1000);
      });
      expect(refreshRequestActivity).not.toHaveBeenCalled();

      act(() => {
        jest.advanceTimersByTime(1000);
      });
      expect(refreshRequestActivity).toHaveBeenCalledTimes(1);
    } finally {
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
