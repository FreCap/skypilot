import React from 'react';
import { act, render, screen } from '@testing-library/react';

import { LastUpdatedTimestamp } from '@/components/utils';

function setDocumentVisibility(state) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value: state,
  });
}

describe('LastUpdatedTimestamp', () => {
  let visibilityDescriptor;

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(new Date('2026-08-02T12:00:00Z'));
    visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');
  });

  afterEach(() => {
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
  });

  it('pauses hidden timestamp refreshes, catches up on visibility restore, and resumes one cadence', () => {
    render(
      <LastUpdatedTimestamp timestamp={new Date('2026-08-02T11:59:30Z')} />
    );

    expect(screen.getByText('Updated 30s ago')).toBeInTheDocument();

    act(() => {
      setDocumentVisibility('hidden');
      window.document.dispatchEvent(new Event('visibilitychange'));
      jest.advanceTimersByTime(40000);
    });
    expect(screen.getByText('Updated 30s ago')).toBeInTheDocument();

    act(() => {
      jest.setSystemTime(new Date('2026-08-02T12:00:40Z'));
      setDocumentVisibility('visible');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(screen.getByText('Updated 1m ago')).toBeInTheDocument();

    act(() => {
      jest.advanceTimersByTime(9000);
    });
    expect(screen.getByText('Updated 1m ago')).toBeInTheDocument();

    act(() => {
      jest.setSystemTime(new Date('2026-08-02T12:01:40Z'));
      jest.advanceTimersByTime(1000);
    });
    expect(screen.getByText('Updated 2m ago')).toBeInTheDocument();
  });
});
