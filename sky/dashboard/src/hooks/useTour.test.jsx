import React from 'react';
import { act, render } from '@testing-library/react';
import Shepherd from 'shepherd.js';
import { useRouter } from 'next/router';
import { useFirstVisit } from '@/hooks/useFirstVisit';
import { TourProvider, useTour } from '@/hooks/useTour';

jest.mock('shepherd.js', () => ({
  __esModule: true,
  default: {
    activeTour: null,
    Tour: jest.fn(() => ({
      addStep: jest.fn(),
      complete: jest.fn(),
      on: jest.fn(),
      start: jest.fn(),
    })),
  },
}));

jest.mock('next/router', () => ({
  useRouter: jest.fn(),
}));

jest.mock('@/hooks/useFirstVisit', () => ({
  useFirstVisit: jest.fn(),
}));

function TourConsumer({ onValue }) {
  const value = useTour();
  onValue(value);
  return null;
}

describe('TourProvider', () => {
  const router = {
    pathname: '/dashboard',
    push: jest.fn(() => Promise.resolve()),
    events: {
      emit: jest.fn(),
      off: jest.fn(),
      on: jest.fn(),
    },
  };

  beforeEach(() => {
    jest.clearAllMocks();
    useRouter.mockReturnValue(router);
    useFirstVisit.mockReturnValue({
      isFirstVisit: false,
      markTourCompleted: jest.fn(),
    });
  });

  afterEach(() => {
    jest.useRealTimers();
    document.getElementById('shepherd-global-custom-style')?.remove();
  });

  it('registers the complete ordered step catalogue and lifecycle events', () => {
    const { unmount } = render(<TourProvider>tour child</TourProvider>);
    const tour = Shepherd.Tour.mock.results[0].value;

    expect(Shepherd.Tour).toHaveBeenCalledWith(
      expect.objectContaining({ useModalOverlay: false })
    );
    expect(tour.on.mock.calls.map(([event]) => event)).toEqual([
      'complete',
      'cancel',
    ]);
    expect(tour.addStep.mock.calls.map(([step]) => step.title)).toEqual([
      '👋 Welcome to SkyPilot!',
      'Clusters',
      'SkyPilot is infra-agnostic',
      'Multi-user support',
      'Spin up compute in seconds',
      'Jobs',
      'Bring one or many infrastructure',
      'Workspaces',
      'Users',
      '🎉 Tour complete!',
    ]);
    expect(document.getElementById('shepherd-global-custom-style')).not.toBe(
      null
    );

    unmount();
    expect(tour.complete).toHaveBeenCalledTimes(1);
  });

  it('preserves step actions, route setup, and the public context controls', async () => {
    jest.useFakeTimers();
    let contextValue;
    const { unmount } = render(
      <TourProvider>
        <TourConsumer onValue={(value) => (contextValue = value)} />
      </TourProvider>
    );
    const tour = Shepherd.Tour.mock.results[0].value;
    const steps = tour.addStep.mock.calls.map(([step]) => step);
    const stepTour = {
      back: jest.fn(),
      cancel: jest.fn(),
      complete: jest.fn(),
      next: jest.fn(),
    };

    steps[0].buttons[0].action.call(stepTour);
    steps[0].buttons[1].action.call(stepTour);
    expect(stepTour.cancel).toHaveBeenCalledTimes(1);
    expect(stepTour.next).toHaveBeenCalledTimes(1);

    await act(async () => {
      const setup = steps[1].beforeShowPromise();
      await Promise.resolve();
      expect(router.push).toHaveBeenCalledWith('/clusters');
      jest.advanceTimersByTime(200);
      await setup;
    });

    act(() => contextValue.startTour());
    expect(tour.start).toHaveBeenCalledTimes(1);
    act(() => contextValue.completeTour());
    expect(tour.complete).toHaveBeenCalledTimes(1);
    unmount();
    expect(tour.complete).toHaveBeenCalledTimes(2);
  });
});
