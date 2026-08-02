import { act, renderHook } from '@testing-library/react';

import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';

function setDocumentVisibility(state) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value: state,
  });
}

describe('useVisibleRefreshInterval', () => {
  let visibilityDescriptor;

  beforeEach(() => {
    jest.useFakeTimers();
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

  it('releases hidden timers and restores one visible cadence timer', () => {
    const onRefresh = jest.fn();
    const { unmount } = renderHook(() =>
      useVisibleRefreshInterval(true, 1000, onRefresh)
    );

    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      setDocumentVisibility('hidden');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(onRefresh).not.toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);

    act(() => {
      jest.advanceTimersByTime(5000);
    });
    expect(onRefresh).not.toHaveBeenCalled();

    act(() => {
      setDocumentVisibility('visible');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(onRefresh).toHaveBeenCalledTimes(1);
    expect(onRefresh).toHaveBeenLastCalledWith('visibilitychange');
    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(999);
    });
    expect(onRefresh).toHaveBeenCalledTimes(1);

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(onRefresh).toHaveBeenCalledTimes(2);
    expect(onRefresh).toHaveBeenLastCalledWith('interval');

    unmount();
    expect(jest.getTimerCount()).toBe(0);
  });

  it('preserves the original due boundary when visibility refresh is declined', () => {
    const onRefresh = jest.fn().mockImplementationOnce((source) => {
      expect(source).toBe('visibilitychange');
      return false;
    });
    const { unmount } = renderHook(() =>
      useVisibleRefreshInterval(true, 1000, onRefresh)
    );

    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(600);
      setDocumentVisibility('hidden');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(jest.getTimerCount()).toBe(0);

    act(() => {
      jest.advanceTimersByTime(300);
      setDocumentVisibility('visible');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(onRefresh).toHaveBeenCalledTimes(1);
    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(99);
    });
    expect(onRefresh).toHaveBeenCalledTimes(1);

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(onRefresh).toHaveBeenCalledTimes(2);
    expect(onRefresh).toHaveBeenLastCalledWith('interval');

    unmount();
  });
});
