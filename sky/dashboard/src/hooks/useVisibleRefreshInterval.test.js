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

  it('preserves the original due boundary when early visibility catch-up is disabled', () => {
    const onRefresh = jest.fn();
    const { unmount } = renderHook(() =>
      useVisibleRefreshInterval(true, 1000, onRefresh, {
        catchUpOnlyWhenOverdue: true,
      })
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
    expect(onRefresh).not.toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(99);
    });
    expect(onRefresh).not.toHaveBeenCalled();

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(onRefresh).toHaveBeenCalledTimes(1);
    expect(onRefresh).toHaveBeenLastCalledWith('interval');

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
  });

  it('supports an aligned first tick without losing the cadence boundary', () => {
    const onRefresh = jest.fn();
    const { unmount } = renderHook(() =>
      useVisibleRefreshInterval(true, 1000, onRefresh, {
        initialDelayMs: 400,
      })
    );

    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(399);
    });
    expect(onRefresh).not.toHaveBeenCalled();

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(onRefresh).toHaveBeenCalledTimes(1);
    expect(onRefresh).toHaveBeenLastCalledWith('interval');

    act(() => {
      setDocumentVisibility('hidden');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(jest.getTimerCount()).toBe(0);

    act(() => {
      jest.advanceTimersByTime(1600);
      setDocumentVisibility('visible');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(onRefresh).toHaveBeenCalledTimes(2);
    expect(onRefresh).toHaveBeenLastCalledWith('visibilitychange');
    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(1399);
    });
    expect(onRefresh).toHaveBeenCalledTimes(2);

    act(() => {
      jest.advanceTimersByTime(1);
    });
    expect(onRefresh).toHaveBeenCalledTimes(3);
    expect(onRefresh).toHaveBeenLastCalledWith('interval');

    unmount();
  });

  it.each([
    ['negative', -1],
    ['infinite', Number.POSITIVE_INFINITY],
  ])('disables %s refresh intervals', (_label, intervalMs) => {
    const onRefresh = jest.fn();
    const { unmount } = renderHook(() =>
      useVisibleRefreshInterval(true, intervalMs, onRefresh)
    );

    expect(jest.getTimerCount()).toBe(0);

    act(() => {
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(onRefresh).not.toHaveBeenCalled();
    expect(jest.getTimerCount()).toBe(0);

    unmount();
  });
});
