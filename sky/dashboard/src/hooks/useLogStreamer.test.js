import { act, renderHook, waitFor } from '@testing-library/react';

import { useLogStreamer } from '@/hooks/useLogStreamer';

function deferredStream() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  const calls = [];
  const streamFn = jest.fn((options) => {
    calls.push(options);
    return promise;
  });
  return { calls, promise, reject, resolve, streamFn };
}

const STREAM_ARGS = { jobId: 42, controller: false };

describe('useLogStreamer progress ownership', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('does not call the stream function while disabled', () => {
    const stream = deferredStream();
    const { result, unmount } = renderHook(() =>
      useLogStreamer({
        streamFn: stream.streamFn,
        streamArgs: STREAM_ARGS,
        enabled: false,
      })
    );

    expect(stream.streamFn).not.toHaveBeenCalled();
    expect(result.current.isLoading).toBe(false);
    expect(result.current.lines).toEqual([]);
    expect(jest.getTimerCount()).toBe(0);
    unmount();
  });

  it('keeps only the newest process progress lines within the render budget', async () => {
    const stream = deferredStream();
    const { result, unmount } = renderHook(() =>
      useLogStreamer({
        streamFn: stream.streamFn,
        streamArgs: STREAM_ARGS,
        maxRenderLines: 3,
      })
    );

    await waitFor(() => expect(stream.calls).toHaveLength(1));
    act(() => {
      stream.calls[0].onNewLog(
        '(worker-1) 10% | first\n' +
          '(worker-2) 20% | second\n' +
          '(worker-3) 30% | third\n' +
          '(worker-4) 40% | fourth\n'
      );
    });

    expect(result.current.lines).toEqual([
      '(worker-2) 20% | second',
      '(worker-3) 30% | third',
      '(worker-4) 40% | fourth',
    ]);
    expect(jest.getTimerCount()).toBe(0);
    unmount();
  });

  it('updates one process in place without consuming another progress slot', async () => {
    const stream = deferredStream();
    const { result, unmount } = renderHook(() =>
      useLogStreamer({
        streamFn: stream.streamFn,
        streamArgs: STREAM_ARGS,
        maxRenderLines: 2,
      })
    );

    await waitFor(() => expect(stream.calls).toHaveLength(1));
    act(() => {
      stream.calls[0].onNewLog(
        '(worker-1) 10% | old\n' +
          '(worker-2) 20% | other\n' +
          '(worker-1) 90% | new\n'
      );
    });

    expect(result.current.lines).toEqual([
      '(worker-1) 90% | new',
      '(worker-2) 20% | other',
    ]);
    unmount();
  });

  it('discards keyed progress safely when the render budget is zero', async () => {
    const stream = deferredStream();
    const { result, unmount } = renderHook(() =>
      useLogStreamer({
        streamFn: stream.streamFn,
        streamArgs: STREAM_ARGS,
        maxRenderLines: 0,
      })
    );

    await waitFor(() => expect(stream.calls).toHaveLength(1));
    act(() => {
      stream.calls[0].onNewLog('(worker-1) 10% | discarded\n');
    });

    expect(result.current.lines).toEqual([]);
    expect(jest.getTimerCount()).toBe(0);
    unmount();
  });

  it('keeps ordinary and unkeyed progress lines on the shared buffer budget', async () => {
    const stream = deferredStream();
    const { result, unmount } = renderHook(() =>
      useLogStreamer({
        streamFn: stream.streamFn,
        streamArgs: STREAM_ARGS,
        maxRenderLines: 3,
        flushIntervalMs: 10,
      })
    );

    await waitFor(() => expect(stream.calls).toHaveLength(1));
    act(() => {
      stream.calls[0].onNewLog(
        'ordinary-1\n10% | unkeyed\nordinary-2\nordinary-3\n'
      );
      jest.advanceTimersByTime(10);
    });

    expect(result.current.lines).toEqual([
      '10% | unkeyed',
      'ordinary-2',
      'ordinary-3',
    ]);
    unmount();
  });

  it('revokes progress, timers, and stale chunks across refresh and unmount', async () => {
    const streams = [deferredStream(), deferredStream()];
    const streamFn = jest
      .fn()
      .mockImplementationOnce(streams[0].streamFn)
      .mockImplementationOnce(streams[1].streamFn);
    const { result, rerender, unmount } = renderHook(
      ({ refreshTrigger }) =>
        useLogStreamer({
          streamFn,
          streamArgs: STREAM_ARGS,
          refreshTrigger,
          flushIntervalMs: 10,
        }),
      { initialProps: { refreshTrigger: 0 } }
    );

    await waitFor(() => expect(streams[0].calls).toHaveLength(1));
    act(() => {
      streams[0].calls[0].onNewLog('(worker-1) 10% | old\nordinary-old\n');
    });
    expect(result.current.lines).toEqual(['(worker-1) 10% | old']);

    rerender({ refreshTrigger: 1 });
    await waitFor(() => expect(streams[1].calls).toHaveLength(1));
    expect(streams[0].calls[0].signal.aborted).toBe(true);
    expect(result.current.lines).toEqual([]);

    act(() => {
      streams[0].calls[0].onNewLog('(worker-stale) 90% | stale\n');
      jest.advanceTimersByTime(10);
    });
    expect(result.current.lines).toEqual([]);

    act(() => {
      streams[1].calls[0].onNewLog('(worker-2) 20% | current\n');
    });
    expect(result.current.lines).toEqual(['(worker-2) 20% | current']);

    unmount();
    expect(streams[1].calls[0].signal.aborted).toBe(true);
    act(() => {
      streams[1].calls[0].onNewLog('(worker-stale) 90% | stale\n');
      jest.advanceTimersByTime(10);
    });
  });

  it('publishes one failure line and releases loading', async () => {
    const stream = deferredStream();
    const onError = jest.fn();
    const { result, unmount } = renderHook(() =>
      useLogStreamer({
        streamFn: stream.streamFn,
        streamArgs: STREAM_ARGS,
        maxRenderLines: 3,
        onError,
      })
    );

    await waitFor(() => expect(stream.calls).toHaveLength(1));
    await act(async () => {
      stream.reject(new Error('stream failed'));
      await stream.promise.catch(() => undefined);
    });

    expect(onError).toHaveBeenCalledWith(
      expect.objectContaining({ message: 'stream failed' })
    );
    expect(result.current.lines).toEqual([
      'Error fetching logs: stream failed',
    ]);
    expect(result.current.isLoading).toBe(false);
    unmount();
  });
});
