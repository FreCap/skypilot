jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    invalidateFunction: jest.fn(),
    setPreloader: jest.fn(),
    getCached: jest.fn(),
    clear: jest.fn(),
  },
}));

jest.mock('@/data/connectors/client', () => ({
  __esModule: true,
  apiClient: { fetchImmediate: jest.fn() },
  getCurrentUserInfo: jest.fn(),
}));

jest.mock('@/data/connectors/toast', () => ({
  showToast: jest.fn(),
}));

import { TextDecoder as NodeTextDecoder } from 'util';

import { apiClient } from '@/data/connectors/client';
import { streamManagedJobLogs } from '@/data/connectors/jobs';
import { showToast } from '@/data/connectors/toast';

function abortError() {
  return new DOMException('The operation was aborted.', 'AbortError');
}

function abortableReader(signal, reads) {
  return {
    read: jest.fn(() => {
      const next = reads.shift();
      if (next) {
        return Promise.resolve(next);
      }
      return new Promise((_, reject) => {
        if (signal.aborted) {
          reject(abortError());
          return;
        }
        signal.addEventListener('abort', () => reject(abortError()), {
          once: true,
        });
      });
    }),
    cancel: jest.fn(),
  };
}

function installStream(reads = []) {
  let requestSignal;
  let reader;
  apiClient.fetchImmediate.mockImplementation(
    async (_path, _body, _method, { signal }) => {
      requestSignal = signal;
      reader = abortableReader(signal, [...reads]);
      return {
        ok: true,
        body: { getReader: () => reader },
      };
    }
  );
  return {
    get requestSignal() {
      return requestSignal;
    },
    get reader() {
      return reader;
    },
  };
}

beforeEach(() => {
  jest.useFakeTimers();
  jest.clearAllMocks();
  global.TextDecoder = NodeTextDecoder;
});

afterEach(() => {
  jest.useRealTimers();
});

it('aborts the owned request when log inactivity times out', async () => {
  const stream = installStream();
  const caller = new AbortController();
  const promise = streamManagedJobLogs({
    jobId: '42',
    signal: caller.signal,
    onNewLog: jest.fn(),
  });

  await Promise.resolve();
  expect(stream.reader.read).toHaveBeenCalledTimes(1);

  await jest.advanceTimersByTimeAsync(30000);
  await promise;

  expect(stream.requestSignal).not.toBe(caller.signal);
  expect(stream.requestSignal.aborted).toBe(true);
  expect(caller.signal.aborted).toBe(false);
  expect(apiClient.fetchImmediate).toHaveBeenCalledTimes(1);
  expect(stream.reader.read).toHaveBeenCalledTimes(1);
  expect(showToast).toHaveBeenCalledWith(
    'Log request for job 42 timed out after 30s of inactivity',
    'warning'
  );
  expect(jest.getTimerCount()).toBe(0);
});

it('returns on timeout even when the aborted transport settles late', async () => {
  let requestSignal;
  let settleTransport;
  apiClient.fetchImmediate.mockImplementation(
    (_path, _body, _method, { signal }) => {
      requestSignal = signal;
      return new Promise((resolve) => {
        settleTransport = resolve;
      });
    }
  );
  let settled = false;
  const promise = streamManagedJobLogs({
    jobId: '42',
    signal: new AbortController().signal,
    onNewLog: jest.fn(),
  }).then(() => {
    settled = true;
  });

  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(30000);
  await Promise.resolve();
  const settledBeforeTransport = settled;
  const toastedBeforeTransport = showToast.mock.calls.length > 0;

  const getReader = jest.fn(() => abortableReader(requestSignal, []));
  const cancel = jest.fn().mockResolvedValue(undefined);
  settleTransport({
    ok: true,
    body: {
      cancel,
      getReader,
    },
  });
  await promise;
  await Promise.resolve();

  expect(requestSignal.aborted).toBe(true);
  expect(settledBeforeTransport).toBe(true);
  expect(toastedBeforeTransport).toBe(true);
  expect(getReader).not.toHaveBeenCalled();
  expect(cancel).toHaveBeenCalledTimes(1);
  expect(showToast).toHaveBeenCalledWith(
    'Log request for job 42 timed out after 30s of inactivity',
    'warning'
  );
  expect(jest.getTimerCount()).toBe(0);
});

it('forwards caller cancellation without reporting an inactivity timeout', async () => {
  const stream = installStream();
  const caller = new AbortController();
  const promise = streamManagedJobLogs({
    jobId: '42',
    signal: caller.signal,
    onNewLog: jest.fn(),
  });

  await Promise.resolve();
  caller.abort();
  await promise;

  expect(stream.requestSignal).not.toBe(caller.signal);
  expect(stream.requestSignal.aborted).toBe(true);
  expect(apiClient.fetchImmediate).toHaveBeenCalledTimes(1);
  expect(stream.reader.read).toHaveBeenCalledTimes(1);
  expect(showToast).not.toHaveBeenCalled();
  expect(jest.getTimerCount()).toBe(0);
});

it('preserves one fetch and one read per chunk plus EOF on normal completion', async () => {
  const stream = installStream([
    { done: false, value: new Uint8Array([0x61]) },
    { done: false, value: new Uint8Array([0x62]) },
    { done: true, value: undefined },
  ]);
  const caller = new AbortController();
  const onNewLog = jest.fn();

  await streamManagedJobLogs({
    jobId: '42',
    signal: caller.signal,
    onNewLog,
  });

  expect(stream.requestSignal).not.toBe(caller.signal);
  expect(stream.requestSignal.aborted).toBe(false);
  expect(caller.signal.aborted).toBe(false);
  expect(apiClient.fetchImmediate).toHaveBeenCalledTimes(1);
  expect(stream.reader.read).toHaveBeenCalledTimes(3);
  expect(stream.reader.cancel).toHaveBeenCalledTimes(1);
  expect(onNewLog.mock.calls.map(([chunk]) => chunk).join('')).toBe('ab');
  expect(showToast).not.toHaveBeenCalled();
  expect(jest.getTimerCount()).toBe(0);
});

it('measures inactivity from the most recently received chunk', async () => {
  let resolveRead;
  let rejectRead;
  let requestSignal;
  const reader = {
    read: jest.fn(
      () =>
        new Promise((resolve, reject) => {
          resolveRead = resolve;
          rejectRead = reject;
        })
    ),
    cancel: jest.fn(),
  };
  apiClient.fetchImmediate.mockImplementation(
    async (_path, _body, _method, { signal }) => {
      requestSignal = signal;
      signal.addEventListener('abort', () => rejectRead(abortError()), {
        once: true,
      });
      return {
        ok: true,
        body: { getReader: () => reader },
      };
    }
  );
  const onNewLog = jest.fn();
  const promise = streamManagedJobLogs({
    jobId: '42',
    signal: new AbortController().signal,
    onNewLog,
  });

  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(20000);
  resolveRead({ done: false, value: new Uint8Array([0x61]) });
  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(20000);
  expect(showToast).not.toHaveBeenCalled();
  resolveRead({ done: false, value: new Uint8Array([0x62]) });
  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(29999);
  expect(showToast).not.toHaveBeenCalled();

  await jest.advanceTimersByTimeAsync(1);
  await promise;

  expect(requestSignal.aborted).toBe(true);
  expect(apiClient.fetchImmediate).toHaveBeenCalledTimes(1);
  expect(reader.read).toHaveBeenCalledTimes(3);
  expect(onNewLog.mock.calls.map(([chunk]) => chunk).join('')).toBe('ab');
  expect(showToast).toHaveBeenCalledTimes(1);
  expect(jest.getTimerCount()).toBe(0);
});

it('detaches caller cancellation after a fetch failure', async () => {
  let requestSignal;
  apiClient.fetchImmediate.mockImplementation(
    async (_path, _body, _method, { signal }) => {
      requestSignal = signal;
      throw new Error('network failed');
    }
  );
  const caller = new AbortController();

  await expect(
    streamManagedJobLogs({
      jobId: '42',
      signal: caller.signal,
      onNewLog: jest.fn(),
    })
  ).rejects.toThrow('network failed');

  expect(requestSignal).not.toBe(caller.signal);
  expect(requestSignal.aborted).toBe(false);
  caller.abort();
  expect(requestSignal.aborted).toBe(false);
  expect(apiClient.fetchImmediate).toHaveBeenCalledTimes(1);
  expect(showToast).not.toHaveBeenCalled();
  expect(jest.getTimerCount()).toBe(0);
});
