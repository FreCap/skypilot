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
