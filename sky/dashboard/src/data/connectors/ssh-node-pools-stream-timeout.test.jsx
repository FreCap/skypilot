jest.mock('@/data/connectors/toast', () => ({
  showToast: jest.fn(),
}));

import { TextDecoder as NodeTextDecoder } from 'util';

import {
  streamSSHDeploymentLogs,
  streamSSHOperationLogs,
} from '@/data/connectors/ssh-node-pools';
import { showToast } from '@/data/connectors/toast';

function abortError() {
  return new DOMException('The operation was aborted.', 'AbortError');
}

function installStream(reads = []) {
  let requestSignal;
  let reader;
  global.fetch.mockImplementation(async (_url, { signal }) => {
    requestSignal = signal;
    reader = {
      read: jest.fn(() => {
        const next = reads.shift();
        if (next) return Promise.resolve(next);
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
    return { ok: true, body: { getReader: () => reader } };
  });
  return {
    get requestSignal() {
      return requestSignal;
    },
    get reader() {
      return reader;
    },
  };
}

const originalFetch = global.fetch;
const originalTextDecoder = global.TextDecoder;

beforeEach(() => {
  jest.useFakeTimers();
  jest.clearAllMocks();
  global.fetch = jest.fn();
  global.TextDecoder = NodeTextDecoder;
});

afterEach(() => {
  global.fetch = originalFetch;
  global.TextDecoder = originalTextDecoder;
  jest.useRealTimers();
});

it.each([
  [
    'deployment',
    streamSSHDeploymentLogs,
    {},
    'SSH deployment log stream timed out after 300s of inactivity',
  ],
  [
    'down operation',
    streamSSHOperationLogs,
    { operationType: 'down' },
    'SSH down log stream timed out after 300s of inactivity',
  ],
])(
  'aborts the owned %s request on inactivity',
  async (_, streamFn, extra, warning) => {
    const stream = installStream();
    const caller = new AbortController();
    const promise = streamFn({
      requestId: 'request-42',
      signal: caller.signal,
      onNewLog: jest.fn(),
      ...extra,
    });

    await Promise.resolve();
    expect(stream.reader.read).toHaveBeenCalledTimes(1);
    await jest.advanceTimersByTimeAsync(300000);
    await promise;

    expect(stream.requestSignal).not.toBe(caller.signal);
    expect(stream.requestSignal.aborted).toBe(true);
    expect(caller.signal.aborted).toBe(false);
    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(stream.reader.read).toHaveBeenCalledTimes(1);
    expect(showToast).toHaveBeenCalledWith(warning, 'warning');
    expect(jest.getTimerCount()).toBe(0);
  }
);

it('forwards caller cancellation without reporting inactivity', async () => {
  const stream = installStream();
  const caller = new AbortController();
  const promise = streamSSHDeploymentLogs({
    requestId: 'request-42',
    signal: caller.signal,
    onNewLog: jest.fn(),
  });

  await Promise.resolve();
  caller.abort();
  await promise;

  expect(stream.requestSignal).not.toBe(caller.signal);
  expect(stream.requestSignal.aborted).toBe(true);
  expect(global.fetch).toHaveBeenCalledTimes(1);
  expect(stream.reader.read).toHaveBeenCalledTimes(1);
  expect(showToast).not.toHaveBeenCalled();
  expect(jest.getTimerCount()).toBe(0);
});

it('bounds timeout return and cancels a transport that settles late', async () => {
  let requestSignal;
  let settleTransport;
  global.fetch.mockImplementation((_url, { signal }) => {
    requestSignal = signal;
    return new Promise((resolve) => {
      settleTransport = resolve;
    });
  });
  let settled = false;
  const promise = streamSSHDeploymentLogs({
    requestId: 'request-42',
    signal: new AbortController().signal,
    onNewLog: jest.fn(),
  }).then(() => {
    settled = true;
  });

  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(300000);
  await Promise.resolve();
  expect(settled).toBe(true);
  expect(requestSignal.aborted).toBe(true);

  const getReader = jest.fn();
  const cancel = jest.fn().mockResolvedValue(undefined);
  settleTransport({ ok: false, status: 503, body: { cancel, getReader } });
  await Promise.resolve();
  await Promise.resolve();

  expect(getReader).not.toHaveBeenCalled();
  expect(cancel).toHaveBeenCalledTimes(1);
  expect(jest.getTimerCount()).toBe(0);
});

it('preserves one fetch and one read per chunk plus EOF', async () => {
  const stream = installStream([
    { done: false, value: new Uint8Array([0x61]) },
    { done: false, value: new Uint8Array([0x62]) },
    { done: true, value: undefined },
  ]);
  const onNewLog = jest.fn();

  await streamSSHOperationLogs({
    requestId: 'request-42',
    onNewLog,
    operationType: 'down',
  });

  expect(global.fetch).toHaveBeenCalledTimes(1);
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
  global.fetch.mockImplementation(async (_url, { signal }) => {
    requestSignal = signal;
    signal.addEventListener('abort', () => rejectRead(abortError()), {
      once: true,
    });
    return { ok: true, body: { getReader: () => reader } };
  });
  const onNewLog = jest.fn();
  const promise = streamSSHDeploymentLogs({
    requestId: 'request-42',
    signal: new AbortController().signal,
    onNewLog,
  });

  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(200000);
  resolveRead({ done: false, value: new Uint8Array([0x61]) });
  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(200000);
  expect(showToast).not.toHaveBeenCalled();
  resolveRead({ done: false, value: new Uint8Array([0x62]) });
  await Promise.resolve();
  await jest.advanceTimersByTimeAsync(299999);
  expect(showToast).not.toHaveBeenCalled();

  await jest.advanceTimersByTimeAsync(1);
  await promise;

  expect(requestSignal.aborted).toBe(true);
  expect(global.fetch).toHaveBeenCalledTimes(1);
  expect(reader.read).toHaveBeenCalledTimes(3);
  expect(onNewLog.mock.calls.map(([chunk]) => chunk).join('')).toBe('ab');
  expect(showToast).toHaveBeenCalledTimes(1);
  expect(jest.getTimerCount()).toBe(0);
});

it('detaches caller cancellation after a fetch failure', async () => {
  let requestSignal;
  global.fetch.mockImplementation(async (_url, { signal }) => {
    requestSignal = signal;
    throw new Error('network failed');
  });
  const caller = new AbortController();

  await expect(
    streamSSHDeploymentLogs({
      requestId: 'request-42',
      signal: caller.signal,
      onNewLog: jest.fn(),
    })
  ).rejects.toThrow('network failed');

  expect(requestSignal).not.toBe(caller.signal);
  expect(requestSignal.aborted).toBe(false);
  caller.abort();
  expect(requestSignal.aborted).toBe(false);
  expect(global.fetch).toHaveBeenCalledTimes(1);
  expect(jest.getTimerCount()).toBe(0);
});
