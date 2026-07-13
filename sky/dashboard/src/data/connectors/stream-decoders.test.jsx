// Regression tests for streamed log decoding: every log stream must reuse a
// single streaming TextDecoder so multi-byte UTF-8 characters split across
// response chunks are reconstructed instead of decoding to replacement
// characters (follow-up to the client.js fix in PR #218).

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

import { TextDecoder as NodeTextDecoder } from 'util';

import { apiClient } from '@/data/connectors/client';
import { streamManagedJobLogs } from '@/data/connectors/jobs';
import {
  streamSSHDeploymentLogs,
  streamSSHOperationLogs,
} from '@/data/connectors/ssh-node-pools';

// 'A🙂B' encoded as UTF-8, split in the middle of the 4-byte emoji.
const SPLIT_UTF8_CHUNKS = [
  new Uint8Array([0x41, 0xf0, 0x9f]),
  new Uint8Array([0x99, 0x82, 0x42]),
];

function makeStreamResponse(chunks) {
  let i = 0;
  return {
    ok: true,
    body: {
      getReader: () => ({
        read: jest.fn(async () =>
          i < chunks.length
            ? { done: false, value: chunks[i++] }
            : { done: true, value: undefined }
        ),
        cancel: jest.fn(),
      }),
    },
  };
}

let originalTextDecoder;

beforeEach(() => {
  originalTextDecoder = global.TextDecoder;
  global.TextDecoder = NodeTextDecoder;
});

afterEach(() => {
  global.TextDecoder = originalTextDecoder;
  jest.restoreAllMocks();
  jest.clearAllMocks();
});

it('streamManagedJobLogs reassembles utf-8 split across chunks', async () => {
  apiClient.fetchImmediate.mockResolvedValue(
    makeStreamResponse(SPLIT_UTF8_CHUNKS)
  );
  const received = [];
  await streamManagedJobLogs({
    jobId: '1',
    onNewLog: (chunk) => received.push(chunk),
  });
  expect(received.join('')).toBe('A🙂B');
  expect(received.join('')).not.toContain('�');
});

it('streamSSHDeploymentLogs reassembles utf-8 split across chunks', async () => {
  jest
    .spyOn(global, 'fetch')
    .mockResolvedValue(makeStreamResponse(SPLIT_UTF8_CHUNKS));
  const received = [];
  await streamSSHDeploymentLogs({
    requestId: 'req-1',
    onNewLog: (chunk) => received.push(chunk),
  });
  expect(received.join('')).toBe('A🙂B');
});

it('streamSSHOperationLogs reassembles utf-8 split across chunks', async () => {
  jest
    .spyOn(global, 'fetch')
    .mockResolvedValue(makeStreamResponse(SPLIT_UTF8_CHUNKS));
  const received = [];
  await streamSSHOperationLogs({
    requestId: 'req-2',
    onNewLog: (chunk) => received.push(chunk),
  });
  expect(received.join('')).toBe('A🙂B');
});

it('streamManagedJobLogs flushes dangling incomplete bytes at EOF', async () => {
  // Stream ends mid-character: the decoder flush must still deliver the
  // replacement character rather than silently dropping the bytes.
  apiClient.fetchImmediate.mockResolvedValue(
    makeStreamResponse([new Uint8Array([0x41, 0xf0, 0x9f])])
  );
  const received = [];
  await streamManagedJobLogs({
    jobId: '1',
    onNewLog: (chunk) => received.push(chunk),
  });
  expect(received.join('')).toBe('A�');
});
