import {
  API_VERSION_HEADER,
  CLIENT_API_VERSION,
  CLIENT_VERSION,
  VERSION_HEADER,
} from '@/data/connectors/constants';
import {
  apiClient,
  getCurrentUserInfo,
  getCurrentUserRole,
  resetCurrentUserCacheForTests,
} from '@/data/connectors/client';
import {
  getRequestActivitySnapshot,
  resetRequestActivityForTests,
} from '@/lib/request-activity';
import { TextDecoder as NodeTextDecoder, TextEncoder } from 'util';

describe('dashboard API contract', () => {
  it('advertises preemptible SkyServe attribution support', () => {
    expect(CLIENT_API_VERSION).toBe('78');
  });
});

describe('current user cache', () => {
  let originalTextDecoder;

  beforeEach(() => {
    resetCurrentUserCacheForTests();
    resetRequestActivityForTests();
    global.fetch.mockReset();
    originalTextDecoder = global.TextDecoder;
    global.TextDecoder = NodeTextDecoder;
  });

  afterEach(() => {
    resetRequestActivityForTests();
    global.TextDecoder = originalTextDecoder;
    window.history.pushState({}, '', '/');
    jest.restoreAllMocks();
  });

  it('tracks dashboard client calls while they are in flight', async () => {
    let resolveResponse;
    global.fetch.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveResponse = resolve;
        })
    );

    const responsePromise = apiClient.get('/api/health');
    expect(getRequestActivitySnapshot().inFlight).toBe(1);
    expect(getRequestActivitySnapshot().history.at(-1).count).toBe(1);

    resolveResponse({ ok: true });
    await expect(responsePromise).resolves.toEqual({ ok: true });
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('keeps streamed requests active until the reader finishes', async () => {
    let resolveRead;
    const read = jest
      .fn()
      .mockImplementationOnce(
        () =>
          new Promise((resolve) => {
            resolveRead = resolve;
          })
      )
      .mockResolvedValueOnce({ done: true });
    global.fetch.mockResolvedValue({
      ok: true,
      body: {
        getReader: () => ({
          read,
        }),
      },
    });

    const onData = jest.fn();
    const streamPromise = apiClient.stream('/stream', {}, onData);

    await Promise.resolve();
    await Promise.resolve();
    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(getRequestActivitySnapshot().inFlight).toBe(1);
    expect(getRequestActivitySnapshot().history.at(-1).count).toBe(1);
    expect(typeof resolveRead).toBe('function');

    resolveRead({
      done: false,
      value: Uint8Array.from([104, 101, 108, 108, 111]),
    });
    await Promise.resolve();
    await Promise.resolve();

    expect(onData).toHaveBeenCalledWith('hello');
    expect(getRequestActivitySnapshot().inFlight).toBe(1);

    await expect(streamPromise).resolves.toBeUndefined();
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('preserves utf-8 characters split across streamed chunks', async () => {
    const encoded = new TextEncoder().encode('A🙂B');
    const read = jest
      .fn()
      .mockResolvedValueOnce({ done: false, value: encoded.slice(0, 3) })
      .mockResolvedValueOnce({ done: false, value: encoded.slice(3) })
      .mockResolvedValueOnce({ done: true });
    global.fetch.mockResolvedValue({
      ok: true,
      body: {
        getReader: () => ({ read }),
      },
    });

    const chunks = [];
    await apiClient.stream('/stream', {}, (chunk) => chunks.push(chunk));

    expect(chunks.join('')).toBe('A🙂B');
    expect(chunks).not.toContain('');
  });

  it('creates one decoder for the entire stream', async () => {
    const decoder = {
      decode: jest
        .fn()
        .mockReturnValueOnce('first')
        .mockReturnValueOnce('second')
        .mockReturnValueOnce(''),
    };
    const Decoder = jest.fn(() => decoder);
    global.TextDecoder = Decoder;
    const first = Uint8Array.from([1]);
    const second = Uint8Array.from([2]);
    const read = jest
      .fn()
      .mockResolvedValueOnce({ done: false, value: first })
      .mockResolvedValueOnce({ done: false, value: second })
      .mockResolvedValueOnce({ done: true });
    global.fetch.mockResolvedValue({
      ok: true,
      body: {
        getReader: () => ({ read }),
      },
    });

    const onData = jest.fn();
    await apiClient.stream('/stream', {}, onData);

    expect(Decoder).toHaveBeenCalledTimes(1);
    expect(decoder.decode).toHaveBeenNthCalledWith(1, first, {
      stream: true,
    });
    expect(decoder.decode).toHaveBeenNthCalledWith(2, second, {
      stream: true,
    });
    expect(decoder.decode).toHaveBeenNthCalledWith(3);
    expect(onData).toHaveBeenNthCalledWith(1, 'first');
    expect(onData).toHaveBeenNthCalledWith(2, 'second');
  });

  it('cleans up streamed requests when the reader fails', async () => {
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    global.fetch.mockResolvedValue({
      ok: true,
      body: {
        getReader: () => ({
          read: jest.fn().mockRejectedValue(new Error('stream broke')),
        }),
      },
    });

    await expect(apiClient.stream('/stream', {}, jest.fn())).rejects.toThrow(
      'stream broke'
    );
    expect(consoleError).toHaveBeenCalled();
    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('releases stream accounting when the response is not ok', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    global.fetch.mockResolvedValue({ ok: false, status: 502 });

    await expect(apiClient.stream('/stream', {}, jest.fn())).rejects.toThrow(
      'status 502'
    );
    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('releases stream accounting when the reader aborts mid-stream', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    const controller = new AbortController();
    const abortError = new Error('aborted');
    abortError.name = 'AbortError';
    let receivedSignal;
    let markReaderWaiting;
    const readerWaiting = new Promise((resolve) => {
      markReaderWaiting = resolve;
    });
    const read = jest
      .fn()
      .mockResolvedValueOnce({
        done: false,
        value: Uint8Array.from([104, 105]),
      })
      .mockImplementationOnce(
        () =>
          new Promise((_, reject) => {
            controller.signal.addEventListener(
              'abort',
              () => reject(abortError),
              {
                once: true,
              }
            );
            markReaderWaiting();
          })
      );
    global.fetch.mockImplementation(async (_url, options) => {
      receivedSignal = options.signal;
      return {
        ok: true,
        body: {
          getReader: () => ({ read }),
        },
      };
    });

    const onData = jest.fn();
    const streamPromise = apiClient.stream('/stream', {}, onData, {
      signal: controller.signal,
    });
    await readerWaiting;
    expect(onData).toHaveBeenCalledWith('hi');
    expect(getRequestActivitySnapshot().inFlight).toBe(1);

    controller.abort();
    await expect(streamPromise).rejects.toMatchObject({ name: 'AbortError' });
    expect(receivedSignal).toBe(controller.signal);
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('ignores caller trackRequest overrides on stream requests', async () => {
    const inFlightSeen = [];
    global.fetch.mockImplementation(async () => {
      inFlightSeen.push(getRequestActivitySnapshot().inFlight);
      return {
        ok: true,
        body: {
          getReader: () => ({
            read: jest.fn().mockResolvedValue({ done: true }),
          }),
        },
      };
    });

    await apiClient.stream('/stream', {}, jest.fn(), { trackRequest: true });
    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(inFlightSeen).toEqual([1]);
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('releases stream accounting when the response has no body', async () => {
    jest.spyOn(console, 'error').mockImplementation(() => {});
    global.fetch.mockResolvedValue({ ok: true, body: null });

    await expect(apiClient.stream('/stream', {}, jest.fn())).rejects.toThrow();
    expect(getRequestActivitySnapshot().inFlight).toBe(0);
  });

  it('dedupes concurrent role and info lookups to one request', async () => {
    let resolveResponse;
    global.fetch.mockImplementation(
      () =>
        new Promise((resolve) => {
          resolveResponse = resolve;
        })
    );

    const rolePromise = getCurrentUserRole();
    const infoPromise = getCurrentUserInfo();
    const secondRolePromise = getCurrentUserRole();

    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(global.fetch).toHaveBeenCalledWith(
      'http://localhost/internal/dashboard/users/role',
      {
        headers: {
          [API_VERSION_HEADER]: CLIENT_API_VERSION,
          [VERSION_HEADER]: CLIENT_VERSION,
        },
      }
    );

    resolveResponse({
      ok: true,
      json: async () => ({
        id: '',
        name: '',
        role: 'admin',
      }),
    });

    await expect(rolePromise).resolves.toEqual({
      id: 'local',
      name: 'local',
      role: 'admin',
    });
    await expect(infoPromise).resolves.toEqual({
      id: 'local',
      name: 'local',
    });
    await expect(secondRolePromise).resolves.toEqual({
      id: 'local',
      name: 'local',
      role: 'admin',
    });

    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'local',
      name: 'local',
      role: 'admin',
    });
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('reuses cached data until the ttl expires, then refreshes', async () => {
    const nowSpy = jest.spyOn(Date, 'now');
    nowSpy.mockReturnValue(0);
    global.fetch
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          id: 'user-1',
          name: 'Alice',
          role: 'admin',
        }),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          id: 'user-2',
          name: 'Bob',
          role: 'user',
        }),
      });

    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'user-1',
      name: 'Alice',
      role: 'admin',
    });
    expect(global.fetch).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(5 * 60 * 1000 - 1);
    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'user-1',
      name: 'Alice',
      role: 'admin',
    });
    expect(global.fetch).toHaveBeenCalledTimes(1);

    nowSpy.mockReturnValue(5 * 60 * 1000 + 1);
    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'user-2',
      name: 'Bob',
      role: 'user',
    });
    expect(global.fetch).toHaveBeenCalledTimes(2);
  });

  it('normalizes failed lookups without caching them for the ttl', async () => {
    global.fetch
      .mockResolvedValueOnce({
        ok: false,
        json: async () => ({}),
      })
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          id: 'user-1',
          name: 'Alice',
          role: 'admin',
        }),
      });

    // The failure is normalized and flagged, but not cached...
    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'local',
      name: 'local',
      role: null,
      roleFetchFailed: true,
    });
    expect(global.fetch).toHaveBeenCalledTimes(1);

    // ...so the next caller retries and can recover immediately.
    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'user-1',
      name: 'Alice',
      role: 'admin',
    });
    expect(global.fetch).toHaveBeenCalledTimes(2);

    await expect(getCurrentUserInfo()).resolves.toEqual({
      id: 'user-1',
      name: 'Alice',
    });
    expect(global.fetch).toHaveBeenCalledTimes(2);
  });

  it('keeps identity fallback for thrown fetches without persisting it', async () => {
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => {});
    global.fetch
      .mockRejectedValueOnce(new Error('network down'))
      .mockResolvedValueOnce({
        ok: true,
        json: async () => ({
          id: 'user-1',
          name: 'Alice',
          role: 'admin',
        }),
      });

    await expect(getCurrentUserInfo()).resolves.toEqual({
      id: 'local',
      name: 'local',
    });
    expect(consoleError).toHaveBeenCalled();

    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'user-1',
      name: 'Alice',
      role: 'admin',
    });
    expect(global.fetch).toHaveBeenCalledTimes(2);
  });

  it('routes current-user lookups through the dashboard base path', async () => {
    window.history.pushState({}, '', '/tenant/dashboard/clusters');
    jest.resetModules();
    const {
      getCurrentUserRole: getPrefixedCurrentUserRole,
      resetCurrentUserCacheForTests: resetPrefixedCurrentUserCache,
    } = require('@/data/connectors/client');
    resetPrefixedCurrentUserCache();
    global.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        id: 'alice-id',
        name: 'alice@example.com',
        role: 'admin',
      }),
    });

    await expect(getPrefixedCurrentUserRole()).resolves.toEqual({
      id: 'alice-id',
      name: 'alice@example.com',
      role: 'admin',
    });
    expect(global.fetch.mock.calls[0][0]).toBe(
      'http://localhost/tenant/internal/dashboard/users/role'
    );
  });
});
