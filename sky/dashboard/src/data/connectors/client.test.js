import {
  API_VERSION_HEADER,
  CLIENT_API_VERSION,
  CLIENT_VERSION,
  VERSION_HEADER,
} from '@/data/connectors/constants';
import {
  getCurrentUserInfo,
  getCurrentUserRole,
  resetCurrentUserCacheForTests,
} from '@/data/connectors/client';

describe('current user cache', () => {
  beforeEach(() => {
    resetCurrentUserCacheForTests();
    global.fetch.mockReset();
  });

  afterEach(() => {
    jest.restoreAllMocks();
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

  it('normalizes failed lookups to the local non-admin user', async () => {
    global.fetch.mockResolvedValue({
      ok: false,
      json: async () => ({}),
    });

    await expect(getCurrentUserRole()).resolves.toEqual({
      id: 'local',
      name: 'local',
      role: null,
    });
    await expect(getCurrentUserInfo()).resolves.toEqual({
      id: 'local',
      name: 'local',
    });
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });
});
