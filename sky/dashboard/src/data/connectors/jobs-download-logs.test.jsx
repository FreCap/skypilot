jest.mock('@/data/connectors/toast', () => ({
  showToast: jest.fn(),
}));

jest.mock('@/lib/analytics', () => ({
  trackJobAction: jest.fn(),
}));

jest.mock('@/plugins/dataEnhancement', () => ({
  applyEnhancements: jest.fn((value) => value),
}));

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

jest.mock('@/lib/jobs-cache-manager', () => ({
  __esModule: true,
  default: {},
}));

describe('downloadManagedJobLogs', () => {
  let anchorClick;
  let createElementSpy;
  let originalCreateObjectURL;
  let originalRevokeObjectURL;

  beforeEach(() => {
    jest.resetModules();
    global.fetch.mockReset();
    window.history.pushState({}, '', '/dashboard/jobs/55');

    anchorClick = jest.fn();
    createElementSpy = jest
      .spyOn(document, 'createElement')
      .mockImplementation((tagName, options) => {
        const element = document.createElementNS(
          'http://www.w3.org/1999/xhtml',
          tagName,
          options
        );
        if (tagName === 'a') {
          element.click = anchorClick;
        }
        return element;
      });

    originalCreateObjectURL = window.URL.createObjectURL;
    originalRevokeObjectURL = window.URL.revokeObjectURL;
    window.URL.createObjectURL = jest.fn(() => 'blob:managed-job-logs');
    window.URL.revokeObjectURL = jest.fn();
  });

  afterEach(() => {
    createElementSpy.mockRestore();
    window.URL.createObjectURL = originalCreateObjectURL;
    window.URL.revokeObjectURL = originalRevokeObjectURL;
  });

  it('reuses the normalized cached dashboard user for both download steps', async () => {
    let dispatchBody;
    let zipBody;

    global.fetch.mockImplementation(async (url, options = {}) => {
      if (url.endsWith('/internal/dashboard/users/role')) {
        return {
          ok: true,
          json: async () => ({
            id: '',
            name: '',
            role: 'admin',
          }),
        };
      }

      if (url.endsWith('/internal/dashboard/jobs/download_logs')) {
        dispatchBody = JSON.parse(options.body);
        return {
          ok: true,
          status: 200,
          headers: {
            get: (name) =>
              name === 'X-Skypilot-Request-ID' ? 'request-123' : null,
          },
        };
      }

      if (url.includes('/internal/dashboard/api/get?request_id=request-123')) {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            return_value: JSON.stringify({
              55: '~/.sky/api_server/clients/local/sky_logs/managed_jobs/job-55',
            }),
          }),
        };
      }

      if (url.includes('/internal/dashboard/download?relative=items')) {
        zipBody = JSON.parse(options.body);
        return {
          ok: true,
          status: 200,
          blob: async () => new Blob(['zip-bytes']),
        };
      }

      throw new Error(`Unexpected fetch: ${url}`);
    });

    const { downloadManagedJobLogs } = await import('@/data/connectors/jobs');

    await downloadManagedJobLogs({ jobId: 55 });

    expect(
      global.fetch.mock.calls.filter(([url]) =>
        url.endsWith('/internal/dashboard/users/role')
      )
    ).toHaveLength(1);

    expect(dispatchBody.env_vars).toMatchObject({
      SKYPILOT_IS_FROM_DASHBOARD: 'true',
      SKYPILOT_USER_ID: 'local',
      SKYPILOT_USER: 'local',
    });
    expect(zipBody.env_vars).toMatchObject({
      SKYPILOT_IS_FROM_DASHBOARD: 'true',
      SKYPILOT_USER_ID: 'local',
      SKYPILOT_USER: 'local',
    });
    expect(zipBody.folder_paths).toEqual([
      '~/.sky/api_server/clients/local/sky_logs/managed_jobs/job-55',
    ]);
    expect(anchorClick).toHaveBeenCalledTimes(1);
    expect(window.URL.createObjectURL).toHaveBeenCalledTimes(1);
    expect(window.URL.revokeObjectURL).toHaveBeenCalledWith(
      'blob:managed-job-logs'
    );
  });
});
