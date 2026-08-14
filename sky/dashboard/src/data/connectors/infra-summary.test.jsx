import {
  getEnabledCloudsList,
  getInfraSummary,
  getWorkspaceContexts,
} from '@/data/connectors/infra';
import { apiClient } from '@/data/connectors/client';
import {
  getEnabledCloudsBatch,
  getWorkspaces,
} from '@/data/connectors/workspaces';
import dashboardCache from '@/lib/cache';

jest.mock('@/data/connectors/client', () => ({
  apiClient: { get: jest.fn() },
}));

jest.mock('@/data/connectors/workspaces', () => ({
  getEnabledCloudsBatch: jest.fn(),
  getWorkspaces: jest.fn(),
}));

function response(status, body = null) {
  return {
    status,
    ok: status >= 200 && status < 300,
    json: jest.fn().mockResolvedValue(body),
  };
}

describe('direct infrastructure summary', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    dashboardCache.clear();
  });

  it('shares one direct read across cloud and context first paint', async () => {
    apiClient.get.mockResolvedValue(
      response(200, {
        version: 1,
        workspaces: [
          {
            name: 'research',
            infrastructure: [
              'aws',
              'kubernetes/research-context',
              'kubernetes/research-context',
            ],
          },
          {
            name: 'burst',
            infrastructure: ['gcp', 'ssh/gpu-pool'],
          },
        ],
      })
    );

    const [contexts, clouds] = await Promise.all([
      getWorkspaceContexts(),
      getEnabledCloudsList(),
    ]);

    expect(apiClient.get).toHaveBeenCalledTimes(1);
    expect(apiClient.get).toHaveBeenCalledWith('/infra_summary');
    expect(getWorkspaces).not.toHaveBeenCalled();
    expect(getEnabledCloudsBatch).not.toHaveBeenCalled();
    expect(contexts).toEqual({
      workspaces: {
        research: {
          config: {},
          clouds: [
            'aws',
            'kubernetes/research-context',
            'kubernetes/research-context',
          ],
          contexts: ['research-context'],
        },
        burst: {
          config: {},
          clouds: ['gcp', 'ssh/gpu-pool'],
          contexts: ['ssh-gpu-pool'],
        },
      },
      allContextNames: ['research-context', 'ssh-gpu-pool'],
      contextWorkspaceMap: {
        'research-context': ['research'],
        'ssh-gpu-pool': ['burst'],
      },
    });
    expect(clouds.clouds).toEqual([
      { name: 'AWS', enabled: true },
      { name: 'GCP', enabled: true },
    ]);
  });

  it('falls back to scheduled reads when the direct route is unsupported', async () => {
    apiClient.get.mockResolvedValue(response(404));
    getWorkspaces.mockResolvedValue({ research: { private: true } });
    getEnabledCloudsBatch.mockImplementation((workspaceNames, expand) => {
      expect(workspaceNames).toEqual(['research']);
      return expand
        ? { research: ['aws', 'kubernetes/research-context'] }
        : { research: ['aws', 'kubernetes'] };
    });

    const [contexts, clouds] = await Promise.all([
      getWorkspaceContexts(),
      getEnabledCloudsList(),
    ]);

    // The cache may issue harmless background rechecks after a cached value
    // while the two legacy paths begin independently.
    expect(apiClient.get).toHaveBeenCalledTimes(2);
    expect(getWorkspaces).toHaveBeenCalledTimes(2);
    expect(getEnabledCloudsBatch).toHaveBeenCalledTimes(2);
    expect(contexts.workspaces.research.config).toEqual({ private: true });
    expect(contexts.allContextNames).toEqual(['research-context']);
    expect(clouds.clouds).toEqual([{ name: 'AWS', enabled: true }]);
  });

  it('does not cache a transient direct-read failure', async () => {
    const consoleWarn = jest
      .spyOn(console, 'warn')
      .mockImplementation(() => undefined);
    apiClient.get
      .mockResolvedValueOnce(response(503))
      .mockResolvedValueOnce(response(200, { version: 1, workspaces: [] }));

    expect((await dashboardCache.get(getInfraSummary)).available).toBe(false);
    expect((await dashboardCache.get(getInfraSummary)).available).toBe(true);
    expect(apiClient.get).toHaveBeenCalledTimes(2);
    consoleWarn.mockRestore();
  });
});
