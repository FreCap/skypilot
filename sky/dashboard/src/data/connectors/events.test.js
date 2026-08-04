import { apiClient } from '@/data/connectors/client';
import {
  getOperationalEvents,
  OperationalEventApiError,
  OPERATIONAL_EVENTS_UNAVAILABLE,
  OPERATIONAL_EVENTS_UPGRADE_REQUIRED,
} from '@/data/connectors/events';

jest.mock('@/data/connectors/client', () => ({
  apiClient: {
    get: jest.fn(),
  },
}));

const response = (payload, options = {}) => ({
  ok: options.ok ?? true,
  status: options.status ?? 200,
  json: jest.fn().mockResolvedValue(payload),
});

describe('operational event connector', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('uses the direct GET API with stable cluster and workspace filters', async () => {
    apiClient.get.mockResolvedValue(response({ items: [] }));
    const controller = new AbortController();

    await getOperationalEvents(
      {
        clusterHash: 'cluster/hash',
        clusterName: 'ignored-name',
        workspace: 'research west',
        limit: 20,
        cursor: 'next+cursor',
      },
      controller.signal
    );

    expect(apiClient.get).toHaveBeenCalledWith(
      '/events?workspace=research+west&target_type=cluster&target_id=cluster%2Fhash&limit=20&cursor=next%2Bcursor',
      { signal: controller.signal }
    );
  });

  it('falls back to cluster name when no stable generation exists', async () => {
    apiClient.get.mockResolvedValue(response({ items: [] }));

    await getOperationalEvents({ clusterName: 'failed launch', limit: 20 });

    expect(apiClient.get.mock.calls[0][0]).toBe(
      '/events?target_type=cluster&target_name=failed+launch&limit=20'
    );
  });

  it('maps unavailable and missing APIs to bounded typed errors', async () => {
    apiClient.get.mockResolvedValueOnce(
      response(
        { detail: { code: OPERATIONAL_EVENTS_UNAVAILABLE, secret: 'nope' } },
        { ok: false, status: 503 }
      )
    );
    await expect(getOperationalEvents({ clusterHash: 'hash' })).rejects.toEqual(
      expect.objectContaining({
        name: 'OperationalEventApiError',
        code: OPERATIONAL_EVENTS_UNAVAILABLE,
        status: 503,
      })
    );

    apiClient.get.mockResolvedValueOnce(
      response({}, { ok: false, status: 404 })
    );
    await expect(getOperationalEvents({ clusterHash: 'hash' })).rejects.toEqual(
      expect.objectContaining({
        code: OPERATIONAL_EVENTS_UPGRADE_REQUIRED,
        status: 404,
      })
    );
    await expect(
      Promise.reject(new OperationalEventApiError('TEST', 400))
    ).rejects.toBeInstanceOf(OperationalEventApiError);
  });
});
