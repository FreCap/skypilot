import { apiClient } from '@/data/connectors/client';
import {
  getImageArtifactCollection,
  getImageArtifactDetail,
  getImageCatalog,
  ImageApiError,
  publishImage,
} from '@/data/connectors/images';

jest.mock('@/data/connectors/client', () => ({
  apiClient: {
    get: jest.fn(),
    post: jest.fn(),
  },
}));

const response = (payload, options = {}) => ({
  ok: options.ok ?? true,
  status: options.status ?? 200,
  json: jest.fn().mockResolvedValue(payload),
});

describe('managed image connectors', () => {
  beforeEach(() => {
    jest.clearAllMocks();
  });

  it('encodes filters and omits empty query values', async () => {
    apiClient.get.mockResolvedValue(response({ items: [] }));
    const controller = new AbortController();

    await getImageCatalog(
      {
        workspace: 'research / west',
        release: 'boltz+l4',
        cursor: '',
        state: 'FAILED',
        target: 'aws-us-east-1',
        limit: 30,
      },
      controller.signal
    );

    const [path, options] = apiClient.get.mock.calls[0];
    expect(path).toBe(
      '/images/catalog?workspace=research+%2F+west&limit=30&release=boltz%2Bl4'
    );
    expect(options.signal).toBe(controller.signal);
  });

  it('sends an explicit idempotency key without changing the body', async () => {
    const body = {
      workspace: 'research',
      release: 'boltz-l4',
      source_ref: `ghcr.io/boltz/image@sha256:${'a'.repeat(64)}`,
    };
    apiClient.post.mockResolvedValue(response({ operation: { id: 'op-1' } }));

    await publishImage(body, 'stable-key', undefined);

    expect(apiClient.post).toHaveBeenCalledWith('/images/publications', body, {
      signal: undefined,
      headers: { 'Idempotency-Key': 'stable-key' },
    });
  });

  it('maps server failures to a bounded typed error', async () => {
    apiClient.get.mockResolvedValue(
      response(
        { detail: { code: 'STALE_IMAGE_CURSOR', provider_text: 'secret' } },
        { ok: false, status: 409 }
      )
    );

    await expect(getImageCatalog({ workspace: 'research' })).rejects.toEqual(
      expect.objectContaining({
        name: 'ImageApiError',
        code: 'STALE_IMAGE_CURSOR',
        status: 409,
      })
    );
    await expect(
      getImageCatalog({ workspace: 'research' })
    ).rejects.toBeInstanceOf(ImageApiError);
  });

  it('loads bounded detail collections in parallel and reports truncation', async () => {
    const pending = [];
    apiClient.get.mockImplementation((path) => {
      pending.push(path);
      if (path.startsWith('/images/artifacts/image-1?')) {
        return Promise.resolve(response({ artifact: { id: 'image-1' } }));
      }
      const relation = path.match(/image-1\/([^?]+)/)?.[1];
      return Promise.resolve(
        response({
          items: [{ id: relation }],
          next_cursor: relation === 'locations' ? 'next' : null,
        })
      );
    });

    const detail = await getImageArtifactDetail('image-1', 'research');

    expect(apiClient.get).toHaveBeenCalledTimes(6);
    expect(pending).toEqual(
      expect.arrayContaining([
        '/images/artifacts/image-1?workspace=research',
        '/images/artifacts/image-1/releases?workspace=research&limit=100',
        '/images/artifacts/image-1/sources?workspace=research&limit=100',
        '/images/artifacts/image-1/publications?workspace=research&limit=100',
        '/images/artifacts/image-1/locations?workspace=research&limit=100',
        '/images/artifacts/image-1/demands?workspace=research&limit=100',
      ])
    );
    expect(detail.artifact).toEqual({ id: 'image-1' });
    expect(detail.locations).toEqual([{ id: 'locations' }]);
    expect(detail.next_cursors).toEqual({
      releases: null,
      sources: null,
      publications: null,
      locations: 'next',
      demands: null,
    });
    expect(detail.truncated).toBe(true);
  });

  it('pages one artifact collection without loading the other collections', async () => {
    apiClient.get.mockResolvedValue(
      response({ items: [{ id: 'location-101' }], next_cursor: 'next-2' })
    );

    const page = await getImageArtifactCollection('image/1', 'locations', {
      workspace: 'research',
      limit: 100,
      cursor: 'opaque + cursor',
    });

    expect(apiClient.get).toHaveBeenCalledWith(
      '/images/artifacts/image%2F1/locations?workspace=research&limit=100&cursor=opaque+%2B+cursor',
      { signal: undefined }
    );
    expect(page.items).toEqual([{ id: 'location-101' }]);
  });

  it('rejects an unknown artifact collection before issuing a request', () => {
    expect(() =>
      getImageArtifactCollection('image-1', 'credentials', {
        workspace: 'research',
      })
    ).toThrow('Unknown image artifact collection');
    expect(apiClient.get).not.toHaveBeenCalled();
  });
});
