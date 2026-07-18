jest.mock('@/data/connectors/client', () => ({
  __esModule: true,
  apiClient: {
    fetch: jest.fn(),
  },
}));

import { apiClient } from '@/data/connectors/client';
import { getVolumes } from '@/data/connectors/volumes';

describe('getVolumes', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    apiClient.fetch.mockResolvedValue([]);
  });

  it('keeps the collection request unfiltered', async () => {
    await getVolumes();

    expect(apiClient.fetch).toHaveBeenCalledTimes(1);
    expect(apiClient.fetch).toHaveBeenCalledWith('/volumes', {}, 'GET');
  });

  it('encodes one target name for a detail request', async () => {
    await getVolumes({ name: 'team/volume one' });

    expect(apiClient.fetch).toHaveBeenCalledTimes(1);
    expect(apiClient.fetch).toHaveBeenCalledWith(
      '/volumes?name=team%2Fvolume%20one',
      {},
      'GET'
    );
  });
});
