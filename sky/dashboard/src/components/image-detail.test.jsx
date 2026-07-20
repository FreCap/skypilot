import { render, screen } from '@testing-library/react';

import { ImageDetail } from '@/components/image-detail';
import {
  getImageArtifactDetail,
  getImageCapabilities,
} from '@/data/connectors/images';

const mockRouter = {
  query: { image: 'image-1', workspace: 'research' },
};

jest.mock('next/router', () => ({
  useRouter: () => mockRouter,
}));

jest.mock('@/data/connectors/images', () => ({
  getImageArtifactDetail: jest.fn(),
  getImageCapabilities: jest.fn(),
}));

jest.mock('@/components/image-action-dialogs', () => ({
  PrepareImageDialog: () => null,
  RetryImageDialog: () => null,
}));

describe('Image artifact detail', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockRouter.query = { image: 'image-1', workspace: 'research' };
  });

  it('shows the API 62 callout on an old-server deep link', async () => {
    getImageCapabilities.mockRejectedValue({ status: 426, code: 'UPGRADE' });

    render(<ImageDetail />);

    expect(
      await screen.findByText('Managed Images requires API version 62')
    ).toBeVisible();
    expect(getImageArtifactDetail).not.toHaveBeenCalled();
  });

  it('keeps bounded detail truncation visible to the operator', async () => {
    getImageCapabilities.mockResolvedValue({
      workspace: 'research',
      publish: false,
    });
    getImageArtifactDetail.mockResolvedValue({
      artifact: {
        id: 'image-1',
        workspace: 'research',
        runtime_digest: `sha256:${'a'.repeat(64)}`,
        platform: 'linux/amd64',
        producer_kind: 'external_oci',
        config_digest: `sha256:${'b'.repeat(64)}`,
        manifest_size_bytes: 100,
        declared_size_bytes: 1000,
        created_at: 100,
        updated_at: 101,
      },
      releases: [],
      sources: [],
      publications: [],
      locations: [],
      demands: [],
      truncated: true,
    });

    render(<ImageDetail />);

    expect(await screen.findByText('Image artifact')).toBeVisible();
    expect(
      screen.getByText(/One detail collection exceeded 100 rows/)
    ).toBeVisible();
    expect(screen.queryByRole('button', { name: 'Prepare target' })).toBeNull();
  });
});
