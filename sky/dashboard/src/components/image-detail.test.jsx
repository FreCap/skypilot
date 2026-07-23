import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import { ImageDetail } from '@/components/image-detail';
import {
  getImageArtifactCollection,
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
  getImageArtifactCollection: jest.fn(),
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
    getImageArtifactCollection.mockResolvedValue({
      items: [],
      next_cursor: null,
    });
  });

  it('shows the API 62 callout on an old-server deep link', async () => {
    getImageCapabilities.mockRejectedValue({ status: 426, code: 'UPGRADE' });

    render(<ImageDetail />);

    expect(
      await screen.findByText('Managed Images requires API version 62')
    ).toBeVisible();
    expect(getImageArtifactDetail).not.toHaveBeenCalled();
  });

  it('pages one bounded detail collection inside the Dashboard', async () => {
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
      locations: [
        {
          id: 'location-1',
          distribution: 'gpu-production',
          target_id: 'west',
          target_ref: `registry/west@sha256:${'a'.repeat(64)}`,
          state: 'READY',
          canonical: false,
          error_code: null,
          last_verified_at: 100,
          attempt_count: 1,
        },
      ],
      demands: [],
      next_cursors: {
        releases: null,
        sources: null,
        publications: null,
        locations: 'locations-next',
        demands: null,
      },
      truncated: true,
    });
    getImageArtifactCollection
      .mockResolvedValueOnce({
        items: [
          {
            id: 'location-101',
            distribution: 'gpu-production',
            target_id: 'east',
            target_ref: `registry/east@sha256:${'b'.repeat(64)}`,
            state: 'READY',
            canonical: false,
            error_code: null,
            last_verified_at: 101,
            attempt_count: 1,
          },
        ],
        next_cursor: null,
      })
      .mockResolvedValueOnce({
        items: [
          {
            id: 'location-1',
            distribution: 'gpu-production',
            target_id: 'west',
            target_ref: `registry/west@sha256:${'a'.repeat(64)}`,
            state: 'READY',
            canonical: false,
            error_code: null,
            last_verified_at: 100,
            attempt_count: 1,
          },
        ],
        next_cursor: 'locations-next',
      });

    render(<ImageDetail />);

    expect(await screen.findByText('Image artifact')).toBeVisible();
    fireEvent.click(
      screen.getByRole('button', { name: 'Next locations page' })
    );
    expect(await screen.findByText('east')).toBeVisible();
    expect(screen.queryByText('west')).toBeNull();
    expect(getImageArtifactCollection).toHaveBeenCalledWith(
      'image-1',
      'locations',
      {
        workspace: 'research',
        limit: 100,
        cursor: 'locations-next',
      },
      expect.anything()
    );
    expect(
      screen.getByRole('button', { name: 'Previous locations page' })
    ).toBeEnabled();
    expect(screen.queryByRole('button', { name: 'Prepare target' })).toBeNull();

    fireEvent.click(
      screen.getByRole('button', { name: 'First locations page' })
    );
    expect(await screen.findByText('west')).toBeVisible();
    expect(
      getImageArtifactCollection.mock.calls.map(
        ([, , options]) => options.cursor
      )
    ).toEqual(['locations-next', null]);
  });

  it('recovers only the stale detail collection at its first page', async () => {
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
      next_cursors: { locations: 'stale-locations' },
      truncated: true,
    });
    getImageArtifactCollection
      .mockRejectedValueOnce({ code: 'STALE_IMAGE_CURSOR' })
      .mockResolvedValueOnce({ items: [], next_cursor: null });

    render(<ImageDetail />);
    fireEvent.click(
      await screen.findByRole('button', { name: 'Next locations page' })
    );

    expect(
      await screen.findByText(
        'The locations collection changed while paging. Reloaded the first page.'
      )
    ).toBeVisible();
    expect(
      getImageArtifactCollection.mock.calls.map(
        ([, , options]) => options.cursor
      )
    ).toEqual(['stale-locations', null]);
    expect(
      screen.getByRole('button', { name: 'Previous locations page' })
    ).toBeDisabled();
  });

  it('makes cached artifact mutations read-only after refresh failure', async () => {
    getImageCapabilities
      .mockResolvedValueOnce({
        workspace: 'research',
        publish: true,
      })
      .mockRejectedValueOnce({ code: 'REFRESH_FAILED' });
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
      locations: [
        {
          id: 'location-1',
          distribution: 'gpu-production',
          target_id: 'west',
          target_ref: `registry/repository@sha256:${'a'.repeat(64)}`,
          state: 'FAILED',
          canonical: false,
          error_code: 'COPY_FAILED',
          last_verified_at: null,
          attempt_count: 1,
        },
      ],
      demands: [],
      truncated: false,
    });

    render(<ImageDetail />);
    const prepare = await screen.findByRole('button', {
      name: 'Prepare target',
    });
    expect(prepare).toBeEnabled();
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));

    await waitFor(() =>
      expect(screen.getByRole('alert')).toHaveTextContent(
        /Cached artifact data is read-only/
      )
    );
    expect(prepare).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeDisabled();
  });

  it('does not relabel a redacted source binding as public', async () => {
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
      sources: [
        {
          id: 'source-1',
          source_ref: `ghcr.io/boltz/runtime@sha256:${'a'.repeat(64)}`,
          requested_platform: 'linux/amd64',
          source_auth_binding_id: null,
        },
      ],
      publications: [],
      locations: [],
      demands: [],
      truncated: false,
    });

    render(<ImageDetail />);

    expect(await screen.findByText('Retained sources')).toBeVisible();
    expect(screen.getAllByText('linux/amd64').length).toBeGreaterThan(0);
    expect(screen.queryByText('public')).toBeNull();
    expect(screen.queryByText(/binding/)).toBeNull();
  });
});
