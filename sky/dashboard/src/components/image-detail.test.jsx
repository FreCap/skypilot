import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

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
  PrepareImageDialog: ({ open, workspace, artifact }) =>
    open ? (
      <div data-testid="prepare-dialog">
        {workspace}:{artifact.id}
      </div>
    ) : null,
  RetryImageDialog: ({ open, workspace, recordId }) =>
    open ? (
      <div data-testid="retry-dialog">
        {workspace}:{recordId}
      </div>
    ) : null,
}));

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function detailFor(workspace, imageId, digestCharacter) {
  return {
    artifact: {
      id: imageId,
      workspace,
      runtime_digest: `sha256:${digestCharacter.repeat(64)}`,
      platform: 'linux/amd64',
      producer_kind: 'external_oci',
      config_digest: `sha256:${'c'.repeat(64)}`,
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
        id: `location-${imageId}`,
        distribution: 'gpu-production',
        target_id: 'west',
        target_ref: `registry/repository@sha256:${digestCharacter.repeat(64)}`,
        state: 'FAILED',
        canonical: false,
        error_code: 'COPY_FAILED',
        last_verified_at: null,
        attempt_count: 1,
      },
    ],
    demands: [],
    next_cursors: {
      releases: null,
      sources: null,
      publications: null,
      locations: null,
      demands: null,
    },
    truncated: false,
  };
}

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

  it('hides the previous compound route scope before replacement detail loads', async () => {
    const replacementCapabilities = deferred();
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'workspace-a', publish: true })
      .mockReturnValueOnce(replacementCapabilities.promise);
    getImageArtifactDetail
      .mockResolvedValueOnce(detailFor('workspace-a', 'image-1', 'a'))
      .mockResolvedValueOnce(detailFor('workspace-b', 'image-2', 'b'));
    mockRouter.query = { image: 'image-1', workspace: 'workspace-a' };

    const view = render(<ImageDetail />);
    expect(await screen.findByText(`sha256:${'a'.repeat(64)}`)).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Prepare target' }));
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    expect(screen.getByTestId('prepare-dialog')).toHaveTextContent(
      'workspace-a:image-1'
    );
    expect(screen.getByTestId('retry-dialog')).toHaveTextContent(
      'workspace-a:location-image-1'
    );

    mockRouter.query = { image: 'image-2', workspace: 'workspace-b' };
    view.rerender(<ImageDetail />);

    expect(screen.getByText('Loading artifact…')).toBeVisible();
    expect(screen.queryByText(`sha256:${'a'.repeat(64)}`)).toBeNull();
    expect(screen.queryByRole('button', { name: 'Prepare target' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
    expect(screen.queryByTestId('prepare-dialog')).toBeNull();
    expect(screen.queryByTestId('retry-dialog')).toBeNull();

    replacementCapabilities.resolve({
      workspace: 'workspace-b',
      publish: true,
    });
    expect(await screen.findByText(`sha256:${'b'.repeat(64)}`)).toBeVisible();
    expect(getImageArtifactDetail).toHaveBeenLastCalledWith(
      'image-2',
      'workspace-b',
      expect.anything()
    );
  });

  it('does not restore a previous artifact when same-workspace replacement fails', async () => {
    const replacementCapabilities = deferred();
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'workspace-a', publish: true })
      .mockReturnValueOnce(replacementCapabilities.promise);
    getImageArtifactDetail.mockResolvedValueOnce(
      detailFor('workspace-a', 'image-1', 'a')
    );
    mockRouter.query = { image: 'image-1', workspace: 'workspace-a' };

    const view = render(<ImageDetail />);
    expect(await screen.findByText(`sha256:${'a'.repeat(64)}`)).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Prepare target' }));

    mockRouter.query = { image: 'image-2', workspace: 'workspace-a' };
    view.rerender(<ImageDetail />);
    replacementCapabilities.reject({
      status: 403,
      code: 'ARTIFACT_FORBIDDEN',
    });

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'ARTIFACT_FORBIDDEN'
    );
    expect(screen.queryByText(`sha256:${'a'.repeat(64)}`)).toBeNull();
    expect(screen.queryByTestId('prepare-dialog')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Prepare target' })).toBeNull();
  });

  it('does not expose an artifact across a workspace-only route change', async () => {
    const replacementCapabilities = deferred();
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'workspace-a', publish: true })
      .mockReturnValueOnce(replacementCapabilities.promise);
    getImageArtifactDetail.mockResolvedValueOnce(
      detailFor('workspace-a', 'image-1', 'a')
    );
    mockRouter.query = { image: 'image-1', workspace: 'workspace-a' };

    const view = render(<ImageDetail />);
    expect(await screen.findByText(`sha256:${'a'.repeat(64)}`)).toBeVisible();

    mockRouter.query = { image: 'image-1', workspace: 'workspace-b' };
    view.rerender(<ImageDetail />);
    expect(screen.getByText('Loading artifact…')).toBeVisible();
    expect(screen.queryByText(`sha256:${'a'.repeat(64)}`)).toBeNull();
    replacementCapabilities.reject({
      status: 403,
      code: 'WORKSPACE_FORBIDDEN',
    });

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'WORKSPACE_FORBIDDEN'
    );
    expect(screen.queryByText(`sha256:${'a'.repeat(64)}`)).toBeNull();
  });

  it('drops a late collection page after the compound route scope changes', async () => {
    const oldCollection = deferred();
    const oldDetail = detailFor('workspace-a', 'image-1', 'a');
    oldDetail.next_cursors.locations = 'old-next';
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'workspace-a', publish: false })
      .mockResolvedValueOnce({ workspace: 'workspace-b', publish: false });
    getImageArtifactDetail
      .mockResolvedValueOnce(oldDetail)
      .mockResolvedValueOnce(detailFor('workspace-b', 'image-2', 'b'));
    getImageArtifactCollection.mockReturnValueOnce(oldCollection.promise);
    mockRouter.query = { image: 'image-1', workspace: 'workspace-a' };

    const view = render(<ImageDetail />);
    fireEvent.click(
      await screen.findByRole('button', { name: 'Next locations page' })
    );

    mockRouter.query = { image: 'image-2', workspace: 'workspace-b' };
    view.rerender(<ImageDetail />);
    expect(await screen.findByText(`sha256:${'b'.repeat(64)}`)).toBeVisible();

    await act(async () => {
      oldCollection.resolve({
        items: [
          {
            id: 'late-location',
            distribution: 'gpu-production',
            target_id: 'late-target',
            target_ref: `registry/late@sha256:${'a'.repeat(64)}`,
            state: 'READY',
            canonical: false,
            error_code: null,
            last_verified_at: 102,
            attempt_count: 1,
          },
        ],
        next_cursor: null,
      });
      await oldCollection.promise;
    });

    expect(screen.getByText(`sha256:${'b'.repeat(64)}`)).toBeVisible();
    expect(screen.queryByText('late-target')).toBeNull();
  });

  it('drops a late stale-cursor fallback notice after the route changes', async () => {
    const oldFallback = deferred();
    const oldDetail = detailFor('workspace-a', 'image-1', 'a');
    oldDetail.next_cursors.locations = 'stale-next';
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'workspace-a', publish: false })
      .mockResolvedValueOnce({ workspace: 'workspace-b', publish: false });
    getImageArtifactDetail
      .mockResolvedValueOnce(oldDetail)
      .mockResolvedValueOnce(detailFor('workspace-b', 'image-2', 'b'));
    getImageArtifactCollection
      .mockRejectedValueOnce({ code: 'STALE_IMAGE_CURSOR' })
      .mockReturnValueOnce(oldFallback.promise);
    mockRouter.query = { image: 'image-1', workspace: 'workspace-a' };

    const view = render(<ImageDetail />);
    fireEvent.click(
      await screen.findByRole('button', { name: 'Next locations page' })
    );
    await waitFor(() =>
      expect(getImageArtifactCollection).toHaveBeenCalledTimes(2)
    );

    mockRouter.query = { image: 'image-2', workspace: 'workspace-b' };
    view.rerender(<ImageDetail />);
    expect(await screen.findByText(`sha256:${'b'.repeat(64)}`)).toBeVisible();

    await act(async () => {
      oldFallback.resolve({ items: [], next_cursor: null });
      await oldFallback.promise;
    });

    expect(
      screen.queryByText(
        'The locations collection changed while paging. Reloaded the first page.'
      )
    ).toBeNull();
    expect(screen.getByText(`sha256:${'b'.repeat(64)}`)).toBeVisible();
  });
});
