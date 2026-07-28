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

function nonterminalDetailFor(
  workspace = 'research',
  imageId = 'image-1',
  digestCharacter = 'a'
) {
  const detail = detailFor(workspace, imageId, digestCharacter);
  detail.locations[0].state = 'COPYING';
  detail.locations[0].error_code = null;
  return detail;
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

  afterEach(() => {
    jest.useRealTimers();
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

  it('keeps one poll owner while a slow detail refresh is pending', async () => {
    jest.useFakeTimers();
    const slowCapabilities = deferred();
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'research', publish: false })
      .mockReturnValueOnce(slowCapabilities.promise)
      .mockResolvedValue({ workspace: 'research', publish: false });
    getImageArtifactDetail.mockResolvedValue(nonterminalDetailFor());

    const view = render(<ImageDetail />);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText('Image artifact')).toBeVisible();

    await act(async () => {
      jest.advanceTimersByTime(5000);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
    const slowSignal = getImageCapabilities.mock.calls[1][1];

    await act(async () => {
      jest.advanceTimersByTime(5000);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
    expect(slowSignal.aborted).toBe(false);

    await act(async () => {
      slowCapabilities.resolve({ workspace: 'research', publish: false });
    });
    await act(async () => {
      jest.advanceTimersByTime(0);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(3);

    view.unmount();
  });

  it('preserves the five-second start cadence after a fast poll', async () => {
    jest.useFakeTimers();
    getImageCapabilities.mockResolvedValue({
      workspace: 'research',
      publish: false,
    });
    getImageArtifactDetail.mockResolvedValue(nonterminalDetailFor());

    const view = render(<ImageDetail />);
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(screen.getByText('Image artifact')).toBeVisible();

    await act(async () => {
      jest.advanceTimersByTime(4999);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(1);
    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);

    await act(async () => {
      jest.advanceTimersByTime(4999);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(3);

    view.unmount();
  });

  it('waits one interval before polling after a slow initial load', async () => {
    jest.useFakeTimers();
    const initialCapabilities = deferred();
    getImageCapabilities
      .mockReturnValueOnce(initialCapabilities.promise)
      .mockResolvedValue({ workspace: 'research', publish: false });
    getImageArtifactDetail.mockResolvedValue(nonterminalDetailFor());

    const view = render(<ImageDetail />);
    await act(async () => {
      jest.advanceTimersByTime(6000);
      initialCapabilities.resolve({
        workspace: 'research',
        publish: false,
      });
    });
    expect(await screen.findByText('Image artifact')).toBeVisible();
    expect(getImageCapabilities).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(4999);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(1);
    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);

    view.unmount();
  });

  it('does not let the poll timer abort a recent manual refresh', async () => {
    jest.useFakeTimers();
    const manualCapabilities = deferred();
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'research', publish: false })
      .mockReturnValueOnce(manualCapabilities.promise)
      .mockResolvedValue({ workspace: 'research', publish: false });
    getImageArtifactDetail.mockResolvedValue(nonterminalDetailFor());

    const view = render(<ImageDetail />);
    expect(await screen.findByText('Image artifact')).toBeVisible();

    await act(async () => {
      jest.advanceTimersByTime(4999);
    });
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
    const manualSignal = getImageCapabilities.mock.calls[1][1];

    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
    expect(manualSignal.aborted).toBe(false);

    view.unmount();
  });

  it('retries a failed poll and stops after a terminal refresh', async () => {
    jest.useFakeTimers();
    const failure = new Error('temporary detail failure');
    failure.code = 'DETAIL_RETRY';
    const terminal = detailFor('research', 'image-1', 'a');
    terminal.locations[0].state = 'READY';
    terminal.locations[0].error_code = null;
    getImageCapabilities.mockResolvedValue({
      workspace: 'research',
      publish: false,
    });
    getImageArtifactDetail
      .mockResolvedValueOnce(nonterminalDetailFor())
      .mockRejectedValueOnce(failure)
      .mockResolvedValueOnce(terminal);

    const view = render(<ImageDetail />);
    expect(await screen.findByText('Image artifact')).toBeVisible();

    await act(async () => {
      jest.advanceTimersByTime(5000);
    });
    expect(await screen.findByRole('alert')).toHaveTextContent('DETAIL_RETRY');
    expect(getImageArtifactDetail).toHaveBeenCalledTimes(2);

    await act(async () => {
      jest.advanceTimersByTime(5000);
    });
    expect(getImageArtifactDetail).toHaveBeenCalledTimes(3);
    await act(async () => {
      jest.advanceTimersByTime(10_000);
    });
    expect(getImageArtifactDetail).toHaveBeenCalledTimes(3);

    view.unmount();
  });

  it('aborts poll-owned work when collection paging disables polling', async () => {
    jest.useFakeTimers();
    const pollCapabilities = deferred();
    const initialDetail = nonterminalDetailFor();
    initialDetail.next_cursors.locations = 'locations-next';
    initialDetail.truncated = true;
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'research', publish: false })
      .mockReturnValueOnce(pollCapabilities.promise);
    getImageArtifactDetail.mockResolvedValue(initialDetail);
    getImageArtifactCollection.mockResolvedValue({
      items: initialDetail.locations,
      next_cursor: null,
    });

    const view = render(<ImageDetail />);
    expect(
      await screen.findByRole('button', { name: 'Next locations page' })
    ).toBeEnabled();

    await act(async () => {
      jest.advanceTimersByTime(5000);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
    const pollSignal = getImageCapabilities.mock.calls[1][1];

    fireEvent.click(
      screen.getByRole('button', { name: 'Next locations page' })
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(getImageArtifactCollection).toHaveBeenCalledTimes(1);
    expect(
      screen.getByRole('button', { name: 'Previous locations page' })
    ).toBeEnabled();
    expect(pollSignal.aborted).toBe(true);
    await act(async () => {
      pollCapabilities.resolve({ workspace: 'research', publish: false });
      await pollCapabilities.promise;
    });
    expect(getImageArtifactDetail).toHaveBeenCalledTimes(1);
    expect(
      screen.getByRole('button', { name: 'Previous locations page' })
    ).toBeEnabled();

    view.unmount();
  });

  it('does not let a due poll abort collection paging in progress', async () => {
    jest.useFakeTimers();
    const pageRequest = deferred();
    const initialDetail = nonterminalDetailFor();
    initialDetail.next_cursors.locations = 'locations-next';
    initialDetail.truncated = true;
    getImageCapabilities.mockResolvedValue({
      workspace: 'research',
      publish: false,
    });
    getImageArtifactDetail.mockResolvedValue(initialDetail);
    getImageArtifactCollection.mockReturnValueOnce(pageRequest.promise);

    const view = render(<ImageDetail />);
    expect(
      await screen.findByRole('button', { name: 'Next locations page' })
    ).toBeEnabled();
    await act(async () => {
      jest.advanceTimersByTime(4999);
    });
    fireEvent.click(
      screen.getByRole('button', { name: 'Next locations page' })
    );
    const pageSignal = getImageArtifactCollection.mock.calls[0][3];

    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(pageSignal.aborted).toBe(false);
    expect(getImageCapabilities).toHaveBeenCalledTimes(1);
    await act(async () => {
      jest.advanceTimersByTime(10_000);
    });
    expect(pageSignal.aborted).toBe(false);
    expect(getImageCapabilities).toHaveBeenCalledTimes(1);

    pageRequest.resolve({
      items: initialDetail.locations,
      next_cursor: null,
    });
    await act(async () => {
      await pageRequest.promise;
    });
    expect(
      screen.getByRole('button', { name: 'Previous locations page' })
    ).toBeEnabled();

    view.unmount();
  });

  it('resumes polling after deferred collection paging fails', async () => {
    jest.useFakeTimers();
    const pageRequest = deferred();
    const pageFailure = new Error('temporary collection failure');
    pageFailure.code = 'COLLECTION_RETRY';
    const initialDetail = nonterminalDetailFor();
    initialDetail.next_cursors.locations = 'locations-next';
    initialDetail.truncated = true;
    getImageCapabilities.mockResolvedValue({
      workspace: 'research',
      publish: false,
    });
    getImageArtifactDetail.mockResolvedValue(initialDetail);
    getImageArtifactCollection.mockReturnValueOnce(pageRequest.promise);

    const view = render(<ImageDetail />);
    expect(
      await screen.findByRole('button', { name: 'Next locations page' })
    ).toBeEnabled();
    await act(async () => {
      jest.advanceTimersByTime(4999);
    });
    fireEvent.click(
      screen.getByRole('button', { name: 'Next locations page' })
    );
    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(1);

    await act(async () => {
      pageRequest.reject(pageFailure);
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(await screen.findByRole('alert')).toHaveTextContent(
      'COLLECTION_RETRY'
    );
    await act(async () => {
      jest.advanceTimersByTime(4999);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(1);
    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);

    view.unmount();
  });

  it('aborts the poll owner and timer when the detail unmounts', async () => {
    jest.useFakeTimers();
    const pollCapabilities = deferred();
    getImageCapabilities
      .mockResolvedValueOnce({ workspace: 'research', publish: false })
      .mockReturnValueOnce(pollCapabilities.promise);
    getImageArtifactDetail.mockResolvedValue(nonterminalDetailFor());

    const view = render(<ImageDetail />);
    expect(await screen.findByText('Image artifact')).toBeVisible();
    await act(async () => {
      jest.advanceTimersByTime(5000);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
    const pollSignal = getImageCapabilities.mock.calls[1][1];

    view.unmount();
    expect(pollSignal.aborted).toBe(true);
    await act(async () => {
      jest.advanceTimersByTime(10_000);
    });
    expect(getImageCapabilities).toHaveBeenCalledTimes(2);
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
