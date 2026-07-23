import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import { Images } from '@/components/images';
import {
  getImageCapabilities,
  getImageCatalog,
  getImagePublications,
} from '@/data/connectors/images';
import { getWorkspaces } from '@/data/connectors/workspaces';

const mockRouter = {
  query: {},
  replace: jest.fn(),
};

jest.mock('next/router', () => ({
  useRouter: () => mockRouter,
}));

jest.mock(
  'next/link',
  () =>
    function MockLink({ children }) {
      return children;
    }
);

jest.mock('@/data/connectors/images', () => ({
  getImageCapabilities: jest.fn(),
  getImageCatalog: jest.fn(),
  getImagePublications: jest.fn(),
  getImageReadiness: jest.fn(),
}));

jest.mock('@/data/connectors/workspaces', () => ({
  getWorkspaces: jest.fn(),
}));

jest.mock('@/components/image-action-dialogs', () => ({
  PublishImageDialog: () => null,
  RetryImageDialog: () => null,
}));

const capabilities = (overrides = {}) => ({
  workspace: 'research',
  workspace_mode: 'managed_required',
  default_distribution: 'gpu-production',
  publish: false,
  admin: false,
  source_bindings: [],
  distributions: [
    {
      name: 'gpu-production',
      active: true,
      targets: [{ name: 'aws-us-west-2', region: 'us-west-2' }],
    },
  ],
  ...overrides,
});

const emptyPage = { items: [], next_cursor: null };

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe('Images dashboard', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockRouter.query = {};
    getWorkspaces.mockResolvedValue({ workspaces: ['research'] });
    getImageCatalog.mockResolvedValue(emptyPage);
    getImagePublications.mockResolvedValue(emptyPage);
  });

  it('shows an explicit API 62 callout for an older server', async () => {
    getImageCapabilities.mockRejectedValue({ status: 404, code: 'NOT_FOUND' });

    render(<Images />);

    expect(
      await screen.findByText('Managed Images requires API version 62')
    ).toBeVisible();
    expect(getImageCatalog).not.toHaveBeenCalled();
  });

  it('keeps non-publishers read-only and hides administrator readiness', async () => {
    getImageCapabilities.mockResolvedValue(capabilities());

    render(<Images />);

    expect(await screen.findByText('Read-only')).toBeVisible();
    expect(screen.queryByRole('button', { name: 'Publish' })).toBeNull();
    expect(screen.queryByRole('button', { name: 'Readiness' })).toBeNull();
    await waitFor(() => expect(getImageCatalog).toHaveBeenCalledTimes(1));
    expect(getImagePublications).not.toHaveBeenCalled();
  });

  it('marks every bounded catalog summary as partial', async () => {
    getImageCapabilities.mockResolvedValue(capabilities());
    getImageCatalog.mockResolvedValue({
      items: [
        {
          id: 'image-1',
          releases: ['boltz-l4'],
          distributions: ['gpu-production'],
          source_refs: ['ghcr.io/boltz-bio/runtime@sha256:abc'],
          targets: ['aws-us-west-2'],
          location_states: { READY: 10 },
          publications_truncated: true,
          sources_truncated: true,
          locations_truncated: true,
          runtime_digest: 'sha256:runtime',
          platform: 'linux/amd64',
          declared_size_bytes: 1024,
          updated_at: 100,
        },
      ],
      next_cursor: null,
    });

    render(<Images />);

    expect(await screen.findByText('boltz-l4, more…')).toBeVisible();
    expect(screen.getByText('gpu-production, more…')).toBeVisible();
    expect(
      screen.getByText('ghcr.io/boltz-bio/runtime@sha256:abc (more…)')
    ).toBeVisible();
    expect(
      screen.getByTitle(
        'More locations exist; open the artifact for paginated details.'
      )
    ).toBeVisible();
  });

  it('resets a stale keyset cursor and reloads the first page', async () => {
    getImageCapabilities.mockResolvedValue(capabilities());
    getImageCatalog
      .mockResolvedValueOnce({ items: [], next_cursor: 'next-1' })
      .mockRejectedValueOnce({ code: 'STALE_IMAGE_CURSOR' })
      .mockResolvedValueOnce(emptyPage);

    render(<Images />);
    const next = await screen.findByRole('button', { name: 'Next' });
    await waitFor(() => expect(next).toBeEnabled());
    fireEvent.click(next);

    expect(
      await screen.findByText(
        'The catalog changed while paging. Reloaded the first page.'
      )
    ).toBeVisible();
    await waitFor(() => expect(getImageCatalog).toHaveBeenCalledTimes(3));
    expect(getImageCatalog.mock.calls.map(([query]) => query.cursor)).toEqual([
      null,
      'next-1',
      null,
    ]);
  });

  it('returns directly to the first catalog page', async () => {
    getImageCapabilities.mockResolvedValue(capabilities());
    getImageCatalog
      .mockResolvedValueOnce({ items: [], next_cursor: 'next-1' })
      .mockResolvedValueOnce({ items: [], next_cursor: 'next-2' })
      .mockResolvedValueOnce(emptyPage);

    render(<Images />);
    const next = await screen.findByRole('button', { name: 'Next' });
    await waitFor(() => expect(next).toBeEnabled());
    fireEvent.click(next);
    expect(await screen.findByText('Page 2')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'First catalog page' }));

    expect(await screen.findByText('Page 1')).toBeVisible();
    await waitFor(() => expect(getImageCatalog).toHaveBeenCalledTimes(3));
    expect(getImageCatalog.mock.calls.map(([query]) => query.cursor)).toEqual([
      null,
      'next-1',
      null,
    ]);
  });

  it('pages the failed publication recovery feed independently', async () => {
    getImageCapabilities.mockResolvedValue(capabilities({ publish: true }));
    getImagePublications
      .mockResolvedValueOnce({
        items: [
          {
            id: 'publication-1',
            requested_release: 'failed-release-1',
            source_ref: 'registry/source-1',
            error_code: 'SOURCE_FAILED',
          },
        ],
        next_cursor: 'failed-next',
      })
      .mockResolvedValueOnce({
        items: [
          {
            id: 'publication-2',
            requested_release: 'failed-release-2',
            source_ref: 'registry/source-2',
            error_code: 'SOURCE_FAILED',
          },
        ],
        next_cursor: null,
      })
      .mockResolvedValueOnce({
        items: [
          {
            id: 'publication-1',
            requested_release: 'failed-release-1',
            source_ref: 'registry/source-1',
            error_code: 'SOURCE_FAILED',
          },
        ],
        next_cursor: 'failed-next',
      });

    render(<Images />);

    expect(await screen.findByText('failed-release-1')).toBeVisible();
    fireEvent.click(
      screen.getByRole('button', {
        name: 'Next failed publications page',
      })
    );
    expect(await screen.findByText('failed-release-2')).toBeVisible();
    expect(screen.queryByText('failed-release-1')).toBeNull();
    expect(
      getImagePublications.mock.calls.map(([query]) => query.cursor)
    ).toEqual([null, 'failed-next']);
    expect(getImageCatalog).toHaveBeenCalledTimes(1);

    fireEvent.click(
      screen.getByRole('button', { name: 'First failed publications page' })
    );
    expect(await screen.findByText('failed-release-1')).toBeVisible();
    expect(
      getImagePublications.mock.calls.map(([query]) => query.cursor)
    ).toEqual([null, 'failed-next', null]);
  });

  it('recovers a stale failed-publication cursor without resetting catalog paging', async () => {
    getImageCapabilities.mockResolvedValue(capabilities({ publish: true }));
    getImagePublications
      .mockResolvedValueOnce({ items: [], next_cursor: 'failed-next' })
      .mockRejectedValueOnce({ code: 'STALE_IMAGE_CURSOR' })
      .mockResolvedValueOnce(emptyPage);

    render(<Images />);
    fireEvent.click(
      await screen.findByRole('button', {
        name: 'Next failed publications page',
      })
    );

    expect(
      await screen.findByText(
        'The failed publication feed changed while paging. Reloaded the first page.'
      )
    ).toBeVisible();
    await waitFor(() => expect(getImagePublications).toHaveBeenCalledTimes(3));
    expect(
      getImagePublications.mock.calls.map(([query]) => query.cursor)
    ).toEqual([null, 'failed-next', null]);
    expect(getImageCatalog).toHaveBeenCalledTimes(1);
  });

  it('suppresses a late capability response after workspace navigation', async () => {
    const oldRequest = deferred();
    const newRequest = deferred();
    mockRouter.query = { workspace: 'old-workspace' };
    getImageCapabilities
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(newRequest.promise);

    const view = render(<Images />);
    mockRouter.query = { workspace: 'new-workspace' };
    view.rerender(<Images />);
    newRequest.resolve(
      capabilities({ workspace: 'new-workspace', workspace_mode: 'direct' })
    );
    expect(await screen.findByText('new-workspace')).toBeVisible();

    oldRequest.resolve(
      capabilities({ workspace: 'old-workspace', workspace_mode: 'managed' })
    );
    await Promise.resolve();
    expect(screen.queryByText('old-workspace')).toBeNull();
    expect(screen.getByText('new-workspace')).toBeVisible();
  });
});
