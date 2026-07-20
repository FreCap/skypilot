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
