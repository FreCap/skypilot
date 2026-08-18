import React from 'react';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from '@testing-library/react';

import {
  buildSplitDiffRows,
  ServiceVersionHistory,
} from './service-version-history';
import { getCurrentUserRole } from '@/data/connectors/client';
import {
  electServiceVersion,
  getServiceVersion,
  getServiceVersions,
} from '@/data/connectors/services';

function deferred() {
  let resolve;
  const promise = new Promise((promiseResolve) => {
    resolve = promiseResolve;
  });
  return { promise, resolve };
}

jest.mock('@/data/connectors/client', () => ({
  getCurrentUserRole: jest.fn(),
}));
jest.mock('@/data/connectors/services', () => ({
  electServiceVersion: jest.fn(),
  getServiceVersion: jest.fn(),
  getServiceVersions: jest.fn(),
}));
jest.mock('@/components/ui/yaml-code-block', () => ({
  YamlCodeBlock: ({ value }) => (
    <pre data-testid="yaml-code-block">{value}</pre>
  ),
}));
const history = {
  service_name: 'svc',
  elected_version: 3,
  active_versions: [2, 3],
  versions: [
    {
      version: 3,
      submitted_yaml_content: 'service:\n  min_replicas: 3\n  max_replicas: 10',
      compiled_yaml_content:
        'resources:\n  accelerators: L4\nservice:\n  max_replicas: 10\n  min_replicas: 3',
      created_at: 1784240584,
      created_by: 'test',
      policy: 'Autoscaling from 0 to 1000 replicas',
      elected: true,
      active: true,
    },
    {
      version: 1,
      submitted_yaml_content: 'service:\n  min_replicas: 1\n  max_replicas: 10',
      compiled_yaml_content:
        'resources:\n  accelerators: A100\nservice:\n  max_replicas: 10\n  min_replicas: 1',
      created_at: null,
      created_by: null,
      policy: 'Fixed 1 replica',
      elected: false,
      active: false,
    },
  ],
};

beforeEach(() => {
  jest.clearAllMocks();
  getCurrentUserRole.mockResolvedValue({ role: 'admin' });
  getServiceVersions.mockResolvedValue(history);
  getServiceVersion.mockImplementation((_, version) =>
    Promise.resolve(
      history.versions.find((candidate) => candidate.version === version)
    )
  );
  electServiceVersion.mockResolvedValue([]);
});

it('loads YAML for only the selected metadata-only version', async () => {
  getServiceVersions.mockResolvedValue({
    ...history,
    versions: history.versions.map((version) => ({
      ...version,
      submitted_yaml_content: null,
      compiled_yaml_content: null,
      yaml_included: false,
    })),
  });
  render(<ServiceVersionHistory serviceName="svc" />);

  await screen.findByText(/Elected 3/);
  fireEvent.click(
    screen.getByRole('button', { name: 'View YAML for version 1' })
  );

  expect(await screen.findByText('Version 1 YAML')).toBeInTheDocument();
  expect(getServiceVersion).toHaveBeenCalledTimes(1);
  expect(getServiceVersion).toHaveBeenCalledWith('svc', 1);
  expect(screen.getByTestId('yaml-code-block')).toHaveTextContent(
    'min_replicas: 1'
  );
});

it('shows elected state and compares a stored version', async () => {
  render(<ServiceVersionHistory serviceName="svc" />);

  expect(await screen.findByText(/Elected 3/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: 'Compare' }));

  expect(screen.getByText('Changes from elected v3')).toBeInTheDocument();
  const changedRow = screen.getByTestId('diff-changed-row');
  expect(within(changedRow).getByText('3')).toBeInTheDocument();
  expect(within(changedRow).getByText('1')).toBeInTheDocument();
  expect(screen.getByText('test')).toBeInTheDocument();
  expect(
    screen.getByText('Autoscaling from 0 to 1000 replicas')
  ).toBeInTheDocument();
  expect(screen.getAllByText('Unknown')).toHaveLength(2);
});

it('shows the submitted and compiled YAML for each version', async () => {
  render(<ServiceVersionHistory serviceName="svc" />);

  await screen.findByText(/Elected 3/);
  fireEvent.click(
    screen.getByRole('button', { name: 'View YAML for version 1' })
  );

  expect(screen.getByText('Version 1 YAML')).toBeInTheDocument();
  expect(screen.getByTestId('yaml-code-block')).toHaveTextContent(
    'min_replicas: 1'
  );

  fireEvent.click(screen.getByRole('button', { name: 'compiled' }));
  expect(screen.getByTestId('yaml-code-block')).toHaveTextContent(
    'accelerators: A100'
  );

  fireEvent.click(
    screen.getByRole('button', { name: 'View YAML for version 3' })
  );
  expect(screen.getByText('Version 3 YAML')).toBeInTheDocument();
  expect(screen.getByTestId('yaml-code-block')).toHaveTextContent(
    'min_replicas: 3'
  );
});

it('does not confirm a stale YAML copy after switching kinds', async () => {
  const clipboardWrite = deferred();
  const originalClipboard = navigator.clipboard;
  const writeText = jest.fn(() => clipboardWrite.promise);
  Object.defineProperty(navigator, 'clipboard', {
    configurable: true,
    value: { writeText },
  });

  try {
    render(<ServiceVersionHistory serviceName="svc" />);
    await screen.findByText(/Elected 3/);
    fireEvent.click(
      screen.getByRole('button', { name: 'View YAML for version 1' })
    );

    fireEvent.click(screen.getByRole('button', { name: 'Copy YAML' }));
    expect(writeText).toHaveBeenCalledWith(
      expect.stringContaining('min_replicas: 1')
    );

    fireEvent.click(screen.getByRole('button', { name: 'compiled' }));
    await act(async () => {
      clipboardWrite.resolve();
      await clipboardWrite.promise;
    });

    expect(screen.getByRole('button', { name: 'Copy YAML' })).toBeTruthy();
  } finally {
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: originalClipboard,
    });
  }
});

it('keeps newer YAML copy feedback independent of an older timer', async () => {
  const originalClipboard = navigator.clipboard;
  const writeText = jest.fn().mockResolvedValue(undefined);
  Object.defineProperty(navigator, 'clipboard', {
    configurable: true,
    value: { writeText },
  });
  render(<ServiceVersionHistory serviceName="svc" />);
  await screen.findByText(/Elected 3/);
  fireEvent.click(
    screen.getByRole('button', { name: 'View YAML for version 1' })
  );
  jest.useFakeTimers();

  try {
    fireEvent.click(screen.getByRole('button', { name: 'Copy YAML' }));
    await act(async () => Promise.resolve());
    expect(screen.getByRole('button', { name: 'Copied!' })).toBeTruthy();

    act(() => jest.advanceTimersByTime(1000));
    fireEvent.click(screen.getByRole('button', { name: 'compiled' }));
    fireEvent.click(screen.getByRole('button', { name: 'Copy YAML' }));
    await act(async () => Promise.resolve());
    expect(writeText).toHaveBeenCalledTimes(2);

    act(() => jest.advanceTimersByTime(1000));
    expect(screen.getByRole('button', { name: 'Copied!' })).toBeTruthy();
    act(() => jest.advanceTimersByTime(1000));
    expect(screen.getByRole('button', { name: 'Copy YAML' })).toBeTruthy();
  } finally {
    jest.runOnlyPendingTimers();
    jest.useRealTimers();
    Object.defineProperty(navigator, 'clipboard', {
      configurable: true,
      value: originalClipboard,
    });
  }
});

it('explains when a version does not have retained YAML', async () => {
  getServiceVersions.mockResolvedValue({
    ...history,
    versions: history.versions.map((version) => ({
      ...version,
      submitted_yaml_content: null,
      compiled_yaml_content: null,
    })),
  });
  render(<ServiceVersionHistory serviceName="svc" />);

  await screen.findByText(/Elected 3/);
  fireEvent.click(
    screen.getByRole('button', { name: 'View YAML for version 1' })
  );

  expect(
    screen.getByText('Submitted YAML was not retained for this version.')
  ).toBeInTheDocument();
  expect(screen.getByRole('button', { name: 'Copy YAML' })).toBeDisabled();

  fireEvent.click(screen.getByRole('button', { name: 'compiled' }));
  expect(
    screen.getByText('Compiled YAML is unavailable for this version.')
  ).toBeInTheDocument();
});

it('reports when the selected and elected YAML are identical', async () => {
  getServiceVersions.mockResolvedValue({
    ...history,
    versions: [
      history.versions[0],
      {
        ...history.versions[1],
        submitted_yaml_content: history.versions[0].submitted_yaml_content,
      },
    ],
  });
  render(<ServiceVersionHistory serviceName="svc" />);

  await screen.findByText(/Elected 3/);
  fireEvent.click(screen.getByRole('button', { name: 'Compare' }));

  expect(screen.getByText('These versions have identical YAML.')).toBeTruthy();
});

it('compares submitted YAML by default and can compare compiled YAML', async () => {
  render(<ServiceVersionHistory serviceName="svc" />);

  await screen.findByText(/Elected 3/);
  fireEvent.click(screen.getByRole('button', { name: 'Compare' }));
  expect(screen.getByTestId('diff-changed-row')).toHaveTextContent('3');

  fireEvent.click(screen.getByRole('button', { name: 'compiled' }));
  expect(screen.getAllByTestId('diff-changed-row').length).toBeGreaterThan(0);
  expect(screen.getByText(/L4/)).toBeTruthy();
  expect(screen.getByText(/A100/)).toBeTruthy();
});

it('directs legacy versions without submitted YAML to compiled comparison', async () => {
  getServiceVersions.mockResolvedValue({
    ...history,
    versions: history.versions.map((version) => ({
      ...version,
      submitted_yaml_content: null,
    })),
  });
  render(<ServiceVersionHistory serviceName="svc" />);

  await screen.findByText(/Elected 3/);
  fireEvent.click(screen.getByRole('button', { name: 'Compare' }));
  expect(
    screen.getByText(/Submitted YAML was not retained/)
  ).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: 'compiled' }));
  expect(screen.queryByText(/Submitted YAML was not retained/)).toBeNull();
});

it('does not offer comparison without an elected baseline', async () => {
  getServiceVersions.mockResolvedValue({
    ...history,
    elected_version: null,
    versions: history.versions.map((version) => ({
      ...version,
      elected: false,
    })),
  });
  render(<ServiceVersionHistory serviceName="svc" />);

  expect(await screen.findByText(/Elected -/)).toBeTruthy();
  expect(screen.queryByRole('button', { name: 'Compare' })).toBeNull();
});

it('aligns changed lines and collapses distant unchanged context', () => {
  const base = Array.from({ length: 20 }, (_, index) => `line ${index + 1}`);
  const comparison = [...base];
  comparison[9] = 'line ten changed';

  const rows = buildSplitDiffRows(base.join('\n'), comparison.join('\n'));
  const changed = rows.find((row) => row.type === 'changed');

  expect(changed).toMatchObject({
    baseLine: 10,
    comparisonLine: 10,
    baseText: 'line 10',
    comparisonText: 'line ten changed',
  });
  expect(rows.filter((row) => row.type === 'gap')).toHaveLength(2);
  expect(rows[0]).toEqual({ type: 'gap', count: 6 });
  expect(rows.at(-1)).toEqual({ type: 'gap', count: 7 });
});

it.each([
  [
    'addition',
    'first\nthird',
    'first\nsecond\nthird',
    {
      type: 'added',
      baseLine: null,
      comparisonLine: 2,
      comparisonText: 'second',
    },
  ],
  [
    'removal',
    'first\nsecond\nthird',
    'first\nthird',
    {
      type: 'removed',
      baseLine: 2,
      comparisonLine: null,
      baseText: 'second',
    },
  ],
])(
  'aligns a pure %s without shifting neighboring lines',
  (_, base, next, row) => {
    expect(buildSplitDiffRows(base, next)).toEqual(
      expect.arrayContaining([expect.objectContaining(row)])
    );
  }
);

it('keeps line numbers aligned across multiple change hunks', () => {
  const base = 'one\ntwo\nthree\nfour\nfive\nsix\nseven';
  const comparison = 'one\nTWO\nthree\nfour\nfive\nSIX\nseven';

  const changed = buildSplitDiffRows(base, comparison).filter(
    (row) => row.type === 'changed'
  );

  expect(changed).toMatchObject([
    { baseLine: 2, comparisonLine: 2, baseText: 'two', comparisonText: 'TWO' },
    { baseLine: 6, comparisonLine: 6, baseText: 'six', comparisonText: 'SIX' },
  ]);
});

it('elects through the existing rolling update path and refreshes', async () => {
  const onElectionComplete = jest.fn();
  jest.spyOn(window, 'confirm').mockReturnValue(true);
  render(
    <ServiceVersionHistory
      serviceName="svc"
      onElectionComplete={onElectionComplete}
    />
  );

  await screen.findByText(/Elected 3/);
  fireEvent.click(screen.getByRole('button', { name: 'Compare' }));
  expect(screen.getByText('Changes from elected v3')).toBeTruthy();
  fireEvent.click(screen.getByRole('button', { name: 'Elect' }));

  await waitFor(() =>
    expect(electServiceVersion).toHaveBeenCalledWith('svc', 1)
  );
  await waitFor(() => expect(getServiceVersions).toHaveBeenCalledTimes(2));
  await waitFor(() =>
    expect(screen.queryByText('Changes from elected v3')).toBeNull()
  );
  expect(onElectionComplete).toHaveBeenCalledTimes(1);
  window.confirm.mockRestore();
});

it('explains that version history is admin-only', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'user' });
  render(<ServiceVersionHistory serviceName="svc" />);

  expect(
    await screen.findByText('Version history is available to administrators.')
  ).toBeTruthy();
  expect(getServiceVersions).not.toHaveBeenCalled();
});

it('does not stay loading before a service name is available', async () => {
  render(<ServiceVersionHistory serviceName={undefined} />);

  expect(await screen.findByText('Version history')).toBeTruthy();
  expect(screen.queryByText('Loading versions...')).toBeNull();
  expect(getServiceVersions).not.toHaveBeenCalled();
});

it('ignores a stale history response after the service changes', async () => {
  const first = deferred();
  const second = deferred();
  getServiceVersions
    .mockImplementationOnce(() => first.promise)
    .mockImplementationOnce(() => second.promise);
  const { rerender } = render(
    <ServiceVersionHistory serviceName="first-service" />
  );

  await waitFor(() => expect(getServiceVersions).toHaveBeenCalledTimes(1));
  rerender(<ServiceVersionHistory serviceName="second-service" />);
  await waitFor(() => expect(getServiceVersions).toHaveBeenCalledTimes(2));

  await act(async () => {
    second.resolve(history);
    await second.promise;
  });
  expect(await screen.findByText(/Elected 3/)).toBeTruthy();

  await act(async () => {
    first.resolve({ ...history, elected_version: 99 });
    await first.promise;
  });
  expect(screen.queryByText(/Elected 99/)).toBeNull();
  expect(screen.getByText(/Elected 3/)).toBeTruthy();
});
