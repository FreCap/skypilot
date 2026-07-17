import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';

import { ServiceVersionHistory } from './service-version-history';
import { getCurrentUserRole } from '@/data/connectors/client';
import {
  electServiceVersion,
  getServiceVersions,
} from '@/data/connectors/services';

jest.mock('@/data/connectors/client', () => ({
  getCurrentUserRole: jest.fn(),
}));
jest.mock('@/data/connectors/services', () => ({
  electServiceVersion: jest.fn(),
  getServiceVersions: jest.fn(),
}));
jest.mock('@/components/ui/yaml-code-block', () => {
  const ReactModule = require('react');
  return {
    YamlCodeBlock: ({ value, onCreateEditor }) => {
      const scrollRef = ReactModule.useRef(null);
      const editorRef = ReactModule.useRef(null);
      ReactModule.useEffect(() => {
        editorRef.current = { scrollDOM: scrollRef.current };
        onCreateEditor?.(editorRef.current);
      }, [onCreateEditor]);
      return (
        <pre data-testid="yaml-pane" ref={scrollRef}>
          {value}
        </pre>
      );
    },
  };
});

const history = {
  service_name: 'svc',
  elected_version: 3,
  active_versions: [2, 3],
  versions: [
    {
      version: 3,
      yaml_content: 'name: current',
      created_at: 1784240584,
      created_by: 'test',
      policy: 'Autoscaling from 0 to 1000 replicas',
      elected: true,
      active: true,
    },
    {
      version: 1,
      yaml_content: 'name: old',
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
  electServiceVersion.mockResolvedValue([]);
});

it('shows elected state and compares a stored version', async () => {
  render(<ServiceVersionHistory serviceName="svc" />);

  expect(await screen.findByText(/Elected generation: 3/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole('button', { name: 'Compare' }));

  expect(
    screen.getByText('Version 1 compared with elected version 3')
  ).toBeInTheDocument();
  expect(screen.getByText('name: old')).toBeInTheDocument();
  expect(screen.getByText('name: current')).toBeInTheDocument();
  expect(screen.getByText('test')).toBeInTheDocument();
  expect(
    screen.getByText('Autoscaling from 0 to 1000 replicas')
  ).toBeInTheDocument();
  expect(screen.getAllByText('Unknown')).toHaveLength(2);
});

it('keeps both comparison panes on the same scroll position', async () => {
  render(<ServiceVersionHistory serviceName="svc" />);

  await screen.findByText(/Elected generation: 3/);
  fireEvent.click(screen.getByRole('button', { name: 'Compare' }));
  const panes = await screen.findAllByTestId('yaml-pane');
  await waitFor(() =>
    expect(screen.getByText('Scrolling is synced')).toBeInTheDocument()
  );

  panes[0].scrollTop = 96;
  panes[0].scrollLeft = 24;
  fireEvent.scroll(panes[0]);

  expect(panes[1].scrollTop).toBe(96);
  expect(panes[1].scrollLeft).toBe(24);
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

  await screen.findByText(/Elected generation: 3/);
  fireEvent.click(screen.getByRole('button', { name: 'Elect' }));

  await waitFor(() =>
    expect(electServiceVersion).toHaveBeenCalledWith('svc', 1)
  );
  await waitFor(() => expect(getServiceVersions).toHaveBeenCalledTimes(2));
  expect(onElectionComplete).toHaveBeenCalledTimes(1);
  window.confirm.mockRestore();
});

it('is hidden from non-admin users', async () => {
  getCurrentUserRole.mockResolvedValue({ role: 'user' });
  const { container } = render(<ServiceVersionHistory serviceName="svc" />);

  await waitFor(() => expect(container).toBeEmptyDOMElement());
  expect(getServiceVersions).not.toHaveBeenCalled();
});
