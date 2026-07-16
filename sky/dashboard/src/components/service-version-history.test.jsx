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
jest.mock('@/components/ui/yaml-code-block', () => ({
  YamlCodeBlock: ({ value }) => <pre>{value}</pre>,
}));

const history = {
  service_name: 'svc',
  elected_version: 3,
  active_versions: [2, 3],
  versions: [
    {
      version: 3,
      yaml_content: 'name: current',
      elected: true,
      active: true,
    },
    {
      version: 1,
      yaml_content: 'name: old',
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
