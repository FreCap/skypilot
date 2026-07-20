import { fireEvent, render, screen } from '@testing-library/react';
import { Status2Actions } from '@/components/jobs';
import { Status2Actions as ExtractedStatus2Actions } from '@/components/job-log-actions';
import { downloadJobLogs } from '@/data/connectors/clusters';
import { downloadManagedJobLogs } from '@/data/connectors/jobs';
import { trackJobAction } from '@/lib/analytics';

const mockRouterPush = jest.fn();

jest.mock('next/router', () => ({
  useRouter: () => ({ push: mockRouterPush }),
}));

jest.mock('@/data/connectors/clusters', () => ({
  downloadJobLogs: jest.fn(),
}));

jest.mock('@/data/connectors/jobs', () => ({
  downloadManagedJobLogs: jest.fn(),
}));

jest.mock('@/lib/analytics', () => ({
  trackJobAction: jest.fn(),
}));

beforeEach(() => {
  jest.clearAllMocks();
});

it('preserves the component identity through the jobs facade', () => {
  expect(Status2Actions).toBe(ExtractedStatus2Actions);
});

it('routes log views without bubbling the table-row click', () => {
  const onRowClick = jest.fn();
  render(
    <div onClick={onRowClick}>
      <Status2Actions withLabel jobParent="/jobs" jobId="41" managed />
    </div>
  );

  fireEvent.click(screen.getByRole('button', { name: 'Logs' }));

  expect(trackJobAction).toHaveBeenCalledWith('view_logs', { jobId: '41' });
  expect(mockRouterPush).toHaveBeenCalledWith({
    pathname: '/jobs/41',
    query: { tab: 'logs' },
  });
  expect(onRowClick).not.toHaveBeenCalled();
});

it('downloads managed-job logs with the numeric job ID', () => {
  render(<Status2Actions withLabel jobParent="/jobs" jobId="42" managed />);

  fireEvent.click(screen.getByRole('button', { name: 'Download' }));

  expect(trackJobAction).toHaveBeenCalledWith('download_logs', {
    jobId: '42',
  });
  expect(downloadManagedJobLogs).toHaveBeenCalledWith({
    jobId: 42,
    controller: false,
  });
  expect(downloadJobLogs).not.toHaveBeenCalled();
});

it('downloads cluster-job logs with the cluster and workspace scope', () => {
  render(
    <Status2Actions
      withLabel
      jobParent="/clusters/training-cluster"
      jobId="7"
      managed={false}
      workspace="research"
    />
  );

  fireEvent.click(screen.getByRole('button', { name: 'Download' }));

  expect(trackJobAction).toHaveBeenCalledWith('download_logs', { jobId: '7' });
  expect(downloadJobLogs).toHaveBeenCalledWith({
    clusterName: 'training-cluster',
    jobIds: ['7'],
    workspace: 'research',
  });
  expect(downloadManagedJobLogs).not.toHaveBeenCalled();
});
