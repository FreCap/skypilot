import React from 'react';
import { fireEvent, render, screen, within } from '@testing-library/react';

import { PoolsTable } from '@/components/pools-table';

function pool(name, overrides = {}) {
  return {
    name,
    jobCounts: { RUNNING: 1 },
    replica_info: [{ status: 'READY', cloud: 'AWS', region: 'us-east-1' }],
    target_num_replicas: 2,
    requested_resources_str: '1x A100',
    ...overrides,
  };
}

describe('PoolsTable rendering', () => {
  it('shows the initial loading state only before the first snapshot arrives', () => {
    render(<PoolsTable data={[]} loading={true} />);

    expect(screen.getByText('Loading...')).toBeInTheDocument();
    expect(screen.queryByText('No pools found')).not.toBeInTheDocument();
  });

  it('keeps the last snapshot visible while a refresh is in flight', () => {
    render(<PoolsTable data={[pool('pool-a')]} loading={true} />);

    expect(screen.getByRole('link', { name: 'pool-a' })).toBeInTheDocument();
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
  });

  it('renders the pool row contract from the shared snapshot', () => {
    render(<PoolsTable data={[pool('training-pool')]} loading={false} />);

    const poolLink = screen.getByRole('link', { name: 'training-pool' });
    const poolRow = poolLink.closest('tr');
    expect(poolLink).toHaveAttribute('href', '/jobs/pools/training-pool');
    expect(within(poolRow).getByText('1 (target: 2)')).toBeInTheDocument();
    expect(within(poolRow).getByText('1x A100')).toBeInTheDocument();
    expect(within(poolRow).getByText('RUNNING')).toBeInTheDocument();
    expect(within(poolRow).getByText('AWS (1 region)')).toBeInTheDocument();
  });

  it('sorts the shared snapshot locally without refetching', () => {
    render(
      <PoolsTable
        data={[pool('pool-b'), pool('pool-a'), pool('pool-c')]}
        loading={false}
      />
    );

    const nameHeader = screen.getByText('Pool');
    fireEvent.click(nameHeader);
    let links = screen
      .getAllByRole('link')
      .filter((link) => link.getAttribute('href')?.startsWith('/jobs/pools/'));
    expect(links.map((link) => link.textContent)).toEqual([
      'pool-a',
      'pool-b',
      'pool-c',
    ]);

    fireEvent.click(nameHeader);
    links = screen
      .getAllByRole('link')
      .filter((link) => link.getAttribute('href')?.startsWith('/jobs/pools/'));
    expect(links.map((link) => link.textContent)).toEqual([
      'pool-c',
      'pool-b',
      'pool-a',
    ]);
  });

  it('shows the empty state once the shared snapshot is empty', () => {
    render(<PoolsTable data={[]} loading={false} />);

    expect(screen.getByText('No pools found')).toBeInTheDocument();
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
  });
});
