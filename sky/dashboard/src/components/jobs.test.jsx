import {
  filterJobsByName,
  filterJobsByPool,
  filterJobsByUser,
  filterJobsByWorkspace,
  getAggregatedStatus,
  statusGroups,
} from '@/components/jobs';
import * as jobDomain from '@/components/job-domain';
import * as jobsFacade from '@/components/jobs';

describe('job domain helpers', () => {
  it('preserves direct identities through the historical jobs facade', () => {
    expect(jobsFacade.statusGroups).toBe(jobDomain.statusGroups);
    expect(jobsFacade.getAggregatedStatus).toBe(jobDomain.getAggregatedStatus);
    expect(jobsFacade.filterJobsByName).toBe(jobDomain.filterJobsByName);
    expect(jobsFacade.filterJobsByWorkspace).toBe(
      jobDomain.filterJobsByWorkspace
    );
    expect(jobsFacade.filterJobsByUser).toBe(jobDomain.filterJobsByUser);
    expect(jobsFacade.filterJobsByPool).toBe(jobDomain.filterJobsByPool);
  });

  it('classifies active and finished statuses', () => {
    expect(statusGroups).toEqual({
      active: [
        'PENDING',
        'RUNNING',
        'RECOVERING',
        'SUBMITTED',
        'STARTING',
        'CANCELLING',
      ],
      finished: [
        'SUCCEEDED',
        'FAILED',
        'CANCELLED',
        'FAILED_SETUP',
        'FAILED_PRECHECKS',
        'FAILED_NO_RESOURCE',
        'FAILED_CONTROLLER',
      ],
    });
  });

  it('aggregates status while ignoring auxiliary job-group tasks', () => {
    expect(getAggregatedStatus()).toBe('PENDING');
    expect(getAggregatedStatus([])).toBe('PENDING');
    expect(getAggregatedStatus([{ status: 'RUNNING' }])).toBe('RUNNING');
    expect(
      getAggregatedStatus([
        { status: 'RUNNING', is_primary_in_job_group: null },
        { status: 'FAILED', is_primary_in_job_group: undefined },
      ])
    ).toBe('FAILED');
    expect(
      getAggregatedStatus([
        { status: 'RUNNING', is_primary_in_job_group: true },
        { status: 'FAILED', is_primary_in_job_group: false },
      ])
    ).toBe('RUNNING');
    expect(
      getAggregatedStatus([
        { status: 'RUNNING', is_primary_in_job_group: false },
        { status: 'FAILED', is_primary_in_job_group: false },
      ])
    ).toBe('FAILED');
    expect(
      getAggregatedStatus([{ status: 'UNKNOWN' }, { status: 'SUCCEEDED' }])
    ).toBe('SUCCEEDED');
  });

  it('filters names case-insensitively and preserves passthrough identity', () => {
    const jobs = [{ name: 'Alpha Train' }, { name: 'beta' }, {}];

    expect(filterJobsByName(jobs, '')).toBe(jobs);
    expect(filterJobsByName(jobs, '  ALPHA ')).toEqual([jobs[0]]);
  });

  it('filters workspaces with the historical default workspace behavior', () => {
    const jobs = [{ workspace: 'Research' }, { workspace: 'default' }, {}];

    expect(filterJobsByWorkspace(jobs, 'ALL_WORKSPACES')).toBe(jobs);
    expect(filterJobsByWorkspace(jobs, 'research')).toEqual([jobs[0]]);
    expect(filterJobsByWorkspace(jobs, 'DEFAULT')).toEqual([jobs[1], jobs[2]]);
  });

  it('prefers user hashes when filtering users', () => {
    const jobs = [{ user: 'alice', user_hash: 'hash-alice' }, { user: 'bob' }];

    expect(filterJobsByUser(jobs, 'ALL_USERS')).toBe(jobs);
    expect(filterJobsByUser(jobs, 'hash-alice')).toEqual([jobs[0]]);
    expect(filterJobsByUser(jobs, 'alice')).toEqual([]);
    expect(filterJobsByUser(jobs, 'bob')).toEqual([jobs[1]]);
  });

  it('filters pools case-insensitively and preserves passthrough identity', () => {
    const jobs = [{ pool: 'GPU-Train' }, { pool: 'cpu' }, {}];

    expect(filterJobsByPool(jobs, '  ')).toBe(jobs);
    expect(filterJobsByPool(jobs, ' gpu ')).toEqual([jobs[0]]);
  });
});
