import { Profiler } from 'react';
import { act, fireEvent, render, screen } from '@testing-library/react';

import { ImageReadiness } from '@/components/image-readiness';

jest.mock('@/components/image-action-dialogs', () => ({
  CanaryProfileDialog: () => null,
  QualifyProfileDialog: () => null,
}));

const capabilities = {
  distributions: [
    {
      name: 'gpu-production',
      targets: [],
    },
  ],
};

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
}

const readiness = {
  workspace: 'research',
  generated_at: 10_000,
  catalog_authority: '00000000-0000-4000-8000-000000000001',
  catalog_authority_base32: 'AAAAAAAAAAAAAAAAAAAAAAAAAA',
  profiles: [
    {
      id: 'profile-1',
      profile: 'gpu-production',
      revision: 2,
      desired_generation: 3,
      state: 'ACTIVE',
      qualification_targets: [
        {
          target: 'aws-us-west-2',
          target_fingerprint: 'target-fingerprint-west',
          region: 'us-west-2',
          repository_arn: 'arn:aws:ecr:us-west-2:123456789012:repository/q',
          repository_generation: 1,
          repository_attested: true,
          repository_quarantined: false,
          required_generation: null,
          quarantine_reason: null,
          quarantined_at: null,
        },
      ],
      attestations: {
        'runtime:aws-us-west-2:aws_vm': {
          status: 'READY',
          observed_at: 9_990,
        },
      },
      updated_at: 9_995,
    },
  ],
  queues: [
    {
      profile: 'gpu-production',
      target: 'aws-us-west-2',
      region: 'us-west-2',
      queue_depth: 501,
      queue_depth_at_least: false,
      failed_count: 3,
      failed_count_at_least: false,
      quarantined_count: 2,
      quarantined_count_at_least: false,
      quarantined_reserved_declared_bytes: 512,
      quarantined_reserved_declared_bytes_at_least: false,
      oldest_queued_at: 9_900,
      quota_bound_eta_seconds: 51,
      quota_bound_eta_at_least: false,
      quota_rate_per_second: 10,
      quota_blocked_until: null,
      max_manifests: 100_000,
      reserved_manifests: 1_000,
      max_declared_bytes: 10_000,
      reserved_declared_bytes: 1_000,
      in_flight: 4,
      max_in_flight: 16,
    },
  ],
  workers: [
    {
      id: 'worker-1',
      kind: 'copy',
      version: 'sha256:worker',
      heartbeat_at: 9_995,
      in_flight: 4,
      max_in_flight: 16,
    },
  ],
  provider_budgets: [
    {
      provider: 'aws',
      account: '123456789012',
      region: 'us-west-2',
      api_family: 'ecr_write',
      applied_rate_per_second: 10,
      burst: 20,
      blocked_until: null,
      throttle_count: 2,
    },
  ],
  profiles_truncated: false,
  shards_truncated: false,
  workers_truncated: false,
  provider_budgets_truncated: false,
  queues_truncated: false,
  qualification_mutation: null,
  qualification_repository_quarantines: [],
  qualification_repository_quarantines_truncated: false,
};

describe('Image readiness', () => {
  it('separates registry ETA from node pull and replica health', () => {
    const onRefresh = jest.fn();
    render(
      <ImageReadiness
        readiness={readiness}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={onRefresh}
      />
    );

    expect(
      screen.getByText(
        /ETA is a provider-quota lower bound. It does not predict node cache/
      )
    ).toBeVisible();
    expect(screen.getByText('51s')).toBeVisible();
    expect(screen.getAllByText('501')).toHaveLength(2);
    expect(screen.getByText('3 failed')).toBeVisible();
    expect(screen.getByText('2 quarantined · 512 B retained')).toBeVisible();
    expect(screen.getByText('copy')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(onRefresh).toHaveBeenCalledTimes(1);
  });

  it('renders only projected evidence, never arbitrary credential fields', () => {
    const withSecret = {
      ...readiness,
      unexpected_secret: 'do-not-render',
      profiles: [
        {
          ...readiness.profiles[0],
          attestations: {
            ...readiness.profiles[0].attestations,
            secret_access_key: {
              status: 'READY',
              observed_at: 9_990,
              value: 'do-not-render',
            },
          },
        },
      ],
    };
    render(
      <ImageReadiness
        readiness={withSecret}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(screen.queryByText('do-not-render')).toBeNull();
    expect(screen.getByText('secret_access_key: READY')).toBeVisible();
  });

  it('keeps cached data visible but disables mutations after refresh failure', () => {
    render(
      <ImageReadiness
        readiness={readiness}
        capabilities={capabilities}
        loading={false}
        error="READINESS_UNAVAILABLE"
        onRefresh={jest.fn()}
      />
    );

    expect(screen.getByRole('alert')).toHaveTextContent(
      /cached snapshot is read-only/
    );
    expect(
      screen.getByRole('button', { name: 'Ingest handoff' })
    ).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Run canary' })).toBeDisabled();
    expect(screen.getByRole('button', { name: 'Refresh' })).toBeEnabled();
  });

  it('renders bounded queue counts and ETAs as lower bounds', () => {
    render(
      <ImageReadiness
        readiness={{
          ...readiness,
          queues: [
            {
              ...readiness.queues[0],
              queue_depth: 10_000,
              queue_depth_at_least: true,
              failed_count: 10_000,
              failed_count_at_least: true,
              quota_bound_eta_seconds: 1000,
              quota_bound_eta_at_least: true,
            },
          ],
        }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(screen.getAllByText('10,000+')).toHaveLength(2);
    expect(screen.getByText('10,000+ terminal failures')).toBeVisible();
    expect(screen.getByText('10,000+ failed')).toBeVisible();
    expect(screen.getByText('≥17m')).toBeVisible();
  });

  it('shows the exact generation remediation for repository quarantine', () => {
    render(
      <ImageReadiness
        readiness={{
          ...readiness,
          generated_at: Math.floor(Date.now() / 1000),
          qualification_repository_quarantines: [
            {
              repository_arn: 'arn:aws:ecr:us-west-2:123456789012:repository/q',
              owner_profile_revision_id: 'profile-1',
              owner_target: 'aws-us-west-2',
              quarantine_reason: 'PROVIDER_OUTCOME_AMBIGUOUS',
              quarantined_at: 9_999,
            },
          ],
          qualification_mutation: {
            state: 'QUARANTINED',
            owner_target: 'aws-us-west-2',
          },
          profiles: [
            {
              ...readiness.profiles[0],
              qualification_targets: [
                {
                  ...readiness.profiles[0].qualification_targets[0],
                  repository_quarantined: true,
                  required_generation: 2,
                  quarantine_reason: 'PROVIDER_OUTCOME_AMBIGUOUS',
                  quarantined_at: 9_999,
                },
              ],
            },
          ],
        }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(
      screen.getByText('Qualification repository cutover required')
    ).toBeVisible();
    expect(screen.getByText(/use generation 2 or higher/)).toBeVisible();
    expect(
      screen.getByText(
        (_content, element) =>
          element?.classList.contains('text-red-700') &&
          element?.textContent === 'aws-us-west-2: g1 (quarantined, use g2+)'
      )
    ).toBeVisible();
    expect(
      screen.getByText('1 target requires a new generation')
    ).toBeVisible();
    expect(
      screen.getByRole('button', { name: 'Ingest handoff' })
    ).toBeEnabled();
    expect(screen.getByRole('button', { name: 'Run canary' })).toBeDisabled();
  });

  it('allows canarying a fresh candidate after the old active repo cutover', () => {
    render(
      <ImageReadiness
        readiness={{
          ...readiness,
          generated_at: Math.floor(Date.now() / 1000),
          qualification_repository_quarantines: [
            {
              repository_arn: 'arn:aws:ecr:us-west-2:123456789012:repository/q',
              owner_profile_revision_id: 'profile-active',
              owner_target: 'aws-us-west-2',
              quarantine_reason: 'PROVIDER_OUTCOME_AMBIGUOUS',
              quarantined_at: 9_999,
            },
          ],
          profiles: [
            {
              ...readiness.profiles[0],
              id: 'profile-active',
              state: 'ACTIVE',
              qualification_targets: [
                {
                  ...readiness.profiles[0].qualification_targets[0],
                  repository_quarantined: true,
                  required_generation: 2,
                },
              ],
            },
            {
              ...readiness.profiles[0],
              id: 'profile-candidate',
              revision: 3,
              desired_generation: 4,
              state: 'QUALIFYING',
              qualification_targets: [
                {
                  ...readiness.profiles[0].qualification_targets[0],
                  repository_arn:
                    'arn:aws:ecr:us-west-2:123456789012:repository/q-g02',
                  repository_generation: 2,
                  repository_quarantined: false,
                  required_generation: null,
                },
              ],
            },
          ],
        }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(screen.getByRole('button', { name: 'Run canary' })).toBeEnabled();
    expect(
      screen.getByText('Qualification repository quarantine retained')
    ).toBeVisible();
    expect(screen.getByText(/fresh generation 2 is qualifying/)).toBeVisible();
    expect(
      screen.getByText('1 replaced target references a retained tombstone')
    ).toBeVisible();
    expect(screen.queryByText('1 target requires a new generation')).toBeNull();
  });

  it('does not treat a changed target fingerprint as a quarantine replacement', () => {
    render(
      <ImageReadiness
        readiness={{
          ...readiness,
          generated_at: Math.floor(Date.now() / 1000),
          profiles: [
            {
              ...readiness.profiles[0],
              id: 'profile-active',
              qualification_targets: [
                {
                  ...readiness.profiles[0].qualification_targets[0],
                  repository_quarantined: true,
                  required_generation: 2,
                },
              ],
            },
            {
              ...readiness.profiles[0],
              id: 'profile-candidate',
              revision: 3,
              state: 'QUALIFYING',
              qualification_targets: [
                {
                  ...readiness.profiles[0].qualification_targets[0],
                  target_fingerprint: 'changed-target-fingerprint',
                  repository_arn:
                    'arn:aws:ecr:us-west-2:123456789012:repository/q-g02',
                  repository_generation: 2,
                },
              ],
            },
          ],
        }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(
      screen.getByText('Qualification repository cutover required')
    ).toBeVisible();
    expect(screen.queryByText(/fresh generation 2 is qualifying/)).toBeNull();
  });

  it('reports exhausted qualification generation space', () => {
    render(
      <ImageReadiness
        readiness={{
          ...readiness,
          generated_at: Math.floor(Date.now() / 1000),
          profiles: [
            {
              ...readiness.profiles[0],
              qualification_targets: [
                {
                  ...readiness.profiles[0].qualification_targets[0],
                  repository_generation: 255,
                  repository_quarantined: true,
                  required_generation: null,
                },
              ],
            },
          ],
        }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(
      screen.getByText(
        (_content, element) =>
          element?.classList.contains('text-red-700') &&
          element?.textContent ===
            'aws-us-west-2: g255 (quarantined, generation space exhausted)'
      )
    ).toBeVisible();
  });

  it('shows selected repository attestation independently per target', () => {
    render(
      <ImageReadiness
        readiness={{
          ...readiness,
          profiles: [
            {
              ...readiness.profiles[0],
              qualification_targets: [
                readiness.profiles[0].qualification_targets[0],
                {
                  ...readiness.profiles[0].qualification_targets[0],
                  target: 'aws-us-east-1',
                  region: 'us-east-1',
                  repository_generation: 2,
                  repository_attested: false,
                },
              ],
            },
          ],
        }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(screen.getByText('aws-us-west-2: g1 attested')).toBeVisible();
    expect(screen.getByText('aws-us-east-1: g2 not attested')).toBeVisible();
  });

  it.each([
    [
      'DELETING',
      /Provider deletion and all copy, canary, staging and activation work remain fenced/,
    ],
    ['RESTORING', /Only the exact owner copy may restore the digest/],
    [
      'QUARANTINED',
      /same logical profile may stage a higher generation and ingest its fresh Terraform handoff/,
    ],
  ])('shows state-specific %s mutation recovery', (state, message) => {
    render(
      <ImageReadiness
        readiness={{
          ...readiness,
          generated_at: Math.floor(Date.now() / 1000),
          qualification_mutation: {
            state,
            owner_target: 'aws-us-west-2',
          },
        }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(screen.getByRole('status')).toHaveTextContent(message);
    expect(screen.getByRole('button', { name: 'Run canary' })).toBeDisabled();
    const handoff = screen.getByRole('button', { name: 'Ingest handoff' });
    if (state === 'QUARANTINED') {
      expect(handoff).toBeEnabled();
    } else {
      expect(handoff).toBeDisabled();
    }
  });

  it('marks aggregate counts and the table as partial when groups truncate', () => {
    render(
      <ImageReadiness
        readiness={{ ...readiness, queues_truncated: true }}
        capabilities={capabilities}
        loading={false}
        error={null}
        onRefresh={jest.fn()}
      />
    );

    expect(screen.getAllByText('501+')).toHaveLength(1);
    expect(screen.getByText('3+ terminal failures')).toBeVisible();
    expect(
      screen.getByText(/Showing the first 100 target groups/)
    ).toBeVisible();
    expect(screen.getByText(/reached a safety bound/)).toBeVisible();
  });

  it('ages worker heartbeats from a live client clock', () => {
    const dateNow = jest.spyOn(Date, 'now').mockReturnValue(10_031_000);
    try {
      render(
        <ImageReadiness
          readiness={readiness}
          capabilities={capabilities}
          loading={false}
          error={null}
          onRefresh={jest.fn()}
        />
      );
      expect(
        screen
          .getAllByText('36s ago')
          .find((element) => element.classList.contains('text-amber-700'))
      ).toBeDefined();
    } finally {
      dateNow.mockRestore();
    }
  });

  it('pauses hidden clock renders and refreshes once on visibility restore', () => {
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    const commits = [];
    jest.useFakeTimers();
    jest.setSystemTime(10_000_000);
    const dateNow = jest.spyOn(Date, 'now');
    setDocumentVisibility('hidden');

    const view = render(
      <Profiler
        id="image-readiness"
        onRender={(id, phase) => commits.push(phase)}
      >
        <ImageReadiness
          readiness={readiness}
          capabilities={capabilities}
          loading={false}
          error={null}
          onRefresh={jest.fn()}
        />
      </Profiler>
    );

    try {
      expect(commits).toEqual(['mount']);
      const clockReadsAfterMount = dateNow.mock.calls.length;
      for (let tick = 0; tick < 5; tick += 1) {
        act(() => jest.advanceTimersByTime(5000));
      }
      act(() => jest.advanceTimersByTime(4999));
      expect(commits).toEqual(['mount']);
      expect(dateNow).toHaveBeenCalledTimes(clockReadsAfterMount);

      setDocumentVisibility('visible');
      act(() => {
        window.document.dispatchEvent(new Event('visibilitychange'));
      });
      expect(commits).toEqual(['mount', 'update']);
      expect(
        screen
          .getAllByText('34s ago')
          .find((element) => element.classList.contains('text-amber-700'))
      ).toBeDefined();

      act(() => jest.advanceTimersByTime(1));
      expect(commits).toEqual(['mount', 'update']);

      act(() => jest.advanceTimersByTime(5000));
      expect(commits).toEqual(['mount', 'update', 'update']);
      expect(
        screen
          .getAllByText('40s ago')
          .find((element) => element.classList.contains('text-amber-700'))
      ).toBeDefined();

      view.unmount();
      const clockReadsAfterUnmount = dateNow.mock.calls.length;
      setDocumentVisibility('hidden');
      window.document.dispatchEvent(new Event('visibilitychange'));
      setDocumentVisibility('visible');
      act(() => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        jest.advanceTimersByTime(10_000);
      });
      expect(commits).toEqual(['mount', 'update', 'update']);
      expect(dateNow).toHaveBeenCalledTimes(clockReadsAfterUnmount);
    } finally {
      view.unmount();
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
      dateNow.mockRestore();
      jest.useRealTimers();
    }
  });
});
