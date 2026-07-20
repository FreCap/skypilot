import { fireEvent, render, screen } from '@testing-library/react';

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
      failed_count: 3,
      oldest_queued_at: 9_900,
      quota_bound_eta_seconds: 51,
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
});
