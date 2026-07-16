import { buildReplicaHistoryView } from './serve-replica-history';

describe('buildReplicaHistoryView', () => {
  it('sums versions, preserves missing-minute gaps, and derives statistics', () => {
    const view = buildReplicaHistoryView({
      available: true,
      bucketSeconds: 60,
      windowStart: 120,
      windowEnd: 240,
      samples: [
        {
          timestamp: 120,
          version: 1,
          readyCount: 2,
          provisioningCount: 1,
          notReadyCount: 0,
          erroredCount: 1,
          preemptedCount: 0,
          stoppingCount: 0,
        },
        {
          timestamp: 120,
          version: 2,
          readyCount: 3,
          provisioningCount: 0,
          notReadyCount: 0,
          erroredCount: 0,
          preemptedCount: 0,
          stoppingCount: 1,
        },
        {
          timestamp: 240,
          version: 2,
          readyCount: 4,
          provisioningCount: 0,
          notReadyCount: 1,
          erroredCount: 2,
          preemptedCount: 1,
          stoppingCount: 0,
        },
      ],
    });

    expect(view.timestamps).toEqual([120, 180, 240]);
    expect(view.datasets.find(({ key }) => key === 'readyCount').data).toEqual([
      5,
      null,
      4,
    ]);
    expect(view.versionBreakdowns[0].map(({ version }) => version)).toEqual([
      1, 2,
    ]);
    expect(view.stats).toMatchObject({
      averageReady: 4.5,
      minimumReady: 4,
      peakErrored: 2,
      erroredMachineMinutes: 3,
      stoppingMachineMinutes: 1,
    });
    expect(view.stats.current.readyCount).toBe(4);
  });

  it('returns an empty view when history is unavailable', () => {
    const view = buildReplicaHistoryView({
      available: false,
      samples: [{ timestamp: 60, version: 1, readyCount: 1 }],
    });
    expect(view.timestamps).toEqual([]);
    expect(view.stats).toBeNull();
  });
});
