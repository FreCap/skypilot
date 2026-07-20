import { buildReplicaHistoryView } from './serve-replica-history';

describe('buildReplicaHistoryView', () => {
  it('sums physical versions, splits reserved ready, and preserves gaps', () => {
    const view = buildReplicaHistoryView(
      {
        available: true,
        bucketSeconds: 60,
        samples: [
          {
            timestamp: 120,
            version: 1,
            readyCount: 2,
            readyReservedCount: 1,
            provisioningCount: 1,
            notReadyCount: 0,
            erroredCount: 1,
            preemptedCount: 0,
            stoppingCount: 0,
            totalCount: 4,
          },
          {
            timestamp: 120,
            version: 2,
            readyCount: 3,
            readyReservedCount: 2,
            provisioningCount: 0,
            notReadyCount: 0,
            erroredCount: 0,
            preemptedCount: 0,
            stoppingCount: 1,
            totalCount: 4,
          },
          {
            timestamp: 240,
            version: 2,
            readyCount: 4,
            readyReservedCount: 1,
            provisioningCount: 0,
            notReadyCount: 1,
            erroredCount: 2,
            preemptedCount: 1,
            stoppingCount: 0,
            totalCount: 8,
          },
        ],
      },
      { start: 120, end: 240 },
      'physical'
    );

    expect(view.timestamps).toEqual([120, 180, 240]);
    expect(
      view.datasets.find(({ key }) => key === 'readyOrdinaryCount').data
    ).toEqual([2, null, 3]);
    expect(
      view.datasets.find(({ key }) => key === 'readyReservedCount').data
    ).toEqual([3, null, 1]);
    expect(view.versionBreakdowns[0].map(({ version }) => version)).toEqual([
      1, 2,
    ]);
    expect(view.stats).toMatchObject({
      averageReady: 4.5,
      minimumReady: 4,
      peakErrored: 2,
      erroredMinutes: 3,
      stoppingMinutes: 1,
    });
    expect(view.stats.current.readyCount).toBe(4);
    expect(view.stats.current.readyReservedCount).toBe(1);
  });

  it('defaults to logical slots and gaps an incomplete legacy minute', () => {
    const view = buildReplicaHistoryView(
      {
        available: true,
        bucketSeconds: 60,
        samples: [
          {
            timestamp: 60,
            version: 1,
            logicalReadyCount: 16,
            logicalReadyReservedCount: 8,
            logicalProvisioningCount: 4,
            logicalNotReadyCount: 0,
            logicalErroredCount: 0,
            logicalPreemptedCount: 0,
            logicalStoppingCount: 0,
            logicalTotalCount: 20,
          },
          {
            timestamp: 120,
            version: 1,
            logicalReadyCount: null,
            logicalReadyReservedCount: null,
            logicalProvisioningCount: null,
            logicalNotReadyCount: null,
            logicalErroredCount: null,
            logicalPreemptedCount: null,
            logicalStoppingCount: null,
            logicalTotalCount: null,
          },
        ],
      },
      { start: 60, end: 120 }
    );

    expect(view.mode).toBe('logical');
    expect(view.config.axisTitle).toBe('Logical slots');
    expect(
      view.datasets.find(({ key }) => key === 'readyOrdinaryCount').data
    ).toEqual([8, null]);
    expect(
      view.datasets.find(({ key }) => key === 'readyReservedCount').data
    ).toEqual([8, null]);
    expect(view.stats.current.readyCount).toBe(16);
  });

  it('keeps legacy physical ready totals without fabricating reserved zeroes', () => {
    const view = buildReplicaHistoryView(
      {
        available: true,
        bucketSeconds: 60,
        samples: [
          {
            timestamp: 60,
            version: 1,
            readyCount: 2,
            readyReservedCount: null,
            provisioningCount: 0,
            notReadyCount: 0,
            erroredCount: 0,
            preemptedCount: 0,
            stoppingCount: 0,
            totalCount: 2,
          },
        ],
      },
      { start: 60, end: 60 },
      'physical'
    );

    expect(view.datasets.map(({ key }) => key)).not.toContain(
      'readyReservedCount'
    );
    expect(
      view.datasets.find(({ key }) => key === 'readyOrdinaryCount').data
    ).toEqual([2]);
    expect(view.stats.current.readyReservedCount).toBeNull();
  });

  it('returns an empty view when history is unavailable', () => {
    const view = buildReplicaHistoryView(
      {
        available: false,
        samples: [{ timestamp: 60, version: 1, readyCount: 1 }],
      },
      { start: 60, end: 120 }
    );
    expect(view.timestamps).toEqual([]);
    expect(view.stats).toBeNull();
  });
});
