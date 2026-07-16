import { buildRequestHistoryView } from './serve-request-history';

describe('buildRequestHistoryView', () => {
  it('fills missing minute buckets and derives selected-range statistics', () => {
    const view = buildRequestHistoryView(
      {
        available: true,
        bucketSeconds: 60,
        requestSamples: [
          { timestamp: 120, requestCount: 3 },
          { timestamp: 240, requestCount: 9 },
          { timestamp: 300, requestCount: 99 },
        ],
      },
      { start: 120, end: 240 }
    );

    expect(view.timestamps).toEqual([120, 180, 240]);
    expect(view.counts).toEqual([3, 0, 9]);
    expect(view.stats).toEqual({
      total: 12,
      averagePerMinute: 4,
      peakPerMinute: 9,
    });
  });

  it('returns no chart when durable history is unavailable', () => {
    expect(
      buildRequestHistoryView(
        {
          available: false,
          requestSamples: [{ timestamp: 120, requestCount: 3 }],
        },
        { start: 120, end: 180 }
      )
    ).toEqual({ timestamps: [], counts: [], stats: null });
  });
});
