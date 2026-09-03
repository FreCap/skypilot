import { buildDemandPressureView } from './serve-demand-pressure';

describe('buildDemandPressureView', () => {
  it('leaves missing rejection coverage as a gap', () => {
    const view = buildDemandPressureView(
      {
        available: true,
        bucketSeconds: 60,
        rejectionHistoryAvailable: true,
        requestSamples: [
          { timestamp: 120, requestCount: 8, rejectedCount: 2 },
          { timestamp: 240, requestCount: 4, rejectedCount: 1 },
        ],
        autoscalerSamples: [
          { timestamp: 120, peakInFlight: 7, peakQueueDepth: 3 },
          { timestamp: 240, peakInFlight: 2, peakQueueDepth: 5 },
        ],
      },
      { start: 120, end: 240 }
    );

    expect(view).toEqual({
      supported: true,
      timestamps: [120, 180, 240],
      inFlight: [7, null, 2],
      queued: [3, null, 5],
      rejected: [2, null, 1],
      stats: {
        peakInFlight: 7,
        peakQueued: 5,
        totalRejected: null,
        peakRejected: 2,
      },
    });
  });

  it('totals rejections when every selected minute is covered', () => {
    const view = buildDemandPressureView(
      {
        available: true,
        bucketSeconds: 60,
        rejectionHistoryAvailable: true,
        requestSamples: [
          { timestamp: 120, requestCount: 8, rejectedCount: 2 },
          { timestamp: 180, requestCount: 5, rejectedCount: 0 },
          { timestamp: 240, requestCount: 4, rejectedCount: 1 },
        ],
        autoscalerSamples: [],
      },
      { start: 120, end: 240 }
    );

    expect(view.rejected).toEqual([2, 0, 1]);
    expect(view.stats.totalRejected).toBe(3);
  });

  it('stays hidden for an old server without pressure fields', () => {
    expect(
      buildDemandPressureView(
        {
          available: true,
          requestSamples: [{ timestamp: 120, requestCount: 1 }],
          autoscalerSamples: [],
        },
        { start: 120, end: 180 }
      )
    ).toEqual({
      supported: false,
      timestamps: [],
      inFlight: [],
      queued: [],
      rejected: [],
      stats: null,
    });
  });

  it('leaves mixed-version rejection minutes as gaps', () => {
    const view = buildDemandPressureView(
      {
        available: true,
        bucketSeconds: 60,
        rejectionHistoryAvailable: true,
        requestSamples: [
          { timestamp: 120, requestCount: 8, rejectedCount: 2 },
          { timestamp: 180, requestCount: 3, rejectedCount: null },
        ],
      },
      { start: 120, end: 240 }
    );

    expect(view.rejected).toEqual([2, null, null]);
    expect(view.stats.totalRejected).toBeNull();
    expect(view.stats.peakRejected).toBe(2);
  });
});
