import {
  getEffectiveHistorySelection,
  getHistoryBounds,
  normalizeDraggedRange,
  resolveHistoryRange,
} from './serve-history-range';

describe('serve history range helpers', () => {
  const history = {
    available: true,
    bucketSeconds: 60,
    windowStart: 0,
    windowEnd: 24 * 60 * 60,
    samples: [],
    requestSamples: [],
  };

  it('defaults to the last hour and supports longer rolling presets', () => {
    expect(resolveHistoryRange(history, null)).toEqual({
      start: 23 * 60 * 60 + 60,
      end: 24 * 60 * 60,
    });
    expect(
      resolveHistoryRange(history, {
        kind: 'preset',
        seconds: 12 * 60 * 60,
      })
    ).toEqual({
      start: 12 * 60 * 60 + 60,
      end: 24 * 60 * 60,
    });
  });

  it('advances rolling presets on refresh but keeps custom ranges fixed', () => {
    const refreshed = { ...history, windowEnd: history.windowEnd + 60 };
    expect(
      resolveHistoryRange(refreshed, {
        kind: 'preset',
        seconds: 60 * 60,
      })
    ).toEqual({
      start: 23 * 60 * 60 + 120,
      end: 24 * 60 * 60 + 60,
    });
    expect(
      resolveHistoryRange(refreshed, {
        kind: 'custom',
        start: 120,
        end: 300,
      })
    ).toEqual({ start: 120, end: 300 });
  });

  it('returns to the default preset after a custom range ages out', () => {
    const expired = {
      ...history,
      windowStart: 24 * 60 * 60,
      windowEnd: 48 * 60 * 60,
    };
    expect(
      getEffectiveHistorySelection(expired, {
        kind: 'custom',
        start: 120,
        end: 300,
      })
    ).toEqual({ kind: 'preset', seconds: 60 * 60 });
  });

  it('keeps custom selections fixed while clamping them to history bounds', () => {
    expect(
      resolveHistoryRange(history, {
        kind: 'custom',
        start: -60,
        end: 180,
      })
    ).toEqual({ start: 0, end: 180 });
  });

  it('normalizes reverse drags, bucket alignment, and minimum width', () => {
    const bounds = getHistoryBounds(history);
    expect(normalizeDraggedRange(185, 65, bounds, 60)).toEqual({
      start: 60,
      end: 240,
    });
    expect(normalizeDraggedRange(120, 121, bounds, 60)).toEqual({
      start: 120,
      end: 180,
    });
  });
});
