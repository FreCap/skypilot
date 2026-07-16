import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';

import { ServeHistorySection } from './serve-history';

jest.mock('./serve-history-range', () => {
  const actual = jest.requireActual('./serve-history-range');
  return {
    ...actual,
    SelectableHistoryLine: ({ range, onRangeSelect, ariaLabel }) => (
      <button
        type="button"
        aria-label={ariaLabel}
        data-start={range.start}
        data-end={range.end}
        onClick={() => onRangeSelect({ start: 120, end: 300 })}
      >
        chart
      </button>
    ),
  };
});

const history = {
  available: true,
  bucketSeconds: 60,
  windowStart: 0,
  windowEnd: 24 * 60 * 60,
  requestSamples: [{ timestamp: 24 * 60 * 60, requestCount: 3 }],
  samples: [
    {
      timestamp: 180,
      version: 1,
      readyCount: 1,
      provisioningCount: 0,
      notReadyCount: 0,
      erroredCount: 0,
      preemptedCount: 0,
      stoppingCount: 0,
    },
    {
      timestamp: 24 * 60 * 60,
      version: 1,
      readyCount: 1,
      provisioningCount: 0,
      notReadyCount: 0,
      erroredCount: 0,
      preemptedCount: 0,
      stoppingCount: 0,
    },
  ],
};

function chartRanges() {
  return ['Request history chart', 'Machine history chart'].map((label) => {
    const chart = screen.getByRole('button', { name: label });
    return {
      start: Number(chart.dataset.start),
      end: Number(chart.dataset.end),
    };
  });
}

describe('ServeHistorySection', () => {
  it('keeps request and machine charts synchronized across presets and drag', () => {
    render(<ServeHistorySection history={history} />);

    expect(screen.getByText('Latest ready')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '1h' })).toHaveAttribute(
      'aria-pressed',
      'true'
    );
    expect(chartRanges()).toEqual([
      { start: 23 * 60 * 60 + 60, end: 24 * 60 * 60 },
      { start: 23 * 60 * 60 + 60, end: 24 * 60 * 60 },
    ]);

    fireEvent.click(screen.getByRole('button', { name: '12h' }));
    expect(chartRanges()).toEqual([
      { start: 12 * 60 * 60 + 60, end: 24 * 60 * 60 },
      { start: 12 * 60 * 60 + 60, end: 24 * 60 * 60 },
    ]);

    fireEvent.click(
      screen.getByRole('button', { name: 'Request history chart' })
    );
    expect(chartRanges()).toEqual([
      { start: 120, end: 300 },
      { start: 120, end: 300 },
    ]);

    fireEvent.click(screen.getByRole('button', { name: '1h' }));
    fireEvent.click(
      screen.getByRole('button', { name: 'Machine history chart' })
    );
    expect(chartRanges()).toEqual([
      { start: 120, end: 300 },
      { start: 120, end: 300 },
    ]);
  });
});
