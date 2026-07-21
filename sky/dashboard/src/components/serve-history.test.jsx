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
  rejectionHistoryAvailable: true,
  requestSamples: [
    { timestamp: 24 * 60 * 60, requestCount: 3, rejectedCount: 0 },
  ],
  autoscalerSamples: [
    {
      timestamp: 24 * 60 * 60,
      peakInFlight: 1,
      peakQueueDepth: 0,
      demandTarget: 1,
      capacityTarget: 1,
      readyCapacity: 1,
      provisioningCapacity: 0,
      version: 1,
    },
  ],
  samples: [
    {
      timestamp: 180,
      version: 1,
      readyCount: 1,
      readyReservedCount: 0,
      provisioningCount: 0,
      notReadyCount: 0,
      erroredCount: 0,
      preemptedCount: 0,
      stoppingCount: 0,
      totalCount: 1,
      logicalReadyCount: 8,
      logicalReadyReservedCount: 0,
      logicalProvisioningCount: 0,
      logicalNotReadyCount: 0,
      logicalErroredCount: 0,
      logicalPreemptedCount: 0,
      logicalStoppingCount: 0,
      logicalTotalCount: 8,
    },
    {
      timestamp: 24 * 60 * 60,
      version: 1,
      readyCount: 1,
      readyReservedCount: 1,
      provisioningCount: 0,
      notReadyCount: 0,
      erroredCount: 0,
      preemptedCount: 0,
      stoppingCount: 0,
      totalCount: 1,
      logicalReadyCount: 8,
      logicalReadyReservedCount: 8,
      logicalProvisioningCount: 0,
      logicalNotReadyCount: 0,
      logicalErroredCount: 0,
      logicalPreemptedCount: 0,
      logicalStoppingCount: 0,
      logicalTotalCount: 8,
    },
  ],
};

function chartRanges() {
  return [
    'Request history chart',
    'Demand pressure chart',
    'Machine history chart',
  ].map((label) => {
    const chart = screen.getByRole('button', { name: label });
    return {
      start: Number(chart.dataset.start),
      end: Number(chart.dataset.end),
    };
  });
}

describe('ServeHistorySection', () => {
  it('keeps all history charts synchronized across presets and drag', () => {
    render(<ServeHistorySection history={history} />);

    expect(screen.getByText('Latest ready')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Logical' })).toHaveAttribute(
      'aria-pressed',
      'true'
    );
    expect(screen.getByRole('button', { name: 'Physical' })).toHaveAttribute(
      'aria-pressed',
      'false'
    );
    fireEvent.click(screen.getByRole('button', { name: 'Physical' }));
    expect(screen.getByRole('button', { name: 'Physical' })).toHaveAttribute(
      'aria-pressed',
      'true'
    );
    expect(
      screen.getByText('Status by physical backend in the selected range')
    ).toBeInTheDocument();
    expect(screen.getByRole('button', { name: '1h' })).toHaveAttribute(
      'aria-pressed',
      'true'
    );
    expect(chartRanges()).toEqual([
      { start: 23 * 60 * 60 + 60, end: 24 * 60 * 60 },
      { start: 23 * 60 * 60 + 60, end: 24 * 60 * 60 },
      { start: 23 * 60 * 60 + 60, end: 24 * 60 * 60 },
    ]);

    fireEvent.click(screen.getByRole('button', { name: '12h' }));
    expect(chartRanges()).toEqual([
      { start: 12 * 60 * 60 + 60, end: 24 * 60 * 60 },
      { start: 12 * 60 * 60 + 60, end: 24 * 60 * 60 },
      { start: 12 * 60 * 60 + 60, end: 24 * 60 * 60 },
    ]);

    fireEvent.click(
      screen.getByRole('button', { name: 'Request history chart' })
    );
    expect(chartRanges()).toEqual([
      { start: 120, end: 300 },
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
      { start: 120, end: 300 },
    ]);
  });
});
