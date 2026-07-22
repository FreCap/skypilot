jest.mock('chart.js', () => ({
  BarElement: {},
  CategoryScale: {},
  Chart: { register: jest.fn() },
  Legend: {},
  LinearScale: {},
  Tooltip: {},
}));

jest.mock('react-chartjs-2', () => ({
  Bar: ({ data }) => (
    <div data-testid="response-histogram">
      {data.datasets[0].data.join('|')}
    </div>
  ),
}));

jest.mock('./serve-history-range', () => ({
  SelectableHistoryLine: ({ data, ariaLabel }) => (
    <div aria-label={ariaLabel} data-testid="response-trend">
      {data.datasets.map((dataset) => dataset.label).join('|')}
    </div>
  ),
  historyLinearScale: () => ({}),
}));

import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';

import {
  buildResponseTimeHistoryView,
  ResponseTimeHistoryCard,
} from './serve-response-time-history';

const history = {
  available: true,
  bucketSeconds: 60,
  responseTimeHistogramVersion: 1,
  responseTimeBucketUpperBoundsSeconds: [1, 2, 4],
  responseTimeSamples: [
    {
      timestamp: 120,
      statusClassCounts: {
        '2xx': [1, 1, 2, 0],
        '5xx': [0, 0, 0, 1],
      },
    },
    {
      timestamp: 180,
      statusClassCounts: {
        '2xx': [0, 2, 0, 0],
      },
    },
  ],
};

describe('response-time history', () => {
  it('aggregates status classes and derives fixed-bucket quantiles', () => {
    const all = buildResponseTimeHistoryView(history, {
      start: 120,
      end: 180,
    });
    expect(all.aggregateCounts).toEqual([1, 3, 2, 1]);
    expect(all.samples).toBe(7);
    expect(all.selectedP50).toBe(2);
    expect(all.selectedP95).toBe(4);
    expect(all.selectedP95Overflow).toBe(true);
    expect(all.p50).toEqual([4, 2]);

    const successes = buildResponseTimeHistoryView(
      history,
      { start: 120, end: 180 },
      '2xx'
    );
    expect(successes.aggregateCounts).toEqual([1, 3, 2, 0]);
    expect(successes.samples).toBe(6);
    expect(successes.selectedP95).toBe(4);
    expect(successes.selectedP95Overflow).toBe(false);
  });

  it('switches the visible histogram by final status class', () => {
    render(
      <ResponseTimeHistoryCard
        history={history}
        range={{ start: 120, end: 180 }}
        onRangeSelect={() => {}}
      />
    );

    expect(
      screen.getByText('Completed in range').nextSibling
    ).toHaveTextContent('7');
    expect(screen.getByTestId('response-trend')).toHaveTextContent(
      'p50|p95|p99'
    );
    fireEvent.click(screen.getByRole('button', { name: '5xx' }));
    expect(
      screen.getByText('Completed in range').nextSibling
    ).toHaveTextContent('1');
    expect(screen.getByTestId('response-histogram')).toHaveTextContent(
      '0|0|0|1'
    );
  });
});
