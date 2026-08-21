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
    <div data-testid="prediction-histogram">
      {data.datasets[0].data.join('|')}
    </div>
  ),
}));

jest.mock('./serve-history-range', () => ({
  SelectableHistoryLine: ({ data, ariaLabel }) => (
    <div aria-label={ariaLabel} data-testid="prediction-trend">
      {data.datasets.map((dataset) => dataset.label).join('|')}
    </div>
  ),
  historyLinearScale: () => ({}),
}));

import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';

import {
  buildPredictionTimeHistoryView,
  PredictionTimeHistoryCard,
} from './serve-prediction-time-history';

const history = {
  available: true,
  bucketSeconds: 60,
  predictionTimeHistogramVersion: 1,
  predictionTimeBucketUpperBoundsSeconds: [1, 2, 4],
  predictionTimeSamples: [
    {
      timestamp: 120,
      outcomeCounts: {
        succeeded: [1, 1, 2, 0],
        failed: [0, 0, 0, 1],
      },
    },
    {
      timestamp: 180,
      outcomeCounts: {
        succeeded: [0, 2, 0, 0],
      },
    },
  ],
};

describe('prediction-time history', () => {
  it('aggregates outcomes and derives fixed-bucket quantiles', () => {
    const all = buildPredictionTimeHistoryView(history, {
      start: 120,
      end: 180,
    });
    expect(all.aggregateCounts).toEqual([1, 3, 2, 1]);
    expect(all.samples).toBe(7);
    expect(all.selectedP50).toBe(2);
    expect(all.selectedP95).toBe(4);
    expect(all.selectedP95Overflow).toBe(true);
    expect(all.p50).toEqual([4, 2]);

    const successes = buildPredictionTimeHistoryView(
      history,
      { start: 120, end: 180 },
      'succeeded'
    );
    expect(successes.aggregateCounts).toEqual([1, 3, 2, 0]);
    expect(successes.samples).toBe(6);
    expect(successes.selectedP95).toBe(4);
    expect(successes.selectedP95Overflow).toBe(false);
  });

  it('switches the visible histogram by prediction outcome', () => {
    render(
      <PredictionTimeHistoryCard
        history={history}
        range={{ start: 120, end: 180 }}
        onRangeSelect={() => {}}
      />
    );

    expect(
      screen.getByText('Recorded terminal observations in range').nextSibling
    ).toHaveTextContent('7');
    expect(screen.getByTestId('prediction-trend')).toHaveTextContent(
      'p50|p95|p99'
    );
    fireEvent.click(screen.getByRole('button', { name: 'Failed' }));
    expect(
      screen.getByText('Recorded terminal observations in range').nextSibling
    ).toHaveTextContent('1');
    expect(screen.getByTestId('prediction-histogram')).toHaveTextContent(
      '0|0|0|1'
    );
  });
});
