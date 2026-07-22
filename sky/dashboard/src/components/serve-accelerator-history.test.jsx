import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';

import {
  AcceleratorHistoryCard,
  buildAcceleratorHistoryView,
} from './serve-accelerator-history';

jest.mock('./serve-history-range', () => {
  const actual = jest.requireActual('./serve-history-range');
  return {
    ...actual,
    SelectableHistoryLine: ({ data, ariaLabel }) => (
      <div aria-label={ariaLabel}>
        {data.datasets.map((dataset) => (
          <span key={dataset.label}>{dataset.label}</span>
        ))}
      </div>
    ),
  };
});

const history = {
  available: true,
  bucketSeconds: 60,
  autoscalerSamples: [
    {
      timestamp: 120,
      replicaUnit: 'logical_slot',
      acceleratorBreakdown: {
        configuredAccelerators: ['A100', 'A100-80GB'],
        minReplicas: { A100: 1, 'A100-80GB': 2 },
        demandTarget: { A100: 3, 'A100-80GB': 4 },
        warmRetentionTarget: { A100: 2, 'A100-80GB': 1 },
        coldLaunchAuthority: { A100: 0, 'A100-80GB': 3 },
        readyCapacity: { A100: 2, 'A100-80GB': 1 },
        provisioningCapacity: { A100: 1, 'A100-80GB': 3 },
        totalCapacity: { A100: 3, 'A100-80GB': 4 },
        zeroCostReadyCapacity: { A100: 1, 'A100-80GB': 0 },
        fillTarget: { A100: 3, 'A100-80GB': 2 },
        freeReservedSlots: { A100: 2, 'A100-80GB': 1 },
      },
    },
  ],
};

describe('AcceleratorHistoryCard', () => {
  it('keeps exact A100 variants as separate time series', () => {
    const view = buildAcceleratorHistoryView(history, {
      start: 120,
      end: 120,
    });

    expect(view.cards).toEqual(['A100', 'A100-80GB']);
    expect(view.valuesByCard.A100.demandTarget).toEqual([3]);
    expect(view.valuesByCard['A100-80GB'].demandTarget).toEqual([4]);
    expect(view.valuesByCard.A100.warmRetentionTarget).toEqual([2]);
    expect(view.valuesByCard['A100-80GB'].coldLaunchAuthority).toEqual([3]);
  });

  it('switches between serving and reserved signals without losing cards', () => {
    render(
      <AcceleratorHistoryCard
        history={history}
        range={{ start: 120, end: 120 }}
        onRangeSelect={jest.fn()}
      />
    );

    expect(
      screen.getByLabelText('A100 accelerator history chart')
    ).toHaveTextContent('Serving target by card');
    expect(
      screen.getByLabelText('A100 accelerator history chart')
    ).toHaveTextContent('Cold-launch authority');
    expect(
      screen.getByLabelText('A100-80GB accelerator history chart')
    ).toHaveTextContent('Ready capacity');

    fireEvent.click(screen.getByRole('button', { name: 'Reserved capacity' }));
    expect(
      screen.getByLabelText('A100 accelerator history chart')
    ).toHaveTextContent('Reserved-fill target');
    expect(
      screen.getByLabelText('A100-80GB accelerator history chart')
    ).toHaveTextContent('Free reserved slots');
  });
});
