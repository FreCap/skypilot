import React from 'react';
import { fireEvent, render, screen } from '@testing-library/react';

import { SelectableHistoryLine } from './serve-history-range';

jest.mock('react-chartjs-2', () => {
  const ReactModule = require('react');
  return {
    Line: ReactModule.forwardRef(function MockLine(_props, ref) {
      ReactModule.useImperativeHandle(ref, () => ({
        canvas: {
          getBoundingClientRect: () => ({
            left: 0,
            top: 0,
            width: 120,
            height: 100,
          }),
        },
        chartArea: { left: 10, right: 110, top: 5, bottom: 95 },
        scales: { x: { getValueForPixel: (pixel) => pixel } },
      }));
      return <canvas data-testid="chart-canvas" />;
    }),
  };
});

describe('SelectableHistoryLine', () => {
  beforeAll(() => {
    window.PointerEvent = MouseEvent;
  });

  it('converts a pointer drag into a bucket-aligned selected range', () => {
    const onRangeSelect = jest.fn();
    render(
      <SelectableHistoryLine
        data={{ datasets: [] }}
        options={{ plugins: {} }}
        range={{ start: 0, end: 180 }}
        bucketSeconds={60}
        onRangeSelect={onRangeSelect}
        ariaLabel="Selectable chart"
      />
    );

    const chart = screen.getByLabelText('Selectable chart');
    fireEvent.pointerDown(chart, {
      button: 0,
      pointerId: 1,
      clientX: 20,
    });
    fireEvent.pointerMove(chart, { pointerId: 1, clientX: 100 });
    fireEvent.pointerUp(chart, { pointerId: 1, clientX: 100 });

    expect(onRangeSelect).toHaveBeenCalledWith({ start: 0, end: 120 });
  });

  it('does not select on a click or a cancelled drag', () => {
    const onRangeSelect = jest.fn();
    render(
      <SelectableHistoryLine
        data={{ datasets: [] }}
        options={{ plugins: {} }}
        range={{ start: 0, end: 180 }}
        bucketSeconds={60}
        onRangeSelect={onRangeSelect}
        ariaLabel="Selectable chart"
      />
    );

    const chart = screen.getByLabelText('Selectable chart');
    fireEvent.pointerDown(chart, {
      button: 0,
      pointerId: 1,
      clientX: 20,
    });
    fireEvent.pointerUp(chart, { pointerId: 1, clientX: 22 });
    fireEvent.pointerDown(chart, {
      button: 0,
      pointerId: 2,
      clientX: 20,
    });
    fireEvent.pointerMove(chart, { pointerId: 2, clientX: 100 });
    fireEvent.pointerCancel(chart, { pointerId: 2, clientX: 100 });

    expect(onRangeSelect).not.toHaveBeenCalled();
  });
});
