jest.mock('@/components/serve-history-range', () => {
  const React = require('react');
  return {
    SelectableHistoryLine: ({ data }) =>
      React.createElement(
        'div',
        {
          'data-testid': 'history-series',
          'data-request-span-gaps': String(
            data.datasets.find(
              (dataset) => dataset.label === 'Recorded request attempts'
            )?.spanGaps
          ),
        },
        data.datasets.map((dataset) => dataset.label).join('|')
      ),
    historyLinearScale: () => ({}),
  };
});

import React from 'react';
import { render, screen } from '@testing-library/react';

import {
  buildRequestHistoryView,
  RequestHistoryCard,
} from './serve-request-history';

describe('buildRequestHistoryView', () => {
  it('distinguishes missing minutes from explicit zero coverage', () => {
    const view = buildRequestHistoryView(
      {
        available: true,
        bucketSeconds: 60,
        requestSamples: [
          { timestamp: 120, requestCount: 3 },
          { timestamp: 240, requestCount: 0 },
          { timestamp: 300, requestCount: 99 },
        ],
      },
      { start: 120, end: 240 }
    );

    expect(view.timestamps).toEqual([120, 180, 240]);
    expect(view.counts).toEqual([3, null, 0]);
    expect(view.demandTargets).toEqual([null, null, null]);
    expect(view.capacityTargets).toEqual([null, null, null]);
    expect(view.readyCapacities).toEqual([null, null, null]);
    expect(view.provisioningCapacities).toEqual([null, null, null]);
    expect(view.totalCapacities).toEqual([null, null, null]);
    expect(view.events).toEqual([]);
    expect(view.stats).toEqual({
      total: 3,
      averagePerMinute: 1.5,
      peakPerMinute: 3,
    });
    expect(view.capacityStats).toBeNull();
  });

  it('derives target deficits and lifecycle markers', () => {
    const view = buildRequestHistoryView(
      {
        available: true,
        bucketSeconds: 60,
        requestSamples: [],
        autoscalerSamples: [
          {
            timestamp: 120,
            controllerSessionId: 'a',
            version: 1,
            demandTarget: 2,
            capacityTarget: 4,
            readyCapacity: 1,
            provisioningCapacity: 3,
            totalCapacity: 6,
          },
          {
            timestamp: 180,
            controllerSessionId: 'a',
            version: 1,
            demandTarget: 3,
            capacityTarget: 5,
            readyCapacity: 3,
            provisioningCapacity: 2,
            totalCapacity: 7,
          },
          {
            timestamp: 240,
            controllerSessionId: 'b',
            version: 2,
            demandTarget: 2,
            capacityTarget: 2,
            readyCapacity: 2,
            provisioningCapacity: 0,
            totalCapacity: 2,
          },
        ],
      },
      { start: 120, end: 240 }
    );

    expect(view.demandTargets).toEqual([2, 3, 2]);
    expect(view.capacityTargets).toEqual([4, 5, 2]);
    expect(view.readyCapacities).toEqual([1, 3, 2]);
    expect(view.provisioningCapacities).toEqual([3, 2, 0]);
    expect(view.totalCapacities).toEqual([6, 7, 2]);
    expect(view.counts).toEqual([null, null, null]);
    expect(view.stats).toEqual({
      total: null,
      averagePerMinute: null,
      peakPerMinute: null,
    });
    expect(view.capacityStats).toEqual({
      peakDemandTarget: 3,
      peakCapacityTarget: 5,
      peakDeficit: 3,
      belowTargetMinutes: 2,
      longestBelowTargetMinutes: 2,
    });
    expect(view.events).toEqual([
      {
        timestamp: 240,
        y: 2,
        kind: 'restart',
        label: 'Controller restarted',
      },
      {
        timestamp: 240,
        y: 2,
        kind: 'update',
        label: 'Service updated to v2',
      },
    ]);
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
    ).toEqual({
      timestamps: [],
      counts: [],
      demandTargets: [],
      capacityTargets: [],
      readyCapacities: [],
      provisioningCapacities: [],
      totalCapacities: [],
      events: [],
      stats: null,
      capacityStats: null,
    });
  });
});

describe('RequestHistoryCard semantics', () => {
  it('distinguishes traffic, reservation, and non-failed capacity', () => {
    render(
      <RequestHistoryCard
        history={{
          available: true,
          bucketSeconds: 60,
          requestSamples: [{ timestamp: 120, requestCount: 3 }],
          autoscalerSamples: [
            {
              timestamp: 120,
              demandTarget: 2,
              capacityTarget: 4,
              readyCapacity: 1,
              provisioningCapacity: 2,
              totalCapacity: 5,
            },
          ],
        }}
        range={{ start: 120, end: 120 }}
        onRangeSelect={() => {}}
      />
    );

    expect(
      screen.getByText(/Traffic target includes autoscaler hysteresis/)
    ).toBeTruthy();
    expect(
      screen.getByText(
        /attempts canceled or disconnected while awaiting admission are excluded/
      )
    ).toBeTruthy();
    expect(screen.getByText('Recorded attempts in range')).toBeTruthy();
    expect(screen.getByTestId('history-series').textContent).toContain(
      'Recorded request attempts|'
    );
    expect(screen.getByTestId('history-series')).toHaveAttribute(
      'data-request-span-gaps',
      'false'
    );
    expect(screen.getByTestId('history-series').textContent).toContain(
      'Traffic target (with hysteresis)|Traffic or reservation target|Ready capacity|Committed / unready capacity|Non-failed tracked capacity'
    );
    expect(
      screen.getByText('Peak traffic target (with hysteresis)')
    ).toBeTruthy();
    expect(screen.getByText('Peak traffic or reservation target')).toBeTruthy();
  });
});
