import React from 'react';
import { act, render, screen } from '@testing-library/react';

import {
  DeploymentVersionContent,
  VersionDetails,
  formatReleaseAge,
  formatReleaseTimestamp,
} from './version-display';

afterEach(() => {
  jest.restoreAllMocks();
});

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
}

test('shows compact deployment age', () => {
  jest.spyOn(Date, 'now').mockReturnValue(Date.parse('2026-08-01T17:30:00Z'));

  render(
    <DeploymentVersionContent
      version="1.1.27"
      latestVersion={null}
      commit="abcdef123456"
      commitTimestamp="2026-08-01T15:00:00Z"
      build="5921"
      deploymentTimestamp="2026-08-01T17:00:00Z"
      plugins={[]}
    />
  );

  const release = screen.getByText('v1.1.27 · deployed 30m ago');
  expect(release).toBeVisible();
});

test('shows exact release details', () => {
  render(
    <VersionDetails
      version="1.1.27"
      latestVersion={null}
      commit="abcdef123456"
      commitTimestamp="2026-08-01T15:00:00Z"
      build="5921"
      deploymentTimestamp="2026-08-01T17:00:00Z"
      plugins={[]}
    />
  );

  expect(screen.getByText('Build: 5921')).toBeVisible();
  expect(screen.getByText(/Checked in:/)).toBeVisible();
  expect(screen.getByText(/Deployed \(API server started\):/)).toBeVisible();
  expect(
    document.querySelector('time[datetime="2026-08-01T15:00:00Z"]')
  ).toBeInTheDocument();
  expect(
    document.querySelector('time[datetime="2026-08-01T17:00:00Z"]')
  ).toBeInTheDocument();
});

test('keeps the version and build fallback for an older server', () => {
  render(
    <DeploymentVersionContent
      version="1.1.27"
      latestVersion={null}
      commit="abcdef123456"
      build="5921"
      plugins={[]}
    />
  );

  expect(screen.getByText('v1.1.27 · build 5921')).toBeVisible();
});

test('formats deployment ages without inventing malformed timestamps', () => {
  const now = Date.parse('2026-08-01T17:30:00Z');
  expect(formatReleaseAge('2026-08-01T17:29:45Z', now)).toBe('just now');
  expect(formatReleaseAge('2026-08-01T15:15:00Z', now)).toBe('2h ago');
  expect(formatReleaseAge('2026-07-30T17:30:00Z', now)).toBe('2d ago');
  expect(formatReleaseAge('not-a-time', now)).toBeNull();
  expect(formatReleaseAge('2026-02-30T12:00:00Z', now)).toBeNull();
  expect(formatReleaseAge('2026-08-01T12:00:00', now)).toBeNull();
  expect(formatReleaseAge('123', now)).toBeNull();
  expect(formatReleaseTimestamp('not-a-time')).toBeNull();
});

test('pauses hidden deployment-age refreshes and restores one visible cadence', () => {
  jest.useFakeTimers();
  jest.setSystemTime(new Date('2026-08-01T17:58:00Z'));
  const visibilityDescriptor = Object.getOwnPropertyDescriptor(
    window.document,
    'visibilityState'
  );
  setDocumentVisibility('visible');

  try {
    render(
      <DeploymentVersionContent
        version="1.1.27"
        latestVersion={null}
        commit="abcdef123456"
        commitTimestamp="2026-08-01T15:00:00Z"
        build="5921"
        deploymentTimestamp="2026-08-01T17:00:00Z"
        plugins={[]}
      />
    );

    expect(screen.getByText('v1.1.27 · deployed 58m ago')).toBeVisible();
    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      setDocumentVisibility('hidden');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(jest.getTimerCount()).toBe(0);

    act(() => {
      jest.advanceTimersByTime(90 * 1000);
    });
    expect(screen.getByText('v1.1.27 · deployed 58m ago')).toBeVisible();

    act(() => {
      setDocumentVisibility('visible');
      window.document.dispatchEvent(new Event('visibilitychange'));
    });
    expect(screen.getByText('v1.1.27 · deployed 59m ago')).toBeVisible();
    expect(jest.getTimerCount()).toBe(1);

    act(() => {
      jest.advanceTimersByTime(89 * 1000);
    });
    expect(screen.getByText('v1.1.27 · deployed 59m ago')).toBeVisible();

    act(() => {
      jest.advanceTimersByTime(1000);
    });
    expect(screen.getByText('v1.1.27 · deployed 1h ago')).toBeVisible();
  } finally {
    if (visibilityDescriptor) {
      Object.defineProperty(
        window.document,
        'visibilityState',
        visibilityDescriptor
      );
    } else {
      delete window.document.visibilityState;
    }
    jest.useRealTimers();
  }
});
