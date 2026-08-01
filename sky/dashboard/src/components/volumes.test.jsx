import React from 'react';
import PropTypes from 'prop-types';
import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

jest.mock('next/router', () => ({
  useRouter: () => ({
    isReady: true,
    pathname: '/volumes',
    query: {},
    replace: jest.fn(),
  }),
}));

jest.mock('@/hooks/useMobile', () => ({
  useMobile: () => false,
}));

const mockUsePluginComponents = jest.fn();

jest.mock('@/plugins/PluginProvider', () => ({
  usePluginComponents: (...args) => mockUsePluginComponents(...args),
  useTableColumns: () => [],
}));

jest.mock('@/lib/analytics', () => ({
  trackVolumeAction: jest.fn(),
}));

jest.mock('@/lib/cache-preloader', () => ({
  __esModule: true,
  default: {
    preloadForPage: jest.fn(),
  },
}));

jest.mock('@/lib/cache', () => ({
  __esModule: true,
  default: {
    get: jest.fn(),
    invalidate: jest.fn(),
    setPreloader: jest.fn(),
  },
}));

import { Volumes, VolumesTable } from '@/components/volumes';
import { getVolumes } from '@/data/connectors/volumes';
import dashboardCache from '@/lib/cache';
import cachePreloader from '@/lib/cache-preloader';
import { trackVolumeAction } from '@/lib/analytics';

const actualCachePreloader = jest.requireActual(
  '@/lib/cache-preloader'
).default;

function PluginMutationAction({ onVolumeChange }) {
  return <button onClick={onVolumeChange}>Plugin mutation</button>;
}

PluginMutationAction.propTypes = {
  onVolumeChange: PropTypes.func.isRequired,
};

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((promiseResolve, promiseReject) => {
    resolve = promiseResolve;
    reject = promiseReject;
  });
  return { promise, resolve, reject };
}

function volume(name) {
  return {
    name,
    status: 'READY',
    size: 100,
    type: 'k8s-pvc',
  };
}

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
}

async function flushPromises(rounds = 6) {
  for (let i = 0; i < rounds; i += 1) {
    await act(async () => {
      await Promise.resolve();
    });
  }
}

describe('Volumes request ownership', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.useFakeTimers();
    cachePreloader.preloadForPage.mockReset().mockResolvedValue();
    dashboardCache.get.mockReset();
    dashboardCache.invalidate.mockReset();
    mockUsePluginComponents.mockReset().mockReturnValue([]);
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('keeps the latest interval request in control when an older success finishes', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise);

    render(<Volumes />);
    await flushPromises();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(30000);
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);

    await act(async () => {
      oldRequest.resolve([volume('stale-volume')]);
      await oldRequest.promise;
    });

    expect(screen.queryByText('stale-volume')).not.toBeInTheDocument();
    expect(screen.getAllByText('Loading...').length).toBeGreaterThan(0);

    await act(async () => {
      currentRequest.resolve([volume('current-volume')]);
      await currentRequest.promise;
    });

    expect(screen.getByText('current-volume')).toBeInTheDocument();
    expect(screen.queryByText('Loading...')).not.toBeInTheDocument();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('refreshes immediately on visibility restore and skips the adjacent timer boundary', async () => {
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    dashboardCache.get.mockResolvedValue([volume('current-volume')]);
    setDocumentVisibility('hidden');
    const { unmount } = render(<Volumes />);
    let mounted = true;

    try {
      await screen.findByText('current-volume');
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      await act(async () => {
        jest.advanceTimersByTime(30000 - 1);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(1);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(1);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(30000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);

      unmount();
      mounted = false;
      window.document.dispatchEvent(new Event('visibilitychange'));
      await act(async () => {
        jest.advanceTimersByTime(30000);
        await Promise.resolve();
      });
      expect(dashboardCache.get).toHaveBeenCalledTimes(3);
    } finally {
      if (mounted) {
        unmount();
      }
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('lets a manual refresh own the only foreground fetch', async () => {
    dashboardCache.get.mockResolvedValue([volume('current-volume')]);

    render(<Volumes />);
    await screen.findByText('current-volume');
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));

    await waitFor(() => {
      expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    });
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(
      2,
      'volumes',
      { force: true }
    );
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();
  });

  it('invalidates and fetches the volume preload key exactly once when forced', async () => {
    dashboardCache.get.mockResolvedValue([]);

    await actualCachePreloader.preloadForPage('volumes', {
      force: true,
      backgroundPreload: false,
    });

    expect(dashboardCache.invalidate).toHaveBeenCalledTimes(1);
    expect(dashboardCache.invalidate).toHaveBeenCalledWith(getVolumes, []);
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    expect(dashboardCache.get).toHaveBeenCalledWith(getVolumes, []);
  });

  it('coalesces overlapping manual refreshes into one forced preload', async () => {
    const initialPreload = deferred();
    const refreshPreload = deferred();
    dashboardCache.get.mockResolvedValue([volume('current-volume')]);
    cachePreloader.preloadForPage
      .mockReturnValueOnce(initialPreload.promise)
      .mockReturnValueOnce(refreshPreload.promise)
      .mockResolvedValueOnce(undefined);
    render(<Volumes />);

    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    fireEvent.click(refreshButton);
    fireEvent.click(refreshButton);

    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(2);
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(
      2,
      'volumes',
      { force: true }
    );
    expect(trackVolumeAction).toHaveBeenCalledTimes(2);
    expect(trackVolumeAction).toHaveBeenCalledWith('refresh');
    expect(dashboardCache.invalidate).not.toHaveBeenCalled();

    await act(async () => {
      initialPreload.resolve();
      await initialPreload.promise;
    });
    expect(dashboardCache.get).not.toHaveBeenCalled();

    await act(async () => {
      refreshPreload.resolve();
      await refreshPreload.promise;
      await Promise.resolve();
    });
    await screen.findByText('current-volume');
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);

    fireEvent.click(refreshButton);
    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(3);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(2));
  });

  it('releases manual refresh ownership after preload failure', async () => {
    const failedRefresh = deferred();
    dashboardCache.get.mockResolvedValue([volume('current-volume')]);
    cachePreloader.preloadForPage
      .mockResolvedValueOnce(undefined)
      .mockReturnValueOnce(failedRefresh.promise)
      .mockResolvedValueOnce(undefined);
    const consoleError = jest
      .spyOn(console, 'error')
      .mockImplementation(() => undefined);
    render(<Volumes />);
    await screen.findByText('current-volume');

    const refreshButton = screen.getByRole('button', { name: 'Refresh' });
    fireEvent.click(refreshButton);
    await act(async () => {
      failedRefresh.reject(new Error('preload unavailable'));
      await failedRefresh.promise.catch(() => {});
      await Promise.resolve();
    });
    await waitFor(() =>
      expect(consoleError).toHaveBeenCalledWith(
        'Error preloading volumes data:',
        expect.objectContaining({ message: 'preload unavailable' })
      )
    );

    fireEvent.click(refreshButton);
    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(3);
    await waitFor(() => expect(dashboardCache.get).toHaveBeenCalledTimes(3));
    consoleError.mockRestore();
  });

  it('lets plugin mutation supersede a pending manual refresh', async () => {
    const manualRefresh = deferred();
    const mutationRefresh = deferred();
    mockUsePluginComponents.mockImplementation((slot) =>
      slot === 'volumes.header-actions'
        ? [{ id: 'mutation', component: PluginMutationAction }]
        : []
    );
    dashboardCache.get.mockResolvedValue([volume('current-volume')]);
    cachePreloader.preloadForPage
      .mockResolvedValueOnce(undefined)
      .mockReturnValueOnce(manualRefresh.promise)
      .mockReturnValueOnce(mutationRefresh.promise);
    render(<Volumes />);
    await screen.findByText('current-volume');

    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    fireEvent.click(screen.getByRole('button', { name: 'Plugin mutation' }));
    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(3);
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(
      3,
      'volumes',
      { force: true }
    );

    await act(async () => {
      manualRefresh.resolve();
      await manualRefresh.promise;
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(cachePreloader.preloadForPage).toHaveBeenCalledTimes(3);

    await act(async () => {
      mutationRefresh.resolve();
      await mutationRefresh.promise;
    });
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
  });

  it('does not let an initial preload republish after a manual refresh supersedes it', async () => {
    const initialPreload = deferred();
    const refreshPreload = deferred();
    dashboardCache.get.mockResolvedValue([volume('current-volume')]);
    cachePreloader.preloadForPage
      .mockReturnValueOnce(initialPreload.promise)
      .mockReturnValueOnce(refreshPreload.promise);

    render(<Volumes />);

    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    expect(cachePreloader.preloadForPage).toHaveBeenNthCalledWith(
      2,
      'volumes',
      { force: true }
    );

    await act(async () => {
      initialPreload.resolve();
      await initialPreload.promise;
    });
    expect(dashboardCache.get).not.toHaveBeenCalled();

    await act(async () => {
      refreshPreload.resolve();
      await refreshPreload.promise;
    });
    await screen.findByText('current-volume');
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });

  it('does not continue a manual refresh that finishes after unmount', async () => {
    const refreshPreload = deferred();
    dashboardCache.get.mockResolvedValue([volume('current-volume')]);
    cachePreloader.preloadForPage
      .mockResolvedValueOnce(undefined)
      .mockReturnValueOnce(refreshPreload.promise);
    const { unmount } = render(<Volumes />);
    await screen.findByText('current-volume');
    fireEvent.click(screen.getByRole('button', { name: 'Refresh' }));
    unmount();
    await act(async () => {
      refreshPreload.resolve();
      await refreshPreload.promise;
    });

    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });

  it('does not let an older failure erase a newer interval result', async () => {
    const oldRequest = deferred();
    const currentRequest = deferred();
    const consoleError = jest.spyOn(console, 'error').mockImplementation();
    dashboardCache.get
      .mockReturnValueOnce(oldRequest.promise)
      .mockReturnValueOnce(currentRequest.promise);

    render(<Volumes />);
    await flushPromises();

    await act(async () => {
      jest.advanceTimersByTime(30000);
      currentRequest.resolve([volume('current-volume')]);
      await currentRequest.promise;
    });
    expect(screen.getByText('current-volume')).toBeInTheDocument();

    await act(async () => {
      oldRequest.reject(new Error('stale failure'));
      await oldRequest.promise.catch(() => {});
    });

    expect(screen.getByText('current-volume')).toBeInTheDocument();
    expect(consoleError).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(2);
    consoleError.mockRestore();
  });

  it('invalidates request ownership when the table unmounts', async () => {
    const pendingRequest = deferred();
    const setLoading = jest.fn();
    const onDataChange = jest.fn();
    dashboardCache.get.mockReturnValueOnce(pendingRequest.promise);

    const { unmount } = render(
      <VolumesTable
        refreshInterval={30000}
        setLoading={setLoading}
        onDeleteVolume={jest.fn()}
        onDataChange={onDataChange}
        preloadingComplete={true}
      />
    );
    expect(setLoading).toHaveBeenCalledWith(true);
    setLoading.mockClear();

    unmount();
    await act(async () => {
      pendingRequest.resolve([volume('late-volume')]);
      await pendingRequest.promise;
    });

    expect(onDataChange).not.toHaveBeenCalled();
    expect(setLoading).not.toHaveBeenCalled();
    expect(dashboardCache.get).toHaveBeenCalledTimes(1);
  });
});
