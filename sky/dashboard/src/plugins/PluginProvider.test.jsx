import React from 'react';
import { act, render, waitFor } from '@testing-library/react';
import { useRouter } from 'next/router';
import { apiClient } from '@/data/connectors/client';
import { PluginProvider } from './PluginProvider';

jest.mock('next/router', () => ({
  useRouter: jest.fn(),
}));

jest.mock('@/data/connectors/client', () => ({
  apiClient: {
    get: jest.fn(),
  },
}));

const originalPushState = window.history.pushState;
const originalReplaceState = window.history.replaceState;

describe('PluginProvider loader lifecycle', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    jest.spyOn(console, 'warn').mockImplementation(() => {});
    useRouter.mockReturnValue({ push: jest.fn() });
    window.history.pushState = originalPushState;
    window.history.replaceState = originalReplaceState;
    localStorage.clear();
    delete window.SkyDashboardPluginAPI;
    delete window.__pluginRouterRef;
    delete window.__pluginStateRef;
    delete window.__skyDashboardPluginsLoaded;
  });

  afterEach(() => {
    window.history.pushState = originalPushState;
    window.history.replaceState = originalReplaceState;
    delete window.SkyDashboardPluginAPI;
    delete window.__pluginRouterRef;
    delete window.__pluginStateRef;
    delete window.__skyDashboardPluginsLoaded;
    jest.restoreAllMocks();
  });

  it('deduplicates scripts and publishes loaded only after all settle', async () => {
    apiClient.get.mockResolvedValue({
      ok: true,
      json: async () => ({
        plugins: [
          {
            js_extension_path: '/plugins/primary.js',
            requires_early_init: true,
          },
          { js_extension_path: '/plugins/primary.js' },
          { js_extension_path: 'https://plugins.example/secondary.js' },
          {},
        ],
      }),
    });
    const appendedScripts = [];
    jest.spyOn(document.head, 'appendChild').mockImplementation((node) => {
      appendedScripts.push(node);
      return node;
    });
    const loadedListener = jest.fn();
    window.addEventListener('skydashboard:plugins-loaded', loadedListener);

    const { unmount } = render(
      <PluginProvider>
        <div>plugin child</div>
      </PluginProvider>
    );

    await waitFor(() => expect(appendedScripts).toHaveLength(2));
    expect(apiClient.get).toHaveBeenCalledTimes(1);
    expect(apiClient.get).toHaveBeenCalledWith('/api/plugins');
    expect(appendedScripts[0]).toMatchObject({
      async: true,
      src: new URL('/plugins/primary.js', window.location.origin).toString(),
      type: 'text/javascript',
    });
    expect(appendedScripts[0].dataset.requiresEarlyInit).toBe('true');
    expect(appendedScripts[1].src).toBe('https://plugins.example/secondary.js');
    expect(window.__skyDashboardPluginsLoaded).toBeUndefined();
    expect(loadedListener).not.toHaveBeenCalled();

    await act(async () => {
      appendedScripts[0].onload();
      await Promise.resolve();
    });
    expect(window.__skyDashboardPluginsLoaded).toBeUndefined();
    expect(loadedListener).not.toHaveBeenCalled();

    await act(async () => {
      appendedScripts[1].onerror(new Event('error'));
      await Promise.resolve();
    });
    expect(console.warn).toHaveBeenCalledWith(
      '[SkyDashboardPlugin] Failed to load plugin script:',
      'https://plugins.example/secondary.js',
      expect.any(Event)
    );
    await waitFor(() => {
      expect(window.__skyDashboardPluginsLoaded).toBe(true);
      expect(loadedListener).toHaveBeenCalledTimes(1);
    });

    unmount();
    window.removeEventListener('skydashboard:plugins-loaded', loadedListener);
  });

  it('continues startup when the manifest request fails', async () => {
    apiClient.get.mockRejectedValue(new Error('manifest unavailable'));
    const appendSpy = jest.spyOn(document.head, 'appendChild');

    const { unmount } = render(
      <PluginProvider>
        <div>plugin child</div>
      </PluginProvider>
    );

    await waitFor(() => expect(window.__skyDashboardPluginsLoaded).toBe(true));
    expect(appendSpy).not.toHaveBeenCalled();
    expect(window.SkyDashboardPluginAPI).toBeDefined();
    expect(console.warn).toHaveBeenCalledWith(
      '[SkyDashboardPlugin] Error fetching plugin manifest:',
      expect.any(Error)
    );

    unmount();
  });
});
