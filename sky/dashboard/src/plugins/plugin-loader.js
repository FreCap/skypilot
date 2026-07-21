import { apiClient } from '@/data/connectors/client';

const pluginScriptPromises = new Map();

export function resolveScriptUrl(jsPath) {
  if (!jsPath || typeof jsPath !== 'string') {
    return null;
  }
  if (/^https?:\/\//.test(jsPath)) {
    return jsPath;
  }
  if (typeof window === 'undefined') {
    return jsPath;
  }
  try {
    return new URL(jsPath, window.location.origin).toString();
  } catch (error) {
    console.warn(
      '[SkyDashboardPlugin] Failed to resolve plugin script path:',
      jsPath,
      error
    );
    return null;
  }
}

export function loadPluginScript(jsPath, requiresEarlyInit = false) {
  if (typeof window === 'undefined') {
    return null;
  }
  const resolved = resolveScriptUrl(jsPath);
  if (!resolved) {
    return null;
  }
  if (pluginScriptPromises.has(resolved)) {
    return pluginScriptPromises.get(resolved);
  }

  console.log('Loading plugin script:', resolved);
  const promise = new Promise((resolve) => {
    const script = document.createElement('script');
    script.type = 'text/javascript';
    script.async = true;
    script.src = resolved;
    if (requiresEarlyInit) script.dataset.requiresEarlyInit = 'true';
    script.onload = () => resolve();
    script.onerror = (error) => {
      console.warn(
        '[SkyDashboardPlugin] Failed to load plugin script:',
        resolved,
        error
      );
      resolve();
    };
    document.head.appendChild(script);
  });

  pluginScriptPromises.set(resolved, promise);
  return promise;
}

export async function fetchPluginManifest() {
  try {
    const response = await apiClient.get(`/api/plugins`);
    if (!response.ok) {
      console.warn(
        '[SkyDashboardPlugin] Failed to fetch plugin manifest:',
        response.status,
        response.statusText
      );
      return [];
    }
    const payload = await response.json();
    if (!payload || !Array.isArray(payload.plugins)) {
      return [];
    }
    console.log('Plugin manifest:', payload.plugins);
    return payload.plugins;
  } catch (error) {
    console.warn('[SkyDashboardPlugin] Error fetching plugin manifest:', error);
    return [];
  }
}

export function extractJsPath(pluginDescriptor) {
  if (!pluginDescriptor || typeof pluginDescriptor !== 'object') {
    return null;
  }
  if (pluginDescriptor.js_extension_path) {
    console.log(
      'Extracting JS extension path:',
      pluginDescriptor.js_extension_path
    );
    return pluginDescriptor.js_extension_path;
  }
  return null;
}
