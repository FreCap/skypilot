import React, { useState, useEffect, createContext, useContext } from 'react';
import { ArrowUpCircle } from 'lucide-react';
import { NonCapitalizedTooltip } from '@/components/utils';
import { apiClient } from '@/data/connectors/client';

const VersionContext = createContext({
  version: null,
  latestVersion: null,
  commit: null,
  build: null,
  plugins: [],
});

export function VersionProvider({ children }) {
  const [version, setVersion] = useState(null);
  const [latestVersion, setLatestVersion] = useState(null);
  const [commit, setCommit] = useState(null);
  const [build, setBuild] = useState(null);
  const [plugins, setPlugins] = useState([]);

  const getVersionAndPlugins = async () => {
    // Concurrently fetch health and plugins data
    const [healthResponse, pluginsResponse] = await Promise.all([
      apiClient.get('/api/health'),
      apiClient.get('/api/plugins'),
    ]);

    // Process health data
    if (healthResponse.ok) {
      const healthData = await healthResponse.json();
      if (healthData.version) {
        setVersion(healthData.version);
      }
      if (healthData.commit) {
        setCommit(healthData.commit);
      }
      if (healthData.build) {
        setBuild(healthData.build);
      }
      if (healthData.latest_version) {
        setLatestVersion(healthData.latest_version);
      }
    } else {
      console.error(
        `API request /api/health failed with status ${healthResponse.status}`
      );
    }

    // Process plugins data
    if (pluginsResponse.ok) {
      const pluginsData = await pluginsResponse.json();
      if (pluginsData.plugins && pluginsData.plugins.length > 0) {
        setPlugins(pluginsData.plugins);
      }
    } else {
      console.error(
        `API request /api/plugins failed with status ${pluginsResponse.status}`
      );
    }
  };

  useEffect(() => {
    getVersionAndPlugins();
  }, []);

  return (
    <VersionContext.Provider
      value={{ version, latestVersion, commit, build, plugins }}
    >
      {children}
    </VersionContext.Provider>
  );
}

export function useVersionInfo() {
  return useContext(VersionContext);
}

export function VersionTooltip({
  children,
  version,
  latestVersion,
  commit,
  build,
  plugins,
  showUpdateInfo = true,
  showCommit = true,
}) {
  // Create tooltip content
  const tooltipContent = (
    <div className="flex flex-col gap-0.5">
      {showUpdateInfo && latestVersion && (
        <div className="mb-1">
          <div className="font-bold">Update Available</div>
          <div>Current version: {version}</div>
          <div>New version available: {latestVersion}</div>
        </div>
      )}
      {showCommit && commit && (
        <div>
          {plugins.length > 0 ? 'Core commit' : 'Commit'}: {commit}
        </div>
      )}
      {build && <div>Build: {build}</div>}
      {plugins
        .filter((plugin) => !plugin.hidden_from_display)
        .map((plugin, index) => {
          const pluginName = plugin.name || 'Unknown Plugin';
          const parts = [];
          if (plugin.version) parts.push(plugin.version);
          if (showCommit && plugin.commit) parts.push(plugin.commit);
          return parts.length > 0 ? (
            <div key={index}>
              {pluginName}: {parts.join(' - ')}
            </div>
          ) : null;
        })}
      {!commit &&
        plugins.length === 0 &&
        (!latestVersion || !showUpdateInfo) && (
          <div>Version information not available</div>
        )}
    </div>
  );

  return (
    <NonCapitalizedTooltip
      content={tooltipContent}
      className="text-sm text-muted-foreground"
    >
      {children}
    </NonCapitalizedTooltip>
  );
}

export function DeploymentVersionContent({
  version,
  latestVersion,
  commit,
  build,
  plugins,
}) {
  if (!version) return null;
  return (
    <VersionTooltip
      version={version}
      latestVersion={latestVersion}
      commit={commit}
      build={build}
      plugins={plugins}
    >
      <div className="inline-flex items-center justify-center transition-colors duration-150 cursor-help">
        <div className="px-2 py-1 text-xs font-medium text-gray-600 border border-gray-200 rounded-md hover:bg-gray-100 hover:text-blue-600">
          v{version}
          {build && ` · build ${build}`}
        </div>
      </div>
    </VersionTooltip>
  );
}

export function DeploymentVersion() {
  const versionInfo = useVersionInfo();
  return <DeploymentVersionContent {...versionInfo} />;
}

export function NewVersionAvailable() {
  const { latestVersion } = useVersionInfo();

  if (!latestVersion) return null;

  return (
    <div className="flex items-center mr-4 text-amber-600 animate-pulse">
      <ArrowUpCircle className="w-4 h-4 mr-1.5" />
      <span className="text-sm font-medium">
        New version available: {latestVersion}
      </span>
    </div>
  );
}

export function VersionDisplay() {
  const { version, latestVersion, commit, build, plugins } = useVersionInfo();

  if (!version) return null;

  return (
    <VersionTooltip
      version={version}
      latestVersion={latestVersion}
      commit={commit}
      build={build}
      plugins={plugins}
      showUpdateInfo={false}
    >
      <div className="inline-flex items-center justify-center transition-colors duration-150 cursor-help">
        <div className="text-sm text-gray-500 border-b border-dotted border-gray-400 hover:text-blue-600 hover:border-blue-600">
          Version: {version}
          {build && ` · build ${build}`}
        </div>
      </div>
    </VersionTooltip>
  );
}
