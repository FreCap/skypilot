import React, { useState, useEffect, createContext, useContext } from 'react';
import { ArrowUpCircle } from 'lucide-react';
import { NonCapitalizedTooltip } from '@/components/utils';
import { apiClient } from '@/data/connectors/client';

const VersionContext = createContext({
  version: null,
  latestVersion: null,
  commit: null,
  commitTimestamp: null,
  build: null,
  deploymentTimestamp: null,
  plugins: [],
});

function parseReleaseTimestamp(timestamp) {
  if (typeof timestamp !== 'string') return null;
  const match = timestamp.match(
    /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d+)?(?:Z|([+-])(\d{2}):(\d{2}))$/
  );
  if (!match) return null;

  const [, yearText, monthText, dayText, hourText, minuteText, secondText] =
    match;
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  const offsetHour = match[8] === undefined ? 0 : Number(match[8]);
  const offsetMinute = match[9] === undefined ? 0 : Number(match[9]);
  const daysInMonth = new Date(Date.UTC(year, month, 0)).getUTCDate();
  if (
    month < 1 ||
    month > 12 ||
    day < 1 ||
    day > daysInMonth ||
    hour > 23 ||
    minute > 59 ||
    second > 59 ||
    offsetHour > 23 ||
    offsetMinute > 59
  ) {
    return null;
  }

  const milliseconds = Date.parse(timestamp);
  return Number.isFinite(milliseconds) ? milliseconds : null;
}

export function formatReleaseAge(timestamp, now = Date.now()) {
  const milliseconds = parseReleaseTimestamp(timestamp);
  if (milliseconds === null || !Number.isFinite(now)) return null;

  const elapsedSeconds = Math.max(0, Math.floor((now - milliseconds) / 1000));
  if (elapsedSeconds < 60) return 'just now';
  if (elapsedSeconds < 60 * 60) {
    return `${Math.floor(elapsedSeconds / 60)}m ago`;
  }
  if (elapsedSeconds < 24 * 60 * 60) {
    return `${Math.floor(elapsedSeconds / (60 * 60))}h ago`;
  }
  return `${Math.floor(elapsedSeconds / (24 * 60 * 60))}d ago`;
}

export function formatReleaseTimestamp(timestamp) {
  const milliseconds = parseReleaseTimestamp(timestamp);
  if (milliseconds === null) return null;
  return new Date(milliseconds).toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    timeZoneName: 'short',
  });
}

function useDeploymentAge(timestamp) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    if (parseReleaseTimestamp(timestamp) === null) return undefined;
    const interval = setInterval(() => setNow(Date.now()), 60 * 1000);
    return () => clearInterval(interval);
  }, [timestamp]);

  return formatReleaseAge(timestamp, now);
}

export function VersionProvider({ children }) {
  const [version, setVersion] = useState(null);
  const [latestVersion, setLatestVersion] = useState(null);
  const [commit, setCommit] = useState(null);
  const [commitTimestamp, setCommitTimestamp] = useState(null);
  const [build, setBuild] = useState(null);
  const [deploymentTimestamp, setDeploymentTimestamp] = useState(null);
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
      if (healthData.commit_timestamp) {
        setCommitTimestamp(healthData.commit_timestamp);
      }
      if (healthData.build) {
        setBuild(healthData.build);
      }
      if (healthData.deployment_timestamp) {
        setDeploymentTimestamp(healthData.deployment_timestamp);
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
      value={{
        version,
        latestVersion,
        commit,
        commitTimestamp,
        build,
        deploymentTimestamp,
        plugins,
      }}
    >
      {children}
    </VersionContext.Provider>
  );
}

export function useVersionInfo() {
  return useContext(VersionContext);
}

export function VersionDetails({
  version,
  latestVersion,
  commit,
  commitTimestamp,
  build,
  deploymentTimestamp,
  plugins = [],
  showUpdateInfo = true,
  showCommit = true,
}) {
  const checkedIn = formatReleaseTimestamp(commitTimestamp);
  const deployed = formatReleaseTimestamp(deploymentTimestamp);

  return (
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
      {checkedIn && (
        <div>
          Checked in: <time dateTime={commitTimestamp}>{checkedIn}</time>
        </div>
      )}
      {deployed && (
        <div>
          Deployed (API server started):{' '}
          <time dateTime={deploymentTimestamp}>{deployed}</time>
        </div>
      )}
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
        !checkedIn &&
        !deployed &&
        plugins.length === 0 &&
        (!latestVersion || !showUpdateInfo) && (
          <div>Version information not available</div>
        )}
    </div>
  );
}

export function VersionTooltip({ children, ...versionDetailsProps }) {
  const tooltipContent = <VersionDetails {...versionDetailsProps} />;

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
  commitTimestamp,
  build,
  deploymentTimestamp,
  plugins,
}) {
  const deploymentAge = useDeploymentAge(deploymentTimestamp);
  if (!version) return null;
  return (
    <VersionTooltip
      version={version}
      latestVersion={latestVersion}
      commit={commit}
      commitTimestamp={commitTimestamp}
      build={build}
      deploymentTimestamp={deploymentTimestamp}
      plugins={plugins}
    >
      <div className="inline-flex items-center justify-center transition-colors duration-150 cursor-help">
        <div className="px-2 py-1 text-xs font-medium text-gray-600 border border-gray-200 rounded-md hover:bg-gray-100 hover:text-blue-600">
          v{version}
          {deploymentAge
            ? ` · deployed ${deploymentAge}`
            : build && ` · build ${build}`}
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
  const {
    version,
    latestVersion,
    commit,
    commitTimestamp,
    build,
    deploymentTimestamp,
    plugins,
  } = useVersionInfo();

  if (!version) return null;

  return (
    <VersionTooltip
      version={version}
      latestVersion={latestVersion}
      commit={commit}
      commitTimestamp={commitTimestamp}
      build={build}
      deploymentTimestamp={deploymentTimestamp}
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
