import React, { useMemo, useState } from 'react';
import Link from 'next/link';
import {
  CheckIcon,
  ChevronDownIcon,
  ChevronRightIcon,
  CopyIcon,
} from 'lucide-react';
import PropTypes from 'prop-types';

import { BatchBadge } from '@/components/elements/BatchBadge';
import { StatusBadge } from '@/components/elements/StatusBadge';
import { UserDisplay } from '@/components/elements/UserDisplay';
import {
  CustomTooltip as Tooltip,
  formatDuration,
  formatFullTimestamp,
  NonCapitalizedTooltip,
  renderPoolLink,
} from '@/components/utils';
import { YamlCodeBlock } from '@/components/ui/yaml-code-block';
import { computeJobGroupStatus } from '@/data/connectors/jobs';
import { formatJobYaml } from '@/lib/yamlUtils';
import { PluginSlot } from '@/plugins/PluginSlot';
import { normalizeUrl } from '@/utils/externalLinks';

function JobInfoSection({
  jobData,
  allTasks = [],
  poolsData,
  links,
  logExtractedLinks = {},
}) {
  const [isYamlExpanded, setIsYamlExpanded] = useState(false);
  const [expandedYamlDocs, setExpandedYamlDocs] = useState({});
  const [showFullYaml, setShowFullYaml] = useState(false);
  const [isCopied, setIsCopied] = useState(false);
  const [isCommandCopied, setIsCommandCopied] = useState(false);

  const computedStatus = useMemo(() => {
    if (allTasks.length > 1) {
      return computeJobGroupStatus(allTasks);
    }
    return jobData.status;
  }, [allTasks, jobData.status]);

  const toggleYamlExpanded = () => {
    setIsYamlExpanded(!isYamlExpanded);
  };

  const toggleYamlDocExpanded = (index) => {
    setExpandedYamlDocs((prev) => ({
      ...prev,
      [index]: !prev[index],
    }));
  };

  const copyYamlToClipboard = async () => {
    try {
      const yamlDocs = formatJobYaml(jobData.dag_yaml);
      const hasJobGroupConfig = jobData.name || jobData.execution;
      const jobGroupHeader = hasJobGroupConfig
        ? [
            jobData.name ? `name: ${jobData.name}` : null,
            jobData.execution ? `execution: ${jobData.execution}` : null,
          ]
            .filter(Boolean)
            .join('\n') + '\n---\n'
        : '';

      let textToCopy = '';
      if (yamlDocs.length === 1) {
        textToCopy = jobGroupHeader + yamlDocs[0].content;
      } else if (yamlDocs.length > 1) {
        textToCopy =
          jobGroupHeader + yamlDocs.map((doc) => doc.content).join('\n---\n');
      } else {
        textToCopy = jobGroupHeader + jobData.dag_yaml;
      }

      await navigator.clipboard.writeText(textToCopy);
      setIsCopied(true);
      setTimeout(() => setIsCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy YAML to clipboard:', err);
    }
  };

  const copyCommandToClipboard = async () => {
    try {
      await navigator.clipboard.writeText(jobData.entrypoint);
      setIsCommandCopied(true);
      setTimeout(() => setIsCommandCopied(false), 2000);
    } catch (err) {
      console.error('Failed to copy command to clipboard:', err);
    }
  };

  const combinedLinks = useMemo(() => {
    const combined = { ...(links || {}) };
    for (const [key, value] of Object.entries(logExtractedLinks)) {
      if (!combined[key]) {
        combined[key] = value;
      }
    }
    return combined;
  }, [links, logExtractedLinks]);

  return (
    <div className="grid grid-cols-2 gap-6">
      <div>
        <div className="text-gray-600 font-medium text-base">Job ID (Name)</div>
        <div className="text-base mt-1 flex items-center gap-2">
          <span>
            {jobData.id} {jobData.name ? `(${jobData.name})` : ''}
          </span>
          {/* Badge for batch job */}
          {(jobData.is_batch === true ||
            jobData.batch_total_batches != null) && <BatchBadge />}
          {/* Badge for job group */}
          {jobData.is_job_group && (
            <span className="px-2 py-0.5 rounded text-xs font-medium bg-gray-200 text-gray-700">
              JobGroup
            </span>
          )}
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">Status</div>
        <div className="text-base mt-1">
          {(() => {
            const isBatchRunning =
              jobData.status === 'RUNNING' &&
              jobData.batch_total_batches != null;
            if (isBatchRunning) {
              const completed = jobData.batch_completed_batches || 0;
              const total = jobData.batch_total_batches;
              const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
              const barColor =
                completed >= total ? 'bg-green-500' : 'bg-blue-500';
              return (
                <div className="flex items-center gap-3">
                  <div className="w-32 bg-gray-200 rounded-full h-2.5">
                    <div
                      className={`${barColor} h-2.5 rounded-full transition-all`}
                      style={{ width: `${pct}%` }}
                    />
                  </div>
                  <span className="text-sm text-gray-600">
                    {completed}/{total} batches ({pct}%)
                  </span>
                </div>
              );
            }
            return (
              <PluginSlot
                name="jobs.detail.status.badge"
                context={jobData}
                fallback={<StatusBadge status={computedStatus} />}
              />
            );
          })()}
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">User</div>
        <div className="text-base mt-1">
          <UserDisplay username={jobData.user} userHash={jobData.user_hash} />
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">Workspace</div>
        <div className="text-base mt-1">
          <Link
            href="/workspaces"
            className="text-gray-700 hover:text-blue-600 hover:underline"
          >
            {jobData.workspace || 'default'}
          </Link>
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">Submitted</div>
        <div className="text-base mt-1">
          {jobData.submitted_at
            ? formatFullTimestamp(jobData.submitted_at)
            : 'N/A'}
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">Duration</div>
        <div className="text-base mt-1">
          {formatDuration(jobData.job_duration)}
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">
          Requested Resources
        </div>
        <div className="text-base mt-1">
          {allTasks.length > 1 ? (
            <NonCapitalizedTooltip
              content={`Aggregated from ${allTasks.length} tasks:\n${allTasks
                .map(
                  (task, index) =>
                    `Task ${index}${task.task ? ` (${task.task})` : ''}: ${task.requested_resources || task.resources_str || 'N/A'}`
                )
                .join('\n')}`}
              className="text-sm text-muted-foreground"
            >
              <span className="cursor-help border-b border-dotted border-gray-400">
                {(() => {
                  const resourcesList = allTasks
                    .map((t) => t.requested_resources || t.resources_str)
                    .filter(Boolean);
                  const uniqueResources = [...new Set(resourcesList)];
                  return uniqueResources.length === 1
                    ? `${uniqueResources[0]} (x${allTasks.length} tasks)`
                    : `${resourcesList[0]} (+${allTasks.length - 1} more)`;
                })()}
              </span>
            </NonCapitalizedTooltip>
          ) : (
            jobData.requested_resources || 'N/A'
          )}
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">Infra</div>
        <div className="text-base mt-1">
          {jobData.infra ? (
            <NonCapitalizedTooltip
              content={jobData.full_infra || jobData.infra}
              className="text-sm text-muted-foreground"
            >
              <span>
                <Link href="/infra" className="text-blue-600 hover:underline">
                  {jobData.cloud || jobData.infra.split('(')[0].trim()}
                </Link>
                {jobData.infra.includes('(') && (
                  <span>
                    {' ' + jobData.infra.substring(jobData.infra.indexOf('('))}
                  </span>
                )}
              </span>
            </NonCapitalizedTooltip>
          ) : (
            '-'
          )}
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">Resources</div>
        <div className="text-base mt-1">
          {jobData.resources_str_full || jobData.resources_str || '-'}
        </div>
      </div>
      <div>
        <div className="text-gray-600 font-medium text-base">Git Commit</div>
        <div className="text-base mt-1 flex items-center">
          {jobData.git_commit && jobData.git_commit !== '-' ? (
            <span className="flex items-center mr-2">
              {jobData.git_commit}
              <Tooltip
                content={isCopied ? 'Copied!' : 'Copy commit'}
                className="text-muted-foreground"
              >
                <button
                  onClick={async () => {
                    await navigator.clipboard.writeText(jobData.git_commit);
                    setIsCopied(true);
                    setTimeout(() => setIsCopied(false), 2000);
                  }}
                  className="flex items-center text-gray-500 hover:text-gray-700 transition-colors duration-200 p-1 ml-2"
                >
                  {isCopied ? (
                    <CheckIcon className="w-4 h-4 text-green-600" />
                  ) : (
                    <CopyIcon className="w-4 h-4" />
                  )}
                </button>
              </Tooltip>
            </span>
          ) : (
            <span className="text-gray-400">-</span>
          )}
        </div>
      </div>

      <div>
        <div className="text-gray-600 font-medium text-base">Pool</div>
        <div className="text-base mt-1">
          {renderPoolLink(jobData.pool, jobData.pool_hash, poolsData)}
        </div>
      </div>

      {/* Batch Progress section - only for batch jobs */}
      {jobData.batch_total_batches != null && (
        <div>
          <div className="text-gray-600 font-medium text-base">
            Batch Progress
          </div>
          <div className="text-base mt-1">
            {(() => {
              const completed = jobData.batch_completed_batches || 0;
              const total = jobData.batch_total_batches;
              const pct = total > 0 ? Math.round((completed / total) * 100) : 0;
              const barColor =
                completed >= total ? 'bg-green-500' : 'bg-blue-500';
              const failed = total - completed;
              const isTerminal = [
                'SUCCEEDED',
                'FAILED',
                'CANCELLED',
                'FAILED_SETUP',
                'FAILED_PRECHECKS',
                'FAILED_NO_RESOURCE',
                'FAILED_CONTROLLER',
              ].includes(jobData.status);
              return (
                <div className="space-y-1.5">
                  <div className="flex items-center gap-3">
                    <div className="w-40 bg-gray-200 rounded-full h-2.5">
                      <div
                        className={`${barColor} h-2.5 rounded-full transition-all`}
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                    <span className="text-sm text-gray-600">
                      {completed}/{total} ({pct}%)
                    </span>
                  </div>
                  {isTerminal && failed > 0 && completed < total && (
                    <div className="text-xs text-red-600">
                      {total - completed} batches incomplete
                    </div>
                  )}
                </div>
              );
            })()}
          </div>
        </div>
      )}

      {/* External Links section - full width row */}
      <div className="col-span-2">
        <div className="text-gray-600 font-medium text-base">
          External Links
        </div>
        <div className="text-base mt-1">
          {combinedLinks && Object.keys(combinedLinks).length > 0 ? (
            <div className="flex flex-wrap gap-4">
              {Object.entries(combinedLinks).map(([label, url]) => {
                const normalizedUrl = normalizeUrl(url);
                return (
                  <a
                    key={label}
                    href={normalizedUrl}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-blue-600 hover:text-blue-800 hover:underline"
                  >
                    {label}
                  </a>
                );
              })}
            </div>
          ) : (
            <span className="text-gray-400">-</span>
          )}
        </div>
      </div>

      {/* Queue Details section - right column */}
      {jobData.details && (
        <PluginSlot
          name="jobs.detail.queue_details"
          context={{
            details: jobData.details,
            queueName: jobData.kueue_queue_name,
            infra: jobData.full_infra,
            jobData: jobData,
            title: 'Queue Details',
          }}
        />
      )}

      {/* Entrypoint section - full width row */}
      {(jobData.entrypoint || jobData.dag_yaml) && (
        <div className="col-span-2">
          <div className="flex items-center">
            <div className="text-gray-600 font-medium text-base">
              Entrypoint
            </div>
            {jobData.entrypoint && (
              <Tooltip
                content={isCommandCopied ? 'Copied!' : 'Copy command'}
                className="text-muted-foreground"
              >
                <button
                  onClick={copyCommandToClipboard}
                  className="flex items-center text-gray-500 hover:text-gray-700 transition-colors duration-200 p-1 ml-2"
                >
                  {isCommandCopied ? (
                    <CheckIcon className="w-4 h-4 text-green-600" />
                  ) : (
                    <CopyIcon className="w-4 h-4" />
                  )}
                </button>
              </Tooltip>
            )}
          </div>

          <div className="space-y-4 mt-3">
            {/* Launch Command */}
            {jobData.entrypoint && (
              <div>
                <div className="bg-gray-50 border border-gray-200 rounded-md p-3">
                  <code className="text-sm text-gray-800 font-mono break-all">
                    {jobData.entrypoint}
                  </code>
                </div>
              </div>
            )}

            {/* Job YAML - Collapsible */}
            {jobData.dag_yaml && jobData.dag_yaml !== '{}' && (
              <div>
                <div className="flex items-center mb-2">
                  <button
                    onClick={toggleYamlExpanded}
                    className="flex items-center text-left focus:outline-none text-gray-700 hover:text-gray-900 transition-colors duration-200"
                  >
                    {isYamlExpanded ? (
                      <ChevronDownIcon className="w-4 h-4 mr-1" />
                    ) : (
                      <ChevronRightIcon className="w-4 h-4 mr-1" />
                    )}
                    <span className="text-base">Show SkyPilot YAML</span>
                  </button>

                  <Tooltip
                    content={isCopied ? 'Copied!' : 'Copy YAML'}
                    className="text-muted-foreground"
                  >
                    <button
                      onClick={copyYamlToClipboard}
                      className="flex items-center text-gray-500 hover:text-gray-700 transition-colors duration-200 p-1 ml-2"
                    >
                      {isCopied ? (
                        <CheckIcon className="w-4 h-4 text-green-600" />
                      ) : (
                        <CopyIcon className="w-4 h-4" />
                      )}
                    </button>
                  </Tooltip>
                </div>

                {isYamlExpanded && (
                  <div>
                    {(() => {
                      const yamlDocs = formatJobYaml(jobData.dag_yaml);
                      // Build JobGroup header with name and execution
                      const hasJobGroupConfig =
                        jobData.name || jobData.execution;
                      const jobGroupHeader = hasJobGroupConfig
                        ? [
                            jobData.name ? `name: ${jobData.name}` : null,
                            jobData.execution
                              ? `execution: ${jobData.execution}`
                              : null,
                          ]
                            .filter(Boolean)
                            .join('\n') + '\n---\n'
                        : '';

                      if (yamlDocs.length === 0) {
                        return (
                          <div className="text-gray-500">No YAML available</div>
                        );
                      } else if (yamlDocs.length === 1) {
                        // Single document - show directly
                        return (
                          <YamlCodeBlock
                            value={jobGroupHeader + yamlDocs[0].content}
                            readOnly
                          />
                        );
                      } else {
                        // Multiple documents - show toggle and content
                        return (
                          <div className="space-y-4">
                            {/* Toggle for Full YAML vs Per-Job */}
                            <div className="flex items-center space-x-4 pb-2 border-b border-gray-200">
                              <button
                                onClick={() => setShowFullYaml(false)}
                                className={`text-sm px-2 py-1 rounded ${!showFullYaml ? 'bg-blue-100 text-blue-700' : 'text-gray-600 hover:text-gray-800'}`}
                              >
                                By Job
                              </button>
                              <button
                                onClick={() => setShowFullYaml(true)}
                                className={`text-sm px-2 py-1 rounded ${showFullYaml ? 'bg-blue-100 text-blue-700' : 'text-gray-600 hover:text-gray-800'}`}
                              >
                                Full YAML
                              </button>
                            </div>

                            {showFullYaml ? (
                              // Show full YAML with JobGroup header
                              <YamlCodeBlock
                                value={
                                  jobGroupHeader +
                                  yamlDocs
                                    .map((doc) => doc.content)
                                    .join('\n---\n')
                                }
                                readOnly
                              />
                            ) : (
                              // Show per-job YAMLs
                              yamlDocs.map((doc, index) => (
                                <div
                                  key={index}
                                  className="border-b border-gray-200 pb-4 last:border-b-0"
                                >
                                  <button
                                    onClick={() => toggleYamlDocExpanded(index)}
                                    className="flex items-center justify-between w-full text-left focus:outline-none"
                                  >
                                    <div className="flex items-center">
                                      {expandedYamlDocs[index] ? (
                                        <ChevronDownIcon className="w-4 h-4 mr-2" />
                                      ) : (
                                        <ChevronRightIcon className="w-4 h-4 mr-2" />
                                      )}
                                      <span className="text-sm font-medium text-gray-700">
                                        Job {index + 1}: {doc.preview}
                                      </span>
                                    </div>
                                  </button>
                                  {expandedYamlDocs[index] && (
                                    <div className="mt-3 ml-6">
                                      <YamlCodeBlock
                                        value={doc.content}
                                        readOnly
                                      />
                                    </div>
                                  )}
                                </div>
                              ))
                            )}
                          </div>
                        );
                      }
                    })()}
                  </div>
                )}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

JobInfoSection.propTypes = {
  jobData: PropTypes.shape({
    id: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    name: PropTypes.oneOfType([PropTypes.string, PropTypes.number]),
    status: PropTypes.string,
    user: PropTypes.string,
    user_hash: PropTypes.string,
    workspace: PropTypes.string,
    submitted_at: PropTypes.oneOfType([
      PropTypes.string,
      PropTypes.number,
      PropTypes.instanceOf(Date),
    ]),
    requested_resources: PropTypes.string,
    infra: PropTypes.string,
    full_infra: PropTypes.string,
    cloud: PropTypes.string,
    resources_str_full: PropTypes.string,
    resources_str: PropTypes.string,
    git_commit: PropTypes.string,
    pool: PropTypes.string,
    pool_hash: PropTypes.string,
    entrypoint: PropTypes.string,
    dag_yaml: PropTypes.string,
  }).isRequired,
  allTasks: PropTypes.array,
  poolsData: PropTypes.array,
  links: PropTypes.object,
  logExtractedLinks: PropTypes.object,
};

export { JobInfoSection };
