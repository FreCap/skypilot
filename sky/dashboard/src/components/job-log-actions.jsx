import { useRouter } from 'next/router';
import { Download, FileSearchIcon } from 'lucide-react';
import { CustomTooltip as Tooltip } from '@/components/utils';
import { downloadJobLogs } from '@/data/connectors/clusters';
import { downloadManagedJobLogs } from '@/data/connectors/jobs';
import { trackJobAction } from '@/lib/analytics';

export function Status2Actions({
  withLabel = false,
  jobParent,
  jobId,
  managed,
  workspace = 'default',
}) {
  const router = useRouter();

  const handleLogsClick = (e, type) => {
    e.preventDefault();
    e.stopPropagation();
    trackJobAction('view_logs', { jobId });
    router.push({
      pathname: `${jobParent}/${jobId}`,
      query: { tab: type },
    });
  };

  const handleDownloadLogs = (e, controller = false) => {
    e.preventDefault();
    e.stopPropagation();
    trackJobAction('download_logs', { jobId });

    if (managed) {
      // For managed jobs
      downloadManagedJobLogs({
        jobId: parseInt(jobId),
        controller: controller,
      });
    } else {
      // For cluster jobs, extract cluster name from jobParent
      const clusterNameMatch = jobParent.match(/\/clusters\/(.+)/);
      if (clusterNameMatch) {
        const clusterName = clusterNameMatch[1];
        downloadJobLogs({
          clusterName: clusterName,
          jobIds: [jobId],
          workspace: workspace,
        });
      }
    }
  };

  return (
    <div className="flex items-center space-x-2">
      <Tooltip
        key="logs"
        content="View Job Logs"
        className="capitalize text-sm text-muted-foreground"
      >
        <button
          onClick={(e) => handleLogsClick(e, 'logs')}
          className="text-sky-blue hover:text-sky-blue-bright font-medium inline-flex items-center h-8"
        >
          <FileSearchIcon className="w-4 h-4" />
          {withLabel && <span className="ml-1.5">Logs</span>}
        </button>
      </Tooltip>
      <Tooltip
        key="downloadlogs"
        content="Download All Task Logs (zip)"
        className="capitalize text-sm text-muted-foreground"
      >
        <button
          onClick={(e) => handleDownloadLogs(e, false)}
          className="text-sky-blue hover:text-sky-blue-bright font-medium inline-flex items-center h-8"
          title="Download logs"
        >
          <Download className="w-4 h-4" />
          {withLabel && <span className="ml-1.5">Download</span>}
        </button>
      </Tooltip>
    </div>
  );
}
