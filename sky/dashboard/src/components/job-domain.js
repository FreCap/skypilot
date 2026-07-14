// Define status groups for active and finished jobs
export const statusGroups = {
  active: [
    'PENDING',
    'RUNNING',
    'RECOVERING',
    'SUBMITTED',
    'STARTING',
    'CANCELLING',
  ],
  finished: [
    'SUCCEEDED',
    'FAILED',
    'CANCELLED',
    'FAILED_SETUP',
    'FAILED_PRECHECKS',
    'FAILED_NO_RESOURCE',
    'FAILED_CONTROLLER',
  ],
};

// Status priority for aggregation (higher index = worse status)
const STATUS_PRIORITY = {
  SUCCEEDED: 0,
  PENDING: 1,
  SUBMITTED: 2,
  STARTING: 3,
  RUNNING: 4,
  RECOVERING: 5,
  CANCELLING: 6,
  CANCELLED: 7,
  FAILED_SETUP: 8,
  FAILED_PRECHECKS: 9,
  FAILED_NO_RESOURCE: 10,
  FAILED: 11,
  FAILED_CONTROLLER: 12,
};

// Helper function to aggregate status for a job group
// Returns the "worst" status based on priority
// For job groups with primary/auxiliary tasks, status is determined only by primary tasks
// Uses is_primary_in_job_group per task: null (non-group), true (primary), false (auxiliary)
export function getAggregatedStatus(tasks) {
  if (!tasks || tasks.length === 0) return 'PENDING';
  if (tasks.length === 1) return tasks[0].status;

  // Filter to only primary tasks for status determination.
  // is_primary_in_job_group: true/false for job groups, null/undefined for non-groups.
  // For non-job-groups (null), all tasks count for status.
  // For job groups, only tasks with is_primary_in_job_group=true count.
  const primaryTasks = tasks.filter(
    (t) =>
      t.is_primary_in_job_group === null ||
      t.is_primary_in_job_group === undefined ||
      t.is_primary_in_job_group === true
  );

  // Use primary tasks for status; fall back to all tasks if none match
  const tasksForStatus = primaryTasks.length > 0 ? primaryTasks : tasks;

  let worstStatus = 'SUCCEEDED';
  let worstPriority = 0;

  for (const task of tasksForStatus) {
    const priority = STATUS_PRIORITY[task.status] ?? 0;
    if (priority > worstPriority) {
      worstPriority = priority;
      worstStatus = task.status;
    }
  }

  return worstStatus;
}

// Helper function to filter jobs by name
export function filterJobsByName(jobs, nameFilter) {
  // If no name filter, return all jobs
  if (!nameFilter || nameFilter.trim() === '') {
    return jobs;
  }

  // Filter jobs by the name filter (case-insensitive partial match)
  const filterLower = nameFilter.toLowerCase().trim();
  return jobs.filter((job) => {
    const jobName = job.name || '';
    return jobName.toLowerCase().includes(filterLower);
  });
}

// Helper function to filter jobs by workspace
export function filterJobsByWorkspace(jobs, workspaceFilter) {
  // If no workspace filter or set to "All Workspaces", return all jobs
  if (!workspaceFilter || workspaceFilter === 'ALL_WORKSPACES') {
    return jobs;
  }

  // Filter jobs by the selected workspace
  return jobs.filter((job) => {
    const jobWorkspace = job.workspace || 'default'; // Treat missing/empty workspace as 'default'
    return jobWorkspace.toLowerCase() === workspaceFilter.toLowerCase();
  });
}

// Helper function to filter jobs by user
export function filterJobsByUser(jobs, userFilter) {
  // If no user filter or set to "All Users", return all jobs
  if (!userFilter || userFilter === 'ALL_USERS') {
    return jobs;
  }

  // Filter jobs by the selected user
  return jobs.filter((job) => {
    const jobUserId = job.user_hash || job.user;
    return jobUserId === userFilter;
  });
}

// Helper function to filter jobs by pool
export function filterJobsByPool(jobs, poolFilter) {
  // If no pool filter, return all jobs
  if (!poolFilter || poolFilter.trim() === '') {
    return jobs;
  }

  // Filter jobs by the pool filter (case-insensitive partial match)
  const filterLower = poolFilter.toLowerCase().trim();
  return jobs.filter((job) => {
    const jobPool = job.pool || '';
    return jobPool.toLowerCase().includes(filterLower);
  });
}
