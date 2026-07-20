import { showToast } from '@/data/connectors/toast';
import {
  API_VERSION_HEADER,
  CLIENT_API_VERSION,
  CLIENT_VERSION,
  ENDPOINT,
  VERSION_HEADER,
} from '@/data/connectors/constants';
import { trackJobAction } from '@/lib/analytics';
import { apiClient, getCurrentUserInfo } from './client';

const DEFAULT_TAIL_LINES = 5000;

export async function streamManagedJobLogs({
  jobId,
  task = null,
  controller = false,
  signal,
  onNewLog,
}) {
  const requestController = new AbortController();
  const forwardCallerAbort = () => requestController.abort();
  if (signal) {
    if (signal.aborted) {
      forwardCallerAbort();
    } else {
      signal.addEventListener('abort', forwardCallerAbort, { once: true });
    }
  }

  // Measure timeout from last received data, not from start of request.
  const inactivityTimeout = 30000; // 30 seconds of no data activity
  let lastActivity = Date.now();
  let timeoutId;

  // Create an activity-based timeout promise
  const createTimeoutPromise = () => {
    return new Promise((resolve) => {
      const checkActivity = () => {
        const timeSinceLastActivity = Date.now() - lastActivity;

        if (timeSinceLastActivity >= inactivityTimeout) {
          resolve({ timeout: true });
        } else {
          // Check again after remaining time
          timeoutId = setTimeout(
            checkActivity,
            inactivityTimeout - timeSinceLastActivity
          );
        }
      };

      timeoutId = setTimeout(checkActivity, inactivityTimeout);
    });
  };

  const timeoutPromise = createTimeoutPromise();

  // Create the fetch promise
  const fetchPromise = (async () => {
    try {
      const requestBody = {
        controller: controller,
        follow: false,
        job_id: jobId,
        tail: DEFAULT_TAIL_LINES,
        task: task,
      };

      const response = await apiClient.fetchImmediate(
        '/jobs/logs',
        requestBody,
        'POST',
        { signal: requestController.signal }
      );
      // The API client performs preflight work before starting fetch. If that
      // work ignored the abort and completed late, do not consume its body.
      if (requestController.signal.aborted) {
        await response.body?.cancel?.();
        return { timeout: false };
      }

      // Stream the logs
      const reader = response.body.getReader();
      const decoder = new TextDecoder();

      try {
        while (true) {
          const { done, value } = await reader.read();
          if (done) {
            const trailingChunk = decoder.decode();
            if (trailingChunk) onNewLog(trailingChunk);
            break;
          }

          // Update activity timestamp when we receive data
          lastActivity = Date.now();

          const chunk = decoder.decode(value, { stream: true });
          if (chunk) onNewLog(chunk);
        }
      } finally {
        // An aborted reader is already being canceled by fetch.
        if (!requestController.signal.aborted) {
          try {
            reader.cancel();
          } catch (cancelError) {
            // Ignore errors from reader cancellation
            if (cancelError.name !== 'AbortError') {
              console.warn('Error canceling reader:', cancelError);
            }
          }
        }
        // Clear the timeout when streaming completes successfully
        if (timeoutId) {
          clearTimeout(timeoutId);
        }
      }
      return { timeout: false };
    } catch (error) {
      // Clear timeout on any error
      if (timeoutId) {
        clearTimeout(timeoutId);
      }

      // If this was an abort, just return silently
      if (error.name === 'AbortError') {
        return { timeout: false };
      }
      throw error;
    }
  })();

  // Race the fetch against the activity-based timeout.
  let result;
  try {
    result = await Promise.race([fetchPromise, timeoutPromise]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    if (signal) {
      signal.removeEventListener('abort', forwardCallerAbort);
    }
  }

  // If inactivity wins, stop and observe the losing request before returning.
  if (result.timeout) {
    requestController.abort();
    // Do not await cleanup here: fetchImmediate can still be blocked in
    // preflight work that does not observe the request signal. Observe a late
    // non-abort failure without making the public timeout unbounded.
    void fetchPromise.catch((error) => {
      console.warn('Error finishing timed-out log request:', error);
    });
    showToast(
      `Log request for job ${jobId} timed out after ${inactivityTimeout / 1000}s of inactivity`,
      'warning'
    );
  }
}

/**
 * Downloads managed job logs as a zip via the API server.
 * Flow:
 * 1) POST /jobs/download_logs - copy logs from cluster to API server tmp dir
 * 2) POST /download - server zips and streams it back as a binary response
 * 3) Save the response blob via `<a download>` (createObjectURL).
 */
// Long-poll /jobs/download_logs by hand instead of using apiClient.fetch.
// For multi-GB running jobs sync_down can take 5+ minutes — well past
// the ~100s edge timeouts (Cloudflare 524 etc.) of a single GET.
// Retry the polling GET when we hit a 5xx so the user-visible request
// resumes waiting on the SAME server-side request_id until it
// completes. (sync_down already passes follow=False, so the underlying
// stream_logs reads to EOF and exits — it just takes a while.)
async function downloadLogsWithRetry(body, maxAttempts = 30) {
  // Step 1: dispatch the request and grab its server-side ID.
  const baseUrl = window.location.origin;
  const userInfo = await getCurrentUserInfo();
  const dispatch = await fetch(`${baseUrl}${ENDPOINT}/jobs/download_logs`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      // /jobs/download_logs is a queued (executor.schedule_request_async)
      // route, so the worker-side gate honors this header to pick up
      // the resolver path — without it, users without 'default'
      // workspace access would be rejected at
      // reject_request_for_unauthorized_workspace. Both
      // API_VERSION_HEADER and VERSION_HEADER are required — the server
      // middleware drops the ContextVar write if either is missing.
      [API_VERSION_HEADER]: CLIENT_API_VERSION,
      [VERSION_HEADER]: CLIENT_VERSION,
    },
    body: JSON.stringify({
      ...body,
      env_vars: {
        SKYPILOT_IS_FROM_DASHBOARD: 'true',
        SKYPILOT_USER_ID: userInfo.id,
        SKYPILOT_USER: userInfo.name,
      },
    }),
  });
  if (!dispatch.ok) {
    throw new Error(`download_logs dispatch failed: ${dispatch.status}`);
  }
  const requestId = dispatch.headers.get('X-Skypilot-Request-ID');
  if (!requestId) {
    throw new Error('download_logs dispatch missing X-Skypilot-Request-ID');
  }

  // Step 2: long-poll /api/get, retrying on edge-timeout responses.
  for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
    const r = await fetch(
      `${baseUrl}${ENDPOINT}/api/get?request_id=${requestId}`
    );
    // 524 Cloudflare timeout / 502/503/504 transient — retry against
    // the same request_id; the server's long-poll resumes waiting.
    if (
      r.status === 524 ||
      r.status === 502 ||
      r.status === 503 ||
      r.status === 504
    ) {
      // Linear backoff capped at 5s. Cloudflare 524 self-paces at
      // ~100s so most attempts gain nothing, but a server-side 502/503
      // hiccup would otherwise hammer the API server back-to-back.
      const backoffMs = Math.min(1000 * (attempt + 1), 5000);
      await new Promise((resolve) => setTimeout(resolve, backoffMs));
      continue;
    }
    if (!r.ok) {
      const text = await r.text();
      throw new Error(`/api/get ${r.status}: ${text}`);
    }
    const data = await r.json();
    return data.return_value ? JSON.parse(data.return_value) : [];
  }
  throw new Error('download_logs timed out after retries');
}

// Prepare a zip via sync_down + /download, read the response as a
// blob, and save via createObjectURL. The wait scales with rsync time
// on the worker, so multi-GB running logs can take a few minutes —
// downloadLogsWithRetry tolerates Cloudflare 524 during that window.
export async function downloadManagedJobLogs({
  jobId = null,
  name = null,
  controller = false,
}) {
  try {
    const ts = new Date().toISOString().replace(/[:.]/g, '-');
    const namePart = jobId ? `job-${jobId}` : name ? `job-${name}` : 'job';
    const logType = controller ? 'controller-logs' : 'logs';
    const filename = `managed-${namePart}-${logType}-${ts}.zip`;

    const mapping = await downloadLogsWithRetry({
      job_id: jobId,
      name: name,
      controller: controller,
      refresh: false,
    });
    const folderPaths = Object.values(mapping || {});
    if (!folderPaths.length) {
      showToast('No logs found to download.', 'warning');
      return;
    }
    const resp = await apiClient.fetchImmediate('/download?relative=items', {
      folder_paths: folderPaths,
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new Error(`Download failed: ${resp.status} ${text}`);
    }
    const blob = await resp.blob();
    const url = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(url);
    trackJobAction('download_logs', { controller });
  } catch (error) {
    console.error('Error downloading managed job logs:', error);
    showToast(`Error downloading managed job logs: ${error.message}`, 'error');
  }
}
