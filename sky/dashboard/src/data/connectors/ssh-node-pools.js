import {
  API_VERSION_HEADER,
  CLIENT_API_VERSION,
  CLIENT_VERSION,
  ENDPOINT,
  VERSION_HEADER,
} from '@/data/connectors/constants';
import { showToast } from '@/data/connectors/toast';

// Configuration
const DEFAULT_TAIL_LINES = 1000;
const SSH_LOG_INACTIVITY_TIMEOUT_MS = 300000;

export async function getSSHNodePools() {
  try {
    const response = await fetch(`${ENDPOINT}/ssh_node_pools`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error fetching SSH Node Pools:', error);
    return {};
  }
}

export async function updateSSHNodePools(poolsConfig) {
  try {
    const response = await fetch(`${ENDPOINT}/ssh_node_pools`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(poolsConfig),
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error updating SSH Node Pools:', error);
    throw error;
  }
}

export async function deleteSSHNodePool(poolName) {
  try {
    const response = await fetch(`${ENDPOINT}/ssh_node_pools/${poolName}`, {
      method: 'DELETE',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error deleting SSH Node Pool:', error);
    throw error;
  }
}

export async function uploadSSHKey(keyName, keyFile) {
  try {
    const formData = new FormData();
    formData.append('key_name', keyName);
    formData.append('key_file', keyFile);

    const response = await fetch(`${ENDPOINT}/ssh_node_pools/keys`, {
      method: 'POST',
      body: formData,
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error uploading SSH key:', error);
    throw error;
  }
}

export async function listSSHKeys() {
  try {
    const response = await fetch(`${ENDPOINT}/ssh_node_pools/keys`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error listing SSH keys:', error);
    return [];
  }
}

export async function deploySSHNodePool(poolName) {
  try {
    const response = await fetch(
      `${ENDPOINT}/ssh_node_pools/${poolName}/deploy`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          // Identify the dashboard as a contemporary client so the
          // server-side workspace resolver runs on this queued ssh_up
          // request — otherwise users without 'default' workspace
          // access would be rejected at
          // reject_request_for_unauthorized_workspace.
          [API_VERSION_HEADER]: CLIENT_API_VERSION,
          [VERSION_HEADER]: CLIENT_VERSION,
        },
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error deploying SSH Node Pool:', error);
    throw error;
  }
}

export async function sshDownNodePool(poolName) {
  try {
    const response = await fetch(
      `${ENDPOINT}/ssh_node_pools/${poolName}/down`,
      {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          // See deploySSHNodePool() above for why the version header is
          // load-bearing on this queued ssh_up cleanup request.
          [API_VERSION_HEADER]: CLIENT_API_VERSION,
          [VERSION_HEADER]: CLIENT_VERSION,
        },
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error tearing down SSH Node Pool:', error);
    throw error;
  }
}

export async function getSSHNodePoolStatus(poolName) {
  try {
    const response = await fetch(
      `${ENDPOINT}/ssh_node_pools/${poolName}/status`,
      {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      }
    );

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    return await response.json();
  } catch (error) {
    console.error('Error fetching SSH Node Pool status:', error);
    throw error;
  }
}

async function streamSSHLogs({ requestId, signal, onNewLog, streamLabel }) {
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
  const inactivityTimeout = SSH_LOG_INACTIVITY_TIMEOUT_MS;
  let lastActivity = performance.now();
  let timeoutId;

  // Create an activity-based timeout promise
  const createTimeoutPromise = () => {
    return new Promise((resolve) => {
      const checkActivity = () => {
        const timeSinceLastActivity = performance.now() - lastActivity;

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
      const response = await fetch(
        `${ENDPOINT}/api/stream?request_id=${requestId}&format=plain&tail=${DEFAULT_TAIL_LINES}&follow=true`,
        {
          method: 'GET',
          headers: {
            'Content-Type': 'application/json',
          },
          signal: requestController.signal,
        }
      );

      // A transport may ignore abort while connecting and settle after the
      // public timeout. Do not consume that stale response body.
      if (requestController.signal.aborted) {
        await response.body?.cancel?.();
        return { timeout: false };
      }
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
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
          lastActivity = performance.now();

          const chunk = decoder.decode(value, { stream: true });
          if (chunk) onNewLog(chunk);
        }
      } finally {
        // Fetch owns cancellation after abort; explicitly release the reader
        // only on normal completion or a non-abort failure.
        if (!requestController.signal.aborted) {
          try {
            await reader.cancel();
          } catch (cancelError) {
            if (cancelError.name !== 'AbortError') {
              console.warn('Error canceling SSH log reader:', cancelError);
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

  if (result.timeout) {
    requestController.abort();
    // A non-compliant or still-connecting transport can settle after the
    // bounded public timeout. Observe its eventual result without waiting.
    void fetchPromise.catch((error) => {
      console.warn('Error finishing timed-out SSH log request:', error);
    });
    showToast(
      `${streamLabel} log stream timed out after ${inactivityTimeout / 1000}s of inactivity`,
      'warning'
    );
  }
}

export async function streamSSHDeploymentLogs({ requestId, signal, onNewLog }) {
  return streamSSHLogs({
    requestId,
    signal,
    onNewLog,
    streamLabel: 'SSH deployment',
  });
}

export async function streamSSHOperationLogs({
  requestId,
  signal,
  onNewLog,
  operationType = 'operation',
}) {
  return streamSSHLogs({
    requestId,
    signal,
    onNewLog,
    streamLabel: `SSH ${operationType}`,
  });
}
