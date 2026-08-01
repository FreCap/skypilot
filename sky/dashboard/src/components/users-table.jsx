'use client';

import React, {
  useState,
  useEffect,
  useCallback,
  useMemo,
  useRef,
} from 'react';
import PropTypes from 'prop-types';
import { CircularProgress } from '@mui/material';
import Link from 'next/link';
import { Button } from '@/components/ui/button';
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from '@/components/ui/table';
import { getUsers } from '@/data/connectors/users';
import dashboardCache from '@/lib/cache';
import cachePreloader from '@/lib/cache-preloader';
import { sortData } from '@/data/utils';
import { TimestampWithTooltip } from '@/components/utils';
import { useVisibleRefreshInterval } from '@/hooks/useVisibleRefreshInterval';
import {
  PenIcon,
  CheckIcon,
  XIcon,
  KeyRoundIcon,
  Trash2Icon,
  PlusIcon,
  MinusIcon,
} from 'lucide-react';
import { Card } from '@/components/ui/card';
import { apiClient } from '@/data/connectors/client';
import {
  BatchRoleDialog,
  BatchAddToWorkspacesDialog,
  BatchRemoveFromWorkspacesDialog,
} from '@/components/users-batch-dialogs';
import { filterData } from '@/components/shared/FilterSystem';
import {
  aggregateUserUsage,
  buildUsageFilterLookup,
  fetchClustersAndJobs,
} from '@/components/user-usage';

const parseUsername = (username, userId) => {
  if (username && username.includes('@')) {
    return username.split('@')[0];
  }
  return username || 'N/A';
};

const getFullEmailID = (username, userId) => {
  if (username && username.includes('@')) {
    return username;
  }
  return userId || '-';
};

export const buildUsersWithUsage = (users = [], clusters = [], jobs = []) => {
  const usageByUser = aggregateUserUsage(clusters, jobs);

  return (users || []).map((user) => ({
    ...user,
    usernameDisplay: parseUsername(user.username, user.userId),
    fullEmailID: getFullEmailID(user.username, user.userId),
    ...(usageByUser.get(user.userId) || {
      clusterCount: 0,
      jobCount: 0,
      gpuCount: 0,
    }),
  }));
};

export function UsersTable({
  refreshInterval,
  setLoading,
  refreshDataRef,
  checkPermissionAndAct,
  roleLoading,
  onResetPassword,
  onDeleteUser,
  basicAuthEnabled,
  ingressBasicAuthEnabled,
  externalProxyAuthEnabled,
  currentUserRole,
  currentUserId,
  filters,
  setValueList,
  deduplicateUsers,
  setLastFetchedTime,
  setCreateError,
}) {
  const [usersWithCounts, setUsersWithCounts] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [hasInitiallyLoaded, setHasInitiallyLoaded] = useState(false);
  const [sortConfig, setSortConfig] = useState({
    key: 'default',
    direction: 'descending',
  });
  const [editingUserId, setEditingUserId] = useState(null);
  const [currentEditingRole, setCurrentEditingRole] = useState('');

  // Multi-select state for batch operations on users.
  // Only enabled when the current user is an admin (the checkbox column is
  // hidden otherwise). System users cannot be selected.
  const [selectedUserIds, setSelectedUserIds] = useState(new Set());
  const [bulkDialog, setBulkDialog] = useState(null); // 'role' | 'add' | 'remove'

  // Lookup dictionary for GPU type and infra filtering
  // Structure: infra -> gpuType -> userId -> { clusterCount, jobCount, gpuCount }
  const [combinedLookup, setCombinedLookup] = useState({});
  const [lookupsReady, setLookupsReady] = useState(false);
  const refreshState = useRef({ generation: 0, active: null });

  const runRefresh = useCallback(
    async (showLoading, owner) => {
      const ownsRefresh = () =>
        refreshState.current.active === owner &&
        refreshState.current.generation === owner.generation;
      if (setLoading && showLoading) setLoading(true);
      if (showLoading) setIsLoading(true);
      setLookupsReady(false); // Reset lookups state when starting to fetch
      try {
        // Step 1: Load users first and show them immediately
        const usersData = await dashboardCache.get(getUsers);
        if (!ownsRefresh()) return;

        // Show users immediately with placeholder counts
        const initialProcessedUsers = (usersData || []).map((user) => ({
          ...user,
          usernameDisplay: parseUsername(user.username, user.userId),
          fullEmailID: getFullEmailID(user.username, user.userId),
          clusterCount: -1, // Use -1 as loading indicator
          jobCount: -1, // Use -1 as loading indicator
          gpuCount: -1, // Use -1 as loading indicator
        }));

        setUsersWithCounts(initialProcessedUsers);
        setHasInitiallyLoaded(true);

        // Clear loading indicators now that we have users
        if (setLoading && showLoading) setLoading(false);
        if (showLoading) setIsLoading(false);

        // Step 2: Load clusters and jobs in background and update counts
        const { clustersData, jobsResponse } = await fetchClustersAndJobs();
        if (!ownsRefresh()) return;

        const jobsData = jobsResponse.jobs || [];

        const newCombinedLookup = buildUsageFilterLookup(
          clustersData,
          jobsData
        );

        // Store the lookup dictionary
        setCombinedLookup(newCombinedLookup);
        setLookupsReady(true); // Mark lookups as ready

        // Update users with actual counts (without filter applied)
        const finalProcessedUsers = buildUsersWithUsage(
          usersData,
          clustersData,
          jobsData
        );

        // Collect unique GPU types and infra values for filter dropdowns
        const infras = new Set();
        const gpuTypes = new Set();

        for (const userLookup of Object.values(newCombinedLookup)) {
          // Collect infras (skip "Total" key)
          for (const infra of Object.keys(userLookup)) {
            if (infra !== 'Total') {
              infras.add(infra);
            }
          }
          // Collect GPU types from cross-infra "Total"
          if (userLookup['Total']) {
            for (const gpuType of Object.keys(userLookup['Total'])) {
              gpuTypes.add(gpuType);
            }
          }
        }

        // Update valueList for filter autocomplete
        const names = new Set();
        const userIds = new Set();
        const roles = new Set();

        finalProcessedUsers.forEach((user) => {
          if (user.usernameDisplay) names.add(user.usernameDisplay);
          if (user.userId) userIds.add(user.userId);
          if (user.role) roles.add(user.role);
        });

        setValueList({
          name: Array.from(names).sort(),
          'user id': Array.from(userIds).sort(),
          role: Array.from(roles).sort(),
          'gpu type': Array.from(gpuTypes).sort(),
          infra: Array.from(infras).sort(),
        });

        setUsersWithCounts(finalProcessedUsers);
      } catch (error) {
        if (!ownsRefresh()) return;
        console.error('Failed to fetch or process user data:', error);
        setUsersWithCounts([]);
        setHasInitiallyLoaded(true);
        if (setLoading && showLoading) setLoading(false);
        if (showLoading) setIsLoading(false);
      } finally {
        if (ownsRefresh() && setLastFetchedTime) {
          setLastFetchedTime(new Date());
        }
      }
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [setLoading, setLastFetchedTime]
  );

  const fetchDataAndProcess = useCallback(
    (showLoading = false) => {
      const state = refreshState.current;
      // Polling ticks share in-flight work. Manual refreshes use loading UI
      // and must supersede it because user mutations may have invalidated the
      // cache since that background snapshot started.
      if (state.active !== null && !showLoading) {
        return state.active.promise;
      }

      const owner = { generation: ++state.generation, promise: null };
      state.active = owner;
      owner.promise = runRefresh(showLoading, owner).finally(() => {
        if (state.active === owner && state.generation === owner.generation) {
          state.active = null;
        }
      });
      return owner.promise;
    },
    [runRefresh]
  );

  useEffect(() => {
    if (refreshDataRef) {
      const refresh = () => fetchDataAndProcess(true);
      refreshDataRef.current = refresh;
      return () => {
        if (refreshDataRef.current === refresh) {
          refreshDataRef.current = null;
        }
      };
    }
    return undefined;
  }, [refreshDataRef, fetchDataAndProcess]);

  useEffect(() => {
    let disposed = false;
    const initializeData = async () => {
      // Reset loading state when component mounts
      setHasInitiallyLoaded(false);
      setIsLoading(true);

      // Trigger cache preloading for users page and background preload other pages
      await cachePreloader.preloadForPage('users');

      if (disposed) return;
      fetchDataAndProcess(true); // Show loading on initial load
    };

    initializeData();
    const state = refreshState.current;
    return () => {
      disposed = true;
      state.generation += 1;
      state.active = null;
    };
  }, [fetchDataAndProcess, refreshInterval]);

  const refreshUsersWhenVisible = useCallback(() => {
    void fetchDataAndProcess(false);
  }, [fetchDataAndProcess]);

  useVisibleRefreshInterval(
    hasInitiallyLoaded,
    refreshInterval,
    refreshUsersWhenVisible
  );

  const filteredAndSortedUsers = useMemo(() => {
    let filtered = usersWithCounts;

    // Separate GPU type and infra filters from standard filters
    // Note: filter.property contains the label (e.g., "GPU", "Infra"), not the value
    const standardFilters = filters.filter(
      (f) => f.property !== 'GPU' && f.property !== 'Infra'
    );
    const gpuTypeFilters = filters.filter((f) => f.property === 'GPU');
    const infraFilters = filters.filter((f) => f.property === 'Infra');

    // Apply standard filters using the shared filter system
    if (standardFilters.length > 0) {
      filtered = filterData(
        usersWithCounts.map((user) => ({
          ...user,
          name: user.usernameDisplay,
          'user id': user.userId, // Note: space to match "User ID" -> "user id" from toLowerCase()
        })),
        standardFilters
      );
    }

    // Helper to get counts from lookup for a user given filter criteria
    // gpuTypeFilters and infraFilters are arrays - we OR within same type, AND across types
    const getFilteredCounts = (
      userId,
      gpuTypeFilterValues,
      infraFilterValues
    ) => {
      let clusterCount = 0;
      let jobCount = 0;
      let gpuCount = 0;

      const userLookup = combinedLookup[userId];
      if (!userLookup) {
        return { clusterCount: 0, jobCount: 0, gpuCount: 0 };
      }

      // Normalize filter values to lowercase
      const normalizedGpuTypes = gpuTypeFilterValues.map((v) =>
        v.toLowerCase()
      );
      const normalizedInfras = infraFilterValues.map((v) => v.toLowerCase());

      const hasGpuTypeFilters = normalizedGpuTypes.length > 0;
      const hasInfraFilters = normalizedInfras.length > 0;

      // Case 1: Both GPU and Infra filters (AND between types, OR within types)
      if (hasGpuTypeFilters && hasInfraFilters) {
        for (const infraFilter of normalizedInfras) {
          for (const [infra, gpuTypeMap] of Object.entries(userLookup)) {
            if (infra === 'Total') continue;
            if (infra.toLowerCase() !== infraFilter) continue;

            for (const gpuTypeFilter of normalizedGpuTypes) {
              for (const [gpuType, counts] of Object.entries(gpuTypeMap)) {
                if (gpuType === 'Total') continue;
                if (gpuType.toLowerCase() === gpuTypeFilter) {
                  clusterCount += counts.clusterCount;
                  jobCount += counts.jobCount;
                  gpuCount += counts.gpuCount;
                }
              }
            }
          }
        }
      }
      // Case 2: Infra only
      else if (hasInfraFilters) {
        for (const infraFilter of normalizedInfras) {
          for (const [infra, gpuTypeMap] of Object.entries(userLookup)) {
            if (infra === 'Total') continue;
            if (infra.toLowerCase() === infraFilter && gpuTypeMap['Total']) {
              const counts = gpuTypeMap['Total'];
              clusterCount += counts.clusterCount;
              jobCount += counts.jobCount;
              gpuCount += counts.gpuCount;
            }
          }
        }
      }
      // Case 3: GPU type only
      else if (hasGpuTypeFilters) {
        if (userLookup['Total']) {
          for (const gpuTypeFilter of normalizedGpuTypes) {
            for (const [gpuType, counts] of Object.entries(
              userLookup['Total']
            )) {
              if (gpuType.toLowerCase() === gpuTypeFilter) {
                clusterCount += counts.clusterCount;
                jobCount += counts.jobCount;
                gpuCount += counts.gpuCount;
              }
            }
          }
        }
      }

      return { clusterCount, jobCount, gpuCount };
    };

    // Apply GPU type and infra filters
    const hasGpuTypeFilter = gpuTypeFilters.length > 0;
    const hasInfraFilter = infraFilters.length > 0;

    if (hasGpuTypeFilter || hasInfraFilter) {
      // Extract filter values - support multiple filters of same type (OR logic)
      const gpuTypeFilterValues = gpuTypeFilters
        .map((f) => f.value)
        .filter(Boolean);
      const infraFilterValues = infraFilters
        .map((f) => f.value)
        .filter(Boolean);

      // Normalize to lowercase for matching
      const normalizedGpuTypes = gpuTypeFilterValues.map((v) =>
        v.toLowerCase()
      );
      const normalizedInfras = infraFilterValues.map((v) => v.toLowerCase());

      // Filter users: check if they have ANY resources matching the filters
      filtered = filtered.filter((user) => {
        const userLookup = combinedLookup[user.userId];
        if (!userLookup) return false;

        // Case 1: Both GPU and Infra filters
        if (hasGpuTypeFilter && hasInfraFilter) {
          for (const infraFilter of normalizedInfras) {
            for (const [infra, gpuTypeMap] of Object.entries(userLookup)) {
              if (infra === 'Total') continue;
              if (infra.toLowerCase() !== infraFilter) continue;

              for (const gpuTypeFilter of normalizedGpuTypes) {
                for (const gpuType of Object.keys(gpuTypeMap)) {
                  if (gpuType === 'Total') continue;
                  if (gpuType.toLowerCase() === gpuTypeFilter) {
                    return true;
                  }
                }
              }
            }
          }
        }
        // Case 2: Infra only
        else if (hasInfraFilter) {
          for (const infraFilter of normalizedInfras) {
            for (const infra of Object.keys(userLookup)) {
              if (infra === 'Total') continue;
              if (infra.toLowerCase() === infraFilter) {
                return true;
              }
            }
          }
        }
        // Case 3: GPU type only
        else if (hasGpuTypeFilter) {
          if (userLookup['Total']) {
            for (const gpuTypeFilter of normalizedGpuTypes) {
              for (const gpuType of Object.keys(userLookup['Total'])) {
                if (gpuType.toLowerCase() === gpuTypeFilter) {
                  return true;
                }
              }
            }
          }
        }

        return false;
      });

      // Update counts for filtered users
      filtered = filtered.map((user) => {
        const filteredCounts = getFilteredCounts(
          user.userId,
          gpuTypeFilterValues,
          infraFilterValues
        );

        return {
          ...user,
          clusterCount: filteredCounts.clusterCount,
          jobCount: filteredCounts.jobCount,
          gpuCount: filteredCounts.gpuCount,
        };
      });
    }

    // Deduplicate by username if toggle is enabled
    if (deduplicateUsers) {
      const deduped = {};
      filtered.forEach((user) => {
        const name = user.usernameDisplay;
        if (!deduped[name]) {
          // Initialize with first user
          deduped[name] = {
            ...user,
            // Track all userIds for this username
            userIds: [user.userId],
            // Track counts that will be summed
            clusterCount: user.clusterCount,
            jobCount: user.jobCount,
            gpuCount: user.gpuCount,
            // Track the oldest created_at
            created_at: user.created_at,
          };
        } else {
          // Merge with existing entry
          deduped[name].userIds.push(user.userId);

          // Sum cluster counts (handle loading state smartly)
          if (user.clusterCount !== -1) {
            // If current user has a valid count
            if (deduped[name].clusterCount === -1) {
              // Replace loading state with actual count
              deduped[name].clusterCount = user.clusterCount;
            } else {
              // Add to existing valid count
              deduped[name].clusterCount += user.clusterCount;
            }
          }
          // If user.clusterCount === -1 and deduped already has valid count, keep existing

          // Sum job counts (same logic)
          if (user.jobCount !== -1) {
            if (deduped[name].jobCount === -1) {
              deduped[name].jobCount = user.jobCount;
            } else {
              deduped[name].jobCount += user.jobCount;
            }
          }

          // Sum GPU counts (same logic)
          if (user.gpuCount !== -1) {
            if (deduped[name].gpuCount === -1) {
              deduped[name].gpuCount = user.gpuCount;
            } else {
              deduped[name].gpuCount += user.gpuCount;
            }
          }

          // Keep the oldest created_at
          if (
            user.created_at &&
            (!deduped[name].created_at ||
              user.created_at < deduped[name].created_at)
          ) {
            deduped[name].created_at = user.created_at;
          }
        }
      });
      filtered = Object.values(deduped);
    }

    if (sortConfig.key === 'default') {
      // Default sort: GPUs desc, then Clusters desc, then Jobs desc, then
      // Joined (most recent first), then Name asc.
      const cmpStr = (a, b) => {
        const sa = (a ?? '').toString().toLowerCase();
        const sb = (b ?? '').toString().toLowerCase();
        if (sa < sb) return -1;
        if (sa > sb) return 1;
        return 0;
      };
      const cmpNum = (a, b) => (a ?? 0) - (b ?? 0);
      return [...filtered].sort((a, b) => {
        const byGpu = cmpNum(b.gpuCount, a.gpuCount);
        if (byGpu !== 0) return byGpu;
        const byClusters = cmpNum(b.clusterCount, a.clusterCount);
        if (byClusters !== 0) return byClusters;
        const byJobs = cmpNum(b.jobCount, a.jobCount);
        if (byJobs !== 0) return byJobs;
        const byJoined = cmpNum(b.created_at, a.created_at);
        if (byJoined !== 0) return byJoined;
        return cmpStr(a.usernameDisplay, b.usernameDisplay);
      });
    }
    return sortData(filtered, sortConfig.key, sortConfig.direction);
  }, [usersWithCounts, sortConfig, filters, deduplicateUsers, combinedLookup]);

  const requestSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const getSortDirection = (key) => {
    if (sortConfig.key === key) {
      return sortConfig.direction === 'ascending' ? ' ↑' : ' ↓';
    }
    return '';
  };

  const handleEditClick = async (userId, currentRole) => {
    await checkPermissionAndAct('cannot edit user role', () => {
      setEditingUserId(userId);
      setCurrentEditingRole(currentRole);
    });
  };

  const handleCancelEdit = () => {
    setEditingUserId(null);
    setCurrentEditingRole('');
  };

  const handleSaveEdit = async (userId) => {
    if (!userId || !currentEditingRole) {
      console.error('User ID or role is missing.');
      setCreateError(new Error('User ID or role is missing.'));
      return;
    }
    setIsLoading(true); // Or use parent setLoading
    try {
      const response = await apiClient.post(`/users/update`, {
        user_id: userId,
        role: currentEditingRole,
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to update role');
      }
      // Invalidate cache before fetching new data
      dashboardCache.invalidate(getUsers);
      await fetchDataAndProcess(true); // Refresh data
      handleCancelEdit(); // Exit edit mode
    } catch (error) {
      console.error('Failed to update user role:', error);
      handleCancelEdit();
      setCreateError(error);
    } finally {
      setIsLoading(false); // Or use parent setLoading
    }
  };

  // Users that are eligible to be selected for batch operations.
  // System users (e.g., dashboard, system API user) are excluded.
  const selectableUsers = useMemo(
    () => (filteredAndSortedUsers || []).filter((u) => u.userType !== 'system'),
    [filteredAndSortedUsers]
  );

  const allOnPageSelected =
    selectableUsers.length > 0 &&
    selectableUsers.every((u) => selectedUserIds.has(u.userId));
  const someOnPageSelected =
    !allOnPageSelected &&
    selectableUsers.some((u) => selectedUserIds.has(u.userId));

  const showBulkColumn =
    currentUserRole === 'admin' &&
    !deduplicateUsers &&
    !ingressBasicAuthEnabled;

  const toggleSelectAllOnPage = () => {
    const next = new Set(selectedUserIds);
    if (allOnPageSelected) {
      selectableUsers.forEach((u) => next.delete(u.userId));
    } else {
      selectableUsers.forEach((u) => next.add(u.userId));
    }
    setSelectedUserIds(next);
  };

  const toggleSelectOne = (userId) => {
    const next = new Set(selectedUserIds);
    if (next.has(userId)) {
      next.delete(userId);
    } else {
      next.add(userId);
    }
    setSelectedUserIds(next);
  };

  const clearSelection = () => setSelectedUserIds(new Set());

  // "Workspaces" dropdown on the floating bar.
  const [wsDropdownOpen, setWsDropdownOpen] = useState(false);
  const wsDropdownRef = useRef(null);

  // Esc clears the current selection (and dismisses the floating bar).
  useEffect(() => {
    if (selectedUserIds.size === 0) return undefined;
    const handler = (e) => {
      if (e.key === 'Escape') {
        // If the workspaces dropdown is open, Esc closes the dropdown
        // first; pressing Esc again clears the selection.
        if (wsDropdownOpen) {
          setWsDropdownOpen(false);
        } else {
          clearSelection();
        }
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [selectedUserIds.size, wsDropdownOpen]);

  // Close the workspaces dropdown on outside click.
  useEffect(() => {
    if (!wsDropdownOpen) return undefined;
    const handler = (e) => {
      if (wsDropdownRef.current && !wsDropdownRef.current.contains(e.target)) {
        setWsDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [wsDropdownOpen]);

  const handleBulkDialogClose = async (fullySucceeded) => {
    setBulkDialog(null);
    if (fullySucceeded) {
      clearSelection();
      dashboardCache.invalidate(getUsers);
      await fetchDataAndProcess(true);
    } else {
      // Refresh even on partial failure so the table reflects the rows that
      // did succeed; keep the selection so the admin can retry failures.
      dashboardCache.invalidate(getUsers);
      await fetchDataAndProcess(false);
    }
  };

  const openBulkDialog = async (kind) => {
    await checkPermissionAndAct(`cannot perform bulk ${kind} on users`, () =>
      setBulkDialog(kind)
    );
  };

  const selectedUserObjects = useMemo(
    () => (usersWithCounts || []).filter((u) => selectedUserIds.has(u.userId)),
    [usersWithCounts, selectedUserIds]
  );

  if (isLoading && usersWithCounts.length === 0 && !hasInitiallyLoaded) {
    return (
      <div className="flex justify-center items-center h-64">
        <CircularProgress />
      </div>
    );
  }

  if (!hasInitiallyLoaded) {
    return (
      <div className="flex justify-center items-center h-64">
        <CircularProgress />
        <span className="ml-2 text-gray-500">Loading users...</span>
      </div>
    );
  }

  // Check if we're still loading lookups for GPU/Infra filters
  const hasGpuOrInfraFilters = filters.some(
    (f) => f.property === 'GPU' || f.property === 'Infra'
  );
  if (hasGpuOrInfraFilters && !lookupsReady) {
    return (
      <div className="flex justify-center items-center h-64">
        <CircularProgress />
        <span className="ml-2 text-gray-500">Loading filtered data...</span>
      </div>
    );
  }

  if (!filteredAndSortedUsers || filteredAndSortedUsers.length === 0) {
    return (
      <div className="text-center py-12">
        <p className="text-lg font-semibold text-gray-500">
          {filters.length > 0
            ? 'No users match your filters.'
            : 'No users found.'}
        </p>
        <p className="text-sm text-gray-400 mt-1">
          {filters.length > 0
            ? 'Try adjusting your filter criteria.'
            : 'There are currently no users to display.'}
        </p>
      </div>
    );
  }

  return (
    <>
      {filteredAndSortedUsers.length > 0 && (
        <div className="text-sm text-gray-500 mb-2">
          {filteredAndSortedUsers.length}{' '}
          {filteredAndSortedUsers.length === 1 ? 'user' : 'users'}
        </div>
      )}
      {showBulkColumn && (
        // Floating bottom-center action bar. Always mounted (so the
        // slide animation works in both directions), but
        // pointer-events-none + translate-y-full + opacity-0 when no
        // selection so it occupies no visual space and can't be
        // interacted with. No layout shift on the table itself.
        <div
          className={`fixed bottom-6 left-1/2 -translate-x-1/2 z-30 transition-all duration-200 ease-out ${
            selectedUserIds.size > 0
              ? 'opacity-100 translate-y-0'
              : 'opacity-0 translate-y-[200%] pointer-events-none'
          }`}
          role="region"
          aria-label="Batch user actions"
          aria-hidden={selectedUserIds.size === 0}
        >
          <div className="flex items-center gap-3 px-4 py-2 bg-white border border-gray-200 shadow-lg rounded-full">
            <div className="text-sm text-gray-700 whitespace-nowrap">
              <span className="font-medium text-sky-blue">
                {selectedUserIds.size}
              </span>{' '}
              selected
            </div>
            <button
              type="button"
              onClick={clearSelection}
              className="text-sm text-sky-blue hover:text-sky-blue-bright underline whitespace-nowrap"
            >
              Clear
            </button>
            <div className="h-5 w-px bg-gray-200" aria-hidden="true" />
            <button
              type="button"
              onClick={() => openBulkDialog('role')}
              disabled={roleLoading}
              className="bg-sky-600 hover:bg-sky-700 text-white flex items-center rounded-md px-3 py-1 text-sm font-medium transition-colors duration-200 disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
            >
              Role
            </button>
            <div className="relative" ref={wsDropdownRef}>
              <button
                type="button"
                onClick={() => setWsDropdownOpen((v) => !v)}
                disabled={roleLoading}
                aria-haspopup="menu"
                aria-expanded={wsDropdownOpen}
                className="bg-sky-600 hover:bg-sky-700 text-white inline-flex items-center gap-1 rounded-md px-3 py-1 text-sm font-medium transition-colors duration-200 disabled:opacity-50 disabled:cursor-not-allowed whitespace-nowrap"
              >
                <span>Workspaces</span>
                <svg
                  className={`w-3.5 h-3.5 transition-transform ${
                    wsDropdownOpen ? 'rotate-180' : ''
                  }`}
                  fill="currentColor"
                  viewBox="0 0 20 20"
                  aria-hidden="true"
                >
                  <path
                    fillRule="evenodd"
                    d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z"
                    clipRule="evenodd"
                  />
                </svg>
              </button>
              {wsDropdownOpen && (
                <div
                  role="menu"
                  className="absolute bottom-full right-0 mb-2 bg-white rounded-lg shadow-xl border border-gray-200 z-40 py-1.5 px-1"
                >
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => {
                      setWsDropdownOpen(false);
                      openBulkDialog('add');
                    }}
                    className="flex items-center gap-2.5 w-full text-left px-3 py-1.5 text-sm text-gray-700 hover:bg-gray-50 rounded transition-colors whitespace-nowrap"
                  >
                    <PlusIcon className="h-4 w-4 text-gray-500 flex-shrink-0" />
                    <span>Add to workspaces</span>
                  </button>
                  <button
                    type="button"
                    role="menuitem"
                    onClick={() => {
                      setWsDropdownOpen(false);
                      openBulkDialog('remove');
                    }}
                    className="flex items-center gap-2.5 w-full text-left px-3 py-1.5 text-sm text-gray-700 hover:bg-gray-50 rounded transition-colors whitespace-nowrap"
                  >
                    <MinusIcon className="h-4 w-4 text-gray-500 flex-shrink-0" />
                    <span>Remove from workspaces</span>
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
      <Card>
        <div className="overflow-x-auto rounded-lg">
          <Table className="min-w-full">
            <TableHeader>
              <TableRow>
                {showBulkColumn && (
                  <TableHead className="w-8 whitespace-nowrap">
                    {/* Header "select all" stays visible as a discoverable
                        cue that the table supports selection; row checkboxes
                        remain hover-only to reduce clutter. */}
                    <input
                      type="checkbox"
                      aria-label="Select all users on this page"
                      checked={allOnPageSelected}
                      ref={(el) => {
                        if (el) el.indeterminate = someOnPageSelected;
                      }}
                      onChange={toggleSelectAllOnPage}
                    />
                  </TableHead>
                )}
                <TableHead
                  onClick={() => requestSort('usernameDisplay')}
                  className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                >
                  Name{getSortDirection('usernameDisplay')}
                </TableHead>
                {!deduplicateUsers && (
                  <TableHead
                    onClick={() => requestSort('fullEmailID')}
                    className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                  >
                    User ID{getSortDirection('fullEmailID')}
                  </TableHead>
                )}
                {!deduplicateUsers && !ingressBasicAuthEnabled && (
                  <TableHead
                    onClick={() => requestSort('role')}
                    className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                  >
                    Role{getSortDirection('role')}
                  </TableHead>
                )}
                {!deduplicateUsers &&
                  !ingressBasicAuthEnabled &&
                  !externalProxyAuthEnabled && (
                    <TableHead
                      onClick={() => requestSort('userType')}
                      className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                    >
                      Type{getSortDirection('userType')}
                    </TableHead>
                  )}
                <TableHead
                  onClick={() => requestSort('created_at')}
                  className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                >
                  Joined{getSortDirection('created_at')}
                </TableHead>
                <TableHead
                  onClick={() => requestSort('gpuCount')}
                  className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                >
                  GPUs{getSortDirection('gpuCount')}
                </TableHead>
                <TableHead
                  onClick={() => requestSort('clusterCount')}
                  className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                >
                  Clusters{getSortDirection('clusterCount')}
                </TableHead>
                <TableHead
                  onClick={() => requestSort('jobCount')}
                  className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50 w-1/6"
                >
                  Jobs{getSortDirection('jobCount')}
                </TableHead>
                {/* Show Actions column if basicAuthEnabled and not deduplicating */}
                {!deduplicateUsers &&
                  (basicAuthEnabled || currentUserRole === 'admin') && (
                    <TableHead className="whitespace-nowrap w-1/7">
                      Actions
                    </TableHead>
                  )}
              </TableRow>
            </TableHeader>
            <TableBody>
              {filteredAndSortedUsers.map((user) => {
                const isSystemUser = user.userType === 'system';
                const isBasicUser = user.userType === 'basic';
                const canResetPassword =
                  isBasicUser &&
                  (currentUserRole === 'admin' ||
                    user.userId === currentUserId);
                return (
                  <TableRow key={user.userId} className="group">
                    {showBulkColumn && (
                      <TableCell className="w-8 whitespace-nowrap">
                        {!isSystemUser && (
                          <div
                            className={`transition-opacity duration-150 ${
                              selectedUserIds.size > 0
                                ? 'opacity-100'
                                : 'opacity-0 group-hover:opacity-100 focus-within:opacity-100'
                            }`}
                          >
                            <input
                              type="checkbox"
                              aria-label={`Select user ${user.usernameDisplay || user.userId}`}
                              checked={selectedUserIds.has(user.userId)}
                              onChange={() => toggleSelectOne(user.userId)}
                            />
                          </div>
                        )}
                      </TableCell>
                    )}
                    <TableCell className="truncate" title={user.username}>
                      {user.usernameDisplay}
                    </TableCell>
                    {!deduplicateUsers && (
                      <TableCell className="truncate" title={user.fullEmailID}>
                        {user.fullEmailID}
                      </TableCell>
                    )}
                    {!deduplicateUsers && !ingressBasicAuthEnabled && (
                      <TableCell className="truncate" title={user.role}>
                        <div className="flex items-center gap-2">
                          {editingUserId === user.userId ? (
                            <>
                              <select
                                value={currentEditingRole}
                                onChange={(e) =>
                                  setCurrentEditingRole(e.target.value)
                                }
                                aria-label="Select user role"
                                className="block w-auto p-1 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-sky-blue focus:border-sky-blue sm:text-sm"
                              >
                                <option value="admin">Admin</option>
                                <option value="user">User</option>
                              </select>
                              <button
                                onClick={() => handleSaveEdit(user.userId)}
                                className="text-green-600 hover:text-green-800 p-1"
                                title="Save"
                              >
                                <CheckIcon className="h-4 w-4" />
                              </button>
                              <button
                                onClick={handleCancelEdit}
                                className="text-gray-500 hover:text-gray-700 p-1"
                                title="Cancel"
                              >
                                <XIcon className="h-4 w-4" />
                              </button>
                            </>
                          ) : (
                            <>
                              <span className="capitalize">{user.role}</span>
                              {/* Only show edit role button if admin and not a system user */}
                              {currentUserRole === 'admin' && (
                                <button
                                  onClick={
                                    !isSystemUser
                                      ? () =>
                                          handleEditClick(
                                            user.userId,
                                            user.role
                                          )
                                      : undefined
                                  }
                                  className={
                                    !isSystemUser
                                      ? 'text-blue-600 hover:text-blue-700 p-1'
                                      : 'text-gray-300 cursor-not-allowed p-1'
                                  }
                                  title={
                                    !isSystemUser
                                      ? 'Edit role'
                                      : 'Cannot edit role for system users'
                                  }
                                  disabled={isSystemUser}
                                >
                                  <PenIcon className="h-3 w-3" />
                                </button>
                              )}
                            </>
                          )}
                        </div>
                      </TableCell>
                    )}
                    {!deduplicateUsers &&
                      !ingressBasicAuthEnabled &&
                      !externalProxyAuthEnabled && (
                        <TableCell className="truncate" title={user.userType}>
                          <span className="capitalize">
                            {user.userType === 'sso' ? 'SSO' : user.userType}
                          </span>
                        </TableCell>
                      )}
                    <TableCell className="truncate">
                      {user.created_at ? (
                        <TimestampWithTooltip
                          date={new Date(user.created_at * 1000)}
                        />
                      ) : (
                        '-'
                      )}
                    </TableCell>
                    <TableCell>
                      {user.gpuCount === -1 ? (
                        <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                          <CircularProgress size={12} />
                        </span>
                      ) : (
                        <span
                          className={`px-2 py-0.5 rounded text-xs font-medium ${
                            user.gpuCount > 0
                              ? 'bg-purple-100 text-purple-600'
                              : 'bg-gray-100 text-gray-500'
                          }`}
                          title={`Total GPUs: ${user.gpuCount}`}
                        >
                          {user.gpuCount}
                        </span>
                      )}
                    </TableCell>
                    <TableCell>
                      {user.clusterCount === -1 ? (
                        <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                          <CircularProgress size={12} />
                        </span>
                      ) : (
                        <Link
                          href={`/clusters?property=user&operator=%3A&value=${encodeURIComponent(user.username)}`}
                          className={`px-2 py-0.5 rounded text-xs font-medium transition-colors duration-200 cursor-pointer inline-block ${
                            user.clusterCount > 0
                              ? 'bg-blue-100 text-blue-600 hover:bg-blue-200 hover:text-blue-700'
                              : 'bg-gray-100 text-gray-500 hover:bg-gray-200 hover:text-gray-700'
                          }`}
                          title={`View ${user.clusterCount} cluster${user.clusterCount !== 1 ? 's' : ''} for ${user.usernameDisplay}`}
                        >
                          {user.clusterCount}
                        </Link>
                      )}
                    </TableCell>
                    <TableCell>
                      {user.jobCount === -1 ? (
                        <span className="px-2 py-0.5 bg-gray-100 text-gray-500 rounded text-xs font-medium">
                          <CircularProgress size={12} />
                        </span>
                      ) : (
                        <Link
                          href={`/jobs?property=user&operator=%3A&value=${encodeURIComponent(user.username)}`}
                          className={`px-2 py-0.5 rounded text-xs font-medium transition-colors duration-200 cursor-pointer inline-block ${
                            user.jobCount > 0
                              ? 'bg-green-100 text-green-600 hover:bg-green-200 hover:text-green-700'
                              : 'bg-gray-100 text-gray-500 hover:bg-gray-200 hover:text-gray-700'
                          }`}
                          title={`View ${user.jobCount} active job${user.jobCount !== 1 ? 's' : ''} for ${user.usernameDisplay}`}
                        >
                          {user.jobCount}
                        </Link>
                      )}
                    </TableCell>
                    {/* Actions cell logic - hide when deduplicating */}
                    {!deduplicateUsers &&
                      (basicAuthEnabled || currentUserRole === 'admin') && (
                        <TableCell className="relative">
                          <div className="flex items-center gap-2">
                            {/* Reset password icon: admin can reset any basic user, user can only reset self (basic auth only) */}
                            {basicAuthEnabled && (
                              <button
                                onClick={
                                  canResetPassword
                                    ? async () => {
                                        onResetPassword(user);
                                      }
                                    : undefined
                                }
                                className={
                                  canResetPassword
                                    ? 'text-blue-600 hover:text-blue-700 p-1'
                                    : 'text-gray-300 cursor-not-allowed p-1'
                                }
                                title={
                                  !isBasicUser
                                    ? 'Password reset only available for basic auth users'
                                    : canResetPassword
                                      ? 'Reset Password'
                                      : 'You can only reset your own password'
                                }
                                disabled={!canResetPassword}
                              >
                                <KeyRoundIcon className="h-4 w-4" />
                              </button>
                            )}
                            {/* Delete button - only show for admin, disabled for system users */}
                            {currentUserRole === 'admin' && (
                              <button
                                onClick={
                                  !isSystemUser
                                    ? () => onDeleteUser(user)
                                    : undefined
                                }
                                className={
                                  !isSystemUser
                                    ? 'text-red-600 hover:text-red-700 p-1'
                                    : 'text-gray-300 cursor-not-allowed p-1'
                                }
                                title={
                                  !isSystemUser
                                    ? 'Delete User'
                                    : 'Cannot delete system users'
                                }
                                disabled={isSystemUser}
                              >
                                <Trash2Icon className="h-4 w-4" />
                              </button>
                            )}
                          </div>
                        </TableCell>
                      )}
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
        </div>
      </Card>
      {bulkDialog === 'role' && (
        <BatchRoleDialog
          open={bulkDialog === 'role'}
          onClose={handleBulkDialogClose}
          selectedUsers={selectedUserObjects}
        />
      )}
      {bulkDialog === 'add' && (
        <BatchAddToWorkspacesDialog
          open={bulkDialog === 'add'}
          onClose={handleBulkDialogClose}
          selectedUsers={selectedUserObjects}
        />
      )}
      {bulkDialog === 'remove' && (
        <BatchRemoveFromWorkspacesDialog
          open={bulkDialog === 'remove'}
          onClose={handleBulkDialogClose}
          selectedUsers={selectedUserObjects}
        />
      )}
    </>
  );
}

UsersTable.propTypes = {
  refreshInterval: PropTypes.number.isRequired,
  setLoading: PropTypes.func.isRequired,
  refreshDataRef: PropTypes.shape({
    current: PropTypes.func,
  }).isRequired,
  checkPermissionAndAct: PropTypes.func.isRequired,
  roleLoading: PropTypes.bool.isRequired,
  onResetPassword: PropTypes.func.isRequired,
  onDeleteUser: PropTypes.func.isRequired,
  basicAuthEnabled: PropTypes.bool,
  ingressBasicAuthEnabled: PropTypes.bool,
  externalProxyAuthEnabled: PropTypes.bool,
  currentUserRole: PropTypes.string,
  currentUserId: PropTypes.string,
  setLastFetchedTime: PropTypes.func,
  setCreateError: PropTypes.func.isRequired,
};
