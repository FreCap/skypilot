'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { CircularProgress } from '@mui/material';
import { useRouter } from 'next/router';
import { Button } from '@/components/ui/button';
import { getUsers } from '@/data/connectors/users';
import dashboardCache from '@/lib/cache';
import { REFRESH_INTERVALS } from '@/lib/config';
import { LastUpdatedTimestamp } from '@/components/utils';
import {
  RotateCwIcon,
  EyeIcon,
  EyeOffIcon,
  UploadIcon,
  DownloadIcon,
  PlusIcon,
} from 'lucide-react';
import { Layout } from '@/components/elements/layout';
import { useMobile } from '@/hooks/useMobile';
import { useSidebar } from '@/components/elements/sidebar';
import { apiClient, getCurrentUserRole } from '@/data/connectors/client';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import { ErrorDisplay } from '@/components/elements/ErrorDisplay';
import { PluginSlot } from '@/plugins/PluginSlot';
import {
  FilterDropdown,
  Filters,
  updateURLParams as sharedUpdateURLParams,
  updateFiltersByURLParams as sharedUpdateFiltersByURLParams,
} from '@/components/shared/FilterSystem';
import { trackUserAction, trackFilterUsed } from '@/lib/analytics';
import { ServiceAccountTokensView } from '@/components/service-account-tokens';
import { UsersTable } from '@/components/users-table';

export { buildUsersWithUsage, UsersTable } from '@/components/users-table';
export { getJobGpuCount } from '@/components/user-usage';

// Define filter options for the filter dropdown
const PROPERTY_OPTIONS = [
  {
    label: 'Name',
    value: 'name',
  },
  {
    label: 'GPU',
    value: 'gpu type', // Match valueList key
  },
  {
    label: 'Infra',
    value: 'infra',
  },
  {
    label: 'User ID',
    value: 'user id', // Match valueList key
  },
  {
    label: 'Role',
    value: 'role',
  },
];

const REFRESH_INTERVAL = REFRESH_INTERVALS.REFRESH_INTERVAL;

// Success display component
const SuccessDisplay = ({ message, onDismiss }) => {
  if (!message) return null;

  return (
    <div className="bg-green-50 border border-green-200 rounded p-4 mb-6">
      <div className="flex items-center justify-between">
        <div className="flex items-center">
          <div className="flex-shrink-0">
            <svg
              className="h-5 w-5 text-green-400"
              viewBox="0 0 20 20"
              fill="currentColor"
            >
              <path
                fillRule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z"
                clipRule="evenodd"
              />
            </svg>
          </div>
          <div className="ml-3">
            <p className="text-sm font-medium text-green-800">{message}</p>
          </div>
        </div>
        {onDismiss && (
          <div className="ml-auto pl-3">
            <div className="-mx-1.5 -my-1.5">
              <button
                type="button"
                onClick={onDismiss}
                className="inline-flex rounded-md bg-green-50 p-1.5 text-green-500 hover:bg-green-100 focus:outline-none focus:ring-2 focus:ring-green-600 focus:ring-offset-2 focus:ring-offset-green-50"
              >
                <span className="sr-only">Dismiss</span>
                <svg
                  className="h-5 w-5"
                  viewBox="0 0 20 20"
                  fill="currentColor"
                >
                  <path
                    fillRule="evenodd"
                    d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z"
                    clipRule="evenodd"
                  />
                </svg>
              </button>
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export function Users() {
  const router = useRouter();
  const { userEmail } = useSidebar();
  const [loading, setLoading] = useState(false);
  const refreshDataRef = useRef(null);
  const isMobile = useMobile();
  const [showCreateUser, setShowCreateUser] = useState(false);
  const [newUser, setNewUser] = useState({
    username: '',
    password: '',
    role: 'user',
  });
  const [creating, setCreating] = useState(false);
  const [permissionDenialState, setPermissionDenialState] = useState({
    open: false,
    message: '',
    userName: '',
  });
  const [currentUser, setCurrentUser] = useState(null);
  const [roleLoading, setRoleLoading] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [showImportExportDialog, setShowImportExportDialog] = useState(false);
  const [csvFile, setCsvFile] = useState(null);
  const [importing, setImporting] = useState(false);
  const [importResults, setImportResults] = useState(null);
  const [activeTab, setActiveTab] = useState('import');
  const [showResetPasswordDialog, setShowResetPasswordDialog] = useState(false);
  const [resetPasswordUser, setResetPasswordUser] = useState(null);
  const [resetPassword, setResetPassword] = useState('');
  const [resetLoading, setResetLoading] = useState(false);
  const [resetError, setResetError] = useState(null);
  const [showDeleteConfirmDialog, setShowDeleteConfirmDialog] = useState(false);
  const [userToDelete, setUserToDelete] = useState(null);
  const [deleteError, setDeleteError] = useState(null);
  const [deleteLoading, setDeleteLoading] = useState(false);
  const [createSuccess, setCreateSuccess] = useState(null);
  const [createError, setCreateError] = useState(null);
  const [basicAuthEnabled, setBasicAuthEnabled] = useState(undefined);
  const [serviceAccountTokenEnabled, setServiceAccountTokenEnabled] =
    useState(undefined);
  const [ingressBasicAuthEnabled, setIngressBasicAuthEnabled] =
    useState(undefined);
  const [externalProxyAuthEnabled, setExternalProxyAuthEnabled] =
    useState(undefined);
  const [healthCheckLoading, setHealthCheckLoading] = useState(true);
  const [activeMainTab, setActiveMainTab] = useState('users');
  const [showCreateDialog, setShowCreateDialog] = useState(false);
  const [showRotateDialog, setShowRotateDialog] = useState(false);
  const [tokenToRotate, setTokenToRotate] = useState(null);
  const [rotating, setRotating] = useState(false);
  const [userSearchQuery, setUserSearchQuery] = useState('');
  const [serviceAccountSearchQuery, setServiceAccountSearchQuery] =
    useState('');
  const [filters, setFilters] = useState([]);
  const [valueList, setValueList] = useState({
    name: [],
    'user id': [],
    role: [],
    'gpu type': [],
    infra: [],
  });
  const [lastFetchedTime, setLastFetchedTime] = useState(null);

  // Initialize deduplicateUsers from URL parameter
  const getInitialDeduplicateUsers = () => {
    if (typeof window !== 'undefined' && router.isReady) {
      const deduplicateParam = router.query.deduplicate;
      // If parameter is explicitly set, use it; otherwise default to true
      if (deduplicateParam !== undefined) {
        return deduplicateParam === 'true';
      }
    }
    return true; // Default to deduplicated view
  };

  const [deduplicateUsers, setDeduplicateUsers] = useState(
    getInitialDeduplicateUsers
  );

  // Sync deduplicateUsers state with URL parameter
  useEffect(() => {
    if (router.isReady) {
      const deduplicateParam = router.query.deduplicate;

      // If URL has no deduplicate parameter, set it to the default
      // Default to false for SSO (userEmail exists), true for non-SSO
      if (deduplicateParam === undefined) {
        const defaultValue = !userEmail; // false for SSO, true for non-SSO
        updateDeduplicateURL(defaultValue);
      } else {
        const expectedState = deduplicateParam === 'true';
        if (deduplicateUsers !== expectedState) {
          setDeduplicateUsers(expectedState);
        }
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [router.isReady, router.query.deduplicate, userEmail]);

  // Helper function to update deduplicate in URL
  const updateDeduplicateURL = (deduplicateValue) => {
    const query = { ...router.query };
    query.deduplicate = deduplicateValue.toString();

    // Use replace to avoid adding to browser history
    router.replace(
      {
        pathname: router.pathname,
        query,
      },
      undefined,
      { shallow: true }
    );
  };

  // Helper function to update URL query parameters for filters
  const updateURLParams = (filters) => {
    sharedUpdateURLParams(router, filters);
  };

  // Create property map for filter URL parameters
  const propertyMap = new Map([
    ['name', 'Name'],
    ['user id', 'User ID'], // Note: lowercase with space to match URL encoding
    ['role', 'Role'],
    ['gpu type', 'GPU'], // Note: lowercase with space to match URL encoding
    ['infra', 'Infra'],
  ]);

  // Initialize filters from URL parameters
  useEffect(() => {
    if (router.isReady && activeMainTab === 'users') {
      const urlFilters = sharedUpdateFiltersByURLParams(router, propertyMap);
      if (urlFilters.length > 0) {
        setFilters(urlFilters);
      }
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [router.isReady, activeMainTab]);

  // Handle URL parameters for tab selection
  useEffect(() => {
    if (router.isReady) {
      const tab = router.query.tab;
      if (tab === 'service-accounts' && serviceAccountTokenEnabled) {
        setActiveMainTab('service-accounts');
      } else if (tab && tab !== 'users') {
        setActiveMainTab(tab); // plugin-managed tab
      } else {
        setActiveMainTab('users');
      }
    }
  }, [router.isReady, router.query.tab, serviceAccountTokenEnabled]);

  useEffect(() => {
    async function fetchHealth() {
      setHealthCheckLoading(true);
      try {
        const resp = await apiClient.get('/api/health');
        if (resp.ok) {
          const data = await resp.json();
          setBasicAuthEnabled(!!data.basic_auth_enabled);
          setServiceAccountTokenEnabled(!!data.service_account_token_enabled);
          setIngressBasicAuthEnabled(!!data.ingress_basic_auth_enabled);
          setExternalProxyAuthEnabled(!!data.external_proxy_auth_enabled);
        } else {
          setBasicAuthEnabled(false);
          setServiceAccountTokenEnabled(false);
          setIngressBasicAuthEnabled(false);
          setExternalProxyAuthEnabled(false);
        }
      } catch {
        setBasicAuthEnabled(false);
        setServiceAccountTokenEnabled(false);
        setIngressBasicAuthEnabled(false);
        setExternalProxyAuthEnabled(false);
      } finally {
        setHealthCheckLoading(false);
      }
    }
    fetchHealth();
  }, []);

  const getUserRole = useCallback(async () => {
    setRoleLoading(true);
    try {
      const roleData = await getCurrentUserRole();
      if (roleData.roleFetchFailed) {
        throw new Error('Failed to get user role');
      }
      setCurrentUser(roleData);
      return roleData;
    } finally {
      setRoleLoading(false);
    }
  }, []);

  useEffect(() => {
    getUserRole().catch(() => {
      console.error('Failed to get user role');
    });
  }, [getUserRole]);

  const checkPermissionAndAct = async (action, actionCallback) => {
    try {
      const roleData = await getUserRole();

      if (roleData.role !== 'admin') {
        setPermissionDenialState({
          open: true,
          message: action,
          userName: roleData.name.toLowerCase(),
        });
        return false;
      }

      actionCallback();
      return true;
    } catch (error) {
      console.error('Failed to check user role:', error);
      setPermissionDenialState({
        open: true,
        message: `Error: ${error.message}`,
        userName: '',
      });
      return false;
    }
  };

  const handleRefresh = () => {
    trackUserAction('refresh');
    dashboardCache.invalidate(getUsers);
    dashboardCache.invalidate(getClusters);
    dashboardCache.invalidate(getManagedJobs, [
      { allUsers: true, skipFinished: true },
    ]);

    if (refreshDataRef.current) {
      refreshDataRef.current();
    }
  };

  // Effect for keyboard shortcut (Cmd+R / Ctrl+R) to trigger in-page refresh
  useEffect(() => {
    const handleKeyDown = (event) => {
      // Check for Cmd+R (Mac) or Ctrl+R (Windows/Linux)
      if ((event.metaKey || event.ctrlKey) && event.key === 'r') {
        event.preventDefault(); // Prevent browser refresh
        event.stopPropagation(); // Stop event from bubbling
        handleRefresh(); // Trigger our in-page refresh
      }
    };

    // Use capture: true to intercept the event before browser handles it
    document.addEventListener('keydown', handleKeyDown, true);

    return () => {
      document.removeEventListener('keydown', handleKeyDown, true);
    };
  }, []);

  const handleCreateUser = async () => {
    if (!newUser.username || !newUser.password) {
      setCreateError(new Error('Username and password are required.'));
      setShowCreateUser(false);
      return;
    }
    trackUserAction('create');
    setCreating(true);
    setCreateError(null);
    setCreateSuccess(null);
    try {
      const response = await apiClient.post('/users/create', newUser);
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to create user');
      }
      setCreateSuccess(`User "${newUser.username}" created successfully!`);
      setShowCreateUser(false);
      setNewUser({ username: '', password: '', role: 'user' });
      handleRefresh();
    } catch (error) {
      setCreateError(error);
      setShowCreateUser(false);
      setNewUser({ username: '', password: '', role: 'user' });
    } finally {
      setCreating(false);
    }
  };

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    setCsvFile(file);
    setImportResults(null);
  };

  const handleImportUsers = async () => {
    if (!csvFile) {
      alert('Please select a CSV file first.');
      return;
    }

    setImporting(true);
    try {
      const reader = new FileReader();
      reader.onload = async (e) => {
        try {
          const csvContent = e.target.result;
          const response = await apiClient.post('/users/import', {
            csv_content: csvContent,
          });

          if (!response.ok) {
            const errorData = await response.json();
            throw new Error(errorData.detail || 'Failed to import users');
          }

          const results = await response.json();

          // Create user-friendly message
          let message = `Import completed. ${results.success_count} users created successfully.`;
          if (results.error_count > 0) {
            message += `\n${results.error_count} failed.`;
            if (results.creation_errors.length > 0) {
              message += `\nErrors: ${results.creation_errors.slice(0, 3).join(', ')}`;
              if (results.creation_errors.length > 3) {
                message += ` and ${results.creation_errors.length - 3} more...`;
              }
            }
          }

          setImportResults({ message });
          if (results.success_count > 0) {
            handleRefresh();
          }
        } catch (error) {
          alert(`Error importing users: ${error.message}`);
        } finally {
          setImporting(false);
        }
      };
      reader.readAsText(csvFile);
    } catch (error) {
      alert(`Error reading file: ${error.message}`);
      setImporting(false);
    }
  };

  const handleResetPasswordClick = async (user) => {
    setResetPasswordUser(user);
    setResetPassword('');
    setShowResetPasswordDialog(true);
  };

  const handleResetPasswordSubmit = async () => {
    if (!resetPassword) {
      setCreateError(new Error('Please enter a new password.'));
      return;
    }
    setResetLoading(true);
    setResetError(null);
    try {
      const response = await apiClient.post('/users/update', {
        user_id: resetPasswordUser.userId,
        password: resetPassword,
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to reset password');
      }
      setCreateSuccess(
        `Password reset successfully for user "${resetPasswordUser.usernameDisplay}"!`
      );
      setShowResetPasswordDialog(false);
      setResetPasswordUser(null);
      setResetPassword('');
    } catch (error) {
      // Show error at top level for better visibility
      setShowResetPasswordDialog(false);
      setResetPasswordUser(null);
      setResetPassword('');
      setResetError(null);
      setCreateError(error);
    } finally {
      setResetLoading(false);
    }
  };

  const handleDeleteUserClick = (user) => {
    trackUserAction('delete');
    checkPermissionAndAct('cannot delete users', () => {
      setUserToDelete(user);
      setShowDeleteConfirmDialog(true);
    });
  };

  const handleDeleteUserConfirm = async () => {
    if (!userToDelete) return;
    setDeleteLoading(true);
    setDeleteError(null);
    try {
      const response = await apiClient.post('/users/delete', {
        user_id: userToDelete.userId,
      });
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to delete user');
      }
      setCreateSuccess(
        `User "${userToDelete.usernameDisplay}" deleted successfully!`
      );
      setShowDeleteConfirmDialog(false);
      setUserToDelete(null);
      handleRefresh();
    } catch (error) {
      // Show error at top level for better visibility
      setShowDeleteConfirmDialog(false);
      setUserToDelete(null);
      setDeleteError(null);
      setCreateError(error);
    } finally {
      setDeleteLoading(false);
    }
  };

  const handleCancelDelete = () => {
    setShowDeleteConfirmDialog(false);
    setUserToDelete(null);
  };

  const handleCancelResetPassword = () => {
    setShowResetPasswordDialog(false);
    setResetPasswordUser(null);
    setResetPassword('');
  };

  // Show loading while fetching health check
  const handleTabChange = useCallback(
    (tab) => {
      trackUserAction('tab_change', { tab });
      setActiveMainTab(tab);
      if (tab === 'users') {
        router.push('/users', undefined, { shallow: true });
      } else {
        router.push(`/users?tab=${tab}`, undefined, { shallow: true });
      }
    },
    [router]
  );

  if (healthCheckLoading) {
    return (
      <div className="flex justify-center items-center h-64">
        <CircularProgress />
        <span className="ml-2 text-gray-500">Loading...</span>
      </div>
    );
  }

  return (
    <>
      {/* Main Tabs with Controls */}
      <div className="flex items-center justify-between mb-2">
        <div className="text-base flex items-center">
          <button
            className={`leading-none mr-6 pb-2 px-2 border-b-2 ${
              activeMainTab === 'users'
                ? 'text-sky-blue border-sky-500'
                : 'text-gray-500 hover:text-gray-700 border-transparent'
            }`}
            onClick={() => handleTabChange('users')}
          >
            Users
          </button>
          {serviceAccountTokenEnabled && (
            <button
              className={`leading-none mr-6 pb-2 px-2 border-b-2 ${
                activeMainTab === 'service-accounts'
                  ? 'text-sky-blue border-sky-500'
                  : 'text-gray-500 hover:text-gray-700 border-transparent'
              }`}
              onClick={() => handleTabChange('service-accounts')}
            >
              Service Accounts
            </button>
          )}
          <PluginSlot
            name="users.tabs"
            context={{ activeTab: activeMainTab, onTabChange: handleTabChange }}
            wrapperClassName="contents"
          />
        </div>

        <div className="flex items-center">
          {loading && (
            <div className="flex items-center mr-2">
              <CircularProgress size={15} className="mt-0" />
              <span className="ml-2 text-gray-500">Loading...</span>
            </div>
          )}
          {activeMainTab === 'users' &&
            basicAuthEnabled &&
            currentUser?.role === 'admin' && (
              <button
                onClick={async () => {
                  await checkPermissionAndAct('cannot create users', () => {
                    setShowCreateUser(true);
                  });
                }}
                className="text-sky-blue hover:text-sky-blue-bright flex items-center rounded px-2 py-1 mr-2"
                title="Create New User"
              >
                + New User
              </button>
            )}
          {activeMainTab === 'users' &&
            basicAuthEnabled &&
            currentUser?.role === 'admin' && (
              <button
                onClick={async () => {
                  await checkPermissionAndAct('cannot import users', () => {
                    setShowImportExportDialog(true);
                  });
                }}
                className="text-sky-blue hover:text-sky-blue-bright flex items-center rounded px-2 py-1 mr-2"
                title="Import/Export Users"
              >
                <UploadIcon className="h-4 w-4 mr-1" />
                Import/Export
              </button>
            )}

          {!loading && lastFetchedTime && (
            <LastUpdatedTimestamp
              timestamp={lastFetchedTime}
              className="mr-2"
            />
          )}
          <button
            onClick={handleRefresh}
            disabled={loading}
            className="text-sky-blue hover:text-sky-blue-bright flex items-center"
          >
            <RotateCwIcon className="h-4 w-4 mr-1.5" />
            {!isMobile && <span>Refresh</span>}
          </button>
        </div>
      </div>

      {/* Filter/Search and Create Service Account Row */}
      <div className="flex items-center justify-between mb-4">
        {activeMainTab === 'users' ? (
          <div className="w-full sm:w-auto max-w-xl">
            <FilterDropdown
              propertyList={PROPERTY_OPTIONS}
              valueList={valueList}
              setFilters={setFilters}
              updateURLParams={updateURLParams}
              onFilterAdd={(property, value) =>
                trackFilterUsed('user', { property, value })
              }
              placeholder="Filter users"
            />
          </div>
        ) : activeMainTab === 'service-accounts' ? (
          <div className="relative flex-1 max-w-md">
            <input
              type="text"
              placeholder="Search by service account name, or created by"
              value={serviceAccountSearchQuery}
              onChange={(e) => {
                setServiceAccountSearchQuery(e.target.value);
              }}
              className="h-8 w-full px-3 pr-8 text-sm border border-gray-300 rounded-md focus:ring-1 focus:ring-sky-500 focus:border-sky-500 outline-none"
            />
            {serviceAccountSearchQuery && (
              <button
                onClick={() => {
                  setServiceAccountSearchQuery('');
                }}
                className="absolute right-2 top-1/2 transform -translate-y-1/2 text-gray-400 hover:text-gray-600"
                title="Clear search"
              >
                <svg
                  className="h-4 w-4"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M6 18L18 6M6 6l12 12"
                  />
                </svg>
              </button>
            )}
          </div>
        ) : (
          <PluginSlot
            name="users.tab-filter"
            context={{ activeTab: activeMainTab }}
            wrapperClassName="contents"
          />
        )}

        {/* Deduplicate Users Toggle - only show on users tab when NOT using SSO/OAuth2 */}
        {activeMainTab === 'users' && !userEmail && (
          <label className="flex items-center cursor-pointer ml-4">
            <input
              type="checkbox"
              checked={deduplicateUsers}
              onChange={(e) => {
                const newValue = e.target.checked;
                setDeduplicateUsers(newValue);
                updateDeduplicateURL(newValue);
              }}
              className="sr-only"
            />
            <div
              className={`relative inline-flex h-5 w-9 items-center rounded-full transition-colors ${
                deduplicateUsers ? 'bg-sky-600' : 'bg-gray-300'
              }`}
            >
              <span
                className={`inline-block h-3 w-3 transform rounded-full bg-white transition-transform ${
                  deduplicateUsers ? 'translate-x-5' : 'translate-x-1'
                }`}
              />
            </div>
            <span className="ml-2 text-sm text-gray-700">
              Deduplicate users
            </span>
          </label>
        )}

        {/* Plugin actions slot for users tab */}
        {activeMainTab === 'users' && <PluginSlot name="users.actions" />}

        {/* Create Service Account Button for Service Accounts Tab */}
        {activeMainTab === 'service-accounts' && serviceAccountTokenEnabled && (
          <button
            onClick={() => {
              checkPermissionAndAct(
                'cannot create service account tokens',
                () => {
                  setShowCreateDialog(true);
                }
              );
            }}
            className="ml-4 bg-sky-600 hover:bg-sky-700 text-white flex items-center rounded-md px-3 py-1 text-sm font-medium transition-colors duration-200"
            title="Create Service Account"
          >
            <PlusIcon className="h-4 w-4 mr-2" />
            Create Service Account
          </button>
        )}
      </div>

      {/* Display Active Filters - only for users tab */}
      {activeMainTab === 'users' && (
        <Filters
          filters={filters}
          setFilters={setFilters}
          updateURLParams={updateURLParams}
        />
      )}

      {/* Error/Success messages positioned at top right, below navigation bar */}
      <div className="fixed top-20 right-4 z-[9999] max-w-md">
        <SuccessDisplay
          message={createSuccess}
          onDismiss={() => setCreateSuccess(null)}
        />
        <ErrorDisplay
          error={createError}
          title="Error"
          onDismiss={() => setCreateError(null)}
        />
      </div>

      {activeMainTab === 'users' ? (
        <UsersTable
          refreshInterval={REFRESH_INTERVAL}
          setLoading={setLoading}
          refreshDataRef={refreshDataRef}
          checkPermissionAndAct={checkPermissionAndAct}
          roleLoading={roleLoading}
          onResetPassword={handleResetPasswordClick}
          onDeleteUser={handleDeleteUserClick}
          basicAuthEnabled={basicAuthEnabled}
          ingressBasicAuthEnabled={ingressBasicAuthEnabled}
          externalProxyAuthEnabled={externalProxyAuthEnabled}
          currentUserRole={currentUser?.role}
          currentUserId={currentUser?.id}
          filters={filters}
          setValueList={setValueList}
          deduplicateUsers={deduplicateUsers}
          setLastFetchedTime={setLastFetchedTime}
          setCreateError={setCreateError}
        />
      ) : activeMainTab === 'service-accounts' ? (
        serviceAccountTokenEnabled && (
          <ServiceAccountTokensView
            checkPermissionAndAct={checkPermissionAndAct}
            userRoleCache={currentUser}
            setCreateSuccess={setCreateSuccess}
            setCreateError={setCreateError}
            showCreateDialog={showCreateDialog}
            setShowCreateDialog={setShowCreateDialog}
            showRotateDialog={showRotateDialog}
            setShowRotateDialog={setShowRotateDialog}
            tokenToRotate={tokenToRotate}
            setTokenToRotate={setTokenToRotate}
            rotating={rotating}
            setRotating={setRotating}
            searchQuery={serviceAccountSearchQuery}
            setSearchQuery={setServiceAccountSearchQuery}
          />
        )
      ) : (
        <PluginSlot
          name="users.tab-content"
          context={{ activeTab: activeMainTab }}
        />
      )}

      {/* Create User Dialog */}
      <Dialog
        open={showCreateUser}
        onOpenChange={(open) => {
          setShowCreateUser(open);
          if (!open) {
            setCreateError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Create User</DialogTitle>
          </DialogHeader>
          <div className="flex flex-col gap-4 py-4">
            <div className="grid gap-2">
              <label className="text-sm font-medium text-gray-700">
                Username
              </label>
              <input
                className="border rounded px-3 py-2 w-full"
                placeholder="Username"
                value={newUser.username}
                onChange={(e) =>
                  setNewUser({ ...newUser, username: e.target.value })
                }
              />
            </div>
            <div className="grid gap-2">
              <label className="text-sm font-medium text-gray-700">
                Password
              </label>
              <div className="relative">
                <input
                  className="border rounded px-3 py-2 w-full pr-10"
                  placeholder="Password"
                  type={showPassword ? 'text' : 'password'}
                  value={newUser.password}
                  onChange={(e) =>
                    setNewUser({ ...newUser, password: e.target.value })
                  }
                />
                <button
                  type="button"
                  className="absolute inset-y-0 right-0 pr-3 flex items-center text-gray-400 hover:text-gray-600"
                  onClick={() => setShowPassword(!showPassword)}
                >
                  {showPassword ? (
                    <EyeOffIcon className="h-4 w-4" />
                  ) : (
                    <EyeIcon className="h-4 w-4" />
                  )}
                </button>
              </div>
            </div>
            <div className="grid gap-2">
              <label className="text-sm font-medium text-gray-700">Role</label>
              <select
                className="border rounded px-3 py-2 w-full"
                value={newUser.role}
                onChange={(e) =>
                  setNewUser({ ...newUser, role: e.target.value })
                }
              >
                <option value="user">User</option>
                <option value="admin">Admin</option>
              </select>
            </div>
          </div>
          <DialogFooter>
            <button
              className="inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-10 px-4 py-2"
              onClick={() => setShowCreateUser(false)}
              disabled={creating}
            >
              Cancel
            </button>
            <button
              className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-sky-600 text-white hover:bg-sky-700 h-10 px-4 py-2"
              onClick={handleCreateUser}
              disabled={creating}
            >
              {creating ? 'Creating...' : 'Create'}
            </button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={permissionDenialState.open}
        onOpenChange={(open) => {
          setPermissionDenialState((prev) => ({ ...prev, open }));
          if (!open) {
            setCreateError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-md transition-all duration-200 ease-in-out">
          <DialogHeader>
            <DialogTitle>Permission Denied</DialogTitle>
            <DialogDescription>
              {roleLoading ? (
                <div className="flex items-center py-2">
                  <CircularProgress size={16} className="mr-2" />
                  <span>Checking permissions...</span>
                </div>
              ) : (
                <>
                  {permissionDenialState.userName ? (
                    <>
                      {permissionDenialState.userName} is logged in as non-admin
                      and {permissionDenialState.message}.
                    </>
                  ) : (
                    permissionDenialState.message
                  )}
                </>
              )}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() =>
                setPermissionDenialState((prev) => ({ ...prev, open: false }))
              }
              disabled={roleLoading}
            >
              OK
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Import/Export Users Dialog */}
      <Dialog
        open={showImportExportDialog}
        onOpenChange={(open) => {
          setShowImportExportDialog(open);
          if (!open) {
            setCreateError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle>Import/Export Users</DialogTitle>
          </DialogHeader>

          {/* Tabs */}
          <div className="flex border-b border-gray-200 mb-4">
            <button
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === 'import'
                  ? 'border-b-2 border-sky-500 text-sky-600'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
              onClick={() => setActiveTab('import')}
            >
              Import
            </button>
            <button
              className={`px-4 py-2 text-sm font-medium ${
                activeTab === 'export'
                  ? 'border-b-2 border-sky-500 text-sky-600'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
              onClick={() => setActiveTab('export')}
            >
              Export
            </button>
          </div>

          <div className="flex flex-col gap-4 py-4">
            {activeTab === 'import' ? (
              <>
                <div className="grid gap-2">
                  <label className="text-sm font-medium text-gray-700">
                    CSV File
                  </label>
                  <input
                    type="file"
                    accept=".csv"
                    onChange={handleFileUpload}
                    className="border rounded px-3 py-2 w-full"
                  />
                  <p className="text-xs text-gray-500">
                    CSV should have columns: username, password, role
                    <br />
                    Supports both plain text passwords and exported password
                    hashes.
                  </p>
                </div>

                {importResults && (
                  <div className="p-3 bg-green-50 border border-green-200 rounded text-green-700 text-sm">
                    {importResults.message}
                  </div>
                )}
              </>
            ) : (
              <>
                <div className="grid gap-2">
                  <label className="text-sm font-medium text-gray-700">
                    Export Users to CSV
                  </label>
                  <p className="text-xs text-gray-500">
                    Download all users as a CSV file with password hashes.
                  </p>
                  <div className="p-3 bg-amber-50 border border-amber-200 rounded">
                    <p className="text-sm text-amber-700">
                      ⚠️ This will export all users with columns: username,
                      password (hashed), role
                    </p>
                    <p className="text-xs text-amber-600 mt-1">
                      Password hashes can be imported directly for system
                      backups.
                    </p>
                  </div>
                </div>
              </>
            )}
          </div>

          <DialogFooter>
            <button
              className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-10 px-4 py-2"
              onClick={() => setShowImportExportDialog(false)}
              disabled={importing}
            >
              Cancel
            </button>
            {activeTab === 'import' ? (
              <button
                className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-sky-600 text-white hover:bg-sky-700 h-10 px-4 py-2"
                onClick={handleImportUsers}
                disabled={importing || !csvFile}
              >
                {importing ? 'Importing...' : 'Import'}
              </button>
            ) : (
              <button
                className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-sky-600 text-white hover:bg-sky-700 h-10 px-4 py-2"
                onClick={async () => {
                  try {
                    const response = await apiClient.get('/users/export');
                    if (!response.ok) {
                      const errorData = await response.json();
                      throw new Error(
                        errorData.detail || 'Failed to export users'
                      );
                    }

                    const data = await response.json();
                    const csvContent = data.csv_content;

                    // Download the CSV file
                    const blob = new Blob([csvContent], {
                      type: 'text/csv;charset=utf-8;',
                    });
                    const url = URL.createObjectURL(blob);
                    const link = document.createElement('a');
                    link.href = url;
                    const now = new Date();
                    const pad = (n) => String(n).padStart(2, '0');
                    const y = now.getFullYear();
                    const m = pad(now.getMonth() + 1);
                    const d = pad(now.getDate());
                    const h = pad(now.getHours());
                    const min = pad(now.getMinutes());
                    const s = pad(now.getSeconds());
                    link.download = `users_export_${y}-${m}-${d}-${h}-${min}-${s}.csv`;
                    link.click();
                    URL.revokeObjectURL(url);
                    trackUserAction('export_csv', {
                      user_count: data.user_count,
                      filename: link.download,
                    });

                    // Show success message
                    alert(
                      `Successfully exported ${data.user_count} users to CSV file.`
                    );
                  } catch (error) {
                    alert(`Error exporting users: ${error.message}`);
                  }
                }}
              >
                <DownloadIcon className="h-4 w-4 mr-1" />
                Export
              </button>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Reset Password Dialog */}
      <Dialog
        open={showResetPasswordDialog}
        onOpenChange={(open) => {
          if (open) return;
          handleCancelResetPassword();
          setCreateError(null);
        }}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Reset Password</DialogTitle>
            <DialogDescription>
              Enter a new password for{' '}
              {resetPasswordUser?.usernameDisplay || 'this user'}.
            </DialogDescription>
          </DialogHeader>

          <div className="flex flex-col gap-4 py-4">
            <div className="grid gap-2">
              <label className="text-sm font-medium text-gray-700">
                New Password
              </label>
              <input
                type="password"
                className="border rounded px-3 py-2 w-full"
                placeholder="Enter new password"
                value={resetPassword}
                onChange={(e) => setResetPassword(e.target.value)}
                autoFocus
              />
            </div>
          </div>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={handleCancelResetPassword}
              disabled={resetLoading}
            >
              Cancel
            </Button>
            <Button
              variant="default"
              onClick={handleResetPasswordSubmit}
              disabled={resetLoading || !resetPassword}
              className="bg-sky-600 text-white hover:bg-sky-700"
            >
              {resetLoading ? 'Resetting...' : 'Reset Password'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete User Confirmation Dialog */}
      <Dialog
        open={showDeleteConfirmDialog}
        onOpenChange={(open) => {
          if (open) return;
          handleCancelDelete();
          setCreateError(null);
        }}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Delete User</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete user &quot;
              {userToDelete?.usernameDisplay || 'this user'}&quot;? This action
              cannot be undone.
            </DialogDescription>
          </DialogHeader>

          <DialogFooter>
            <Button
              variant="outline"
              onClick={handleCancelDelete}
              disabled={deleteLoading}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={handleDeleteUserConfirm}
              disabled={deleteLoading}
            >
              {deleteLoading ? 'Deleting...' : 'Delete'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
