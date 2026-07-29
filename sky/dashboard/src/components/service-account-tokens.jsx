'use client';

import React, { useState, useEffect, useCallback, useRef } from 'react';
import { CircularProgress } from '@mui/material';
import Link from 'next/link';
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from '@/components/ui/table';
import {
  getServiceAccountTokens,
  getServiceAccountTokensPaginated,
  isServiceAccountTokensPaginationAvailable,
} from '@/data/connectors/users';
import dashboardCache from '@/lib/cache';
import {
  CustomTooltip,
  TimestampWithTooltip,
  CustomTooltip as Tooltip,
} from '@/components/utils';
import {
  RotateCwIcon,
  PenIcon,
  CheckIcon,
  XIcon,
  KeyRoundIcon,
  Trash2Icon,
  CopyIcon,
} from 'lucide-react';
import { Card } from '@/components/ui/card';
import { apiClient } from '@/data/connectors/client';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from '@/components/ui/dialog';
import {
  aggregateUserUsage,
  fetchClustersAndJobs,
} from '@/components/user-usage';

// Service Account Tokens Management Component
export function ServiceAccountTokensView({
  checkPermissionAndAct,
  userRoleCache,
  setCreateSuccess,
  setCreateError,
  showCreateDialog,
  setShowCreateDialog,
  showRotateDialog,
  setShowRotateDialog,
  tokenToRotate,
  setTokenToRotate,
  rotating,
  setRotating,
  searchQuery,
  setSearchQuery,
}) {
  const [loading, setLoading] = useState(true);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [tokenToDelete, setTokenToDelete] = useState(null);
  const [deleteError, setDeleteError] = useState(null);
  const [newToken, setNewToken] = useState({
    token_name: '',
    expires_in_days: 30,
  });
  const [rotateExpiration, setRotateExpiration] = useState('');
  const [creating, setCreating] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [copySuccess, setCopySuccess] = useState('');

  // Add new state for tokens displayed within dialogs
  const [createdTokenInDialog, setCreatedTokenInDialog] = useState(null);
  const [rotatedTokenInDialog, setRotatedTokenInDialog] = useState(null);

  // Role editing state
  const [editingTokenId, setEditingTokenId] = useState(null);
  const [currentEditingRole, setCurrentEditingRole] = useState('');

  // Enhanced tokens with cluster/job counts
  const [tokensWithCounts, setTokensWithCounts] = useState([]);
  const refreshIdRef = useRef(0);
  const mountedRef = useRef(false);

  // Server-side pagination state (used only when the pagination plugin
  // exposes window.__skyServiceAccountTokensPaginationFetch). Keep the
  // initial server render and the first client render aligned by resolving
  // the mode in an effect, then start the first fetch only after that mode
  // is known.
  const [serverPaginated, setServerPaginated] = useState(null);
  useEffect(() => {
    setServerPaginated(isServiceAccountTokensPaginationAvailable());
  }, []);
  const [page, setPage] = useState(1);
  const [limit, setLimit] = useState(20);
  const [total, setTotal] = useState(0);
  const [totalPages, setTotalPages] = useState(1);
  const [hasNext, setHasNext] = useState(false);
  const [hasPrev, setHasPrev] = useState(false);
  // Debounce search input to avoid hammering the server on every keystroke.
  const [debouncedSearch, setDebouncedSearch] = useState(searchQuery || '');
  useEffect(() => {
    if (!serverPaginated) return undefined;
    const t = setTimeout(() => setDebouncedSearch(searchQuery || ''), 250);
    return () => clearTimeout(t);
  }, [searchQuery, serverPaginated]);
  // Reset to first page whenever the active search changes.
  useEffect(() => {
    if (!serverPaginated) return;
    setPage(1);
  }, [debouncedSearch, serverPaginated]);

  // Fetch tokens and related data
  const fetchTokensAndCounts = useCallback(
    async (forceRefresh = false) => {
      if (serverPaginated === null) {
        return;
      }
      const refreshId = ++refreshIdRef.current;
      const ownsRefresh = () => refreshId === refreshIdRef.current;
      try {
        setLoading(true);

        if (serverPaginated) {
          // Server-paginated path: skip client-side cluster/job count
          // fan-out. Counts are deliberately omitted here because
          // computing them requires loading all clusters + jobs across
          // all SAs, which is the bottleneck this pagination flow
          // exists to avoid. Counts will be surfaced per-row via
          // dedicated drill-ins later.
          if (forceRefresh) {
            dashboardCache.invalidate(getServiceAccountTokensPaginated);
          }
          const resp = await dashboardCache.get(
            getServiceAccountTokensPaginated,
            [
              {
                page,
                limit,
                search: debouncedSearch,
                sortBy: 'created_at',
                sortOrder: 'desc',
              },
            ]
          );
          if (!ownsRefresh()) return;
          const items = resp.items || [];
          setTotal(resp.total ?? items.length);
          setTotalPages(resp.total_pages ?? resp.totalPages ?? 1);
          setHasNext(resp.has_next ?? resp.hasNext ?? false);
          setHasPrev(resp.has_prev ?? resp.hasPrev ?? false);
          const enhanced = items.map((token) => ({
            ...token,
            clusterCount: undefined,
            jobCount: undefined,
            gpuCount: undefined,
            primaryRole:
              token.service_account_roles &&
              token.service_account_roles.length > 0
                ? token.service_account_roles[0]
                : 'user',
          }));
          setTokensWithCounts(enhanced);
          return;
        }

        // Invalidate cache if force refresh requested (after mutations)
        if (forceRefresh) {
          dashboardCache.invalidate(getServiceAccountTokens);
        }

        const [tokensData, { clustersData, jobsResponse }] = await Promise.all([
          dashboardCache.get(getServiceAccountTokens),
          fetchClustersAndJobs(),
        ]);
        if (!ownsRefresh()) return;
        const jobsData = jobsResponse?.jobs || [];
        const usageByUser = aggregateUserUsage(clustersData, jobsData);

        const enhancedTokens = (tokensData || []).map((token) => {
          const usage = usageByUser.get(token.service_account_user_id) || {
            clusterCount: 0,
            jobCount: 0,
            gpuCount: 0,
          };

          return {
            ...token,
            ...usage,
            // Extract primary role
            primaryRole:
              token.service_account_roles &&
              token.service_account_roles.length > 0
                ? token.service_account_roles[0]
                : 'user',
          };
        });

        setTokensWithCounts(enhancedTokens);
      } catch (error) {
        if (!ownsRefresh()) return;
        console.error('Error fetching tokens and counts:', error);
        setTokensWithCounts([]);
      } finally {
        if (ownsRefresh()) setLoading(false);
      }
    },
    [page, limit, debouncedSearch, serverPaginated]
  );
  const fetchTokensAndCountsRef = useRef(fetchTokensAndCounts);
  fetchTokensAndCountsRef.current = fetchTokensAndCounts;

  const refreshAfterMutation = useCallback(async () => {
    if (!mountedRef.current) return;
    await fetchTokensAndCountsRef.current(true);
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    if (serverPaginated !== null) {
      fetchTokensAndCounts();
    }
    return () => {
      mountedRef.current = false;
      refreshIdRef.current += 1;
    };
  }, [fetchTokensAndCounts, serverPaginated]);

  // Role editing functions
  const handleEditClick = async (tokenId, currentRole) => {
    await checkPermissionAndAct('cannot edit service account role', () => {
      setEditingTokenId(tokenId);
      setCurrentEditingRole(currentRole);
    });
  };

  const handleCancelEdit = () => {
    setEditingTokenId(null);
    setCurrentEditingRole('');
  };

  const handleSaveEdit = async (tokenId) => {
    if (!tokenId || !currentEditingRole) {
      console.error('Token ID or role is missing.');
      setCreateError(new Error('Token ID or role is missing.'));
      return;
    }

    const loadingId = ++refreshIdRef.current;
    setLoading(true);
    try {
      const response = await apiClient.post(
        '/users/service-account-tokens/update-role',
        {
          token_id: tokenId,
          role: currentEditingRole,
        }
      );

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to update role');
      }

      setCreateSuccess('Service account role updated successfully!');
      await refreshAfterMutation();
      handleCancelEdit(); // Exit edit mode
    } catch (error) {
      console.error('Failed to update service account role:', error);
      setCreateError(error);
    } finally {
      if (loadingId === refreshIdRef.current) setLoading(false);
    }
  };

  // Copy to clipboard
  const copyToClipboard = async (text) => {
    try {
      await navigator.clipboard.writeText(text);
      setCopySuccess('Copied!');
      setTimeout(() => setCopySuccess(''), 2000);
    } catch (err) {
      console.error('Failed to copy:', err);
    }
  };

  // Handle create token
  const handleCreateToken = async () => {
    if (!newToken.token_name.trim()) {
      setCreateError(new Error('Token name is required'));
      return;
    }

    setCreating(true);
    try {
      const payload = {
        token_name: newToken.token_name.trim(),
        expires_in_days:
          newToken.expires_in_days === '' ? null : newToken.expires_in_days,
      };

      const response = await apiClient.post(
        '/users/service-account-tokens',
        payload
      );

      if (response.ok) {
        const data = await response.json();
        setCreatedTokenInDialog(data.token);
        setNewToken({ token_name: '', expires_in_days: 30 });
        await refreshAfterMutation();
      } else {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to create token');
      }
    } catch (error) {
      setCreateError(error);
    } finally {
      setCreating(false);
    }
  };

  // Handle delete token
  const handleDeleteToken = async () => {
    if (!tokenToDelete) return;

    setDeleting(true);
    setDeleteError(null);
    try {
      const response = await apiClient.post(
        '/users/service-account-tokens/delete',
        {
          token_id: tokenToDelete.token_id,
        }
      );

      if (response.ok) {
        setCreateSuccess(
          `Service account "${tokenToDelete.token_name}" deleted successfully!`
        );
        setShowDeleteDialog(false);
        setTokenToDelete(null);
        setDeleteError(null);
        await refreshAfterMutation();
      } else {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to delete service account');
      }
    } catch (error) {
      // Show error at top level for better visibility
      setShowDeleteDialog(false);
      setTokenToDelete(null);
      setDeleteError(null);
      setCreateError(error);
    } finally {
      setDeleting(false);
    }
  };

  // Handle rotate token
  const handleRotateToken = async () => {
    if (!tokenToRotate) return;

    setRotating(true);
    try {
      const payload = {
        token_id: tokenToRotate.token_id,
        expires_in_days:
          rotateExpiration === '' ? null : parseInt(rotateExpiration),
      };

      const response = await apiClient.post(
        '/users/service-account-tokens/rotate',
        payload
      );

      if (response.ok) {
        const data = await response.json();
        setRotatedTokenInDialog(data.token);
        await refreshAfterMutation();
      } else {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to rotate token');
      }
    } catch (error) {
      setCreateError(error);
    } finally {
      setRotating(false);
    }
  };

  // Filter tokens based on search query.
  // Server-paginated mode pushes search to the backend, so the client list
  // is already filtered — do not double-filter, which would hide rows that
  // matched on backend-only fields (e.g. service_account_user_id).
  const filteredTokens = serverPaginated
    ? tokensWithCounts
    : tokensWithCounts.filter((token) => {
        if (!searchQuery?.trim()) return true;

        const query = searchQuery.toLowerCase();
        return (
          token.token_name?.toLowerCase().includes(query) ||
          token.creator_name?.toLowerCase().includes(query) ||
          token.service_account_name?.toLowerCase().includes(query) ||
          token.primaryRole?.toLowerCase().includes(query)
        );
      });

  if (loading && tokensWithCounts.length === 0) {
    return (
      <div className="flex items-center justify-center py-8">
        <CircularProgress size={32} />
        <span className="ml-3">Loading tokens...</span>
      </div>
    );
  }

  return (
    <>
      {/* Tokens Table */}
      {filteredTokens.length === 0 ? (
        <div className="text-center py-12">
          <KeyRoundIcon className="mx-auto h-12 w-12 text-gray-400" />
          <h3 className="mt-2 text-sm font-medium text-gray-900">
            {searchQuery?.trim()
              ? 'No tokens match your search'
              : 'No service accounts'}
          </h3>
          <p className="mt-1 text-sm text-gray-500">
            {searchQuery?.trim()
              ? 'Try adjusting your search terms.'
              : 'No service accounts have been created yet.'}
          </p>
        </div>
      ) : (
        <>
          <div className="text-sm text-gray-500 mb-2">
            {filteredTokens.length}{' '}
            {filteredTokens.length === 1
              ? 'service account'
              : 'service accounts'}
          </div>
          <Card>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Name</TableHead>
                  <TableHead>Created by</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Clusters</TableHead>
                  <TableHead>Jobs</TableHead>
                  <TableHead>GPUs</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead>Last used</TableHead>
                  <TableHead>Expires</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {filteredTokens.map((token) => (
                  <TableRow key={token.token_id}>
                    <TableCell className="truncate" title={token.token_name}>
                      {token.token_name}
                    </TableCell>
                    <TableCell className="truncate">
                      <div className="flex items-center">
                        <span>{token.creator_name || 'Unknown'}</span>
                        {token.creator_user_hash !== userRoleCache?.id && (
                          <span className="ml-2 px-1.5 py-0.5 text-xs bg-gray-100 text-gray-600 rounded">
                            Other
                          </span>
                        )}
                      </div>
                    </TableCell>
                    <TableCell className="truncate">
                      <div className="flex items-center gap-2">
                        {editingTokenId === token.token_id ? (
                          <>
                            <select
                              value={currentEditingRole}
                              onChange={(e) =>
                                setCurrentEditingRole(e.target.value)
                              }
                              className="block w-auto p-1 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-sky-blue focus:border-sky-blue sm:text-sm"
                            >
                              <option value="admin">Admin</option>
                              <option value="user">User</option>
                            </select>
                            <button
                              onClick={() => handleSaveEdit(token.token_id)}
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
                            <span className="capitalize">
                              {token.primaryRole}
                            </span>
                            {/* Only show edit role button if admin or owner */}
                            {(userRoleCache?.role === 'admin' ||
                              token.creator_user_hash ===
                                userRoleCache?.id) && (
                              <button
                                onClick={() =>
                                  handleEditClick(
                                    token.token_id,
                                    token.primaryRole
                                  )
                                }
                                className="text-blue-600 hover:text-blue-700 p-1"
                                title="Edit role"
                              >
                                <PenIcon className="h-3 w-3" />
                              </button>
                            )}
                          </>
                        )}
                      </div>
                    </TableCell>
                    <TableCell>
                      {token.clusterCount === undefined ? (
                        <span
                          className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-400"
                          title="Counts hidden in server-paginated view"
                        >
                          —
                        </span>
                      ) : (
                        <Link
                          href={`/clusters?property=user&operator=%3A&value=${encodeURIComponent(token.service_account_name)}`}
                          className={`px-2 py-0.5 rounded text-xs font-medium transition-colors duration-200 cursor-pointer inline-block ${
                            token.clusterCount > 0
                              ? 'bg-blue-100 text-blue-600 hover:bg-blue-200 hover:text-blue-700'
                              : 'bg-gray-100 text-gray-500 hover:bg-gray-200 hover:text-gray-700'
                          }`}
                          title={`View ${token.clusterCount} cluster${token.clusterCount !== 1 ? 's' : ''} for ${token.token_name}`}
                        >
                          {token.clusterCount}
                        </Link>
                      )}
                    </TableCell>
                    <TableCell>
                      {token.jobCount === undefined ? (
                        <span
                          className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-400"
                          title="Counts hidden in server-paginated view"
                        >
                          —
                        </span>
                      ) : (
                        <Link
                          href={`/jobs?property=user&operator=%3A&value=${encodeURIComponent(token.service_account_name)}`}
                          className={`px-2 py-0.5 rounded text-xs font-medium transition-colors duration-200 cursor-pointer inline-block ${
                            token.jobCount > 0
                              ? 'bg-green-100 text-green-600 hover:bg-green-200 hover:text-green-700'
                              : 'bg-gray-100 text-gray-500 hover:bg-gray-200 hover:text-gray-700'
                          }`}
                          title={`View ${token.jobCount} active job${token.jobCount !== 1 ? 's' : ''} for ${token.token_name}`}
                        >
                          {token.jobCount}
                        </Link>
                      )}
                    </TableCell>
                    <TableCell>
                      {token.gpuCount === undefined ? (
                        <span
                          className="px-2 py-0.5 rounded text-xs font-medium bg-gray-100 text-gray-400"
                          title="Counts hidden in server-paginated view"
                        >
                          —
                        </span>
                      ) : (
                        <span
                          className={`px-2 py-0.5 rounded text-xs font-medium ${
                            token.gpuCount > 0
                              ? 'bg-purple-100 text-purple-600'
                              : 'bg-gray-100 text-gray-500'
                          }`}
                          title={`Total GPUs: ${token.gpuCount}`}
                        >
                          {token.gpuCount}
                        </span>
                      )}
                    </TableCell>
                    <TableCell className="truncate">
                      {token.created_at ? (
                        <TimestampWithTooltip
                          date={new Date(token.created_at * 1000)}
                        />
                      ) : (
                        'Never'
                      )}
                    </TableCell>
                    <TableCell className="truncate">
                      {token.last_used_at ? (
                        <TimestampWithTooltip
                          date={new Date(token.last_used_at * 1000)}
                        />
                      ) : (
                        'Never'
                      )}
                    </TableCell>
                    <TableCell className="truncate">
                      {!token.expires_at ? (
                        'Never'
                      ) : new Date(token.expires_at * 1000) < new Date() ? (
                        <span className="text-red-600">Expired</span>
                      ) : (
                        <TimestampWithTooltip
                          date={new Date(token.expires_at * 1000)}
                        />
                      )}
                    </TableCell>
                    <TableCell>
                      <div className="flex items-center space-x-2">
                        {/* Show rotate button only if user owns the token or is admin */}
                        {(userRoleCache?.role === 'admin' ||
                          token.creator_user_hash === userRoleCache?.id) && (
                          <CustomTooltip
                            content={`Rotate token`}
                            className="capitalize text-sm text-muted-foreground"
                          >
                            <button
                              onClick={() => {
                                checkPermissionAndAct(
                                  'cannot rotate service account tokens',
                                  () => {
                                    setTokenToRotate(token);
                                    setShowRotateDialog(true);
                                  }
                                );
                              }}
                              className="text-sky-blue hover:text-sky-blue-bright font-medium inline-flex items-center"
                            >
                              <RotateCwIcon className="h-4 w-4" />
                            </button>
                          </CustomTooltip>
                        )}
                        {/* Show delete button only if user owns the token or is admin */}
                        {(userRoleCache?.role === 'admin' ||
                          token.creator_user_hash === userRoleCache?.id) && (
                          <Tooltip
                            content={`Delete ${token.token_name}`}
                            className="capitalize text-sm text-muted-foreground"
                          >
                            <button
                              onClick={() => {
                                checkPermissionAndAct(
                                  'cannot delete service account tokens',
                                  () => {
                                    setTokenToDelete(token);
                                    setShowDeleteDialog(true);
                                  }
                                );
                              }}
                              className="text-red-600 hover:text-red-800 font-medium inline-flex items-center"
                            >
                              <Trash2Icon className="h-4 w-4" />
                            </button>
                          </Tooltip>
                        )}
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </Card>
          {serverPaginated && (
            <div className="flex items-center justify-between mt-3 text-sm text-gray-600">
              <div>
                Showing {(page - 1) * limit + 1}-{Math.min(page * limit, total)}{' '}
                of {total}
              </div>
              <div className="flex items-center gap-2">
                <select
                  value={limit}
                  onChange={(e) => {
                    setLimit(Number(e.target.value));
                    setPage(1);
                  }}
                  className="h-7 px-2 border border-gray-300 rounded text-sm"
                  disabled={loading}
                >
                  {[10, 20, 50, 100, 200].map((opt) => (
                    <option key={opt} value={opt}>
                      {opt} / page
                    </option>
                  ))}
                </select>
                <button
                  onClick={() => setPage((p) => Math.max(1, p - 1))}
                  disabled={!hasPrev || loading}
                  className="h-7 px-3 border border-gray-300 rounded text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Previous
                </button>
                <span>
                  Page {page} of {totalPages}
                </span>
                <button
                  onClick={() => setPage((p) => p + 1)}
                  disabled={!hasNext || loading}
                  className="h-7 px-3 border border-gray-300 rounded text-sm disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  Next
                </button>
              </div>
            </div>
          )}
        </>
      )}

      {/* Create Service Account Dialog */}
      <Dialog
        open={showCreateDialog}
        onOpenChange={(open) => {
          setShowCreateDialog(open);
          if (!open) {
            setCreatedTokenInDialog(null);
            setCreateError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Create Service Account</DialogTitle>
            <DialogDescription>
              Create a new service account with an API token for programmatic
              access to SkyPilot.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-4 py-4">
            {createdTokenInDialog ? (
              /* Token Created Successfully - Show Token */
              <>
                <div className="p-4 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-center mb-3">
                    <h4 className="text-sm font-medium text-green-900">
                      ⚠️ Service account created successfully - save this token
                      now!
                    </h4>
                    <CustomTooltip
                      content={copySuccess ? 'Copied!' : 'Copy token'}
                      className="text-muted-foreground"
                    >
                      <button
                        onClick={() => copyToClipboard(createdTokenInDialog)}
                        className="flex items-center text-green-600 hover:text-green-800 transition-colors duration-200 p-1 ml-2"
                      >
                        {copySuccess ? (
                          <CheckIcon className="w-4 h-4" />
                        ) : (
                          <CopyIcon className="w-4 h-4" />
                        )}
                      </button>
                    </CustomTooltip>
                  </div>
                  <p className="text-sm text-green-700 mb-3">
                    This service account token will not be shown again. Please
                    copy and store it securely.
                  </p>
                  <div className="bg-white border border-green-300 rounded-md p-3">
                    <code className="text-sm text-gray-800 font-mono break-all block">
                      {createdTokenInDialog}
                    </code>
                  </div>
                </div>
              </>
            ) : (
              /* Token Creation Form */
              <>
                <div className="grid gap-2">
                  <label className="text-sm font-medium text-gray-700">
                    Service Account Name
                  </label>
                  <input
                    className="border rounded px-3 py-2 w-full"
                    placeholder="e.g., ci-pipeline, monitoring-system"
                    value={newToken.token_name}
                    onChange={(e) =>
                      setNewToken({ ...newToken, token_name: e.target.value })
                    }
                  />
                </div>
                <div className="grid gap-2">
                  <label className="text-sm font-medium text-gray-700">
                    Expiration (days)
                  </label>
                  <input
                    type="number"
                    className="border rounded px-3 py-2 w-full"
                    placeholder="e.g., 30"
                    min="0"
                    max="365"
                    value={newToken.expires_in_days ?? ''}
                    onChange={(e) =>
                      setNewToken({
                        ...newToken,
                        expires_in_days: e.target.value
                          ? parseInt(e.target.value)
                          : null,
                      })
                    }
                  />
                  <p className="text-xs text-gray-500">
                    Leave empty or enter 0 to never expire. Maximum 365 days.
                  </p>
                </div>
              </>
            )}
          </div>
          <DialogFooter>
            {createdTokenInDialog ? (
              <button
                className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-sky-600 text-white hover:bg-sky-700 h-10 px-4 py-2"
                onClick={() => {
                  setShowCreateDialog(false);
                  setCreatedTokenInDialog(null);
                }}
              >
                Close
              </button>
            ) : (
              <>
                <button
                  className="inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-10 px-4 py-2"
                  onClick={() => {
                    setShowCreateDialog(false);
                    setCreatedTokenInDialog(null);
                  }}
                  disabled={creating}
                >
                  Cancel
                </button>
                <button
                  className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-sky-600 text-white hover:bg-sky-700 h-10 px-4 py-2"
                  onClick={handleCreateToken}
                  disabled={creating || !newToken.token_name.trim()}
                >
                  {creating ? 'Creating...' : 'Create Token'}
                </button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Token Dialog */}
      <Dialog
        open={showDeleteDialog}
        onOpenChange={(open) => {
          setShowDeleteDialog(open);
          if (!open) {
            setTokenToDelete(null);
            setCreateError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>Delete Service Account Token</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete the service account &quot;
              {tokenToDelete?.token_name}&quot;
              {tokenToDelete?.creator_user_hash !== userRoleCache?.id &&
              userRoleCache?.role === 'admin'
                ? ` owned by ${tokenToDelete?.creator_name}`
                : ''}
              ? This action cannot be undone and will immediately revoke access
              for any systems using this token.
            </DialogDescription>
          </DialogHeader>

          <DialogFooter>
            <button
              className="inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-10 px-4 py-2"
              onClick={() => {
                setShowDeleteDialog(false);
                setTokenToDelete(null);
              }}
              disabled={deleting}
            >
              Cancel
            </button>
            <button
              className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-red-600 text-white hover:bg-red-700 h-10 px-4 py-2"
              onClick={handleDeleteToken}
              disabled={deleting}
            >
              {deleting ? 'Deleting...' : 'Delete Token'}
            </button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Rotate Token Dialog */}
      <Dialog
        open={showRotateDialog}
        onOpenChange={(open) => {
          setShowRotateDialog(open);
          if (!open) {
            setTokenToRotate(null);
            setRotateExpiration('');
            setRotatedTokenInDialog(null);
            setCreateError(null);
          }
        }}
      >
        <DialogContent className="sm:max-w-2xl">
          <DialogHeader>
            <DialogTitle>Rotate Service Account Token</DialogTitle>
            <DialogDescription>
              Rotate the service account token &quot;{tokenToRotate?.token_name}
              &quot;
              {tokenToRotate?.creator_user_hash !== userRoleCache?.id &&
              userRoleCache?.role === 'admin'
                ? ` owned by ${tokenToRotate?.creator_name}`
                : ''}
              . This will generate a new token value and invalidate the current
              one.
            </DialogDescription>
          </DialogHeader>
          <div className="flex flex-col gap-4 py-4">
            {rotatedTokenInDialog ? (
              /* Token Rotated Successfully - Show Token */
              <>
                <div className="p-4 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-center mb-3">
                    <h4 className="text-sm font-medium text-green-900">
                      🔄 Service account token rotated successfully - save this
                      new token now!
                    </h4>
                    <CustomTooltip
                      content={copySuccess ? 'Copied!' : 'Copy token'}
                      className="text-muted-foreground"
                    >
                      <button
                        onClick={() => copyToClipboard(rotatedTokenInDialog)}
                        className="flex items-center text-green-600 hover:text-green-800 transition-colors duration-200 p-1 ml-2"
                      >
                        {copySuccess ? (
                          <CheckIcon className="w-4 h-4" />
                        ) : (
                          <CopyIcon className="w-4 h-4" />
                        )}
                      </button>
                    </CustomTooltip>
                  </div>
                  <p className="text-sm text-green-700 mb-3">
                    This new token replaces the old one. Please copy and store
                    it securely. The old token is now invalid.
                  </p>
                  <div className="bg-white border border-green-300 rounded-md p-3">
                    <code className="text-sm text-gray-800 font-mono break-all block">
                      {rotatedTokenInDialog}
                    </code>
                  </div>
                </div>
              </>
            ) : (
              /* Token Rotation Form */
              <>
                <div className="grid gap-2">
                  <label className="text-sm font-medium text-gray-700">
                    New Expiration (days)
                  </label>
                  <input
                    type="number"
                    className="border rounded px-3 py-2 w-full"
                    placeholder="Leave empty to preserve current expiration"
                    min="0"
                    max="365"
                    value={rotateExpiration}
                    onChange={(e) => setRotateExpiration(e.target.value)}
                  />
                  <p className="text-xs text-gray-500">
                    Leave empty to preserve current expiration. Enter number of
                    days for new expiration, or enter 0 to set to never expire.
                    Maximum 365 days.
                  </p>
                </div>
                <div className="p-3 bg-amber-50 border border-amber-200 rounded">
                  <p className="text-sm text-amber-700">
                    ⚠️ Any systems using the current token will need to be
                    updated with the new token.
                  </p>
                </div>
              </>
            )}
          </div>
          <DialogFooter>
            {rotatedTokenInDialog ? (
              <button
                className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-green-600 text-white hover:bg-green-700 h-10 px-4 py-2"
                onClick={() => {
                  setShowRotateDialog(false);
                  setTokenToRotate(null);
                  setRotateExpiration('');
                  setRotatedTokenInDialog(null);
                }}
              >
                Close
              </button>
            ) : (
              <>
                <button
                  className="inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-10 px-4 py-2"
                  onClick={() => {
                    setShowRotateDialog(false);
                    setTokenToRotate(null);
                    setRotateExpiration('');
                    setRotatedTokenInDialog(null);
                  }}
                  disabled={rotating}
                >
                  Cancel
                </button>
                <button
                  className="inline-flex items-center justify-center rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 bg-sky-600 text-white hover:bg-sky-700 h-10 px-4 py-2"
                  onClick={handleRotateToken}
                  disabled={rotating}
                >
                  {rotating ? 'Rotating...' : 'Rotate Token'}
                </button>
              </>
            )}
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
