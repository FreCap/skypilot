'use client';

import { useMemo, useState } from 'react';
import Link from 'next/link';
import { CircularProgress } from '@mui/material';
import { EditIcon, PlusIcon, Trash2Icon } from 'lucide-react';

import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { CLOUD_CANONICALIZATIONS } from '@/data/connectors/constants';
import { sortData } from '@/data/utils';

const WorkspaceBadge = ({ isPrivate }) => {
  if (isPrivate) {
    return (
      <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-700 border border-gray-300">
        Private
      </span>
    );
  }
  return (
    <span className="inline-flex items-center px-2 py-1 rounded-full text-xs font-medium bg-green-100 text-green-700 border border-green-300">
      Public
    </span>
  );
};

export function WorkspacesTable({
  workspaceDetails,
  rawWorkspacesData,
  clustersLoading,
  jobsLoading,
  isInitialLoad,
  roleLoading,
  onCreateWorkspace,
  onEditWorkspace,
  onDeleteWorkspace,
  router,
}) {
  const [sortConfig, setSortConfig] = useState({
    key: 'name',
    direction: 'asc',
  });
  const [searchQuery, setSearchQuery] = useState('');

  const handleSort = (key) => {
    let direction = 'asc';
    if (sortConfig.key === key && sortConfig.direction === 'asc') {
      direction = 'desc';
    }
    setSortConfig({ key, direction });
  };

  const getSortDirection = (key) => {
    if (sortConfig.key === key) {
      return sortConfig.direction === 'asc' ? ' ↑' : ' ↓';
    }
    return '';
  };

  const sortedWorkspaces = useMemo(() => {
    if (!workspaceDetails) return [];

    let filtered = workspaceDetails;
    if (searchQuery && searchQuery.trim() !== '') {
      const searchLower = searchQuery.toLowerCase().trim();
      filtered = workspaceDetails.filter((workspace) => {
        if (workspace.name.toLowerCase().includes(searchLower)) {
          return true;
        }

        if (
          workspace.clouds.some((cloud) => {
            const canonicalCloudName =
              CLOUD_CANONICALIZATIONS[cloud.toLowerCase()] || cloud;
            return (
              cloud.toLowerCase().includes(searchLower) ||
              canonicalCloudName.toLowerCase().includes(searchLower)
            );
          })
        ) {
          return true;
        }

        const workspaceConfig = rawWorkspacesData?.[workspace.name] || {};
        const isPrivate = workspaceConfig.private === true;
        const status = isPrivate ? 'private' : 'public';
        return status.includes(searchLower);
      });
    }

    return sortData(filtered, sortConfig.key, sortConfig.direction);
  }, [workspaceDetails, sortConfig, searchQuery, rawWorkspacesData]);

  return (
    <>
      <div className="flex items-center justify-between mb-4">
        <div className="relative flex-1 sm:flex-none">
          <input
            type="text"
            placeholder="Filter workspaces"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="h-8 w-full sm:w-96 px-3 pr-8 text-sm border border-gray-300 rounded-md focus:ring-0 focus:outline-none"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery('')}
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

        <button
          onClick={onCreateWorkspace}
          disabled={roleLoading}
          className="ml-4 bg-sky-600 hover:bg-sky-700 text-white flex items-center rounded-md px-3 py-1 text-sm font-medium transition-colors duration-200"
          title="Create Workspace"
        >
          {roleLoading ? (
            <>
              <CircularProgress size={12} className="mr-2" />
              <span>Create Workspace</span>
            </>
          ) : (
            <>
              <PlusIcon className="h-4 w-4 mr-2" />
              Create Workspace
            </>
          )}
        </button>
      </div>

      {workspaceDetails.length === 0 && !isInitialLoad ? (
        <div className="text-center py-10">
          <p className="text-lg text-gray-600">No workspaces found.</p>
          <p className="text-sm text-gray-500 mt-2">
            Create a cluster to see its workspace here.
          </p>
        </div>
      ) : (
        <Card>
          <div className="overflow-x-auto rounded-lg">
            <Table className="min-w-full">
              <TableHeader>
                <TableRow>
                  <TableHead
                    className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50"
                    onClick={() => handleSort('name')}
                  >
                    Workspace{getSortDirection('name')}
                  </TableHead>
                  <TableHead
                    className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50"
                    onClick={() => handleSort('runningClusterCount')}
                  >
                    Running Clusters {getSortDirection('runningClusterCount')}
                  </TableHead>
                  <TableHead
                    className="sortable whitespace-nowrap cursor-pointer hover:bg-gray-50"
                    onClick={() => handleSort('managedJobsCount')}
                  >
                    Jobs{getSortDirection('managedJobsCount')}
                  </TableHead>
                  <TableHead className="whitespace-nowrap">
                    Enabled infra
                  </TableHead>
                  <TableHead className="whitespace-nowrap">Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {isInitialLoad && sortedWorkspaces.length === 0 ? (
                  <TableRow>
                    <TableCell
                      colSpan={5}
                      className="text-center py-6 text-gray-500"
                    >
                      <div className="flex justify-center items-center">
                        <CircularProgress size={20} className="mr-2" />
                        <span>Loading...</span>
                      </div>
                    </TableCell>
                  </TableRow>
                ) : sortedWorkspaces.length > 0 ? (
                  sortedWorkspaces.map((workspace) => {
                    const workspaceConfig =
                      rawWorkspacesData?.[workspace.name] || {};
                    const isPrivate = workspaceConfig.private === true;

                    return (
                      <TableRow
                        key={workspace.name}
                        className="hover:bg-gray-50"
                      >
                        <TableCell className="">
                          <button
                            onClick={() => onEditWorkspace(workspace.name)}
                            disabled={roleLoading}
                            className="text-blue-600 hover:text-blue-600 hover:underline text-left"
                          >
                            {workspace.name}
                          </button>
                          <span className="ml-2">
                            <WorkspaceBadge isPrivate={isPrivate} />
                          </span>
                        </TableCell>
                        <TableCell>
                          <button
                            onClick={() => {
                              router.push({
                                pathname: '/clusters',
                                query: { workspace: workspace.name },
                              });
                            }}
                            className="text-gray-700 hover:text-blue-600 hover:underline"
                          >
                            <span className="inline-flex items-center px-2 py-0.5 bg-gray-100 text-gray-700 rounded text-sm">
                              {clustersLoading ? (
                                <CircularProgress size={12} />
                              ) : (
                                workspace.runningClusterCount
                              )}
                            </span>
                          </button>
                        </TableCell>
                        <TableCell>
                          <button
                            onClick={() => {
                              router.push({
                                pathname: '/jobs',
                                query: { workspace: workspace.name },
                              });
                            }}
                            className="text-gray-700 hover:text-blue-600 hover:underline"
                          >
                            <span className="inline-flex items-center px-2 py-0.5 bg-gray-100 text-gray-700 rounded text-sm">
                              {jobsLoading ? (
                                <CircularProgress size={12} />
                              ) : (
                                workspace.managedJobsCount
                              )}
                            </span>
                          </button>
                        </TableCell>
                        <TableCell>
                          {workspace.clouds.length > 0 ? (
                            [...workspace.clouds].sort().map((cloud, index) => {
                              const canonicalCloudName =
                                CLOUD_CANONICALIZATIONS[cloud.toLowerCase()] ||
                                cloud;
                              return (
                                <span key={cloud}>
                                  <Link
                                    href="/infra"
                                    className="inline-flex items-center px-2 py-1 rounded text-sm bg-sky-100 text-sky-800 hover:bg-sky-200 hover:text-sky-900 transition-colors duration-200"
                                  >
                                    {canonicalCloudName}
                                  </Link>
                                  {index < workspace.clouds.length - 1 && ' '}
                                </span>
                              );
                            })
                          ) : (
                            <span className="text-gray-500 text-sm">-</span>
                          )}
                        </TableCell>
                        <TableCell>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => onEditWorkspace(workspace.name)}
                            disabled={roleLoading}
                            className="text-gray-600 hover:text-gray-800 mr-1"
                          >
                            <EditIcon className="w-4 h-4" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => onDeleteWorkspace(workspace.name)}
                            disabled={
                              workspace.name === 'default' || roleLoading
                            }
                            title={
                              workspace.name === 'default'
                                ? 'Cannot delete default workspace'
                                : 'Delete workspace'
                            }
                            className="text-red-600 hover:text-red-700 hover:bg-red-50"
                          >
                            <Trash2Icon className="w-4 h-4" />
                          </Button>
                        </TableCell>
                      </TableRow>
                    );
                  })
                ) : (
                  <TableRow>
                    <TableCell
                      colSpan={5}
                      className="text-center py-6 text-gray-500"
                    >
                      No workspaces found
                    </TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
          </div>
        </Card>
      )}
    </>
  );
}
