import React, { useEffect, useRef, useState } from 'react';
import Link from 'next/link';
import { CircularProgress } from '@mui/material';

import { PaginationControls } from '@/components/elements/PaginationControls';
import { getStatusStyle } from '@/components/elements/StatusBadge';
import {
  JobStatusBadges as SharedJobStatusBadges,
  InfraBadges as SharedInfraBadges,
} from '@/components/utils';
import { Card } from '@/components/ui/card';
import {
  Table,
  TableHeader,
  TableRow,
  TableHead,
  TableBody,
  TableCell,
} from '@/components/ui/table';
import { buildFilterUrl } from '@/components/shared/FilterSystem';
import { getPoolStatus } from '@/data/connectors/jobs';
import dashboardCache from '@/lib/cache';

export function PoolsTable({ refreshInterval, setLoading, refreshDataRef }) {
  const [data, setData] = useState([]);
  const [sortConfig, setSortConfig] = useState({
    key: null,
    direction: 'ascending',
  });
  const [loading, setLocalLoading] = useState(false);
  const [isInitialLoad, setIsInitialLoad] = useState(true);
  const [currentPage, setCurrentPage] = useState(1);
  const [pageSize, setPageSize] = useState(10);
  const mountedRef = useRef(false);
  const requestVersionRef = useRef(0);

  const fetchData = React.useCallback(async () => {
    const version = requestVersionRef.current + 1;
    requestVersionRef.current = version;
    const ownsState = () =>
      mountedRef.current && version === requestVersionRef.current;
    setLocalLoading(true);
    setLoading(true);
    try {
      const poolsResponse = await dashboardCache.get(getPoolStatus, [{}]);
      if (!ownsState()) return;
      const { pools = [] } = poolsResponse || {};
      setData(pools);
      setIsInitialLoad(false);
    } catch (err) {
      if (!ownsState()) return;
      console.error('Error fetching pools data:', err);
      setData([]);
      setIsInitialLoad(false);
    } finally {
      if (ownsState()) {
        setLocalLoading(false);
        setLoading(false);
      }
    }
  }, [setLoading]);

  // Expose fetchData to parent component
  React.useEffect(() => {
    if (refreshDataRef) {
      refreshDataRef.current = fetchData;
    }
  }, [refreshDataRef, fetchData]);

  useEffect(() => {
    mountedRef.current = true;
    setData([]);
    let isCurrent = true;

    fetchData();

    const interval = setInterval(() => {
      if (isCurrent && window.document.visibilityState === 'visible') {
        fetchData();
      }
    }, refreshInterval);

    return () => {
      isCurrent = false;
      mountedRef.current = false;
      requestVersionRef.current += 1;
      clearInterval(interval);
    };
  }, [refreshInterval, fetchData]);

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

  // Sort the data
  const sortedData = React.useMemo(() => {
    if (!sortConfig.key) return data;

    return [...data].sort((a, b) => {
      if (a[sortConfig.key] < b[sortConfig.key]) {
        return sortConfig.direction === 'ascending' ? -1 : 1;
      }
      if (a[sortConfig.key] > b[sortConfig.key]) {
        return sortConfig.direction === 'ascending' ? 1 : -1;
      }
      return 0;
    });
  }, [data, sortConfig]);

  // Calculate pagination
  const totalPages = Math.ceil(sortedData.length / pageSize);
  const startIndex = (currentPage - 1) * pageSize;
  const endIndex = startIndex + pageSize;
  const paginatedData = sortedData.slice(startIndex, endIndex);

  // Page navigation handlers
  const goToPreviousPage = () => {
    setCurrentPage((page) => Math.max(page - 1, 1));
  };

  const goToNextPage = () => {
    setCurrentPage((page) => Math.min(page + 1, totalPages));
  };

  const handlePageSizeChange = (e) => {
    const newSize = parseInt(e.target.value, 10);
    setPageSize(newSize);
    setCurrentPage(1);
  };

  const getWorkersCount = (pool) => {
    if (!pool || !pool.replica_info || pool.replica_info.length === 0)
      return '0 (target: 0)';

    const readyWorkers = pool.replica_info.filter(
      (worker) => worker.status === 'READY'
    ).length;
    const targetWorkers = pool.target_num_replicas || 0;
    return `${readyWorkers} (target: ${targetWorkers})`;
  };

  const JobStatusBadges = ({ jobCounts }) => {
    return (
      <SharedJobStatusBadges
        jobCounts={jobCounts}
        getStatusStyle={getStatusStyle}
      />
    );
  };

  const InfraBadges = ({ replicaInfo }) => {
    return <SharedInfraBadges replicaInfo={replicaInfo} />;
  };

  return (
    <Card>
      <div className="overflow-x-auto rounded-lg">
        <Table className="min-w-full table-fixed">
          <TableHeader>
            <TableRow>
              <TableHead
                className="sortable whitespace-nowrap w-32"
                onClick={() => requestSort('name')}
              >
                Pool{getSortDirection('name')}
              </TableHead>
              <TableHead
                className="sortable whitespace-nowrap w-40"
                onClick={() => requestSort('job_counts')}
              >
                Jobs{getSortDirection('job_counts')}
              </TableHead>
              <TableHead className="whitespace-nowrap w-20">Workers</TableHead>
              <TableHead
                className="sortable whitespace-nowrap w-36"
                onClick={() => requestSort('requested_resources_str')}
              >
                Worker Details{getSortDirection('requested_resources_str')}
              </TableHead>
              <TableHead
                className="sortable whitespace-nowrap w-40"
                onClick={() => requestSort('requested_resources_str')}
              >
                Worker Resources{getSortDirection('requested_resources_str')}
              </TableHead>
            </TableRow>
          </TableHeader>
          <TableBody>
            {loading && isInitialLoad ? (
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
            ) : paginatedData.length > 0 ? (
              paginatedData.map((pool) => (
                <TableRow key={pool.name}>
                  <TableCell>
                    <Link
                      href={`/jobs/pools/${pool.name}`}
                      className="text-blue-600 hover:text-blue-800"
                    >
                      {pool.name}
                    </Link>
                  </TableCell>
                  <TableCell>
                    <div className="flex items-center gap-2 flex-wrap">
                      <JobStatusBadges jobCounts={pool.jobCounts} />
                      <Link
                        href={buildFilterUrl('/jobs', 'pool', ':', pool.name)}
                        className="text-blue-600 hover:text-blue-800 text-xs"
                      >
                        See all jobs
                      </Link>
                    </div>
                  </TableCell>
                  <TableCell>{getWorkersCount(pool)}</TableCell>
                  <TableCell>
                    <InfraBadges replicaInfo={pool.replica_info} />
                  </TableCell>
                  <TableCell>{pool.requested_resources_str || '-'}</TableCell>
                </TableRow>
              ))
            ) : (
              <TableRow>
                <TableCell
                  colSpan={5}
                  className="text-center py-6 text-gray-500"
                >
                  No pools found
                </TableCell>
              </TableRow>
            )}
          </TableBody>
        </Table>
      </div>

      {/* Pagination */}
      {paginatedData.length > 0 && totalPages > 1 && (
        <PaginationControls
          currentPage={currentPage}
          totalPages={totalPages}
          totalCount={sortedData.length}
          startIndex={startIndex}
          endIndex={endIndex}
          onPageChange={setCurrentPage}
          onPreviousPage={goToPreviousPage}
          onNextPage={goToNextPage}
          isPrevDisabled={currentPage === 1}
          isNextDisabled={currentPage === totalPages}
          pageSize={pageSize}
          onPageSizeChange={handlePageSizeChange}
          pageSizeOptions={[5, 10, 25, 50]}
        />
      )}
    </Card>
  );
}
