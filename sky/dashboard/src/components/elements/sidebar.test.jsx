import React from 'react';
import { act, render, screen } from '@testing-library/react';

import { resetCurrentUserCacheForTests } from '@/data/connectors/client';
import { SidebarProvider, useSidebar } from './sidebar';

function UserSnapshot() {
  const { userEmail, userRole } = useSidebar();
  return (
    <div>
      {userEmail ?? 'no-email'}|{userRole ?? 'no-role'}
    </div>
  );
}

describe('SidebarProvider current user snapshot', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    resetCurrentUserCacheForTests();
    global.fetch = jest.fn();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('publishes identity and role from one shared snapshot', async () => {
    global.fetch.mockResolvedValue({
      ok: true,
      json: async () => ({
        id: 'alice-id',
        name: 'alice@example.com',
        role: 'admin',
      }),
    });

    render(
      <SidebarProvider>
        <UserSnapshot />
      </SidebarProvider>
    );

    expect(
      await screen.findByText('alice@example.com|admin')
    ).toBeInTheDocument();
    expect(global.fetch).toHaveBeenCalledTimes(1);
    expect(global.fetch.mock.calls[0][0]).toBe(
      'http://localhost/internal/dashboard/users/role'
    );
  });

  it('does not publish a fallback after a failed role lookup', async () => {
    global.fetch.mockResolvedValue({ ok: false });

    render(
      <SidebarProvider>
        <UserSnapshot />
      </SidebarProvider>
    );

    await act(async () => {});
    expect(screen.getByText('no-email|no-role')).toBeInTheDocument();
    expect(global.fetch).toHaveBeenCalledTimes(1);
  });

  it('ignores a shared snapshot that resolves after unmount', async () => {
    let resolveFetch;
    global.fetch.mockReturnValue(
      new Promise((resolve) => {
        resolveFetch = resolve;
      })
    );
    const setUserEmail = jest.fn();
    const setUserRole = jest.fn();
    const useState = jest.spyOn(React, 'useState');
    useState
      .mockImplementationOnce((initial) => [initial, jest.fn()])
      .mockImplementationOnce((initial) => [initial, jest.fn()])
      .mockImplementationOnce((initial) => [initial, setUserEmail])
      .mockImplementationOnce((initial) => [initial, setUserRole]);

    const { unmount } = render(
      <SidebarProvider>
        <UserSnapshot />
      </SidebarProvider>
    );
    unmount();
    await act(async () => {
      resolveFetch({
        ok: true,
        json: async () => ({
          id: 'late-id',
          name: 'late@example.com',
          role: 'admin',
        }),
      });
    });

    expect(setUserEmail).not.toHaveBeenCalled();
    expect(setUserRole).not.toHaveBeenCalled();
  });
});
