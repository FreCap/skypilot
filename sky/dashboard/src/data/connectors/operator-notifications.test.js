import { apiClient } from '@/data/connectors/client';
import {
  acknowledgeOperatorNotifications,
  getOperatorNotifications,
  resetOperatorNotificationRequestsForTests,
} from './operator-notifications';

jest.mock('@/data/connectors/client', () => ({
  apiClient: { get: jest.fn(), post: jest.fn() },
}));

function response(payload) {
  return { ok: true, json: jest.fn().mockResolvedValue(payload) };
}

describe('operator notification connector', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    resetOperatorNotificationRequestsForTests();
  });

  it('deduplicates concurrent recent-history requests', async () => {
    let resolveFetch;
    apiClient.get.mockReturnValue(
      new Promise((resolve) => {
        resolveFetch = resolve;
      })
    );

    const first = getOperatorNotifications(7);
    const second = getOperatorNotifications(7);
    expect(first).toBe(second);
    expect(apiClient.get).toHaveBeenCalledTimes(1);
    expect(apiClient.get).toHaveBeenCalledWith('/notifications?days=7');

    resolveFetch(response({ notifications: [], unread_count: 0 }));
    await expect(first).resolves.toEqual({
      notifications: [],
      unread_count: 0,
    });
  });

  it('sends a monotonic cursor acknowledgement', async () => {
    apiClient.post.mockResolvedValue(response({ last_seen_sequence: 17 }));
    await expect(acknowledgeOperatorNotifications(17)).resolves.toEqual({
      last_seen_sequence: 17,
    });
    expect(apiClient.post).toHaveBeenCalledWith('/notifications/read', {
      through_sequence: 17,
    });
  });
});
