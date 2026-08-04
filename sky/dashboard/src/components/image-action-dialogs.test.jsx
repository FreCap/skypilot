import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

import {
  CanaryProfileDialog,
  IMAGE_REMEDIATIONS,
  PublishImageDialog,
} from '@/components/image-action-dialogs';
import {
  canaryImageProfile,
  getImageOperation,
  newIdempotencyKey,
  publishImage,
} from '@/data/connectors/images';

jest.mock('@/data/connectors/images', () => ({
  canaryImageProfile: jest.fn(),
  getImageOperation: jest.fn(),
  newIdempotencyKey: jest.fn(),
  prepareImage: jest.fn(),
  publishImage: jest.fn(),
  qualifyImageProfile: jest.fn(),
  retryImageLocation: jest.fn(),
  retryImagePublication: jest.fn(),
}));

const capabilities = {
  workspace: 'research',
  default_distribution: 'gpu-production',
  source_bindings: ['private-source'],
  distributions: [
    {
      name: 'gpu-production',
      active: true,
      targets: [
        {
          name: 'aws-us-west-2',
          region: 'us-west-2',
          runtime_backends: ['aws_vm'],
          runtime_ids: { aws_vm: ['us-west-2'] },
        },
      ],
    },
  ],
};

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function setDocumentVisibility(value) {
  Object.defineProperty(window.document, 'visibilityState', {
    configurable: true,
    value,
  });
}

function renderPublish(onOpenChange = jest.fn(), onChanged = jest.fn()) {
  return render(
    <PublishImageDialog
      open
      onOpenChange={onOpenChange}
      workspace="research"
      capabilities={capabilities}
      onChanged={onChanged}
    />
  );
}

function fillValidPublishForm() {
  fireEvent.change(screen.getByLabelText('OCI source digest'), {
    target: {
      value: `ghcr.io/boltz/image@sha256:${'a'.repeat(64)}`,
    },
  });
  fireEvent.change(screen.getByLabelText('Release'), {
    target: { value: 'boltz-l4-2026-07-20' },
  });
}

describe('managed image action dialogs', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    newIdempotencyKey.mockReturnValue('stable-key');
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  it('offers bounded remediation for an unavailable registry shard', () => {
    expect(IMAGE_REMEDIATIONS.REGISTRY_SHARD_UNAVAILABLE).toBe(
      'Repair shard drift or activate a qualified revision before retrying.'
    );
  });

  it('never recommends retrying a quarantined physical reference', () => {
    expect(IMAGE_REMEDIATIONS.REGISTRY_LOCATION_QUARANTINED).toBe(
      'Activate a qualified target with a new repository ring before preparing again.'
    );
    expect(IMAGE_REMEDIATIONS.PROVIDER_OUTCOME_AMBIGUOUS).toContain(
      'This physical reference is quarantined.'
    );
  });

  it('rejects mutable tags and enables publish only for a digest', async () => {
    renderPublish();
    await waitFor(() => expect(newIdempotencyKey).toHaveBeenCalledTimes(1));
    const submit = screen.getByRole('button', { name: 'Publish' });

    fireEvent.change(screen.getByLabelText('OCI source digest'), {
      target: { value: 'ghcr.io/boltz/image:latest' },
    });
    fireEvent.change(screen.getByLabelText('Release'), {
      target: { value: 'boltz-l4' },
    });
    expect(submit).toBeDisabled();
    expect(screen.getByLabelText('OCI source digest')).toHaveAttribute(
      'aria-invalid',
      'true'
    );

    fireEvent.change(screen.getByLabelText('OCI source digest'), {
      target: {
        value: `ghcr.io/boltz/image@sha256:${'a'.repeat(64)}`,
      },
    });
    expect(submit).toBeEnabled();
  });

  it('reuses one idempotency key after a retryable submission failure', async () => {
    const failure = new Error('bounded');
    failure.code = 'PROVIDER_THROTTLED';
    publishImage.mockRejectedValueOnce(failure).mockResolvedValueOnce({
      operation: { id: 'op-1', state: 'SUCCEEDED', error_code: null },
    });
    renderPublish();
    await waitFor(() => expect(newIdempotencyKey).toHaveBeenCalledTimes(1));
    fillValidPublishForm();

    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));
    expect(await screen.findByText('PROVIDER_THROTTLED')).toBeVisible();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));

    await waitFor(() => expect(publishImage).toHaveBeenCalledTimes(2));
    expect(publishImage.mock.calls.map((call) => call[1])).toEqual([
      'stable-key',
      'stable-key',
    ]);
    expect(await screen.findByText('Background operation')).toBeVisible();
  });

  it('labels closing a nonterminal operation as detach, not cancel', async () => {
    const onOpenChange = jest.fn();
    publishImage.mockResolvedValue({
      operation: { id: 'op-pending', state: 'PENDING', error_code: null },
    });
    getImageOperation.mockImplementation(() => new Promise(() => {}));
    renderPublish(onOpenChange);
    fillValidPublishForm();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));

    const detach = await screen.findByRole('button', { name: 'Detach' });
    expect(screen.queryByRole('button', { name: /cancel/i })).toBeNull();
    fireEvent.click(detach);
    expect(onOpenChange).toHaveBeenCalledWith(false);
  });

  it('keeps one poll owner while a slow operation request is pending', async () => {
    jest.useFakeTimers();
    const pendingPoll = deferred();
    publishImage.mockResolvedValue({
      operation: { id: 'op-slow', state: 'PENDING', error_code: null },
    });
    getImageOperation.mockReturnValue(pendingPoll.promise);

    const view = renderPublish();
    fillValidPublishForm();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));
    await act(async () => {
      await Promise.resolve();
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(6000);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);

    await act(async () => {
      pendingPoll.resolve({
        id: 'op-slow',
        state: 'RUNNING',
        error_code: null,
      });
    });
    await act(async () => {
      jest.advanceTimersByTime(0);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(2);

    view.unmount();
  });

  it('preserves the two-second cadence after a fast nonterminal poll', async () => {
    jest.useFakeTimers();
    const firstPoll = deferred();
    const secondPoll = deferred();
    publishImage.mockResolvedValue({
      operation: { id: 'op-fast', state: 'PENDING', error_code: null },
    });
    getImageOperation
      .mockReturnValueOnce(firstPoll.promise)
      .mockReturnValue(secondPoll.promise);

    const view = renderPublish();
    fillValidPublishForm();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));
    await act(async () => {
      await Promise.resolve();
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);

    await act(async () => {
      firstPoll.resolve({
        id: 'op-fast',
        state: 'RUNNING',
        error_code: null,
      });
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(1999);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);

    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(2);

    view.unmount();
  });

  it('pauses hidden operation polls and catches up once when visibility restores after the due boundary', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');
    publishImage.mockResolvedValue({
      operation: { id: 'op-hidden', state: 'PENDING', error_code: null },
    });
    getImageOperation.mockResolvedValue({
      id: 'op-hidden',
      state: 'RUNNING',
      error_code: null,
    });

    const view = renderPublish();
    fillValidPublishForm();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);

    try {
      setDocumentVisibility('hidden');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        jest.advanceTimersByTime(20_000);
        await Promise.resolve();
      });
      expect(getImageOperation).toHaveBeenCalledTimes(1);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
        await Promise.resolve();
      });
      expect(getImageOperation).toHaveBeenCalledTimes(2);

      await act(async () => {
        jest.advanceTimersByTime(1999);
        await Promise.resolve();
      });
      expect(getImageOperation).toHaveBeenCalledTimes(2);
      await act(async () => {
        jest.advanceTimersByTime(1);
        await Promise.resolve();
      });
      expect(getImageOperation).toHaveBeenCalledTimes(3);
    } finally {
      view.unmount();
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('does not fire an early operation poll when visibility returns before the due boundary', async () => {
    jest.useFakeTimers();
    const visibilityDescriptor = Object.getOwnPropertyDescriptor(
      window.document,
      'visibilityState'
    );
    setDocumentVisibility('visible');
    publishImage.mockResolvedValue({
      operation: { id: 'op-visible', state: 'PENDING', error_code: null },
    });
    getImageOperation.mockResolvedValue({
      id: 'op-visible',
      state: 'RUNNING',
      error_code: null,
    });

    const view = renderPublish();
    fillValidPublishForm();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);

    try {
      setDocumentVisibility('hidden');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        jest.advanceTimersByTime(1999);
        await Promise.resolve();
      });
      expect(getImageOperation).toHaveBeenCalledTimes(1);

      setDocumentVisibility('visible');
      await act(async () => {
        window.document.dispatchEvent(new Event('visibilitychange'));
        await Promise.resolve();
      });
      expect(getImageOperation).toHaveBeenCalledTimes(1);

      await act(async () => {
        jest.advanceTimersByTime(1);
        await Promise.resolve();
      });
      expect(getImageOperation).toHaveBeenCalledTimes(2);
    } finally {
      view.unmount();
      if (visibilityDescriptor) {
        Object.defineProperty(
          window.document,
          'visibilityState',
          visibilityDescriptor
        );
      } else {
        delete window.document.visibilityState;
      }
    }
  });

  it('stops after terminal completion and notifies exactly once', async () => {
    jest.useFakeTimers();
    const terminalPoll = deferred();
    const onChanged = jest.fn();
    publishImage.mockResolvedValue({
      operation: { id: 'op-terminal', state: 'PENDING', error_code: null },
    });
    getImageOperation.mockReturnValue(terminalPoll.promise);

    const view = renderPublish(jest.fn(), onChanged);
    fillValidPublishForm();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));
    await act(async () => {
      await Promise.resolve();
    });

    const terminalOperation = {
      id: 'op-terminal',
      state: 'SUCCEEDED',
      error_code: null,
    };
    await act(async () => {
      terminalPoll.resolve(terminalOperation);
    });
    expect(onChanged).toHaveBeenCalledTimes(1);
    expect(onChanged).toHaveBeenCalledWith(terminalOperation);

    await act(async () => {
      jest.advanceTimersByTime(10_000);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);
    expect(onChanged).toHaveBeenCalledTimes(1);
    view.unmount();
  });

  it('retries failures on cadence and aborts the owner on detach', async () => {
    jest.useFakeTimers();
    const failedPoll = deferred();
    const retryPoll = deferred();
    const failure = new Error('temporary polling failure');
    failure.code = 'POLL_RETRY';
    publishImage.mockResolvedValue({
      operation: { id: 'op-retry', state: 'PENDING', error_code: null },
    });
    getImageOperation
      .mockReturnValueOnce(failedPoll.promise)
      .mockReturnValue(retryPoll.promise);

    const view = renderPublish();
    fillValidPublishForm();
    fireEvent.click(screen.getByRole('button', { name: 'Publish' }));
    await act(async () => {
      await Promise.resolve();
    });
    const signal = getImageOperation.mock.calls[0][2];

    await act(async () => {
      failedPoll.reject(failure);
    });
    expect(screen.getByText('POLL_RETRY')).toBeVisible();
    await act(async () => {
      jest.advanceTimersByTime(1999);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(1);
    await act(async () => {
      jest.advanceTimersByTime(1);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(2);

    view.unmount();
    expect(signal.aborted).toBe(true);
    await act(async () => {
      jest.advanceTimersByTime(10_000);
    });
    expect(getImageOperation).toHaveBeenCalledTimes(2);
  });

  it('requires explicit cost acknowledgement for an actual-principal canary', async () => {
    canaryImageProfile.mockResolvedValue({
      operation: { id: 'canary-1', state: 'SUCCEEDED', error_code: null },
    });
    render(
      <CanaryProfileDialog
        open
        onOpenChange={jest.fn()}
        workspace="research"
        capabilities={capabilities}
        onChanged={jest.fn()}
      />
    );
    const submit = screen.getByRole('button', { name: 'Run canary' });
    expect(submit).toBeDisabled();

    fireEvent.click(
      screen.getByText(/I understand this may launch temporary compute/)
    );
    expect(submit).toBeEnabled();
    fireEvent.click(submit);

    await waitFor(() =>
      expect(canaryImageProfile).toHaveBeenCalledWith(
        'gpu-production',
        {
          workspace: 'research',
          target: 'aws-us-west-2',
          backend: 'aws_vm',
          runtime_id: 'us-west-2',
          confirm_cost: true,
        },
        'stable-key'
      )
    );
  });
});
