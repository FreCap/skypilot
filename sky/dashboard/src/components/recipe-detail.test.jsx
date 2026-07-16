import {
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';

let mockRouter;

jest.mock('next/router', () => ({
  useRouter: () => mockRouter,
}));

jest.mock('@/data/connectors/recipes', () => ({
  getRecipe: jest.fn(),
  updateRecipe: jest.fn(),
  deleteRecipe: jest.fn(),
  togglePinRecipe: jest.fn(),
}));

jest.mock('@/data/connectors/toast', () => ({
  showToast: jest.fn(),
}));

jest.mock('@/plugins/PluginProvider', () => ({
  usePluginRecipeTypes: () => [],
}));

jest.mock('@/plugins/PluginSlot', () => ({
  PluginSlot: ({ fallback = null }) => fallback,
}));

jest.mock('@/components/ui/yaml-code-block', () => ({
  YamlCodeBlock: ({ value }) => <pre>{value}</pre>,
}));

import {
  deleteRecipe,
  getRecipe,
  togglePinRecipe,
} from '@/data/connectors/recipes';
import { showToast } from '@/data/connectors/toast';
import { RecipeDetail } from '@/components/recipe-detail';

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function recipe(name) {
  return {
    name,
    recipe_type: 'job',
    content: `name: ${name}`,
    is_editable: true,
    is_pinnable: true,
    pinned: false,
  };
}

function setRoute(recipeSlug, isReady = true) {
  mockRouter = {
    isReady,
    query: recipeSlug ? { recipe: recipeSlug } : {},
    push: jest.fn(),
  };
}

describe('RecipeDetail request ownership', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    setRoute('recipe-a');
  });

  it('keeps the newest route loading when an older route resolves first', async () => {
    const recipeA = deferred();
    const recipeB = deferred();
    getRecipe
      .mockImplementationOnce(() => recipeA.promise)
      .mockImplementationOnce(() => recipeB.promise);

    const view = render(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(1));

    setRoute('recipe-b');
    view.rerender(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(2));

    await act(async () => {
      recipeA.resolve(recipe('recipe-a'));
      await recipeA.promise;
    });

    expect(screen.getByText('Loading...')).toBeTruthy();
    expect(screen.queryByText('recipe-a')).toBeNull();

    await act(async () => {
      recipeB.resolve(recipe('recipe-b'));
      await recipeB.promise;
    });
    expect(screen.getAllByText('recipe-b').length).toBeGreaterThan(0);
  });

  it('drops a stale success after the newest route has loaded', async () => {
    const recipeA = deferred();
    const recipeB = deferred();
    getRecipe
      .mockImplementationOnce(() => recipeA.promise)
      .mockImplementationOnce(() => recipeB.promise);

    const view = render(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(1));

    setRoute('recipe-b');
    view.rerender(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(2));

    await act(async () => {
      recipeB.resolve(recipe('recipe-b'));
      await recipeB.promise;
    });
    expect(screen.getAllByText('recipe-b').length).toBeGreaterThan(0);

    await act(async () => {
      recipeA.resolve(recipe('recipe-a'));
      await recipeA.promise;
    });

    expect(screen.queryByText('recipe-a')).toBeNull();
    expect(screen.getAllByText('recipe-b').length).toBeGreaterThan(0);
    expect(getRecipe).toHaveBeenCalledTimes(2);
  });

  it('drops a stale failure after the newest route has loaded', async () => {
    const recipeA = deferred();
    const recipeB = deferred();
    getRecipe
      .mockImplementationOnce(() => recipeA.promise)
      .mockImplementationOnce(() => recipeB.promise);

    const view = render(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(1));

    setRoute('recipe-b');
    view.rerender(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(2));

    await act(async () => {
      recipeB.resolve(recipe('recipe-b'));
      await recipeB.promise;
    });
    await act(async () => {
      recipeA.reject(new Error('stale recipe failure'));
      await expect(recipeA.promise).rejects.toThrow('stale recipe failure');
    });

    expect(screen.queryByText('stale recipe failure')).toBeNull();
    expect(screen.getAllByText('recipe-b').length).toBeGreaterThan(0);
    expect(getRecipe).toHaveBeenCalledTimes(2);
  });

  it('revokes a pending route when the router becomes unready', async () => {
    const recipeA = deferred();
    getRecipe.mockImplementationOnce(() => recipeA.promise);

    const view = render(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(1));

    setRoute(null, false);
    view.rerender(<RecipeDetail />);

    await act(async () => {
      recipeA.resolve(recipe('recipe-a'));
      await recipeA.promise;
    });

    expect(screen.getByText('Loading...')).toBeTruthy();
    expect(screen.queryByText('recipe-a')).toBeNull();
    expect(getRecipe).toHaveBeenCalledTimes(1);
  });

  it('hides a loaded route when the router becomes unready', async () => {
    getRecipe.mockResolvedValueOnce(recipe('recipe-a'));
    const view = render(<RecipeDetail />);

    await screen.findAllByText('recipe-a');
    expect(getRecipe).toHaveBeenCalledTimes(1);

    setRoute(null, false);
    view.rerender(<RecipeDetail />);

    expect(screen.getByText('Loading...')).toBeTruthy();
    expect(screen.queryByText('recipe-a')).toBeNull();
    expect(getRecipe).toHaveBeenCalledTimes(1);
  });

  it('handles current not-found and failure results without extra calls', async () => {
    getRecipe.mockResolvedValueOnce(null);
    const view = render(<RecipeDetail />);

    expect(await screen.findByText('Recipe not found')).toBeTruthy();
    expect(getRecipe).toHaveBeenCalledTimes(1);

    setRoute('recipe-b');
    getRecipe.mockRejectedValueOnce(new Error('current failure'));
    view.rerender(<RecipeDetail />);

    expect(await screen.findByText('current failure')).toBeTruthy();
    expect(getRecipe).toHaveBeenCalledTimes(2);
  });

  it('starts exactly one request for each ready route', async () => {
    setRoute(null, false);
    getRecipe.mockResolvedValue(recipe('recipe-a'));

    const view = render(<RecipeDetail />);
    expect(getRecipe).not.toHaveBeenCalled();

    setRoute('recipe-a');
    view.rerender(<RecipeDetail />);
    await waitFor(() => expect(getRecipe).toHaveBeenCalledTimes(1));
    expect(getRecipe).toHaveBeenLastCalledWith('recipe-a');

    view.rerender(<RecipeDetail />);
    expect(getRecipe).toHaveBeenCalledTimes(1);
  });

  it('drops a stale pin result after the newest route has loaded', async () => {
    const pinA = deferred();
    getRecipe
      .mockResolvedValueOnce(recipe('recipe-a'))
      .mockResolvedValueOnce(recipe('recipe-b'));
    togglePinRecipe.mockImplementationOnce(() => pinA.promise);

    const view = render(<RecipeDetail />);
    await screen.findAllByText('recipe-a');

    fireEvent.click(screen.getByText('Pin'));
    expect(togglePinRecipe).toHaveBeenCalledWith('recipe-a', true);

    setRoute('recipe-b');
    view.rerender(<RecipeDetail />);
    await screen.findAllByText('recipe-b');

    await act(async () => {
      pinA.resolve({ ...recipe('recipe-a'), pinned: true });
      await pinA.promise;
    });

    expect(screen.queryAllByText('recipe-a')).toHaveLength(0);
    expect(screen.getAllByText('recipe-b').length).toBeGreaterThan(0);
  });

  it('drops a stale pin failure after the newest route has loaded', async () => {
    const pinA = deferred();
    getRecipe
      .mockResolvedValueOnce(recipe('recipe-a'))
      .mockResolvedValueOnce(recipe('recipe-b'));
    togglePinRecipe.mockImplementationOnce(() => pinA.promise);

    const view = render(<RecipeDetail />);
    await screen.findAllByText('recipe-a');
    fireEvent.click(screen.getByText('Pin'));

    setRoute('recipe-b');
    view.rerender(<RecipeDetail />);
    await screen.findAllByText('recipe-b');

    await act(async () => {
      pinA.reject(new Error('stale pin failure'));
      await expect(pinA.promise).rejects.toThrow('stale pin failure');
    });

    expect(screen.queryAllByText('recipe-a')).toHaveLength(0);
    expect(screen.getAllByText('recipe-b').length).toBeGreaterThan(0);
    expect(showToast).not.toHaveBeenCalled();
  });

  it('does not navigate after a stale delete succeeds', async () => {
    const deleteA = deferred();
    getRecipe
      .mockResolvedValueOnce(recipe('recipe-a'))
      .mockResolvedValueOnce(recipe('recipe-b'));
    deleteRecipe.mockImplementationOnce(() => deleteA.promise);

    const view = render(<RecipeDetail />);
    await screen.findAllByText('recipe-a');
    const recipeARouter = mockRouter;
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    await screen.findByRole('dialog');
    fireEvent.click(screen.getByRole('button', { name: 'Delete' }));
    expect(deleteRecipe).toHaveBeenCalledWith('recipe-a');

    setRoute('recipe-b');
    view.rerender(<RecipeDetail />);
    await screen.findAllByText('recipe-b');
    expect(screen.queryByRole('dialog')).toBeNull();

    await act(async () => {
      deleteA.resolve(true);
      await deleteA.promise;
    });

    expect(recipeARouter.push).not.toHaveBeenCalled();
    expect(screen.getAllByText('recipe-b').length).toBeGreaterThan(0);
    expect(showToast).not.toHaveBeenCalled();
  });

  it('publishes a current pin result', async () => {
    getRecipe.mockResolvedValueOnce(recipe('recipe-a'));
    togglePinRecipe.mockResolvedValueOnce({
      ...recipe('recipe-a'),
      pinned: true,
    });

    render(<RecipeDetail />);
    await screen.findAllByText('recipe-a');
    fireEvent.click(screen.getByText('Pin'));

    expect(await screen.findByText('Unpin')).toBeTruthy();
    expect(showToast).toHaveBeenCalledWith('Recipe pinned!', 'success');
  });
});
