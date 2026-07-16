import { fireEvent, render, screen, waitFor } from '@testing-library/react';

let mockPluginRecipeTypes = [];
const mockRouter = {
  query: {},
  replace: jest.fn(),
};

jest.mock('next/router', () => ({
  useRouter: () => mockRouter,
}));

jest.mock('@/plugins/PluginProvider', () => ({
  usePluginRoutes: () => [],
  usePluginRecipeTypes: () => mockPluginRecipeTypes,
}));

jest.mock('@/components/ui/dialog', () => ({
  Dialog: ({ open, children }) => (open ? <div>{children}</div> : null),
  DialogContent: ({ children }) => <div>{children}</div>,
  DialogHeader: ({ children }) => <div>{children}</div>,
  DialogTitle: ({ children }) => <h2>{children}</h2>,
  DialogDescription: ({ children }) => <p>{children}</p>,
  DialogFooter: ({ children }) => <div>{children}</div>,
}));

jest.mock('@/components/ui/select', () => ({
  Select: ({ value, onValueChange, children }) => (
    <select
      aria-label="Recipe type"
      value={value}
      onChange={(event) => onValueChange(event.target.value)}
    >
      {children}
    </select>
  ),
  SelectContent: ({ children }) => <>{children}</>,
  SelectItem: ({ value }) => <option value={value}>{value}</option>,
  SelectTrigger: () => null,
  SelectValue: () => null,
}));

jest.mock('@/components/ui/yaml-editor', () => ({
  YamlEditor: ({ value, onChange }) => (
    <textarea
      id="content"
      aria-label="YAML Content"
      value={value}
      onChange={(event) => onChange(event.target.value)}
    />
  ),
}));

jest.mock('@/data/connectors/recipes', () => ({
  getRecipes: jest.fn(),
  createRecipe: jest.fn(),
  deleteRecipe: jest.fn(),
  togglePinRecipe: jest.fn(),
}));

jest.mock('@/data/connectors/toast', () => ({
  showToast: jest.fn(),
}));

jest.mock('@/lib/analytics', () => ({
  trackRecipeAction: jest.fn(),
  trackFilterUsed: jest.fn(),
}));

import { createRecipe, getRecipes } from '@/data/connectors/recipes';
import { RecipeHub } from '@/components/recipe-hub';

describe('RecipeHub creation form', () => {
  beforeEach(() => {
    jest.clearAllMocks();
    mockPluginRecipeTypes = [];
    getRecipes.mockResolvedValue([]);
    createRecipe.mockResolvedValue(undefined);
    global.fetch = jest.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ id: 'local' }),
    });
  });

  afterEach(() => {
    delete global.fetch;
  });

  async function openCreateForm() {
    render(<RecipeHub />);
    fireEvent.click(await screen.findByRole('button', { name: 'New Recipe' }));
  }

  it('opens with the built-in cluster template and local owner field', async () => {
    await openCreateForm();

    expect(
      screen.getByRole('heading', { name: 'Create New Recipe' })
    ).toBeInTheDocument();
    expect(screen.getByLabelText('Recipe type')).toHaveValue('cluster');
    expect(screen.getByLabelText('YAML Content').value).toEqual(
      expect.stringContaining('name: my-cluster')
    );
    expect(screen.getByLabelText('Your Name')).toBeInTheDocument();
  });

  it('rejects invalid YAML without calling the create connector', async () => {
    await openCreateForm();
    fireEvent.change(screen.getByLabelText('Name *'), {
      target: { value: 'broken-recipe' },
    });
    fireEvent.change(screen.getByLabelText('YAML Content'), {
      target: { value: 'name: [' },
    });

    fireEvent.click(screen.getByRole('button', { name: 'Create Recipe' }));

    await waitFor(() =>
      expect(screen.getByText(/Invalid YAML:/)).toBeInTheDocument()
    );
    expect(createRecipe).not.toHaveBeenCalled();
  });

  it('uses a plugin template and preserves the submission payload', async () => {
    mockPluginRecipeTypes = [
      {
        id: 'training',
        label: 'Training',
        template: 'name: plugin-training\nrun: train.py\n',
      },
    ];
    await openCreateForm();
    fireEvent.change(screen.getByLabelText('Name *'), {
      target: { value: 'plugin-recipe' },
    });
    fireEvent.change(screen.getByLabelText('Recipe type'), {
      target: { value: 'training' },
    });

    expect(screen.getByLabelText('YAML Content')).toHaveValue(
      'name: plugin-training\nrun: train.py\n'
    );
    fireEvent.click(screen.getByRole('button', { name: 'Create Recipe' }));

    await waitFor(() =>
      expect(createRecipe).toHaveBeenCalledWith({
        name: 'plugin-recipe',
        description: null,
        content: 'name: plugin-training\nrun: train.py\n',
        recipeType: 'training',
        ownerName: null,
      })
    );
    await waitFor(() => expect(getRecipes).toHaveBeenCalledTimes(2));
  });
});
