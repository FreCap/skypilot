'use client';

import React, { useEffect, useState } from 'react';
import { CircularProgress } from '@mui/material';
import yaml from 'js-yaml';
import { AlertTriangleIcon } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { YamlEditor } from '@/components/ui/yaml-editor';
import {
  BUILTIN_RECIPE_TYPES,
  getRecipeTypeInfo,
  RecipeType,
} from '@/data/constants/recipeTypes';

// Helper to generate example YAML based on type
function getExampleRecipe(recipeType, pluginRecipeTypes = []) {
  switch (recipeType) {
    case RecipeType.CLUSTER:
      return `name: my-cluster
resources:
  infra: aws
  accelerators: A100:1

run: |
  echo "Hello, SkyPilot!"
`;
    case RecipeType.JOB:
      return `name: my-job
resources:
  infra: aws
  accelerators: A100:1

run: |
  echo "Running managed job..."
`;
    case RecipeType.POOL:
      return `pool:
  name: my-pool

resources:
  infra: aws
  accelerators: A100:1
`;
    default: {
      // Check if a plugin provides a template for this type
      const pluginType = pluginRecipeTypes.find((t) => t.id === recipeType);
      if (pluginType && pluginType.template) {
        return pluginType.template;
      }
      return `name: my-${recipeType}
resources:
  infra: aws
  accelerators: A100:1

run: |
  echo "Hello, SkyPilot!"
`;
    }
  }
}

// Create YAML Modal with Owner Name field
export function CreateRecipeModal({
  isOpen,
  onClose,
  onSubmit,
  initialData,
  isAuthenticated,
  visibleRecipeTypes,
  pluginRecipeTypes = [],
}) {
  const [name, setName] = useState('');
  const [description, setDescription] = useState('');
  const [content, setContent] = useState(
    getExampleRecipe(RecipeType.CLUSTER, pluginRecipeTypes)
  );
  const [recipeType, setRecipeType] = useState(RecipeType.CLUSTER);
  const [ownerName, setOwnerName] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [formError, setFormError] = useState(null);

  useEffect(() => {
    if (isOpen) {
      if (initialData) {
        setName(initialData.name || '');
        setDescription(initialData.description || '');
        setContent(initialData.content || '');
        setRecipeType(initialData.recipe_type || RecipeType.CLUSTER);
      } else {
        setName('');
        setDescription('');
        setRecipeType(RecipeType.CLUSTER);
        setContent(getExampleRecipe(RecipeType.CLUSTER, pluginRecipeTypes));
      }
      setOwnerName('');
      setFormError(null);
    }
  }, [initialData, isOpen, pluginRecipeTypes]);

  // Update example YAML when type changes (only if content matches previous example)
  const handleRecipeTypeChange = (newType) => {
    const oldExample = getExampleRecipe(recipeType, pluginRecipeTypes);
    // If user hasn't modified the content, update it with new example
    if (content === oldExample || content === '') {
      setContent(getExampleRecipe(newType, pluginRecipeTypes));
    }
    setRecipeType(newType);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setIsSubmitting(true);
    setFormError(null);

    // Validate YAML syntax
    try {
      yaml.load(content);
    } catch (yamlError) {
      setFormError(`Invalid YAML: ${yamlError.message}`);
      setIsSubmitting(false);
      return;
    }

    try {
      await onSubmit({
        name,
        description: description || null,
        content,
        recipeType,
        ownerName: ownerName || null,
      });
      onClose();
    } catch (error) {
      setFormError(error.message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <Dialog open={isOpen} onOpenChange={onClose}>
      <DialogContent className="sm:max-w-2xl max-h-[90vh] overflow-y-auto px-8">
        <DialogHeader>
          <DialogTitle className="text-xl text-gray-900">
            Create New Recipe
          </DialogTitle>
          <DialogDescription>
            Create a reusable recipe for clusters, jobs, and more.
          </DialogDescription>
        </DialogHeader>

        <div className="bg-amber-50 border border-amber-200 rounded-md p-3 flex items-start gap-2 mt-4">
          <AlertTriangleIcon className="w-4 h-4 text-amber-600 mt-0.5 flex-shrink-0" />
          <p className="text-sm text-amber-800">
            This recipe will be visible to everyone with access to this
            dashboard.
          </p>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4 mt-4">
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="name">Name *</Label>
              <Input
                id="name"
                value={name}
                onChange={(e) => {
                  setName(e.target.value);
                  setFormError(null);
                }}
                placeholder="my-gpu-training"
                className="placeholder:text-gray-400"
                required
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="recipe-type">Type *</Label>
              <Select value={recipeType} onValueChange={handleRecipeTypeChange}>
                <SelectTrigger>
                  <SelectValue placeholder="Select type" />
                </SelectTrigger>
                <SelectContent>
                  {(visibleRecipeTypes || BUILTIN_RECIPE_TYPES).map((type) => {
                    const info = getRecipeTypeInfo(type, pluginRecipeTypes);
                    const TypeIcon = info.icon;
                    return (
                      <SelectItem key={type} value={type}>
                        <div className="flex items-center gap-2">
                          <TypeIcon className={`w-4 h-4 ${info.colorClass}`} />
                          <span>{info.fullLabel}</span>
                        </div>
                      </SelectItem>
                    );
                  })}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Owner Name field - only shown when not authenticated */}
          {!isAuthenticated && (
            <div className="space-y-2">
              <Label htmlFor="owner-name">Your Name</Label>
              <Input
                id="owner-name"
                value={ownerName}
                onChange={(e) => setOwnerName(e.target.value)}
                placeholder="Enter your name (optional)"
                className="placeholder:text-gray-400"
              />
              <p className="text-xs text-gray-500">
                This name will be shown as the template owner.
              </p>
            </div>
          )}

          <div className="space-y-2">
            <Label htmlFor="description">Description</Label>
            <Input
              id="description"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              placeholder="A brief description of what this recipe does..."
              className="placeholder:text-gray-400"
            />
          </div>

          <div className="space-y-2">
            <Label htmlFor="content">YAML Content *</Label>
            <YamlEditor
              value={content}
              onChange={(val) => {
                setContent(val);
                setFormError(null);
              }}
              maxHeight="400px"
            />
          </div>

          {formError && (
            <div className="rounded-md border border-red-200 bg-red-50 p-3 flex items-start gap-2">
              <AlertTriangleIcon className="w-4 h-4 text-red-600 mt-0.5 flex-shrink-0" />
              <p className="text-sm text-red-800">{formError}</p>
            </div>
          )}

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={onClose}
              disabled={isSubmitting}
            >
              Cancel
            </Button>
            <Button
              type="submit"
              disabled={isSubmitting}
              className="bg-sky-600 hover:bg-sky-700 text-white"
            >
              {isSubmitting ? (
                <>
                  <CircularProgress size={16} className="mr-2" />
                  Creating...
                </>
              ) : (
                'Create Recipe'
              )}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
