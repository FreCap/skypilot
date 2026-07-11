import React from 'react';
import { render, screen } from '@testing-library/react';

import { DeploymentVersionContent } from './version-display';

test('shows the deployed version and build without requiring an upgrade', () => {
  render(
    <DeploymentVersionContent
      version="1.1.27"
      latestVersion={null}
      commit="abcdef123456"
      build="5921"
      plugins={[]}
    />
  );

  expect(screen.getByText('v1.1.27 · build 5921')).toBeVisible();
});
