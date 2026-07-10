import React, { useEffect, useState } from 'react';
import { Moon, Sun } from 'lucide-react';
import { CustomTooltip } from '@/components/utils';
import { getTheme, setTheme } from '@/lib/theme';

export function ThemeToggle() {
  const [theme, setThemeState] = useState(null);

  useEffect(() => {
    setThemeState(getTheme());
  }, []);

  if (theme === null) {
    // Avoid a hydration mismatch: render nothing until mounted.
    return null;
  }

  const toggle = () => {
    const next = theme === 'dark' ? 'light' : 'dark';
    setTheme(next);
    setThemeState(next);
  };

  return (
    <CustomTooltip
      content={
        theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'
      }
      className="text-sm text-muted-foreground"
    >
      <button
        onClick={toggle}
        className="inline-flex items-center justify-center p-2 rounded-full text-gray-600 hover:bg-gray-100 transition-colors duration-150 cursor-pointer"
        title={
          theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'
        }
      >
        {theme === 'dark' ? (
          <Sun className="w-5 h-5" />
        ) : (
          <Moon className="w-5 h-5" />
        )}
      </button>
    </CustomTooltip>
  );
}
