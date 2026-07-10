// Light/dark theme handling. The dashboard defaults to dark mode; the choice
// is persisted in localStorage and applied by toggling the `dark` class on
// <html> (Tailwind `darkMode: ['class']`).

const THEME_STORAGE_KEY = 'skypilot-theme';
const DEFAULT_THEME = 'dark';

export function getTheme() {
  try {
    const stored = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (stored === 'light' || stored === 'dark') {
      return stored;
    }
  } catch (e) {
    // localStorage unavailable (e.g. blocked); fall through to the default.
  }
  return DEFAULT_THEME;
}

export function applyTheme(theme) {
  document.documentElement.classList.toggle('dark', theme === 'dark');
}

export function setTheme(theme) {
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, theme);
  } catch (e) {
    // Non-persistable; still apply for the current session.
  }
  applyTheme(theme);
}

export function initTheme() {
  applyTheme(getTheme());
}
