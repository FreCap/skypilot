/** @type {import('tailwindcss').Config} */
const twColors = require('tailwindcss/colors');
const plugin = require('tailwindcss/plugin');

const extraContent = (process.env.SKYPILOT_DASHBOARD_TAILWIND_CONTENT || '')
  .split(';')
  .map((s) => s.trim())
  .filter(Boolean);

// The dashboard hard-codes palette shades everywhere (`bg-white`,
// `text-gray-600`, `bg-blue-50 text-blue-700` chips, ...). Instead of adding
// `dark:` variants to every component, the hues below are remapped to CSS
// variables and flipped under `.dark`: light tints (50-300) become deep tints,
// dark text shades (700-900) become light ones, and mid shades (400-600) are
// kept as-is since they read fine on both backgrounds.
// `slate` is intentionally excluded: the shadcn/ui primitives already style it
// with explicit `dark:` variants.
const THEMED_HUES = [
  'gray',
  'red',
  'orange',
  'amber',
  'yellow',
  'green',
  'emerald',
  'teal',
  'cyan',
  'sky',
  'blue',
  'indigo',
  'purple',
  'pink',
];
const SHADES = [50, 100, 200, 300, 400, 500, 600, 700, 800, 900];
const DARK_FLIP = {
  50: 950,
  100: 900,
  200: 800,
  300: 700,
  700: 300,
  800: 200,
  900: 100,
};

// Bespoke dark ramp for the neutral scale (slate-tinted so it pairs with the
// shadcn `.dark` background tokens). Surfaces get progressively lighter:
// body (#020817) < gray-50 wash < `bg-white` cards (--tw-c-surface) < gray-100.
const DARK_GRAY = {
  50: '#0b1220',
  100: '#1c2534',
  200: '#293448',
  300: '#3b4961',
  400: '#64748b',
  500: '#94a3b8',
  600: '#b8c4d4',
  700: '#cbd5e1',
  800: '#e2e8f0',
  900: '#f1f5f9',
};

// Brand colors: navy is unreadable on dark backgrounds.
const BRAND = {
  'sky-blue': { light: '#0E2E65', dark: '#a8c7fa' },
  'sky-blue-bright': { light: '#1E62CC', dark: '#7ab0f5' },
};

// `bg-white` card surface in dark mode (white itself stays white so that
// `text-white` on colored buttons keeps its contrast).
const DARK_SURFACE = '#131c2e';

function rgbTriplet(hex) {
  const n = parseInt(hex.slice(1), 16);
  return `${(n >> 16) & 255} ${(n >> 8) & 255} ${n & 255}`;
}

const themedColors = {};
const lightVars = { '--tw-c-surface': '255 255 255' };
const darkVars = { '--tw-c-surface': rgbTriplet(DARK_SURFACE) };
for (const hue of THEMED_HUES) {
  themedColors[hue] = {};
  for (const shade of SHADES) {
    const varName = `--tw-c-${hue}-${shade}`;
    themedColors[hue][shade] = `rgb(var(${varName}) / <alpha-value>)`;
    lightVars[varName] = rgbTriplet(twColors[hue][shade]);
    const darkHex =
      hue === 'gray'
        ? DARK_GRAY[shade]
        : twColors[hue][DARK_FLIP[shade] ?? shade];
    darkVars[varName] = rgbTriplet(darkHex);
  }
}
for (const [name, { light, dark }] of Object.entries(BRAND)) {
  const varName = `--tw-c-${name}`;
  themedColors[name] = `rgb(var(${varName}) / <alpha-value>)`;
  lightVars[varName] = rgbTriplet(light);
  darkVars[varName] = rgbTriplet(dark);
}

module.exports = {
  darkMode: ['class'],
  content: [
    './pages/**/*.{js,jsx}',
    './components/**/*.{js,jsx}',
    './app/**/*.{js,jsx}',
    './src/**/*.{js,jsx}',
    ...extraContent,
  ],
  prefix: '',
  theme: {
    container: {
      center: true,
      padding: '2rem',
      screens: {
        '2xl': '1400px',
      },
    },
    extend: {
      colors: {
        ...themedColors,
        gcpgreen: '#188038',
        border: 'hsl(var(--border))',
        input: 'hsl(var(--input))',
        ring: 'hsl(var(--ring))',
        background: 'hsl(var(--background))',
        foreground: 'hsl(var(--foreground))',
        primary: {
          DEFAULT: 'hsl(var(--primary))',
          foreground: 'hsl(var(--primary-foreground))',
        },
        secondary: {
          DEFAULT: 'hsl(var(--secondary))',
          foreground: 'hsl(var(--secondary-foreground))',
        },
        destructive: {
          DEFAULT: 'hsl(var(--destructive))',
          foreground: 'hsl(var(--destructive-foreground))',
        },
        muted: {
          DEFAULT: 'hsl(var(--muted))',
          foreground: 'hsl(var(--muted-foreground))',
        },
        accent: {
          DEFAULT: 'hsl(var(--accent))',
          foreground: 'hsl(var(--accent-foreground))',
        },
        popover: {
          DEFAULT: 'hsl(var(--popover))',
          foreground: 'hsl(var(--popover-foreground))',
        },
        card: {
          DEFAULT: 'hsl(var(--card))',
          foreground: 'hsl(var(--card-foreground))',
        },
      },
      borderRadius: {
        lg: 'var(--radius)',
        md: 'calc(var(--radius) - 2px)',
        sm: 'calc(var(--radius) - 4px)',
      },
      keyframes: {
        'accordion-down': {
          from: { height: '0' },
          to: { height: 'var(--radix-accordion-content-height)' },
        },
        'accordion-up': {
          from: { height: 'var(--radix-accordion-content-height)' },
          to: { height: '0' },
        },
      },
      animation: {
        'accordion-down': 'accordion-down 0.2s ease-out',
        'accordion-up': 'accordion-up 0.2s ease-out',
      },
    },
  },
  plugins: [
    require('tailwindcss-animate'),
    plugin(({ addBase }) => addBase({ ':root': lightVars, '.dark': darkVars })),
  ],
};
