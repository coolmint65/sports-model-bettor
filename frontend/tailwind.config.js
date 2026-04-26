import tailwindcssAnimate from 'tailwindcss-animate'

/** @type {import('tailwindcss').Config} */
// Tailwind config for the Phase 2a infrastructure.
//
// Theme extensions point at CSS variables defined in src/styles/tokens.css
// so the existing token system stays the single source of truth — adding
// a new color or radius is a one-line change in tokens.css that both old
// CSS-module code and new Tailwind classes pick up.
//
// shadcn-ui's generated components reference HSL-form variables
// (--primary, --background, --foreground, etc.); those are added to
// tokens.css alongside the hex semantic tokens we already use.
export default {
  darkMode: ['class'],
  content: [
    './index.html',
    './src/**/*.{js,jsx,ts,tsx}',
  ],
  theme: {
    container: {
      center: true,
      padding: '2rem',
      screens: { '2xl': '1400px' },
    },
    extend: {
      colors: {
        // shadcn-ui semantic colors (HSL-encoded variables in tokens.css)
        border:     'hsl(var(--border))',
        input:      'hsl(var(--input))',
        ring:       'hsl(var(--ring))',
        background: 'hsl(var(--background))',
        foreground: 'hsl(var(--foreground))',
        primary: {
          DEFAULT:    'hsl(var(--primary))',
          foreground: 'hsl(var(--primary-foreground))',
        },
        secondary: {
          DEFAULT:    'hsl(var(--secondary))',
          foreground: 'hsl(var(--secondary-foreground))',
        },
        destructive: {
          DEFAULT:    'hsl(var(--destructive))',
          foreground: 'hsl(var(--destructive-foreground))',
        },
        muted: {
          DEFAULT:    'hsl(var(--muted))',
          foreground: 'hsl(var(--muted-foreground))',
        },
        accent: {
          DEFAULT:    'hsl(var(--accent))',
          foreground: 'hsl(var(--accent-foreground))',
        },
        popover: {
          DEFAULT:    'hsl(var(--popover))',
          foreground: 'hsl(var(--popover-foreground))',
        },
        card: {
          DEFAULT:    'hsl(var(--card))',
          foreground: 'hsl(var(--card-foreground))',
        },
        // Domain-specific semantic colors (read straight from hex tokens)
        positive:        'var(--color-positive)',
        'positive-strong': 'var(--color-positive-strong)',
        negative:        'var(--color-negative)',
        'negative-strong': 'var(--color-negative-strong)',
        warning:         'var(--color-warning)',
        'edge-strong':   'var(--color-edge-strong)',
        'edge-moderate': 'var(--color-edge-moderate)',
        'edge-lean':     'var(--color-edge-lean)',
      },
      borderRadius: {
        lg: 'var(--radius-lg)',
        md: 'var(--radius-md)',
        sm: 'var(--radius-sm)',
      },
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', '-apple-system'],
        mono: ['SF Mono', 'Monaco', 'Courier New', 'monospace'],
      },
      keyframes: {
        'accordion-down': {
          from: { height: '0' },
          to:   { height: 'var(--radix-accordion-content-height)' },
        },
        'accordion-up': {
          from: { height: 'var(--radix-accordion-content-height)' },
          to:   { height: '0' },
        },
      },
      animation: {
        'accordion-down': 'accordion-down 0.2s ease-out',
        'accordion-up':   'accordion-up 0.2s ease-out',
      },
    },
  },
  plugins: [
    // Required for shadcn-ui Accordion / Dialog / Sheet animations.
    tailwindcssAnimate,
  ],
}
