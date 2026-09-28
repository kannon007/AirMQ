/** @type {import('tailwindcss').Config} */
export default {
  content: ['./index.html', './src/**/*.{js,ts,jsx,tsx}'],
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        brand: {
          50: '#ecfdf5',
          100: '#d1fae5',
          200: '#a7f3d0',
          300: '#6ee7b7',
          400: '#34d399',
          500: '#10b981', // EMQX Emerald
          600: '#059669',
          700: '#047857',
          800: '#065f46',
          900: '#064e3b',
        },
        dark: {
          950: '#080c14', // Page Background
          900: '#0d121f', // Surface / Sidebar
          850: '#111728', // Cards / Panels
          800: '#172036', // Elevated / Hover
          700: '#23304e', // Inputs / Borders
          600: '#33446b',
        },
      },
    },
  },
  plugins: [],
};
