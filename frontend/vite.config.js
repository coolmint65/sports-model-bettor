import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      // Match the `@/...` alias pattern shadcn-ui generated components
      // expect (per components.json). Lets future shadcn add commands
      // drop files into src/components/ui without manual import fixes.
      '@': path.resolve(__dirname, './src'),
    },
  },
  server: {
    // Bind 0.0.0.0 so devices on the Tailscale net (phone, iPad,
    // other boxes) can reach the dev server by the host's Tailscale
    // IP. Default is localhost-only, which blocks anything but the
    // dev machine itself.
    host: true,
    port: 5173,
    // Allow the Tailscale hostname + raw tailnet IP in case Vite's
    // Host-header check tightens in future versions. Explicit list
    // keeps the dev server from 403'ing remote browsers.
    allowedHosts: ['desktop-jscmnio', '.ts.net', 'localhost'],
    proxy: {
      '/api': 'http://localhost:8000',
    },
  },
})
