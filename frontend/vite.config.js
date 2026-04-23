import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

export default defineConfig({
  plugins: [react()],
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
