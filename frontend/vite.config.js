import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

// Which backend the dev server proxies /api to. Defaults to the
// local uvicorn on :8000, but a `.env.local` (or a `VITE_API_TARGET=`
// env var on the shell) can point at a Tailscale IP so the frontend
// can stay on my dev box while the backend runs elsewhere (Beelink /
// VPS / etc.). Frontend URL routing is unchanged — the proxy handles
// the swap transparently.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, path.resolve(__dirname, '.'), '')
  const API_TARGET = env.VITE_API_TARGET || 'http://localhost:8000'
  return {
  plugins: [react()],
  resolve: {
    alias: {
      // Match the `@/...` alias pattern shadcn-ui generated components
      // expect (per components.json). Lets future shadcn add commands
      // drop files into src/components/ui without manual import fixes.
      '@': path.resolve(__dirname, './src'),
    },
  },
  build: {
    // Manual chunk splits so the initial JS load doesn't ship every
    // sport panel + vendored deps as one 630 KB blob. Big libraries land
    // in named vendor chunks that browsers cache across deploys.
    rollupOptions: {
      output: {
        manualChunks: (id) => {
          if (id.includes('node_modules')) {
            if (id.includes('recharts') || id.includes('d3-')) return 'vendor-charts'
            if (id.includes('lucide-react')) return 'vendor-icons'
            if (id.includes('react-dom') || id.includes('/react/')) return 'vendor-react'
            return 'vendor-misc'
          }
          return undefined
        },
      },
    },
    chunkSizeWarningLimit: 700,
  },
  server: {
    // Bind 0.0.0.0 so devices on the Tailscale net (phone, iPad,
    // other boxes) can reach the dev server by the host's Tailscale
    // IP. Default is localhost-only, which blocks anything but the
    // dev machine itself.
    host: true,
    port: 5173,
    // Allow the Tailscale hostname + raw tailnet IPs in case Vite's
    // Host-header check tightens in future versions. Explicit list
    // keeps the dev server from 403'ing remote browsers. Raw IPs are
    // needed alongside `.ts.net` because Vite's suffix match doesn't
    // cover IP hosts.
    allowedHosts: ['desktop-jscmnio', '.ts.net', 'localhost',
                    '100.80.245.68'],
    proxy: {
      '/api': API_TARGET,
    },
  },
  }
})
