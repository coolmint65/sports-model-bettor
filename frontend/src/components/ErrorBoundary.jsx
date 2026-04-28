import { Component } from 'react'

/**
 * ErrorBoundary — top-level safety net.
 *
 * Without this, a single component throwing during render (e.g. an
 * API response shape change that breaks PropPickCard, an upstream
 * `null.matchup` access on a malformed bet object) blanks the entire
 * dashboard. The user reported wanting "bulletproof" — this is the
 * frontend half of that. Logs to console + reports to /api/_log
 * (best-effort, fire-and-forget) so the broken render gets surfaced
 * the next morning instead of being a silent UX failure.
 *
 * React class component because hooks have no equivalent of
 * componentDidCatch as of React 18.
 */
export default class ErrorBoundary extends Component {
  constructor(props) {
    super(props)
    this.state = { error: null, info: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    // Always log to console so devtools shows the stack.
    // eslint-disable-next-line no-console
    console.error('[ErrorBoundary]', error, info?.componentStack)
    this.setState({ info })

    // Best-effort report to the backend so failures the user hits in
    // production end up in the server logs even when they don't open
    // devtools. The fetch is fire-and-forget — never blocks recovery.
    try {
      fetch('/api/_client_error', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          message: String(error?.message || error),
          stack: String(error?.stack || ''),
          componentStack: String(info?.componentStack || ''),
          ts: new Date().toISOString(),
        }),
        keepalive: true,
      }).catch(() => { /* never propagate */ })
    } catch { /* never propagate */ }
  }

  reset = () => {
    this.setState({ error: null, info: null })
  }

  render() {
    if (this.state.error) {
      return (
        <div className="flex min-h-screen items-center justify-center bg-background p-6">
          <div className="max-w-lg space-y-3 rounded-lg border border-border bg-card p-6 shadow-lg">
            <h1 className="text-lg font-semibold tracking-tight text-foreground">
              Something went wrong
            </h1>
            <p className="text-sm text-muted-foreground">
              The dashboard hit an unexpected error. The details have been
              logged. Try reloading — if it keeps happening, capture this
              message and tell the developer:
            </p>
            <pre className="overflow-x-auto rounded bg-muted p-3 text-xs text-foreground">
              {String(this.state.error?.message || this.state.error)}
            </pre>
            <div className="flex gap-2">
              <button
                onClick={() => window.location.reload()}
                className="rounded-md bg-primary px-3 py-1.5 text-xs font-semibold text-primary-foreground hover:bg-primary/90"
              >
                Reload
              </button>
              <button
                onClick={this.reset}
                className="rounded-md border border-border bg-card px-3 py-1.5 text-xs font-semibold text-foreground hover:bg-accent"
              >
                Try again
              </button>
            </div>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
