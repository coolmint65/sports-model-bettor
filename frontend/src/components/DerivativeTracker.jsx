import { useEffect, useMemo, useState } from 'react'
import { humanizeBetType } from '../lib/betType'
import { cn } from '../lib/utils'
import PicksTable from './primitives/PicksTable'
import PotdHero from './PotdHero'
import { cachedGet, peek, invalidate } from '../lib/apiCache'

// Derivative ROI breaks down to cents on small-edge alt markets, so
// the table needs 2-decimal precision on the P/L column. PickHistory
// is whole-dollar; this overrides via the profitFormatter prop.
const derivProfitFmt = n => `$${n.toFixed(2)}`

/**
 * DerivativeTracker — Tracker tab default content.
 *
 * Paper-bet log for Phase 1 derivative markets, separated from the
 * core PickHistory so derivative profitability tracks in isolation.
 * Phase 2d restyle: hero / POTD callout / per-bet-type tiles / shell
 * on Tailwind. Table uses the existing `.picks-table` styles for
 * consistency with PickHistory until table redesign in 2e.
 *
 * Props:
 *   sport — 'mlb' | 'nhl' | 'nba'
 *   api   — axios-shaped client with .get() / .post()
 */
export default function DerivativeTracker({ sport, api }) {
  const summaryUrl = `/${sport}/derivative-tracker/summary`
  const historyUrl = `/${sport}/derivative-tracker/history`
  const potdUrl    = `/${sport}/derivative-pick-of-day`

  // Hydrate from cache so a tab-switch back doesn't flash a spinner.
  const [summary, setSummary] = useState(() => peek(summaryUrl) ?? null)
  const [history, setHistory] = useState(() => peek(historyUrl) ?? [])
  const [potd, setPotd] = useState(() => {
    const p = peek(potdUrl)
    return p && !p.message && !p.error ? p : null
  })
  const [loading, setLoading] = useState(peek(summaryUrl) === undefined)

  const refresh = () => {
    setLoading(true)
    Promise.all([
      cachedGet(summaryUrl).then(d => setSummary(d)).catch(() => {}),
      cachedGet(historyUrl).then(d => setHistory(d)).catch(() => {}),
      cachedGet(potdUrl)
        .then(d => setPotd(d && !d.message && !d.error ? d : null))
        .catch(() => setPotd(null)),
    ])
      .catch(() => {})
      .finally(() => setLoading(false))
  }

  useEffect(() => { refresh() }, [sport])  // eslint-disable-line react-hooks/exhaustive-deps

  const settleNow = () => {
    setLoading(true)
    api.post(`/${sport}/derivative-tracker/settle`)
      .then(() => {
        // Settle moves rows pending → settled; drop cached views so
        // the refetch reflects the new state instead of the stale one.
        invalidate(summaryUrl)
        invalidate(historyUrl)
        invalidate(potdUrl)
        refresh()
      })
      .catch(() => setLoading(false))
  }

  const { todaysPicks, pastPicks } = useMemo(() => {
    const now = new Date()
    const today = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`
    const todays = []
    const past = []
    for (const p of history || []) {
      if (p.date === today) todays.push(p)
      else past.push(p)
    }
    return { todaysPicks: todays, pastPicks: past }
  }, [history])

  if (loading && !summary) {
    return (
      <div className="flex items-center justify-center gap-3 py-16 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading derivative tracker…
      </div>
    )
  }

  const grand = summary?._grand || {}
  const typesWithData = summary
    ? Object.entries(summary)
        .filter(([k, v]) => k !== '_grand' && (v?.total || 0) > 0)
        .sort(([, a], [, b]) => {
          if ((b.roi || 0) !== (a.roi || 0)) return (b.roi || 0) - (a.roi || 0)
          return (b.total || 0) - (a.total || 0)
        })
    : []

  const profitTone = grand.profit > 0
    ? 'text-positive' : grand.profit < 0 ? 'text-negative' : 'text-foreground'

  return (
    <div className="space-y-5 py-4">
      {/* Header + actions */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">
            {sport.toUpperCase()} Derivative Tracker
          </h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Paper-bet log · top 1 per game above 4% edge
          </p>
        </div>
        <button
          onClick={settleNow}
          className="rounded-md bg-positive-strong px-3 py-1.5 text-xs font-semibold text-background hover:opacity-90 transition-opacity"
        >
          Settle completed
        </button>
      </div>

      {/* Derivative POTD — same shell as the core POTD card so the
          two read as siblings. Amber accent keeps them visually
          distinct. */}
      {potd && potd.matchup && (
        <PotdHero
          label={`${sport.toUpperCase()} · Derivative Pick of the Day`}
          sport={sport}
          pick={potd}
          accent="warning"
        />
      )}

      {/* Hero summary */}
      {grand.total > 0 && (
        <section className="rounded-lg border border-border bg-card p-5">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
            Derivative P/L
          </div>
          <div className={cn('mt-1 text-3xl font-bold tabular-nums', profitTone)}>
            {grand.profit > 0 ? '+' : ''}${(grand.profit || 0).toFixed(2)}
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span className="tabular-nums">{grand.wins || 0}-{grand.losses || 0}</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">{grand.win_pct || 0}% WR</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">{grand.total || 0} picks</span>
            {grand.pending > 0 && (
              <>
                <span className="text-border">·</span>
                <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning font-semibold tabular-nums">
                  {grand.pending} pending
                </span>
              </>
            )}
          </div>

          {typesWithData.length > 0 && (
            <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5">
              {typesWithData.map(([key, s]) => {
                const tone = s.profit > 0
                  ? 'text-positive' : s.profit < 0 ? 'text-negative' : 'text-foreground'
                return (
                  <div
                    key={key}
                    className="rounded-md border border-border bg-background/50 px-3 py-2.5"
                  >
                    <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground truncate">
                      {humanizeBetType(key)}
                    </div>
                    <div className={cn('mt-0.5 text-base font-semibold tabular-nums', tone)}>
                      {s.profit > 0 ? '+' : ''}${(s.profit || 0).toFixed(2)}
                    </div>
                    <div className="text-[10px] text-muted-foreground tabular-nums">
                      {s.wins}-{s.losses} ({s.win_pct}%)
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </section>
      )}

      {todaysPicks.length > 0 && (
        <section className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold text-foreground">
              Today's Picks
            </h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {todaysPicks.length} live
            </span>
          </div>
          <PicksTable
            picks={todaysPicks}
            typeColumnLabel="Market"
            profitFormatter={derivProfitFmt}
          />
        </section>
      )}

      {pastPicks.length > 0 && (
        <section className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold text-foreground">
              Recent History
            </h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {pastPicks.length} settled
            </span>
          </div>
          <PicksTable
            picks={pastPicks}
            typeColumnLabel="Market"
            profitFormatter={derivProfitFmt}
          />
        </section>
      )}

      {(!history || history.length === 0) && (!summary || grand.total === 0) && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
          <div className="text-sm font-semibold text-foreground">
            No derivative picks logged yet.
          </div>
          <div className="mt-1 text-xs text-muted-foreground">
            Auto-records on each best-bets refresh once today's slate has
            playable derivative edges.
          </div>
        </div>
      )}
    </div>
  )
}


