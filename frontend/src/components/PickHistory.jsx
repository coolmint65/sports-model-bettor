import { useMemo } from 'react'
import { cn } from '../lib/utils'
import PicksTable from './primitives/PicksTable'

/**
 * PickHistory — Tracker / History tab content.
 *
 * Two tables (today's live activity + recent settled), a hero summary
 * (overall P/L + WR + CLV), and a row of per-bet-type tiles. Phase 2d
 * restyle: hero / tiles / shell on Tailwind + design tokens, table
 * keeps the legacy `.picks-table` styles for now (table-cell density
 * is its own redesign — Phase 2e polish).
 */
export default function PickHistory({ summary, history, loading, onRecord, onSettle, sport }) {
  const overall = summary?.overall || {}
  const byType = summary?.by_type || {}

  // Partition by settle status, not date. Tomorrow's pending picks
  // were landing in "Recent History" with the "settled" badge — wrong
  // and invisible. Active = anything without a result (today, tomorrow,
  // or older unsettled like postponed/canceled). Past = anything with
  // W/L/P/V. Inside Active, sort today before tomorrow before later.
  const { activePicks, pastPicks } = useMemo(() => {
    const now = new Date()
    const today = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`
    const active = []
    const past = []
    // Pending = no result yet. 'P' (push / refund) IS settled — same
    // way W/L are. Earlier this incorrectly grouped pushes into Active
    // because we used "no profit" as a proxy for pending.
    const isPending = (p) => !p.result || p.result === ''
    for (const p of history || []) {
      if (isPending(p)) active.push(p)
      else past.push(p)
    }
    // Active picks: today first (offset 0), then tomorrow (1), then
    // future, then any past-dated stragglers. Stable inside each day.
    const dayOffset = (p) => {
      if (!p.date) return 99
      const d = new Date(`${p.date}T00:00:00`)
      const t = new Date(`${today}T00:00:00`)
      const diff = Math.round((d - t) / 86400000)
      return diff < 0 ? 99 + Math.abs(diff) : diff
    }
    active.sort((a, b) => dayOffset(a) - dayOffset(b))
    return { activePicks: active, pastPicks: past }
  }, [history])

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-16 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading pick history…
      </div>
    )
  }

  const typesWithData = [
    { key: 'ML',                   label: 'Moneyline' },
    { key: 'O/U',                  label: 'Over/Under' },
    { key: '1st INN',              label: '1st Inning' },
    { key: 'F5',                   label: 'First 5 Innings' },
    { key: 'RL',                   label: 'Run Line' },
    { key: 'PL',                   label: 'Puck Line' },
    { key: 'Q1_ML',                label: 'Q1 Moneyline' },
    { key: 'Q1_SPREAD',            label: 'Q1 Spread' },
    { key: 'Q1_TOTAL',             label: 'Q1 Total' },
    // Basketball framework bet types (WNBA / NCAAM / Euroleague).
    // The framework's _picks emits these as ML / SPREAD / TOTAL, so
    // ML reuses the existing key above; SPREAD + TOTAL are basketball-
    // specific labels.
    { key: 'SPREAD',               label: 'Spread' },
    { key: 'TOTAL',                label: 'Total' },
    // Tennis bet types — same tile shape, separate keys.
    { key: 'SET_BETTING',          label: 'Set Betting' },
    { key: 'SET_SPREAD',           label: 'Set Spread' },
    { key: 'TOTAL_GAMES',          label: 'Total Games' },
    { key: 'TOTAL_SETS',           label: 'Total Sets' },
    { key: 'WIN_AT_LEAST_ONE_SET', label: 'Win 1+ Set' },
    { key: 'P1_TOTAL_GAMES',       label: 'P1 Total Games' },
    { key: 'P2_TOTAL_GAMES',       label: 'P2 Total Games' },
  ].filter(({ key }) => byType[key] && byType[key].total > 0)

  const profitTone = overall.profit > 0
    ? 'text-positive' : overall.profit < 0 ? 'text-negative' : 'text-foreground'

  return (
    <div className="space-y-5 py-4">
      {/* Header + actions */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">
            Pick Tracker
          </h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Live and settled picks for the active sport.
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={onRecord}
            className="rounded-md border border-border bg-card px-3 py-1.5 text-xs font-medium text-foreground hover:bg-accent transition-colors"
          >
            Record today
          </button>
          <button
            onClick={onSettle}
            className="rounded-md bg-positive-strong px-3 py-1.5 text-xs font-semibold text-background hover:opacity-90 transition-opacity"
          >
            Settle completed
          </button>
        </div>
      </div>

      {/* Hero summary — show when there's any activity (pending or
          settled). Earlier this gated on settled-only and the hero
          disappeared entirely for fresh leagues that only had pending
          picks; the user sees "0-0 · $0 · N pending" instead. */}
      {(overall.total > 0 || overall.pending > 0) && (
        <section className="rounded-lg border border-border bg-card p-5">
          <div className="flex flex-col gap-1">
            <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
              Overall P/L
            </div>
            <div className={cn('text-3xl font-bold tabular-nums', profitTone)}>
              {overall.profit > 0 ? '+' : ''}${overall.profit}
            </div>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
              <span className="tabular-nums">{overall.wins}-{overall.losses}</span>
              <span className="text-border">·</span>
              <span className="tabular-nums">{overall.win_pct}% WR</span>
              <span className="text-border">·</span>
              <span className="tabular-nums">{overall.total} picks</span>
              {overall.avg_clv != null && (
                <>
                  <span className="text-border">·</span>
                  <span
                    title={`Average Closing Line Value across ${overall.clv_sample || 0} settled picks. Positive = beating close (sharp).`}
                    className={cn(
                      'tabular-nums font-semibold cursor-help',
                      overall.avg_clv > 0 ? 'text-positive' : overall.avg_clv < 0 ? 'text-negative' : '',
                    )}
                  >
                    CLV {overall.avg_clv > 0 ? '+' : ''}{overall.avg_clv}%
                  </span>
                </>
              )}
              {overall.pending > 0 && (
                <>
                  <span className="text-border">·</span>
                  <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning font-semibold tabular-nums">
                    {overall.pending} pending
                  </span>
                </>
              )}
            </div>
          </div>

          {/* Per-bet-type tiles */}
          {typesWithData.length > 0 && (
            <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5">
              {typesWithData.map(({ key, label }) => {
                const s = byType[key]
                const tone = s.profit > 0
                  ? 'text-positive' : s.profit < 0 ? 'text-negative' : 'text-foreground'
                return (
                  <div
                    key={key}
                    className="rounded-md border border-border bg-background/50 px-3 py-2.5"
                  >
                    <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground truncate">
                      {label}
                    </div>
                    <div className={cn('mt-0.5 text-base font-semibold tabular-nums', tone)}>
                      {s.profit > 0 ? '+' : ''}${s.profit}
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

      {/* Active picks — pending across today + tomorrow + later */}
      {activePicks.length > 0 && (
        <section className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold text-foreground">
              Active Picks
            </h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {activePicks.length} pending
            </span>
          </div>
          <PicksTable picks={activePicks} sport={sport} />
        </section>
      )}

      {/* Past picks */}
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
          <PicksTable picks={pastPicks} sport={sport} />
        </section>
      )}

      {(!history || history.length === 0) && (!summary || overall.total === 0) && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
          <div className="text-sm font-semibold text-foreground">No picks recorded yet.</div>
          <div className="mt-1 text-xs text-muted-foreground">
            Click "Record today" to start tracking the slate.
          </div>
        </div>
      )}
    </div>
  )
}


