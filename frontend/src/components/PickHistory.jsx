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
export default function PickHistory({ summary, history, loading, onRecord, onSettle }) {
  const overall = summary?.overall || {}
  const byType = summary?.by_type || {}

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

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-16 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading pick history…
      </div>
    )
  }

  const typesWithData = [
    { key: 'ML',         label: 'Moneyline' },
    { key: 'O/U',        label: 'Over/Under' },
    { key: '1st INN',    label: '1st Inning' },
    { key: 'F5',         label: 'First 5 Innings' },
    { key: 'RL',         label: 'Run Line' },
    { key: 'PL',         label: 'Puck Line' },
    { key: 'Q1_ML',      label: 'Q1 Moneyline' },
    { key: 'Q1_SPREAD',  label: 'Q1 Spread' },
    { key: 'Q1_TOTAL',   label: 'Q1 Total' },
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

      {/* Hero summary */}
      {overall.total > 0 && (
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

      {/* Today's picks */}
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
          <PicksTable picks={todaysPicks} />
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
          <PicksTable picks={pastPicks} />
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


