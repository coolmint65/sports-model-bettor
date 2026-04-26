import { useEffect, useMemo, useState } from 'react'
import { humanizeBetType } from '../lib/betType'
import { cn } from '../lib/utils'

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
  const [summary, setSummary] = useState(null)
  const [history, setHistory] = useState([])
  const [potd, setPotd] = useState(null)
  const [loading, setLoading] = useState(true)

  const refresh = () => {
    setLoading(true)
    Promise.all([
      api.get(`/${sport}/derivative-tracker/summary`).then(r => setSummary(r.data)),
      api.get(`/${sport}/derivative-tracker/history`).then(r => setHistory(r.data)),
      api.get(`/${sport}/derivative-pick-of-day`)
        .then(r => setPotd(r.data && !r.data.message && !r.data.error ? r.data : null))
        .catch(() => setPotd(null)),
    ])
      .catch(() => {})
      .finally(() => setLoading(false))
  }

  useEffect(() => { refresh() }, [sport])

  const settleNow = () => {
    setLoading(true)
    api.post(`/${sport}/derivative-tracker/settle`)
      .then(() => refresh())
      .catch(() => setLoading(false))
  }

  const pct = n => n != null ? `${(n * 100).toFixed(1)}%` : '-'

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

      {/* POTD callout */}
      {potd && potd.matchup && (
        <section className="rounded-lg border border-warning/40 bg-warning/5 p-5">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-warning">
            {sport.toUpperCase()} Derivative Pick of the Day
          </div>
          <div className="mt-1 text-lg font-bold text-foreground">
            {potd.matchup} · {humanizeBetType(potd.bet_type)}
          </div>
          <div className="mt-0.5 text-base font-semibold text-warning">
            {potd.pick}
          </div>
          <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span>edge {potd.edge != null ? `+${potd.edge.toFixed(1)}%` : '-'}</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">odds {potd.odds > 0 ? '+' : ''}{potd.odds}</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">prob {potd.model_prob ? `${(potd.model_prob * 100).toFixed(1)}%` : '-'}</span>
            {potd.kelly_pct != null && (
              <>
                <span className="text-border">·</span>
                <span className="tabular-nums">Kelly {potd.kelly_pct}%</span>
              </>
            )}
          </div>
        </section>
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
          <DerivativePicksTable picks={todaysPicks} pct={pct} />
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
          <DerivativePicksTable picks={pastPicks} pct={pct} />
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


function DerivativePicksTable({ picks, pct }) {
  return (
    <div className="overflow-x-auto">
      <table className="picks-table">
        <thead>
          <tr>
            <th>Date</th>
            <th>Matchup</th>
            <th>Market</th>
            <th>Pick</th>
            <th>Odds</th>
            <th>Prob</th>
            <th>Edge</th>
            <th>Result</th>
            <th>P/L</th>
          </tr>
        </thead>
        <tbody>
          {picks.map((p, i) => {
            const resultClass = p.result === 'W' ? 'row-win'
              : p.result === 'L' ? 'row-loss' : 'row-pending'
            return (
              <tr key={p.id || i} className={resultClass}>
                <td className="col-date">{p.date?.slice(5)}</td>
                <td className="col-matchup">{p.matchup}</td>
                <td><span className="type-badge">{humanizeBetType(p.bet_type)}</span></td>
                <td style={{ fontWeight: 600 }}>{p.pick}</td>
                <td style={{ color: '#94a3b8' }}>
                  {p.odds ? `${p.odds > 0 ? '+' : ''}${p.odds}` : '-'}
                </td>
                <td>{p.model_prob ? pct(p.model_prob) : '-'}</td>
                <td className={p.edge > 4 ? 'positive' : ''}>
                  {p.edge ? `+${p.edge.toFixed(1)}%` : '-'}
                </td>
                <td>
                  {p.result === 'W' && <span className="result-pill win">W</span>}
                  {p.result === 'L' && <span className="result-pill loss">L</span>}
                  {p.result === 'P' && <span className="result-pill push">P</span>}
                  {!p.result && <span className="result-pill pending">PEND</span>}
                </td>
                <td className={p.profit > 0 ? 'positive' : p.profit < 0 ? 'negative' : ''}
                    style={{ fontWeight: 600 }}>
                  {p.profit != null
                    ? `${p.profit > 0 ? '+' : ''}$${p.profit.toFixed(2)}`
                    : '-'}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
