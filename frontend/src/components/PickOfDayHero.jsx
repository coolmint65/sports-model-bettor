/**
 * PickOfDayHero — full-bleed editorial hero atop the Bets tab.
 *
 * Phase 2d-v restyle: Tailwind tokens, structured layout (primary
 * pick column on the left, running record on the right). The accent
 * bar is a thin gradient strip at the top of the card so the eye
 * lands on the bold pick text rather than the chrome.
 *
 * Silently renders nothing when the backend returns an error or
 * "no pick today" message — the dashboard falls back to just the
 * scoreboard in that case.
 */

import { useEffect, useState } from 'react'
import axios from 'axios'
import { humanizeBetType } from '../lib/betType'
import { cn } from '../lib/utils'

export default function PickOfDayHero({ sport }) {
  const [potd, setPotd] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    if (loaded) return
    setLoaded(true)
    const a = axios.create({ baseURL: '/api' })
    Promise.all([
      a.get(`/pick-of-day/${sport}`),
      a.get(`/pick-of-day/${sport}/summary`),
    ]).then(([p, s]) => {
      setPotd(p.data)
      setSummary(s.data)
    }).catch(() => {})
  }, [sport, loaded])

  if (!potd || potd.error || potd.message) return null

  const s = summary || {}
  const odds = potd.odds
  const oddsStr = odds ? `${odds > 0 ? '+' : ''}${odds}` : ''
  const betTypeLabel = humanizeBetType(potd.bet_type)
  const modelStr =
    potd.model_prob != null && !isNaN(potd.model_prob)
      ? `${(potd.model_prob * 100).toFixed(1)}%`
      : null
  const profitTone = s.profit > 0
    ? 'text-positive' : s.profit < 0 ? 'text-negative' : 'text-foreground'

  return (
    <section className="relative overflow-hidden rounded-xl border border-border bg-card mb-5">
      {/* Top accent strip — subtle gradient signals "editorial pick". */}
      <div
        aria-hidden="true"
        className="absolute inset-x-0 top-0 h-0.5 bg-gradient-to-r from-positive via-primary to-warning"
      />

      <div className="flex flex-col gap-4 p-5 sm:flex-row sm:items-start sm:justify-between">
        {/* Primary column — pick + matchup + metrics + reasoning */}
        <div className="min-w-0 flex-1">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-positive">
            {sport.toUpperCase()} Pick of the Day
          </div>

          <div className="mt-1.5 flex items-baseline gap-2 flex-wrap">
            <span className="text-2xl font-bold tracking-tight text-foreground">
              {potd.pick}
            </span>
            {oddsStr && (
              <span className="text-sm font-semibold tabular-nums text-muted-foreground">
                ({oddsStr})
              </span>
            )}
          </div>

          <div className="mt-1 text-sm text-muted-foreground">
            <span className="font-medium text-foreground">{potd.matchup}</span>
            {betTypeLabel && (
              <>
                <span className="text-border mx-1.5">·</span>
                <span>{betTypeLabel}</span>
              </>
            )}
          </div>

          <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
            {modelStr && (
              <span>
                Model <strong className="font-semibold tabular-nums text-foreground">{modelStr}</strong>
              </span>
            )}
            {potd.edge != null && (
              <span>
                Edge <strong className="font-semibold tabular-nums text-positive">+{potd.edge.toFixed(1)}%</strong>
              </span>
            )}
          </div>

          {potd.reasoning && (
            <p className="mt-3 max-w-2xl text-xs leading-relaxed text-muted-foreground">
              {potd.reasoning}
            </p>
          )}
        </div>

        {/* Record column — running W/L + P/L for the POTD selector */}
        {s.total > 0 && (
          <div className="flex sm:flex-col sm:items-end sm:text-right gap-x-4 gap-y-1 sm:gap-y-0.5 border-t border-border pt-3 sm:border-t-0 sm:pt-0 sm:border-l sm:pl-5">
            <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
              POTD Record
            </div>
            <div className="text-2xl font-bold tabular-nums text-foreground sm:mt-1">
              {s.wins}-{s.losses}
            </div>
            <div className={cn('text-sm font-semibold tabular-nums', profitTone)}>
              {s.profit > 0 ? '+' : ''}${s.profit}
            </div>
            <div className="text-[11px] text-muted-foreground tabular-nums">
              {s.win_pct}% WR · {s.total} picks
            </div>
          </div>
        )}
      </div>
    </section>
  )
}
