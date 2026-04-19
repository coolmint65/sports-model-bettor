/**
 * PickOfDayHero
 * ──────────────────────────────────────────────────────────────
 * The editorial "Pick of the Day" hero that sits at the top of
 * each sport's dashboard. This is the "Direction 3" headline
 * element from the UI redesign brief: one high-conviction play
 * called out above the scoreboard grid, with the running POTD
 * record off to the side so the user can gut-check the track
 * record in the same visual frame as today's call.
 *
 * Data source: /api/pick-of-day/{sport} + /summary endpoints.
 * Silently renders nothing when the backend returns an error or
 * "no pick today" message — the dashboard falls back to just
 * the scoreboard in that case.
 */

import { useEffect, useState } from 'react'
import axios from 'axios'

// Backend emits engine enum keys (Q1_ML, Q1_SPREAD, F5_OU, etc). Make
// them presentable without touching the engine side — those keys are
// also used as dict lookups in generate_*_picks.
const BET_TYPE_LABELS = {
  Q1_ML: 'Q1 ML',
  Q1_SPREAD: 'Q1 Spread',
  Q1_TOTAL: 'Q1 Total',
  F5_ML: 'F5 ML',
  F5_OU: 'F5 O/U',
  F5_SPREAD: 'F5 Spread',
  F5_RL: 'F5 Run Line',
  '1ST_INNING': '1st Inning',
}

function humanizeBetType(t) {
  if (!t) return ''
  return BET_TYPE_LABELS[t] || t.replace(/_/g, ' ')
}

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
  const profitTone = s.profit > 0 ? 'positive' : s.profit < 0 ? 'negative' : 'neutral'

  return (
    <div className="potd-hero">
      <div className="potd-accent-bar" aria-hidden="true" />

      <div className="potd-body">
        <div className="potd-primary">
          <div className="potd-eyebrow">
            {sport.toUpperCase()} PICK OF THE DAY
          </div>
          <div className="potd-pick">
            {potd.pick}
            {oddsStr && <span className="potd-odds">({oddsStr})</span>}
          </div>
          <div className="potd-matchup">
            {potd.matchup}
            {betTypeLabel && <> &middot; <span className="potd-bet-type">{betTypeLabel}</span></>}
          </div>
          <div className="potd-metrics">
            {modelStr && <span>Model: <strong>{modelStr}</strong></span>}
            {potd.edge != null && (
              <span>Edge: <strong className="text-positive">+{potd.edge.toFixed(1)}%</strong></span>
            )}
            {potd.kelly_pct != null && <span>Kelly: <strong>{potd.kelly_pct}%</strong></span>}
          </div>
          {potd.reasoning && (
            <div className="potd-reasoning">{potd.reasoning}</div>
          )}
        </div>

        {s.total > 0 && (
          <div className="potd-record">
            <div className="potd-record-label">POTD Record</div>
            <div className="potd-record-wl">{s.wins}-{s.losses}</div>
            <div className={`potd-record-profit potd-record-${profitTone}`}>
              {s.profit > 0 ? '+' : ''}${s.profit}
            </div>
            <div className="potd-record-meta">
              {s.win_pct}% WR &middot; {s.total} picks
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
