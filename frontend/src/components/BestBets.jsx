import { useEffect, useMemo, useRef, useState } from 'react'
import { humanizeBetType } from '../lib/betType'

export default function BestBets({ bets, loading }) {
  // Bucket the bets into confidence tiers once per `bets` reference
  // instead of running three full passes on every parent re-render
  // (App.jsx rerenders frequently during scoreboard refresh / nav
  // changes, and the list can exceed 30 games on big slate days).
  const { strong, moderate, lean } = useMemo(() => {
    if (!bets || bets.length === 0) {
      return { strong: [], moderate: [], lean: [] }
    }
    const s = [], m = [], l = []
    for (const b of bets) {
      if (b.confidence === 'strong') s.push(b)
      else if (b.confidence === 'moderate') m.push(b)
      else if (b.confidence === 'lean') l.push(b)
    }
    return { strong: s, moderate: m, lean: l }
  }, [bets])

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Analyzing today's slate...</p>
      </div>
    )
  }

  if (!bets || bets.length === 0) {
    return (
      <div className="no-games">
        <p>No bets available. Games may not have started yet or no edge found.</p>
      </div>
    )
  }

  return (
    <div className="best-bets-page">
      <h2 className="section-title">Today's Best Bets</h2>
      <p className="bb-subtitle">Ranked by model edge across all {bets.length} games</p>

      {strong.length > 0 && (
        <div className="bb-section">
          <h3 className="bb-tier strong">Strong Edge (8%+)</h3>
          {strong.map(b => <BetCard key={b.game_id} bet={b} />)}
        </div>
      )}

      {moderate.length > 0 && (
        <div className="bb-section">
          <h3 className="bb-tier moderate">Moderate Edge (4-8%)</h3>
          {moderate.map(b => <BetCard key={b.game_id} bet={b} />)}
        </div>
      )}

      {lean.length > 0 && (
        <div className="bb-section">
          <h3 className="bb-tier lean">Lean (1.5-4%)</h3>
          {lean.map(b => <BetCard key={b.game_id} bet={b} />)}
        </div>
      )}
    </div>
  )
}

function BetCard({ bet }) {
  const pct = n => `${(n * 100).toFixed(1)}%`
  const { best_pick, all_picks } = bet
  // Cache the formatted time per-card. With 30+ cards on a big slate,
  // this avoids spinning up 30 Date objects + Intl formatters on every
  // parent re-render.
  const gameTime = useMemo(
    () => new Date(bet.time).toLocaleTimeString(
      [], { hour: 'numeric', minute: '2-digit' },
    ),
    [bet.time],
  )

  // Pick-changed flash. When the model swaps the headline pick (line
  // moved enough to flip the recommendation), light up a "Pick updated"
  // badge for 30s so the user notices. Persists across the 5-min
  // auto-refresh via a localStorage key per (sport+game) so it doesn't
  // re-flash on every reload — only on actual changes.
  const pickKey = `${best_pick?.type}|${best_pick?.pick}`
  const storageKey = `pickseen:${bet.game_id}`
  const [flash, setFlash] = useState(false)
  const flashTimerRef = useRef(null)
  useEffect(() => {
    if (typeof window === 'undefined' || !best_pick) return
    let prev = null
    try {
      prev = window.localStorage.getItem(storageKey)
    } catch {
      // storage disabled — fall through, just won't flash
    }
    if (prev && prev !== pickKey) {
      setFlash(true)
      if (flashTimerRef.current) clearTimeout(flashTimerRef.current)
      flashTimerRef.current = setTimeout(() => setFlash(false), 30_000)
    }
    try {
      window.localStorage.setItem(storageKey, pickKey)
    } catch {}
    return () => {
      if (flashTimerRef.current) clearTimeout(flashTimerRef.current)
    }
  }, [pickKey, storageKey, best_pick])

  return (
    <div className={`bb-card conf-${bet.confidence}`}>
      <div className="bb-card-header">
        <div className="bb-matchup">
          {bet.away.logo && <img src={bet.away.logo} alt="" className="bb-logo" />}
          <span className="bb-teams">{bet.matchup}</span>
          {bet.home.logo && <img src={bet.home.logo} alt="" className="bb-logo" />}
        </div>
        <div className="bb-meta">
          <span>{gameTime}</span>
          {bet.venue && <span className="bb-venue">{bet.venue}</span>}
          {bet.generated_at && (
            <span className="bb-generated" title={`Picks computed at ${bet.generated_at}`}>
              picks as of {new Date(bet.generated_at).toLocaleTimeString(
                [], { hour: 'numeric', minute: '2-digit' })}
            </span>
          )}
        </div>
      </div>

      <div className="bb-card-body">
        {/* Best pick callout */}
        <div className={`bb-best-pick conf-${bet.confidence}`}>
          <div className="bb-pick-type">
            {humanizeBetType(best_pick.type)}
            {flash && (
              <span className="bb-pick-changed-badge"
                    title="Model swapped this pick since your last view (line moved)">
                Pick updated
              </span>
            )}
            {bet.is_locked && (
              <span className="bb-pick-locked-badge"
                    title="Game has started — pick is locked">
                🔒 Locked
              </span>
            )}
          </div>
          <div className="bb-pick-name">{best_pick.pick}</div>
          <div className="bb-pick-edge">+{best_pick.edge}% edge</div>
          <div className="bb-pick-prob">{pct(best_pick.prob)}</div>
        </div>

        {/* All picks for this game */}
        <div className="bb-all-picks">
          {all_picks.map((p, i) => (
            <div key={i} className={`bb-pick-row ${i === 0 ? 'best' : ''}`}>
              <span className="bb-pr-type">{humanizeBetType(p.type)}</span>
              <span className="bb-pr-pick">{p.pick}</span>
              <span className={`bb-pr-prob ${p.edge > 4 ? 'positive' : ''}`}>{pct(p.prob)}</span>
              <span className={`bb-pr-edge ${p.edge > 4 ? 'positive' : p.edge < 0 ? 'negative' : ''}`}>
                {p.edge > 0 ? '+' : ''}{p.edge}%
              </span>
            </div>
          ))}
        </div>

        {/* Pick odds */}
        <div className="bb-summary">
          {all_picks.slice(0, 4).map((p, i) => (
            p.odds && <span key={i}>{humanizeBetType(p.type)}: {p.odds > 0 ? '+' : ''}{p.odds}</span>
          ))}
        </div>
      </div>
    </div>
  )
}
