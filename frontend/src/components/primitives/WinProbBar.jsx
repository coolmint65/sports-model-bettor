/**
 * WinProbBar
 * ──────────────────────────────────────────────────────────────
 * Two-color horizontal bar showing home vs away win probability.
 * Two visual sizes:
 *   variant="card"   -> compact bar inside scorecards (.wp-bar-card)
 *   variant="detail" -> larger bar inside game-detail panes (.prob-bar)
 *
 * Both variants share the same underlying data shape:
 *   wp = { home: 0..1, away: 0..1 }
 * Replaces three near-identical local WinProbBar components in
 * Scoreboard.jsx, NHLScoreboard.jsx and the prob-bar JSX duplicated
 * inside NHLGameDetail / NBAGameDetail / PredictionResults.
 */

function pct(v) {
  return `${Math.round((v || 0) * 100)}%`
}

export default function WinProbBar({ wp, home, away, variant = 'card' }) {
  const h = wp?.home || 0
  const a = wp?.away || 0
  const homeFavored = h > a

  if (variant === 'detail') {
    return (
      <div className="prob-bar-container">
        <div className="prob-bar-labels">
          <span className={homeFavored ? 'favored' : ''}>{home.abbreviation} {pct(h)}</span>
          <span className={!homeFavored ? 'favored' : ''}>{away.abbreviation} {pct(a)}</span>
        </div>
        <div className="prob-bar">
          <div className="home" style={{ width: pct(h) }} />
          <div className="away" style={{ width: pct(a) }} />
        </div>
      </div>
    )
  }

  // card variant: away first to match the existing scorecard layout
  return (
    <>
      <div className="wp-labels">
        <span className={!homeFavored ? 'wp-favored' : ''}>{away.abbreviation} {pct(a)}</span>
        <span className={homeFavored ? 'wp-favored' : ''}>{home.abbreviation} {pct(h)}</span>
      </div>
      <div className="wp-bar-card">
        <div className="wp-away" style={{ width: pct(a) }} />
        <div className="wp-home" style={{ width: pct(h) }} />
      </div>
    </>
  )
}
