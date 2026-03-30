export default function BestBets({ bets, loading }) {
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

  const strong = bets.filter(b => b.confidence === 'strong')
  const moderate = bets.filter(b => b.confidence === 'moderate')
  const lean = bets.filter(b => b.confidence === 'lean')

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
  const { best_pick, all_picks, prediction_summary: ps, situational: sit } = bet

  return (
    <div className={`bb-card conf-${bet.confidence}`}>
      <div className="bb-card-header">
        <div className="bb-matchup">
          {bet.away.logo && <img src={bet.away.logo} alt="" className="bb-logo" />}
          <span className="bb-teams">{bet.matchup}</span>
          {bet.home.logo && <img src={bet.home.logo} alt="" className="bb-logo" />}
        </div>
        <div className="bb-meta">
          <span>{new Date(bet.time).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })}</span>
          {bet.venue && <span className="bb-venue">{bet.venue}</span>}
        </div>
      </div>

      <div className="bb-card-body">
        {/* Best pick callout */}
        <div className={`bb-best-pick conf-${bet.confidence}`}>
          <div className="bb-pick-type">{best_pick.type}</div>
          <div className="bb-pick-name">{best_pick.pick}</div>
          <div className="bb-pick-edge">+{best_pick.edge}% edge</div>
          <div className="bb-pick-prob">{pct(best_pick.prob)}</div>
        </div>

        {/* All picks for this game */}
        <div className="bb-all-picks">
          {all_picks.map((p, i) => (
            <div key={i} className={`bb-pick-row ${i === 0 ? 'best' : ''}`}>
              <span className="bb-pr-type">{p.type}</span>
              <span className="bb-pr-pick">{p.pick}</span>
              <span className={`bb-pr-prob ${p.edge > 4 ? 'positive' : ''}`}>{pct(p.prob)}</span>
              <span className={`bb-pr-edge ${p.edge > 4 ? 'positive' : p.edge < 0 ? 'negative' : ''}`}>
                {p.edge > 0 ? '+' : ''}{p.edge}%
              </span>
            </div>
          ))}
        </div>

        {/* Quick prediction summary */}
        <div className="bb-summary">
          <span>Score: {ps.home_score}-{ps.away_score}</span>
          <span>Total: {ps.total}</span>
          <span>WP: {pct(Math.max(ps.home_wp, ps.away_wp))}</span>
          {sit && sit.weather !== 1.0 && (
            <span className={sit.weather > 1.02 ? 'positive' : sit.weather < 0.98 ? 'negative' : ''}>
              Wx: {sit.weather.toFixed(2)}x
            </span>
          )}
        </div>
      </div>
    </div>
  )
}
