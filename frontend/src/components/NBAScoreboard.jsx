export default function NBAScoreboard({ games, loading, onSelectGame, bestBets }) {
  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Loading NBA games...</p>
      </div>
    )
  }

  if (!games || games.length === 0) {
    return (
      <div className="no-games">
        <p>No NBA games scheduled today.</p>
        <p className="sub">Check back for the next slate.</p>
      </div>
    )
  }

  const betMap = {}
  if (bestBets) {
    for (const b of bestBets) {
      betMap[b.game_id] = b
    }
  }

  // Split games into active (pregame/live) and finals
  const activeGames = []
  const finalGames = []
  for (const g of games) {
    if (g.status?.state === 'post' || g.status?.completed) {
      finalGames.push(g)
    } else {
      activeGames.push(g)
    }
  }

  // Sort active games by edge (highest first)
  activeGames.sort((a, b) => {
    const aEdge = betMap[a.id]?.best_pick?.edge || -99
    const bEdge = betMap[b.id]?.best_pick?.edge || -99
    return bEdge - aEdge
  })

  const edgeCount = activeGames.filter(g =>
    betMap[g.id]?.confidence === 'strong' || betMap[g.id]?.confidence === 'moderate'
  ).length

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        NBA Games ({games.length})
        {edgeCount > 0 && <span className="edge-count">{edgeCount} Q1 plays with edge</span>}
      </h2>

      {activeGames.length > 0 && (
        <>
          {finalGames.length > 0 && (
            <div className="games-section-header">
              {activeGames.some(g => g.status?.state === 'in') ? 'Live & Upcoming' : 'Upcoming'} ({activeGames.length})
            </div>
          )}
          <div className="games-feature-grid">
            {activeGames.map(game => (
              <NBAGameCard
                key={game.id}
                game={game}
                bet={betMap[game.id]}
                onClick={() => onSelectGame && onSelectGame(game)}
              />
            ))}
          </div>
        </>
      )}

      {finalGames.length > 0 && (
        <>
          <div className="games-section-header">Final ({finalGames.length})</div>
          <div className="games-finals-grid">
            {finalGames.map(game => (
              <NBAFinalRow
                key={game.id}
                game={game}
                bet={betMap[game.id]}
                onClick={() => onSelectGame && onSelectGame(game)}
              />
            ))}
          </div>
        </>
      )}
    </div>
  )
}


function NBAFinalRow({ game, bet, onClick }) {
  const { home, away } = game
  const hs = parseInt(home.score) || 0
  const as = parseInt(away.score) || 0
  const homeWon = hs > as

  // Extract Q1 scores from linescores
  const hQ1 = game.home_linescores?.[0] ?? null
  const aQ1 = game.away_linescores?.[0] ?? null

  return (
    <div className="game-final-row" onClick={onClick}>
      <span className="final-label">FINAL</span>
      <div className="final-teams">
        <div className="final-team">
          {away.logo && <img src={away.logo} alt="" />}
          <span className={`final-abbr ${!homeWon ? 'winner' : ''}`}>{away.abbreviation}</span>
        </div>
        <span className={`final-score ${!homeWon ? 'winner' : ''}`}>{as}</span>
        <span className="final-dash">—</span>
        <span className={`final-score ${homeWon ? 'winner' : ''}`}>{hs}</span>
        <div className="final-team">
          {home.logo && <img src={home.logo} alt="" />}
          <span className={`final-abbr ${homeWon ? 'winner' : ''}`}>{home.abbreviation}</span>
        </div>
      </div>
      {hQ1 != null && aQ1 != null && (
        <div className="q1-final-line">
          Q1: {aQ1} - {hQ1}
        </div>
      )}
    </div>
  )
}


function NBAGameCard({ game, bet, onClick }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'
  const conf = bet?.confidence || 'skip'

  const rest = bet?.rest || {}
  const homeB2B = rest.home_b2b
  const awayB2B = rest.away_b2b
  const homeRest = rest.home_rest_advantage && !rest.away_rest_advantage
  const awayRest = rest.away_rest_advantage && !rest.home_rest_advantage

  // Q1 scores from linescores
  const hQ1 = game.home_linescores?.[0] ?? null
  const aQ1 = game.away_linescores?.[0] ?? null
  const hasQ1 = hQ1 != null && aQ1 != null

  // Current quarter for live games
  const quarter = game.quarter || status.period || null

  return (
    <div className={`game-card ${isLive ? 'live' : ''} card-${conf}`} onClick={onClick}>
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      {/* Q1 Spread pick badge — only for pregame games */}
      {isPre && bet && bet.best_pick && conf !== 'skip' && (
        <div className={`pick-badge q1-badge badge-${conf}`}>
          <span className="pick-badge-type">Q1 SPREAD</span>
          <span className="pick-badge-pick">{bet.best_pick.pick}</span>
          <span className="pick-badge-edge">+{bet.best_pick.edge}%</span>
        </div>
      )}

      {/* Rest / back-to-back indicators */}
      {isPre && (homeB2B || awayB2B || homeRest || awayRest) && (
        <div style={{display:'flex',gap:4,flexWrap:'wrap',marginBottom:6}}>
          {awayB2B && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(239,68,68,0.15)',color:'#ef4444',border:'1px solid rgba(239,68,68,0.3)'}}>
              {away.abbreviation} B2B
            </span>
          )}
          {homeB2B && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(239,68,68,0.15)',color:'#ef4444',border:'1px solid rgba(239,68,68,0.3)'}}>
              {home.abbreviation} B2B
            </span>
          )}
          {awayRest && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(96,165,250,0.12)',color:'#60a5fa',border:'1px solid rgba(96,165,250,0.25)'}}>
              {away.abbreviation} rested
            </span>
          )}
          {homeRest && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(96,165,250,0.12)',color:'#60a5fa',border:'1px solid rgba(96,165,250,0.25)'}}>
              {home.abbreviation} rested
            </span>
          )}
        </div>
      )}

      <div className="game-teams">
        <NBATeamRow team={away} isLive={isLive} isFinal={isFinal} />
        <div className="game-at">@</div>
        <NBATeamRow team={home} isLive={isLive} isFinal={isFinal} />
      </div>

      {/* Q1 score — prominent display for live/final */}
      {(isLive || isFinal) && hasQ1 && (
        <div className="q1-score-display">
          <span className="q1-score-label">Q1:</span>
          <span className="q1-score-value">{away.abbreviation} {aQ1} - {home.abbreviation} {hQ1}</span>
          {isLive && quarter && (
            <span className="q1-quarter-indicator">
              {quarter <= 1 ? 'Q1' : `Q${quarter}`}
            </span>
          )}
        </div>
      )}

      {/* Win probability bar */}
      {isPre && bet?.win_prob?.home != null && (
        <Q1ProbBar wp={bet.win_prob} home={home} away={away} />
      )}

      {/* Key insight line */}
      {isPre && bet && bet.best_pick && conf !== 'skip' && (
        <NBACardInsight bet={bet} home={home} away={away} />
      )}

      {/* Q1 Odds — only Q1 markets, not full game */}
      {game.odds && (
        <div className="game-odds-grid">
          {/* Q1 Spread */}
          {(game.odds.q1_home_spread != null || game.odds.home_spread_point != null) && (
            <div className="odds-line">
              <span className="odds-label q1-odds-label">Q1 SPR</span>
              <span className="odds-val">
                {away.abbreviation} {game.odds.q1_away_spread != null
                  ? `${game.odds.q1_away_spread > 0 ? '+' : ''}${game.odds.q1_away_spread}`
                  : game.odds.away_spread_point != null
                    ? `${game.odds.away_spread_point > 0 ? '+' : ''}${game.odds.away_spread_point}`
                    : '-'}
                {game.odds.q1_away_spread_odds
                  ? ` (${game.odds.q1_away_spread_odds > 0 ? '+' : ''}${Math.round(game.odds.q1_away_spread_odds)})`
                  : ''}
              </span>
              <span className="odds-val">
                {home.abbreviation} {game.odds.q1_home_spread != null
                  ? `${game.odds.q1_home_spread > 0 ? '+' : ''}${game.odds.q1_home_spread}`
                  : game.odds.home_spread_point != null
                    ? `${game.odds.home_spread_point > 0 ? '+' : ''}${game.odds.home_spread_point}`
                    : '-'}
                {game.odds.q1_home_spread_odds
                  ? ` (${game.odds.q1_home_spread_odds > 0 ? '+' : ''}${Math.round(game.odds.q1_home_spread_odds)})`
                  : ''}
              </span>
            </div>
          )}
          {/* Q1 Total */}
          {game.odds.q1_total && (
            <div className="odds-line">
              <span className="odds-label q1-odds-label">Q1 O/U</span>
              <span className="odds-val">
                o{game.odds.q1_total}
                {game.odds.q1_over_odds ? ` (${game.odds.q1_over_odds > 0 ? '+' : ''}${Math.round(game.odds.q1_over_odds)})` : ''}
              </span>
              <span className="odds-val">
                u{game.odds.q1_total}
                {game.odds.q1_under_odds ? ` (${game.odds.q1_under_odds > 0 ? '+' : ''}${Math.round(game.odds.q1_under_odds)})` : ''}
              </span>
            </div>
          )}
          {/* Q1 ML */}
          {(game.odds.q1_home_ml || game.odds.home_ml) && (
            <div className="odds-line">
              <span className="odds-label q1-odds-label">Q1 ML</span>
              <span className="odds-val">
                {away.abbreviation} {(game.odds.q1_away_ml || game.odds.away_ml) > 0 ? '+' : ''}{game.odds.q1_away_ml || game.odds.away_ml || '-'}
              </span>
              <span className="odds-val">
                {home.abbreviation} {(game.odds.q1_home_ml || game.odds.home_ml) > 0 ? '+' : ''}{game.odds.q1_home_ml || game.odds.home_ml || '-'}
              </span>
            </div>
          )}
        </div>
      )}

      {/* Game time or status */}
      <div className="game-meta">
        {isPre && (
          <span className="game-time">
            {new Date(game.date).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })}
          </span>
        )}
        {isLive && <span className="game-inning">{status.detail}</span>}
        {game.broadcast && <span className="game-broadcast">{game.broadcast}</span>}
      </div>
    </div>
  )
}


function NBATeamRow({ team, isLive, isFinal }) {
  return (
    <div className="game-team">
      {team.logo && <img src={team.logo} alt="" className="team-logo" />}
      <span className="team-abbr">{team.abbreviation}</span>
      <span className="team-name">{team.name}</span>
      <span className="team-record">{team.record}</span>
      {(isLive || isFinal) && (
        <span className={`game-score ${team.winner ? 'winner' : ''}`}>{team.score}</span>
      )}
    </div>
  )
}


function Q1ProbBar({ wp, home, away }) {
  const hPct = Math.round(wp.home * 100)
  const aPct = Math.round(wp.away * 100)
  const homeFavored = wp.home > wp.away

  return (
    <>
      <div className="wp-labels">
        <span className={!homeFavored ? 'wp-favored' : ''}>{away.abbreviation} {aPct}%</span>
        <span className={homeFavored ? 'wp-favored' : ''}>{home.abbreviation} {hPct}%</span>
      </div>
      <div className="wp-bar-card">
        <div className="wp-away" style={{ width: `${aPct}%` }} />
        <div className="wp-home" style={{ width: `${hPct}%` }} />
      </div>
    </>
  )
}


function NBACardInsight({ bet, home, away }) {
  const f = bet.factors || {}
  const wp = bet.win_prob || {}
  const ctx = bet.season_context || {}
  const rest = bet.rest || {}

  const reasons = []

  // Q1 offensive rating edge
  if (f.home_q1_ortg && f.away_q1_ortg) {
    const diff = Math.abs(f.home_q1_ortg - f.away_q1_ortg)
    if (diff > 3) {
      const better = f.home_q1_ortg > f.away_q1_ortg ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: diff,
        text: <><strong>{better}</strong> has superior Q1 offense ({Math.max(f.home_q1_ortg, f.away_q1_ortg).toFixed(1)} ORtg)</>
      })
    }
  }

  // Pace advantage
  if (f.home_pace && f.away_pace) {
    const diff = Math.abs(f.home_pace - f.away_pace)
    if (diff > 3) {
      const faster = f.home_pace > f.away_pace ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: diff,
        text: <><strong>{faster}</strong> plays at a much faster pace — more Q1 possessions</>
      })
    }
  }

  // Recent form
  const hL10 = ctx.home?.l10_win_pct
  const aL10 = ctx.away?.l10_win_pct
  if (hL10 != null && aL10 != null && Math.abs(hL10 - aL10) > 0.3) {
    const hotter = hL10 > aL10 ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(hL10 - aL10) * 20,
      text: <><strong>{hotter}</strong> hot in L10 ({(Math.max(hL10, aL10) * 100).toFixed(0)}% WR)</>
    })
  }

  // Rest advantage
  if (rest.home_b2b || rest.away_b2b) {
    const tired = rest.home_b2b ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: 5,
      text: <><strong>{tired}</strong> on back-to-back (fatigue penalty)</>
    })
  }

  // Home court advantage
  if (f.home_court_edge && f.home_court_edge > 2) {
    reasons.push({
      weight: f.home_court_edge,
      text: <><strong>{home.abbreviation}</strong> strong home court edge (+{f.home_court_edge.toFixed(1)} pts)</>
    })
  }

  if (reasons.length === 0) return null

  reasons.sort((a, b) => b.weight - a.weight)
  return (
    <div className="card-insight">
      {reasons[0].text}
    </div>
  )
}
