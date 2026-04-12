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
  const q1 = game.q1 || {}

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
      {/* Q1 score in final row */}
      {q1.home != null && q1.away != null && (
        <div className="q1-final-line">
          Q1: {away.abbreviation} {q1.away} - {home.abbreviation} {q1.home}
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
  const q1 = game.q1 || {}

  return (
    <div className={`game-card ${isLive ? 'live' : ''} card-${conf}`} onClick={onClick}>
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      {/* Q1 pick badge — only for pregame games */}
      {isPre && bet && bet.best_pick && conf !== 'skip' && (
        <div className={`pick-badge q1-badge badge-${conf}`}>
          <span className="pick-badge-type">
            {bet.best_pick.type === 'Q1_SPREAD' ? 'Q1 SPREAD'
              : bet.best_pick.type === 'Q1_TOTAL' ? 'Q1 TOTAL'
              : bet.best_pick.type === 'Q1_ML' ? 'Q1 WINNER'
              : 'Q1'}
          </span>
          <span className="pick-badge-pick">{bet.best_pick.pick}</span>
          <span className="pick-badge-edge">+{bet.best_pick.edge}%</span>
        </div>
      )}

      {/* Show secondary Q1 pick (e.g. spread when winner is best) */}
      {isPre && bet?.all_picks && bet.all_picks.length > 1 && conf !== 'skip' && (() => {
        const best = bet.best_pick
        const second = bet.all_picks.find(p => p && p.type !== best.type && (p.confidence || 'skip') !== 'skip')
        if (!second) return null
        return (
          <div className={`pick-badge q1-badge-secondary badge-${second.confidence || 'lean'}`} style={{opacity:0.9}}>
            <span className="pick-badge-type">
              {second.type === 'Q1_SPREAD' ? 'Q1 SPREAD'
                : second.type === 'Q1_TOTAL' ? 'Q1 TOTAL'
                : second.type === 'Q1_ML' ? 'Q1 WINNER'
                : 'Q1'}
            </span>
            <span className="pick-badge-pick">{second.pick}</span>
            <span className="pick-badge-edge">+{second.edge}%</span>
          </div>
        )
      })()}

      {/* Rest indicators */}
      {isPre && bet?.rest && (bet.rest.home_b2b || bet.rest.away_b2b) && (
        <div style={{display:'flex',gap:4,flexWrap:'wrap',marginBottom:6}}>
          {bet.rest.away_b2b && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(239,68,68,0.15)',color:'#ef4444',border:'1px solid rgba(239,68,68,0.3)'}}>
              {away.abbreviation} B2B
            </span>
          )}
          {bet.rest.home_b2b && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(239,68,68,0.15)',color:'#ef4444',border:'1px solid rgba(239,68,68,0.3)'}}>
              {home.abbreviation} B2B
            </span>
          )}
        </div>
      )}

      {/* Team rows */}
      <div className="game-teams">
        <NBATeamRow team={away} isLive={isLive} isFinal={isFinal} />
        <div className="game-at">@</div>
        <NBATeamRow team={home} isLive={isLive} isFinal={isFinal} />
      </div>

      {/* Q1 score display for live/final games */}
      {(isLive || isFinal) && q1.home != null && q1.away != null && (
        <div className="q1-score-display">
          <span className="q1-label">Q1</span>
          <span className="q1-score-away">{away.abbreviation} {q1.away}</span>
          <span className="q1-separator">-</span>
          <span className="q1-score-home">{home.abbreviation} {q1.home}</span>
        </div>
      )}

      {/* Quarter indicator for live games */}
      {isLive && (
        <div style={{textAlign:'center',fontSize:'0.72rem',color:'#94a3b8',marginTop:2}}>
          {status.detail}
        </div>
      )}

      {/* Win probability bar */}
      {isPre && bet?.win_prob?.home != null && (
        <Q1ProbBar wp={bet.win_prob} home={home} away={away} />
      )}

      {/* Odds — Q1 focused */}
      {game.odds && (
        <div className="game-odds-grid">
          {/* Spread */}
          {(game.odds.home_spread_point != null || game.odds.away_spread_point != null) && (
            <div className="odds-line">
              <span className="odds-label">SPR</span>
              <span className="odds-val">
                {away.abbreviation} {game.odds.away_spread_point > 0 ? '+' : ''}{game.odds.away_spread_point || '-'}
                {game.odds.away_spread_odds ? ` (${game.odds.away_spread_odds > 0 ? '+' : ''}${Math.round(game.odds.away_spread_odds)})` : ''}
              </span>
              <span className="odds-val">
                {home.abbreviation} {game.odds.home_spread_point > 0 ? '+' : ''}{game.odds.home_spread_point || '-'}
                {game.odds.home_spread_odds ? ` (${game.odds.home_spread_odds > 0 ? '+' : ''}${Math.round(game.odds.home_spread_odds)})` : ''}
              </span>
            </div>
          )}
          {/* O/U */}
          {game.odds.over_under && (
            <div className="odds-line">
              <span className="odds-label">O/U</span>
              <span className="odds-val">o{game.odds.over_under} {game.odds.over_odds ? `(${Math.round(game.odds.over_odds) > 0 ? '+' : ''}${Math.round(game.odds.over_odds)})` : ''}</span>
              <span className="odds-val">u{game.odds.over_under} {game.odds.under_odds ? `(${Math.round(game.odds.under_odds) > 0 ? '+' : ''}${Math.round(game.odds.under_odds)})` : ''}</span>
            </div>
          )}
          {/* ML */}
          {(game.odds.home_ml || game.odds.away_ml) && (
            <div className="odds-line">
              <span className="odds-label">ML</span>
              <span className="odds-val">{away.abbreviation} {game.odds.away_ml > 0 ? '+' : ''}{game.odds.away_ml || '-'}</span>
              <span className="odds-val">{home.abbreviation} {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml || '-'}</span>
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
