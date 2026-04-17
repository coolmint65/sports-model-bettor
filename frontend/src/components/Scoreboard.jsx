export default function Scoreboard({ games, loading, progress, onSelectGame, bestBets }) {
  if (loading) {
    // Progress is the live snapshot from /api/best-bets/progress -- shows
    // X / N games and a percent so the cold-load wait isn't a black box.
    const total = progress?.total || 0
    const done = progress?.done || 0
    const pct = total ? Math.min(100, Math.round((done / total) * 100)) : null
    const phase = progress?.phase
    let label = "Loading today's slate..."
    if (phase === 'predicting' && total > 0) {
      label = `Computing predictions: ${done}/${total} games (${pct}%)`
    } else if (phase === 'building') {
      label = 'Assembling picks...'
    }
    return (
      <div className="loading">
        <div className="spinner" />
        <p>{label}</p>
      </div>
    )
  }

  if (games.length === 0) {
    return (
      <div className="no-games">
        <p>No games scheduled today.</p>
        <p className="sub">Check back tomorrow for the next slate.</p>
      </div>
    )
  }

  // Build lookup: game id -> best bet data
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

  // Sort active by edge
  activeGames.sort((a, b) => {
    const aEdge = betMap[a.id]?.best_pick?.edge || -99
    const bEdge = betMap[b.id]?.best_pick?.edge || -99
    return bEdge - aEdge
  })

  const edgeCount = activeGames.filter(g => betMap[g.id]?.confidence === 'strong' || betMap[g.id]?.confidence === 'moderate').length

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        Today's Games ({games.length})
        {edgeCount > 0 && <span className="edge-count">{edgeCount} plays with edge</span>}
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
              <GameCard
                key={game.id}
                game={game}
                bet={betMap[game.id]}
                onClick={() => onSelectGame(game)}
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
              <MLBFinalRow
                key={game.id}
                game={game}
                onClick={() => onSelectGame(game)}
              />
            ))}
          </div>
        </>
      )}
    </div>
  )
}


function MLBFinalRow({ game, onClick }) {
  const { home, away } = game
  const hs = parseInt(home.score) || 0
  const as = parseInt(away.score) || 0
  const homeWon = hs > as
  return (
    <div className="game-final-row" onClick={onClick}>
      <span className="final-label">FINAL</span>
      <div className="final-teams">
        <div className="final-team">
          {away.logo && <img src={away.logo} alt="" />}
          <span className={`final-abbr ${!homeWon ? 'winner' : ''}`}>{away.abbreviation}</span>
        </div>
        <span className={`final-score ${!homeWon ? 'winner' : ''}`}>{as}</span>
        <span className="final-dash">-</span>
        <span className={`final-score ${homeWon ? 'winner' : ''}`}>{hs}</span>
        <div className="final-team">
          {home.logo && <img src={home.logo} alt="" />}
          <span className={`final-abbr ${homeWon ? 'winner' : ''}`}>{home.abbreviation}</span>
        </div>
      </div>
    </div>
  )
}

function GameCard({ game, bet, onClick }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'
  const conf = bet?.confidence || 'skip'

  // Rest / fatigue signals for MLB (back-to-back equivalents: short rest,
  // travel fatigue, getaway day). Gated on bet.rest availability from the
  // prediction payload so the card degrades gracefully.
  const rest = bet?.rest || {}
  const homeB2B = rest.home_b2b || rest.home_short_rest
  const awayB2B = rest.away_b2b || rest.away_short_rest
  const homeRest = rest.home_rest_advantage && !rest.away_rest_advantage
  const awayRest = rest.away_rest_advantage && !rest.home_rest_advantage

  return (
    <div className={`game-card ${isLive ? 'live' : ''} card-${conf}`} onClick={onClick}>
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      {/* Model pick badge - only for pregame games */}
      {isPre && bet && bet.best_pick && conf !== 'skip' && (
        <div className={`pick-badge badge-${conf}`}>
          <span className="pick-badge-type">{bet.best_pick.type}</span>
          <span className="pick-badge-pick">{bet.best_pick.pick}</span>
          <span className="pick-badge-edge">+{bet.best_pick.edge}%</span>
        </div>
      )}

      {/* Rest / fatigue indicators (MLB equivalents of B2B / rest advantage) */}
      {isPre && (homeB2B || awayB2B || homeRest || awayRest) && (
        <div style={{display:'flex',gap:4,flexWrap:'wrap',marginBottom:6}}>
          {awayB2B && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(239,68,68,0.15)',color:'#ef4444',border:'1px solid rgba(239,68,68,0.3)'}}>
              {away.abbreviation} tired
            </span>
          )}
          {homeB2B && (
            <span style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(239,68,68,0.15)',color:'#ef4444',border:'1px solid rgba(239,68,68,0.3)'}}>
              {home.abbreviation} tired
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

      {/* Line movement indicator (gated on availability) */}
      {isPre && game.line_movement && game.line_movement.significance && game.line_movement.significance !== 'none' && (
        <div style={{marginBottom:6}}>
          <span
            title={`Line moved ${game.line_movement.significance} since opening`}
            style={{fontSize:'0.66rem',fontWeight:700,padding:'2px 6px',borderRadius:4,background:'rgba(245,158,11,0.12)',color: game.line_movement.significance === 'major' ? '#ef4444' : '#f59e0b',border:'1px solid rgba(245,158,11,0.25)'}}
          >
            LINE MOVED
          </span>
        </div>
      )}

      <div className="game-teams">
        <TeamRow team={away} isLive={isLive} isFinal={isFinal} />
        <div className="game-at">@</div>
        <TeamRow team={home} isLive={isLive} isFinal={isFinal} />
      </div>

      {/* Win probability bar - parity with NHL */}
      {isPre && bet?.win_prob?.home != null && (
        <WinProbBar wp={bet.win_prob} home={home} away={away} />
      )}

      {/* Key insight - one-line "why the model picked this" */}
      {isPre && bet && bet.best_pick && conf !== 'skip' && (
        <MLBCardInsight bet={bet} home={home} away={away} game={game} />
      )}

      {/* Probable pitchers */}
      {isPre && (game.home_pitcher || game.away_pitcher) && (
        <div className="game-pitchers">
          <span className="pitcher">{game.away_pitcher?.name || 'TBD'}</span>
          <span className="vs">vs</span>
          <span className="pitcher">{game.home_pitcher?.name || 'TBD'}</span>
        </div>
      )}

      {/* Odds */}
      {game.odds && (
        <div className="game-odds-grid">
          {/* ML */}
          {(game.odds.home_ml || game.odds.away_ml) && (
            <div className="odds-line">
              <span className="odds-label">ML</span>
              <span className="odds-val">{away.abbreviation} {game.odds.away_ml > 0 ? '+' : ''}{game.odds.away_ml || '-'}</span>
              <span className="odds-val">{home.abbreviation} {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml || '-'}</span>
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
          {/* RL - real spread points or assumed ±1.5 */}
          {(() => {
            const hasReal = game.odds.away_spread_point != null || game.odds.home_spread_point != null
            const awayPt = game.odds.away_spread_point
            const homePt = game.odds.home_spread_point
            const awayOdds = game.odds.away_spread_odds
            const homeOdds = game.odds.home_spread_odds

            // If no real RL, derive from ML: favorite gets -1.5, underdog gets +1.5
            const homeFav = game.odds.home_ml && game.odds.away_ml && game.odds.home_ml < game.odds.away_ml
            const dAwayPt = hasReal ? awayPt : (homeFav ? 1.5 : -1.5)
            const dHomePt = hasReal ? homePt : (homeFav ? -1.5 : 1.5)
            const dAwayOdds = awayOdds || (dAwayPt > 0 ? -140 : 120)
            const dHomeOdds = homeOdds || (dHomePt > 0 ? -140 : 120)

            return (
              <div className="odds-line">
                <span className="odds-label">RL</span>
                <span className="odds-val">
                  {away.abbreviation} {dAwayPt > 0 ? '+' : ''}{dAwayPt}
                  {` (${dAwayOdds > 0 ? '+' : ''}${Math.round(dAwayOdds)})`}
                </span>
                <span className="odds-val">
                  {home.abbreviation} {dHomePt > 0 ? '+' : ''}{dHomePt}
                  {` (${dHomeOdds > 0 ? '+' : ''}${Math.round(dHomeOdds)})`}
                </span>
              </div>
            )
          })()}
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

function TeamRow({ team, isLive, isFinal }) {
  return (
    <div className="game-team">
      {team.logo && <img src={team.logo} alt="" className="team-logo" />}
      <span className="team-abbr">{team.abbreviation}</span>
      <span className="team-name">{team.name}</span>
      <span className="team-record">{team.record}</span>
      {team.streak && <span className="team-streak">{team.streak}</span>}
      {(isLive || isFinal) && (
        <span className={`game-score ${team.winner ? 'winner' : ''}`}>{team.score}</span>
      )}
    </div>
  )
}


function WinProbBar({ wp, home, away }) {
  const hPct = Math.round((wp.home || 0) * 100)
  const aPct = Math.round((wp.away || 0) * 100)
  const homeFavored = (wp.home || 0) > (wp.away || 0)

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


function MLBCardInsight({ bet, home, away, game }) {
  // One-liner explaining WHY the model picked this side. MLB-specific
  // signals: pitcher ERA gap, wRC+ gap, form, injuries, home/road
  // splits. Mirrors NHL's CardInsight in structure.
  const f = bet.factors || {}
  const reasons = []

  // Pitcher matchup
  const hP = game.home_pitcher || {}
  const aP = game.away_pitcher || {}
  if (hP.era > 0 && aP.era > 0) {
    const diff = Math.abs(hP.era - aP.era)
    if (diff >= 1.0) {
      const better = hP.era < aP.era ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: diff * 5,
        text: <><strong>{better}</strong> has SP ERA edge ({Math.min(hP.era, aP.era).toFixed(2)})</>
      })
    }
  }

  // Offense gap via wRC+
  const hWrc = f.home_wrc_plus
  const aWrc = f.away_wrc_plus
  if (hWrc != null && aWrc != null && Math.abs(hWrc - aWrc) > 15) {
    const better = hWrc > aWrc ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(hWrc - aWrc) / 3,
      text: <><strong>{better}</strong> wRC+ edge ({Math.max(hWrc, aWrc).toFixed(0)})</>
    })
  }

  // Recent form gap
  const hForm = f.home_form
  const aForm = f.away_form
  if (hForm != null && aForm != null && Math.abs(hForm - aForm) > 0.05) {
    const hotter = hForm > aForm ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(hForm - aForm) * 40,
      text: <><strong>{hotter}</strong> running hot ({((hForm > aForm ? hForm : aForm) * 100).toFixed(0)}%)</>
    })
  }

  // Injuries - plain English
  const hImp = bet.injuries?.home_impact
  const aImp = bet.injuries?.away_impact
  if (hImp != null && hImp < 0.92) {
    reasons.push({
      weight: (1 - hImp) * 50,
      text: <><strong>{home.abbreviation}</strong> shorthanded ({Math.round((1 - hImp) * 100)}% weaker)</>
    })
  }
  if (aImp != null && aImp < 0.92) {
    reasons.push({
      weight: (1 - aImp) * 50,
      text: <><strong>{away.abbreviation}</strong> shorthanded ({Math.round((1 - aImp) * 100)}% weaker)</>
    })
  }

  // Park factor (distinctive venues only)
  const park = f.park_factor
  if (park != null && Math.abs(park - 1.0) >= 0.04) {
    const tag = park > 1.04 ? 'hitter-friendly park' : 'pitcher-friendly park'
    reasons.push({
      weight: Math.abs(park - 1.0) * 20,
      text: <>Venue: {tag} ({park.toFixed(2)})</>
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
