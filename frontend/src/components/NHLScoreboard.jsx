export default function NHLScoreboard({ games, loading, onSelectGame, bestBets }) {
  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Loading NHL games...</p>
      </div>
    )
  }

  if (!games || games.length === 0) {
    return (
      <div className="no-games">
        <p>No NHL games scheduled today.</p>
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

  const sorted = [...games].sort((a, b) => {
    const aEdge = betMap[a.id]?.best_pick?.edge || -99
    const bEdge = betMap[b.id]?.best_pick?.edge || -99
    return bEdge - aEdge
  })

  const edgeCount = sorted.filter(g =>
    betMap[g.id]?.confidence === 'strong' || betMap[g.id]?.confidence === 'moderate'
  ).length

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        NHL Games ({games.length})
        {edgeCount > 0 && <span className="edge-count">{edgeCount} plays with edge</span>}
      </h2>
      <div className="games-grid">
        {sorted.map(game => (
          <NHLGameCard
            key={game.id}
            game={game}
            bet={betMap[game.id]}
            onClick={() => onSelectGame && onSelectGame(game)}
          />
        ))}
      </div>
    </div>
  )
}

function NHLGameCard({ game, bet, onClick }) {
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

  return (
    <div className={`game-card ${isLive ? 'live' : ''} card-${conf}`} onClick={onClick}>
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      {/* Model pick badge — only for pregame games */}
      {isPre && bet && bet.best_pick && conf !== 'skip' && (
        <div className={`pick-badge badge-${conf}`}>
          <span className="pick-badge-type">{bet.best_pick.type}</span>
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

      {/* Line movement indicator */}
      {isPre && game.line_movement && game.line_movement.significance && game.line_movement.significance !== 'none' && (
        <div style={{marginBottom:6}}>
          <span
            title={`Line moved ${game.line_movement.significance} since opening`}
            style={{
              fontSize:'0.66rem',
              fontWeight:700,
              padding:'2px 6px',
              borderRadius:4,
              background:'rgba(245,158,11,0.12)',
              color: game.line_movement.significance === 'major' ? '#ef4444' : '#f59e0b',
              border:'1px solid rgba(245,158,11,0.25)',
            }}
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

      {/* Starting goalies */}
      {isPre && (game.away_goalie || game.home_goalie) && (
        <div className="game-pitchers">
          <span className="pitcher">
            {game.away_goalie?.name || 'TBD'}
            {game.away_goalie?.status === 'confirmed' && <span style={{color:'#34d399',marginLeft:4,fontSize:'0.7rem'}}>✓</span>}
            {game.away_goalie?.status === 'expected' && <span style={{color:'#fbbf24',marginLeft:4,fontSize:'0.7rem'}}>~</span>}
            {game.away_goalie?.save_pct > 0 && (
              <span style={{color:'#64748b',fontSize:'0.7rem',marginLeft:6}}>
                {game.away_goalie.save_pct.toFixed(3)} SV%
              </span>
            )}
          </span>
          <span className="vs">vs</span>
          <span className="pitcher">
            {game.home_goalie?.name || 'TBD'}
            {game.home_goalie?.status === 'confirmed' && <span style={{color:'#34d399',marginLeft:4,fontSize:'0.7rem'}}>✓</span>}
            {game.home_goalie?.status === 'expected' && <span style={{color:'#fbbf24',marginLeft:4,fontSize:'0.7rem'}}>~</span>}
            {game.home_goalie?.save_pct > 0 && (
              <span style={{color:'#64748b',fontSize:'0.7rem',marginLeft:6}}>
                {game.home_goalie.save_pct.toFixed(3)} SV%
              </span>
            )}
          </span>
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
          {/* Puck Line */}
          {(() => {
            const hasReal = game.odds.away_spread_point != null || game.odds.home_spread_point != null
            const awayPt = game.odds.away_spread_point
            const homePt = game.odds.home_spread_point
            const awayOdds = game.odds.away_spread_odds
            const homeOdds = game.odds.home_spread_odds

            const homeFav = game.odds.home_ml && game.odds.away_ml && game.odds.home_ml < game.odds.away_ml
            const dAwayPt = hasReal ? awayPt : (homeFav ? 1.5 : -1.5)
            const dHomePt = hasReal ? homePt : (homeFav ? -1.5 : 1.5)
            const dAwayOdds = awayOdds || (dAwayPt > 0 ? -180 : 150)
            const dHomeOdds = homeOdds || (dHomePt > 0 ? -180 : 150)

            return (
              <div className="odds-line">
                <span className="odds-label">PL</span>
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
      {(isLive || isFinal) && (
        <span className={`game-score ${team.winner ? 'winner' : ''}`}>{team.score}</span>
      )}
    </div>
  )
}
