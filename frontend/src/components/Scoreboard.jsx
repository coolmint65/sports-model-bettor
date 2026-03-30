export default function Scoreboard({ games, loading, onSelectGame, bestBets }) {
  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Loading today's slate...</p>
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

  // Sort: games with strong edge first, then moderate, then rest by time
  const sorted = [...games].sort((a, b) => {
    const aEdge = betMap[a.id]?.best_pick?.edge || -99
    const bEdge = betMap[b.id]?.best_pick?.edge || -99
    return bEdge - aEdge
  })

  const edgeCount = sorted.filter(g => betMap[g.id]?.confidence === 'strong' || betMap[g.id]?.confidence === 'moderate').length

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        Today's Games ({games.length})
        {edgeCount > 0 && <span className="edge-count">{edgeCount} plays with edge</span>}
      </h2>
      <div className="games-grid">
        {sorted.map(game => (
          <GameCard
            key={game.id}
            game={game}
            bet={betMap[game.id]}
            onClick={() => onSelectGame(game)}
          />
        ))}
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

  return (
    <div className={`game-card ${isLive ? 'live' : ''} card-${conf}`} onClick={onClick}>
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      {/* Model pick badge */}
      {bet && bet.best_pick && conf !== 'skip' && (
        <div className={`pick-badge badge-${conf}`}>
          <span className="pick-badge-type">{bet.best_pick.type}</span>
          <span className="pick-badge-pick">{bet.best_pick.pick}</span>
          <span className="pick-badge-edge">+{bet.best_pick.edge}%</span>
        </div>
      )}

      <div className="game-teams">
        <TeamRow team={away} isLive={isLive} isFinal={isFinal} />
        <div className="game-at">@</div>
        <TeamRow team={home} isLive={isLive} isFinal={isFinal} />
      </div>

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
        <div className="game-odds">
          {game.odds.home_ml && (
            <span className="odds-chip ml">
              {home.abbreviation} {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml}
            </span>
          )}
          {game.odds.away_ml && (
            <span className="odds-chip ml">
              {away.abbreviation} {game.odds.away_ml > 0 ? '+' : ''}{game.odds.away_ml}
            </span>
          )}
          {game.odds.over_under && <span className="odds-chip">O/U {game.odds.over_under}</span>}
          {game.odds.spread && <span className="odds-chip">{game.odds.spread}</span>}
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
