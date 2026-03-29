export default function Scoreboard({ games, loading, onSelectGame }) {
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

  return (
    <div className="scoreboard">
      <h2 className="section-title">Today's Games ({games.length})</h2>
      <div className="games-grid">
        {games.map(game => (
          <GameCard key={game.id} game={game} onClick={() => onSelectGame(game)} />
        ))}
      </div>
    </div>
  )
}

function GameCard({ game, onClick }) {
  const { home, away, status, venue } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'

  return (
    <div className={`game-card ${isLive ? 'live' : ''}`} onClick={onClick}>
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      <div className="game-teams">
        <TeamRow team={away} isLive={isLive} isFinal={isFinal} />
        <div className="game-at">@</div>
        <TeamRow team={home} isLive={isLive} isFinal={isFinal} />
      </div>

      {/* Probable pitchers */}
      {isPre && (game.home_pitcher || game.away_pitcher) && (
        <div className="game-pitchers">
          <span className="pitcher">
            {game.away_pitcher?.name || 'TBD'}
          </span>
          <span className="vs">vs</span>
          <span className="pitcher">
            {game.home_pitcher?.name || 'TBD'}
          </span>
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
          {game.odds.over_under && <span className="odds-chip">O/U {game.odds.over_under}</span>}
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
      {team.logo && (
        <img src={team.logo} alt="" className="team-logo" />
      )}
      <span className="team-abbr">{team.abbreviation}</span>
      <span className="team-name">{team.name}</span>
      <span className="team-record">{team.record}</span>
      {(isLive || isFinal) && (
        <span className={`game-score ${team.winner ? 'winner' : ''}`}>{team.score}</span>
      )}
    </div>
  )
}
