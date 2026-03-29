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
        <div className="game-team">
          <span className="team-abbr">{away.abbreviation}</span>
          <span className="team-name">{away.name}</span>
          <span className="team-record">{away.record}</span>
          {(isLive || isFinal) && (
            <span className={`game-score ${away.winner ? 'winner' : ''}`}>{away.score}</span>
          )}
        </div>
        <div className="game-at">@</div>
        <div className="game-team">
          <span className="team-abbr">{home.abbreviation}</span>
          <span className="team-name">{home.name}</span>
          <span className="team-record">{home.record}</span>
          {(isLive || isFinal) && (
            <span className={`game-score ${home.winner ? 'winner' : ''}`}>{home.score}</span>
          )}
        </div>
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
          {game.odds.spread && <span className="odds-chip">{game.odds.spread}</span>}
          {game.odds.over_under && <span className="odds-chip">O/U {game.odds.over_under}</span>}
          {game.odds.home_ml && (
            <span className="odds-chip ml">
              {home.abbreviation} {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml}
            </span>
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
