export default function GamesList({ games, loading, leagueInfo, onSelectGame, onCustomMatchup }) {
  if (loading) {
    return (
      <div className="games-section">
        <div className="loading">
          <div className="spinner" />
          <p>Loading games...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="games-section">
      <div className="games-header">
        <h2 className="section-title">
          {leagueInfo?.name || 'Games'}
          <span className="game-count">{games.length} game{games.length !== 1 ? 's' : ''}</span>
        </h2>
        <button className="custom-btn" onClick={onCustomMatchup}>
          Custom Matchup
        </button>
      </div>

      {games.length === 0 ? (
        <div className="no-games">
          <p>No games scheduled today.</p>
          <button className="custom-btn-large" onClick={onCustomMatchup}>
            Build a Custom Matchup
          </button>
        </div>
      ) : (
        <div className="games-grid">
          {games.map(game => (
            <GameCard key={game.id} game={game} onClick={() => onSelectGame(game)} />
          ))}
        </div>
      )}
    </div>
  )
}

function GameCard({ game, onClick }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'

  const formatTime = (dateStr) => {
    try {
      const d = new Date(dateStr)
      return d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
    } catch {
      return ''
    }
  }

  return (
    <div className={`game-card ${isLive ? 'live' : ''}`} onClick={onClick}>
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      <div className="game-teams">
        <div className="game-team">
          <div className="team-info">
            <span className={`team-name ${isFinal && away.winner ? 'loser' : ''}`}>
              {away.short_name || away.name}
            </span>
            <span className="team-record">{away.record}</span>
          </div>
          {!isPre && <span className={`game-score ${away.winner ? 'winner' : ''}`}>{away.score}</span>}
        </div>

        <div className="game-team">
          <div className="team-info">
            <span className={`team-name ${isFinal && home.winner ? '' : isFinal ? 'loser' : ''}`}>
              {home.short_name || home.name}
            </span>
            <span className="team-record">{home.record}</span>
          </div>
          {!isPre && <span className={`game-score ${home.winner ? 'winner' : ''}`}>{home.score}</span>}
        </div>
      </div>

      <div className="game-meta">
        {isPre && (
          <>
            <span className="game-time">{formatTime(game.date)}</span>
            {game.broadcast && <span className="game-broadcast">{game.broadcast}</span>}
          </>
        )}
        {isLive && (
          <span className="game-clock">{status.detail || status.description}</span>
        )}
        {isFinal && (
          <span className="game-final-detail">{status.detail || 'Final'}</span>
        )}
      </div>

      {game.odds && isPre && (
        <div className="game-odds">
          {game.odds.spread && <span>{game.odds.spread}</span>}
          {game.odds.over_under && <span>O/U {game.odds.over_under}</span>}
        </div>
      )}

      <div className="game-action">
        <span>View Prediction &rarr;</span>
      </div>
    </div>
  )
}
