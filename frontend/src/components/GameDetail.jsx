import PredictionResults from './PredictionResults'

export default function GameDetail({ game, prediction, loading, onBack }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'

  return (
    <div className="game-detail">
      <button className="back-btn" onClick={onBack}>
        <span className="back-arrow">&larr;</span> Back to games
      </button>

      {/* Game header */}
      <div className="detail-header">
        {isLive && <div className="live-badge">LIVE</div>}
        {isFinal && <div className="final-badge">FINAL</div>}

        <div className="detail-matchup">
          <div className="detail-team">
            <div className="detail-team-name">{away.name}</div>
            <div className="detail-team-record">{away.record}</div>
            {away.streak && <div className="detail-streak">{away.streak}</div>}
            {(isLive || isFinal) && (
              <div className={`detail-score ${away.winner ? 'winner' : ''}`}>{away.score}</div>
            )}
          </div>

          <div className="detail-at">@</div>

          <div className="detail-team">
            <div className="detail-team-name">{home.name}</div>
            <div className="detail-team-record">{home.record}</div>
            {home.streak && <div className="detail-streak">{home.streak}</div>}
            {(isLive || isFinal) && (
              <div className={`detail-score ${home.winner ? 'winner' : ''}`}>{home.score}</div>
            )}
          </div>
        </div>

        {/* Pitching matchup */}
        {(game.home_pitcher || game.away_pitcher) && (
          <div className="pitching-matchup">
            <div className="pitcher-card">
              <div className="pitcher-label">Away SP</div>
              <div className="pitcher-name">{game.away_pitcher?.name || 'TBD'}</div>
              {game.away_pitcher?.stats?.length > 0 && (
                <div className="pitcher-stats-row">
                  {game.away_pitcher.stats.map((s, i) => (
                    <span key={i} className="pitcher-stat">
                      {s.name}: {s.value}
                    </span>
                  ))}
                </div>
              )}
            </div>
            <div className="vs-label">VS</div>
            <div className="pitcher-card">
              <div className="pitcher-label">Home SP</div>
              <div className="pitcher-name">{game.home_pitcher?.name || 'TBD'}</div>
              {game.home_pitcher?.stats?.length > 0 && (
                <div className="pitcher-stats-row">
                  {game.home_pitcher.stats.map((s, i) => (
                    <span key={i} className="pitcher-stat">
                      {s.name}: {s.value}
                    </span>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}

        <div className="detail-info">
          {game.venue && <span>{game.venue}</span>}
          {game.broadcast && <span>{game.broadcast}</span>}
          {status.state === 'pre' && (
            <span>{new Date(game.date).toLocaleString([], {
              weekday: 'short', month: 'short', day: 'numeric',
              hour: 'numeric', minute: '2-digit'
            })}</span>
          )}
          {isLive && <span className="live-clock">{status.detail}</span>}
        </div>

        {game.odds && (
          <div className="detail-odds">
            {game.odds.spread && <span className="odds-chip">{game.odds.spread}</span>}
            {game.odds.over_under && <span className="odds-chip">O/U {game.odds.over_under}</span>}
            {game.odds.home_ml && (
              <span className="odds-chip">
                {home.abbreviation} {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml}
              </span>
            )}
            {game.odds.away_ml && (
              <span className="odds-chip">
                {away.abbreviation} {game.odds.away_ml > 0 ? '+' : ''}{game.odds.away_ml}
              </span>
            )}
          </div>
        )}
      </div>

      {/* Model Prediction */}
      <div className="detail-prediction">
        <h2 className="section-title">Model Prediction</h2>

        {loading && (
          <div className="loading">
            <div className="spinner" />
            <p>Running model...</p>
          </div>
        )}

        {prediction && <PredictionResults data={prediction} odds={game.odds} />}

        {!loading && !prediction && (
          <div className="no-prediction">
            <p>Prediction unavailable. Run the data sync first:</p>
            <code>python -m scrapers.mlb_stats --today</code>
          </div>
        )}
      </div>
    </div>
  )
}
