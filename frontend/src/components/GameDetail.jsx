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
            {(isLive || isFinal) && (
              <div className={`detail-score ${away.winner ? 'winner' : ''}`}>{away.score}</div>
            )}
          </div>

          <div className="detail-at">@</div>

          <div className="detail-team">
            <div className="detail-team-name">{home.name}</div>
            <div className="detail-team-record">{home.record}</div>
            {(isLive || isFinal) && (
              <div className={`detail-score ${home.winner ? 'winner' : ''}`}>{home.score}</div>
            )}
          </div>
        </div>

        <div className="detail-info">
          {game.venue && <span>{game.venue}</span>}
          {game.broadcast && <span>{game.broadcast}</span>}
          {status.state === 'pre' && (
            <span>{new Date(game.date).toLocaleString([], {
              weekday: 'short', month: 'short', day: 'numeric',
              hour: 'numeric', minute: '2-digit'
            })}</span>
          )}
          {isLive && <span className="live-clock">{status.detail || status.clock}</span>}
        </div>

        {game.odds && (
          <div className="detail-odds">
            {game.odds.spread && <span className="odds-chip">{game.odds.spread}</span>}
            {game.odds.over_under && <span className="odds-chip">O/U {game.odds.over_under}</span>}
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

        {prediction && <PredictionResults data={prediction} />}

        {!loading && !prediction && (
          <div className="no-prediction">
            <p>Prediction unavailable for this matchup. One or both teams may not have enough data.</p>
          </div>
        )}
      </div>
    </div>
  )
}
