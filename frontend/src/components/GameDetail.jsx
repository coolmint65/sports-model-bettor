import PredictionResults from './PredictionResults'

export default function GameDetail({ game, prediction, loading, onBack }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'

  // Merge ESPN pitcher data into prediction if prediction pitchers are TBD
  const mergedPrediction = prediction ? mergePitcherData(prediction, game) : null

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
            {away.logo && <img src={away.logo} alt="" className="detail-logo" />}
            <div className="detail-team-name">{away.name}</div>
            <div className="detail-team-record">{away.record}</div>
            {(isLive || isFinal) && (
              <div className={`detail-score ${away.winner ? 'winner' : ''}`}>{away.score}</div>
            )}
          </div>

          <div className="detail-at">@</div>

          <div className="detail-team">
            {home.logo && <img src={home.logo} alt="" className="detail-logo" />}
            <div className="detail-team-name">{home.name}</div>
            <div className="detail-team-record">{home.record}</div>
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

        {mergedPrediction && <PredictionResults data={mergedPrediction} odds={game.odds} />}

        {!loading && !prediction && (
          <div className="no-prediction">
            <p>Prediction unavailable. Run the data sync first:</p>
            <code>sync.bat</code>
          </div>
        )}
      </div>
    </div>
  )
}

function mergePitcherData(prediction, game) {
  // If prediction has TBD pitchers but ESPN has names, use ESPN data
  const p = { ...prediction }

  if (p.home?.pitcher?.name === 'TBD' && game.home_pitcher?.name) {
    p.home = {
      ...p.home,
      pitcher: { ...p.home.pitcher, name: game.home_pitcher.name },
    }
  }
  if (p.away?.pitcher?.name === 'TBD' && game.away_pitcher?.name) {
    p.away = {
      ...p.away,
      pitcher: { ...p.away.pitcher, name: game.away_pitcher.name },
    }
  }

  return p
}
