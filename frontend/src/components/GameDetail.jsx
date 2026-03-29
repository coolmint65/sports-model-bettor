import PredictionResults from './PredictionResults'

export default function GameDetail({ game, prediction, loading, onBack }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'

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
                    <span key={i} className="pitcher-stat">{s.name}: {s.value}</span>
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
                    <span key={i} className="pitcher-stat">{s.name}: {s.value}</span>
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

      {/* Model Prediction — two-column layout */}
      <div className="detail-prediction">
        {loading && (
          <div className="loading">
            <div className="spinner" />
            <p>Running model...</p>
          </div>
        )}

        {mergedPrediction && (
          <div className="prediction-layout">
            {/* Left: detailed breakdown */}
            <div className="prediction-main">
              <PredictionResults data={mergedPrediction} odds={game.odds} />
            </div>

            {/* Right: quick picks summary */}
            <div className="prediction-sidebar">
              <BettingPicks data={mergedPrediction} odds={game.odds} />
            </div>
          </div>
        )}

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


function BettingPicks({ data, odds }) {
  const d = data
  const home = d.home
  const away = d.away
  const wp = d.win_prob
  const es = d.expected_score
  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  // Determine picks
  const mlPick = homeWins ? home : away
  const mlProb = homeWins ? wp.home : wp.away
  const mlOdds = homeWins ? odds?.home_ml : odds?.away_ml

  const total = d.total
  const vegasTotal = odds?.over_under
  const ouPick = vegasTotal ? (total > vegasTotal ? 'Over' : 'Under') : null
  const ouConf = vegasTotal && d.over_under
    ? getOUConfidence(d.over_under, vegasTotal)
    : null

  const nrfi = d.first_inning?.nrfi
  const nrfiPick = nrfi != null ? (nrfi > 0.50 ? 'NRFI' : 'YRFI') : null
  const nrfiProb = nrfi != null ? (nrfi > 0.50 ? nrfi : d.first_inning.yrfi) : null

  const rl = d.run_line
  const rlPick = rl
    ? (rl.home_minus_1_5 > 0.50
        ? `${home.abbreviation} -1.5`
        : `${away.abbreviation} +1.5`)
    : null
  const rlProb = rl
    ? Math.max(rl.home_minus_1_5, rl.away_plus_1_5)
    : null

  const f5 = d.f5
  const f5Pick = f5
    ? (f5.win_prob.home > f5.win_prob.away ? home.abbreviation : away.abbreviation)
    : null
  const f5Prob = f5 ? Math.max(f5.win_prob.home, f5.win_prob.away) : null

  return (
    <div className="picks-card">
      <h2>Model Picks</h2>

      <PickRow
        label="Moneyline"
        pick={`${mlPick.abbreviation}`}
        prob={mlProb}
        odds={mlOdds}
        pct={pct}
      />

      {ouPick && (
        <PickRow
          label={`O/U ${vegasTotal}`}
          pick={ouPick}
          prob={ouConf}
          pct={pct}
        />
      )}

      {nrfiPick && (
        <PickRow
          label="1st Inning"
          pick={nrfiPick}
          prob={nrfiProb}
          pct={pct}
        />
      )}

      {rlPick && (
        <PickRow
          label="Run Line"
          pick={rlPick}
          prob={rlProb}
          pct={pct}
        />
      )}

      {f5Pick && (
        <PickRow
          label="F5 Winner"
          pick={f5Pick}
          prob={f5Prob}
          pct={pct}
        />
      )}

      <div className="picks-footer">
        Model projected total: <strong>{total.toFixed(1)}</strong>
      </div>
    </div>
  )
}


function PickRow({ label, pick, prob, odds, pct }) {
  const conf = prob > 0.60 ? 'high' : prob > 0.53 ? 'med' : 'low'

  return (
    <div className={`pick-row conf-${conf}`}>
      <div className="pick-label">{label}</div>
      <div className="pick-choice">
        <span className="pick-name">{pick}</span>
        {odds && (
          <span className="pick-odds">({odds > 0 ? '+' : ''}{odds})</span>
        )}
      </div>
      <div className={`pick-prob conf-${conf}`}>{pct(prob)}</div>
    </div>
  )
}


function getOUConfidence(ouLines, vegasTotal) {
  const key = String(parseFloat(vegasTotal))
  if (ouLines[key]) {
    return Math.max(ouLines[key].over, ouLines[key].under)
  }
  // Find closest line
  const lines = Object.keys(ouLines).map(Number).sort((a, b) => a - b)
  let closest = lines[0]
  for (const l of lines) {
    if (Math.abs(l - vegasTotal) < Math.abs(closest - vegasTotal)) closest = l
  }
  const entry = ouLines[String(closest)]
  return entry ? Math.max(entry.over, entry.under) : null
}


function mergePitcherData(prediction, game) {
  const p = { ...prediction }
  if (p.home?.pitcher?.name === 'TBD' && game.home_pitcher?.name) {
    p.home = { ...p.home, pitcher: { ...p.home.pitcher, name: game.home_pitcher.name } }
  }
  if (p.away?.pitcher?.name === 'TBD' && game.away_pitcher?.name) {
    p.away = { ...p.away, pitcher: { ...p.away.pitcher, name: game.away_pitcher.name } }
  }
  return p
}
