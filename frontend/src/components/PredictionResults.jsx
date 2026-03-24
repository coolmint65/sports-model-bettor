export default function PredictionResults({ data }) {
  const d = data
  const home = d.home
  const away = d.away
  const es = d.expected_score
  const wp = d.win_prob

  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  return (
    <div className="results">
      {/* Projected Score */}
      <div className="result-card">
        <h2>{d.league_name} Prediction</h2>
        <div className="score-display">
          <div className="score-team">
            <div className="name">{home.name}</div>
            <div className="record">{home.record}</div>
            <div className={`score ${homeWins ? 'winner' : ''}`}>{es.home}</div>
          </div>
          <div className="score-vs">-</div>
          <div className="score-team">
            <div className="name">{away.name}</div>
            <div className="record">{away.record}</div>
            <div className={`score ${!homeWins ? 'winner' : ''}`}>{es.away}</div>
          </div>
        </div>

        <div className="stat-row">
          <span className="stat-label">Spread</span>
          <span className="stat-value">
            {home.name} {(-d.spread) > 0 ? '-' : '+'}{Math.abs(d.spread).toFixed(1)}
          </span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Total</span>
          <span className="stat-value">{d.total}</span>
        </div>
      </div>

      {/* Win Probability */}
      <div className="result-card">
        <h2>Win Probability</h2>
        <div className="prob-bar-container">
          <div className="prob-bar-labels">
            <span>{home.name} {pct(wp.home)}</span>
            {wp.draw != null && <span>Draw {pct(wp.draw)}</span>}
            <span>{away.name} {pct(wp.away)}</span>
          </div>
          <div className="prob-bar">
            <div className="home" style={{ width: pct(wp.home) }} />
            {wp.draw != null && <div className="draw" style={{ width: pct(wp.draw) }} />}
            <div className="away" style={{ width: pct(wp.away) }} />
          </div>
        </div>

        {d.regulation_draw_prob != null && (
          <div className="stat-row">
            <span className="stat-label">Regulation Draw (goes to OT)</span>
            <span className="stat-value">{pct(d.regulation_draw_prob)}</span>
          </div>
        )}
        {d.btts != null && (
          <div className="stat-row">
            <span className="stat-label">Both Teams To Score</span>
            <span className="stat-value">Yes {pct(d.btts)} / No {pct(1 - d.btts)}</span>
          </div>
        )}
      </div>

      {/* Over/Under */}
      {d.over_under && Object.keys(d.over_under).length > 0 && (
        <div className="result-card">
          <h2>Over / Under</h2>
          <div className="ou-row" style={{ fontWeight: 600, color: '#64748b', fontSize: '0.75rem' }}>
            <span>LINE</span>
            <span style={{ textAlign: 'center' }}>OVER</span>
            <span style={{ textAlign: 'center' }}>UNDER</span>
          </div>
          {Object.entries(d.over_under).map(([line, probs]) => (
            <div key={line} className="ou-row">
              <span className="ou-line">{line}</span>
              <span className={`ou-prob ${probs.over > 0.5 ? 'over' : ''}`}>
                {pct(probs.over)}
              </span>
              <span className={`ou-prob ${probs.under > 0.5 ? 'under' : ''}`}>
                {pct(probs.under)}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Half Breakdown */}
      {d.halves && d.halves.length > 0 && (
        <div className="result-card">
          <h2>Half Breakdown</h2>
          <table className="period-table">
            <thead>
              <tr>
                <th></th>
                <th>{home.name}</th>
                <th>{away.name}</th>
                <th>Total</th>
              </tr>
            </thead>
            <tbody>
              {d.halves.map(h => (
                <tr key={h.period}>
                  <td>{h.period}</td>
                  <td>{h.home}</td>
                  <td>{h.away}</td>
                  <td>{h.total}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Period Breakdown */}
      {d.periods && d.periods.length > 0 && (
        <div className="result-card">
          <h2>Period Breakdown</h2>
          <table className="period-table">
            <thead>
              <tr>
                <th></th>
                <th>{home.name}</th>
                <th>{away.name}</th>
                <th>Total</th>
              </tr>
            </thead>
            <tbody>
              {d.periods.map(p => (
                <tr key={p.period}>
                  <td>{p.period}</td>
                  <td>{p.home}</td>
                  <td>{p.away}</td>
                  <td>{p.total}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Correct Scores */}
      {d.correct_scores && d.correct_scores.length > 0 && (
        <div className="result-card">
          <h2>Most Likely Correct Scores</h2>
          <div className="correct-scores">
            {d.correct_scores.map((cs, i) => (
              <div key={i} className="cs-chip">
                <div className="score">{cs.home}-{cs.away}</div>
                <div className="prob">{pct(cs.prob)}</div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Analysis */}
      {d.reasoning && d.reasoning.length > 0 && (
        <div className="result-card">
          <h2>Analysis</h2>
          <ul className="reasoning-list">
            {d.reasoning.map((r, i) => (
              <li key={i}>{r}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}
