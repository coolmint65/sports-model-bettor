export default function PredictionResults({ data, odds }) {
  const d = data
  const home = d.home
  const away = d.away
  const es = d.expected_score
  const wp = d.win_prob

  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  // Calculate edge vs Vegas
  const edge = odds ? computeEdge(wp, odds) : null

  return (
    <div className="results">
      {/* Projected Score */}
      <div className="result-card">
        <h2>MLB Prediction</h2>
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
          <span className="stat-label">Run Line</span>
          <span className="stat-value">
            {homeWins ? home.abbreviation : away.abbreviation} {
              homeWins ? (d.spread < 0 ? d.spread.toFixed(1) : '+' + d.spread.toFixed(1))
                       : (d.spread > 0 ? (-d.spread).toFixed(1) : '+' + (-d.spread).toFixed(1))
            }
          </span>
        </div>
        <div className="stat-row">
          <span className="stat-label">Total</span>
          <span className="stat-value">{d.total}</span>
        </div>
        {d.park_factor && d.park_factor !== 1.0 && (
          <div className="stat-row">
            <span className="stat-label">Park Factor</span>
            <span className={`stat-value ${d.park_factor > 1.03 ? 'positive' : d.park_factor < 0.97 ? 'negative' : ''}`}>
              {d.park_factor.toFixed(2)}x
              {d.park_factor > 1.03 ? ' (hitter-friendly)' : d.park_factor < 0.97 ? ' (pitcher-friendly)' : ''}
            </span>
          </div>
        )}
      </div>

      {/* Win Probability + Edge */}
      <div className="result-card">
        <h2>Win Probability</h2>
        <div className="prob-bar-container">
          <div className="prob-bar-labels">
            <span>{home.abbreviation} {pct(wp.home)}</span>
            <span>{away.abbreviation} {pct(wp.away)}</span>
          </div>
          <div className="prob-bar">
            <div className="home" style={{ width: pct(wp.home) }} />
            <div className="away" style={{ width: pct(wp.away) }} />
          </div>
        </div>

        {edge && (
          <div className="edge-section">
            <h3>Edge vs Vegas</h3>
            {edge.home_edge !== null && (
              <div className={`stat-row ${edge.home_edge > 3 ? 'edge-positive' : edge.home_edge < -3 ? 'edge-negative' : ''}`}>
                <span className="stat-label">{home.abbreviation} ML ({odds.home_ml > 0 ? '+' : ''}{odds.home_ml})</span>
                <span className="stat-value">
                  {edge.home_edge > 0 ? '+' : ''}{edge.home_edge.toFixed(1)}% edge
                </span>
              </div>
            )}
            {edge.away_edge !== null && (
              <div className={`stat-row ${edge.away_edge > 3 ? 'edge-positive' : edge.away_edge < -3 ? 'edge-negative' : ''}`}>
                <span className="stat-label">{away.abbreviation} ML ({odds.away_ml > 0 ? '+' : ''}{odds.away_ml})</span>
                <span className="stat-value">
                  {edge.away_edge > 0 ? '+' : ''}{edge.away_edge.toFixed(1)}% edge
                </span>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Pitching Matchup Detail */}
      {(home.pitcher || away.pitcher) && (
        <div className="result-card">
          <h2>Starting Pitchers</h2>
          <div className="pitcher-comparison">
            {[
              { label: home.abbreviation, p: home.pitcher },
              { label: away.abbreviation, p: away.pitcher },
            ].map(({ label, p }) => p && (
              <div key={label} className="pitcher-detail">
                <div className="pitcher-header">
                  <span className="pitcher-team">{label}</span>
                  <span className="pitcher-name-lg">{p.name}</span>
                  {p.throws && <span className="pitcher-hand">({p.throws}HP)</span>}
                </div>
                {p.era != null && (
                  <div className="pitcher-stats-grid">
                    <div className="pstat"><span className="pstat-val">{p.record || '-'}</span><span className="pstat-label">W-L</span></div>
                    <div className="pstat"><span className="pstat-val">{p.era?.toFixed(2) || '-'}</span><span className="pstat-label">ERA</span></div>
                    <div className="pstat"><span className="pstat-val">{p.fip?.toFixed(2) || '-'}</span><span className="pstat-label">FIP</span></div>
                    <div className="pstat"><span className="pstat-val">{p.whip?.toFixed(2) || '-'}</span><span className="pstat-label">WHIP</span></div>
                    <div className="pstat"><span className="pstat-val">{p.k_per_9?.toFixed(1) || '-'}</span><span className="pstat-label">K/9</span></div>
                    <div className="pstat"><span className="pstat-val">{p.innings?.toFixed(1) || '-'}</span><span className="pstat-label">IP</span></div>
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}

      {/* F5 (First 5 Innings) */}
      {d.f5 && (
        <div className="result-card">
          <h2>First 5 Innings (F5)</h2>
          <div className="f5-display">
            <div className="stat-row">
              <span className="stat-label">F5 Score</span>
              <span className="stat-value">{home.abbreviation} {d.f5.home} - {d.f5.away} {away.abbreviation}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label">F5 Total</span>
              <span className="stat-value">{d.f5.total}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label">F5 ML</span>
              <span className="stat-value">
                {home.abbreviation} {pct(d.f5.win_prob.home)} / {away.abbreviation} {pct(d.f5.win_prob.away)}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Over/Under */}
      {d.over_under && Object.keys(d.over_under).length > 0 && (
        <div className="result-card">
          <h2>Over / Under</h2>
          <div className="ou-row header">
            <span>LINE</span>
            <span>OVER</span>
            <span>UNDER</span>
          </div>
          {Object.entries(d.over_under).map(([line, probs]) => (
            <div key={line} className="ou-row">
              <span className="ou-line">{line}</span>
              <span className={`ou-prob ${probs.over > 0.55 ? 'over' : ''}`}>
                {pct(probs.over)}
              </span>
              <span className={`ou-prob ${probs.under > 0.55 ? 'under' : ''}`}>
                {pct(probs.under)}
              </span>
            </div>
          ))}
        </div>
      )}

      {/* Run Line Probabilities */}
      {d.run_line && (
        <div className="result-card">
          <h2>Run Line</h2>
          <div className="ou-row header">
            <span>LINE</span>
            <span>{home.abbreviation} COVERS</span>
            <span>{away.abbreviation} COVERS</span>
          </div>
          {Object.entries(d.run_line).map(([line, pCover]) => {
            const rl = parseFloat(line)
            return (
              <div key={line} className="ou-row">
                <span className="ou-line">{rl > 0 ? '+' : ''}{line}</span>
                <span className={`ou-prob ${pCover > 0.55 ? 'over' : ''}`}>
                  {pct(pCover)}
                </span>
                <span className={`ou-prob ${(1 - pCover) > 0.55 ? 'under' : ''}`}>
                  {pct(1 - pCover)}
                </span>
              </div>
            )
          })}
        </div>
      )}

      {/* Inning Breakdown */}
      {d.innings && d.innings.length > 0 && (
        <div className="result-card">
          <h2>Inning Breakdown</h2>
          <table className="period-table">
            <thead>
              <tr>
                <th></th>
                {d.innings.map(inn => <th key={inn.inning}>{inn.inning}</th>)}
                <th>TOT</th>
              </tr>
            </thead>
            <tbody>
              <tr>
                <td className="team-label">{away.abbreviation}</td>
                {d.innings.map(inn => <td key={inn.inning}>{inn.away}</td>)}
                <td className="total-cell">{es.away}</td>
              </tr>
              <tr>
                <td className="team-label">{home.abbreviation}</td>
                {d.innings.map(inn => <td key={inn.inning}>{inn.home}</td>)}
                <td className="total-cell">{es.home}</td>
              </tr>
            </tbody>
          </table>
        </div>
      )}

      {/* Most Likely Scores */}
      {d.correct_scores && d.correct_scores.length > 0 && (
        <div className="result-card">
          <h2>Most Likely Final Scores</h2>
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

      {/* H2H Insights */}
      {d.h2h_insights && d.h2h_insights.length > 0 && (
        <div className="result-card">
          <h2>Batter vs Pitcher</h2>
          <ul className="reasoning-list">
            {d.h2h_insights.map((r, i) => <li key={i}>{r}</li>)}
          </ul>
        </div>
      )}

      {/* Analysis */}
      {d.reasoning && d.reasoning.length > 0 && (
        <div className="result-card">
          <h2>Analysis</h2>
          <ul className="reasoning-list">
            {d.reasoning.map((r, i) => <li key={i}>{r}</li>)}
          </ul>
        </div>
      )}
    </div>
  )
}

function computeEdge(wp, odds) {
  if (!odds) return null

  const result = { home_edge: null, away_edge: null }

  if (odds.home_ml) {
    const impliedHome = mlToProb(odds.home_ml)
    result.home_edge = (wp.home - impliedHome) * 100
  }
  if (odds.away_ml) {
    const impliedAway = mlToProb(odds.away_ml)
    result.away_edge = (wp.away - impliedAway) * 100
  }

  return result
}

function mlToProb(ml) {
  if (ml < 0) {
    return (-ml) / (-ml + 100)
  }
  return 100 / (ml + 100)
}
