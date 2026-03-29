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
  const bestEdge = edge ? getBestEdge(edge, home, away, odds) : null

  return (
    <div className="results">
      {/* ── Top Card: Score + Win Prob + Edge ── */}
      <div className="result-card">
        <h2>Projected Outcome</h2>
        <div className="score-display">
          <div className="score-team">
            <div className="name">{home.name}</div>
            <div className="record">{home.record}</div>
            <div className={`score ${homeWins ? 'winner' : ''}`}>
              {Math.round(es.home)}
            </div>
          </div>
          <div className="score-vs">-</div>
          <div className="score-team">
            <div className="name">{away.name}</div>
            <div className="record">{away.record}</div>
            <div className={`score ${!homeWins ? 'winner' : ''}`}>
              {Math.round(es.away)}
            </div>
          </div>
        </div>

        {/* Win Prob Bar inline */}
        <div className="prob-bar-container">
          <div className="prob-bar-labels">
            <span className={homeWins ? 'favored' : ''}>{home.abbreviation} {pct(wp.home)}</span>
            <span className={!homeWins ? 'favored' : ''}>{away.abbreviation} {pct(wp.away)}</span>
          </div>
          <div className="prob-bar">
            <div className="home" style={{ width: pct(wp.home) }} />
            <div className="away" style={{ width: pct(wp.away) }} />
          </div>
        </div>

        {/* Key numbers */}
        <div className="key-stats">
          <div className="key-stat">
            <span className="key-label">Total</span>
            <span className="key-value">{d.total.toFixed(1)}</span>
          </div>
          <div className="key-stat">
            <span className="key-label">Spread</span>
            <span className="key-value">
              {homeWins ? home.abbreviation : away.abbreviation} {Math.abs(d.spread).toFixed(1)}
            </span>
          </div>
          {d.park_factor && d.park_factor !== 1.0 && (
            <div className="key-stat">
              <span className="key-label">Park</span>
              <span className={`key-value ${d.park_factor > 1.03 ? 'positive' : d.park_factor < 0.97 ? 'negative' : ''}`}>
                {d.park_factor > 1.03 ? 'Hitter' : d.park_factor < 0.97 ? 'Pitcher' : 'Neutral'}
              </span>
            </div>
          )}
        </div>

        {/* Edge vs Vegas callout */}
        {bestEdge && (
          <div className={`edge-callout ${bestEdge.rating}`}>
            <span className="edge-icon">{bestEdge.rating === 'strong' ? '!!' : bestEdge.rating === 'moderate' ? '!' : ''}</span>
            <span className="edge-text">
              {bestEdge.side} ML ({bestEdge.ml > 0 ? '+' : ''}{bestEdge.ml}) — {bestEdge.edge > 0 ? '+' : ''}{bestEdge.edge.toFixed(1)}% edge
            </span>
          </div>
        )}
      </div>

      {/* ── Pitching Matchup ── */}
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

      {/* ── Betting Lines ── */}
      <div className="result-card">
        <h2>Betting Lines</h2>

        {/* O/U */}
        {d.over_under && Object.keys(d.over_under).length > 0 && (
          <>
            <h3>Over / Under</h3>
            {Object.entries(d.over_under).map(([line, probs]) => (
              <div key={line} className="ou-row">
                <span className="ou-line">{line}</span>
                <span className={`ou-prob ${probs.over > 0.55 ? 'over' : ''}`}>{pct(probs.over)}</span>
                <span className={`ou-prob ${probs.under > 0.55 ? 'under' : ''}`}>{pct(probs.under)}</span>
              </div>
            ))}
          </>
        )}

        {/* Run Line */}
        {d.run_line && (
          <>
            <h3 style={{marginTop: 20}}>Run Line (-1.5)</h3>
            <div className="ou-row">
              <span className="ou-line">{home.abbreviation} -1.5</span>
              <span className={`ou-prob ${d.run_line.home_minus_1_5 > 0.50 ? 'over' : ''}`}>{pct(d.run_line.home_minus_1_5)}</span>
              <span className={`ou-prob ${d.run_line.home_minus_1_5 < 0.50 ? 'under' : ''}`}>{pct(1 - d.run_line.home_minus_1_5)}</span>
            </div>
            <div className="ou-row">
              <span className="ou-line">{away.abbreviation} +1.5</span>
              <span className={`ou-prob ${d.run_line.away_plus_1_5 > 0.50 ? 'over' : ''}`}>{pct(d.run_line.away_plus_1_5)}</span>
              <span className={`ou-prob ${d.run_line.away_plus_1_5 < 0.50 ? 'under' : ''}`}>{pct(1 - d.run_line.away_plus_1_5)}</span>
            </div>
          </>
        )}

        {/* F5 */}
        {d.f5 && (
          <>
            <h3 style={{marginTop: 20}}>First 5 Innings</h3>
            <div className="stat-row">
              <span className="stat-label">F5 Total</span>
              <span className="stat-value">{d.f5.total}</span>
            </div>
            <div className="stat-row">
              <span className="stat-label">F5 Winner</span>
              <span className="stat-value">
                {d.f5.win_prob.home > d.f5.win_prob.away ? home.abbreviation : away.abbreviation}{' '}
                {pct(Math.max(d.f5.win_prob.home, d.f5.win_prob.away))}
              </span>
            </div>
          </>
        )}
      </div>

      {/* ── First Inning / NRFI ── */}
      {d.first_inning && (
        <div className="result-card">
          <h2>First Inning</h2>
          <div className="nrfi-display">
            <div className={`nrfi-box ${d.first_inning.nrfi > 0.55 ? 'favored' : ''}`}>
              <div className="nrfi-label">NRFI</div>
              <div className="nrfi-value">{pct(d.first_inning.nrfi)}</div>
              <div className="nrfi-sub">No Run First Inning</div>
            </div>
            <div className={`nrfi-box yrfi ${d.first_inning.yrfi > 0.55 ? 'favored' : ''}`}>
              <div className="nrfi-label">YRFI</div>
              <div className="nrfi-value">{pct(d.first_inning.yrfi)}</div>
              <div className="nrfi-sub">Yes Run First Inning</div>
            </div>
          </div>

          <div className="stat-row">
            <span className="stat-label">{away.abbreviation} scores in 1st</span>
            <span className="stat-value">{pct(d.first_inning.away_scores_1st)}</span>
          </div>
          <div className="stat-row">
            <span className="stat-label">{home.abbreviation} scores in 1st</span>
            <span className="stat-value">{pct(d.first_inning.home_scores_1st)}</span>
          </div>
        </div>
      )}

      {/* ── Score Predictions ── */}
      <div className="result-card">
        <h2>Most Likely Final Scores</h2>
        <div className="correct-scores">
          {d.correct_scores?.map((cs, i) => (
            <div key={i} className="cs-chip">
              <div className="score">{cs.home}-{cs.away}</div>
              <div className="prob">{pct(cs.prob)}</div>
            </div>
          ))}
        </div>
      </div>

      {/* ── Analysis ── */}
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
  const result = { home_edge: null, away_edge: null }
  if (odds.home_ml) {
    result.home_edge = (wp.home - mlToProb(odds.home_ml)) * 100
  }
  if (odds.away_ml) {
    result.away_edge = (wp.away - mlToProb(odds.away_ml)) * 100
  }
  return result
}

function getBestEdge(edge, home, away, odds) {
  let best = null
  if (edge.home_edge != null && edge.home_edge > 1.5) {
    best = { side: home.abbreviation, ml: odds.home_ml, edge: edge.home_edge }
  }
  if (edge.away_edge != null && edge.away_edge > 1.5) {
    if (!best || edge.away_edge > best.edge) {
      best = { side: away.abbreviation, ml: odds.away_ml, edge: edge.away_edge }
    }
  }
  if (best) {
    best.rating = best.edge > 8 ? 'strong' : best.edge > 4 ? 'moderate' : 'lean'
  }
  return best
}

function mlToProb(ml) {
  if (ml < 0) return (-ml) / (-ml + 100)
  return 100 / (ml + 100)
}
