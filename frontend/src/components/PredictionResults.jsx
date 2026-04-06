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
  const bestEdge = edge ? getBestEdge(edge, home, away, odds, d) : null

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
              {bestEdge.label} ({bestEdge.odds > 0 ? '+' : ''}{bestEdge.odds}) — +{bestEdge.edge.toFixed(1)}% edge
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

        {/* Run Line / Spreads */}
        {d.run_line && (
          <>
            <h3 style={{marginTop: 20}}>
              Run Line
              {d.run_line.model_spread != null && (
                <span style={{fontWeight: 400, color: '#64748b', fontSize: '0.7rem', marginLeft: 8}}>
                  Model spread: {home.abbreviation} {d.run_line.model_spread > 0 ? '-' : '+'}{Math.abs(d.run_line.model_spread).toFixed(1)}
                </span>
              )}
            </h3>
            {d.run_line.spreads && Object.entries(d.run_line.spreads).map(([spread, probs]) => {
              const s = parseFloat(spread)
              const homeLabel = s > 0
                ? `${home.abbreviation} -${s.toFixed(1)}`
                : `${home.abbreviation} +${Math.abs(s).toFixed(1)}`
              const awayLabel = s > 0
                ? `${away.abbreviation} +${s.toFixed(1)}`
                : `${away.abbreviation} -${Math.abs(s).toFixed(1)}`
              return (
                <div key={spread} className="ou-row">
                  <span className="ou-line">{homeLabel}</span>
                  <span className={`ou-prob ${probs.home_cover > 0.55 ? 'over' : ''}`}>{pct(probs.home_cover)}</span>
                  <span className={`ou-prob ${probs.away_cover > 0.55 ? 'over' : ''}`}>{pct(probs.away_cover)}</span>
                </div>
              )
            })}
            {!d.run_line.spreads && (
              <>
                <div className="ou-row">
                  <span className="ou-line">{home.abbreviation} -1.5</span>
                  <span className={`ou-prob ${d.run_line.home_minus_1_5 > 0.50 ? 'over' : ''}`}>{pct(d.run_line.home_minus_1_5)}</span>
                  <span className="ou-prob">{pct(1 - d.run_line.home_minus_1_5)}</span>
                </div>
                <div className="ou-row">
                  <span className="ou-line">{away.abbreviation} +1.5</span>
                  <span className={`ou-prob ${d.run_line.away_plus_1_5 > 0.50 ? 'over' : ''}`}>{pct(d.run_line.away_plus_1_5)}</span>
                  <span className="ou-prob">{pct(1 - d.run_line.away_plus_1_5)}</span>
                </div>
              </>
            )}
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

      {/* ── Matchup Insights ── */}
      {d.matchup_insights && d.matchup_insights.length > 0 && (
        <div className="result-card">
          <h2>Matchup Analysis</h2>
          <ul className="reasoning-list">
            {d.matchup_insights.map((insight, i) => <li key={i}>{insight}</li>)}
          </ul>
        </div>
      )}

      {/* ── H2H History ── */}
      {d.h2h_history && d.h2h_history.games > 0 && (
        <div className="result-card">
          <h2>Head to Head ({d.h2h_history.seasons_covered || 3}yr)</h2>
          <div className="key-stats">
            <div className="key-stat">
              <span className="key-value">{d.h2h_history.a_wins}-{d.h2h_history.b_wins}</span>
              <span className="key-label">{home.abbreviation} Record</span>
            </div>
            <div className="key-stat">
              <span className="key-value">{d.h2h_history.a_runs_pg}</span>
              <span className="key-label">{home.abbreviation} R/G</span>
            </div>
            <div className="key-stat">
              <span className="key-value">{d.h2h_history.b_runs_pg}</span>
              <span className="key-label">{away.abbreviation} R/G</span>
            </div>
            <div className="key-stat">
              <span className="key-value">{d.h2h_history.games}</span>
              <span className="key-label">Games</span>
            </div>
          </div>
          {d.h2h_history.recent && d.h2h_history.recent.length > 0 && (
            <div style={{marginTop: 12}}>
              <h3>Recent Meetings</h3>
              {d.h2h_history.recent.slice(0, 5).map((g, i) => (
                <div key={i} className="stat-row">
                  <span className="stat-label">{g.date}</span>
                  <span className={`stat-value ${g.a_won ? 'positive' : 'negative'}`}>
                    {home.abbreviation} {g.a_score} - {g.b_score} {away.abbreviation}
                  </span>
                </div>
              ))}
            </div>
          )}
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
  // Keep for backward compat
  const result = { home_edge: null, away_edge: null }
  if (odds.home_ml) result.home_edge = (wp.home - mlToProb(odds.home_ml)) * 100
  if (odds.away_ml) result.away_edge = (wp.away - mlToProb(odds.away_ml)) * 100
  return result
}

function getBestEdge(edge, home, away, odds, data) {
  // Find best edge across ALL bet types — ML, O/U, RL, 1st Inning
  const candidates = []

  // ML
  if (edge.home_edge != null && edge.home_edge > 1.5)
    candidates.push({ label: `${home.abbreviation} ML`, odds: odds.home_ml, edge: edge.home_edge })
  if (edge.away_edge != null && edge.away_edge > 1.5)
    candidates.push({ label: `${away.abbreviation} ML`, odds: odds.away_ml, edge: edge.away_edge })

  // O/U
  if (data && odds.over_under && data.over_under) {
    const vt = parseFloat(odds.over_under)
    const key = Object.keys(data.over_under).find(k => Math.abs(parseFloat(k) - vt) < 0.5)
    if (key) {
      const ou = data.over_under[key]
      const pickOver = ou.over > ou.under
      const prob = Math.max(ou.over, ou.under)
      const realOdds = pickOver ? odds.over_odds : odds.under_odds
      if (realOdds) {
        const e = (prob - mlToProb(realOdds)) * 100
        if (e > 1.5) candidates.push({ label: `${pickOver ? 'Over' : 'Under'} ${vt}`, odds: realOdds, edge: e })
      }
    }
  }

  // RL
  if (data && data.run_line && odds.home_spread_odds) {
    const prob = data.run_line.home_minus_1_5
    if (prob > 0.5) {
      const e = (prob - mlToProb(odds.home_spread_odds)) * 100
      if (e > 1.5) candidates.push({ label: `${home.abbreviation} -1.5`, odds: odds.home_spread_odds, edge: e })
    }
  }
  if (data && data.run_line && odds.away_spread_odds) {
    const prob = data.run_line.away_plus_1_5
    if (prob > 0.5) {
      const e = (prob - mlToProb(odds.away_spread_odds)) * 100
      if (e > 1.5) candidates.push({ label: `${away.abbreviation} +1.5`, odds: odds.away_spread_odds, edge: e })
    }
  }

  // 1st Inning
  if (data && data.first_inning) {
    const nrfi = data.first_inning.nrfi
    const nrfiProb = nrfi > 0.5 ? nrfi : data.first_inning.yrfi
    const nrfiEdge = (nrfiProb - 0.545) * 100  // -120 implied
    if (nrfiEdge > 1.5) {
      candidates.push({ label: nrfi > 0.5 ? 'NRFI' : 'YRFI', odds: -120, edge: nrfiEdge })
    }
  }

  if (candidates.length === 0) return null
  const best = candidates.sort((a, b) => b.edge - a.edge)[0]
  best.rating = best.edge > 8 ? 'strong' : best.edge > 4 ? 'moderate' : 'lean'
  return best
}

function mlToProb(ml) {
  if (ml < 0) return (-ml) / (-ml + 100)
  return 100 / (ml + 100)
}
