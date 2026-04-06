export default function NHLGameDetail({ game, prediction, loading, onBack }) {
  const { home, away, status } = game
  const pred = prediction
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const pct = n => `${(n * 100).toFixed(1)}%`

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

        {/* Goalie matchup */}
        {(game.home_goalie || game.away_goalie) && (
          <div className="pitching-matchup">
            <GoalieCard
              label="Away G"
              goalie={game.away_goalie}
              predGoalie={pred?.goalie_matchup?.away}
            />
            <div className="vs-label">VS</div>
            <GoalieCard
              label="Home G"
              goalie={game.home_goalie}
              predGoalie={pred?.goalie_matchup?.home}
            />
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

        {pred && (
          <div className="prediction-layout">
            <div className="prediction-main">
              <NHLPredictionResults data={pred} odds={game.odds} home={home} away={away} />
            </div>
            <div className="prediction-sidebar">
              <NHLBettingPicks data={pred} odds={game.odds} home={home} away={away} />
            </div>
          </div>
        )}

        {!loading && !pred && (
          <div className="no-prediction">
            <p>Prediction unavailable. Run the NHL sync first:</p>
            <code>sync_nhl.bat --full</code>
          </div>
        )}
      </div>
    </div>
  )
}


function GoalieCard({ label, goalie, predGoalie }) {
  // Use DailyFaceoff data first (has SV%, GAA, record), fall back to prediction model
  const name = goalie?.name || predGoalie?.name || 'TBD'
  const svPct = goalie?.save_pct || predGoalie?.save_pct || 0
  const gaa = goalie?.gaa || predGoalie?.gaa || 0
  const wins = goalie?.wins
  const losses = goalie?.losses
  const otl = goalie?.otl
  const status = goalie?.status
  const hasRecord = wins != null && losses != null

  return (
    <div className="pitcher-card">
      <div className="pitcher-label">{label}</div>
      <div className="pitcher-name">
        {name}
        {status === 'confirmed' && <span style={{color:'#34d399',marginLeft:6}}>✓</span>}
        {status === 'expected' && <span style={{color:'#fbbf24',marginLeft:6}}>~</span>}
      </div>
      {(svPct > 0 || hasRecord) && (
        <div className="pitcher-stats-row">
          {svPct > 0 && <span className="pitcher-stat">SV%: {svPct.toFixed(3)}</span>}
          {gaa > 0 && <span className="pitcher-stat">GAA: {gaa.toFixed(2)}</span>}
          {hasRecord && <span className="pitcher-stat">{wins}-{losses}-{otl || 0}</span>}
        </div>
      )}
    </div>
  )
}


function NHLPredictionResults({ data, odds, home, away }) {
  const d = data
  const es = d.expected_score
  const wp = d.win_prob
  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  const edge = odds ? computeEdge(wp, odds) : null
  const bestEdge = edge ? getBestEdge(edge, home, away, odds) : null

  return (
    <div className="results">
      {/* Projected Outcome */}
      <div className="result-card">
        <h2>Projected Outcome</h2>
        <div className="score-display">
          <div className="score-team">
            <div className="name">{home.name}</div>
            <div className="record">{home.record}</div>
            <div className={`score ${homeWins ? 'winner' : ''}`}>{Math.round(es.home)}</div>
          </div>
          <div className="score-vs">-</div>
          <div className="score-team">
            <div className="name">{away.name}</div>
            <div className="record">{away.record}</div>
            <div className={`score ${!homeWins ? 'winner' : ''}`}>{Math.round(es.away)}</div>
          </div>
        </div>

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

        {d.regulation_draw_prob > 0 && (
          <div style={{textAlign:'center',color:'#64748b',fontSize:'0.75rem',marginTop:4}}>
            Regulation draw: {pct(d.regulation_draw_prob)} (goes to OT)
          </div>
        )}

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
        </div>

        {bestEdge && (
          <div className={`edge-callout ${bestEdge.rating}`}>
            <span className="edge-icon">{bestEdge.rating === 'strong' ? '!!' : bestEdge.rating === 'moderate' ? '!' : ''}</span>
            <span className="edge-text">
              {bestEdge.side} ML ({bestEdge.ml > 0 ? '+' : ''}{bestEdge.ml}) — {bestEdge.edge > 0 ? '+' : ''}{bestEdge.edge.toFixed(1)}% edge
            </span>
          </div>
        )}
      </div>

      {/* Betting Lines */}
      <div className="result-card">
        <h2>Betting Lines</h2>

        {/* O/U */}
        {d.over_under && Object.keys(d.over_under).length > 0 && (
          <>
            <h3>Over / Under</h3>
            <div className="ou-row header">
              <span>Line</span><span>Over</span><span>Under</span>
            </div>
            {Object.entries(d.over_under).map(([line, probs]) => (
              <div key={line} className="ou-row">
                <span className="ou-line">{line}</span>
                <span className={`ou-prob ${probs.over > 0.55 ? 'over' : ''}`}>{pct(probs.over)}</span>
                <span className={`ou-prob ${probs.under > 0.55 ? 'under' : ''}`}>{pct(probs.under)}</span>
              </div>
            ))}
          </>
        )}

        {/* Puck Line */}
        {d.puck_line && (
          <>
            <h3 style={{marginTop: 20}}>Puck Line</h3>
            <div className="ou-row">
              <span className="ou-line">{home.abbreviation} -1.5</span>
              <span className={`ou-prob ${d.puck_line.home_minus_1_5 > 0.50 ? 'over' : ''}`}>{pct(d.puck_line.home_minus_1_5)}</span>
              <span className="ou-prob">{pct(1 - d.puck_line.home_minus_1_5)}</span>
            </div>
            <div className="ou-row">
              <span className="ou-line">{away.abbreviation} +1.5</span>
              <span className={`ou-prob ${d.puck_line.away_plus_1_5 > 0.50 ? 'over' : ''}`}>{pct(d.puck_line.away_plus_1_5)}</span>
              <span className="ou-prob">{pct(1 - d.puck_line.away_plus_1_5)}</span>
            </div>
          </>
        )}
      </div>

      {/* First Period */}
      {d.first_period && (
        <div className="result-card">
          <h2>First Period</h2>
          <div className="nrfi-display">
            <div className={`nrfi-box ${d.first_period.scoreless > 0.45 ? 'favored' : ''}`}>
              <div className="nrfi-label">Scoreless P1</div>
              <div className="nrfi-value">{pct(d.first_period.scoreless)}</div>
              <div className="nrfi-sub">No goal in 1st period</div>
            </div>
            <div className={`nrfi-box yrfi ${d.first_period.scoring > 0.55 ? 'favored' : ''}`}>
              <div className="nrfi-label">Scoring P1</div>
              <div className="nrfi-value">{pct(d.first_period.scoring)}</div>
              <div className="nrfi-sub">Goal in 1st period</div>
            </div>
          </div>
        </div>
      )}

      {/* Period Breakdown */}
      {d.periods && d.periods.length > 0 && (
        <div className="result-card">
          <h2>Period Breakdown</h2>
          <table className="standings-table">
            <thead>
              <tr>
                <th>Period</th>
                <th>{away.abbreviation}</th>
                <th>{home.abbreviation}</th>
                <th>Total</th>
              </tr>
            </thead>
            <tbody>
              {d.periods.map(p => (
                <tr key={p.period}>
                  <td style={{fontWeight:600}}>{p.period}</td>
                  <td>{p.away}</td>
                  <td>{p.home}</td>
                  <td>{p.total}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Key Factors */}
      {d.factors && (
        <div className="result-card">
          <h2>Key Factors</h2>
          <div className="stat-row">
            <span className="stat-label">Power Play</span>
            <span className="stat-value">
              {away.abbreviation} {d.factors.away_pp != null ? (d.factors.away_pp * 100).toFixed(1) + '%' : '-'}
              {' / '}
              {home.abbreviation} {d.factors.home_pp != null ? (d.factors.home_pp * 100).toFixed(1) + '%' : '-'}
            </span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Penalty Kill</span>
            <span className="stat-value">
              {away.abbreviation} {d.factors.away_pk != null ? (d.factors.away_pk * 100).toFixed(1) + '%' : '-'}
              {' / '}
              {home.abbreviation} {d.factors.home_pk != null ? (d.factors.home_pk * 100).toFixed(1) + '%' : '-'}
            </span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Save %</span>
            <span className="stat-value">
              {away.abbreviation} {d.factors.away_sv?.toFixed(3) || '-'}
              {' / '}
              {home.abbreviation} {d.factors.home_sv?.toFixed(3) || '-'}
            </span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Shots/Game</span>
            <span className="stat-value">
              {away.abbreviation} {d.factors.away_shots}
              {' / '}
              {home.abbreviation} {d.factors.home_shots}
            </span>
          </div>
          <div className="stat-row">
            <span className="stat-label">Faceoff %</span>
            <span className="stat-value">
              {away.abbreviation} {(d.factors.away_fo * 100).toFixed(1)}%
              {' / '}
              {home.abbreviation} {(d.factors.home_fo * 100).toFixed(1)}%
            </span>
          </div>
        </div>
      )}

      {/* H2H History */}
      {d.h2h && d.h2h.games > 0 && (
        <div className="result-card">
          <h2>Head to Head (3yr)</h2>
          <div className="key-stats">
            <div className="key-stat">
              <span className="key-value">{d.h2h.team1_wins}-{d.h2h.team2_wins}</span>
              <span className="key-label">Record</span>
            </div>
            <div className="key-stat">
              <span className="key-value">{d.h2h.games}</span>
              <span className="key-label">Games</span>
            </div>
          </div>
        </div>
      )}

      {/* Most Likely Scores */}
      <div className="result-card">
        <h2>Most Likely Final Scores</h2>
        <div className="correct-scores">
          {d.correct_scores?.map((cs, i) => (
            <div key={i} className="cs-chip">
              <div className="score">{cs.score}</div>
              <div className="prob">{pct(cs.prob)}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  )
}


function NHLBettingPicks({ data, odds, home, away }) {
  const d = data
  const wp = d.win_prob
  const es = d.expected_score
  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  const mlPick = homeWins ? home : away
  const mlProb = homeWins ? wp.home : wp.away
  const mlOdds = homeWins ? odds?.home_ml : odds?.away_ml

  const vegasTotal = odds?.over_under
  let ouPick = null, ouConf = null
  if (vegasTotal && d.over_under) {
    const vt = parseFloat(vegasTotal)
    let entry = d.over_under[String(vt)] || d.over_under[vt.toFixed(1)]
    if (!entry) {
      const lines = Object.keys(d.over_under).map(Number).sort((a, b) => a - b)
      let closest = lines[0]
      for (const l of lines) {
        if (Math.abs(l - vt) < Math.abs(closest - vt)) closest = l
      }
      entry = d.over_under[String(closest)] || d.over_under[closest.toFixed(1)]
    }
    if (entry) {
      ouPick = entry.over > entry.under ? 'Over' : 'Under'
      ouConf = Math.max(entry.over, entry.under)
    }
  }

  const pl = d.puck_line
  const plPick = pl
    ? (pl.home_minus_1_5 > 0.50
        ? `${home.abbreviation} -1.5`
        : `${away.abbreviation} +1.5`)
    : null
  const plProb = pl
    ? Math.max(pl.home_minus_1_5, pl.away_plus_1_5)
    : null

  const p1 = d.first_period
  const p1Pick = p1 ? (p1.scoreless > 0.45 ? 'Scoreless' : 'Scoring') : null
  const p1Prob = p1 ? Math.max(p1.scoreless, p1.scoring) : null

  return (
    <div className="picks-card">
      <h2>Model Picks</h2>

      <PickRow label="Moneyline" pick={mlPick.abbreviation} prob={mlProb} odds={mlOdds} pct={pct} />

      {ouPick && (
        <PickRow label={`O/U ${vegasTotal}`} pick={ouPick} prob={ouConf} pct={pct} />
      )}

      {plPick && (
        <PickRow label="Puck Line" pick={plPick} prob={plProb} pct={pct} />
      )}

      {p1Pick && (
        <PickRow label="1st Period" pick={p1Pick} prob={p1Prob} pct={pct} />
      )}

      <div className="picks-footer">
        Model projected total: <strong>{d.total.toFixed(1)}</strong>
      </div>
    </div>
  )
}


function PickRow({ label, pick, prob, odds, pct }) {
  const conf = prob > 0.60 ? 'high' : prob > 0.53 ? 'med' : 'low'

  let edge = null
  if (odds && prob) {
    const implied = odds < 0 ? Math.abs(odds) / (Math.abs(odds) + 100) : 100 / (odds + 100)
    edge = ((prob - implied) * 100).toFixed(1)
  }

  return (
    <div className={`pick-row conf-${conf}`}>
      <div className="pick-label">{label}</div>
      <div className="pick-choice">
        <span className="pick-name">{pick}</span>
        {odds && (
          <span className="pick-odds">({odds > 0 ? '+' : ''}{odds})</span>
        )}
      </div>
      <div className="pick-numbers">
        <span className={`pick-prob conf-${conf}`}>{pct(prob)}</span>
        {edge && parseFloat(edge) > 0 && (
          <span className="pick-edge positive">+{edge}%</span>
        )}
      </div>
    </div>
  )
}


function computeEdge(wp, odds) {
  const result = { home_edge: null, away_edge: null }
  if (odds?.home_ml) {
    result.home_edge = (wp.home - mlToProb(odds.home_ml)) * 100
  }
  if (odds?.away_ml) {
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
