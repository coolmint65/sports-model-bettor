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


function FactorRow({ label, awayVal, awayRank, homeVal, homeRank }) {
  const rankColor = (r) => {
    if (!r) return '#64748b'
    if (r <= 5) return '#34d399'   // Top 5 = green
    if (r <= 10) return '#60a5fa'  // Top 10 = blue
    if (r <= 20) return '#94a3b8'  // Middle = gray
    if (r <= 27) return '#f59e0b'  // Bottom 10 = yellow
    return '#ef4444'               // Bottom 5 = red
  }
  const rankLabel = (r) => r ? `${r}${r === 1 ? 'st' : r === 2 ? 'nd' : r === 3 ? 'rd' : 'th'}` : '-'

  return (
    <tr>
      <td style={{textAlign:'left',fontWeight:500}}>{label}</td>
      <td style={{textAlign:'center'}}>{awayVal}</td>
      <td style={{textAlign:'center',color:rankColor(awayRank),fontWeight:600}}>{rankLabel(awayRank)}</td>
      <td style={{textAlign:'center'}}>{homeVal}</td>
      <td style={{textAlign:'center',color:rankColor(homeRank),fontWeight:600}}>{rankLabel(homeRank)}</td>
    </tr>
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

  const bestEdge = odds ? findBestEdge(d, odds, home, away) : null

  return (
    <div className="results">
      {/* Season Context Banner */}
      {d.season_context && d.season_context.implications && (
        <div style={{
          background: d.season_context.phase === 'playoffs' ? '#1e3a2f' : '#1e2a3f',
          border: `1px solid ${d.season_context.phase === 'playoffs' ? '#34d399' : '#60a5fa'}`,
          borderRadius: 8, padding: '8px 16px', marginBottom: 12,
          fontSize: '0.8rem', fontWeight: 600,
          color: d.season_context.phase === 'playoffs' ? '#34d399' : '#60a5fa',
          textAlign: 'center',
        }}>
          {d.season_context.phase === 'playoffs' ? 'PLAYOFF GAME' : 'LATE SEASON — Playoff Race'}
          {' '}— Model adjusts for higher intensity
        </div>
      )}

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
              {bestEdge.label} ({bestEdge.odds > 0 ? '+' : ''}{bestEdge.odds}) — +{bestEdge.edge.toFixed(1)}% edge
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

      {/* First Period Total Goals */}
      {d.first_period && (
        <div className="result-card">
          <h2>1st Period Total Goals</h2>
          <div className="nrfi-display">
            <div className={`nrfi-box ${d.first_period.over_15 > 0.55 ? 'favored' : ''}`}>
              <div className="nrfi-label">Over 1.5</div>
              <div className="nrfi-value">{pct(d.first_period.over_15)}</div>
              <div className="nrfi-sub">2+ goals in 1st period</div>
            </div>
            <div className={`nrfi-box yrfi ${d.first_period.under_15 > 0.55 ? 'favored' : ''}`}>
              <div className="nrfi-label">Under 1.5</div>
              <div className="nrfi-value">{pct(d.first_period.under_15)}</div>
              <div className="nrfi-sub">0-1 goals in 1st period</div>
            </div>
          </div>
          <div style={{textAlign:'center',color:'#64748b',fontSize:'0.75rem',marginTop:8}}>
            Expected P1 total: ~{Math.round(d.first_period.expected_total)} goals
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
                  <td>{p.away.toFixed(1)}</td>
                  <td>{p.home.toFixed(1)}</td>
                  <td>{p.total.toFixed(1)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Key Factors with Rankings */}
      {d.factors && (
        <div className="result-card">
          <h2>Key Factors</h2>
          <table className="standings-table" style={{fontSize:'0.85rem'}}>
            <thead>
              <tr>
                <th style={{textAlign:'left'}}>Stat</th>
                <th>{away.abbreviation}</th>
                <th>Rank</th>
                <th>{home.abbreviation}</th>
                <th>Rank</th>
              </tr>
            </thead>
            <tbody>
              <FactorRow
                label="Power Play"
                awayVal={d.factors.away_pp != null ? (d.factors.away_pp * 100).toFixed(1) + '%' : '-'}
                awayRank={d.factors.away_pp_rank}
                homeVal={d.factors.home_pp != null ? (d.factors.home_pp * 100).toFixed(1) + '%' : '-'}
                homeRank={d.factors.home_pp_rank}
              />
              <FactorRow
                label="Penalty Kill"
                awayVal={d.factors.away_pk != null ? (d.factors.away_pk * 100).toFixed(1) + '%' : '-'}
                awayRank={d.factors.away_pk_rank}
                homeVal={d.factors.home_pk != null ? (d.factors.home_pk * 100).toFixed(1) + '%' : '-'}
                homeRank={d.factors.home_pk_rank}
              />
              <FactorRow
                label="Save %"
                awayVal={d.factors.away_sv?.toFixed(3) || '-'}
                awayRank={d.factors.away_sv_rank}
                homeVal={d.factors.home_sv?.toFixed(3) || '-'}
                homeRank={d.factors.home_sv_rank}
              />
              <FactorRow
                label="Shots/Game"
                awayVal={d.factors.away_shots}
                awayRank={d.factors.away_shots_rank}
                homeVal={d.factors.home_shots}
                homeRank={d.factors.home_shots_rank}
              />
              <FactorRow
                label="Faceoff %"
                awayVal={(d.factors.away_fo * 100).toFixed(1) + '%'}
                awayRank={d.factors.away_fo_rank}
                homeVal={(d.factors.home_fo * 100).toFixed(1) + '%'}
                homeRank={d.factors.home_fo_rank}
              />
            </tbody>
          </table>
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

      {/* Injuries */}
      {d.injuries && (d.injuries.home?.length > 0 || d.injuries.away?.length > 0) && (
        <div className="result-card">
          <h2>Injuries</h2>
          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16}}>
            <div>
              <h3 style={{fontSize:'0.85rem',color:'#94a3b8',marginBottom:8}}>{home.abbreviation}</h3>
              {d.injuries.home?.length > 0 ? d.injuries.home.map((inj, i) => (
                <div key={i} style={{fontSize:'0.8rem',marginBottom:4,display:'flex',justifyContent:'space-between'}}>
                  <span>
                    <span style={{fontWeight:600}}>{inj.name}</span>
                    {inj.position && <span style={{color:'#64748b',marginLeft:4}}>({inj.position})</span>}
                  </span>
                  <span style={{color: inj.status === 'Out' ? '#ef4444' : '#f59e0b',fontSize:'0.75rem'}}>
                    {inj.status || inj.type || 'Out'}
                  </span>
                </div>
              )) : <span style={{color:'#64748b',fontSize:'0.8rem'}}>No injuries reported</span>}
            </div>
            <div>
              <h3 style={{fontSize:'0.85rem',color:'#94a3b8',marginBottom:8}}>{away.abbreviation}</h3>
              {d.injuries.away?.length > 0 ? d.injuries.away.map((inj, i) => (
                <div key={i} style={{fontSize:'0.8rem',marginBottom:4,display:'flex',justifyContent:'space-between'}}>
                  <span>
                    <span style={{fontWeight:600}}>{inj.name}</span>
                    {inj.position && <span style={{color:'#64748b',marginLeft:4}}>({inj.position})</span>}
                  </span>
                  <span style={{color: inj.status === 'Out' ? '#ef4444' : '#f59e0b',fontSize:'0.75rem'}}>
                    {inj.status || inj.type || 'Out'}
                  </span>
                </div>
              )) : <span style={{color:'#64748b',fontSize:'0.8rem'}}>No injuries reported</span>}
            </div>
          </div>
        </div>
      )}
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
  let ouPick = null, ouConf = null, ouOdds = null
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
      const isOver = entry.over > entry.under
      ouPick = isOver ? 'Over' : 'Under'
      ouConf = Math.max(entry.over, entry.under)
      ouOdds = isOver ? odds?.over_odds : odds?.under_odds
    }
  }

  // Puck line pick — determine which side has higher probability
  // and use the actual spread point from odds when available
  const pl = d.puck_line
  let plPick = null
  let plProb = null
  let plOdds = null
  if (pl) {
    const hPt = odds?.home_spread_point
    const aPt = odds?.away_spread_point
    const homeIsFav = (hPt != null && hPt < 0) || (pl.home_minus_1_5 > pl.away_minus_1_5)

    if (homeIsFav) {
      // Home is -1.5 favorite
      if (pl.home_minus_1_5 > 0.50) {
        plPick = `${home.abbreviation} ${hPt != null ? hPt : '-1.5'}`
        plProb = pl.home_minus_1_5
        plOdds = odds?.home_spread_odds
      } else {
        plPick = `${away.abbreviation} ${aPt != null ? (aPt > 0 ? '+' + aPt : aPt) : '+1.5'}`
        plProb = pl.away_plus_1_5
        plOdds = odds?.away_spread_odds
      }
    } else {
      // Away is -1.5 favorite
      if (pl.away_minus_1_5 > 0.50) {
        plPick = `${away.abbreviation} ${aPt != null ? aPt : '-1.5'}`
        plProb = pl.away_minus_1_5
        plOdds = odds?.away_spread_odds
      } else {
        plPick = `${home.abbreviation} ${hPt != null ? (hPt > 0 ? '+' + hPt : hPt) : '+1.5'}`
        plProb = pl.home_plus_1_5 || (1 - pl.away_minus_1_5)
        plOdds = odds?.home_spread_odds
      }
    }
  }

  const p1 = d.first_period
  const p1Pick = p1 ? (p1.over_15 > 0.50 ? 'Over 1.5' : 'Under 1.5') : null
  const p1Prob = p1 ? Math.max(p1.over_15, p1.under_15) : null

  return (
    <div className="picks-card">
      <h2>Model Picks</h2>

      <PickRow label="Moneyline" pick={mlPick.abbreviation} prob={mlProb} odds={mlOdds} pct={pct} />

      {ouPick && (
        <PickRow label={`O/U ${vegasTotal}`} pick={ouPick} prob={ouConf} odds={ouOdds} pct={pct} />
      )}

      {plPick && (
        <PickRow label="Puck Line" pick={plPick} prob={plProb} odds={plOdds} pct={pct} />
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


function findBestEdge(data, odds, home, away) {
  const candidates = []
  const wp = data.win_prob

  // ML edges
  if (odds.home_ml && wp.home) {
    const e = (wp.home - mlToProb(odds.home_ml)) * 100
    if (e > 1.5) candidates.push({ label: `${home.abbreviation} ML`, odds: odds.home_ml, edge: e })
  }
  if (odds.away_ml && wp.away) {
    const e = (wp.away - mlToProb(odds.away_ml)) * 100
    if (e > 1.5) candidates.push({ label: `${away.abbreviation} ML`, odds: odds.away_ml, edge: e })
  }

  // O/U edge
  if (odds.over_under && data.over_under) {
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

  // PL edge — use actual spread points to determine which prob to compare
  if (data.puck_line && odds.home_spread_odds && odds.home_spread_point != null) {
    const pt = odds.home_spread_point
    // Spread point tells us which side home is on:
    // pt = -1.5 means home is -1.5 (favorite), use home_minus_1_5 prob
    // pt = +1.5 means home is +1.5 (underdog), use home_plus_1_5 prob
    const hProb = pt < 0
      ? data.puck_line.home_minus_1_5
      : (data.puck_line.home_plus_1_5 || 1 - data.puck_line.away_minus_1_5)
    const e = (hProb - mlToProb(odds.home_spread_odds)) * 100
    if (e > 1.5) {
      candidates.push({
        label: `${home.abbreviation} ${pt > 0 ? '+' : ''}${pt}`,
        odds: odds.home_spread_odds,
        edge: e,
      })
    }
  }
  if (data.puck_line && odds.away_spread_odds && odds.away_spread_point != null) {
    const pt = odds.away_spread_point
    const aProb = pt < 0
      ? (data.puck_line.away_minus_1_5 || 1 - data.puck_line.home_plus_1_5)
      : data.puck_line.away_plus_1_5
    const e = (aProb - mlToProb(odds.away_spread_odds)) * 100
    if (e > 1.5) {
      candidates.push({
        label: `${away.abbreviation} ${pt > 0 ? '+' : ''}${pt}`,
        odds: odds.away_spread_odds,
        edge: e,
      })
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
