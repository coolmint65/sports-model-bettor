import SharedGameHeader from './gameDetail/SharedGameHeader'
import RestBadges from './gameDetail/RestBadges'
import EdgeCallout from './gameDetail/EdgeCallout'
import { kellyFraction, mlToProb, impliedFromOdds } from './gameDetail/kelly'

export default function NHLGameDetail({ game, prediction, loading, onBack }) {
  const { home, away } = game
  const pred = prediction

  // Goalie data-source indicator — DailyFaceoff "confirmed" is the gold standard
  const anyConfirmed =
    game.home_goalie?.status === 'confirmed' ||
    game.away_goalie?.status === 'confirmed'
  const anyGoalie = game.home_goalie || game.away_goalie

  const matchupExtras = (
    <>
      {/* Goalie matchup */}
      {anyGoalie && (
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

      {/* Goalie confirmation badge */}
      {anyGoalie && (
        <div style={{textAlign:'center',marginTop:6}}>
          <span style={{
            display:'inline-block',
            padding:'2px 10px',
            borderRadius:6,
            fontSize:'0.72rem',
            fontWeight:600,
            background: anyConfirmed ? 'rgba(52,211,153,0.12)' : 'rgba(251,191,36,0.10)',
            color: anyConfirmed ? '#34d399' : '#fbbf24',
            border: `1px solid ${anyConfirmed ? 'rgba(52,211,153,0.25)' : 'rgba(251,191,36,0.20)'}`,
          }}>
            {anyConfirmed ? '\u2713 Confirmed goalies' : '~ Expected goalies'}
          </span>
        </div>
      )}

      <RestBadges rest={pred?.rest} home={home} away={away} />
    </>
  )

  return (
    <div className="game-detail">
      <SharedGameHeader game={game} onBack={onBack} matchupExtras={matchupExtras} />

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
  const rankLabel = (r) => r ? ordinal(r) : '-'

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
  const reasons = getReasoning(d, home, away)

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
      <div className="result-card" style={{minHeight: 260}}>
        <h2>Projected Outcome</h2>
        {(() => {
          // Hockey doesn't end in draws. If scores round equal, show OT winner.
          let homeScore = Math.round(es.home)
          let awayScore = Math.round(es.away)
          const goesToOT = homeScore === awayScore
          const regDrawPct = d.regulation_draw_prob || 0

          if (goesToOT) {
            // Break the tie: give the OT goal to the team with higher win prob
            if (wp.home >= wp.away) {
              homeScore += 1
            } else {
              awayScore += 1
            }
          }
          const homeWinsDisplay = homeScore > awayScore

          return (
            <>
              <div className="score-display">
                <div className="score-team">
                  <div className="name">{home.name}</div>
                  <div className="record">{home.record}</div>
                  <div className={`score ${homeWinsDisplay ? 'winner' : ''}`}>{homeScore}</div>
                </div>
                <div className="score-vs">-</div>
                <div className="score-team">
                  <div className="name">{away.name}</div>
                  <div className="record">{away.record}</div>
                  <div className={`score ${!homeWinsDisplay ? 'winner' : ''}`}>{awayScore}</div>
                </div>
              </div>

              {goesToOT && (
                <div style={{textAlign:'center',marginTop:-6,marginBottom:6}}>
                  <span style={{
                    display:'inline-block',
                    padding:'2px 10px',
                    borderRadius:6,
                    fontSize:'0.72rem',
                    fontWeight:600,
                    background:'rgba(251,191,36,0.10)',
                    color:'#fbbf24',
                    border:'1px solid rgba(251,191,36,0.20)',
                  }}>
                    Projected to go to OT/SO
                  </span>
                </div>
              )}

              <div style={{textAlign:'center',color:'#64748b',fontSize:'0.75rem',marginTop: goesToOT ? 2 : -8,marginBottom:8}}>
                ~{(es.home + es.away).toFixed(1)} regulation goals expected
                {regDrawPct > 0.10 && ` (${pct(regDrawPct)} chance of OT)`}
              </div>
            </>
          )
        })()}

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

        <EdgeCallout edge={bestEdge} badgeClassName={bestEdge ? `conf-badge conf-${bestEdge.rating}` : undefined} />
      </div>

      {/* Key Factors with Rankings — moved up for immediate visibility */}
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

      {/* Goalie Impact */}
      {d.goalie_matchup && (d.goalie_matchup.home || d.goalie_matchup.away) &&
       (d.goalie_matchup.home?.save_pct || d.goalie_matchup.away?.save_pct) && (
        <GoalieImpactCard gm={d.goalie_matchup} home={home} away={away} />
      )}

      {/* Why this pick? */}
      {reasons.length > 0 && (
        <div className="result-card">
          <h2>Why this pick?</h2>
          <ul style={{listStyle:'none',padding:0,margin:0}}>
            {reasons.map((r, i) => (
              <li key={i} style={{
                padding:'8px 0',
                borderBottom: i < reasons.length - 1 ? '1px solid #1e293b' : 'none',
                fontSize:'0.85rem',
                color:'#cbd5e1',
                display:'flex',
                alignItems:'flex-start',
                gap:10,
              }}>
                <span style={{color:'#60a5fa',fontWeight:700,minWidth:14}}>{i + 1}.</span>
                <span>{r}</span>
              </li>
            ))}
          </ul>
        </div>
      )}

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
          <table className="standings-table" style={{fontSize:'0.85rem'}}>
            <thead>
              <tr>
                <th style={{textAlign:'left'}}>Period</th>
                <th>{home.abbreviation}</th>
                <th>{away.abbreviation}</th>
                <th>Total</th>
                <th>Scoring %</th>
              </tr>
            </thead>
            <tbody>
              {d.periods.map(p => {
                const scoringPct = (es.home + es.away) > 0
                  ? (p.total / (es.home + es.away)) * 100
                  : 33
                const homeLeads = p.home > p.away
                return (
                  <tr key={p.period}>
                    <td style={{fontWeight:600,textAlign:'left'}}>
                      {p.period === 'P1' ? '1st Period' : p.period === 'P2' ? '2nd Period' : '3rd Period'}
                    </td>
                    <td style={{color: homeLeads ? '#34d399' : '#cbd5e1', fontWeight: homeLeads ? 600 : 400}}>
                      {p.home.toFixed(2)}
                    </td>
                    <td style={{color: !homeLeads ? '#34d399' : '#cbd5e1', fontWeight: !homeLeads ? 600 : 400}}>
                      {p.away.toFixed(2)}
                    </td>
                    <td style={{fontWeight:600}}>{p.total.toFixed(2)}</td>
                    <td style={{color:'#64748b'}}>{scoringPct.toFixed(0)}%</td>
                  </tr>
                )
              })}
              <tr style={{borderTop:'1px solid #334155',fontWeight:700}}>
                <td style={{textAlign:'left'}}>Regulation</td>
                <td>{es.home.toFixed(2)}</td>
                <td>{es.away.toFixed(2)}</td>
                <td>{(es.home + es.away).toFixed(2)}</td>
                <td></td>
              </tr>
            </tbody>
          </table>
          {d.regulation_draw_prob > 0.05 && (
            <div style={{
              marginTop:10,
              padding:'6px 12px',
              background:'rgba(251,191,36,0.06)',
              border:'1px solid rgba(251,191,36,0.15)',
              borderRadius:6,
              fontSize:'0.78rem',
              color:'#fbbf24',
              textAlign:'center',
            }}>
              {pct(d.regulation_draw_prob)} chance this game is tied after regulation and goes to OT
            </div>
          )}
        </div>
      )}

      {/* H2H History */}
      {d.h2h && d.h2h.games > 0 && (
        <div className="result-card">
          <h2>Head to Head (3yr)</h2>
          <div className="key-stats" style={{gap:24}}>
            <div className="key-stat">
              <span className="key-value" style={{fontSize:'1.1rem'}}>{home.abbreviation}</span>
              <span className="key-label" style={{fontSize:'0.7rem',marginTop:2}}>
                {d.h2h.team1_wins} {d.h2h.team1_wins === 1 ? 'Win' : 'Wins'}
              </span>
            </div>
            <div className="key-stat">
              <span className="key-value" style={{color:'#64748b',fontSize:'0.85rem'}}>vs</span>
            </div>
            <div className="key-stat">
              <span className="key-value" style={{fontSize:'1.1rem'}}>{away.abbreviation}</span>
              <span className="key-label" style={{fontSize:'0.7rem',marginTop:2}}>
                {d.h2h.team2_wins} {d.h2h.team2_wins === 1 ? 'Win' : 'Wins'}
              </span>
            </div>
          </div>
          <div style={{textAlign:'center',color:'#64748b',fontSize:'0.72rem',marginTop:8}}>
            {d.h2h.games} meetings over the last 3 seasons
            {d.h2h.team1_wins > d.h2h.team2_wins
              ? ` \u2014 ${home.abbreviation} leads the series`
              : d.h2h.team2_wins > d.h2h.team1_wins
                ? ` \u2014 ${away.abbreviation} leads the series`
                : ' \u2014 Even'}
          </div>
        </div>
      )}

      {/* Injuries */}
      {d.injuries && (d.injuries.home?.length > 0 || d.injuries.away?.length > 0) && (
        <div className="result-card">
          <h2>Injuries</h2>

          {/* Quantified xG impact summary */}
          {(d.injuries.home_impact != null && d.injuries.home_impact < 1) ||
           (d.injuries.away_impact != null && d.injuries.away_impact < 1) ? (
            <div style={{
              textAlign:'center',
              fontSize:'0.82rem',
              marginBottom:10,
              display:'flex',
              justifyContent:'center',
              gap:16,
              flexWrap:'wrap',
            }}>
              {d.injuries.home_impact != null && d.injuries.home_impact < 1 && (
                <span style={{color:'#ef4444'}}>
                  {home.abbreviation}: ~{Math.round((1 - d.injuries.home_impact) * 100)}% weaker from injuries
                </span>
              )}
              {d.injuries.away_impact != null && d.injuries.away_impact < 1 && (
                <span style={{color:'#ef4444'}}>
                  {away.abbreviation}: ~{Math.round((1 - d.injuries.away_impact) * 100)}% weaker from injuries
                </span>
              )}
            </div>
          ) : null}

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
  let kelly = null
  if (odds && prob) {
    const implied = impliedFromOdds(odds)
    edge = ((prob - implied) * 100).toFixed(1)
    // Only surface Kelly sizing when we have a positive edge
    if (parseFloat(edge) > 0) {
      kelly = kellyFraction(prob, odds)
    }
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
        {kelly != null && kelly > 0 && (
          <span
            className="pick-kelly"
            title="Quarter-Kelly bet sizing. Bet this fraction of your bankroll to maximize long-run growth while staying safe from variance."
            style={{
              fontSize: '0.68rem',
              color: '#94a3b8',
              marginTop: 2,
              letterSpacing: '0.02em',
              cursor: 'help',
            }}
          >
            Kelly: {(kelly * 100).toFixed(1)}%
          </span>
        )}
      </div>
    </div>
  )
}


function GoalieImpactCard({ gm, home, away }) {
  const hSv = gm.home?.save_pct || 0
  const aSv = gm.away?.save_pct || 0
  const diff = hSv - aSv  // positive -> home goalie is better
  // Rough xG suppression estimate: ~30 shots/game * SV% differential
  // (same shots * 0.015 SV% gap = ~0.45 goals suppressed)
  const SHOTS = 30
  const xgAdvantage = Math.abs(diff) * SHOTS
  const better = diff > 0 ? home.abbreviation : away.abbreviation
  const hasEdge = Math.abs(diff) >= 0.005

  return (
    <div className="result-card">
      <h2>Goalie Impact</h2>
      <div className="stat-row">
        <span className="stat-label">{away.abbreviation} SV%</span>
        <span className="stat-value">{aSv > 0 ? aSv.toFixed(3) : '-'}</span>
      </div>
      <div className="stat-row">
        <span className="stat-label">{home.abbreviation} SV%</span>
        <span className="stat-value">{hSv > 0 ? hSv.toFixed(3) : '-'}</span>
      </div>
      <div className="stat-row">
        <span className="stat-label">SV% differential</span>
        <span className="stat-value" style={{color: hasEdge ? (diff > 0 ? '#34d399' : '#60a5fa') : '#94a3b8'}}>
          {diff >= 0 ? '+' : ''}{diff.toFixed(3)}
        </span>
      </div>
      {hasEdge && (
        <div style={{
          marginTop:10,
          padding:'8px 12px',
          background: 'rgba(52,211,153,0.06)',
          border: '1px solid rgba(52,211,153,0.15)',
          borderRadius: 6,
          fontSize: '0.82rem',
          color: '#34d399',
          textAlign: 'center',
        }}>
          {better} goalie edge: ~{xgAdvantage.toFixed(2)} goals suppressed per game
        </div>
      )}
      {!hasEdge && (
        <div style={{marginTop:10,textAlign:'center',color:'#64748b',fontSize:'0.78rem'}}>
          Goalie matchup is roughly even
        </div>
      )}
    </div>
  )
}


/**
 * Produce the top reasons that explain the model's lean for this game.
 * Keeps it to at most 4 bullet points, ordered by impact.
 */
function ordinal(n) {
  if (!n) return ''
  const s = ['th','st','nd','rd']
  const v = n % 100
  return n + (s[(v-20)%10] || s[v] || s[0])
}

function getReasoning(pred, home, away) {
  const reasons = []
  const f = pred.factors || {}
  const ctx = pred.season_context || {}
  const hCtx = ctx.home || {}
  const aCtx = ctx.away || {}
  const wp = pred.win_prob || {}

  // Goalie matchup
  if (pred.goalie_matchup?.home && pred.goalie_matchup?.away) {
    const h_sv = pred.goalie_matchup.home.save_pct || 0
    const a_sv = pred.goalie_matchup.away.save_pct || 0
    if (h_sv > 0 && a_sv > 0) {
      const diff = Math.abs(h_sv - a_sv)
      const better = h_sv > a_sv ? home.abbreviation : away.abbreviation
      const worse = h_sv > a_sv ? away.abbreviation : home.abbreviation
      if (diff > 0.020) {
        reasons.push(`${better}'s goalie is stopping significantly more shots than ${worse}'s (${Math.max(h_sv, a_sv).toFixed(3)} vs ${Math.min(h_sv, a_sv).toFixed(3)} save %)`)
      } else if (diff > 0.010) {
        reasons.push(`${better} has a slight goalie edge tonight, saving about 1 more goal per 100 shots`)
      } else {
        reasons.push(`Goalie matchup is roughly even tonight (${h_sv.toFixed(3)} vs ${a_sv.toFixed(3)} save %)`)
      }
    }
  }

  // Record / quality gap
  const hPace = hCtx.points_pace || 0
  const aPace = aCtx.points_pace || 0
  if (Math.abs(hPace - aPace) > 0.12) {
    const better = hPace > aPace ? home.name : away.name
    const worse = hPace > aPace ? away.name : home.name
    reasons.push(`${better} is a fundamentally better team this season than ${worse}`)
  } else if (Math.abs(hPace - aPace) > 0.05) {
    const better = hPace > aPace ? home.abbreviation : away.abbreviation
    reasons.push(`${better} has a slight edge in overall team quality this season`)
  }

  // Home ice advantage
  if (wp.home > 0.55) {
    reasons.push(`${home.abbreviation} playing at home where they have a clear advantage this season`)
  }

  // Power play vs penalty kill mismatch
  if (f.home_pp != null && f.away_pk != null) {
    const h_pp = f.home_pp * 100
    const a_pk = f.away_pk * 100
    if (h_pp > 22 && a_pk < 78) {
      reasons.push(`${home.abbreviation}'s power play (${h_pp.toFixed(1)}%) could feast against ${away.abbreviation}'s weak penalty kill (${a_pk.toFixed(1)}%)`)
    } else if (h_pp > 20 && a_pk < 80) {
      reasons.push(`${home.abbreviation} has a decent power play that could take advantage of ${away.abbreviation}'s penalty kill`)
    }
  }
  if (f.away_pp != null && f.home_pk != null) {
    const a_pp = f.away_pp * 100
    const h_pk = f.home_pk * 100
    if (a_pp > 22 && h_pk < 78) {
      reasons.push(`${away.abbreviation}'s power play (${a_pp.toFixed(1)}%) could feast against ${home.abbreviation}'s weak penalty kill (${h_pk.toFixed(1)}%)`)
    }
  }

  // Save % rankings
  if (f.home_sv_rank && f.home_sv_rank <= 5) {
    reasons.push(`${home.abbreviation} has one of the best goaltending units in the league (ranked ${ordinal(f.home_sv_rank)})`)
  } else if (f.home_sv_rank && f.home_sv_rank >= 28) {
    reasons.push(`${home.abbreviation}'s goaltending has been among the worst in the league this season`)
  }
  if (f.away_sv_rank && f.away_sv_rank <= 5) {
    reasons.push(`${away.abbreviation} has elite goaltending this season (ranked ${ordinal(f.away_sv_rank)})`)
  } else if (f.away_sv_rank && f.away_sv_rank >= 28) {
    reasons.push(`${away.abbreviation}'s goaltending has been a liability all season`)
  }

  // Recent form
  const hL10 = hCtx.l10_pts_pct
  const aL10 = aCtx.l10_pts_pct
  if (hL10 != null && hL10 > 0.7) {
    reasons.push(`${home.abbreviation} is red hot, going ${hCtx.l10_record} in their last 10 games`)
  } else if (hL10 != null && hL10 < 0.35) {
    reasons.push(`${home.abbreviation} is ice cold, just ${hCtx.l10_record} in their last 10`)
  }
  if (aL10 != null && aL10 > 0.7) {
    reasons.push(`${away.abbreviation} is rolling with a ${aCtx.l10_record} record in their last 10 games`)
  } else if (aL10 != null && aL10 < 0.35) {
    reasons.push(`${away.abbreviation} has been struggling, going ${aCtx.l10_record} in their last 10`)
  }

  // Back-to-back fatigue
  if (pred.rest?.home_b2b) {
    reasons.push(`${home.abbreviation} played last night, so tired legs tend to cost about half a goal`)
  }
  if (pred.rest?.away_b2b) {
    reasons.push(`${away.abbreviation} is on back-to-back nights, expect slower play and more mistakes`)
  }
  if (pred.rest?.home_rest_advantage && !pred.rest?.away_rest_advantage) {
    reasons.push(`${home.abbreviation} has had extra rest, giving them a fresh-legs advantage`)
  }
  if (pred.rest?.away_rest_advantage && !pred.rest?.home_rest_advantage) {
    reasons.push(`${away.abbreviation} has had extra rest, giving them a fresh-legs advantage`)
  }

  // Injuries
  if (pred.injuries?.home_impact != null && pred.injuries.home_impact < 0.92) {
    const pct = Math.round((1 - pred.injuries.home_impact) * 100)
    reasons.push(`${home.abbreviation} is severely shorthanded, about ${pct}% weaker than full strength`)
  } else if (pred.injuries?.home_impact != null && pred.injuries.home_impact < 0.97) {
    reasons.push(`${home.abbreviation} has some players out but their depth should cover it`)
  }
  if (pred.injuries?.away_impact != null && pred.injuries.away_impact < 0.92) {
    const pct = Math.round((1 - pred.injuries.away_impact) * 100)
    reasons.push(`${away.abbreviation} is severely shorthanded, about ${pct}% of their scoring power is out`)
  } else if (pred.injuries?.away_impact != null && pred.injuries.away_impact < 0.97) {
    reasons.push(`${away.abbreviation} missing a few players but their depth should cover it`)
  }

  // Motivation / playoff context
  if (hCtx.fighting && aCtx.eliminated) {
    reasons.push(`${home.abbreviation} is fighting for their playoff life while ${away.abbreviation} has nothing to play for`)
  } else if (aCtx.fighting && hCtx.eliminated) {
    reasons.push(`${away.abbreviation} is desperate for points while ${home.abbreviation}'s season is already over`)
  } else if (hCtx.clinched && !aCtx.clinched && aCtx.fighting) {
    reasons.push(`${home.abbreviation} already clinched so they might not have the same urgency as ${away.abbreviation}`)
  } else if (aCtx.clinched && !hCtx.clinched && hCtx.fighting) {
    reasons.push(`${away.abbreviation} has their spot locked while ${home.abbreviation} needs this win more`)
  }

  // Shot volume advantage
  if (f.home_shots_rank && f.away_shots_rank) {
    if (f.home_shots_rank <= 5 && f.away_shots_rank >= 25) {
      reasons.push(`${home.abbreviation} generates a ton of shots (${f.home_shots}/game) while ${away.abbreviation} gives up a lot, creating more scoring chances`)
    } else if (f.away_shots_rank <= 5 && f.home_shots_rank >= 25) {
      reasons.push(`${away.abbreviation} is an elite shot-generating team (${f.away_shots}/game) and will pepper the net tonight`)
    }
  }

  // Faceoff dominance
  if (f.home_fo_rank && f.away_fo_rank) {
    if (f.home_fo_rank <= 5 && f.away_fo_rank >= 25) {
      reasons.push(`${home.abbreviation} dominates the faceoff circle (ranked ${ordinal(f.home_fo_rank)}) which means more puck possession`)
    } else if (f.away_fo_rank <= 5 && f.home_fo_rank >= 25) {
      reasons.push(`${away.abbreviation} wins faceoffs at an elite rate, giving them a possession edge`)
    }
  }

  // H2H history
  if (pred.h2h && typeof pred.h2h === 'object' && pred.h2h.games >= 3) {
    const h2h = pred.h2h
    const homeWins = h2h.team1_wins || 0
    const awayWins = h2h.team2_wins || 0
    if (homeWins > awayWins + 2) {
      reasons.push(`${home.abbreviation} has owned this matchup recently, going ${homeWins}-${awayWins} in the last ${h2h.games} meetings`)
    } else if (awayWins > homeWins + 2) {
      reasons.push(`${away.abbreviation} has dominated this matchup lately, winning ${awayWins} of the last ${h2h.games} meetings`)
    }
  }

  // Overall model conviction
  const maxWp = Math.max(wp.home || 0, wp.away || 0)
  const fav = (wp.home || 0) > (wp.away || 0) ? home.abbreviation : away.abbreviation
  if (maxWp > 0.65) {
    reasons.push(`The model gives ${fav} a strong ${(maxWp * 100).toFixed(0)}% chance of winning this game`)
  }

  return reasons.slice(0, 5)
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
