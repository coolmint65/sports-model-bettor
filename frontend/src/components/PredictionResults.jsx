import EdgeCallout from './gameDetail/EdgeCallout'
import ModelSignals from './gameDetail/ModelSignals'

export default function PredictionResults({ data, odds }) {
  const d = data
  const home = d.home
  const away = d.away
  const es = d.expected_score
  const wp = d.win_prob

  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  // Pull the EdgeCallout pick directly from d.best_pick (produced by
  // engine.picks.get_best_pick()). This is the SAME pick the Scoreboard
  // best-bet badge and the POTD selector use, so the three surfaces can
  // no longer drift.
  const bestEdge = getBestEdge(d)

  return (
    <div className="results">
      {/* Season Context Banner - parity with NHL. Renders when the
          prediction payload includes season_context (late-season,
          playoff race, etc). Gated on availability so MLB data without
          this key just skips the banner. */}
      {d.season_context && d.season_context.implications && (
        <div style={{
          background: d.season_context.phase === 'playoffs' ? '#1e3a2f' : '#1e2a3f',
          border: `1px solid ${d.season_context.phase === 'playoffs' ? '#34d399' : '#60a5fa'}`,
          borderRadius: 8, padding: '8px 16px', marginBottom: 12,
          fontSize: '0.8rem', fontWeight: 600,
          color: d.season_context.phase === 'playoffs' ? '#34d399' : '#60a5fa',
          textAlign: 'center',
        }}>
          {d.season_context.phase === 'playoffs' ? 'PLAYOFF GAME' : 'LATE SEASON - Playoff Race'}
          {' '}- Model adjusts for higher intensity
        </div>
      )}

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
        <EdgeCallout edge={bestEdge} />
      </div>

      {/* ── Why this pick? ──
          Merges matchup_insights (pick-specific reasoning from
          compute_matchup_interaction, often empty) and reasoning
          (stat context always present) into a single top-of-page
          card. Matchup insights render first when present.
      */}
      {(() => {
        const lines = []
        if (d.matchup_insights && d.matchup_insights.length > 0) {
          lines.push(...d.matchup_insights)
        }
        if (d.reasoning && d.reasoning.length > 0) {
          lines.push(...d.reasoning)
        }
        if (lines.length === 0) return null
        return (
          <div className="result-card">
            <h2>Why this pick?</h2>
            <ul style={{listStyle:'none',padding:0,margin:0}}>
              {lines.map((line, i) => (
                <li key={i} style={{
                  padding:'8px 0',
                  borderBottom: i < lines.length - 1 ? '1px solid #1e293b' : 'none',
                  fontSize:'0.85rem',
                  color:'#cbd5e1',
                  display:'flex',
                  alignItems:'flex-start',
                  gap:10,
                }}>
                  <span style={{color:'#60a5fa',fontWeight:700,minWidth:14}}>{i + 1}.</span>
                  <span>{line}</span>
                </li>
              ))}
            </ul>
          </div>
        )
      })()}

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

      {/* ── Confirmed Lineups ── */}
      {d.lineups && (d.lineups.home?.length > 0 || d.lineups.away?.length > 0) && (
        <div className="result-card">
          <h2>Confirmed Lineups</h2>
          <div style={{display:'grid',gridTemplateColumns:'1fr 1fr',gap:16}}>
            <LineupColumn abbr={home.abbreviation} lineup={d.lineups.home} />
            <LineupColumn abbr={away.abbreviation} lineup={d.lineups.away} />
          </div>
          <p style={{fontSize:'0.72rem',color:'#64748b',marginTop:8,textAlign:'center'}}>
            Lineups post ~2 hours before first pitch. Blank teams haven't posted yet.
          </p>
        </div>
      )}

      {/* ── Weather (non-domed venues) ── */}
      {d.weather && (d.weather.temp_f != null || d.weather.wind_mph != null) && (
        <div className="result-card">
          <h2>Weather</h2>
          <div style={{display:'flex',gap:24,flexWrap:'wrap',alignItems:'center'}}>
            {d.weather.temp_f != null && (
              <div>
                <div style={{fontSize:'0.72rem',color:'#64748b',textTransform:'uppercase'}}>Temp</div>
                <div style={{fontSize:'1.15rem',fontWeight:700,color:'#e2e8f0'}}>{Math.round(d.weather.temp_f)}&deg;F</div>
              </div>
            )}
            {(() => {
              // wind_mph can come in as a number or as a string like
              // "SW 8 mph" or "Calm" from the DB. Parse out the first
              // numeric chunk when present; otherwise render the raw
              // string so we never display NaN.
              const w = d.weather.wind_mph
              if (w == null) return null
              if (typeof w === 'number' && isFinite(w)) {
                return (
                  <div>
                    <div style={{fontSize:'0.72rem',color:'#64748b',textTransform:'uppercase'}}>Wind</div>
                    <div style={{fontSize:'1.15rem',fontWeight:700,color:'#e2e8f0'}}>{Math.round(w)} mph</div>
                  </div>
                )
              }
              const s = String(w).trim()
              if (!s || s.toLowerCase() === 'nan') return null
              const numMatch = s.match(/(-?\d+(?:\.\d+)?)/)
              if (numMatch) {
                // Keep the direction prefix (e.g. "SW 8 mph") but show rounded value
                const dir = s.slice(0, numMatch.index).trim()
                return (
                  <div>
                    <div style={{fontSize:'0.72rem',color:'#64748b',textTransform:'uppercase'}}>Wind</div>
                    <div style={{fontSize:'1.15rem',fontWeight:700,color:'#e2e8f0'}}>
                      {dir ? `${dir} ` : ''}{Math.round(parseFloat(numMatch[1]))} mph
                    </div>
                  </div>
                )
              }
              return (
                <div>
                  <div style={{fontSize:'0.72rem',color:'#64748b',textTransform:'uppercase'}}>Wind</div>
                  <div style={{fontSize:'1.15rem',fontWeight:700,color:'#e2e8f0'}}>{s}</div>
                </div>
              )
            })()}
            {d.weather.adjustment != null && d.weather.adjustment !== 1 && (
              <div>
                <div style={{fontSize:'0.72rem',color:'#64748b',textTransform:'uppercase'}}>Run Impact</div>
                <div style={{
                  fontSize:'1.15rem', fontWeight:700,
                  color: d.weather.adjustment > 1.02 ? '#34d399' : d.weather.adjustment < 0.98 ? '#ef4444' : '#e2e8f0',
                }}>
                  {d.weather.adjustment > 1 ? '+' : ''}{((d.weather.adjustment - 1) * 100).toFixed(1)}%
                </div>
              </div>
            )}
            <div style={{flex:1,textAlign:'right',fontSize:'0.72rem',color:'#64748b'}}>
              {d.weather.applied ? 'Weather factor active' : 'Weather data fetched; factor currently gated off'}
            </div>
          </div>
        </div>
      )}

      {/* ── HP Umpire tendency ── */}
      {/* Factor is the all-time rollup from `umpires`, clamped ±5%. Name
          is the MLB Stats API HP ump pulled pre-game. Factor > 1.0 means
          this ump's games historically score above league average (hitter
          friendly / big zone); < 1.0 means pitcher-friendly. The GBM
          model uses a separate point-in-time (prior-season) lookup that
          isn't clamped -- display here is the legacy factor-path value
          so bettors see a stable career tendency. */}
      {d.umpire && d.umpire.name && (
        <div className="result-card">
          <h2>HP Umpire</h2>
          <div style={{display:'flex',gap:24,flexWrap:'wrap',alignItems:'center'}}>
            <div>
              <div style={{fontSize:'0.72rem',color:'#64748b',textTransform:'uppercase'}}>Umpire</div>
              <div style={{fontSize:'1.15rem',fontWeight:700,color:'#e2e8f0'}}>{d.umpire.name}</div>
            </div>
            {d.umpire.factor != null && (
              <div>
                <div style={{fontSize:'0.72rem',color:'#64748b',textTransform:'uppercase'}}>Run Factor</div>
                <div style={{
                  fontSize:'1.15rem', fontWeight:700,
                  color: d.umpire.factor > 1.01 ? '#34d399' : d.umpire.factor < 0.99 ? '#ef4444' : '#e2e8f0',
                }}>
                  {d.umpire.factor > 1 ? '+' : ''}{((d.umpire.factor - 1) * 100).toFixed(1)}%
                </div>
              </div>
            )}
            <div style={{flex:1,textAlign:'right',fontSize:'0.72rem',color:'#64748b'}}>
              {d.umpire.factor != null && d.umpire.factor !== 1
                ? 'Lean: ' + (d.umpire.factor > 1 ? 'hitter-friendly' : 'pitcher-friendly')
                : 'No historical lean (or new umpire)'}
            </div>
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

      {/* ── Injuries - NHL-style two-column layout with quantified impact ── */}
      {d.injuries && (d.injuries.home?.length > 0 || d.injuries.away?.length > 0) && (
        <div className="result-card">
          <h2>Injuries</h2>

          {/* Quantified impact summary */}
          {((d.injuries.home_impact != null && d.injuries.home_impact < 1)
            || (d.injuries.away_impact != null && d.injuries.away_impact < 1)) && (
            <div style={{
              textAlign:'center', fontSize:'0.82rem', marginBottom:10,
              display:'flex', justifyContent:'center', gap:16, flexWrap:'wrap',
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
          )}

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

      {/* ── Model Signals: factor / MC / GBM breakdown ──
          Renders the three component models side-by-side with the
          ensemble blend the picks layer actually uses. Null-safe
          so pre-flag-flip cached predictions just skip the card. */}
      <ModelSignals pred={d} sport="mlb" home={home} away={away} />

      {/* ── Key Factors - ranked stat comparison matching NHL ── */}
      {(() => {
        const hRec = d.home?.record_details || d.home
        const aRec = d.away?.record_details || d.away
        // Pull comparative stats that are almost always populated.
        // Rank is best-of-two here (small universe) but the structure
        // mirrors the NHL factors table visually.
        const stats = []
        const push = (label, hv, av, hvFmt, avFmt, higherBetter=true) => {
          if (hv == null || av == null) return
          const hBetter = higherBetter ? hv >= av : hv <= av
          stats.push({label, hv, av, hvFmt: hvFmt ?? String(hv), avFmt: avFmt ?? String(av), hBetter})
        }
        const hF = d.factors || {}
        const aF = d.factors || {}
        // Offense
        if (hF.home_wrc_plus != null || d.home?.wrc_plus != null) {
          push('wRC+',
            d.home?.wrc_plus ?? hF.home_wrc_plus,
            d.away?.wrc_plus ?? hF.away_wrc_plus,
            null, null, true)
        }
        if (d.home?.ops != null || d.away?.ops != null) {
          push('OPS',
            d.home?.ops, d.away?.ops,
            d.home?.ops?.toFixed(3), d.away?.ops?.toFixed(3), true)
        }
        // Pitching
        const hPit = d.home?.pitcher || {}
        const aPit = d.away?.pitcher || {}
        if (hPit.era != null && aPit.era != null) {
          push('SP ERA',
            hPit.era, aPit.era,
            hPit.era.toFixed(2), aPit.era.toFixed(2), false)
        }
        if (hPit.whip != null && aPit.whip != null) {
          push('SP WHIP',
            hPit.whip, aPit.whip,
            hPit.whip.toFixed(2), aPit.whip.toFixed(2), false)
        }

        if (stats.length === 0) return null
        return (
          <div className="result-card">
            <h2>Key Factors</h2>
            <table style={{width:'100%',fontSize:'0.82rem',borderCollapse:'collapse'}}>
              <thead>
                <tr style={{color:'#64748b',textAlign:'left',fontSize:'0.72rem',textTransform:'uppercase'}}>
                  <th style={{padding:'6px 0',fontWeight:600}}></th>
                  <th style={{padding:'6px 0',fontWeight:600,textAlign:'right'}}>{home.abbreviation}</th>
                  <th style={{padding:'6px 0',fontWeight:600,textAlign:'right'}}>{away.abbreviation}</th>
                </tr>
              </thead>
              <tbody>
                {stats.map((s, i) => (
                  <tr key={i} style={{borderTop:'1px solid #1e293b'}}>
                    <td style={{padding:'8px 0',color:'#94a3b8',fontWeight:600}}>{s.label}</td>
                    <td style={{padding:'8px 0',textAlign:'right',fontWeight:600,color: s.hBetter ? '#34d399' : '#cbd5e1'}}>
                      {s.hvFmt}
                    </td>
                    <td style={{padding:'8px 0',textAlign:'right',fontWeight:600,color: !s.hBetter ? '#34d399' : '#cbd5e1'}}>
                      {s.avFmt}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )
      })()}

    </div>
  )
}

function LineupColumn({ abbr, lineup }) {
  if (!lineup || lineup.length === 0) {
    return (
      <div>
        <h3 style={{fontSize:'0.85rem',color:'#94a3b8',marginBottom:8}}>{abbr}</h3>
        <span style={{color:'#64748b',fontSize:'0.8rem'}}>Lineup not posted yet</span>
      </div>
    )
  }
  return (
    <div>
      <h3 style={{fontSize:'0.85rem',color:'#94a3b8',marginBottom:8}}>{abbr}</h3>
      {lineup.map((b, i) => (
        <div key={i} style={{
          fontSize:'0.8rem',
          marginBottom:4,
          display:'flex',
          gap:8,
          alignItems:'baseline',
        }}>
          <span style={{color:'#64748b',fontWeight:600,minWidth:16}}>{i + 1}.</span>
          <span style={{fontWeight:600,color:'#e2e8f0'}}>{b.name || b.full_name || b.player || '?'}</span>
          {b.position && (
            <span style={{color:'#64748b',fontSize:'0.72rem'}}>{b.position}</span>
          )}
          {b.bats && (
            <span style={{color:'#64748b',fontSize:'0.72rem',marginLeft:'auto'}}>{b.bats}</span>
          )}
        </div>
      ))}
    </div>
  )
}


// Build the EdgeCallout payload directly from data.best_pick, which is
// produced by engine.picks.generate_picks() and engine.picks.get_best_pick()
// on the backend. This is the SAME pick the Scoreboard best-bet badge and
// POTD selector use -- no client-side edge/direction-filter logic here.
function getBestEdge(data) {
  const bp = data?.best_pick
  if (!bp) return null
  // engine confidence is 'strong' | 'moderate' | 'lean' | 'skip'
  if (bp.confidence === 'skip') return null
  return {
    label: _formatBestPickLabel(bp),
    odds: bp.odds,
    edge: bp.edge,
    rating: bp.confidence,
  }
}


function _formatBestPickLabel(pick) {
  // For ML we render "{team} ML"; for other markets the engine's pick
  // string already contains the team + line (e.g. "Over 8.5", "NYM +1.5").
  switch (pick.type) {
    case 'ML':    return `${pick.pick} ML`
    case 'F5 ML': return `${pick.pick} F5 ML`
    default:      return pick.pick
  }
}
