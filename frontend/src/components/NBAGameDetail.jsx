import SharedGameHeader from './gameDetail/SharedGameHeader'
import EdgeCallout from './gameDetail/EdgeCallout'
import { kellyFraction, impliedFromOdds } from './gameDetail/kelly'

export default function NBAGameDetail({ game, prediction, loading, onBack }) {
  const { home, away, status } = game
  const pred = prediction

  return (
    <div className="game-detail">
      <SharedGameHeader game={game} onBack={onBack} />

      <div className="detail-prediction">
        {loading && (
          <div className="loading"><div className="spinner" /><p>Running Q1 model...</p></div>
        )}

        {pred && (
          <div className="prediction-layout">
            <div className="prediction-main">
              <Q1PredictionResults data={pred} odds={game.odds} home={home} away={away} />
            </div>
            <div className="prediction-sidebar">
              <Q1BettingPicks data={pred} odds={game.odds} home={home} away={away} />
            </div>
          </div>
        )}

        {!loading && !pred && (
          <div className="no-prediction">
            <p>Q1 prediction unavailable. Run NBA sync first:</p>
            <code>sync_nba.bat --full</code>
          </div>
        )}
      </div>
    </div>
  )
}

function Q1PredictionResults({ data, odds, home, away }) {
  const d = data
  const pct = n => n != null ? `${(n * 100).toFixed(1)}%` : '-'

  const homeQ1 = d.home_q1_expected || 0
  const awayQ1 = d.away_q1_expected || 0
  const margin = d.predicted_margin || 0
  const total = d.predicted_total || 0
  const homeFav = margin > 0

  const bestEdge = odds ? findBestQ1Edge(d, odds, home, away) : null

  return (
    <div className="results">
      <div className="result-card" style={{minHeight: 240}}>
        <h2>Q1 Projected Outcome</h2>
        <div className="score-display">
          <div className="score-team">
            <div className="name">{home.name}</div>
            <div className="record">{home.record}</div>
            <div className={`score ${homeFav ? 'winner' : ''}`}>{homeQ1.toFixed(1)}</div>
          </div>
          <div className="score-vs">-</div>
          <div className="score-team">
            <div className="name">{away.name}</div>
            <div className="record">{away.record}</div>
            <div className={`score ${!homeFav ? 'winner' : ''}`}>{awayQ1.toFixed(1)}</div>
          </div>
        </div>

        <div style={{textAlign:'center',color:'#64748b',fontSize:'0.75rem',marginTop:4,marginBottom:8}}>
          Projected Q1 total: {total.toFixed(1)} pts | Spread: {homeFav ? home.abbreviation : away.abbreviation} {Math.abs(margin).toFixed(1)}
        </div>

        {d.spread_cover_prob != null && (
          <div className="prob-bar-container">
            <div className="prob-bar-labels">
              <span className={homeFav ? 'favored' : ''}>{home.abbreviation} {pct(d.q1_ml_home)}</span>
              <span className={!homeFav ? 'favored' : ''}>{away.abbreviation} {pct(1 - (d.q1_ml_home || 0.5))}</span>
            </div>
            <div className="prob-bar">
              <div className="home" style={{width: pct(d.q1_ml_home || 0.5)}} />
              <div className="away" style={{width: pct(1 - (d.q1_ml_home || 0.5))}} />
            </div>
          </div>
        )}

        <div className="key-stats">
          <div className="key-stat">
            <span className="key-label">Q1 Total</span>
            <span className="key-value">{total.toFixed(1)}</span>
          </div>
          <div className="key-stat">
            <span className="key-label">Q1 Spread</span>
            <span className="key-value">{homeFav ? home.abbreviation : away.abbreviation} {Math.abs(margin).toFixed(1)}</span>
          </div>
          {d.spread_cover_prob != null && (
            <div className="key-stat">
              <span className="key-label">Cover %</span>
              <span className="key-value">{pct(d.spread_cover_prob)}</span>
            </div>
          )}
        </div>

        <EdgeCallout edge={bestEdge} />
      </div>

      {/* Key Factors */}
      {d.factors && (
        <div className="result-card">
          <h2>Q1 Key Factors</h2>
          {d.factors.pace_factor && (
            <div className="stat-row">
              <span className="stat-label">Pace Factor</span>
              <span className="stat-value">{d.factors.pace_factor.toFixed(2)}x</span>
            </div>
          )}
          {d.factors.home_court_boost && (
            <div className="stat-row">
              <span className="stat-label">Home Court Q1 Boost</span>
              <span className="stat-value positive">+{d.factors.home_court_boost} pts</span>
            </div>
          )}
          {d.factors.rest_adj && (
            <>
              {d.factors.rest_adj.home !== 0 && (
                <div className="stat-row">
                  <span className="stat-label">{home.abbreviation} Rest</span>
                  <span className={`stat-value ${d.factors.rest_adj.home < 0 ? 'negative' : 'positive'}`}>
                    {d.factors.rest_adj.home > 0 ? '+' : ''}{d.factors.rest_adj.home} pts
                  </span>
                </div>
              )}
              {d.factors.rest_adj.away !== 0 && (
                <div className="stat-row">
                  <span className="stat-label">{away.abbreviation} Rest</span>
                  <span className={`stat-value ${d.factors.rest_adj.away < 0 ? 'negative' : 'positive'}`}>
                    {d.factors.rest_adj.away > 0 ? '+' : ''}{d.factors.rest_adj.away} pts
                  </span>
                </div>
              )}
            </>
          )}
          {d.factors.home_q1_off && (
            <div className="stat-row">
              <span className="stat-label">Q1 Offense</span>
              <span className="stat-value">{away.abbreviation} {d.factors.away_q1_off?.toFixed(1)} / {home.abbreviation} {d.factors.home_q1_off?.toFixed(1)}</span>
            </div>
          )}
          {d.factors.home_q1_def && (
            <div className="stat-row">
              <span className="stat-label">Q1 Defense</span>
              <span className="stat-value">{away.abbreviation} {d.factors.away_q1_def?.toFixed(1)} / {home.abbreviation} {d.factors.home_q1_def?.toFixed(1)}</span>
            </div>
          )}
        </div>
      )}

      {/* Reasoning */}
      {d.reasoning && d.reasoning.length > 0 && (
        <div className="result-card">
          <h2>Why this Q1 pick?</h2>
          <ul style={{listStyle:'none',padding:0,margin:0}}>
            {d.reasoning.map((r, i) => (
              <li key={i} style={{padding:'8px 0',borderBottom:i<d.reasoning.length-1?'1px solid #1e293b':'none',fontSize:'0.85rem',color:'#cbd5e1',display:'flex',gap:10}}>
                <span style={{color:'#f59e0b',fontWeight:700,minWidth:14}}>{i+1}.</span>
                <span>{r}</span>
              </li>
            ))}
          </ul>
        </div>
      )}
    </div>
  )
}

function Q1BettingPicks({ data, odds, home, away }) {
  const d = data
  const pct = n => `${(n * 100).toFixed(1)}%`

  const picks = []

  // Q1 Spread
  if (d.spread_cover_prob != null) {
    const margin = d.predicted_margin || 0
    const fav = margin > 0 ? home : away
    picks.push({
      label: 'Q1 Spread',
      pick: `${fav.abbreviation} ${Math.abs(margin) > 0 ? (margin > 0 ? '-' : '+') + Math.abs(margin).toFixed(1) : ''}`,
      prob: d.spread_cover_prob,
      odds: odds?.q1_spread_home_odds || -110,
    })
  }

  // Q1 Total
  if (d.over_prob != null) {
    const total = d.predicted_total || 0
    const pickOver = d.over_prob > 0.5
    picks.push({
      label: `Q1 O/U ${total.toFixed(1)}`,
      pick: pickOver ? 'Over' : 'Under',
      prob: pickOver ? d.over_prob : 1 - d.over_prob,
      odds: pickOver ? (odds?.q1_over_odds || -110) : (odds?.q1_under_odds || -110),
    })
  }

  // Q1 ML
  if (d.q1_ml_home != null) {
    const homeFav = d.q1_ml_home > 0.5
    picks.push({
      label: 'Q1 Winner',
      pick: homeFav ? home.abbreviation : away.abbreviation,
      prob: homeFav ? d.q1_ml_home : 1 - d.q1_ml_home,
      odds: homeFav ? (odds?.home_ml || -110) : (odds?.away_ml || -110),
    })
  }

  return (
    <div className="picks-card">
      <h2>Q1 Model Picks</h2>
      {picks.map((p, i) => (
        <PickRow key={i} label={p.label} pick={p.pick} prob={p.prob} odds={p.odds} pct={pct} />
      ))}
      <div className="picks-footer">
        Q1 projected total: <strong>{(d.predicted_total || 0).toFixed(1)}</strong>
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
    if (parseFloat(edge) > 0) kelly = kellyFraction(prob, odds)
  }

  return (
    <div className={`pick-row conf-${conf}`}>
      <div className="pick-label">{label}</div>
      <div className="pick-choice">
        <span className="pick-name">{pick}</span>
        {odds && <span className="pick-odds">({odds > 0 ? '+' : ''}{odds})</span>}
      </div>
      <div className="pick-numbers">
        <span className={`pick-prob conf-${conf}`}>{pct(prob)}</span>
        {edge && parseFloat(edge) > 0 && <span className="pick-edge positive">+{edge}%</span>}
        {kelly != null && kelly > 0 && (
          <span style={{fontSize:'0.68rem',color:'#94a3b8',marginTop:2,cursor:'help'}} title="Quarter-Kelly">
            Kelly: {(kelly * 100).toFixed(1)}%
          </span>
        )}
      </div>
    </div>
  )
}

function findBestQ1Edge(data, odds, home, away) {
  const candidates = []
  if (data.spread_cover_prob != null && odds) {
    const spreadOdds = odds.q1_spread_home_odds || -110
    const implied = impliedFromOdds(spreadOdds)
    const e = (data.spread_cover_prob - implied) * 100
    if (e > 1.5) {
      const m = data.predicted_margin || 0
      const fav = m > 0 ? home.abbreviation : away.abbreviation
      candidates.push({ label: `${fav} Q1 Spread`, odds: spreadOdds, edge: e })
    }
  }
  if (data.over_prob != null && odds) {
    const total = data.predicted_total || 0
    const pickOver = data.over_prob > 0.5
    const prob = pickOver ? data.over_prob : 1 - data.over_prob
    const ouOdds = pickOver ? (odds.q1_over_odds || -110) : (odds.q1_under_odds || -110)
    const implied = impliedFromOdds(ouOdds)
    const e = (prob - implied) * 100
    if (e > 1.5) candidates.push({ label: `${pickOver ? 'Over' : 'Under'} ${total.toFixed(1)} Q1`, odds: ouOdds, edge: e })
  }
  if (!candidates.length) return null
  const best = candidates.sort((a, b) => b.edge - a.edge)[0]
  best.rating = best.edge > 8 ? 'strong' : best.edge > 4 ? 'moderate' : 'lean'
  return best
}
