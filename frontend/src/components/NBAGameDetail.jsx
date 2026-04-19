import GameDetailShell from './primitives/GameDetailShell'
import WinProbBar from './primitives/WinProbBar'
import StatRow from './primitives/StatRow'
import EdgeCallout from './gameDetail/EdgeCallout'
import WhyThisPick from './gameDetail/WhyThisPick'
import { kellyFraction, impliedFromOdds } from './gameDetail/kelly'
import ProbHistogram from './gameDetail/ProbHistogram'
import ModelSignals from './gameDetail/ModelSignals'

export default function NBAGameDetail({ game, prediction, loading, onBack }) {
  const { home, away } = game

  return (
    <GameDetailShell
      game={game}
      onBack={onBack}
      loading={loading}
      loadingLabel="Running Q1 model..."
      prediction={prediction}
      noPredictionMessage="Q1 prediction unavailable. Run NBA sync first:"
      noPredictionCommand="sync_nba.bat --full"
      renderMain={p => <Q1PredictionResults data={p} odds={game.odds} home={home} away={away} />}
      renderSidebar={p => <Q1BettingPicks data={p} odds={game.odds} home={home} away={away} />}
    />
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

  // Prefer the engine-generated pick attached by /api/nba/predict --
  // same selector + empirical calibration that powers the Scoreboard
  // card. Fall back to the client-side findBestQ1Edge if the backend
  // didn't attach a pick (older cached prediction, odds fetch failed).
  const bestEdge = d.best_pick
    ? edgeFromBackendPick(d.best_pick)
    : (odds ? findBestQ1Edge(d, odds, home, away) : null)

  return (
    <div className="results">
      {/* Season Context Banner - parity with NHL. Shows regular-season
          vs playoff context when the prediction payload provides it. */}
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
          <WinProbBar
            wp={{ home: d.q1_ml_home || 0.5, away: 1 - (d.q1_ml_home || 0.5) }}
            home={home}
            away={away}
            variant="detail"
          />
        )}

        <div className="key-stats">
          <StatRow label="Q1 Total" value={total.toFixed(1)} />
          <StatRow
            label="Q1 Spread"
            value={`${homeFav ? home.abbreviation : away.abbreviation} ${Math.abs(margin).toFixed(1)}`}
          />
          {d.spread_cover_prob != null && (
            <StatRow label="Cover %" value={pct(d.spread_cover_prob)} />
          )}
        </div>

        <EdgeCallout edge={bestEdge} />
      </div>

      {/* Model Signals: factor / MC breakdown for Q1 home_win + total.
          GBM column stays blank until NBA GBM is trained. Renders
          nothing when pred.ensemble is empty. */}
      <ModelSignals pred={d} sport="nba" home={home} away={away} />

      {/* Key Factors */}
      {d.factors && (
        <div className="result-card">
          <h2>Q1 Key Factors</h2>
          {d.factors.pace_factor && (
            <div className="stat-row">
              <span
                className="stat-label"
                title="Combined possessions per game vs the league baseline (1.00x = average). Above 1 means more shot attempts in Q1 = higher scoring; below 1 = slower."
              >
                Pace Factor
              </span>
              <span className="stat-value">{d.factors.pace_factor.toFixed(2)}x</span>
            </div>
          )}
          {d.factors.home_court_boost && (
            <div className="stat-row">
              <span
                className="stat-label"
                title="Empirically-calibrated home-team Q1 advantage (~+0.7 pts vs road)."
              >
                Home Court Q1 Boost
              </span>
              <span className="stat-value positive">+{d.factors.home_court_boost} pts</span>
            </div>
          )}
          {d.factors.rest_adj && (
            <>
              {d.factors.rest_adj.home !== 0 && (
                <div className="stat-row">
                  <span
                    className="stat-label"
                    title="Penalty for playing on the second night of a back-to-back. Tired legs typically cost ~1 pt in Q1."
                  >
                    {home.abbreviation} on back-to-back
                  </span>
                  <span className={`stat-value ${d.factors.rest_adj.home < 0 ? 'negative' : 'positive'}`}>
                    {d.factors.rest_adj.home > 0 ? '+' : ''}{d.factors.rest_adj.home} pts
                  </span>
                </div>
              )}
              {d.factors.rest_adj.away !== 0 && (
                <div className="stat-row">
                  <span
                    className="stat-label"
                    title="Penalty for playing on the second night of a back-to-back. Tired legs typically cost ~1 pt in Q1."
                  >
                    {away.abbreviation} on back-to-back
                  </span>
                  <span className={`stat-value ${d.factors.rest_adj.away < 0 ? 'negative' : 'positive'}`}>
                    {d.factors.rest_adj.away > 0 ? '+' : ''}{d.factors.rest_adj.away} pts
                  </span>
                </div>
              )}
            </>
          )}
          {d.factors.home_q1_off && (
            <div className="stat-row">
              <span
                className="stat-label"
                title="Average points each team scores in Q1 over the season (higher = better offense)."
              >
                Q1 PPG scored
              </span>
              <span className="stat-value">{away.abbreviation} {d.factors.away_q1_off?.toFixed(1)} / {home.abbreviation} {d.factors.home_q1_off?.toFixed(1)}</span>
            </div>
          )}
          {d.factors.home_q1_def && (
            <div className="stat-row">
              <span
                className="stat-label"
                title="Average points each team allows in Q1 over the season (lower = better defense)."
              >
                Q1 PPG allowed
              </span>
              <span className="stat-value">{away.abbreviation} {d.factors.away_q1_def?.toFixed(1)} / {home.abbreviation} {d.factors.home_q1_def?.toFixed(1)}</span>
            </div>
          )}
        </div>
      )}

      <WhyThisPick
        pred={d}
        pick={bestEdge ? { type: 'Q1', pick: bestEdge.label, odds: bestEdge.odds } : null}
        home={home}
        away={away}
        title="Why this Q1 pick?"
      />
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

  // CI half-width on the win-probability point estimate, driven by
  // team games played. Served on engine.nba_q1_predict.confidence_ci.
  const ciHw = d.confidence_ci?.ci_half_width ?? null

  return (
    <div className="picks-card">
      <h2>Q1 Model Picks</h2>
      {picks.map((p, i) => (
        <PickRow key={i} label={p.label} pick={p.pick} prob={p.prob} odds={p.odds} pct={pct} ciHw={ciHw} />
      ))}
      <div className="picks-footer">
        Q1 projected total: <strong>{(d.predicted_total || 0).toFixed(1)}</strong>
      </div>
    </div>
  )
}

function PickRow({ label, pick, prob, odds, pct, ciHw }) {
  const conf = prob > 0.60 ? 'high' : prob > 0.53 ? 'med' : 'low'
  let edge = null
  let kelly = null
  if (odds && prob) {
    const implied = impliedFromOdds(odds)
    edge = ((prob - implied) * 100).toFixed(1)
    if (parseFloat(edge) > 0) kelly = kellyFraction(prob, odds)
  }

  const probLow  = (prob != null && ciHw != null) ? Math.max(0, prob - ciHw) : null
  const probHigh = (prob != null && ciHw != null) ? Math.min(1, prob + ciHw) : null

  return (
    <div className={`pick-row conf-${conf}`}>
      <div className="pick-label">{label}</div>
      <div className="pick-choice">
        <span className="pick-name">{pick}</span>
        {odds && <span className="pick-odds">({odds > 0 ? '+' : ''}{odds})</span>}
      </div>
      <div className="pick-numbers">
        <span className={`pick-prob conf-${conf}`}>{pct(prob)}</span>
        <ProbHistogram prob={prob} low={probLow} high={probHigh} halfWidth={ciHw} />
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

// Engine best_pick → EdgeCallout shape. NBA Q1 picks emit type like
// "Q1_SPREAD" / "Q1_TOTAL" / "Q1_ML"; for ML we render "{abbr} Q1 ML",
// otherwise the engine's pick.pick already contains the full label
// ("Over 54.5 Q1", "LAL -3.5 Q1", etc).
function edgeFromBackendPick(pick) {
  if (!pick) return null
  const label = pick.type === 'Q1_ML' ? `${pick.pick} Q1 ML` : pick.pick
  return {
    label,
    odds: pick.odds,
    edge: pick.edge,
    rating: pick.confidence || 'lean',
  }
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
