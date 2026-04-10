import PredictionResults from './PredictionResults'
import SharedGameHeader from './gameDetail/SharedGameHeader'
import { kellyFraction, impliedFromOdds } from './gameDetail/kelly'

export default function GameDetail({ game, prediction, loading, onBack }) {
  const mergedPrediction = prediction ? mergePitcherData(prediction, game) : null

  const matchupExtras = (game.home_pitcher || game.away_pitcher) ? (
    <div className="pitching-matchup">
      <div className="pitcher-card">
        <div className="pitcher-label">Away SP</div>
        <div className="pitcher-name">{game.away_pitcher?.name || 'TBD'}</div>
        {game.away_pitcher?.stats?.length > 0 && (
          <div className="pitcher-stats-row">
            {game.away_pitcher.stats.map((s, i) => (
              <span key={i} className="pitcher-stat">{s.name}: {s.value}</span>
            ))}
          </div>
        )}
      </div>
      <div className="vs-label">VS</div>
      <div className="pitcher-card">
        <div className="pitcher-label">Home SP</div>
        <div className="pitcher-name">{game.home_pitcher?.name || 'TBD'}</div>
        {game.home_pitcher?.stats?.length > 0 && (
          <div className="pitcher-stats-row">
            {game.home_pitcher.stats.map((s, i) => (
              <span key={i} className="pitcher-stat">{s.name}: {s.value}</span>
            ))}
          </div>
        )}
      </div>
    </div>
  ) : null

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

        {mergedPrediction && (
          <div className="prediction-layout">
            {/* Left: detailed breakdown */}
            <div className="prediction-main">
              <PredictionResults data={mergedPrediction} odds={game.odds} />
            </div>

            {/* Right: quick picks summary */}
            <div className="prediction-sidebar">
              <BettingPicks data={mergedPrediction} odds={game.odds} />
            </div>
          </div>
        )}

        {!loading && !prediction && (
          <div className="no-prediction">
            <p>Prediction unavailable. Run the data sync first:</p>
            <code>sync.bat</code>
          </div>
        )}
      </div>
    </div>
  )
}


function BettingPicks({ data, odds }) {
  const d = data
  const home = d.home
  const away = d.away
  const wp = d.win_prob
  const es = d.expected_score
  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  // Determine picks
  const mlPick = homeWins ? home : away
  const mlProb = homeWins ? wp.home : wp.away
  const mlOdds = homeWins ? odds?.home_ml : odds?.away_ml

  const total = d.total
  const vegasTotal = odds?.over_under
  const ouResult = vegasTotal && d.over_under
    ? getOUPick(d.over_under, vegasTotal, total)
    : null
  const ouPick = ouResult?.pick
  const ouConf = ouResult?.prob

  const nrfi = d.first_inning?.nrfi
  const nrfiPick = nrfi != null ? (nrfi > 0.50 ? 'NRFI' : 'YRFI') : null
  const nrfiProb = nrfi != null ? (nrfi > 0.50 ? nrfi : d.first_inning.yrfi) : null

  const rl = d.run_line
  const rlPick = rl
    ? (rl.home_minus_1_5 > 0.50
        ? `${home.abbreviation} -1.5`
        : `${away.abbreviation} +1.5`)
    : null
  const rlProb = rl
    ? Math.max(rl.home_minus_1_5, rl.away_plus_1_5)
    : null

  const f5 = d.f5
  const f5Pick = f5
    ? (f5.win_prob.home > f5.win_prob.away ? home.abbreviation : away.abbreviation)
    : null
  const f5Prob = f5 ? Math.max(f5.win_prob.home, f5.win_prob.away) : null

  return (
    <div className="picks-card">
      <h2>Model Picks</h2>

      <PickRow
        label="Moneyline"
        pick={`${mlPick.abbreviation}`}
        prob={mlProb}
        odds={mlOdds}
        pct={pct}
      />

      {ouPick && (
        <PickRow
          label={`O/U ${vegasTotal}`}
          pick={ouPick}
          prob={ouConf}
          pct={pct}
        />
      )}

      {nrfiPick && (
        <PickRow
          label="1st Inning"
          pick={nrfiPick}
          prob={nrfiProb}
          pct={pct}
        />
      )}

      {rlPick && (
        <PickRow
          label="Run Line"
          pick={rlPick}
          prob={rlProb}
          pct={pct}
        />
      )}

      {f5Pick && (
        <PickRow
          label="F5 Winner"
          pick={f5Pick}
          prob={f5Prob}
          pct={pct}
        />
      )}

      <div className="picks-footer">
        Model projected total: <strong>{total.toFixed(1)}</strong>
      </div>
    </div>
  )
}


function PickRow({ label, pick, prob, odds, pct }) {
  const conf = prob > 0.60 ? 'high' : prob > 0.53 ? 'med' : 'low'

  // Calculate edge vs Vegas when real odds available
  let edge = null
  let kelly = null
  if (odds && prob) {
    const implied = impliedFromOdds(odds)
    edge = ((prob - implied) * 100).toFixed(1)
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
            title="Quarter-Kelly bet sizing — fraction of bankroll to wager"
            style={{fontSize:'0.68rem',color:'#94a3b8',marginTop:2,cursor:'help'}}
          >
            Kelly: {(kelly * 100).toFixed(1)}%
          </span>
        )}
      </div>
    </div>
  )
}


function getOUPick(ouLines, vegasTotal, modelTotal) {
  // Try exact match with different string formats
  const vt = parseFloat(vegasTotal)
  let entry = ouLines[String(vt)] || ouLines[vt.toFixed(1)] || ouLines[String(Math.round(vt))]

  // If no exact match, find closest line
  if (!entry) {
    const lines = Object.keys(ouLines).map(Number).sort((a, b) => a - b)
    let closest = lines[0]
    for (const l of lines) {
      if (Math.abs(l - vt) < Math.abs(closest - vt)) closest = l
    }
    entry = ouLines[String(closest)] || ouLines[closest.toFixed(1)]
  }

  if (!entry) {
    // Fallback: use model total vs vegas total
    const pick = modelTotal > vt ? 'Over' : 'Under'
    return { pick, prob: modelTotal > vt ? 0.55 : 0.55 }
  }

  const pick = entry.over > entry.under ? 'Over' : 'Under'
  const prob = Math.max(entry.over, entry.under)
  return { pick, prob }
}


function mergePitcherData(prediction, game) {
  const p = { ...prediction }
  if (p.home?.pitcher?.name === 'TBD' && game.home_pitcher?.name) {
    p.home = { ...p.home, pitcher: { ...p.home.pitcher, name: game.home_pitcher.name } }
  }
  if (p.away?.pitcher?.name === 'TBD' && game.away_pitcher?.name) {
    p.away = { ...p.away, pitcher: { ...p.away.pitcher, name: game.away_pitcher.name } }
  }
  return p
}
