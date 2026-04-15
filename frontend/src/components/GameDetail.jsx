import PredictionResults from './PredictionResults'
import SharedGameHeader from './gameDetail/SharedGameHeader'
import { kellyFraction } from './gameDetail/kelly'
import ProbHistogram from './gameDetail/ProbHistogram'

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

      {/* Model Prediction - two-column layout */}
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

            {/* Right: quick picks summary. mergedPrediction.picks comes
                from engine.picks.generate_picks() -- same engine the
                Scoreboard best-bet badge and POTD use. */}
            <div className="prediction-sidebar">
              <BettingPicks data={mergedPrediction} />
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


// Render the unified picks list from engine.picks.generate_picks() -- the
// SAME engine the Scoreboard best-bet badge and POTD selection use. No
// client-side edge/direction-filter logic lives here: if the engine says a
// market has no playable edge (or the direction is disabled in config),
// the row simply doesn't appear. This guarantees the sidebar, scoreboard
// card, and POTD never disagree on what the model is recommending.
function BettingPicks({ data }) {
  const d = data
  const pct = n => n == null ? '-' : `${(n * 100).toFixed(1)}%`
  const picks = Array.isArray(d?.picks) ? d.picks : []
  const bestKey = d?.best_pick
    ? `${d.best_pick.type}|${d.best_pick.pick}`
    : null
  const total = d?.total
  // Odds snapshot the engine used. Carries `provider` (full-game book,
  // typically DraftKings) and `per_event_provider` (NRFI/F5 book, which
  // is whichever of DK/FD/MGM/Bovada had data). Surfaced per row so the
  // user knows where each price came from.
  const odds = d?.odds || {}
  // Runtime overrides written by engine.train --apply. When non-empty
  // the sidebar renders a subtle notice so the user knows certain
  // markets have been suppressed by data, not by a source-code edit.
  const overrides = Array.isArray(d?.active_overrides) ? d.active_overrides : []

  return (
    <div className="picks-card">
      <h2>Model Picks</h2>

      <OverrideNotice overrides={overrides} />

      {picks.length === 0 && (
        <div className="picks-empty" style={{padding:'1rem', color:'#94a3b8', fontSize:'0.85rem'}}>
          No playable edge found on any market.
        </div>
      )}

      {picks.map((p, i) => (
        <PickRow
          key={`${p.type}-${p.pick}-${i}`}
          engine={p}
          isBest={bestKey === `${p.type}|${p.pick}`}
          provider={_providerForPick(p.type, odds)}
          pct={pct}
        />
      ))}

      {total != null && (
        <div className="picks-footer">
          Model projected total: <strong>{total.toFixed(1)}</strong>
        </div>
      )}
    </div>
  )
}


// Small amber notice listing any runtime overrides currently silencing
// picks. Individual overrides show on hover with the supporting stats
// (N samples, p-value, days until expiry). Rendering nothing when the
// list is empty keeps the card clean in the default case.
function OverrideNotice({ overrides }) {
  if (!overrides || overrides.length === 0) return null
  const labels = overrides.map(o => _prettyFlagName(o.key))
  const tooltip = overrides.map(o =>
    `${o.key} = ${o.value}\n` +
    `  ${o.reason || '(no reason recorded)'}\n` +
    `  N=${o.n_samples ?? '?'}, p=${o.p_value ?? '?'}, ` +
    `${o.days_remaining != null ? `${o.days_remaining}d left` : 'no expiry'}`
  ).join('\n\n')
  return (
    <div
      title={tooltip}
      style={{
        margin: '0.25rem 0 0.75rem',
        padding: '0.5rem 0.75rem',
        borderRadius: 4,
        background: 'rgba(251,191,36,0.08)',
        border: '1px solid rgba(251,191,36,0.35)',
        color: '#fbbf24',
        fontSize: '0.72rem',
        cursor: 'help',
        lineHeight: 1.35,
      }}
    >
      <strong style={{ letterSpacing: '0.04em' }}>
        {overrides.length === 1 ? '1 MARKET' : `${overrides.length} MARKETS`} SUPPRESSED BY DATA
      </strong>
      <div style={{ opacity: 0.85, marginTop: 2 }}>
        {labels.join(', ')}
      </div>
    </div>
  )
}


// Pretty-print a flag name for the override notice. Strips the
// MLB_ALLOW_ prefix so "MLB_ALLOW_OU_UNDER" renders as "OU Under".
function _prettyFlagName(flag) {
  return String(flag)
    .replace(/^MLB_ALLOW_/, '')
    .replace(/^ENABLE_MLB_/, '')
    .replace(/_/g, ' ')
    .replace(/\bOU\b/g, 'O/U')
    .replace(/\bRL\b/g, 'RL')
    .replace(/\bNRFI\b/g, 'NRFI')
    .replace(/\bYRFI\b/g, 'YRFI')
    .replace(/\bF5\b/g, 'F5')
}


// Per-event markets (NRFI / F5*) come from whichever of DK/FD/MGM/Bovada
// had the line. Full-game markets (ML/O/U/RL) always use DraftKings.
const _PER_EVENT_TYPES = new Set(['1st INN', 'F5 ML', 'F5 O/U', 'F5 RL'])
function _providerForPick(type, odds) {
  if (_PER_EVENT_TYPES.has(type)) {
    return odds.per_event_provider || null
  }
  return odds.provider || null
}


// Engine-produced pick fields: {type, pick, prob, prob_low, prob_high,
// ci_half_width, edge, odds, confidence, adjusted_ev}. The engine
// already filtered for positive edge + allowed direction, so the client
// renders fields verbatim without recomputation.
function PickRow({ engine, isBest, provider, pct }) {
  const {type, pick, prob, prob_low, prob_high, ci_half_width,
         edge, odds, confidence} = engine
  const conf = confidence === 'strong' ? 'high'
    : confidence === 'moderate' ? 'med'
    : confidence === 'lean' ? 'med'
    : 'low'

  const kelly = (odds && prob && parseFloat(edge) > 0)
    ? kellyFraction(prob, odds)
    : null

  return (
    <div className={`pick-row conf-${conf}${isBest ? ' is-best' : ''}`}>
      <div className="pick-label">{labelForType(type)}</div>
      <div className="pick-choice">
        <span className="pick-name">{pick}</span>
        {odds != null && (
          <span className="pick-odds">({odds > 0 ? '+' : ''}{odds})</span>
        )}
        {provider && (
          <span
            className="pick-provider"
            title={`Price source: ${provider}`}
            style={{fontSize:'0.62rem', color:'#64748b', marginLeft:6, letterSpacing:'0.04em'}}
          >
            {provider}
          </span>
        )}
      </div>
      <div className="pick-numbers">
        <span className={`pick-prob conf-${conf}`}>{pct(prob)}</span>
        <ProbHistogram
          prob={prob}
          low={prob_low}
          high={prob_high}
          halfWidth={ci_half_width}
        />
        {edge != null && edge > 0 && (
          <span className="pick-edge positive">+{edge.toFixed ? edge.toFixed(1) : edge}%</span>
        )}
        {kelly != null && kelly > 0 && (
          <span
            title="Quarter-Kelly bet sizing - fraction of bankroll to wager"
            style={{fontSize:'0.68rem',color:'#94a3b8',marginTop:2,cursor:'help'}}
          >
            Kelly: {(kelly * 100).toFixed(1)}%
          </span>
        )}
      </div>
    </div>
  )
}


// Map engine pick types to sidebar row labels. Keep the Moneyline / O/U
// phrasing the old UI used for familiarity.
function labelForType(type) {
  switch (type) {
    case 'ML':      return 'Moneyline'
    case 'O/U':     return 'Over/Under'
    case '1st INN': return '1st Inning'
    case 'RL':      return 'Run Line'
    case 'F5 ML':   return 'F5 Winner'
    case 'F5 O/U':  return 'F5 O/U'
    case 'F5 RL':   return 'F5 Run Line'
    default:        return type
  }
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
