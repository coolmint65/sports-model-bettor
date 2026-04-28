import PredictionResults from './PredictionResults'
import GameDetailShell from './primitives/GameDetailShell'
import SectionCard from './primitives/SectionCard'
import ProbHistogram from './gameDetail/ProbHistogram'
import { cn } from '../lib/utils'

/**
 * MLB GameDetail page wrapper. Phase 2-cleanup restyle: matchup
 * pitcher row + sidebar Model Picks now Tailwind. Main column
 * delegates to PredictionResults (already restyled in 2-cleanup).
 */
export default function GameDetail({ game, prediction, loading, onBack }) {
  const mergedPrediction = prediction ? mergePitcherData(prediction, game) : null

  const matchupExtras = (game.home_pitcher || game.away_pitcher) ? (
    <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto_1fr] sm:items-center">
      <PitcherCard label="Away SP" pitcher={game.away_pitcher} />
      <span className="text-center text-[10px] font-bold uppercase tracking-widest text-muted-foreground/60">
        VS
      </span>
      <PitcherCard label="Home SP" pitcher={game.home_pitcher} />
    </div>
  ) : null

  return (
    <GameDetailShell
      game={game}
      sport="mlb"
      onBack={onBack}
      matchupExtras={matchupExtras}
      loading={loading}
      prediction={mergedPrediction}
      noPredictionMessage="Prediction unavailable. Run the data sync first:"
      noPredictionCommand="sync.bat"
      renderMain={pred => <PredictionResults data={pred} odds={game.odds} />}
      renderSidebar={pred => <BettingPicks data={pred} />}
    />
  )
}


function PitcherCard({ label, pitcher }) {
  return (
    <div className="rounded-md border border-border bg-background/40 px-3 py-2.5">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className="mt-0.5 text-sm font-bold text-foreground truncate">
        {pitcher?.name || 'TBD'}
      </div>
      {pitcher?.stats?.length > 0 && (
        <div className="mt-1 flex flex-wrap gap-x-3 text-[11px] tabular-nums text-muted-foreground">
          {pitcher.stats.map((s, i) => (
            <span key={i}>{s.name}: {s.value}</span>
          ))}
        </div>
      )}
    </div>
  )
}


function BettingPicks({ data }) {
  const d = data
  const pct = n => n == null ? '-' : `${(n * 100).toFixed(1)}%`
  const picks = Array.isArray(d?.picks) ? d.picks : []
  const bestKey = d?.best_pick ? `${d.best_pick.type}|${d.best_pick.pick}` : null
  const total = d?.total
  const odds = d?.odds || {}
  const overrides = Array.isArray(d?.active_overrides) ? d.active_overrides : []

  return (
    <SectionCard
      title="Model Picks"
      rightSlot={total != null && (
        <span className="text-[11px] text-muted-foreground tabular-nums">
          Total: <strong>{Math.round(total)}</strong>
        </span>
      )}
    >
      <div className="space-y-3">
        <OverrideNotice overrides={overrides} />
        {picks.length === 0 && (
          <div className="rounded-md border border-dashed border-border bg-card/50 px-3 py-4 text-center text-xs text-muted-foreground">
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
      </div>
    </SectionCard>
  )
}


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
      className="rounded-md border border-warning/35 bg-warning/10 px-3 py-2 text-xs text-warning cursor-help leading-snug"
    >
      <strong className="tracking-wider">
        {overrides.length === 1 ? '1 MARKET' : `${overrides.length} MARKETS`} SUPPRESSED BY DATA
      </strong>
      <div className="mt-0.5 opacity-85">{labels.join(', ')}</div>
    </div>
  )
}


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


const _PER_EVENT_TYPES = new Set(['1st INN', 'F5 ML', 'F5 O/U', 'F5 RL'])
function _providerForPick(type, odds) {
  if (_PER_EVENT_TYPES.has(type)) return odds.per_event_provider || null
  return odds.provider || null
}


function PickRow({ engine, isBest, provider, pct }) {
  const { type, pick, prob, prob_low, prob_high, ci_half_width,
          edge, odds, confidence } = engine
  const conf = confidence === 'strong' ? 'high'
    : confidence === 'moderate' ? 'med'
    : confidence === 'lean' ? 'med'
    : 'low'
  const confTone =
    conf === 'high' ? 'text-positive' :
    conf === 'med'  ? 'text-primary'  :
                       'text-muted-foreground'

  return (
    <div className={cn(
      'rounded-md border bg-background/40 px-3 py-2.5',
      isBest ? 'border-primary/40 ring-1 ring-primary/20' : 'border-border',
    )}>
      <div className="flex items-center justify-between text-xs">
        <span className="font-semibold uppercase tracking-wider text-muted-foreground">
          {labelForType(type)}
        </span>
        {edge != null && edge > 0 && (
          <span className="font-bold tabular-nums text-positive">
            +{edge.toFixed ? edge.toFixed(1) : edge}%
          </span>
        )}
      </div>
      <div className="mt-1 flex items-baseline gap-2">
        <span className="text-sm font-bold text-foreground">{pick}</span>
        {odds != null && (
          <span className="text-xs tabular-nums text-muted-foreground">
            ({odds > 0 ? '+' : ''}{odds})
          </span>
        )}
        {provider && (
          <span
            title={`Price source: ${provider}`}
            className="ml-auto text-[10px] uppercase tracking-wider text-muted-foreground/70"
          >
            {provider}
          </span>
        )}
      </div>
      <div className="mt-1 flex items-center gap-2">
        <span className={cn('text-sm font-semibold tabular-nums', confTone)}>
          {pct(prob)}
        </span>
        <ProbHistogram prob={prob} low={prob_low} high={prob_high} halfWidth={ci_half_width} />
      </div>
    </div>
  )
}


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
