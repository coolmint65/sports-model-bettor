import GameDetailShell from './primitives/GameDetailShell'
import WinProbBar from './primitives/WinProbBar'
import StatRow from './primitives/StatRow'
import SectionCard from './primitives/SectionCard'
import ScoreDisplay from './primitives/ScoreDisplay'
import EdgeCallout from './gameDetail/EdgeCallout'
import WhyThisPick from './gameDetail/WhyThisPick'
import { impliedFromOdds } from './gameDetail/kelly'
import ProbHistogram from './gameDetail/ProbHistogram'
import ModelSignals from './gameDetail/ModelSignals'
import { cn } from '../lib/utils'

/**
 * NBA Game Detail — Q1 prediction page. Phase 2-cleanup restyle:
 * every section composes Tailwind primitives. Sport-specific Q1
 * factors (pace / home boost / B2B / Q1 PPG) inline since they're
 * single-use; injuries get their own InjuriesCard for the per-side
 * "out players" pattern Q1 cares about.
 */
export default function NBAGameDetail({ game, prediction, loading, onBack }) {
  const { home, away } = game

  return (
    <GameDetailShell
      game={game}
      onBack={onBack}
      loading={loading}
      loadingLabel="Running Q1 model…"
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

  const backendRanPicks = Array.isArray(d.picks)
  const bestEdge = backendRanPicks
    ? (d.best_pick ? edgeFromBackendPick(d.best_pick) : null)
    : (odds ? findBestQ1Edge(d, odds, home, away) : null)

  return (
    <div className="space-y-4">
      {d.season_context && d.season_context.implications && (
        <SeasonContextBanner ctx={d.season_context} />
      )}

      <SectionCard title="Q1 Projected Outcome">
        <ScoreDisplay
          home={home}
          away={away}
          homeScore={homeQ1.toFixed(1)}
          awayScore={awayQ1.toFixed(1)}
          homeWins={homeFav}
        />
        <div className="mt-2 text-center text-xs text-muted-foreground tabular-nums">
          Projected Q1 total: <strong>{total.toFixed(1)}</strong> pts ·
          Spread: <strong>{homeFav ? home.abbreviation : away.abbreviation} {Math.abs(margin).toFixed(1)}</strong>
        </div>

        {d.spread_cover_prob != null && (
          <div className="mt-4">
            <WinProbBar
              wp={{ home: d.q1_ml_home || 0.5, away: 1 - (d.q1_ml_home || 0.5) }}
              home={home}
              away={away}
              variant="detail"
            />
          </div>
        )}

        <div className="mt-4 space-y-2">
          <StatRow label="Q1 Total" value={total.toFixed(1)} />
          <StatRow
            label="Q1 Spread"
            value={`${homeFav ? home.abbreviation : away.abbreviation} ${Math.abs(margin).toFixed(1)}`}
          />
          {d.spread_cover_prob != null && (
            <StatRow label="Cover %" value={pct(d.spread_cover_prob)} />
          )}
        </div>

        <div className="mt-4">
          <EdgeCallout edge={bestEdge} />
        </div>
      </SectionCard>

      <ModelSignals pred={d} sport="nba" home={home} away={away} />

      <InjuriesCard
        home={home}
        away={away}
        homeOut={d.factors?.home_roster?.out_players}
        awayOut={d.factors?.away_roster?.out_players}
        homeDelta={d.factors?.home_roster?.q1_delta}
        awayDelta={d.factors?.away_roster?.q1_delta}
      />

      {d.factors && <Q1FactorsCard factors={d.factors} home={home} away={away} />}

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


function Q1FactorsCard({ factors: f, home, away }) {
  return (
    <SectionCard title="Q1 Key Factors">
      <div className="space-y-2">
        {f.pace_factor && (
          <StatRow
            label={
              <span title="Combined possessions per game vs the league baseline (1.00x = average). Above 1 means more shot attempts in Q1; below 1 = slower.">
                Pace Factor
              </span>
            }
            value={`${f.pace_factor.toFixed(2)}x`}
          />
        )}
        {f.home_court_boost && (
          <StatRow
            label={
              <span title="Empirically-calibrated home-team Q1 advantage (~+0.7 pts vs road).">
                Home Court Q1 Boost
              </span>
            }
            value={`+${f.home_court_boost} pts`}
            valueClassName="positive"
          />
        )}
        {f.rest_adj?.home !== 0 && f.rest_adj?.home != null && (
          <StatRow
            label={`${home.abbreviation} on back-to-back`}
            value={`${f.rest_adj.home > 0 ? '+' : ''}${f.rest_adj.home} pts`}
            valueClassName={f.rest_adj.home < 0 ? 'negative' : 'positive'}
          />
        )}
        {f.rest_adj?.away !== 0 && f.rest_adj?.away != null && (
          <StatRow
            label={`${away.abbreviation} on back-to-back`}
            value={`${f.rest_adj.away > 0 ? '+' : ''}${f.rest_adj.away} pts`}
            valueClassName={f.rest_adj.away < 0 ? 'negative' : 'positive'}
          />
        )}
        {f.home_q1_off && (
          <StatRow
            label="Q1 PPG scored"
            value={`${away.abbreviation} ${f.away_q1_off?.toFixed(1)} / ${home.abbreviation} ${f.home_q1_off?.toFixed(1)}`}
          />
        )}
        {f.home_q1_def && (
          <StatRow
            label="Q1 PPG allowed"
            value={`${away.abbreviation} ${f.away_q1_def?.toFixed(1)} / ${home.abbreviation} ${f.home_q1_def?.toFixed(1)}`}
          />
        )}
      </div>
    </SectionCard>
  )
}


function InjuriesCard({ home, away, homeOut, awayOut, homeDelta, awayDelta }) {
  const hOut = Array.isArray(homeOut) ? homeOut : []
  const aOut = Array.isArray(awayOut) ? awayOut : []
  if (hOut.length === 0 && aOut.length === 0) return null

  return (
    <SectionCard title="Injuries (Out)">
      <div className="space-y-4">
        {hOut.length > 0 && (
          <InjurySide
            abbr={home.abbreviation}
            count={hOut.length}
            delta={homeDelta}
            out={hOut}
          />
        )}
        {aOut.length > 0 && (
          <InjurySide
            abbr={away.abbreviation}
            count={aOut.length}
            delta={awayDelta}
            out={aOut}
          />
        )}
      </div>
    </SectionCard>
  )
}

function InjurySide({ abbr, count, delta, out }) {
  return (
    <div>
      <div className="mb-1.5 flex items-center gap-2 text-xs">
        <span className="font-semibold uppercase tracking-wider text-muted-foreground">
          {abbr} — {count} out
        </span>
        {typeof delta === 'number' && delta < 0 && (
          <span className="font-semibold tabular-nums text-negative">
            ({delta.toFixed(1)} Q1 pts)
          </span>
        )}
      </div>
      <ul className="space-y-1">
        {out.map(p => (
          <li
            key={`${abbr}-${p.player_id || p.name}`}
            className="flex items-baseline justify-between gap-2 text-xs"
          >
            <span>
              {p.starter
                ? <strong className="text-foreground">{p.name}</strong>
                : <span className="text-foreground">{p.name}</span>}
              {p.position && (
                <span className="ml-1 text-muted-foreground">({p.position})</span>
              )}
            </span>
            <span className="text-negative font-semibold">
              {(p.status || 'OUT').toUpperCase()}
              {typeof p.q1_impact === 'number' && p.q1_impact > 0 && (
                <span className="ml-2 text-muted-foreground tabular-nums">
                  -{p.q1_impact.toFixed(1)} Q1 pts
                </span>
              )}
            </span>
          </li>
        ))}
      </ul>
    </div>
  )
}


function Q1BettingPicks({ data, odds, home, away }) {
  const d = data
  const pct = n => `${(n * 100).toFixed(1)}%`

  const picks = []
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
  if (d.q1_ml_home != null) {
    const homeFav = d.q1_ml_home > 0.5
    picks.push({
      label: 'Q1 Winner',
      pick: homeFav ? home.abbreviation : away.abbreviation,
      prob: homeFav ? d.q1_ml_home : 1 - d.q1_ml_home,
      odds: homeFav ? (odds?.home_ml || -110) : (odds?.away_ml || -110),
    })
  }

  const ciHw = d.confidence_ci?.ci_half_width ?? null

  return (
    <SectionCard
      title="Q1 Model Picks"
      rightSlot={
        <span className="text-[11px] text-muted-foreground tabular-nums">
          Total: <strong>{(d.predicted_total || 0).toFixed(1)}</strong>
        </span>
      }
    >
      <div className="space-y-3">
        {picks.map((p, i) => (
          <PickRow key={i} {...p} pct={pct} ciHw={ciHw} />
        ))}
      </div>
    </SectionCard>
  )
}


function PickRow({ label, pick, prob, odds, pct, ciHw }) {
  const conf = prob > 0.60 ? 'high' : prob > 0.53 ? 'med' : 'low'
  const confTone =
    conf === 'high' ? 'text-positive' :
    conf === 'med'  ? 'text-primary'  :
                       'text-muted-foreground'
  const probLow  = (prob != null && ciHw != null) ? Math.max(0, prob - ciHw) : null
  const probHigh = (prob != null && ciHw != null) ? Math.min(1, prob + ciHw) : null

  let edge = null
  if (odds && prob) {
    const implied = impliedFromOdds(odds)
    edge = ((prob - implied) * 100).toFixed(1)
  }

  return (
    <div className="rounded-md border border-border bg-background/40 px-3 py-2.5">
      <div className="flex items-center justify-between text-xs">
        <span className="font-semibold uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        {edge && parseFloat(edge) > 0 && (
          <span className="font-bold tabular-nums text-positive">+{edge}%</span>
        )}
      </div>
      <div className="mt-1 flex items-baseline gap-2">
        <span className="text-sm font-bold text-foreground">{pick}</span>
        {odds && (
          <span className="text-xs tabular-nums text-muted-foreground">
            ({odds > 0 ? '+' : ''}{odds})
          </span>
        )}
      </div>
      <div className="mt-1 flex items-center gap-2">
        <span className={cn('text-sm font-semibold tabular-nums', confTone)}>
          {pct(prob)}
        </span>
        <ProbHistogram prob={prob} low={probLow} high={probHigh} halfWidth={ciHw} />
      </div>
    </div>
  )
}


function SeasonContextBanner({ ctx }) {
  const isPlayoffs = ctx.phase === 'playoffs'
  return (
    <div className={cn(
      'rounded-md border px-4 py-2 text-center text-sm font-semibold',
      isPlayoffs
        ? 'border-positive/40 bg-positive/10 text-positive'
        : 'border-primary/40 bg-primary/10 text-primary',
    )}>
      {isPlayoffs ? 'PLAYOFF GAME' : 'LATE SEASON · Playoff Race'}
      {' — '}Model adjusts for higher intensity
    </div>
  )
}


function edgeFromBackendPick(pick) {
  if (!pick) return null
  return {
    label: pick.pick,
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
