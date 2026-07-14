import { useState } from 'react'
import { edgeFromBackendPick, findBestQ1Edge } from './nba/detailHelpers'
import GameDetailShell from './primitives/GameDetailShell'
import MarketToggle from './primitives/MarketToggle'
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
 * NBA Game Detail — Full Game (default) or Q1 view, switched via the
 * header MarketToggle. Earlier behaviour stacked both panels which
 * made it hard to read at a glance; user requested a toggle on
 * 2026-04-29 so the detail mirrors the Bets-tab toggle pattern.
 *
 * The toggle defaults to 'full' even when prediction.full is missing
 * — falls back to Q1 only if the backend didn't ship Full data
 * (NBA preseason / partial sync).
 */
export default function NBAGameDetail({ game, prediction, loading, onBack }) {
  const { home, away } = game
  const hasFull = !!(prediction && prediction.full)
  const [view, setView] = useState(hasFull ? 'full' : 'q1')

  // Defensive: if prediction lands without `full` we can't honour
  // 'full', force back to Q1.
  const activeView = (view === 'full' && !hasFull) ? 'q1' : view

  const toggleOptions = hasFull
    ? [{ id: 'full', label: 'Full Game' }, { id: 'q1', label: 'Q1' }]
    : [{ id: 'q1', label: 'Q1' }]

  const headerSlot = toggleOptions.length > 1
    ? <MarketToggle options={toggleOptions} active={activeView} onChange={setView} />
    : null

  const renderMain = (p) => (
    activeView === 'full'
      ? <FullPredictionResults data={p} odds={game.odds} home={home} away={away} />
      : <Q1PredictionResults data={p} odds={game.odds} home={home} away={away} />
  )

  const renderSidebar = (p) => (
    activeView === 'full'
      ? <FullBettingPicks data={p} odds={game.odds} home={home} away={away} />
      : <Q1BettingPicks data={p} odds={game.odds} home={home} away={away} />
  )

  return (
    <GameDetailShell
      game={game}
      sport="nba"
      onBack={onBack}
      loading={loading}
      loadingLabel="Running NBA model…"
      prediction={prediction}
      noPredictionMessage="NBA prediction unavailable. Run NBA sync first:"
      noPredictionCommand="sync_nba.bat --full"
      renderMain={renderMain}
      renderSidebar={renderSidebar}
      headerSlot={headerSlot}
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
          homeScore={Math.round(homeQ1)}
          awayScore={Math.round(awayQ1)}
          homeWins={homeFav}
        />
        <div className="mt-2 text-center text-xs text-muted-foreground tabular-nums">
          Projected Q1 total: <strong>{Math.round(total)}</strong> pts ·
          Spread: <strong>{homeFav ? home.abbreviation : away.abbreviation} {Math.round(Math.abs(margin))}</strong>
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
          <StatRow label="Q1 Total" value={Math.round(total)} />
          <StatRow
            label="Q1 Spread"
            value={`${homeFav ? home.abbreviation : away.abbreviation} ${Math.round(Math.abs(margin))}`}
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


// ── Full-Game view (Phase 2k) ─────────────────────────────────────
//
// Reads from data.full (factor model output) and uses data.full_picks
// for the right-side Model Picks panel. Mirrors the Q1 layout so
// switching the toggle keeps the user oriented.

function FullPredictionResults({ data, odds, home, away }) {
  const full = data.full || {}
  const pct = n => n != null ? `${(n * 100).toFixed(1)}%` : '-'

  const homeExp = full.home_expected || 0
  const awayExp = full.away_expected || 0
  const margin = full.predicted_margin || 0
  const total = full.predicted_total || 0
  const homeFav = margin > 0
  const wp = {
    home: full.ml_home || 0.5,
    away: 1 - (full.ml_home || 0.5),
  }

  // Best full-game edge for the EdgeCallout — pick the highest-edge
  // primary-market pick from data.full_picks.
  const fullPicks = Array.isArray(data.full_picks) ? data.full_picks : []
  const PRIMARY = new Set(['ML', 'SPREAD', 'TOTAL'])
  const primary = fullPicks
    .filter(p => PRIMARY.has(p.type) && (p.confidence || 'lean') !== 'skip')
    .sort((a, b) => (b.edge || 0) - (a.edge || 0))
  const bestPick = primary[0] || fullPicks[0] || null
  const bestEdge = bestPick ? {
    label: bestPick.pick,
    odds: bestPick.odds,
    edge: bestPick.edge,
    rating: bestPick.confidence || 'lean',
  } : null

  return (
    <div className="space-y-4">
      {data.season_context && data.season_context.implications && (
        <SeasonContextBanner ctx={data.season_context} />
      )}

      <SectionCard title="Full Game Projected Outcome">
        <ScoreDisplay
          home={home}
          away={away}
          homeScore={Math.round(homeExp)}
          awayScore={Math.round(awayExp)}
          homeWins={homeFav}
        />
        <div className="mt-2 text-center text-xs text-muted-foreground tabular-nums">
          Projected total: <strong>{Math.round(total)}</strong> pts ·
          Spread: <strong>{homeFav ? home.abbreviation : away.abbreviation} {Math.round(Math.abs(margin))}</strong>
        </div>

        <div className="mt-4">
          <WinProbBar wp={wp} home={home} away={away} variant="detail" />
        </div>

        <div className="mt-4 space-y-2">
          <StatRow label="Total" value={Math.round(total)} />
          <StatRow
            label="Spread"
            value={`${homeFav ? home.abbreviation : away.abbreviation} ${Math.round(Math.abs(margin))}`}
          />
        </div>

        <div className="mt-4">
          <EdgeCallout edge={bestEdge} />
        </div>
      </SectionCard>

      <ModelSignals pred={data} sport="nba" home={home} away={away} view="full" />

      <InjuriesCard
        home={home}
        away={away}
        homeOut={full.factors?.home_roster?.out_players}
        awayOut={full.factors?.away_roster?.out_players}
        homeDelta={full.factors?.home_roster?.delta}
        awayDelta={full.factors?.away_roster?.delta}
        scopeLabel="pts"
        impactKey="full_impact"
      />

      {full.factors && <FullFactorsCard factors={full.factors} home={home} away={away} />}

      <WhyThisPick
        pred={full}
        pick={bestEdge ? { type: bestPick?.type, pick: bestEdge.label, odds: bestEdge.odds } : null}
        home={home}
        away={away}
        title="Why this pick?"
      />
    </div>
  )
}


function FullBettingPicks({ data, odds, home, away }) {
  const pct = n => `${(n * 100).toFixed(1)}%`
  const ciHw = (data.full?.confidence_ci?.ci_half_width)
              ?? (data.confidence_ci?.ci_half_width)
              ?? null

  const picks = Array.isArray(data.full_picks) ? data.full_picks : []
  // One row per market category. Highest-edge variant per type wins.
  // Categories shown: Moneyline, Spread, Total — alt lines available
  // in tooltips/hover but don't get their own panel rows so the
  // layout stays scannable and the user isn't tempted to chase the
  // longshot ALT bets the backtest hasn't validated.
  const bestByType = new Map()
  for (const p of picks) {
    const t = p.type || ''
    if (t === 'ALT SPREAD' || t === 'ALT TOTAL') continue  // primary-only headline
    const existing = bestByType.get(t)
    if (!existing || (p.edge || 0) > (existing.edge || 0)) {
      bestByType.set(t, p)
    }
  }

  const rows = []
  for (const t of ['SPREAD', 'TOTAL', 'ML']) {
    const p = bestByType.get(t)
    const labelFor = t === 'ML' ? 'Moneyline' : (t === 'SPREAD' ? 'Spread' : 'O/U')
    if (p) {
      rows.push({ label: labelFor, pick: p.pick, prob: p.prob,
                  odds: p.odds, edge: p.edge })
    } else {
      // No backend pick — render the model's projected line as a
      // reference row so the panel layout is stable.
      const full = data.full || {}
      if (t === 'SPREAD' && full.predicted_margin != null) {
        const margin = full.predicted_margin
        const fav = margin > 0 ? home : away
        rows.push({
          label: labelFor,
          pick: `${fav.abbreviation} ${Math.abs(margin) > 0 ? (margin > 0 ? '-' : '+') + Math.round(Math.abs(margin)) : ''}`,
          prob: full.spread_cover_prob,
          odds: odds?.home_spread_odds || -110,
        })
      } else if (t === 'TOTAL' && full.predicted_total != null) {
        const ot = full.predicted_total
        const pickOver = (full.over_prob || 0.5) > 0.5
        rows.push({
          label: labelFor,
          pick: `${pickOver ? 'Over' : 'Under'} ${Math.round(ot)}`,
          prob: pickOver ? full.over_prob : 1 - full.over_prob,
          odds: pickOver ? (odds?.over_odds || -110) : (odds?.under_odds || -110),
        })
      } else if (t === 'ML' && full.ml_home != null) {
        const homeFav = full.ml_home > 0.5
        rows.push({
          label: labelFor,
          pick: homeFav ? home.abbreviation : away.abbreviation,
          prob: homeFav ? full.ml_home : 1 - full.ml_home,
          odds: homeFav ? (odds?.home_ml || -110) : (odds?.away_ml || -110),
        })
      }
    }
  }

  return (
    <SectionCard
      title="Full-Game Model Picks"
      rightSlot={
        <span className="text-[11px] text-muted-foreground tabular-nums">
          Total: <strong>{Math.round((data.full?.predicted_total) || 0)}</strong>
        </span>
      }
    >
      <div className="space-y-3">
        {rows.map((r, i) => <PickRow key={i} {...r} pct={pct} ciHw={ciHw} />)}
      </div>
    </SectionCard>
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


function FullFactorsCard({ factors: f, home, away }) {
  return (
    <SectionCard title="Full Game Key Factors">
      <div className="space-y-2">
        {f.pace_factor != null && (
          <StatRow
            label={
              <span title="Combined possessions per game vs the league baseline (1.00x = average). Above 1 means more shot attempts; below 1 = slower.">
                Pace Factor
              </span>
            }
            value={`${f.pace_factor.toFixed(2)}x`}
          />
        )}
        {f.home_court_boost != null && (
          <StatRow
            label={
              <span title="Empirically-calibrated home-team full-game advantage (~+2 pts vs road).">
                Home Court Boost
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
        {f.home_off != null && f.away_off != null && (
          <StatRow
            label="Full Game PPG scored"
            value={`${away.abbreviation} ${f.away_off.toFixed(1)} / ${home.abbreviation} ${f.home_off.toFixed(1)}`}
          />
        )}
        {f.home_def != null && f.away_def != null && (
          <StatRow
            label="Full Game PPG allowed"
            value={`${away.abbreviation} ${f.away_def.toFixed(1)} / ${home.abbreviation} ${f.home_def.toFixed(1)}`}
          />
        )}
        {f.home_off_rtg != null && f.away_off_rtg != null && (
          <StatRow
            label={
              <span title="Points scored per 100 possessions. Pace-adjusted scoring efficiency.">
                Off Rating
              </span>
            }
            value={`${away.abbreviation} ${f.away_off_rtg.toFixed(1)} / ${home.abbreviation} ${f.home_off_rtg.toFixed(1)}`}
          />
        )}
        {f.home_def_rtg != null && f.away_def_rtg != null && (
          <StatRow
            label={
              <span title="Points allowed per 100 possessions. Lower = better defense.">
                Def Rating
              </span>
            }
            value={`${away.abbreviation} ${f.away_def_rtg.toFixed(1)} / ${home.abbreviation} ${f.home_def_rtg.toFixed(1)}`}
          />
        )}
        {f.recent_form?.home && (
          <StatRow
            label={`${home.abbreviation} recent form`}
            value={f.recent_form.home}
          />
        )}
        {f.recent_form?.away && (
          <StatRow
            label={`${away.abbreviation} recent form`}
            value={f.recent_form.away}
          />
        )}
      </div>
    </SectionCard>
  )
}


function InjuriesCard({ home, away, homeOut, awayOut, homeDelta, awayDelta,
                        scopeLabel = 'Q1 pts',
                        impactKey = 'q1_impact' }) {
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
            scopeLabel={scopeLabel}
            impactKey={impactKey}
          />
        )}
        {aOut.length > 0 && (
          <InjurySide
            abbr={away.abbreviation}
            count={aOut.length}
            delta={awayDelta}
            out={aOut}
            scopeLabel={scopeLabel}
            impactKey={impactKey}
          />
        )}
      </div>
    </SectionCard>
  )
}

function InjurySide({ abbr, count, delta, out, scopeLabel = 'Q1 pts',
                     impactKey = 'q1_impact' }) {
  return (
    <div>
      <div className="mb-1.5 flex items-center gap-2 text-xs">
        <span className="font-semibold uppercase tracking-wider text-muted-foreground">
          {abbr} — {count} out
        </span>
        {typeof delta === 'number' && delta < 0 && (
          <span className="font-semibold tabular-nums text-negative">
            ({delta.toFixed(1)} {scopeLabel})
          </span>
        )}
      </div>
      <ul className="space-y-1">
        {out.map(p => {
          const impact = p[impactKey]
          return (
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
                {typeof impact === 'number' && impact > 0 && (
                  <span className="ml-2 text-muted-foreground tabular-nums">
                    -{impact.toFixed(1)} {scopeLabel}
                  </span>
                )}
              </span>
            </li>
          )
        })}
      </ul>
    </div>
  )
}


function Q1BettingPicks({ data, odds, home, away }) {
  const d = data
  const pct = n => `${(n * 100).toFixed(1)}%`

  // Always render all three categories (Spread / O/U / Winner) so the
  // panel layout is stable even when the backend picker doesn't
  // surface a betable edge in every market. Categories with a
  // backend pick use HR's actual line + computed edge; categories
  // without one fall back to the model's raw projection (no edge
  // badge, but the histogram + prob row still render).
  const backendPicks = Array.isArray(d.picks) ? d.picks : []
  const bestByType = new Map()
  for (const p of backendPicks) {
    const t = p.type || ''
    const existing = bestByType.get(t)
    if (!existing || (p.edge || 0) > (existing.edge || 0)) {
      bestByType.set(t, p)
    }
  }

  const picks = []

  // ── Q1 Spread ──
  {
    const p = bestByType.get('Q1_SPREAD')
    if (p) {
      picks.push({ label: 'Q1 Spread', pick: p.pick, prob: p.prob,
                   odds: p.odds, edge: p.edge })
    } else if (d.spread_cover_prob != null) {
      const margin = d.predicted_margin || 0
      const fav = margin > 0 ? home : away
      picks.push({
        label: 'Q1 Spread',
        pick: `${fav.abbreviation} ${Math.abs(margin) > 0 ? (margin > 0 ? '-' : '+') + Math.round(Math.abs(margin)) : ''}`,
        prob: d.spread_cover_prob,
        odds: odds?.q1_spread_home_odds || -110,
      })
    }
  }

  // ── Q1 O/U ──
  {
    const p = bestByType.get('Q1_TOTAL')
    if (p) {
      picks.push({ label: 'Q1 O/U', pick: p.pick, prob: p.prob,
                   odds: p.odds, edge: p.edge })
    } else if (d.over_prob != null) {
      const total = d.predicted_total || 0
      const pickOver = d.over_prob > 0.5
      picks.push({
        label: 'Q1 O/U',
        pick: `${pickOver ? 'Over' : 'Under'} ${Math.round(total)}`,
        prob: pickOver ? d.over_prob : 1 - d.over_prob,
        odds: pickOver ? (odds?.q1_over_odds || -110) : (odds?.q1_under_odds || -110),
      })
    }
  }

  // ── Q1 Winner / ML ──
  {
    const p = bestByType.get('Q1_ML')
    if (p) {
      picks.push({ label: 'Q1 Winner', pick: p.pick, prob: p.prob,
                   odds: p.odds, edge: p.edge })
    } else if (d.q1_ml_home != null) {
      const homeFav = d.q1_ml_home > 0.5
      picks.push({
        label: 'Q1 Winner',
        pick: homeFav ? home.abbreviation : away.abbreviation,
        prob: homeFav ? d.q1_ml_home : 1 - d.q1_ml_home,
        odds: homeFav ? (odds?.home_ml || -110) : (odds?.away_ml || -110),
      })
    }
  }

  const ciHw = d.confidence_ci?.ci_half_width ?? null

  return (
    <SectionCard
      title="Q1 Model Picks"
      rightSlot={
        <span className="text-[11px] text-muted-foreground tabular-nums">
          Total: <strong>{Math.round(d.predicted_total || 0)}</strong>
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


function PickRow({ label, pick, prob, odds, edge, pct, ciHw }) {
  const conf = prob > 0.60 ? 'high' : prob > 0.53 ? 'med' : 'low'
  const confTone =
    conf === 'high' ? 'text-positive' :
    conf === 'med'  ? 'text-primary'  :
                       'text-muted-foreground'
  const probLow  = (prob != null && ciHw != null) ? Math.max(0, prob - ciHw) : null
  const probHigh = (prob != null && ciHw != null) ? Math.min(1, prob + ciHw) : null

  // Backend supplies `edge` already computed against HR's actual
  // line/price; recompute only as a fallback for the synthesized path.
  let edgeStr = null
  if (edge != null) {
    edgeStr = Number(edge).toFixed(1)
  } else if (odds && prob) {
    const implied = impliedFromOdds(odds)
    edgeStr = ((prob - implied) * 100).toFixed(1)
  }

  return (
    <div className="rounded-md border border-border bg-background/40 px-3 py-2.5">
      <div className="flex items-center justify-between text-xs">
        <span className="font-semibold uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        {edgeStr && parseFloat(edgeStr) > 0 && (
          <span className="font-bold tabular-nums text-positive">+{edgeStr}%</span>
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


