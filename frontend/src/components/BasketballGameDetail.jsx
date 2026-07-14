/**
 * BasketballGameDetail — click-through detail pane for any framework
 * basketball league (WNBA / NCAAM / Euroleague / international).
 *
 * Mirrors NBAGameDetail using the same primitives — GameDetailShell,
 * SectionCard, ScoreDisplay, WinProbBar, StatRow, EdgeCallout — so a
 * user moving from NBA to WNBA sees the same layout, just different
 * data underneath.
 *
 * Renders four sections in the main column:
 *   1. Projected Outcome — score, margin, total, win-prob bar, edge call-out
 *   2. Model Signals — factor / GBM / MC / Blended breakdown per market
 *   3. Per-Team Rates — pace / ORtg / DRtg / recent form (the B1 data)
 *   4. Calibration & Confidence — sample size + calibration source tag
 *
 * Sidebar:
 *   1. Picks list — every emitted pick ranked by edge with confidence chips
 *   2. HR Odds — ML / Spread / Total grid the picks were scored against
 */

import GameDetailShell from './primitives/GameDetailShell'
import SectionCard from './primitives/SectionCard'
import ScoreDisplay from './primitives/ScoreDisplay'
import WinProbBar from './primitives/WinProbBar'
import StatRow from './primitives/StatRow'
import EdgeCallout from './gameDetail/EdgeCallout'
import { cn } from '../lib/utils'


export default function BasketballGameDetail({ rawGame, league, onBack }) {
  // Adapt our backend slate row into the (game, prediction) shape the
  // shell + render-prop callbacks expect.
  const home = {
    abbreviation: rawGame.home_abbr,
    name: rawGame.home_name,
    score: rawGame.home_score,
  }
  const away = {
    abbreviation: rawGame.away_abbr,
    name: rawGame.away_name,
    score: rawGame.away_score,
  }
  const game = {
    id: rawGame.game_id,
    date: rawGame.date ? `${rawGame.date}T19:00:00` : null,
    home,
    away,
    status: {
      state: rawGame.status === 'final' ? 'post'
        : rawGame.status === 'in' ? 'in'
        : 'pre',
      detail: rawGame.status === 'final' ? 'Final' : '',
    },
    odds: rawGame.odds,
  }

  const prediction = rawGame.prediction
  const ensemble = prediction?.ensemble
  const picks = rawGame.picks || []
  const bestPick = rawGame.best_pick

  const renderMain = () => (
    <div className="space-y-4">
      <ProjectedOutcomeCard
        prediction={prediction}
        ensemble={ensemble}
        bestPick={bestPick}
        home={home}
        away={away}
      />
      {ensemble && (
        <ModelSignalsCard
          ensemble={ensemble}
          home={home}
          away={away}
        />
      )}
      {prediction?.factors && (
        <TeamRatesCard
          factors={prediction.factors}
          home={home}
          away={away}
        />
      )}
      <CalibrationCard
        prediction={prediction}
        league={league}
      />
    </div>
  )

  const renderSidebar = () => (
    <div className="space-y-4">
      <PicksListCard picks={picks} />
      <HrOddsCard odds={rawGame.odds} home={home} away={away} />
    </div>
  )

  return (
    <GameDetailShell
      game={game}
      sport={league}
      onBack={onBack}
      loading={false}
      prediction={prediction}
      noPredictionMessage={`Prediction unavailable for ${league.toUpperCase()}.`}
      renderMain={renderMain}
      renderSidebar={renderSidebar}
    />
  )
}


// ── Projected Outcome ───────────────────────────────────────

function ProjectedOutcomeCard({ prediction, ensemble, bestPick, home, away }) {
  // Prefer ensemble outputs when available — that's what the picks
  // layer actually uses. Falls back to factor outputs.
  const ens = ensemble?.ensemble || {}
  const margin = ens.margin ?? prediction?.predicted_margin ?? 0
  const total = ens.total ?? prediction?.predicted_total ?? 0
  const mlHome = ens.ml_home ?? prediction?.ml_home ?? 0.5
  const homeFav = margin > 0
  const homeScore = Math.round((total + margin) / 2)
  const awayScore = Math.round((total - margin) / 2)

  const edge = bestPick ? {
    label: bestPick.pick,
    odds: bestPick.odds,
    edge: bestPick.edge,
    rating: confFor(bestPick.edge),
  } : null

  return (
    <SectionCard title="Projected Outcome">
      <ScoreDisplay
        home={home}
        away={away}
        homeScore={homeScore}
        awayScore={awayScore}
        homeWins={homeFav}
      />
      <div className="mt-2 text-center text-xs text-muted-foreground tabular-nums">
        Projected total: <strong>{Math.round(total)}</strong> pts ·
        Spread: <strong>
          {homeFav ? home.abbreviation : away.abbreviation} {Math.round(Math.abs(margin))}
        </strong>
      </div>

      <div className="mt-4">
        <WinProbBar
          wp={{ home: mlHome, away: 1 - mlHome }}
          home={home}
          away={away}
          variant="detail"
        />
      </div>

      <div className="mt-4 space-y-2">
        <StatRow label="Total" value={Math.round(total)} />
        <StatRow
          label="Spread"
          value={`${homeFav ? home.abbreviation : away.abbreviation} ${Math.round(Math.abs(margin))}`}
        />
      </div>

      <div className="mt-4">
        <EdgeCallout edge={edge} />
      </div>
    </SectionCard>
  )
}


// ── Model Signals (per market) ──────────────────────────────

function ModelSignalsCard({ ensemble, home, away }) {
  const sig = ensemble?.signals || {}
  const ens = ensemble?.ensemble || {}
  const w = ensemble?.weights || {}

  const pct = n => n != null ? `${(n * 100).toFixed(1)}%` : '—'
  const num = n => n != null ? n.toFixed(2) : '—'
  const sgn = n => n != null ? (n > 0 ? `+${n.toFixed(1)}` : n.toFixed(1)) : '—'

  const rows = [
    {
      label: `${home.abbreviation} ML`,
      factor: sig.factor?.ml_home, gbm: sig.gbm?.ml_home, mc: sig.mc?.ml_home,
      blended: ens.ml_home,
      weights: w.ml_home,
      fmt: pct,
    },
    {
      label: 'Margin',
      factor: sig.factor?.margin, gbm: sig.gbm?.margin, mc: sig.mc?.margin,
      blended: ens.margin,
      weights: w.margin,
      fmt: sgn,
    },
    {
      label: 'Total',
      factor: sig.factor?.total, gbm: sig.gbm?.total, mc: sig.mc?.total,
      blended: ens.total,
      weights: w.total,
      fmt: num,
    },
  ]

  return (
    <section className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border">
        <h3 className="text-sm font-semibold text-foreground">Model Signals</h3>
        <p className="mt-0.5 text-[11px] text-muted-foreground leading-snug">
          Each market is the weighted blend of three independent models.
          Picks and edges use the <em>Blended</em> column.
        </p>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-background/40">
              <Th>Market</Th>
              <Th align="right">Factor</Th>
              <Th align="right">GBM</Th>
              <Th align="right">MC</Th>
              <Th align="right" className="text-primary">Blended</Th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i} className="border-b border-border/60 hover:bg-accent/20">
                <td className="px-3 py-2.5">
                  <div className="font-semibold text-foreground">{r.label}</div>
                  {r.weights && (
                    <div className="mt-0.5 text-[10px] text-muted-foreground tabular-nums">
                      F {(r.weights.factor * 100).toFixed(0)}% ·
                      GBM {(r.weights.gbm * 100).toFixed(0)}% ·
                      MC {(r.weights.mc * 100).toFixed(0)}%
                    </div>
                  )}
                </td>
                <Cell value={r.factor} fmt={r.fmt} />
                <Cell value={r.gbm} fmt={r.fmt} />
                <Cell value={r.mc} fmt={r.fmt} />
                <td className={cn(
                  'px-3 py-2.5 text-right tabular-nums font-bold',
                  r.blended != null ? 'text-primary' : 'text-muted-foreground/40',
                )}>
                  {r.fmt(r.blended)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}


// ── Per-team rates (B1 data) ────────────────────────────────

function TeamRatesCard({ factors, home, away }) {
  const hr = factors.home_rates
  const ar = factors.away_rates
  const hrec = factors.home_recent
  const arec = factors.away_recent
  if (!hr && !ar) return null

  return (
    <SectionCard title="Per-Team Rates">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border/60 text-[10px] uppercase tracking-wider text-muted-foreground">
            <Th>Team</Th>
            <Th align="right">Pace</Th>
            <Th align="right">ORtg</Th>
            <Th align="right">DRtg</Th>
            <Th align="right">L10 PPG</Th>
            <Th align="right">L10 Margin</Th>
            <Th align="right">N</Th>
          </tr>
        </thead>
        <tbody>
          <RateRow label={home.abbreviation} rates={hr} recent={hrec} />
          <RateRow label={away.abbreviation} rates={ar} recent={arec} />
        </tbody>
      </table>
    </SectionCard>
  )
}


function RateRow({ label, rates, recent }) {
  const fmt = (n, d=1) => n != null ? Number(n).toFixed(d) : '—'
  return (
    <tr className="border-b border-border/40">
      <td className="px-3 py-2 font-semibold text-foreground">{label}</td>
      <td className="px-3 py-2 text-right tabular-nums">{fmt(rates?.pace)}</td>
      <td className="px-3 py-2 text-right tabular-nums">{fmt(rates?.ortg)}</td>
      <td className="px-3 py-2 text-right tabular-nums">{fmt(rates?.drtg)}</td>
      <td className="px-3 py-2 text-right tabular-nums">{fmt(recent?.recent_scored)}</td>
      <td className={cn(
        'px-3 py-2 text-right tabular-nums',
        recent?.recent_margin > 0 ? 'text-positive'
          : recent?.recent_margin < 0 ? 'text-negative' : '',
      )}>
        {recent?.recent_margin != null
          ? (recent.recent_margin > 0 ? '+' : '') + recent.recent_margin.toFixed(1)
          : '—'}
      </td>
      <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">
        {rates?.n ?? '—'}
      </td>
    </tr>
  )
}


// ── Calibration / confidence note ───────────────────────────

function CalibrationCard({ prediction, league }) {
  const source = prediction?.constants_source || 'unknown'
  const reasoning = prediction?.reasoning || []
  return (
    <SectionCard title="Calibration & Confidence">
      <div className="text-xs text-muted-foreground space-y-1">
        <div>
          Constants:{' '}
          <span className={cn(
            'font-semibold',
            source === 'fitted' ? 'text-positive' : 'text-warning',
          )}>
            {source}
          </span>
          {source === 'prior' && (
            <span className="ml-2 italic">
              — basketball-wide prior; no per-league fit yet
            </span>
          )}
        </div>
        {reasoning.length > 0 && (
          <ul className="mt-2 space-y-0.5 text-foreground/85">
            {reasoning.map((r, i) => <li key={i}>· {r}</li>)}
          </ul>
        )}
      </div>
    </SectionCard>
  )
}


// ── Picks sidebar ───────────────────────────────────────────

function PicksListCard({ picks }) {
  if (!picks.length) {
    return (
      <SectionCard title="Picks">
        <div className="text-xs text-muted-foreground italic py-2">
          No picks above the edge floor for this game.
        </div>
      </SectionCard>
    )
  }
  const sorted = [...picks].sort((a, b) => (b.edge || 0) - (a.edge || 0))
  return (
    <SectionCard title="Picks" rightSlot={
      <span className="text-[10px] text-muted-foreground">
        {sorted.length} pick{sorted.length === 1 ? '' : 's'}
      </span>
    }>
      <div className="space-y-2">
        {sorted.map((p, i) => (
          <div key={i} className="rounded border border-border/60 px-3 py-2">
            <div className="flex items-center justify-between">
              <span className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">
                {p.type}
              </span>
              <ConfChip conf={confFor(p.edge)} />
            </div>
            <div className="mt-1 flex items-center justify-between">
              <span className="text-sm text-foreground">{p.pick}</span>
              <span className="text-sm tabular-nums">
                {p.odds >= 0 ? '+' : ''}{p.odds}
              </span>
            </div>
            <div className="mt-1 flex items-center justify-between text-[11px] text-muted-foreground tabular-nums">
              <span>prob {(p.prob * 100).toFixed(1)}%</span>
              <span className={cn(
                'font-semibold',
                p.edge >= 7 ? 'text-positive' : p.edge >= 4 ? 'text-primary' : '',
              )}>
                edge {p.edge >= 0 ? '+' : ''}{p.edge.toFixed(1)}%
              </span>
            </div>
          </div>
        ))}
      </div>
    </SectionCard>
  )
}


function HrOddsCard({ odds, home, away }) {
  if (!odds) {
    return (
      <SectionCard title="HR Odds">
        <div className="text-xs text-muted-foreground italic py-2">
          No HR markets matched for this game.
        </div>
      </SectionCard>
    )
  }
  return (
    <SectionCard title="HR Odds">
      <div className="space-y-2 text-xs">
        {odds.ml && (
          <div className="flex justify-between">
            <span className="text-muted-foreground">Moneyline</span>
            <span className="tabular-nums">
              {home.abbreviation} {odds.ml.home >= 0 ? '+' : ''}{odds.ml.home}
              {' · '}
              {away.abbreviation} {odds.ml.away >= 0 ? '+' : ''}{odds.ml.away}
            </span>
          </div>
        )}
        {odds.spread && (
          <div className="flex justify-between">
            <span className="text-muted-foreground">Spread</span>
            <span className="tabular-nums">
              {home.abbreviation} {odds.spread.home_pt >= 0 ? '+' : ''}{odds.spread.home_pt}
              {' '}({odds.spread.home_odds >= 0 ? '+' : ''}{odds.spread.home_odds})
            </span>
          </div>
        )}
        {odds.total && (
          <div className="flex justify-between">
            <span className="text-muted-foreground">O/U</span>
            <span className="tabular-nums">
              {odds.total.line}
              {' · O '}{odds.total.over_odds >= 0 ? '+' : ''}{odds.total.over_odds}
              {' / U '}{odds.total.under_odds >= 0 ? '+' : ''}{odds.total.under_odds}
            </span>
          </div>
        )}
      </div>
    </SectionCard>
  )
}


// ── Helpers ────────────────────────────────────────────────

function confFor(edge) {
  if (edge == null) return 'skip'
  if (edge >= 12) return 'strong'
  if (edge >= 7) return 'moderate'
  if (edge >= 4) return 'lean'
  return 'skip'
}


function ConfChip({ conf }) {
  const cls = {
    strong:   'bg-positive/20 text-positive',
    moderate: 'bg-primary/20 text-primary',
    lean:     'bg-muted text-muted-foreground',
    skip:     'bg-muted text-muted-foreground',
  }[conf] || 'bg-muted text-muted-foreground'
  return (
    <span className={cn(
      'rounded-full px-1.5 py-0 text-[9px] font-semibold uppercase tracking-wider',
      cls,
    )}>
      {conf}
    </span>
  )
}


function Th({ children, align = 'left', className }) {
  return (
    <th
      scope="col"
      className={cn(
        'px-3 py-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground',
        align === 'right' && 'text-right',
        className,
      )}
    >
      {children}
    </th>
  )
}


function Cell({ value, fmt }) {
  return (
    <td className={cn(
      'px-3 py-2.5 text-right tabular-nums font-semibold',
      value != null ? 'text-foreground' : 'text-muted-foreground/40',
    )}>
      {value != null ? fmt(value) : '—'}
    </td>
  )
}
