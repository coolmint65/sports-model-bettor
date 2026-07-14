import {
  ordinal, getReasoning, findBestEdge,
  edgeFromBackendPick, pickFromEdge,
} from './nhl/detailHelpers'
import GameDetailShell from './primitives/GameDetailShell'
import WinProbBar from './primitives/WinProbBar'
import StatRow from './primitives/StatRow'
import SectionCard from './primitives/SectionCard'
import ScoreDisplay from './primitives/ScoreDisplay'
import { ProbRow, ProbRowHeader, ProbBox } from './primitives/ProbBar'
import { KeyStatsGrid, KeyStat } from './primitives/KeyStatsGrid'
import RestBadges from './gameDetail/RestBadges'
import EdgeCallout from './gameDetail/EdgeCallout'
import UnderdogNote from './gameDetail/UnderdogNote'
import WhyThisPick from './gameDetail/WhyThisPick'
import { mlToProb, impliedFromOdds } from './gameDetail/kelly'
import ProbHistogram from './gameDetail/ProbHistogram'
import ModelSignals from './gameDetail/ModelSignals'
import { cn } from '../lib/utils'

/**
 * NHL Game Detail page. Phase 2-cleanup: every section composes
 * Tailwind primitives. The thick logic blocks (getReasoning,
 * findBestEdge, OT-tie display, etc.) stay verbatim — only the
 * visible chrome changed.
 */
export default function NHLGameDetail({ game, prediction, loading, onBack }) {
  const { home, away } = game
  const pred = prediction

  const anyConfirmed =
    game.home_goalie?.status === 'confirmed' ||
    game.away_goalie?.status === 'confirmed'
  const anyGoalie = game.home_goalie || game.away_goalie

  const matchupExtras = (
    <div className="space-y-3">
      {anyGoalie && (
        <div className="grid grid-cols-1 gap-2 sm:grid-cols-[1fr_auto_1fr] sm:items-center">
          <GoalieCard label="Away G" goalie={game.away_goalie} predGoalie={pred?.goalie_matchup?.away} />
          <span className="text-center text-[10px] font-bold uppercase tracking-widest text-muted-foreground/60">VS</span>
          <GoalieCard label="Home G" goalie={game.home_goalie} predGoalie={pred?.goalie_matchup?.home} />
        </div>
      )}
      {anyGoalie && (
        <div className="text-center">
          <span className={cn(
            'inline-flex items-center rounded-md border px-2.5 py-0.5 text-[11px] font-semibold',
            anyConfirmed
              ? 'border-positive/30 bg-positive/10 text-positive'
              : 'border-warning/30 bg-warning/10 text-warning',
          )}>
            {anyConfirmed ? '✓ Confirmed goalies' : '~ Expected goalies'}
          </span>
        </div>
      )}
      <RestBadges rest={pred?.rest} home={home} away={away} />
    </div>
  )

  return (
    <GameDetailShell
      game={game}
      sport="nhl"
      onBack={onBack}
      matchupExtras={matchupExtras}
      loading={loading}
      prediction={pred}
      noPredictionMessage="Prediction unavailable. Run the NHL sync first:"
      noPredictionCommand="sync_nhl.bat --full"
      renderMain={p => <NHLPredictionResults data={p} odds={game.odds} home={home} away={away} />}
      renderSidebar={p => <NHLBettingPicks data={p} odds={game.odds} home={home} away={away} />}
    />
  )
}


function GoalieCard({ label, goalie, predGoalie }) {
  const name = goalie?.name || predGoalie?.name || 'TBD'
  const svPct = goalie?.save_pct || predGoalie?.save_pct || 0
  const gaa = goalie?.gaa || predGoalie?.gaa || 0
  const wins = goalie?.wins
  const losses = goalie?.losses
  const otl = goalie?.otl
  const status = goalie?.status
  const hasRecord = wins != null && losses != null

  return (
    <div className="rounded-md border border-border bg-background/40 px-3 py-2.5">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className="mt-0.5 flex items-center gap-1 text-sm font-bold text-foreground">
        {name}
        {status === 'confirmed' && <span className="text-positive">✓</span>}
        {status === 'expected' && <span className="text-warning">~</span>}
      </div>
      {(svPct > 0 || hasRecord) && (
        <div className="mt-1 flex flex-wrap gap-x-3 text-[11px] tabular-nums text-muted-foreground">
          {svPct > 0 && <span>SV%: {svPct.toFixed(3)}</span>}
          {gaa > 0 && <span>GAA: {gaa.toFixed(2)}</span>}
          {hasRecord && <span>{wins}-{losses}-{otl || 0}</span>}
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

  const backendRanPicks = Array.isArray(d.picks)
  const bestEdge = backendRanPicks
    ? (d.best_pick ? edgeFromBackendPick(d.best_pick) : null)
    : (odds ? findBestEdge(d, odds, home, away) : null)
  const reasons = getReasoning(d, home, away)

  // Hockey doesn't end in draws — give the OT goal to the better
  // win-prob side when projected scores tie, and surface the OT
  // disclaimer + regulation-draw odds.
  let homeScore = Math.round(es.home)
  let awayScore = Math.round(es.away)
  const goesToOT = homeScore === awayScore
  const regDrawPct = d.regulation_draw_prob || 0
  if (goesToOT) {
    if (wp.home >= wp.away) homeScore += 1
    else awayScore += 1
  }
  const homeWinsDisplay = homeScore > awayScore

  return (
    <div className="space-y-4">
      {d.season_context && d.season_context.implications && (
        <SeasonContextBanner ctx={d.season_context} />
      )}

      <SectionCard title="Projected Outcome">
        <ScoreDisplay
          home={home}
          away={away}
          homeScore={homeScore}
          awayScore={awayScore}
          homeWins={homeWinsDisplay}
        />

        {goesToOT && regDrawPct >= 0.30 && (
          <div className="mt-2 text-center">
            <span className="inline-flex items-center rounded-md border border-warning/25 bg-warning/10 px-2.5 py-0.5 text-[11px] font-semibold text-warning">
              Projected to go to OT/SO
            </span>
          </div>
        )}

        <div className="mt-2 text-center text-xs text-muted-foreground tabular-nums">
          ~{Math.round(es.home + es.away)} regulation goals expected
          {regDrawPct > 0.10 && ` (${pct(regDrawPct)} chance of OT)`}
        </div>

        <div className="mt-4">
          <WinProbBar wp={wp} home={home} away={away} variant="detail" />
        </div>

        <div className="mt-4 space-y-2">
          <StatRow label="Total" value={Math.round(d.total)} />
          <StatRow
            label="Spread"
            value={`${homeWins ? home.abbreviation : away.abbreviation} ${Math.round(Math.abs(d.spread))}`}
          />
        </div>

        <div className="mt-4 space-y-2">
          <EdgeCallout edge={bestEdge} />
          <UnderdogNote pick={pickFromEdge(bestEdge, home, away)} wp={wp} home={home} away={away} />
        </div>
      </SectionCard>

      <ModelSignals pred={d} sport="nhl" home={home} away={away} />

      {d.factors && <NHLKeyFactorsCard factors={d.factors} home={home} away={away} />}

      {d.goalie_matchup
        && (d.goalie_matchup.home || d.goalie_matchup.away)
        && (d.goalie_matchup.home?.save_pct || d.goalie_matchup.away?.save_pct) && (
        <GoalieImpactCard gm={d.goalie_matchup} home={home} away={away} />
      )}

      <WhyThisPick
        pred={{ ...d, reasoning: reasons }}
        pick={pickFromEdge(bestEdge, home, away)}
        home={home}
        away={away}
      />

      <SectionCard title="Betting Lines">
        {d.over_under && Object.keys(d.over_under).length > 0 && (
          <div>
            <SectionLabel>Over / Under</SectionLabel>
            <ProbRowHeader leftLabel="Over" rightLabel="Under" />
            {Object.entries(d.over_under).map(([line, probs]) => (
              <ProbRow key={line} line={line} leftProb={probs.over} rightProb={probs.under} />
            ))}
          </div>
        )}
        {d.puck_line && (
          <div className="mt-5">
            <SectionLabel>Puck Line</SectionLabel>
            <ProbRowHeader leftLabel={`${home.abbreviation} side`} rightLabel="Other side" />
            <ProbRow line={`${home.abbreviation} -1.5`}
              leftProb={d.puck_line.home_minus_1_5}
              rightProb={1 - d.puck_line.home_minus_1_5} />
            <ProbRow line={`${away.abbreviation} +1.5`}
              leftProb={d.puck_line.away_plus_1_5}
              rightProb={1 - d.puck_line.away_plus_1_5} />
          </div>
        )}
      </SectionCard>

      {d.first_period && (
        <SectionCard title="1st Period Total Goals">
          <div className="flex gap-3">
            <ProbBox label="Over 1.5" value={pct(d.first_period.over_15)}
              sub="2+ goals in 1st period" favored={d.first_period.over_15 > 0.55} />
            <ProbBox label="Under 1.5" value={pct(d.first_period.under_15)}
              sub="0-1 goals in 1st period" favored={d.first_period.under_15 > 0.55} />
          </div>
          <div className="mt-3 text-center text-xs text-muted-foreground tabular-nums">
            Expected P1 total: ~{Math.round(d.first_period.expected_total)} goals
          </div>
        </SectionCard>
      )}

      {d.periods && d.periods.length > 0 && (
        <PeriodBreakdownCard periods={d.periods} es={es} regDrawPct={regDrawPct}
          home={home} away={away} pct={pct} />
      )}

      {d.h2h && d.h2h.games > 0 && (
        <SectionCard title="Head to Head (3yr)">
          <div className="flex items-center justify-center gap-6">
            <div className="text-center">
              <div className="text-lg font-bold text-foreground">{home.abbreviation}</div>
              <div className="text-xs text-muted-foreground tabular-nums mt-0.5">
                {d.h2h.team1_wins} {d.h2h.team1_wins === 1 ? 'Win' : 'Wins'}
              </div>
            </div>
            <span className="text-sm text-muted-foreground">vs</span>
            <div className="text-center">
              <div className="text-lg font-bold text-foreground">{away.abbreviation}</div>
              <div className="text-xs text-muted-foreground tabular-nums mt-0.5">
                {d.h2h.team2_wins} {d.h2h.team2_wins === 1 ? 'Win' : 'Wins'}
              </div>
            </div>
          </div>
          <div className="mt-3 text-center text-xs text-muted-foreground">
            {d.h2h.games} meetings over the last 3 seasons
            {d.h2h.team1_wins > d.h2h.team2_wins
              ? ` — ${home.abbreviation} owns the matchup`
              : d.h2h.team2_wins > d.h2h.team1_wins
                ? ` — ${away.abbreviation} owns the matchup`
                : ' — Even'}
          </div>
        </SectionCard>
      )}

      {d.injuries && (d.injuries.home?.length > 0 || d.injuries.away?.length > 0) && (
        <SectionCard title="Injuries">
          {((d.injuries.home_impact != null && d.injuries.home_impact < 1)
            || (d.injuries.away_impact != null && d.injuries.away_impact < 1)) && (
            <div className="mb-3 flex flex-wrap justify-center gap-4 text-sm">
              {d.injuries.home_impact != null && d.injuries.home_impact < 1 && (
                <span className="text-negative">
                  {home.abbreviation}: ~{Math.round((1 - d.injuries.home_impact) * 100)}% weaker from injuries
                </span>
              )}
              {d.injuries.away_impact != null && d.injuries.away_impact < 1 && (
                <span className="text-negative">
                  {away.abbreviation}: ~{Math.round((1 - d.injuries.away_impact) * 100)}% weaker from injuries
                </span>
              )}
            </div>
          )}
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <InjuryColumn abbr={home.abbreviation} injuries={d.injuries.home} />
            <InjuryColumn abbr={away.abbreviation} injuries={d.injuries.away} />
          </div>
        </SectionCard>
      )}
    </div>
  )
}


function NHLKeyFactorsCard({ factors: f, home, away }) {
  return (
    <SectionCard title="Key Factors" bodyClassName="px-0 py-0">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-background/40">
              <Th>Stat</Th>
              <Th align="right">{away.abbreviation}</Th>
              <Th align="right">Rank</Th>
              <Th align="right">{home.abbreviation}</Th>
              <Th align="right">Rank</Th>
            </tr>
          </thead>
          <tbody>
            <FactorRow label="Power Play"
              awayVal={f.away_pp != null ? (f.away_pp * 100).toFixed(1) + '%' : '-'} awayRank={f.away_pp_rank}
              homeVal={f.home_pp != null ? (f.home_pp * 100).toFixed(1) + '%' : '-'} homeRank={f.home_pp_rank} />
            <FactorRow label="Penalty Kill"
              awayVal={f.away_pk != null ? (f.away_pk * 100).toFixed(1) + '%' : '-'} awayRank={f.away_pk_rank}
              homeVal={f.home_pk != null ? (f.home_pk * 100).toFixed(1) + '%' : '-'} homeRank={f.home_pk_rank} />
            <FactorRow label="Save %"
              awayVal={f.away_sv?.toFixed(3) || '-'} awayRank={f.away_sv_rank}
              homeVal={f.home_sv?.toFixed(3) || '-'} homeRank={f.home_sv_rank} />
            <FactorRow label="Shots/Game"
              awayVal={f.away_shots} awayRank={f.away_shots_rank}
              homeVal={f.home_shots} homeRank={f.home_shots_rank} />
            <FactorRow label="Faceoff %"
              awayVal={(f.away_fo * 100).toFixed(1) + '%'} awayRank={f.away_fo_rank}
              homeVal={(f.home_fo * 100).toFixed(1) + '%'} homeRank={f.home_fo_rank} />
          </tbody>
        </table>
      </div>
    </SectionCard>
  )
}


function FactorRow({ label, awayVal, awayRank, homeVal, homeRank }) {
  return (
    <tr className="border-b border-border/60 last:border-0 hover:bg-accent/20 transition-colors">
      <td className="px-5 py-2.5 font-semibold text-foreground/90">{label}</td>
      <td className="px-3 py-2.5 text-right tabular-nums">{awayVal}</td>
      <td className={cn('px-3 py-2.5 text-right tabular-nums font-bold', rankTone(awayRank))}>
        {awayRank ? ordinal(awayRank) : '-'}
      </td>
      <td className="px-3 py-2.5 text-right tabular-nums">{homeVal}</td>
      <td className={cn('px-3 py-2.5 text-right tabular-nums font-bold', rankTone(homeRank))}>
        {homeRank ? ordinal(homeRank) : '-'}
      </td>
    </tr>
  )
}

function rankTone(r) {
  if (!r) return 'text-muted-foreground'
  if (r <= 5) return 'text-positive'
  if (r <= 10) return 'text-primary'
  if (r <= 20) return 'text-foreground/70'
  if (r <= 27) return 'text-warning'
  return 'text-negative'
}


function PeriodBreakdownCard({ periods, es, regDrawPct, home, away, pct }) {
  return (
    <SectionCard title="Period Breakdown" bodyClassName="px-0 py-0">
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-background/40">
              <Th>Period</Th>
              <Th align="right">{home.abbreviation}</Th>
              <Th align="right">{away.abbreviation}</Th>
              <Th align="right">Total</Th>
              <Th align="right">Scoring %</Th>
            </tr>
          </thead>
          <tbody>
            {periods.map(p => {
              const scoringPct = (es.home + es.away) > 0
                ? (p.total / (es.home + es.away)) * 100 : 33
              const homeLeads = p.home > p.away
              return (
                <tr key={p.period} className="border-b border-border/60 last:border-0 hover:bg-accent/20 transition-colors">
                  <td className="px-5 py-2.5 font-semibold text-foreground/90">
                    {p.period === 'P1' ? '1st Period' : p.period === 'P2' ? '2nd Period' : '3rd Period'}
                  </td>
                  <td className={cn(
                    'px-3 py-2.5 text-right tabular-nums',
                    homeLeads ? 'font-bold text-positive' : 'text-foreground/85',
                  )}>
                    {p.home.toFixed(2)}
                  </td>
                  <td className={cn(
                    'px-3 py-2.5 text-right tabular-nums',
                    !homeLeads ? 'font-bold text-positive' : 'text-foreground/85',
                  )}>
                    {p.away.toFixed(2)}
                  </td>
                  <td className="px-3 py-2.5 text-right tabular-nums font-semibold">{p.total.toFixed(2)}</td>
                  <td className="px-3 py-2.5 text-right tabular-nums text-muted-foreground">{scoringPct.toFixed(0)}%</td>
                </tr>
              )
            })}
            <tr className="border-t border-border bg-background/40 font-bold">
              <td className="px-5 py-2.5">Regulation</td>
              <td className="px-3 py-2.5 text-right tabular-nums">{es.home.toFixed(2)}</td>
              <td className="px-3 py-2.5 text-right tabular-nums">{es.away.toFixed(2)}</td>
              <td className="px-3 py-2.5 text-right tabular-nums">{(es.home + es.away).toFixed(2)}</td>
              <td className="px-3 py-2.5"></td>
            </tr>
          </tbody>
        </table>
      </div>
      {regDrawPct > 0.05 && (
        <div className="mx-5 my-3 rounded-md border border-warning/20 bg-warning/5 px-3 py-1.5 text-center text-xs font-semibold text-warning">
          {pct(regDrawPct)} chance this game is tied after regulation and goes to OT
        </div>
      )}
    </SectionCard>
  )
}


function GoalieImpactCard({ gm, home, away }) {
  const hSv = gm.home?.save_pct || 0
  const aSv = gm.away?.save_pct || 0
  const diff = hSv - aSv
  const SHOTS = 30
  const xgAdvantage = Math.abs(diff) * SHOTS
  const better = diff > 0 ? home.abbreviation : away.abbreviation
  const hasEdge = Math.abs(diff) >= 0.005

  return (
    <SectionCard title="Goalie Impact">
      <div className="space-y-2">
        <StatRow label={`${away.abbreviation} SV%`} value={aSv > 0 ? aSv.toFixed(3) : '-'} />
        <StatRow label={`${home.abbreviation} SV%`} value={hSv > 0 ? hSv.toFixed(3) : '-'} />
        <StatRow
          label="SV% differential"
          value={`${diff >= 0 ? '+' : ''}${diff.toFixed(3)}`}
          valueClassName={!hasEdge ? '' : diff > 0 ? 'positive' : 'warning'}
        />
      </div>
      {hasEdge ? (
        <div className="mt-3 rounded-md border border-positive/15 bg-positive/[0.06] px-3 py-2 text-center text-sm text-positive">
          <span className="font-semibold">{better}</span> goalie edge:
          ~{xgAdvantage.toFixed(2)} goals suppressed per game
        </div>
      ) : (
        <div className="mt-3 text-center text-xs text-muted-foreground">
          Goalie matchup is roughly even
        </div>
      )}
    </SectionCard>
  )
}


function InjuryColumn({ abbr, injuries }) {
  return (
    <div>
      <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
        {abbr}
      </div>
      {injuries?.length > 0 ? (
        <ul className="space-y-1.5">
          {injuries.map((inj, i) => (
            <li key={i} className="flex items-baseline justify-between gap-2 text-xs">
              <span>
                <span className="font-semibold text-foreground">{inj.name}</span>
                {inj.position && (
                  <span className="ml-1 text-muted-foreground">({inj.position})</span>
                )}
              </span>
              <span className={cn(
                'text-[10px] font-semibold uppercase tracking-wider',
                inj.status === 'Out' ? 'text-negative' : 'text-warning',
              )}>
                {inj.status || inj.type || 'Out'}
              </span>
            </li>
          ))}
        </ul>
      ) : (
        <div className="text-xs text-muted-foreground">No injuries reported</div>
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
  const ciHw = d.confidence?.ci_half_width ?? null

  // Always render all four categories so the panel layout is stable.
  // Each row uses the backend's HR-line-aware pick when present; falls
  // back to model-projection-based synth (no edge badge) otherwise so
  // the histogram + prob row still render.
  const backendPicks = Array.isArray(d.picks) ? d.picks : []
  const bestByType = new Map()
  for (const p of backendPicks) {
    const t = p.type || ''
    const existing = bestByType.get(t)
    if (!existing || (p.edge || 0) > (existing.edge || 0)) {
      bestByType.set(t, p)
    }
  }

  const rows = []

  // ── Moneyline ──
  {
    const p = bestByType.get('ML')
    if (p) {
      rows.push({ label: 'Moneyline', pick: p.pick, prob: p.prob,
                  odds: p.odds, edge: p.edge })
    } else {
      const mlPick = homeWins ? home : away
      rows.push({
        label: 'Moneyline',
        pick: mlPick.abbreviation,
        prob: homeWins ? wp.home : wp.away,
        odds: homeWins ? odds?.home_ml : odds?.away_ml,
      })
    }
  }

  // ── O/U ──
  {
    const p = bestByType.get('O/U')
    if (p) {
      rows.push({ label: 'O/U', pick: p.pick, prob: p.prob,
                  odds: p.odds, edge: p.edge })
    } else {
      const vegasTotal = odds?.over_under
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
          rows.push({
            label: `O/U ${vegasTotal}`,
            pick: isOver ? 'Over' : 'Under',
            prob: Math.max(entry.over, entry.under),
            odds: isOver ? odds?.over_odds : odds?.under_odds,
          })
        }
      }
    }
  }

  // ── Puck Line ──
  {
    const p = bestByType.get('PL') || bestByType.get('ALT PL')
    if (p) {
      rows.push({ label: 'Puck Line', pick: p.pick, prob: p.prob,
                  odds: p.odds, edge: p.edge })
    } else {
      const pl = d.puck_line
      if (pl) {
        const hPt = odds?.home_spread_point
        const aPt = odds?.away_spread_point
        const homeIsFav = (hPt != null && hPt < 0) || (pl.home_minus_1_5 > pl.away_minus_1_5)
        let plPick, plProb, plOdds
        if (homeIsFav) {
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
        rows.push({ label: 'Puck Line', pick: plPick, prob: plProb, odds: plOdds })
      }
    }
  }

  // ── 1st Period ──
  // NHL emits P1 picks with bet_type "Period Total" / "Period DNB" /
  // "Period BTS" and the period encoded in the pick text ("P1 Over 1.5",
  // "P1 Winner", "P1 BTS Yes"). Pull the highest-edge "P1 …" pick from
  // the backend list.
  //
  // Suppress the row entirely when no real P1 pick fired — the
  // previous behaviour synthesized a "lean" row from raw probabilities
  // which read as a recommendation, contradicting the P1 segment on
  // the bets card (which correctly says "no pick"). Raw P1 probabilities
  // are still visible in the dedicated "1st Period Total Goals" section
  // above; the Model Picks row is reserved for actual edge-evaluated
  // picks.
  {
    let p = null
    for (const cand of backendPicks) {
      const t = cand.type || ''
      const pk = String(cand.pick || '')
      const isP1 = (t === 'Period DNB' || t === 'Period Total' || t === 'Period BTS')
                    && pk.startsWith('P1 ')
      if (!isP1) continue
      if (!p || (cand.edge || 0) > (p.edge || 0)) p = cand
    }
    if (!p) p = bestByType.get('1st INN') || bestByType.get('1st Period')
    if (p) {
      rows.push({ label: '1st Period', pick: p.pick, prob: p.prob,
                  odds: p.odds, edge: p.edge })
    }
  }

  return (
    <SectionCard
      title="Model Picks"
      rightSlot={
        <span className="text-[11px] text-muted-foreground tabular-nums">
          Total: <strong>{Math.round(d.total)}</strong>
        </span>
      }
    >
      <div className="space-y-3">
        {rows.map((r, i) => <PickRow key={i} {...r} pct={pct} ciHw={ciHw} />)}
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

  // Backend supplies edge already evaluated against HR's offered price;
  // recompute only when caller didn't pass one (legacy synth path).
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


function SectionLabel({ children }) {
  return (
    <h4 className="mb-2 text-[11px] font-semibold uppercase tracking-widest text-muted-foreground">
      {children}
    </h4>
  )
}


function Th({ children, align = 'left' }) {
  return (
    <th
      scope="col"
      className={cn(
        'px-3 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground',
        align === 'right' && 'text-right',
      )}
    >
      {children}
    </th>
  )
}


