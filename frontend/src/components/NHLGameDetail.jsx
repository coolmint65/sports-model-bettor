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
          ~{(es.home + es.away).toFixed(1)} regulation goals expected
          {regDrawPct > 0.10 && ` (${pct(regDrawPct)} chance of OT)`}
        </div>

        <div className="mt-4">
          <WinProbBar wp={wp} home={home} away={away} variant="detail" />
        </div>

        <div className="mt-4 space-y-2">
          <StatRow label="Total" value={d.total.toFixed(1)} />
          <StatRow
            label="Spread"
            value={`${homeWins ? home.abbreviation : away.abbreviation} ${Math.abs(d.spread).toFixed(1)}`}
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

  const mlPick = homeWins ? home : away
  const mlProb = homeWins ? wp.home : wp.away
  const mlOdds = homeWins ? odds?.home_ml : odds?.away_ml

  const vegasTotal = odds?.over_under
  let ouPick = null, ouConf = null, ouOdds = null
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
      ouPick = isOver ? 'Over' : 'Under'
      ouConf = Math.max(entry.over, entry.under)
      ouOdds = isOver ? odds?.over_odds : odds?.under_odds
    }
  }

  const pl = d.puck_line
  let plPick = null, plProb = null, plOdds = null
  if (pl) {
    const hPt = odds?.home_spread_point
    const aPt = odds?.away_spread_point
    const homeIsFav = (hPt != null && hPt < 0) || (pl.home_minus_1_5 > pl.away_minus_1_5)

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
  }

  const p1 = d.first_period
  const p1Pick = p1 ? (p1.over_15 > 0.50 ? 'Over 1.5' : 'Under 1.5') : null
  const p1Prob = p1 ? Math.max(p1.over_15, p1.under_15) : null

  const ciHw = d.confidence?.ci_half_width ?? null

  return (
    <SectionCard
      title="Model Picks"
      rightSlot={
        <span className="text-[11px] text-muted-foreground tabular-nums">
          Total: <strong>{d.total.toFixed(1)}</strong>
        </span>
      }
    >
      <div className="space-y-3">
        <PickRow label="Moneyline" pick={mlPick.abbreviation} prob={mlProb} odds={mlOdds} pct={pct} ciHw={ciHw} />
        {ouPick && <PickRow label={`O/U ${vegasTotal}`} pick={ouPick} prob={ouConf} odds={ouOdds} pct={pct} ciHw={ciHw} />}
        {plPick && <PickRow label="Puck Line" pick={plPick} prob={plProb} odds={plOdds} pct={pct} ciHw={ciHw} />}
        {p1Pick && <PickRow label="1st Period" pick={p1Pick} prob={p1Prob} pct={pct} ciHw={ciHw} />}
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


function ordinal(n) {
  if (!n) return ''
  const s = ['th', 'st', 'nd', 'rd']
  const v = n % 100
  return n + (s[(v - 20) % 10] || s[v] || s[0])
}


// ─── Reasoning + edge selection (logic preserved verbatim) ──────────

function getReasoning(pred, home, away) {
  const reasons = []
  const f = pred.factors || {}
  const ctx = pred.season_context || {}
  const hCtx = ctx.home || {}
  const aCtx = ctx.away || {}
  const wp = pred.win_prob || {}

  if (pred.goalie_matchup?.home && pred.goalie_matchup?.away) {
    const h_sv = pred.goalie_matchup.home.save_pct || 0
    const a_sv = pred.goalie_matchup.away.save_pct || 0
    if (h_sv > 0 && a_sv > 0) {
      const diff = Math.abs(h_sv - a_sv)
      const better = h_sv > a_sv ? home.abbreviation : away.abbreviation
      const worse = h_sv > a_sv ? away.abbreviation : home.abbreviation
      if (diff > 0.020) {
        reasons.push(`${better}'s goalie is stopping significantly more shots than ${worse}'s (${Math.max(h_sv, a_sv).toFixed(3)} vs ${Math.min(h_sv, a_sv).toFixed(3)} save %)`)
      } else if (diff > 0.010) {
        reasons.push(`${better} has a slight goalie edge tonight, saving about 1 more goal per 100 shots`)
      } else {
        reasons.push(`Goalie matchup is roughly even tonight (${h_sv.toFixed(3)} vs ${a_sv.toFixed(3)} save %)`)
      }
    }
  }

  const hPace = hCtx.points_pace || 0
  const aPace = aCtx.points_pace || 0
  if (Math.abs(hPace - aPace) > 0.12) {
    const better = hPace > aPace ? home.name : away.name
    const worse = hPace > aPace ? away.name : home.name
    reasons.push(`${better} is a fundamentally better team this season than ${worse}`)
  } else if (Math.abs(hPace - aPace) > 0.05) {
    const better = hPace > aPace ? home.abbreviation : away.abbreviation
    reasons.push(`${better} has a slight edge in overall team quality this season`)
  }

  if (wp.home > 0.55) {
    reasons.push(`${home.abbreviation} playing at home where they have a clear advantage this season`)
  }

  const strongPP = (rank) => rank != null && rank <= 10
  const weakPK   = (rank) => rank != null && rank >= 23
  if (f.home_pp != null && f.away_pk != null
      && strongPP(f.home_pp_rank) && weakPK(f.away_pk_rank)) {
    const h_pp = f.home_pp * 100
    const a_pk = f.away_pk * 100
    reasons.push(`${home.abbreviation}'s top-${Math.min(10, f.home_pp_rank)} power play (${h_pp.toFixed(1)}%) lines up against ${away.abbreviation}'s ${ordinal(f.away_pk_rank)}-ranked penalty kill (${a_pk.toFixed(1)}%)`)
  }
  if (f.away_pp != null && f.home_pk != null
      && strongPP(f.away_pp_rank) && weakPK(f.home_pk_rank)) {
    const a_pp = f.away_pp * 100
    const h_pk = f.home_pk * 100
    reasons.push(`${away.abbreviation}'s top-${Math.min(10, f.away_pp_rank)} power play (${a_pp.toFixed(1)}%) lines up against ${home.abbreviation}'s ${ordinal(f.home_pk_rank)}-ranked penalty kill (${h_pk.toFixed(1)}%)`)
  }

  if (f.home_sv_rank && f.home_sv_rank <= 5) {
    reasons.push(`${home.abbreviation} has one of the best goaltending units in the league (ranked ${ordinal(f.home_sv_rank)})`)
  } else if (f.home_sv_rank && f.home_sv_rank >= 28) {
    reasons.push(`${home.abbreviation}'s goaltending has been among the worst in the league this season`)
  }
  if (f.away_sv_rank && f.away_sv_rank <= 5) {
    reasons.push(`${away.abbreviation} has elite goaltending this season (ranked ${ordinal(f.away_sv_rank)})`)
  } else if (f.away_sv_rank && f.away_sv_rank >= 28) {
    reasons.push(`${away.abbreviation}'s goaltending has been a liability all season`)
  }

  const hL10 = hCtx.l10_pts_pct
  const aL10 = aCtx.l10_pts_pct
  if (hL10 != null && hL10 > 0.7) {
    reasons.push(`${home.abbreviation} is red hot, going ${hCtx.l10_record} in their last 10 games`)
  } else if (hL10 != null && hL10 < 0.35) {
    reasons.push(`${home.abbreviation} is ice cold, just ${hCtx.l10_record} in their last 10`)
  }
  if (aL10 != null && aL10 > 0.7) {
    reasons.push(`${away.abbreviation} is rolling with a ${aCtx.l10_record} record in their last 10 games`)
  } else if (aL10 != null && aL10 < 0.35) {
    reasons.push(`${away.abbreviation} has been struggling, going ${aCtx.l10_record} in their last 10`)
  }

  if (pred.rest?.home_b2b) {
    reasons.push(`${home.abbreviation} played last night, so tired legs tend to cost about half a goal`)
  }
  if (pred.rest?.away_b2b) {
    reasons.push(`${away.abbreviation} is on back-to-back nights, expect slower play and more mistakes`)
  }
  if (pred.rest?.home_rest_advantage && !pred.rest?.away_rest_advantage) {
    reasons.push(`${home.abbreviation} has had extra rest, giving them a fresh-legs advantage`)
  }
  if (pred.rest?.away_rest_advantage && !pred.rest?.home_rest_advantage) {
    reasons.push(`${away.abbreviation} has had extra rest, giving them a fresh-legs advantage`)
  }

  if (pred.injuries?.home_impact != null && pred.injuries.home_impact < 0.92) {
    const pct = Math.round((1 - pred.injuries.home_impact) * 100)
    reasons.push(`${home.abbreviation} is notably shorthanded (~${pct}% weaker from injuries)`)
  }
  if (pred.injuries?.away_impact != null && pred.injuries.away_impact < 0.92) {
    const pct = Math.round((1 - pred.injuries.away_impact) * 100)
    reasons.push(`${away.abbreviation} is notably shorthanded (~${pct}% weaker from injuries)`)
  }

  if (hCtx.fighting && aCtx.eliminated) {
    reasons.push(`${home.abbreviation} is fighting for their playoff life while ${away.abbreviation} has nothing to play for`)
  } else if (aCtx.fighting && hCtx.eliminated) {
    reasons.push(`${away.abbreviation} is desperate for points while ${home.abbreviation}'s season is already over`)
  } else if (hCtx.clinched && !aCtx.clinched && aCtx.fighting) {
    reasons.push(`${home.abbreviation} already clinched so they might not have the same urgency as ${away.abbreviation}`)
  } else if (aCtx.clinched && !hCtx.clinched && hCtx.fighting) {
    reasons.push(`${away.abbreviation} has their spot locked while ${home.abbreviation} needs this win more`)
  }

  if (f.home_shots_rank && f.away_shots_rank) {
    if (f.home_shots_rank <= 5 && f.away_shots_rank >= 25) {
      reasons.push(`${home.abbreviation} generates a ton of shots (${f.home_shots}/game) while ${away.abbreviation} gives up a lot, creating more scoring chances`)
    } else if (f.away_shots_rank <= 5 && f.home_shots_rank >= 25) {
      reasons.push(`${away.abbreviation} is an elite shot-generating team (${f.away_shots}/game) and will pepper the net tonight`)
    }
  }

  if (f.home_fo_rank && f.away_fo_rank) {
    if (f.home_fo_rank <= 5 && f.away_fo_rank >= 25) {
      reasons.push(`${home.abbreviation} dominates the faceoff circle (ranked ${ordinal(f.home_fo_rank)}) which means more puck possession`)
    } else if (f.away_fo_rank <= 5 && f.home_fo_rank >= 25) {
      reasons.push(`${away.abbreviation} wins faceoffs at an elite rate, giving them a possession edge`)
    }
  }

  if (pred.h2h && typeof pred.h2h === 'object' && pred.h2h.games >= 3) {
    const h2h = pred.h2h
    const homeWins = h2h.team1_wins || 0
    const awayWins = h2h.team2_wins || 0
    if (homeWins > awayWins + 2) {
      reasons.push(`${home.abbreviation} has owned this matchup recently, going ${homeWins}-${awayWins} in the last ${h2h.games} meetings`)
    } else if (awayWins > homeWins + 2) {
      reasons.push(`${away.abbreviation} has dominated this matchup lately, winning ${awayWins} of the last ${h2h.games} meetings`)
    }
  }

  const maxWp = Math.max(wp.home || 0, wp.away || 0)
  const fav = (wp.home || 0) > (wp.away || 0) ? home.abbreviation : away.abbreviation
  if (maxWp > 0.65) {
    reasons.push(`The model gives ${fav} a strong ${(maxWp * 100).toFixed(0)}% chance of winning this game`)
  }

  return reasons.slice(0, 5)
}


function findBestEdge(data, odds, home, away) {
  const candidates = []
  const wp = data.win_prob

  if (odds.home_ml && wp.home) {
    const e = (wp.home - mlToProb(odds.home_ml)) * 100
    if (e > 1.5) candidates.push({ label: `${home.abbreviation} ML`, odds: odds.home_ml, edge: e })
  }
  if (odds.away_ml && wp.away) {
    const e = (wp.away - mlToProb(odds.away_ml)) * 100
    if (e > 1.5) candidates.push({ label: `${away.abbreviation} ML`, odds: odds.away_ml, edge: e })
  }

  if (odds.over_under && data.over_under) {
    const vt = parseFloat(odds.over_under)
    const key = Object.keys(data.over_under).find(k => Math.abs(parseFloat(k) - vt) < 0.5)
    if (key) {
      const ou = data.over_under[key]
      const pickOver = ou.over > ou.under
      const prob = Math.max(ou.over, ou.under)
      const realOdds = pickOver ? odds.over_odds : odds.under_odds
      if (realOdds) {
        const e = (prob - mlToProb(realOdds)) * 100
        if (e > 1.5) candidates.push({ label: `${pickOver ? 'Over' : 'Under'} ${vt}`, odds: realOdds, edge: e })
      }
    }
  }

  if (data.puck_line && odds.home_spread_odds && odds.home_spread_point != null) {
    const pt = odds.home_spread_point
    const hProb = pt < 0
      ? data.puck_line.home_minus_1_5
      : (data.puck_line.home_plus_1_5 || 1 - data.puck_line.away_minus_1_5)
    const e = (hProb - mlToProb(odds.home_spread_odds)) * 100
    if (e > 1.5) {
      candidates.push({
        label: `${home.abbreviation} ${pt > 0 ? '+' : ''}${pt}`,
        odds: odds.home_spread_odds,
        edge: e,
      })
    }
  }
  if (data.puck_line && odds.away_spread_odds && odds.away_spread_point != null) {
    const pt = odds.away_spread_point
    const aProb = pt < 0
      ? (data.puck_line.away_minus_1_5 || 1 - data.puck_line.home_plus_1_5)
      : data.puck_line.away_plus_1_5
    const e = (aProb - mlToProb(odds.away_spread_odds)) * 100
    if (e > 1.5) {
      candidates.push({
        label: `${away.abbreviation} ${pt > 0 ? '+' : ''}${pt}`,
        odds: odds.away_spread_odds,
        edge: e,
      })
    }
  }

  if (candidates.length === 0) return null
  const best = candidates.sort((a, b) => b.edge - a.edge)[0]
  best.rating = best.edge > 8 ? 'strong' : best.edge > 4 ? 'moderate' : 'lean'
  return best
}


function edgeFromBackendPick(pick) {
  if (!pick) return null
  const type = pick.type || ''
  const label = type === 'ML' ? `${pick.pick} ML` : pick.pick
  return {
    label,
    odds: pick.odds,
    edge: pick.edge,
    rating: pick.confidence || 'lean',
  }
}


function pickFromEdge(edge, home, away) {
  if (!edge) return null
  const m = edge.label.match(/^([A-Z]{2,4})\s+ML$/)
  if (!m) return null
  const abbr = m[1]
  if (abbr !== home.abbreviation && abbr !== away.abbreviation) return null
  return { type: 'ML', pick: abbr, odds: edge.odds }
}
