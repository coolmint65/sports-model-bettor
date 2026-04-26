import EdgeCallout from './gameDetail/EdgeCallout'
import ModelSignals from './gameDetail/ModelSignals'
import UnderdogNote from './gameDetail/UnderdogNote'
import WhyThisPick from './gameDetail/WhyThisPick'
import WinProbBar from './primitives/WinProbBar'
import StatRow from './primitives/StatRow'
import SectionCard from './primitives/SectionCard'
import ScoreDisplay from './primitives/ScoreDisplay'
import { ProbRow, ProbRowHeader, ProbBox } from './primitives/ProbBar'
import { KeyStatsGrid, KeyStat } from './primitives/KeyStatsGrid'
import { cn } from '../lib/utils'

/**
 * PredictionResults — main column of the MLB GameDetail page.
 *
 * Phase 2-cleanup restyle: every section now composes Tailwind-tokened
 * primitives (SectionCard, ScoreDisplay, ProbRow, ProbBox, KeyStat,
 * StatRow) instead of legacy CSS classes. Result-cards, ou-rows,
 * nrfi-displays, key-stats grids — all gone.
 */
export default function PredictionResults({ data, odds }) {
  const d = data
  const home = d.home
  const away = d.away
  const es = d.expected_score
  const wp = d.win_prob
  const homeWins = es.home > es.away
  const pct = n => `${(n * 100).toFixed(1)}%`

  const bestEdge = getBestEdge(d)

  return (
    <div className="space-y-4">
      {/* Season Context Banner */}
      {d.season_context && d.season_context.implications && (
        <SeasonContextBanner ctx={d.season_context} />
      )}

      {/* Projected Outcome */}
      <SectionCard title="Projected Outcome">
        <ScoreDisplay
          home={home}
          away={away}
          homeScore={Math.round(es.home)}
          awayScore={Math.round(es.away)}
          homeWins={homeWins}
        />
        <div className="mt-4">
          <WinProbBar wp={wp} home={home} away={away} variant="detail" />
        </div>
        <div className="mt-4 space-y-2">
          <StatRow label="Total" value={d.total.toFixed(1)} />
          <StatRow
            label="Spread"
            value={`${homeWins ? home.abbreviation : away.abbreviation} ${Math.abs(d.spread).toFixed(1)}`}
          />
          {d.park_factor && d.park_factor !== 1.0 && (
            <StatRow
              label="Park"
              value={d.park_factor > 1.03 ? 'Hitter' : d.park_factor < 0.97 ? 'Pitcher' : 'Neutral'}
              valueClassName={d.park_factor > 1.03 ? 'positive' : d.park_factor < 0.97 ? 'negative' : ''}
            />
          )}
        </div>
        <div className="mt-4 space-y-2">
          <EdgeCallout edge={bestEdge} />
          <UnderdogNote pick={d.best_pick} wp={wp} home={home} away={away} />
        </div>
      </SectionCard>

      <WhyThisPick pred={d} pick={d.best_pick} home={home} away={away} />

      {/* Pitching Matchup */}
      {(home.pitcher || away.pitcher) && (
        <SectionCard title="Starting Pitchers">
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
            {[
              { label: home.abbreviation, p: home.pitcher },
              { label: away.abbreviation, p: away.pitcher },
            ].map(({ label, p }) => p && (
              <PitcherCard key={label} label={label} pitcher={p} />
            ))}
          </div>
        </SectionCard>
      )}

      {/* Confirmed Lineups */}
      {d.lineups && (d.lineups.home?.length > 0 || d.lineups.away?.length > 0) && (
        <SectionCard
          title="Confirmed Lineups"
          subtitle="Lineups post ~2 hours before first pitch."
        >
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2">
            <LineupColumn abbr={home.abbreviation} lineup={d.lineups.home} />
            <LineupColumn abbr={away.abbreviation} lineup={d.lineups.away} />
          </div>
        </SectionCard>
      )}

      {/* Weather */}
      {d.weather && (d.weather.temp_f != null || d.weather.wind_mph != null) && (
        <SectionCard
          title="Weather"
          rightSlot={
            <span className="text-[11px] text-muted-foreground">
              {d.weather.applied ? 'Factor active' : 'Factor gated off'}
            </span>
          }
        >
          <KeyStatsGrid cols="auto">
            {d.weather.temp_f != null && (
              <KeyStat label="Temp" value={`${Math.round(d.weather.temp_f)}°F`} />
            )}
            <WindStat wind={d.weather.wind_mph} />
            {d.weather.adjustment != null && d.weather.adjustment !== 1 && (
              <KeyStat
                label="Run impact"
                value={`${d.weather.adjustment > 1 ? '+' : ''}${((d.weather.adjustment - 1) * 100).toFixed(1)}%`}
                valueClassName={
                  d.weather.adjustment > 1.02 ? 'positive' :
                  d.weather.adjustment < 0.98 ? 'negative' : ''
                }
              />
            )}
          </KeyStatsGrid>
        </SectionCard>
      )}

      {/* HP Umpire */}
      {d.umpire && d.umpire.name && (
        <SectionCard
          title="HP Umpire"
          rightSlot={
            <span className="text-[11px] text-muted-foreground">
              {d.umpire.factor != null && Math.abs(d.umpire.factor - 1) > 1e-4
                ? d.umpire.factor > 1 ? 'Lean: hitter-friendly' : 'Lean: pitcher-friendly'
                : 'No historical lean'}
            </span>
          }
        >
          <KeyStatsGrid cols="auto">
            <KeyStat label="Umpire" value={d.umpire.name} />
            {d.umpire.factor != null && (
              <KeyStat
                label="Run factor"
                value={`${d.umpire.factor > 1 ? '+' : ''}${((d.umpire.factor - 1) * 100).toFixed(1)}%`}
                valueClassName={
                  d.umpire.factor > 1.01 ? 'positive' :
                  d.umpire.factor < 0.99 ? 'negative' : ''
                }
              />
            )}
          </KeyStatsGrid>
        </SectionCard>
      )}

      {/* Betting Lines */}
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

        {d.run_line && (
          <div className="mt-5">
            <SectionLabel
              right={d.run_line.model_spread != null && (
                <span className="text-[11px] text-muted-foreground tabular-nums">
                  Model spread: {home.abbreviation}{' '}
                  {d.run_line.model_spread > 0 ? '-' : '+'}{Math.abs(d.run_line.model_spread).toFixed(1)}
                </span>
              )}
            >
              Run Line
            </SectionLabel>
            <ProbRowHeader leftLabel={home.abbreviation} rightLabel={away.abbreviation} />
            {d.run_line.spreads ? (
              Object.entries(d.run_line.spreads).map(([spread, probs]) => {
                const s = parseFloat(spread)
                const homeLabel = s > 0
                  ? `${home.abbreviation} -${s.toFixed(1)}`
                  : `${home.abbreviation} +${Math.abs(s).toFixed(1)}`
                return (
                  <ProbRow key={spread} line={homeLabel.split(' ').slice(-1)[0]}
                    leftProb={probs.home_cover} rightProb={probs.away_cover} />
                )
              })
            ) : (
              <>
                <ProbRow line="-1.5"
                  leftProb={d.run_line.home_minus_1_5}
                  rightProb={1 - d.run_line.home_minus_1_5} />
                <ProbRow line="+1.5"
                  leftProb={d.run_line.away_plus_1_5}
                  rightProb={1 - d.run_line.away_plus_1_5} />
              </>
            )}
          </div>
        )}

        {d.f5 && (
          <div className="mt-5">
            <SectionLabel>First 5 Innings</SectionLabel>
            <div className="space-y-2">
              <StatRow label="F5 Total" value={d.f5.total} />
              <StatRow
                label="F5 Winner"
                value={`${d.f5.win_prob.home > d.f5.win_prob.away ? home.abbreviation : away.abbreviation} ${pct(Math.max(d.f5.win_prob.home, d.f5.win_prob.away))}`}
              />
            </div>
          </div>
        )}
      </SectionCard>

      {/* First Inning / NRFI */}
      {d.first_inning && (
        <SectionCard title="First Inning">
          <div className="flex gap-3">
            <ProbBox
              label="NRFI"
              value={pct(d.first_inning.nrfi)}
              sub="No Run First Inning"
              favored={d.first_inning.nrfi > 0.55}
            />
            <ProbBox
              label="YRFI"
              value={pct(d.first_inning.yrfi)}
              sub="Yes Run First Inning"
              favored={d.first_inning.yrfi > 0.55}
            />
          </div>
          <div className="mt-3 space-y-2">
            <StatRow label={`${away.abbreviation} scores in 1st`} value={pct(d.first_inning.away_scores_1st)} />
            <StatRow label={`${home.abbreviation} scores in 1st`} value={pct(d.first_inning.home_scores_1st)} />
          </div>
        </SectionCard>
      )}

      {/* H2H */}
      {d.h2h_history && d.h2h_history.games > 0 && (
        <SectionCard title={`Head to Head (${d.h2h_history.seasons_covered || 3}yr)`}>
          <KeyStatsGrid cols={4}>
            <KeyStat label={`${home.abbreviation} Record`} value={`${d.h2h_history.a_wins}-${d.h2h_history.b_wins}`} />
            <KeyStat label={`${home.abbreviation} R/G`} value={d.h2h_history.a_runs_pg} />
            <KeyStat label={`${away.abbreviation} R/G`} value={d.h2h_history.b_runs_pg} />
            <KeyStat label="Games" value={d.h2h_history.games} />
          </KeyStatsGrid>
          {d.h2h_history.recent && d.h2h_history.recent.length > 0 && (
            <div className="mt-4">
              <SectionLabel>Recent Meetings</SectionLabel>
              <div className="space-y-2">
                {d.h2h_history.recent.slice(0, 5).map((g, i) => (
                  <StatRow key={i} label={g.date}
                    value={`${home.abbreviation} ${g.a_score} - ${g.b_score} ${away.abbreviation}`}
                    valueClassName={g.a_won ? 'positive' : 'negative'}
                  />
                ))}
              </div>
            </div>
          )}
        </SectionCard>
      )}

      {/* Injuries */}
      {d.injuries && (d.injuries.home?.length > 0 || d.injuries.away?.length > 0) && (
        <SectionCard title="Injuries">
          {((d.injuries.home_impact != null && d.injuries.home_impact < 1)
            || (d.injuries.away_impact != null && d.injuries.away_impact < 1)) && (
            <div className="flex flex-wrap justify-center gap-4 text-sm mb-3">
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

      <ModelSignals pred={d} sport="mlb" home={home} away={away} />

      {/* Key Factors */}
      <KeyFactorsCard data={d} home={home} away={away} />
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


function SectionLabel({ children, right }) {
  return (
    <div className="mb-2 flex items-center justify-between">
      <h4 className="text-[11px] font-semibold uppercase tracking-widest text-muted-foreground">
        {children}
      </h4>
      {right && <div>{right}</div>}
    </div>
  )
}


function PitcherCard({ label, pitcher: p }) {
  return (
    <div className="rounded-md border border-border bg-background/40 px-4 py-3">
      <div className="flex items-center gap-2">
        <span className="text-xs font-bold text-muted-foreground tabular-nums">{label}</span>
        <span className="text-sm font-bold text-foreground">{p.name}</span>
        {p.throws && (
          <span className="text-[10px] text-muted-foreground">({p.throws}HP)</span>
        )}
      </div>
      {p.era != null && (
        <div className="mt-2 grid grid-cols-3 gap-x-3 gap-y-1.5">
          <PitcherStat label="W-L" value={p.record || '-'} />
          <PitcherStat label="ERA" value={p.era?.toFixed(2) || '-'} />
          <PitcherStat label="FIP" value={p.fip?.toFixed(2) || '-'} />
          <PitcherStat label="WHIP" value={p.whip?.toFixed(2) || '-'} />
          <PitcherStat label="K/9" value={p.k_per_9?.toFixed(1) || '-'} />
          <PitcherStat label="IP" value={p.innings?.toFixed(1) || '-'} />
        </div>
      )}
    </div>
  )
}

function PitcherStat({ label, value }) {
  return (
    <div className="text-center">
      <div className="text-sm font-bold tabular-nums text-foreground">{value}</div>
      <div className="text-[9px] uppercase tracking-wider text-muted-foreground">{label}</div>
    </div>
  )
}


function LineupColumn({ abbr, lineup }) {
  if (!lineup || lineup.length === 0) {
    return (
      <div>
        <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
          {abbr}
        </div>
        <div className="text-xs text-muted-foreground">Lineup not posted yet</div>
      </div>
    )
  }
  return (
    <div>
      <div className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
        {abbr}
      </div>
      <ol className="space-y-1">
        {lineup.map((b, i) => (
          <li key={i} className="flex items-baseline gap-2 text-xs">
            <span className="w-5 font-semibold tabular-nums text-muted-foreground">{i + 1}.</span>
            <span className="font-semibold text-foreground">
              {b.name || b.full_name || b.player || '?'}
            </span>
            {b.position && (
              <span className="text-[10px] text-muted-foreground">{b.position}</span>
            )}
            {b.bats && (
              <span className="ml-auto text-[10px] text-muted-foreground tabular-nums">{b.bats}</span>
            )}
          </li>
        ))}
      </ol>
    </div>
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


function KeyFactorsCard({ data: d, home, away }) {
  const stats = []
  const push = (label, hv, av, hvFmt, avFmt, higherBetter = true) => {
    if (hv == null || av == null) return
    const hBetter = higherBetter ? hv >= av : hv <= av
    stats.push({
      label, hvFmt: hvFmt ?? String(hv), avFmt: avFmt ?? String(av), hBetter,
    })
  }
  const hF = d.factors || {}
  if (hF.home_wrc_plus != null || d.home?.wrc_plus != null) {
    push('wRC+',
      d.home?.wrc_plus ?? hF.home_wrc_plus,
      d.away?.wrc_plus ?? hF.away_wrc_plus,
      null, null, true)
  }
  if (d.home?.ops != null || d.away?.ops != null) {
    push('OPS',
      d.home?.ops, d.away?.ops,
      d.home?.ops?.toFixed(3), d.away?.ops?.toFixed(3), true)
  }
  const hPit = d.home?.pitcher || {}
  const aPit = d.away?.pitcher || {}
  if (hPit.era != null && aPit.era != null) {
    push('SP ERA', hPit.era, aPit.era, hPit.era.toFixed(2), aPit.era.toFixed(2), false)
  }
  if (hPit.whip != null && aPit.whip != null) {
    push('SP WHIP', hPit.whip, aPit.whip, hPit.whip.toFixed(2), aPit.whip.toFixed(2), false)
  }

  if (stats.length === 0) return null

  return (
    <SectionCard title="Key Factors" bodyClassName="px-0 py-0">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/40">
            <th className="px-5 py-2.5 text-left text-[10px] font-semibold uppercase tracking-wider text-muted-foreground"></th>
            <th className="px-3 py-2.5 text-right text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {home.abbreviation}
            </th>
            <th className="px-3 py-2.5 text-right text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {away.abbreviation}
            </th>
          </tr>
        </thead>
        <tbody>
          {stats.map((s, i) => (
            <tr key={i} className="border-b border-border/60 last:border-0 hover:bg-accent/20 transition-colors">
              <td className="px-5 py-2.5 text-muted-foreground font-semibold">{s.label}</td>
              <td className={cn(
                'px-3 py-2.5 text-right font-bold tabular-nums',
                s.hBetter ? 'text-positive' : 'text-foreground/85',
              )}>
                {s.hvFmt}
              </td>
              <td className={cn(
                'px-3 py-2.5 text-right font-bold tabular-nums',
                !s.hBetter ? 'text-positive' : 'text-foreground/85',
              )}>
                {s.avFmt}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </SectionCard>
  )
}


function WindStat({ wind: w }) {
  if (w == null) return null
  if (typeof w === 'number' && isFinite(w)) {
    return <KeyStat label="Wind" value={`${Math.round(w)} mph`} />
  }
  const s = String(w).trim()
  if (!s || s.toLowerCase() === 'nan') return null
  const numMatch = s.match(/(-?\d+(?:\.\d+)?)/)
  if (numMatch) {
    const dir = s.slice(0, numMatch.index).trim()
    return (
      <KeyStat
        label="Wind"
        value={`${dir ? `${dir} ` : ''}${Math.round(parseFloat(numMatch[1]))} mph`}
      />
    )
  }
  return <KeyStat label="Wind" value={s} />
}


function getBestEdge(data) {
  const bp = data?.best_pick
  if (!bp) return null
  if (bp.confidence === 'skip') return null
  return {
    label: _formatBestPickLabel(bp),
    odds: bp.odds,
    edge: bp.edge,
    rating: bp.confidence,
  }
}


function _formatBestPickLabel(pick) {
  switch (pick.type) {
    case 'ML':    return `${pick.pick} ML`
    case 'F5 ML': return `${pick.pick} F5 ML`
    default:      return pick.pick
  }
}
