/**
 * RaceHeroCard — big race header card + featured-pick hero.
 *
 * Matches the visual language of PickOfDayHero + team-sport GameCards
 * so the motorsports surface reads as the same product as the other
 * sports panels. Two stacked hero blocks:
 *   1. Race hero — big country flag, circuit image, race time
 *      countdown, round number.
 *   2. Featured pick — mint-tinted callout showing the model's top
 *      edge pick (driver + team + odds + edge + market).
 */
import { flagForCountry, flagForNationality, teamColor } from '../../lib/f1Flags'
import { fmtOdds, fmtRaceTime } from './cells'
import { cn } from '../../lib/utils'


export function RaceHero({ race, slate }) {
  const daysUntil = slate?.days_until_race
  const untilLabel = daysUntil === 0
    ? 'Race day'
    : daysUntil === 1
      ? 'Tomorrow'
      : daysUntil > 1
        ? `In ${daysUntil} days`
        : 'Race concluded'
  return (
    <section className="relative overflow-hidden rounded-2xl border border-primary/25
                        bg-gradient-to-br from-card via-card to-primary/[0.05]">
      <div
        aria-hidden="true"
        className="absolute -right-24 -top-24 h-72 w-72 rounded-full bg-primary/10 blur-3xl"
      />
      {race.circuit_image_url && (
        <div
          aria-hidden="true"
          className="absolute inset-0 opacity-[0.08] bg-cover bg-center"
          style={{ backgroundImage: `url(${race.circuit_image_url})` }}
        />
      )}

      <div className="relative flex flex-col lg:flex-row gap-6 p-6">
        <div className="flex-1 min-w-0 space-y-2">
          <div className="flex items-center gap-2 text-[10px] font-bold uppercase tracking-[0.18em] text-primary">
            Round {race.round}
            {slate?.date && (
              <>
                <span className="text-primary/40">·</span>
                <span>{slate.date}</span>
              </>
            )}
            {daysUntil != null && (
              <>
                <span className="text-primary/40">·</span>
                <span className={cn(
                  daysUntil === 0 && 'text-positive font-bold',
                )}>
                  {untilLabel}
                </span>
              </>
            )}
          </div>

          <div className="flex items-baseline gap-3 flex-wrap">
            {race.country && (
              <span className="text-3xl leading-none" aria-label={race.country}>
                {flagForCountry(race.country)}
              </span>
            )}
            <h2 className="text-3xl font-bold tracking-tight text-foreground sm:text-4xl">
              {race.name}
            </h2>
          </div>

          <div className="text-sm text-muted-foreground">
            {race.circuit}
            {race.country ? ` · ${race.country}` : ''}
          </div>

          {race.info && (
            <div className="mt-3 flex items-center gap-4 text-xs text-muted-foreground">
              {race.info.length_km && (
                <span>
                  <span className="font-semibold text-foreground">{race.info.length_km} km</span>
                  {' '}per lap
                </span>
              )}
              {race.info.laps && (
                <span>
                  <span className="font-semibold text-foreground">{race.info.laps}</span>
                  {' '}laps
                </span>
              )}
              {race.info.turns && (
                <span>
                  <span className="font-semibold text-foreground">{race.info.turns}</span>
                  {' '}turns
                </span>
              )}
            </div>
          )}
        </div>

        <div className="lg:text-right lg:min-w-[200px] flex flex-col justify-center gap-2">
          <div>
            <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
              Green flag
            </div>
            <div className="text-lg font-semibold text-foreground mt-1">
              {fmtRaceTime(race.race_time, race.race_date)}
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}


/**
 * FeaturedPick — mint-tinted callout for the model's top pick.
 * Renders the highest-edge pick across WINNER + PODIUM markets.
 */
export function FeaturedPick({ drivers }) {
  if (!drivers?.length) return null
  const candidates = []
  for (const d of drivers) {
    if (d.is_winner_pick && d.winner_edge != null) {
      candidates.push({ driver: d, market: 'WINNER', edge: d.winner_edge,
                         odds: d.winner_odds, prob: d.p_win })
    }
    if (d.is_podium_pick && d.podium_edge != null) {
      candidates.push({ driver: d, market: 'PODIUM', edge: d.podium_edge,
                         odds: d.podium_odds, prob: d.p_podium })
    }
  }
  if (!candidates.length) return null
  const best = candidates.sort((a, b) => b.edge - a.edge)[0]
  const { driver: d, market, edge, odds, prob } = best

  return (
    <section className="relative overflow-hidden rounded-2xl border border-positive/30
                        bg-gradient-to-br from-card via-card to-positive/[0.04]">
      <div
        aria-hidden="true"
        className="absolute -right-20 -top-20 h-64 w-64 rounded-full bg-positive/10 blur-3xl"
      />
      <div
        className="absolute inset-y-0 left-0 w-1"
        style={{ backgroundColor: teamColor(d.team) }}
        aria-hidden="true"
      />

      <div className="relative grid grid-cols-1 lg:grid-cols-[1fr_auto] gap-6 p-6">
        <div className="min-w-0 space-y-3">
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-positive">
              Pick of the Race
            </span>
            <span className="rounded-full bg-positive/15 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider text-positive">
              {market === 'WINNER' ? 'Winner' : 'Podium'}
            </span>
          </div>

          <div className="flex items-baseline gap-3 flex-wrap">
            <h2 className="text-3xl font-bold tracking-tight text-foreground sm:text-4xl">
              {d.name}
            </h2>
            {d.nationality && (
              <span className="text-2xl leading-none" aria-label={d.nationality}>
                {flagForNationality(d.nationality)}
              </span>
            )}
          </div>

          <div className="flex items-center gap-3 flex-wrap text-sm">
            <span className="inline-flex items-center gap-1.5">
              <span
                className="inline-block w-2.5 h-2.5 rounded-full"
                style={{ backgroundColor: teamColor(d.team) }}
                aria-hidden="true"
              />
              <span className="text-foreground font-semibold">{d.team}</span>
            </span>
            {d.grid_pos != null && (
              <>
                <span className="text-muted-foreground/60">·</span>
                <span className="text-muted-foreground">
                  Grid <span className="text-foreground font-semibold">P{d.grid_pos}</span>
                </span>
              </>
            )}
            <span className="text-muted-foreground/60">·</span>
            <span className="text-muted-foreground">
              Odds <span className="text-foreground font-semibold tabular-nums">{fmtOdds(odds)}</span>
            </span>
          </div>
        </div>

        <div className="flex flex-row gap-6 items-center lg:flex-col lg:items-end lg:justify-center lg:text-right">
          <div>
            <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
              Model
            </div>
            <div className="text-2xl font-bold tabular-nums text-foreground mt-0.5">
              {prob != null ? `${(prob * 100).toFixed(1)}%` : '—'}
            </div>
          </div>
          <div className="border-l border-border pl-6 lg:border-l-0 lg:border-t lg:pl-0 lg:pt-4 lg:mt-2 lg:w-full">
            <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
              Edge
            </div>
            <div className="text-5xl font-black text-positive tabular-nums leading-none mt-1">
              +{(edge * 100).toFixed(1)}
              <span className="text-2xl">%</span>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}


/**
 * PicksGrid — 3 compact pick cards (WINNER + top-2 PODIUM) beneath the
 * featured hero. Each card shows the driver, team color bar, odds, and
 * edge in a tight callout. Mirrors the "top edges" horizontal strip
 * other sports use.
 */
export function PicksGrid({ drivers }) {
  if (!drivers?.length) return null
  const winner = drivers.find(d => d.is_winner_pick)
  const podiums = drivers.filter(d => d.is_podium_pick)
  const cards = [winner, ...podiums].filter(Boolean)
  if (!cards.length) return null

  return (
    <section>
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
        Race Picks
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
        {cards.map((d, i) => {
          const isWin = d.is_winner_pick && i === 0
          const market = isWin ? 'WINNER' : 'PODIUM'
          const edge = isWin ? d.winner_edge : d.podium_edge
          const odds = isWin ? d.winner_odds : d.podium_odds
          const prob = isWin ? d.p_win : d.p_podium
          return (
            <PickCard
              key={`${d.driver_id}-${market}`}
              driver={d} market={market}
              edge={edge} odds={odds} prob={prob}
            />
          )
        })}
      </div>
    </section>
  )
}


function PickCard({ driver: d, market, edge, odds, prob }) {
  const isWin = market === 'WINNER'
  return (
    <div
      className={cn(
        'relative overflow-hidden rounded-lg border bg-card/60 p-3',
        'border-l-4 hover:bg-accent/30 transition-colors',
      )}
      style={{ borderLeftColor: teamColor(d.team) }}
    >
      <div className="flex items-center justify-between gap-2">
        <span className={cn(
          'inline-flex rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider',
          isWin
            ? 'bg-positive/15 text-positive'
            : 'bg-primary/15 text-primary',
        )}>
          {isWin ? 'Winner' : 'Podium'}
        </span>
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground tabular-nums">
          {fmtOdds(odds)}
        </span>
      </div>

      <div className="mt-2 flex items-baseline gap-2">
        <span className="text-lg font-bold text-foreground truncate">
          {d.name}
        </span>
        {d.nationality && (
          <span className="text-base leading-none" aria-label={d.nationality}>
            {flagForNationality(d.nationality)}
          </span>
        )}
      </div>

      <div className="mt-0.5 text-[11px] text-muted-foreground truncate">
        {d.team}
        {d.grid_pos != null && ` · Grid P${d.grid_pos}`}
      </div>

      <div className="mt-2 flex items-baseline justify-between border-t border-border/60 pt-2">
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Edge
        </span>
        <span className={cn(
          'text-lg font-bold tabular-nums',
          edge > 0 ? 'text-positive' : 'text-muted-foreground',
        )}>
          {edge > 0 ? '+' : ''}{(edge * 100).toFixed(1)}%
        </span>
      </div>

      {prob != null && (
        <div className="mt-1 flex items-baseline justify-between text-[10px]">
          <span className="uppercase tracking-wider text-muted-foreground">Model</span>
          <span className="font-semibold tabular-nums text-foreground/85">
            {(prob * 100).toFixed(1)}%
          </span>
        </div>
      )}
    </div>
  )
}
