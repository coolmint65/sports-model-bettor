/**
 * TrackerView — historical performance panel for the bet queue.
 *
 * Renders three sections stacked: a KPI header (profit / ROI / record /
 * volume), a per-cell breakdown ranked by dollar profit, and a recent
 * settled-picks stream. Each row uses MatchupCard so tracker rows have
 * the same sport-specific card language as the Today view.
 */
import { useState } from 'react'
import { cn } from '../../lib/utils'
import { humanizeBetType } from '../../lib/betType'
import { resolveTeamLogo } from '../../lib/teamLogo'
import { confidenceFor, CONF_META } from './confidence'


export default function TrackerView({ tracker, loading }) {
  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card/50 px-4 py-6 text-sm text-muted-foreground">
        Loading tracker…
      </div>
    )
  }
  if (!tracker || !tracker.summary?.n) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/40 px-6 py-12 text-center">
        <div className="text-sm font-semibold text-foreground">
          No settled queue picks yet.
        </div>
        <div className="mt-1 text-xs text-muted-foreground">
          Historical performance shows up here once picks in
          queue-eligible cells settle.
        </div>
      </div>
    )
  }
  const s = tracker.summary
  return (
    <div className="space-y-6">
      <SummaryHeader summary={s} />
      <CellBreakdown cells={tracker.by_cell || []} />
      <RecentPicks recent={tracker.recent || []} />
    </div>
  )
}


function SummaryHeader({ summary }) {
  const profitTone = summary.profit_dollars > 0 ? 'text-positive'
    : summary.profit_dollars < 0 ? 'text-negative' : 'text-foreground'
  const roiTone = summary.roi > 0 ? 'text-positive'
    : summary.roi < 0 ? 'text-negative' : 'text-foreground'
  return (
    <div className="rounded-lg border border-border bg-card/60 p-6">
      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <Stat
          label="Profit"
          value={`${summary.profit_dollars >= 0 ? '+' : ''}$${Math.round(summary.profit_dollars).toLocaleString()}`}
          tone={profitTone}
        />
        <Stat
          label="ROI"
          value={`${summary.roi >= 0 ? '+' : ''}${summary.roi.toFixed(1)}%`}
          tone={roiTone}
        />
        <Stat
          label="Record"
          value={`${summary.wins}–${summary.losses}${summary.pushes ? `–${summary.pushes}` : ''}`}
          hint={`${summary.wr.toFixed(1)}% WR`}
        />
        <Stat
          label="Volume"
          value={`${summary.n} picks`}
          hint={`$${Math.round(summary.staked_dollars).toLocaleString()} staked`}
        />
      </div>
    </div>
  )
}


function Stat({ label, value, tone, hint }) {
  return (
    <div>
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className={cn('mt-0.5 text-2xl font-bold tabular-nums',
                          tone || 'text-foreground')}>
        {value}
      </div>
      {hint && (
        <div className="text-[11px] text-muted-foreground tabular-nums">
          {hint}
        </div>
      )}
    </div>
  )
}


function CellBreakdown({ cells }) {
  if (!cells.length) return null
  return (
    <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
      <div className="border-b border-border px-4 py-3">
        <div className="text-sm font-semibold text-foreground">
          Which markets are earning
        </div>
        <div className="text-[11px] text-muted-foreground">
          Every queue-eligible cell, ranked by dollar profit.
        </div>
      </div>
      <ul className="divide-y divide-border/60">
        {cells.map((c, i) => {
          const tier = confidenceFor(c.roi)
          const meta = CONF_META[tier]
          const profitTone = c.profit_dollars > 0 ? 'text-positive'
            : 'text-negative'
          return (
            <li key={i} className={cn(
                  'flex items-center gap-3 px-4 py-3 border-l-4',
                  meta.accent,
                )}>
              <span className={cn(
                'inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
                meta.badge,
              )}>
                {meta.label}
              </span>
              <div className="flex-1 min-w-0">
                <div className="text-sm font-semibold text-foreground">
                  <span className="text-muted-foreground uppercase tracking-wider text-[10px] mr-1.5">
                    {c.sport}
                  </span>
                  {humanizeBetType(c.bet_type)}
                  {c.direction && (
                    <span className="text-muted-foreground">
                      {' '}| {c.direction}
                    </span>
                  )}
                </div>
                <div className="text-[11px] text-muted-foreground tabular-nums">
                  {c.n} picks · {c.wr.toFixed(1)}% WR · +{c.roi.toFixed(1)}% ROI
                </div>
              </div>
              <div className={cn('text-right text-sm font-semibold tabular-nums',
                                  profitTone)}>
                {c.profit_dollars > 0 ? '+' : ''}${Math.round(c.profit_dollars).toLocaleString()}
              </div>
            </li>
          )
        })}
      </ul>
    </div>
  )
}


function RecentPicks({ recent }) {
  if (!recent.length) return null
  return (
    <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
      <div className="border-b border-border px-4 py-3">
        <div className="text-sm font-semibold text-foreground">
          Recent settled picks
        </div>
        <div className="text-[11px] text-muted-foreground">
          Last {recent.length} queue-eligible picks to grade.
        </div>
      </div>
      <ul className="divide-y divide-border/60">
        {recent.map((r, i) => (
          <RecentRow key={i} pick={r} />
        ))}
      </ul>
    </div>
  )
}


function RecentRow({ pick }) {
  const oddsTxt = pick.odds >= 0 ? `+${pick.odds}` : `${pick.odds}`
  const label = pick.display_label || pick.sport
  const resultMeta = {
    W: { txt: 'W', tone: 'bg-positive/20 text-positive border-positive/40' },
    L: { txt: 'L', tone: 'bg-negative/20 text-negative border-negative/40' },
    P: { txt: 'P', tone: 'bg-muted text-muted-foreground border-border' },
  }[pick.result] || { txt: '?', tone: 'bg-muted text-muted-foreground border-border' }
  const profitTone = pick.profit_dollars > 0 ? 'text-positive'
    : pick.profit_dollars < 0 ? 'text-negative' : 'text-muted-foreground'
  return (
    <li className="flex items-center gap-3 px-4 py-2.5">
      <span className={cn(
        'inline-flex h-6 w-6 items-center justify-center rounded-full border text-[10px] font-bold flex-shrink-0',
        resultMeta.tone,
      )}>
        {resultMeta.txt}
      </span>
      <div className="text-[11px] text-muted-foreground tabular-nums w-16 flex-shrink-0">
        {pick.date?.slice(5) || '—'}
      </div>
      <span className="inline-flex rounded-full border border-border bg-background/60 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground flex-shrink-0">
        {label}
      </span>
      <div className="min-w-0 flex-1">
        <RecentMatchupSnippet pick={pick} />
        <div className="text-sm font-medium text-foreground truncate">
          {pick.pick}{' '}
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
            {humanizeBetType(pick.bet_type)}
          </span>
        </div>
      </div>
      <div className="text-right text-xs tabular-nums text-muted-foreground flex-shrink-0">
        {oddsTxt}
      </div>
      <div className={cn('text-right text-sm font-semibold tabular-nums w-20 flex-shrink-0',
                          profitTone)}>
        {pick.profit_dollars > 0 ? '+' : ''}${Math.round(pick.profit_dollars)}
      </div>
    </li>
  )
}


// Recent-picks stream lives inside a scrolling list so the row stays
// small — a slim inline snippet (matchup text + team abbrev / player
// name) matches the tracker's compact rhythm.
function RecentMatchupSnippet({ pick }) {
  const card = pick.card || {}
  if (card.away_abbr && card.home_abbr) {
    return (
      <div className="flex items-center gap-2 text-[11px]">
        <TeamMini sport={pick.sport} abbr={card.away_abbr} />
        <span className="text-muted-foreground/60">@</span>
        <TeamMini sport={pick.sport} abbr={card.home_abbr} />
      </div>
    )
  }
  if (card.p1_name && card.p2_name) {
    return (
      <div className="flex items-center gap-1.5 text-[11px] text-foreground truncate"
           title={pick.matchup}>
        <span className="font-medium truncate max-w-[130px]">{card.p1_name}</span>
        <span className="text-muted-foreground/60">vs</span>
        <span className="font-medium truncate max-w-[130px]">{card.p2_name}</span>
      </div>
    )
  }
  return (
    <div className="text-[11px] text-muted-foreground truncate" title={pick.matchup}>
      {pick.matchup || '—'}
    </div>
  )
}


function TeamMini({ sport, abbr }) {
  const logo = resolveTeamLogo(sport, abbr, null)
  const [err, setErr] = useState(false)
  return (
    <span className="inline-flex items-center gap-1">
      {logo && !err ? (
        <img
          src={logo}
          alt=""
          onError={() => setErr(true)}
          className="h-5 w-5 rounded-full object-contain bg-foreground/[0.06] ring-1 ring-border p-0.5 flex-shrink-0"
        />
      ) : (
        <span className="h-5 w-5 rounded-full bg-primary/15 ring-1 ring-border flex items-center justify-center text-[8px] font-bold text-primary flex-shrink-0">
          {(abbr || '?').slice(0, 3).toUpperCase()}
        </span>
      )}
      <span className="font-bold text-foreground">{abbr}</span>
    </span>
  )
}
