/**
 * PickCard — sport-native card dispatcher for the bet queue.
 *
 * When the queue has the sport's own scheduled-endpoint payload for
 * this pick (fetched via useSportData), we render that sport's actual
 * card component — the same one the sport's own panel uses. When the
 * payload isn't available yet (or the sport doesn't have an integrated
 * card fetcher), we fall back to a slim row so the pick still shows.
 *
 * Right column (odds / stake / copy) is a shared wrapper so the queue
 * itself stays scannable regardless of which card style each row uses.
 */
import { useState } from 'react'
import { cn } from '../../lib/utils'
import { humanizeBetType } from '../../lib/betType'
import { resolveTeamLogo } from '../../lib/teamLogo'
import MatchCard from '../tennis/MatchCard'
import { confidenceFor, CONF_META } from './confidence'


// Sports we have a full sport-native card wired up for. Everything
// else falls through to the slim row layout.
const NATIVE_CARD_SPORTS = new Set(['tennis'])


export default function PickCard({ pick, native, compact = false }) {
  const tier = confidenceFor(pick.cell_roi)
  const meta = CONF_META[tier]

  // Native tennis card — full MatchCard from TennisPanel, wrapped
  // with the queue's confidence badges + copy affordances.
  if (native && pick.sport === 'tennis') {
    return <TennisQueueCard pick={pick} match={native} meta={meta} />
  }

  // Fallback: slim row with sport-specific matchup snippet.
  return <SlimQueueRow pick={pick} meta={meta} compact={compact} />
}


// ── Copy button + odds/stake bar (shared) ───────────────────

function StakeStrip({ pick, meta }) {
  const [copied, setCopied] = useState(false)
  const oddsTxt = pick.odds >= 0 ? `+${pick.odds}` : `${pick.odds}`
  const label = pick.display_label || pick.sport
  const copy = () => {
    const txt = [
      label,
      pick.matchup,
      `${humanizeBetType(pick.bet_type)}: ${pick.pick} @ ${oddsTxt}`,
      `${pick.stake_units.toFixed(2)}u ($${pick.stake_dollars.toFixed(0)})`,
    ].join(' · ')
    navigator.clipboard?.writeText(txt)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }
  return (
    <div className="flex items-center gap-3 justify-between rounded-md border border-border/60 bg-background/40 px-3 py-2 mt-2">
      <div className="flex items-center gap-2">
        <span className={cn(
          'inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
          meta.badge,
        )}>
          {meta.label}
        </span>
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Queue pick
        </span>
      </div>
      <div className="flex items-center gap-4">
        <div className="text-right">
          <div className="text-sm font-semibold text-foreground tabular-nums">
            {oddsTxt}
          </div>
          <div className="text-[9px] uppercase tracking-wider text-muted-foreground">odds</div>
        </div>
        <div className="text-right">
          <div className="text-sm font-semibold text-foreground tabular-nums">
            {pick.stake_units.toFixed(2)}u
          </div>
          <div className="text-[9px] uppercase tracking-wider text-muted-foreground tabular-nums">
            ${pick.stake_dollars.toFixed(0)}
          </div>
        </div>
        <button
          onClick={copy}
          className={cn(
            'rounded-md border px-3 py-2 text-[11px] font-semibold uppercase tracking-wider transition-colors',
            copied
              ? 'border-positive text-positive bg-positive/10'
              : 'border-border text-muted-foreground hover:border-foreground hover:text-foreground hover:bg-accent/40',
          )}
          title="Copy pick details to clipboard"
        >
          {copied ? '✓ Copied' : 'Copy'}
        </button>
      </div>
    </div>
  )
}


// ── Tennis (native MatchCard) ────────────────────────────────

function TennisQueueCard({ pick, match, meta }) {
  // Fill the grid cell top-to-bottom so a mix of long/short cards
  // (different odds grid rows, insight vs no insight) still line up.
  return (
    <div className={cn(
      'flex flex-col h-full rounded-xl border-l-4 border-border bg-transparent',
      meta.accent,
    )}>
      <div className="flex-1">
        {/* Full tennis MatchCard — identical to TennisPanel. */}
        <MatchCard match={match} />
      </div>
      {/* Queue-specific stake + copy strip pinned to the bottom. */}
      <div className="px-4 pb-3">
        <StakeStrip pick={pick} meta={meta} />
      </div>
    </div>
  )
}


// ── Slim fallback (motorsports / non-integrated sports) ─────

function SlimQueueRow({ pick, meta, compact }) {
  const [copied, setCopied] = useState(false)
  const oddsTxt = pick.odds >= 0 ? `+${pick.odds}` : `${pick.odds}`
  const label = pick.display_label || pick.sport
  const copy = () => {
    const txt = [
      label,
      pick.matchup,
      `${humanizeBetType(pick.bet_type)}: ${pick.pick} @ ${oddsTxt}`,
      `${pick.stake_units.toFixed(2)}u ($${pick.stake_dollars.toFixed(0)})`,
    ].join(' · ')
    navigator.clipboard?.writeText(txt)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }
  return (
    <div className={cn(
      'flex flex-col gap-3 rounded-lg border border-border bg-card/60 p-4 h-full',
      'border-l-4',
      meta.accent,
    )}>
      <div className="flex items-center gap-2 flex-shrink-0">
        <span className="inline-flex rounded-full border border-border bg-background/60 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          {label}
        </span>
        <span className={cn(
          'inline-flex rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wider',
          meta.badge,
        )}>
          {meta.label}
        </span>
        {!compact && !NATIVE_CARD_SPORTS.has(pick.sport) && (
          <span className="text-[9px] uppercase tracking-wider text-muted-foreground/50">
            summary view
          </span>
        )}
      </div>
      <div className="min-w-0 flex-1 space-y-1.5">
        <MatchupSnippet pick={pick} compact={compact} />
        <div className="text-sm font-semibold text-foreground">
          {pick.pick}{' '}
          <span className="ml-1 text-xs font-normal text-muted-foreground">
            {humanizeBetType(pick.bet_type)}
          </span>
        </div>
      </div>
      <div className="flex items-center gap-4 flex-shrink-0">
        <div className="text-right">
          <div className="text-sm font-semibold text-foreground tabular-nums">{oddsTxt}</div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">odds</div>
        </div>
        <div className="text-right">
          <div className="text-sm font-semibold text-foreground tabular-nums">
            {pick.stake_units.toFixed(2)}u
          </div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground tabular-nums">
            ${pick.stake_dollars.toFixed(0)}
          </div>
        </div>
        <button
          onClick={copy}
          className={cn(
            'rounded-md border px-3 py-2 text-[11px] font-semibold uppercase tracking-wider transition-colors',
            copied
              ? 'border-positive text-positive bg-positive/10'
              : 'border-border text-muted-foreground hover:border-foreground hover:text-foreground hover:bg-accent/40',
          )}
          title="Copy pick details to clipboard"
        >
          {copied ? '✓ Copied' : 'Copy'}
        </button>
      </div>
    </div>
  )
}


// ── Matchup snippet for slim row ────────────────────────────

function MatchupSnippet({ pick, compact }) {
  const card = pick.card || {}
  const font = compact ? 'text-[11px]' : 'text-xs'
  if (card.away_abbr && card.home_abbr) {
    const size = compact ? 'h-6 w-6' : 'h-7 w-7'
    return (
      <div className={cn('flex items-center gap-2 flex-wrap', font)}>
        <TeamBadge sport={pick.sport} abbr={card.away_abbr} size={size} />
        <span className="text-muted-foreground/60">@</span>
        <TeamBadge sport={pick.sport} abbr={card.home_abbr} size={size} />
      </div>
    )
  }
  if (card.p1_name && card.p2_name) {
    return (
      <div className={cn('flex items-center gap-2 flex-wrap', font)}>
        <span className="font-medium text-foreground truncate max-w-[160px]">
          {card.p1_name}
        </span>
        <span className="text-muted-foreground/60">vs</span>
        <span className="font-medium text-foreground truncate max-w-[160px]">
          {card.p2_name}
        </span>
      </div>
    )
  }
  return (
    <div className={cn('truncate text-muted-foreground', font)}
         title={pick.matchup}>
      {pick.matchup || '—'}
    </div>
  )
}


function TeamBadge({ sport, abbr, size }) {
  const logo = resolveTeamLogo(sport, abbr, null)
  const [err, setErr] = useState(false)
  return (
    <span className="inline-flex items-center gap-1.5">
      {logo && !err ? (
        <img
          src={logo}
          alt=""
          onError={() => setErr(true)}
          className={cn(
            size,
            'rounded-full object-contain bg-foreground/[0.06] ring-1 ring-border p-0.5 flex-shrink-0',
          )}
        />
      ) : (
        <span
          className={cn(
            size,
            'rounded-full bg-primary/15 ring-1 ring-border flex items-center justify-center text-[9px] font-bold text-primary flex-shrink-0',
          )}
        >
          {(abbr || '?').slice(0, 3).toUpperCase()}
        </span>
      )}
      <span className="font-bold text-foreground">{abbr}</span>
    </span>
  )
}
