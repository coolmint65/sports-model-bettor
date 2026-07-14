/**
 * GameCard
 * ──────────────────────────────────────────────────────────────
 * Sport-agnostic scorecard shell. Phase 2d-iv restyle: Tailwind +
 * design tokens for the wrapper, LIVE/FINAL badges, team stack,
 * and meta row. Confidence accent now lives on the LEFT border so
 * the user can scan a slate of cards and see the top picks at a
 * glance without color-coding bleeding into the whole surface.
 *
 * Sport-specific render-prop slots (insight / starters / odds /
 * liveExtras) are still consumed as-is — those carry their own
 * sport-specific markup that gets restyled per-sport.
 *
 * Props:
 *   pickAccent = 'q1'  -> amber EdgeBadge styling for NBA
 *   restTiredLabel     -> "tired" (MLB) or "B2B" (NHL/NBA)
 *   sport              -> 'mlb'/'nhl'/'nba' so PickEventsBadge can scope
 */

import { memo, useMemo } from 'react'
import EdgeBadge from './EdgeBadge'
import WinProbBar from './WinProbBar'
import TeamRow from './TeamRow'
import RestChips from './RestChips'
import LineMovedChip from './LineMovedChip'
import PickEventsBadge from '../PickEventsBadge'
import { cn } from '../../lib/utils'
import { humanizeBetType } from '../../lib/betType'

// Confidence accent applied as a left-border color so a slate of
// cards reads like a heat map. Subtle background tint reinforces
// without overwhelming dense per-card content.
const CONF_ACCENT = {
  strong:   'border-l-positive bg-positive/[0.03]',
  moderate: 'border-l-primary bg-primary/[0.025]',
  lean:     'border-l-border',
  skip:     'border-l-border',
}

function GameCardImpl({
  game,
  bet,
  onClick,
  insight,
  starters,
  odds,
  liveExtras,
  pickAccent,
  restTiredLabel = 'tired',
  sport,
  pickEventsScope,
  isPotd = false,
}) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'
  const conf = bet?.confidence || 'skip'
  const hasPick = bet?.best_pick && conf !== 'skip'
  const gameTimeLabel = useMemo(
    () => {
      const d = new Date(game.date)
      const time = d.toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
      // Prefix the local-day name (Tue, Wed, ...) when the card's date
      // isn't TODAY in the viewer's timezone — otherwise tomorrow's
      // games look like tonight's. Today stays just "8:30 PM".
      const today = new Date()
      const sameDay = d.getFullYear() === today.getFullYear()
        && d.getMonth() === today.getMonth()
        && d.getDate() === today.getDate()
      if (sameDay) return time
      const dow = d.toLocaleDateString([], { weekday: 'short' })
      return `${dow} ${time}`
    },
    [game.date],
  )

  return (
    <div
      className={cn(
        'group relative flex flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4 cursor-pointer',
        'transition-all duration-150',
        'hover:border-border hover:bg-accent/40 hover:-translate-y-px hover:shadow-lg',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        CONF_ACCENT[conf] || CONF_ACCENT.lean,
        isLive && 'ring-1 ring-negative/30',
      )}
      onClick={onClick}
      role="button"
      tabIndex={0}
    >
      {/* POTD star — top-right corner. Surfaces the active Pick of the
          Day before the calendar rolls over (key for far-east leagues
          whose POTD pick is on tomorrow ET). Mint-themed to match the
          POTD hero's accent so users link the two visually. */}
      {isPotd && (
        <span
          className="absolute -top-1.5 -right-1.5 z-10 inline-flex items-center gap-0.5 rounded-full bg-primary px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-primary-foreground shadow"
          title="Pick of the Day"
        >
          ★ POTD
        </span>
      )}

      {/* Top status row — LIVE/FINAL inline so the team-row score on
          the right never collides with an absolute corner pill. Pre-game
          renders the EdgeBadge here instead; the slot is single-occupant. */}
      {isLive && (
        <span className="inline-flex w-max items-center gap-1 rounded-full bg-negative/20 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-negative">
          ● Live
        </span>
      )}
      {isFinal && (
        <span className="inline-flex w-max items-center rounded-full bg-muted px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
          Final
        </span>
      )}

      {/* EdgeBadge + 📜 popover sit on the same row so the badge never
          overlaps the +edge% text. Pick-events badge takes its own slot
          to the right of the EdgeBadge with no overlap risk. */}
      {isPre && (
        <div className="flex items-stretch gap-2">
          <div className="flex-1 min-w-0">
            {hasPick
              ? <EdgeBadge pick={bet.best_pick} confidence={conf} accent={pickAccent} />
              : <EdgeBadge empty />}
          </div>
          {sport && bet?.game_id && (
            <div className="flex-shrink-0">
              <PickEventsBadge sport={sport} gameId={bet.game_id} scope={pickEventsScope} />
            </div>
          )}
        </div>
      )}

      {/* Secondary derivative badge — only renders when the core has
          no pick AND a non-skip derivative exists. Replaces (not
          augments) the "NO PICK" empty badge: one bet per card always. */}
      {isPre && !hasPick && bet?.best_derivative
        && (bet.best_derivative.confidence || 'lean') !== 'skip' && (
        <DerivativeBadge pick={bet.best_derivative} />
      )}

      {isPre && <RestChips rest={bet?.rest} home={home} away={away} tiredLabel={restTiredLabel} />}
      {isPre && <LineMovedChip lm={game.line_movement} />}

      {game.series?.in_series && (
        <div className="text-center text-[11px] text-muted-foreground tracking-wide py-0.5">
          Game {game.series.game_number}
          {game.series.game_number > 1 && (
            <> · {
              game.series.is_tied
                ? `Tied ${game.series.home_wins}-${game.series.away_wins}`
                : game.series.series_leader === 'home'
                  ? `${home.abbreviation} leads ${game.series.home_wins}-${game.series.away_wins}`
                  : `${away.abbreviation} leads ${game.series.away_wins}-${game.series.home_wins}`
            }</>
          )}
          {game.series.is_elimination && <> · <span className="font-semibold text-negative">ELIMINATION</span></>}
        </div>
      )}

      <div className="flex flex-col gap-1">
        <TeamRow team={away} isLive={isLive} isFinal={isFinal} sport={sport} />
        <TeamRow team={home} isLive={isLive} isFinal={isFinal} sport={sport} />
      </div>

      {liveExtras}

      {isPre && bet?.win_prob?.home != null && (
        <WinProbBar wp={bet.win_prob} home={home} away={away} />
      )}

      {isPre && hasPick && insight}
      {isPre && starters}
      {odds}

      <div className="mt-1 flex items-center justify-between text-[11px] text-muted-foreground">
        <span className="tabular-nums">
          {isPre && gameTimeLabel}
          {isLive && status.detail}
        </span>
        {game.broadcast && (
          <span className="truncate ml-2 text-right">{game.broadcast}</span>
        )}
      </div>
    </div>
  )
}

/**
 * DerivativeBadge — secondary EdgeBadge variant for the derivative
 * pick attached to a card. Visually subordinate to the main EdgeBadge:
 * smaller text, muted color, "DERIV" prefix so it's never confused
 * with the core headline.
 *
 * Renders nothing when pick is null (parent already gates on
 * confidence != 'skip').
 */
function DerivativeBadge({ pick }) {
  if (!pick) return null
  // Coerce to Number — backend serialization is reliable for picks_core
  // output, but downstream (POTD, signal log, partial JSON merges) can
  // occasionally hand us a stringified edge. Numeric.toFixed throws
  // on strings; this kills the whole card render. Defensive.
  const edge = Number(pick.edge ?? 0)
  const odds = pick.odds
  const oddsLabel = odds == null ? ''
    : odds > 0 ? `+${odds}` : `${odds}`
  const conf = pick.confidence || 'lean'
  const tone = conf === 'strong'
    ? 'border-positive/40 bg-positive/[0.06] text-positive'
    : conf === 'moderate'
      ? 'border-primary/40 bg-primary/[0.06] text-primary'
      : 'border-border bg-muted/40 text-muted-foreground'
  return (
    <div className={cn(
      'flex items-center gap-2 rounded-md border px-2.5 py-1 text-[11px]',
      tone,
    )}>
      <span className="text-[9px] font-bold uppercase tracking-wider opacity-70">
        Deriv
      </span>
      <span className="text-[10px] font-semibold uppercase tracking-wider">
        {humanizeBetType(pick.type) || pick.type}
      </span>
      <span className="font-semibold text-foreground/90 truncate flex-1">
        {pick.pick}
      </span>
      <span className="tabular-nums text-foreground/80">{oddsLabel}</span>
      <span className="font-bold tabular-nums">
        {edge >= 0 ? '+' : ''}{Number.isFinite(edge) ? edge.toFixed(1) : '0.0'}%
      </span>
    </div>
  )
}


const GameCard = memo(GameCardImpl)
export default GameCard
