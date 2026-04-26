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
}) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'
  const conf = bet?.confidence || 'skip'
  const hasPick = bet?.best_pick && conf !== 'skip'
  const gameTimeLabel = useMemo(
    () => new Date(game.date).toLocaleTimeString(
      [], { hour: 'numeric', minute: '2-digit' },
    ),
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
              <PickEventsBadge sport={sport} gameId={bet.game_id} />
            </div>
          )}
        </div>
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

const GameCard = memo(GameCardImpl)
export default GameCard
