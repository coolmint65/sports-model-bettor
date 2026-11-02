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
import { pickParts } from '../../lib/rail'

// Confidence accent applied as a left-border color so a slate of
// cards reads like a heat map. Subtle background tint reinforces
// without overwhelming dense per-card content.
const CONF_ACCENT = {
  strong:   'border-l-positive bg-positive/[0.03]',
  moderate: 'border-l-primary bg-primary/[0.025]',
  lean:     'border-l-border',
  skip:     'border-l-border',
  // Paper-only cards (Austin 2026-09-29): sky accent for a paper lean so the
  // border matches the sky PaperBadge instead of the hidden real pick's color.
  paper:    'border-l-sky-500/70 bg-sky-500/[0.03]',
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
  view,
}) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'
  const conf = bet?.confidence || 'skip'
  const hasPick = bet?.best_pick && conf !== 'skip'
  // game?.paper_pick comes from the scoreboard sync (recorded_nhl_paper_pick),
  // which is shaped like a FULL-GAME pick — it has no notion of which sub-tab
  // is active. Only let it stand in for a missing bet?.paper_pick on the
  // 'full' view; a P1 (or other non-full) tab must never show a full-game
  // pick that way (Austin 2026-10-01 bugfix). `view` is undefined for
  // sports/callers that don't pass it (e.g. before this was threaded through
  // everywhere) -- treat that as 'full' so existing behavior is unchanged.
  const _fullViewFallbackOk = !view || view === 'full'
  const _gamePaperPick = _fullViewFallbackOk ? game?.paper_pick : null
  // Paper-only: the left-border accent follows the PAPER pick (sky), not the
  // hidden real pick's confidence — so a NO-PICK or paper card never shows the
  // green "active pick" border (Austin 2026-09-29).
  const _rawShown = bet?.paper_pick || _gamePaperPick
  const _shownPick = _rawShown   // leans stay on live + final cards (Austin 2026-10-27, was pre-game only); the backend holds each game's pre-game lean
  const accentKey = _shownPick ? 'paper' : 'lean'
  const gameTimeLabel = useMemo(
    () => {
      const d = new Date(game.date || game.time)
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
    [game.date, game.time],
  )

  return (
    <div
      className={cn(
        'group relative flex h-full flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4',
        'transition-all duration-150',
        'hover:border-border hover:bg-accent/40',
        onClick && 'cursor-pointer hover:-translate-y-px hover:shadow-lg',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        CONF_ACCENT[accentKey] || CONF_ACCENT.lean,
        isLive && 'ring-1 ring-negative/30',
      )}
      onClick={onClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
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
      {/* Pick persists across pre/live/final (Austin 2026-09-19 — "stays like WNBA").
          The empty NO-PICK badge stays pre-game-only so finished games without a
          pick don't show a dangling placeholder. */}
      {(isPre || hasPick || _shownPick) && (
        <div className="flex items-stretch gap-2">
          <div className="flex-1 min-w-0">
            {/* Paper-ONLY (Austin 2026-09-29): show the PAPER lean, else NO PICK.
                The real gated pick NEVER renders on a card — real picks live on
                the Tracker tab. (Was falling back to the real EdgeBadge when a
                game had no paper lean, leaking real picks like FLA -1.5.)
                bet?.paper_pick is always view-correct (BetsView re-keys it per
                sub-tab); game?.paper_pick (the scoreboard-sync fallback) is
                full-game-shaped, so it's gated to the 'full' view via
                _gamePaperPick above (Austin 2026-10-01). */}
            {_shownPick
              ? <PaperBadge pick={_shownPick} />
              : isPre ? <EdgeBadge empty /> : null}
          </div>
          {sport && bet?.game_id && (
            <div className="flex-shrink-0">
              <PickEventsBadge sport={sport} gameId={bet.game_id} scope={pickEventsScope} />
            </div>
          )}
        </div>
      )}

      {/* Derivative badge removed from the game card (Austin 2026-09-19) — derivatives
          have their own tab now, so the full-game card stays single-bet (the paper lean). */}

      {/* One chip row, its height reserved on every pre-game card so the team rows line up
          across a slate (Austin 2026-10-27 "fix up the spacing of the game cards"). */}
      {isPre && (
        <div className="flex min-h-[1.375rem] flex-wrap items-center gap-1">
          <RestChips rest={bet?.rest} home={home} away={away} tiredLabel={restTiredLabel} />
          <LineMovedChip lm={game.line_movement} />
        </div>
      )}

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
      {/* Odds + footer pinned to the bottom: cards in a grid row share the tallest card's height
          (h-full), so their odds tables and footers line up (Austin 2026-10-27). */}
      <div className="mt-auto flex flex-col gap-2">
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


/**
 * PaperBadge — the model's best PAPER pick, shown in the headline slot IN PLACE OF the
 * "NO PICK" empty badge when the gated pipeline produced no real bet. Sky-tinted + "paper"
 * tag so it's never mistaken for a staked recommendation. Reads bet.paper_pick (best-of by
 * edge, set in adaptBetShape). Austin 2026-09-18. Mirrors DerivativeBadge.
 */
function PaperBadge({ pick }) {
  if (!pick) return null
  const edge = Number(pick.edge ?? 0)
  const { mkt, label } = pickParts(pick)
  // Austin 2026-10-26: every card shows its best lean -- VERY CLEARLY not graduated -- until its market
  // graduates (Pick Authority proven edge). Graduated = green; a lean = amber, dashed, spelled out.
  const grad = pick.graduated === true
  return (
    <div className={cn('rounded-md border px-2.5 py-1 text-[11px]',
                       grad ? 'border-emerald-500/50 bg-emerald-500/[0.08]' : 'border-dashed border-amber-500/70 bg-amber-500/[0.07]')}
         title={grad ? 'Graduated market: this pick type has proven edge (Pick Authority).'
                     : 'Model lean only. This market has NOT graduated: no proven edge yet. Not a bet.'}>
    <div className="flex items-center gap-2">
      <span className={cn('rounded-sm px-1 text-[9px] font-bold uppercase tracking-wider whitespace-nowrap',
                          grad ? 'bg-emerald-500/25 text-emerald-200' : 'bg-amber-500/25 text-amber-200')}>
        {grad ? 'graduated' : 'lean · not graduated'}
      </span>
      <span className="flex-1" />
      <span className={cn('font-bold tabular-nums', edge >= 0 ? 'text-emerald-400' : 'text-muted-foreground')}>
        {edge >= 0 ? '+' : ''}{Number.isFinite(edge) ? edge.toFixed(1) : '0.0'}pp
      </span>
      {pick.result && (
        <span className={cn(
          'rounded-sm px-1 text-[9px] font-bold uppercase tracking-wider',
          pick.result === 'W' ? 'bg-emerald-500/20 text-emerald-300'
            : pick.result === 'L' ? 'bg-red-500/20 text-red-300'
            : 'bg-muted text-muted-foreground',
        )}>
          {pick.result === 'W' ? 'won' : pick.result === 'L' ? 'lost' : pick.result === 'P' ? 'push' : pick.result}
        </span>
      )}
    </div>
    <div className="mt-0.5 flex items-baseline gap-1.5 min-w-0">
      <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground whitespace-nowrap">{mkt}</span>
      <span className="font-semibold text-foreground/90 break-words min-w-0">{label}</span>
    </div>
    {!grad && (
      <div className="mt-0.5 text-[9px] text-amber-300/80">model edge, unproven &middot; not a bet</div>
    )}
    </div>
  )
}


const GameCard = memo(GameCardImpl)
export default GameCard
