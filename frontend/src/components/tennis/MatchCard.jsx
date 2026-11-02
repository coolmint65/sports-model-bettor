/**
 * Tennis MatchCard — single match card used by TennisPanel + QueuePanel.
 *
 * Mirrors engine team-sport `primitives/GameCard` shell so a tennis
 * match reads visually identical to an NBA / NHL row: bordered rounded
 * card with a left-border accent, status pill at the top, players
 * stacked vertically (p1 above p2 — the analog of away above home in
 * team sports), a thin win-prob bar, and the time + tournament context
 * at the bottom.
 *
 * Extracted from TennisPanel 2026-07-03 so the cross-sport bet queue
 * renders tennis picks with the exact same card the tennis panel uses —
 * the user's "copy and paste effectively" directive.
 *
 * Compact restyle (PickMint tennis parity Phase 1, 2026-10-10): trimmed
 * to GameCard's summary-card density. Dropped from the card — not from
 * the app — because tennis/MatchDetail.jsx already carries a strictly
 * superset of this content behind the click (every HR market, H2H,
 * recent form, per-player Elo, every qualifying pick):
 *   - the always-on ML/Set/O-U/Sets odds grid (was TennisOddsGrid)
 *   - the Elo/form "why" blurb (was TennisCardInsight)
 *   - player avatars/flags + the inline Elo-rating text per row
 * Also now follows GameCard's real-pick rule: the card NEVER shows a
 * real gated pick, only the model's best calibrated PAPER lean (or the
 * empty state) — same as every other sport's card since 2026-09-29.
 */
import { cn } from '../../lib/utils'
import PickEventsBadge from '../PickEventsBadge'
import SetScoreGrid from './SetScoreGrid'

// Tournament tier chips (Slam / Masters / 500 / etc.) — same treatment
// used in the tournament index.
const TIER_LABEL = {
  G:   { label: 'Slam',      cls: 'bg-positive/15 text-positive' },
  F:   { label: 'Finals',    cls: 'bg-positive/15 text-positive' },
  M:   { label: 'Masters',   cls: 'bg-primary/15 text-primary' },
  P:   { label: 'Premier',   cls: 'bg-primary/15 text-primary' },
  PM:  { label: 'Premier',   cls: 'bg-primary/15 text-primary' },
  P5:  { label: 'Premier',   cls: 'bg-primary/15 text-primary' },
  '500': { label: '500',     cls: 'bg-warning/15 text-warning' },
  '250': { label: '250',     cls: 'bg-muted text-muted-foreground' },
  A:   { label: 'Tour',      cls: 'bg-muted text-muted-foreground' },
  '125': { label: '125',     cls: 'bg-muted/60 text-muted-foreground' },
  C:   { label: 'Challenger', cls: 'bg-muted/60 text-muted-foreground' },
  ITF: { label: 'ITF',       cls: 'bg-muted/60 text-muted-foreground' },
}

export function TierChip({ level }) {
  const cfg = TIER_LABEL[level] || TIER_LABEL.A
  return (
    <span className={cn(
      'rounded-full px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider',
      cfg.cls,
    )}>
      {cfg.label}
    </span>
  )
}


export default function MatchCard({ match, onOpen }) {
  const pred = match.prediction || {}
  // Card % = the Pick Authority's market blend (backend card_win_prob, home = p1), model's own number below.
  const cwp = match.card_win_prob
  const p1Prob = cwp?.home ?? pred.p1_win_prob ?? null
  const p2Prob = cwp?.away ?? pred.p2_win_prob ?? null
  const p1Model = cwp?.source === 'blend' ? cwp.model_home : null
  const p2Model = cwp?.source === 'blend' ? cwp.model_away : null
  const isLive = match.status === 'in'
  const isFinal = match.status === 'post'
  const isPre = !isLive && !isFinal
  // Card-level pick is ALWAYS the model's best calibrated PAPER lean —
  // never the real gated pick (GameCard's rule, Austin 2026-09-29: "the
  // real gated pick NEVER renders on the card"; real picks live on the
  // Tracker tab only). Max-edge of paper_picks, independent of whether
  // a real best_pick also exists for this match.
  const paperPicks = match.paper_picks || []
  const paperPick = paperPicks.length
    ? paperPicks.reduce((a, b) => ((b.edge ?? -99) > (a.edge ?? -99) ? b : a))
    : null

  // Left-border accent follows the paper pick (sky), matching GameCard's
  // CONF_ACCENT — confidence-tinted (strong/moderate) borders are retired
  // from the card along with the real pick they used to describe.
  const accentClass = paperPick
    ? 'border-l-sky-500/70 bg-sky-500/[0.03]'
    : 'border-l-border'

  const dayLabel = (() => {
    if (!match.date) return null
    const today = new Date(); today.setHours(0,0,0,0)
    const md = new Date(`${match.date}T00:00:00`)
    const diff = Math.round((md - today) / 86400000)
    if (diff === 0) return null
    if (diff === 1) return 'TOMORROW'
    if (diff > 1) return md.toLocaleDateString([], { weekday: 'short' }).toUpperCase()
    return null
  })()

  const startLabel = match.start_time
    ? new Date(match.start_time).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })
    : ''

  return (
    <div
      className={cn(
        'group relative flex flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4',
        'transition-all duration-150',
        'hover:border-border hover:bg-accent/40',
        onOpen && 'cursor-pointer hover:-translate-y-px hover:shadow-lg',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        accentClass,
        isLive && 'ring-1 ring-negative/30',
      )}
      onClick={onOpen}
      role={onOpen ? 'button' : undefined}
      tabIndex={onOpen ? 0 : undefined}
      onKeyDown={onOpen ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpen?.() } } : undefined}
    >
      <div className="flex items-center gap-2 flex-wrap">
        {match.tournament && (
          <span className="inline-flex items-center gap-1 text-[10px] font-semibold uppercase tracking-wider text-foreground/75 max-w-[62%]" title={match.tournament}>
            <TierChip level={match.tournament_level} />
            <span className="truncate">{match.tournament}</span>
          </span>
        )}
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
        {dayLabel && (
          <span className="inline-flex w-max items-center rounded-full bg-primary/10 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-primary">
            {dayLabel}
          </span>
        )}
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          BO{match.best_of || 3}
        </span>
        {match.round && (
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground/70">
            · {match.round}
          </span>
        )}
      </div>

      {isPre && (
        <div className="flex items-stretch gap-2" onClick={(e) => e.stopPropagation()}>
          <div className="flex-1 min-w-0">
            <TennisEdgeBadge pick={paperPick} />
          </div>
          {match.match_id && (
            <div className="flex-shrink-0">
              <PickEventsBadge sport="tennis" gameId={match.match_id} />
            </div>
          )}
        </div>
      )}

      <div className="flex flex-col gap-1">
        <PlayerRow name={match.p1_name} country={match.p1_country} image={match.p1_image} odds={match.odds?.markets?.ml?.p1_odds} />
        <PlayerRow name={match.p2_name} country={match.p2_country} image={match.p2_image} odds={match.odds?.markets?.ml?.p2_odds} />
      </div>

      {(isPre || isLive) && p1Prob != null && p2Prob != null && (
        <TennisProbBar
          p1Prob={p1Prob} p2Prob={p2Prob} p1Model={p1Model} p2Model={p2Model}
          p1Name={match.p1_name} p2Name={match.p2_name}
        />
      )}

      {isPre && match.line_movement && (
        <LineMovedChip lm={match.line_movement} match={match} />
      )}

      {(isLive || isFinal) && match.score && (
        <SetScoreGrid
          score={match.score}
          p1Name={match.p1_name}
          p2Name={match.p2_name}
          winner={match.winner}
        />
      )}

      <div className="mt-1 flex items-center justify-between text-[11px] text-muted-foreground">
        <span className="tabular-nums">
          {isPre && startLabel}
          {isLive && 'In progress'}
          {isFinal && 'Completed'}
        </span>
        <span className="text-right truncate ml-2 uppercase tracking-wider text-[10px]">
          {match.surface || ''}
        </span>
      </div>
    </div>
  )
}


// ── EdgeBadge ────────────────────────────────────────────────

const TENNIS_TYPE_LABEL = {
  ML:                       'ML',
  SET_SPREAD:               'Set Spread',
  GAME_SPREAD:              'Game Spread',
  TOTAL_GAMES:              'Total Games',
  TOTAL_SETS:               'Total Sets',
  P1_TOTAL_GAMES:           'P1 Games',
  P2_TOTAL_GAMES:           'P2 Games',
  WIN_AT_LEAST_ONE_SET:     'Win 1+ Set',
  SET_BETTING:              'Set Betting',
  MOST_GAMES:               'Most Games',
}
function prettyTennisType(t) {
  return TENNIS_TYPE_LABEL[t] || (t ? t.replace(/_/g, ' ') : '')
}

// Card ONLY ever shows the model's best calibrated PAPER lean (or the
// empty state) — never a real gated pick, matching primitives/GameCard's
// PaperBadge treatment exactly (same classes) so a tennis card and an
// NBA/NHL card read as the same visual language. Real picks + settled
// W/L live on the Tracker tab only.
function TennisEdgeBadge({ pick }) {
  if (!pick) {
    return (
      <div className="flex items-center justify-center rounded-md border border-dashed border-border bg-muted/20 px-3 py-1.5 text-[11px] font-bold uppercase tracking-widest text-muted-foreground"
           title="No tennis edge for this match">
        No pick
      </div>
    )
  }
  // Edge is in pp (percentage points of model prob over market-implied) —
  // same unit NHL/GameCard's PaperBadge uses.
  const edge = pick.edge != null ? Math.round(pick.edge * 10) / 10 : null
  const sign = (edge ?? 0) >= 0 ? '+' : ''
  return (
    <div className="flex items-center gap-2 rounded-md border border-dashed border-amber-500/70 bg-amber-500/[0.07] px-2.5 py-1 text-[11px]"
         title="Model lean only. Tennis markets have NOT graduated: no proven edge yet. Not a bet. (Austin 2026-10-26)">
      <span className="rounded-sm bg-amber-500/25 px-1 text-[9px] font-bold uppercase tracking-wider text-amber-200 whitespace-nowrap">
        lean · not graduated
      </span>
      <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {prettyTennisType(pick.type)}
      </span>
      <span className="font-semibold text-foreground/90 truncate flex-1">{pick.pick}</span>
      {edge != null && (
        <span className={cn('font-bold tabular-nums', edge >= 0 ? 'text-emerald-400' : 'text-muted-foreground')}>
          {sign}{edge.toFixed(1)}pp
        </span>
      )}
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
  )
}


// ── Line-moved chip ──────────────────────────────────────────

function LineMovedChip({ lm, match }) {
  if (!lm || lm.significance === 'none') return null
  const ml = lm.ml || {}
  const tg = lm.total_games || {}
  const lastName = (n) => (n || '').split(' ').slice(-1)[0]
  const parts = []
  if (lm.direction === 'p1' && ml.p1_implied_delta_pp != null) {
    parts.push(`${lastName(match.p1_name)} +${ml.p1_implied_delta_pp.toFixed(1)}pp`)
  } else if (lm.direction === 'p2' && ml.p2_implied_delta_pp != null) {
    parts.push(`${lastName(match.p2_name)} +${ml.p2_implied_delta_pp.toFixed(1)}pp`)
  }
  if (tg.line_delta && Math.abs(tg.line_delta) >= 0.5) {
    const sign = tg.line_delta > 0 ? '+' : ''
    parts.push(`Total ${sign}${tg.line_delta}`)
  }
  if (!parts.length) return null
  const tone = {
    major:    'bg-warning/15 text-warning border-warning/30',
    moderate: 'bg-primary/10 text-primary border-primary/30',
    minor:    'bg-muted/40 text-muted-foreground border-border',
  }[lm.significance] || 'bg-muted/40 text-muted-foreground border-border'
  return (
    <div className={cn(
      'flex items-center gap-2 rounded-md border px-2.5 py-1 text-[11px]',
      tone,
    )} title={`Opening captured ${lm.captured_at?.slice(11, 16) || ''} UTC`}>
      <span className="text-[9px] font-bold uppercase tracking-widest opacity-75">
        Line moved
      </span>
      <span className="tabular-nums">{parts.join(' · ')}</span>
    </div>
  )
}


// TennisCardInsight (Elo-diff / form-hot / model-favorite "why" blurb)
// and PlayerAvatar (headshot + flag images) removed from the summary
// card in the Phase 1 compact restyle (2026-10-10) — not lost, just
// relocated: tennis/MatchDetail.jsx's DetailPlayerBlock already shows
// each player's Elo rating + win prob, and FormBlock already shows
// each player's recent-form record, behind the card's onClick. That's
// the same underlying data this blurb summarized, just not pre-written
// into one sentence.


// ── Player row ───────────────────────────────────────────────
//
// Matches primitives/TeamRow's density: name + a thin muted trailing
// label, no imagery, no inline Elo/probability text. Win probability
// (and which side is favored) lives solely in TennisProbBar below,
// same split GameCard uses (TeamRow + a separate WinProbBar).

function PlayerRow({ name, country, image, odds }) {
  // Polish (Austin 2026-10-11): headshot + ML odds back on the card. The
  // avatar was dropped for compactness; a 24px circle keeps it tight while
  // matching the other sports' logo+odds density. Broken images hide cleanly.
  // Missing/broken headshots (common for lower-ranked players) fall back to
  // the player's INITIALS, not a blank circle, so no card looks half-empty
  // (Austin 2026-10-12).
  const _initials = (name || '').split(/\s+/).map(w => w[0]).filter(Boolean).slice(0, 2).join('').toUpperCase()
  return (
    <div className="flex items-center gap-2 min-w-0">
      <span className="relative h-6 w-6 flex-shrink-0">
        <span className="absolute inset-0 flex items-center justify-center rounded-full bg-muted text-[9px] font-bold text-muted-foreground">
          {_initials}
        </span>
        {image && (
          <img
            src={image}
            alt=""
            loading="lazy"
            className="absolute inset-0 h-6 w-6 rounded-full object-cover"
            onError={(e) => { e.currentTarget.style.display = 'none' }}
          />
        )}
      </span>
      <span className="text-sm font-semibold text-foreground/90 truncate flex-1">
        {name}
      </span>
      {country && (
        <span className="text-[11px] text-muted-foreground tabular-nums">
          {country}
        </span>
      )}
      {odds != null && (
        <span className="ml-1 text-[11px] font-semibold tabular-nums text-foreground/70">
          {odds > 0 ? '+' : ''}{odds}
        </span>
      )}
    </div>
  )
}


function TennisProbBar({ p1Prob, p2Prob, p1Name, p2Name, p1Model, p2Model }) {
  const p1Fav = p1Prob >= p2Prob
  const lastName = (n) => (n || '').split(' ').slice(-1)[0]
  return (
    <div className="mt-1">
      <div className="flex justify-between text-[10px] tabular-nums text-muted-foreground mb-1">
        <span className={cn(p1Fav && 'font-semibold text-foreground')}>
          {lastName(p1Name)} {Math.round(p1Prob * 100)}%
        </span>
        <span className={cn(!p1Fav && 'font-semibold text-foreground')}>
          {lastName(p2Name)} {Math.round(p2Prob * 100)}%
        </span>
      </div>
      <div className="flex h-1 overflow-hidden rounded-full bg-secondary">
        <div className="bg-warning" style={{ width: `${p1Prob * 100}%` }} />
        <div className="bg-primary" style={{ width: `${p2Prob * 100}%` }} />
      </div>
      {p1Model != null && p2Model != null && Math.abs(p1Model - p1Prob) >= 0.005 && (
        <div className="mt-0.5 text-[9px] tabular-nums text-muted-foreground/70">
          model: {lastName(p1Name)} {Math.round(p1Model * 100)}% · {lastName(p2Name)} {Math.round(p2Model * 100)}%
        </div>
      )}
    </div>
  )
}


