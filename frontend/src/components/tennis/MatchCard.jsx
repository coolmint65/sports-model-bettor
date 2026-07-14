/**
 * Tennis MatchCard — single match card used by TennisPanel + QueuePanel.
 *
 * Mirrors engine team-sport `primitives/GameCard` shell so a tennis
 * match reads visually identical to an NBA / NHL row: bordered rounded
 * card with a left-border confidence accent, status pill at the top,
 * players stacked vertically (p1 above p2 — the analog of away above
 * home in team sports), a win-prob bar, and the time + tournament
 * context at the bottom.
 *
 * Extracted from TennisPanel 2026-07-03 so the cross-sport bet queue
 * renders tennis picks with the exact same card the tennis panel uses —
 * the user's "copy and paste effectively" directive.
 */
import { useState } from 'react'
import { User } from 'lucide-react'
import { cn } from '../../lib/utils'
import PickEventsBadge from '../PickEventsBadge'
import SetScoreGrid from './SetScoreGrid'
import TennisOddsGrid from './TennisOddsGrid'

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
  const p1Prob = pred.p1_win_prob ?? null
  const p2Prob = pred.p2_win_prob ?? null
  const elo1 = pred.p1_rating != null ? Math.round(pred.p1_rating) : null
  const elo2 = pred.p2_rating != null ? Math.round(pred.p2_rating) : null
  const isLive = match.status === 'in'
  const isFinal = match.status === 'post'
  const isPre = !isLive && !isFinal
  const bestPick = match.best_pick || null
  const odds = match.odds || {}
  const conf = match.confidence || (bestPick ? 'lean' : 'skip')

  const accentClass = {
    strong:   'border-l-positive bg-positive/[0.03]',
    moderate: 'border-l-primary bg-primary/[0.025]',
    lean:     'border-l-border',
    skip:     'border-l-border',
  }[conf] || 'border-l-border'

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
        'group relative flex flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4 cursor-pointer',
        'transition-all duration-150',
        'hover:border-border hover:bg-accent/40 hover:-translate-y-px hover:shadow-lg',
        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        accentClass,
        isLive && 'ring-1 ring-negative/30',
      )}
      onClick={onOpen}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpen?.() } }}
    >
      <div className="flex items-center gap-2 flex-wrap">
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
            <TennisEdgeBadge pick={bestPick} confidence={conf} />
          </div>
          {match.match_id && (
            <div className="flex-shrink-0">
              <PickEventsBadge sport="tennis" gameId={match.match_id} />
            </div>
          )}
        </div>
      )}

      <div className="flex flex-col gap-1">
        <PlayerRow
          name={match.p1_name}
          country={match.p1_country}
          image={match.p1_image}
          flag={match.p1_flag}
          elo={elo1}
          prob={p1Prob}
          isFav={p1Prob != null && p1Prob >= (p2Prob ?? 0)}
          showProb={isPre || isLive}
        />
        <PlayerRow
          name={match.p2_name}
          country={match.p2_country}
          image={match.p2_image}
          flag={match.p2_flag}
          elo={elo2}
          prob={p2Prob}
          isFav={p2Prob != null && p2Prob > (p1Prob ?? 0)}
          showProb={isPre || isLive}
        />
      </div>

      {(isPre || isLive) && p1Prob != null && p2Prob != null && (
        <TennisProbBar
          p1Prob={p1Prob} p2Prob={p2Prob}
          p1Name={match.p1_name} p2Name={match.p2_name}
        />
      )}

      {isPre && <TennisOddsGrid odds={odds} match={match} />}

      {isPre && (
        <TennisCardInsight match={match} pred={match.prediction} />
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

function TennisEdgeBadge({ pick, confidence }) {
  const STYLE = {
    strong:   'bg-positive/10 text-positive border-positive/30',
    moderate: 'bg-primary/10 text-primary border-primary/30',
    lean:     'bg-muted/40 text-foreground/85 border-border',
  }
  if (!pick) {
    return (
      <div className="flex items-center justify-center rounded-md border border-dashed border-border bg-muted/20 px-3 py-1.5 text-[11px] font-bold uppercase tracking-widest text-muted-foreground"
           title="No tennis edge above the floor for this match">
        No pick
      </div>
    )
  }
  const tone = STYLE[confidence] || STYLE.lean
  const edge = pick.edge != null ? Math.round(pick.edge * 10) / 10 : null
  const sign = (edge ?? 0) >= 0 ? '+' : ''
  return (
    <div className={cn('flex items-center gap-2 rounded-md border px-3 py-1.5', tone)}>
      <span className="text-[10px] font-semibold uppercase tracking-wider opacity-75">
        {prettyTennisType(pick.type)}
      </span>
      <span className="text-sm font-bold leading-none truncate">{pick.pick}</span>
      {edge != null && (
        <span className="ml-auto text-xs font-bold tabular-nums">
          {sign}{edge.toFixed(1)}%
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


// ── Card insight ─────────────────────────────────────────────

function TennisCardInsight({ match, pred }) {
  if (!pred) return null
  const reasons = []
  const lastName = (n) => (n || '').split(' ').slice(-1)[0]
  const p1 = lastName(match.p1_name)
  const p2 = lastName(match.p2_name)
  const r1 = pred.p1_rating, r2 = pred.p2_rating
  if (r1 != null && r2 != null) {
    const diff = Math.abs(r1 - r2)
    if (diff >= 60) {
      const better = r1 > r2 ? p1 : p2
      reasons.push({
        weight: diff,
        text: <><strong>{better}</strong> +{Math.round(diff)} surface Elo edge</>,
      })
    }
  }
  const f1 = match.p1_form?.win_pct
  const f2 = match.p2_form?.win_pct
  if (f1 != null && f2 != null) {
    const diff = (f1 - f2) * 100
    if (Math.abs(diff) >= 25) {
      const better = diff > 0 ? p1 : p2
      reasons.push({
        weight: Math.abs(diff),
        text: <><strong>{better}</strong> hot ({Math.round(Math.max(f1, f2) * 100)}% L10)</>,
      })
    }
  }
  const p1Prob = pred.p1_win_prob
  if (p1Prob != null && Math.abs(p1Prob - 0.5) >= 0.20) {
    const fav = p1Prob > 0.5 ? p1 : p2
    const pct = Math.round(Math.max(p1Prob, 1 - p1Prob) * 100)
    reasons.push({
      weight: Math.abs(p1Prob - 0.5) * 10,
      text: <><strong>{fav}</strong> model favorite ({pct}%)</>,
    })
  }
  if (reasons.length === 0) return null
  reasons.sort((a, b) => b.weight - a.weight)
  return (
    <div className="rounded-md border border-border/60 bg-background/30 px-2.5 py-1.5 text-[11px] text-foreground/85">
      {reasons[0].text}
    </div>
  )
}


// ── Player row + avatar + prob bar ──────────────────────────

function PlayerRow({ name, country, image, flag, elo, prob, isFav, showProb }) {
  const metaParts = []
  if (country) metaParts.push(country)
  if (elo != null) metaParts.push(`Elo ${elo}`)
  return (
    <div className={cn(
      'flex items-center gap-2.5 min-w-0 rounded-md px-1 py-1',
      isFav && 'bg-positive/[0.04]',
    )}>
      <PlayerAvatar image={image} flag={flag} name={name} />
      <div className="flex flex-col min-w-0 flex-1">
        <span className={cn(
          'text-sm leading-tight truncate',
          isFav ? 'font-bold text-foreground' : 'font-semibold text-foreground/90',
        )}>
          {name}
        </span>
        {metaParts.length > 0 && (
          <span className="text-[10px] tabular-nums text-muted-foreground">
            {metaParts.join(' · ')}
          </span>
        )}
      </div>
      {showProb && prob != null && (
        <span className={cn(
          'ml-auto text-lg tabular-nums',
          isFav ? 'font-bold text-positive' : 'font-semibold text-muted-foreground',
        )}>
          {(prob * 100).toFixed(0)}%
        </span>
      )}
    </div>
  )
}


function PlayerAvatar({ image, flag, name }) {
  const [imgFailed, setImgFailed] = useState(false)
  const [flagFailed, setFlagFailed] = useState(false)
  const showImg = image && !imgFailed
  return (
    <div className="relative flex-shrink-0">
      {showImg ? (
        <img
          src={image}
          alt=""
          onError={() => setImgFailed(true)}
          className="h-10 w-10 rounded-full object-cover bg-foreground/[0.06] ring-1 ring-border"
        />
      ) : (
        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-secondary ring-1 ring-border"
             title={name || 'No headshot available'}>
          <User className="h-5 w-5 text-muted-foreground" aria-hidden="true" />
        </div>
      )}
      {flag && !flagFailed && (
        <img
          src={flag}
          alt=""
          onError={() => setFlagFailed(true)}
          className="absolute -bottom-0.5 -right-0.5 h-4 w-4 rounded-full object-cover ring-1 ring-card bg-card"
        />
      )}
    </div>
  )
}


function TennisProbBar({ p1Prob, p2Prob, p1Name, p2Name }) {
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
    </div>
  )
}


