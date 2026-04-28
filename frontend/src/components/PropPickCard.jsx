/**
 * PropPickCard — single-pick card matching the GameCard visual
 * language. Used in PropsPanel to replace the legacy PicksTable
 * for pending picks.
 *
 * Layout:
 *   [photo]  Player Name                         [edge badge]
 *            Bet Type · Pick                     odds
 *            ───── model probability bar ─────
 */

import { useState } from 'react'
import { humanizeBetType } from '../lib/betType'
import { playerPhotoUrl } from '../lib/playerPhoto'
import { cn } from '../lib/utils'

const CONF_ACCENT = {
  strong:   'border-l-positive bg-positive/[0.03]',
  moderate: 'border-l-primary bg-primary/[0.025]',
  lean:     'border-l-border',
}

// Once a pick settles, the card communicates outcome instead of
// confidence — the conviction tier no longer matters and the result
// (W / L / Push) becomes the dominant signal.
const RESULT_ACCENT = {
  W: 'border-l-positive bg-positive/[0.10] ring-1 ring-positive/30',
  L: 'border-l-negative bg-negative/[0.08] ring-1 ring-negative/25',
  P: 'border-l-muted-foreground bg-muted/40',
}


export default function PropPickCard({ pick, sport }) {
  const [photoErr, setPhotoErr] = useState(false)
  if (!pick) return null
  const photo = !photoErr ? playerPhotoUrl(sport, pick.player_id) : null
  const odds = pick.odds
  const oddsStr = odds != null ? `${odds > 0 ? '+' : ''}${odds}` : ''
  const conf = (pick.confidence || 'lean').toString().toLowerCase()
  const edgePct = Number(pick.edge || 0)
  const modelPct = Math.round((Number(pick.model_prob) || 0) * 100)
  const result = pick.result // 'W' | 'L' | 'P' | null
  const settled = !!result
  const accent = settled
    ? (RESULT_ACCENT[result] || CONF_ACCENT.lean)
    : (CONF_ACCENT[conf] || CONF_ACCENT.lean)

  return (
    <div
      className={cn(
        'group relative flex flex-col gap-3 rounded-xl border border-border border-l-4 bg-card p-4',
        'transition-all duration-150',
        'hover:border-border hover:bg-accent/40 hover:-translate-y-px hover:shadow-lg',
        accent,
      )}
    >
      <div className="flex items-start gap-3">
        {/* Player photo */}
        <div className="flex-shrink-0">
          {photo ? (
            <img
              src={photo}
              alt=""
              className="h-14 w-14 rounded-full object-cover bg-muted ring-1 ring-border"
              onError={() => setPhotoErr(true)}
            />
          ) : (
            <div className="h-14 w-14 rounded-full bg-muted ring-1 ring-border flex items-center justify-center text-xs font-bold text-muted-foreground">
              {(pick.player_name || '?').slice(0, 1)}
            </div>
          )}
        </div>

        {/* Body */}
        <div className="flex-1 min-w-0">
          <div className="flex items-start justify-between gap-2">
            <div className="min-w-0">
              <div className="text-sm font-bold text-foreground truncate">
                {pick.player_name}
              </div>
              <div className="text-[11px] text-muted-foreground truncate">
                {humanizeBetType(pick.bet_type)} · {pick.matchup}
              </div>
            </div>
            {settled
              ? <ResultBadge result={result} profit={pick.profit} />
              : <EdgeBadge edge={edgePct} confidence={conf} />}
          </div>

          <div className="mt-2 flex items-baseline gap-2 flex-wrap">
            <span className="text-base font-bold tracking-tight text-foreground">
              {pick.pick}
            </span>
            {oddsStr && (
              <span className="text-sm font-semibold tabular-nums text-muted-foreground">
                {oddsStr}
              </span>
            )}
          </div>
        </div>
      </div>

      <ModelBar modelPct={modelPct} />
    </div>
  )
}


function EdgeBadge({ edge, confidence }) {
  const tone = confidence === 'strong'
    ? 'bg-positive/15 text-positive'
    : confidence === 'moderate'
      ? 'bg-primary/15 text-primary'
      : 'bg-muted text-muted-foreground'
  return (
    <span className={cn(
      'flex-shrink-0 rounded-md px-2 py-1 text-xs font-bold tabular-nums whitespace-nowrap',
      tone,
    )}>
      +{edge.toFixed(1)}%
    </span>
  )
}


function ResultBadge({ result, profit }) {
  const cfg = result === 'W' ? { label: 'WIN',  cls: 'bg-positive text-background' }
    : result === 'L'         ? { label: 'LOSS', cls: 'bg-negative text-background' }
    : result === 'P'         ? { label: 'PUSH', cls: 'bg-muted text-muted-foreground' }
    :                          { label: '—',    cls: 'bg-muted text-muted-foreground' }
  const profitTone = profit > 0 ? 'text-positive' : profit < 0 ? 'text-negative' : 'text-muted-foreground'
  return (
    <div className="flex flex-shrink-0 flex-col items-end gap-1">
      <span className={cn(
        'rounded-md px-2 py-1 text-[10px] font-bold tracking-wider whitespace-nowrap',
        cfg.cls,
      )}>
        {cfg.label}
      </span>
      {profit != null && (
        <span className={cn('text-xs font-semibold tabular-nums', profitTone)}>
          {profit > 0 ? '+' : ''}${Number(profit).toFixed(0)}
        </span>
      )}
    </div>
  )
}


function ModelBar({ modelPct }) {
  const w = `${Math.min(100, Math.max(0, modelPct))}%`
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
        <span>Model probability</span>
        <span className="tabular-nums font-bold text-foreground">{modelPct}%</span>
      </div>
      {/* Bumped from h-2/bg-secondary to h-2.5/bg-muted for more
          contrast against the card background; fill is now full-
          opacity bg-positive so it actually reads in dark mode. */}
      <div className="relative h-2.5 w-full rounded-full bg-muted overflow-hidden ring-1 ring-border/50">
        <div
          className="absolute inset-y-0 left-0 rounded-full bg-positive transition-all"
          style={{ width: w }}
        />
      </div>
    </div>
  )
}
