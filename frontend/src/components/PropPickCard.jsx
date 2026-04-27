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


export default function PropPickCard({ pick, sport }) {
  const [photoErr, setPhotoErr] = useState(false)
  const photo = !photoErr ? playerPhotoUrl(sport, pick.player_id) : null
  const odds = pick.odds
  const oddsStr = odds != null ? `${odds > 0 ? '+' : ''}${odds}` : ''
  const conf = (pick.confidence || 'lean').toLowerCase()
  const edgePct = Number(pick.edge || 0)
  const modelPct = Math.round((pick.model_prob || 0) * 100)

  return (
    <div
      className={cn(
        'group relative flex flex-col gap-3 rounded-xl border border-border border-l-4 bg-card p-4',
        'transition-all duration-150',
        'hover:border-border hover:bg-accent/40 hover:-translate-y-px hover:shadow-lg',
        CONF_ACCENT[conf] || CONF_ACCENT.lean,
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
            <EdgeBadge edge={edgePct} confidence={conf} />
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


function ModelBar({ modelPct }) {
  const w = `${Math.min(100, Math.max(0, modelPct))}%`
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
        <span>Model probability</span>
        <span className="tabular-nums font-bold text-foreground">{modelPct}%</span>
      </div>
      <div className="relative h-2 w-full rounded-full bg-secondary overflow-hidden">
        <div
          className="absolute inset-y-0 left-0 rounded-full bg-positive/80 transition-all"
          style={{ width: w }}
        />
      </div>
    </div>
  )
}
