/**
 * PropPickCard — player-prop card in the GameCard visual language.
 * Restyle 2026-10-22 (Austin): matchup strip with team logos (like the
 * game cards), tiled in a grid, plus a last-10 recent-form sparkline.
 *
 * Layout:
 *   [awayLogo AWAY @ homeLogo HOME]              [edge / result]
 *   [photo]  Player Name · Bet Type
 *            Pick  odds  [CLV]
 *            ───── model probability bar ─────
 *            L10  ▁▅▃▇▂  7/10 hit
 */

import { useState } from 'react'
import { humanizeBetType } from '../lib/betType'
import { playerPhotoUrl } from '../lib/playerPhoto'
import { resolveTeamLogo } from '../lib/teamLogo'
import { cn } from '../lib/utils'
import CLVBadge from './CLVBadge'
import L10Spark from './L10Spark'

const CONF_ACCENT = {
  strong:   'border-l-positive bg-positive/[0.03]',
  moderate: 'border-l-primary bg-primary/[0.025]',
  lean:     'border-l-border',
}

const RESULT_ACCENT = {
  W: 'border-l-positive bg-positive/[0.10] ring-1 ring-positive/30',
  L: 'border-l-negative bg-negative/[0.08] ring-1 ring-negative/25',
  P: 'border-l-muted-foreground bg-muted/40',
}

// "AWAY @ HOME" -> { away, home }. Tolerates extra spaces / missing side.
function parseMatchup(m) {
  if (!m || typeof m !== 'string' || !m.includes('@')) return { away: null, home: null }
  const [a, h] = m.split('@').map(s => s.trim())
  return { away: a || null, home: h || null }
}


// Game date for a pick locked ahead of its game day ("Tue 10/27") -- NHL props lock the
// first time HR prices the game, usually the evening before (Austin 2026-10-26).
function upcomingDate(pick) {
  const d = pick?.date
  if (!d || pick.result) return null
  const todayET = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' })
  if (d <= todayET) return null
  const dt = new Date(`${d}T12:00:00Z`)
  return isNaN(dt.getTime()) ? null
    : dt.toLocaleDateString('en-US', { timeZone: 'America/New_York', weekday: 'short', month: 'numeric', day: 'numeric' })
}


export default function PropPickCard({ pick, sport }) {
  const [photoErr, setPhotoErr] = useState(false)
  if (!pick) return null
  const photo = !photoErr ? (pick.headshot || playerPhotoUrl(sport, pick.player_id)) : null
  const odds = pick.odds
  const oddsStr = odds != null ? `${odds > 0 ? '+' : ''}${odds}` : ''
  const conf = (pick.confidence || 'lean').toString().toLowerCase()
  const edgePct = Number(pick.edge || 0)
  const modelPct = Math.round((Number(pick.model_prob) || 0) * 100)
  const result = pick.result
  const settled = !!result
  const accent = settled
    ? (RESULT_ACCENT[result] || CONF_ACCENT.lean)
    : (CONF_ACCENT[conf] || CONF_ACCENT.lean)
  const { away, home } = parseMatchup(pick.matchup)
  const gameDay = upcomingDate(pick)

  return (
    <div
      className={cn(
        'group relative flex flex-col gap-3 rounded-xl border border-border border-l-4 bg-card p-4',
        'transition-all duration-150',
        'hover:border-border hover:bg-accent/40 hover:-translate-y-px hover:shadow-lg',
        accent,
      )}
    >
      {/* Matchup strip — team logos, so the card reads like a game card */}
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <MatchupStrip sport={sport} away={away} home={home} rawMatchup={pick.matchup} />
          {gameDay && (
            <span className="flex-shrink-0 rounded-full bg-warning/15 px-2 py-0.5 text-[10px] font-semibold text-warning">
              {gameDay}
            </span>
          )}
        </div>
        {settled
          ? <ResultBadge result={result} profit={pick.profit} />
          : <EdgeBadge edge={edgePct} confidence={conf} />}
      </div>

      {/* Player + pick */}
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0">
          {photo ? (
            <img
              src={photo}
              alt=""
              className="h-12 w-12 rounded-full object-cover bg-muted ring-1 ring-border"
              onError={() => setPhotoErr(true)}
            />
          ) : (
            <div className="h-12 w-12 rounded-full bg-muted ring-1 ring-border flex items-center justify-center text-xs font-bold text-muted-foreground">
              {(pick.player_name || '?').slice(0, 1)}
            </div>
          )}
        </div>

        <div className="flex-1 min-w-0">
          <div className="text-sm font-bold text-foreground truncate">
            {pick.player_name}
          </div>
          <div className="text-[11px] text-muted-foreground truncate">
            {humanizeBetType(pick.bet_type)}
          </div>
          <div className="mt-1.5 flex items-baseline gap-2 flex-wrap">
            <span className="text-base font-bold tracking-tight text-foreground">
              {pick.pick}
            </span>
            {oddsStr && (
              <span className="text-sm font-semibold tabular-nums text-muted-foreground">
                {oddsStr}
              </span>
            )}
            {pick.odds != null && (
              <CLVBadge entryOdds={pick.odds} closingOdds={pick.closing_odds} />
            )}
            {!settled && pick.stake && (
              <span
                title={`Suggested stake: ${pick.stake.why}`}
                className={cn('rounded-full px-2 py-0.5 text-[10px] font-bold',
                  pick.stake.units >= 1 ? 'bg-emerald-500/15 text-emerald-400'
                    : pick.stake.units >= 0.5 ? 'bg-amber-500/15 text-amber-400'
                    : 'bg-orange-500/15 text-orange-400')}
              >
                {pick.stake.label}
              </span>
            )}
          </div>
        </div>
      </div>

      <ModelBar modelPct={modelPct} />

      {pick.l10 && pick.l10.n > 0 && (
        <L10Spark l10={pick.l10} pick={pick} />
      )}
    </div>
  )
}


function MatchupStrip({ sport, away, home, rawMatchup }) {
  if (!away || !home) {
    return (
      <span className="min-w-0 truncate text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
        {rawMatchup || ''}
      </span>
    )
  }
  return (
    <div className="flex min-w-0 items-center gap-1.5 text-[11px] font-bold tracking-wide">
      <TeamChip sport={sport} abbr={away} />
      <span className="font-normal text-muted-foreground/50">@</span>
      <TeamChip sport={sport} abbr={home} />
    </div>
  )
}

function TeamChip({ sport, abbr }) {
  const logo = resolveTeamLogo(sport, abbr)
  return (
    <span className="inline-flex items-center gap-1">
      {logo ? (
        <img
          src={logo}
          alt=""
          className="h-5 w-5 flex-shrink-0 rounded-full object-contain bg-foreground/[0.06] ring-1 ring-border p-px"
        />
      ) : null}
      <span className="text-foreground/80">{abbr}</span>
    </span>
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
      <div className="relative h-2.5 w-full rounded-full bg-muted overflow-hidden ring-1 ring-border/50">
        <div
          className="absolute inset-y-0 left-0 rounded-full bg-positive transition-all"
          style={{ width: w }}
        />
      </div>
    </div>
  )
}
