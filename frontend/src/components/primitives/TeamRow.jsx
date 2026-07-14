/**
 * TeamRow — single team line inside a scorecard.
 *
 * Phase 2d-iv restyle: Tailwind tokens, larger team abbreviation
 * for at-a-glance scanning, optional record + streak as muted
 * trailing metadata, score lock-aligned right with bold winner
 * highlight when live/final.
 *
 * Layout:
 *   [logo] [ABBR] [Full Name … record · streak]                [score]
 */

import { cn } from '../../lib/utils'
import { resolveTeamLogo } from '../../lib/teamLogo'

export default function TeamRow({ team, isLive, isFinal, sport }) {
  const showScore = isLive || isFinal
  const logo = resolveTeamLogo(sport, team.abbreviation, team.logo)
  return (
    <div className="flex items-center gap-2.5 min-w-0">
      {logo ? (
        <img
          src={logo}
          alt=""
          className="h-8 w-8 flex-shrink-0 rounded-full object-contain bg-foreground/[0.06] ring-1 ring-border p-0.5"
        />
      ) : (
        // Initials fallback for leagues without a logo CDN (Euroleague,
        // RealGM-sourced international leagues). Keeps the row's visual
        // rhythm consistent across sports — no jagged "logos sometimes,
        // sometimes not" layout shift.
        <div
          className="h-8 w-8 flex-shrink-0 rounded-full bg-primary/15 ring-1 ring-border flex items-center justify-center text-[10px] font-bold text-primary"
          aria-hidden="true"
        >
          {(team.abbreviation || '?').slice(0, 3)}
        </div>
      )}
      {/* Skip the abbreviation text when the fallback badge already
          shows it — otherwise the row reads "[MAD] MAD Real Madrid"
          with the team code repeated twice. Logo path keeps the
          abbreviation since the image isn't a text label. */}
      {logo && (
        <span className="text-sm font-bold text-foreground tabular-nums">
          {team.abbreviation}
        </span>
      )}
      <span className="text-sm text-muted-foreground truncate hidden sm:inline">
        {team.name}
      </span>
      <span className="text-[11px] text-muted-foreground tabular-nums">
        {team.record}
      </span>
      {team.streak && (
        <span
          className={cn(
            'text-[10px] font-semibold tabular-nums px-1 rounded',
            team.streak.startsWith('W')
              ? 'bg-positive/10 text-positive'
              : team.streak.startsWith('L')
                ? 'bg-negative/10 text-negative'
                : 'bg-muted text-muted-foreground',
          )}
        >
          {team.streak}
        </span>
      )}
      {showScore && (
        <span
          className={cn(
            'ml-auto text-lg tabular-nums',
            team.winner ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground',
          )}
        >
          {team.score}
        </span>
      )}
    </div>
  )
}
