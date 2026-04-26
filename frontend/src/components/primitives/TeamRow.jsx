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

export default function TeamRow({ team, isLive, isFinal }) {
  const showScore = isLive || isFinal
  return (
    <div className="flex items-center gap-2.5 min-w-0">
      {team.logo && (
        <img
          src={team.logo}
          alt=""
          className="h-7 w-7 flex-shrink-0 object-contain"
        />
      )}
      <span className="text-sm font-bold text-foreground tabular-nums">
        {team.abbreviation}
      </span>
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
