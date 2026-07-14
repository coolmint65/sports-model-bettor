/**
 * LeagueButton — child button inside an expanded sidebar group.
 *
 * Every top-level sport (Basketball / Hockey / Soccer / Motorsports /
 * Baseball) renders these under its expandable parent. Shared shell:
 * active-state highlight, off-season dim + "off" tag, per-league game
 * count badge, hover / focus rings.
 */
import { cn } from '../../lib/utils'


export default function LeagueButton({ league, isActive, onSelect }) {
  const count = league.game_count_today || 0
  return (
    <button
      onClick={() => onSelect(league.key)}
      className={cn(
        'flex w-full items-center justify-between rounded-md px-2.5 py-1.5 text-[12.5px] transition-colors',
        'focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
        isActive
          ? 'bg-primary text-primary-foreground font-semibold'
          : 'text-foreground/85 hover:bg-accent hover:text-accent-foreground',
        !league.in_season && !isActive && 'opacity-55',
      )}
      title={league.in_season
        ? league.display_name
        : `${league.display_name} (off-season)`}
    >
      <span className="truncate text-left">{league.display_name}</span>
      <span className="flex items-center gap-1 shrink-0 ml-1">
        {count > 0 && (
          <span className={cn(
            'rounded-full px-1.5 py-0 text-[10px] font-semibold tabular-nums',
            isActive
              ? 'bg-primary-foreground/20 text-primary-foreground'
              : 'bg-muted text-muted-foreground',
          )}>
            {count}
          </span>
        )}
        {!league.in_season && (
          <span className="text-[9px] uppercase tracking-wider opacity-70">
            off
          </span>
        )}
      </span>
    </button>
  )
}
