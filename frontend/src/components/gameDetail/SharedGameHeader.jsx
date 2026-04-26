import { useMemo } from 'react'
import { ArrowLeft } from 'lucide-react'
import LineMovementBadge from './LineMovementBadge'
import { cn } from '../../lib/utils'

/**
 * SharedGameHeader — top section of a game-detail page.
 *
 * Phase 2-cleanup restyle: Tailwind tokens. Layout still:
 *   - back button
 *   - LIVE / FINAL badge (inline so it doesn't crash with score)
 *   - team matchup (logos, names, records, scores)
 *   - venue / broadcast / start-time
 *   - odds chips + line movement
 *
 * Sport-specific extras (pitcher/goalie cards, lineup confirmations,
 * rest pills) plug in via the `matchupExtras` slot — see
 * NHLGameDetail / GameDetail / NBAGameDetail wrappers.
 */
export default function SharedGameHeader({ game, onBack, matchupExtras }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const lm = game.line_movement

  const gameDateLabel = useMemo(
    () => new Date(game.date).toLocaleString([], {
      weekday: 'short', month: 'short', day: 'numeric',
      hour: 'numeric', minute: '2-digit',
    }),
    [game.date],
  )
  const lmSignificant = lm && lm.significance && lm.significance !== 'none'

  return (
    <header className="space-y-4">
      <button
        onClick={onBack}
        className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors focus:outline-none focus-visible:ring-2 focus-visible:ring-ring rounded-sm"
      >
        <ArrowLeft className="h-3.5 w-3.5" aria-hidden="true" />
        Back to games
      </button>

      <div className="rounded-xl border border-border bg-card p-5 space-y-4">
        {(isLive || isFinal) && (
          <div className="flex">
            {isLive && (
              <span className="inline-flex items-center gap-1 rounded-full bg-negative/20 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-negative">
                ● Live
              </span>
            )}
            {isFinal && (
              <span className="inline-flex items-center rounded-full bg-muted px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
                Final
              </span>
            )}
          </div>
        )}

        <div className="flex items-center gap-4">
          <DetailTeam team={away} isLive={isLive} isFinal={isFinal} align="right" />
          <span className="text-2xl font-bold text-muted-foreground/40">@</span>
          <DetailTeam team={home} isLive={isLive} isFinal={isFinal} align="left" />
        </div>

        {matchupExtras}

        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          {game.venue && <span>{game.venue}</span>}
          {game.broadcast && <span>{game.broadcast}</span>}
          {status.state === 'pre' && <span className="tabular-nums">{gameDateLabel}</span>}
          {isLive && (
            <span className="font-semibold text-negative tabular-nums">
              {status.detail}
            </span>
          )}
        </div>

        {game.odds && (
          <div className="flex flex-wrap items-center gap-2">
            {game.odds.home_ml && (
              <OddsChip>
                {home.abbreviation}{' '}
                <span className="tabular-nums">
                  {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml}
                </span>
              </OddsChip>
            )}
            {game.odds.away_ml && (
              <OddsChip>
                {away.abbreviation}{' '}
                <span className="tabular-nums">
                  {game.odds.away_ml > 0 ? '+' : ''}{game.odds.away_ml}
                </span>
              </OddsChip>
            )}
            {game.odds.over_under && (
              <OddsChip>O/U <span className="tabular-nums">{game.odds.over_under}</span></OddsChip>
            )}
            {lmSignificant && <LineMovementBadge lm={lm} home={home} away={away} />}
          </div>
        )}
      </div>
    </header>
  )
}

function DetailTeam({ team, isLive, isFinal, align }) {
  const showScore = isLive || isFinal
  return (
    <div className={cn(
      'flex flex-1 items-center gap-3 min-w-0',
      align === 'right' && 'flex-row-reverse text-right',
    )}>
      {team.logo && (
        <img
          src={team.logo}
          alt=""
          className="h-12 w-12 flex-shrink-0 object-contain"
        />
      )}
      <div className="min-w-0">
        <div className="text-base font-bold text-foreground truncate">
          {team.name}
        </div>
        <div className="text-xs text-muted-foreground tabular-nums">
          {team.record}
        </div>
      </div>
      {showScore && (
        <div className={cn(
          'text-3xl tabular-nums ml-auto',
          align === 'right' && 'mr-auto ml-0',
          team.winner ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground',
        )}>
          {team.score}
        </div>
      )}
    </div>
  )
}

function OddsChip({ children }) {
  return (
    <span className="inline-flex items-center rounded-md border border-border bg-background px-2 py-0.5 text-[11px] font-semibold text-foreground">
      {children}
    </span>
  )
}
