import { cn } from '../../lib/utils'

/**
 * ScoreDisplay — the projected-score block at the top of every
 * sport's game-detail Projected Outcome card.
 *
 * Two team columns + a centered "-" separator. Winner side bolds
 * larger text in the foreground color; loser side stays muted. Shared
 * across MLB / NHL / NBA so the visual language of "model expects
 * this score" is identical.
 *
 * Props:
 *   home, away  - { name, abbreviation?, record? }
 *   homeScore, awayScore - the projected score values (number or string)
 *   homeWins   - bool — pass derived from caller's expected_score
 */
export default function ScoreDisplay({ home, away, homeScore, awayScore, homeWins }) {
  return (
    <div className="flex items-center gap-2">
      <Team team={home} score={homeScore} winner={homeWins} align="right" />
      <span className="text-2xl font-bold text-muted-foreground/40 px-2">-</span>
      <Team team={away} score={awayScore} winner={!homeWins} align="left" />
    </div>
  )
}

function Team({ team, score, winner, align }) {
  return (
    <div className={cn(
      'flex flex-1 items-center gap-3 min-w-0',
      align === 'right' && 'flex-row-reverse text-right',
    )}>
      <div className={cn(
        'text-4xl tabular-nums',
        winner ? 'font-bold text-foreground' : 'font-semibold text-muted-foreground',
      )}>
        {score}
      </div>
      <div className="min-w-0 flex-1">
        <div className={cn(
          'text-base font-semibold truncate',
          winner ? 'text-foreground' : 'text-muted-foreground',
        )}>
          {team.name}
        </div>
        {team.record && (
          <div className="text-xs text-muted-foreground tabular-nums">
            {team.record}
          </div>
        )}
      </div>
    </div>
  )
}
