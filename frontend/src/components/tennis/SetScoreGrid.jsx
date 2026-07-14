/**
 * SetScoreGrid — per-player linescore grid for live/final tennis.
 *
 * Extracted from TennisPanel 2026-07-03 so both TennisPanel and the
 * shared MatchCard can render the same set-by-set grid.
 */
import { cn } from '../../lib/utils'

function _parseSetScore(score) {
  if (!score) return []
  const sets = []
  const re = /(\d+)-(\d+)(?:\(([^)]+)\))?/g
  let m
  while ((m = re.exec(score)) !== null) {
    sets.push({
      p1: parseInt(m[1], 10),
      p2: parseInt(m[2], 10),
      tiebreak: m[3] || null,
    })
  }
  return sets
}

export default function SetScoreGrid({ score, p1Name, p2Name, winner }) {
  const sets = _parseSetScore(score)
  if (!sets.length) return null
  const lastName = (n) => (n || '').split(' ').slice(-1)[0]
  let p1Wins = 0, p2Wins = 0
  for (const s of sets) {
    if (s.p1 > s.p2) p1Wins++
    else if (s.p2 > s.p1) p2Wins++
  }
  const p1IsWinner = winner === 'p1' || (winner == null && p1Wins > p2Wins)
  return (
    <div className="rounded-md border border-border/60 bg-background/30 px-3 py-2 text-sm font-sans tabular-nums">
      <div className="grid items-center" style={{
        gridTemplateColumns: `1fr repeat(${sets.length}, 2.25rem)`,
      }}>
        <span />
        {sets.map((_, i) => (
          <span key={i} className="text-center text-[9px] uppercase tracking-wider text-muted-foreground/70 font-sans">
            S{i + 1}
          </span>
        ))}
      </div>
      <div className="grid items-center" style={{
        gridTemplateColumns: `1fr repeat(${sets.length}, 2.25rem)`,
      }}>
        <span className={cn(
          'text-xs font-sans truncate pr-2',
          p1IsWinner ? 'font-bold text-foreground' : 'text-muted-foreground',
        )}>
          {lastName(p1Name)}
        </span>
        {sets.map((s, i) => {
          const won = s.p1 > s.p2
          return (
            <span key={i} className={cn('text-center',
              won ? 'font-bold text-foreground' : 'text-muted-foreground/70',
            )}>
              {s.p1}{s.tiebreak && won && <sup className="text-[9px] text-muted-foreground/70">{s.tiebreak}</sup>}
            </span>
          )
        })}
      </div>
      <div className="grid items-center" style={{
        gridTemplateColumns: `1fr repeat(${sets.length}, 2.25rem)`,
      }}>
        <span className={cn(
          'text-xs font-sans truncate pr-2',
          !p1IsWinner ? 'font-bold text-foreground' : 'text-muted-foreground',
        )}>
          {lastName(p2Name)}
        </span>
        {sets.map((s, i) => {
          const won = s.p2 > s.p1
          return (
            <span key={i} className={cn('text-center',
              won ? 'font-bold text-foreground' : 'text-muted-foreground/70',
            )}>
              {s.p2}{s.tiebreak && won && <sup className="text-[9px] text-muted-foreground/70">{s.tiebreak}</sup>}
            </span>
          )
        })}
      </div>
    </div>
  )
}
