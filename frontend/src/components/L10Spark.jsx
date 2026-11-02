/**
 * L10Spark — last-10 recent form. One bar per game (height ∝ value),
 * GREEN when that game would have hit THIS pick, RED when it missed,
 * with the pick's line drawn as a dashed threshold. Reads pick.l10
 * (backend-supplied { values[], hits, n }) + the pick's line/side.
 * Shared by PropPickCard and PotdHero. `height` is a Tailwind class
 * for the bar track (default h-9; the POTD hero passes a taller one).
 */
import { cn } from '../lib/utils'

export default function L10Spark({ l10, pick, height = 'h-9' }) {
  if (!l10) return null
  const vals = Array.isArray(l10.values) ? l10.values.map(Number).filter(Number.isFinite) : []
  if (!vals.length) return null
  const txt = `${pick.side || ''} ${pick.pick || ''}`.toLowerCase()
  const isOver = /over/.test(txt)
  const isUnder = /under/.test(txt)
  // Yes/no markets (Anytime Goal etc.) carry no numeric Over/Under line —
  // a game "hits" when the player recorded the event (value >= 1).
  const isYesNo = !isOver && !isUnder
  const line = isYesNo ? 0.5 : Number(pick.line)
  const hasLine = Number.isFinite(line)
  const hitOf = (v) => (isYesNo ? v >= 1 : isOver ? v > line : v < line)
  const max = Math.max(...vals, hasLine ? line : 0, 1)
  const lineBottom = hasLine ? Math.min(97, Math.max(3, (line / max) * 100)) : null
  return (
    <div className="space-y-1">
      <div className="flex items-center justify-between text-[10px] uppercase tracking-wider text-muted-foreground">
        <span>Last {l10.n}</span>
        <span className="tabular-nums font-bold text-foreground">
          {l10.hits}/{l10.n}<span className="font-normal text-muted-foreground"> hit</span>
        </span>
      </div>
      <div className={cn('relative flex items-end gap-0.5 rounded bg-muted/30 px-1 pt-1 ring-1 ring-border/40', height)}>
        {vals.map((v, i) => {
          const h = Math.max(8, (v / max) * 100)
          const hit = hasLine && hitOf(v)
          return (
            <div
              key={i}
              title={String(v)}
              className={cn('min-w-[3px] flex-1 rounded-sm transition-colors', hit ? 'bg-positive' : 'bg-negative')}
              style={{ height: `${h}%` }}
            />
          )
        })}
        {lineBottom != null && (
          <div className="pointer-events-none absolute left-1 right-1 border-t border-dashed border-foreground/40" style={{ bottom: `${lineBottom}%` }} />
        )}
      </div>
    </div>
  )
}
