import { memo } from 'react'
import { jumpToCard } from '../../lib/rail'

// Ranked side rail (Austin 2026-10-17): the slate's cards listed best-first, click a row to jump to
// its card. Ranking lives in lib/rail.js buildRailItems (graduated picks, then leans by their
// market's track record, then nothing -- Austin 2026-10-27).
// The day's W-L across every pick on the rail (Austin 2026-10-31: "a W/L record for the day ... easier to see
// if we were green or red"), split graduated / lean when both are present.
function dayRecord(items) {
  const r = { W: 0, L: 0, P: 0, open: 0, grad: [0, 0], lean: [0, 0] }
  for (const it of items) {
    if (!it.pick) continue
    const res = String(it.result || '').toUpperCase()
    if (res === 'W' || res === 'L') {
      r[res] += 1
      const b = it.tag === 'grad' ? r.grad : r.lean
      b[res === 'W' ? 0 : 1] += 1
    } else if (res === 'P' || res === 'PUSH') r.P += 1
    else r.open += 1
  }
  return r
}

function GameRailImpl({ items, onJump = jumpToCard }) {
  if (!items || items.length === 0) return null
  const rec = dayRecord(items)
  const settled = rec.W + rec.L
  const tone = !settled ? 'text-muted-foreground' : rec.W > rec.L ? 'text-positive' : rec.W < rec.L ? 'text-negative' : 'text-foreground'
  const split = rec.grad[0] + rec.grad[1] > 0 && rec.lean[0] + rec.lean[1] > 0
  return (
    <aside className="hidden lg:block w-52 shrink-0">
      <div className="sticky top-4 rounded-lg border border-border bg-card p-2">
        <div className="px-1 pb-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Picks · best first
        </div>
        <div className="mx-1 mb-1.5 rounded border border-border/60 px-2 py-1">
          <div className="flex items-baseline justify-between gap-2">
            <span className="text-[10px] uppercase tracking-wider text-muted-foreground">Today</span>
            <span className={`text-sm font-bold tabular-nums ${tone}`}>
              {rec.W}-{rec.L}{rec.P ? `-${rec.P}` : ''}
              {settled > 0 && <span className="ml-1 text-[10px] font-medium">({Math.round((100 * rec.W) / settled)}%)</span>}
            </span>
          </div>
          <div className="flex justify-between gap-2 text-[9px] tabular-nums text-muted-foreground">
            <span>{split ? `grad ${rec.grad[0]}-${rec.grad[1]} · lean ${rec.lean[0]}-${rec.lean[1]}` : ''}</span>
            <span>{rec.open ? `${rec.open} to go` : settled ? 'all settled' : ''}</span>
          </div>
        </div>
        <div className="space-y-0.5 overflow-y-auto" style={{ maxHeight: 'calc(100vh - 7rem)' }}>
          {items.map((it, i) => (
            <button
              key={it.id}
              onClick={() => onJump(it.id)}
              className="group w-full rounded px-2 py-1.5 text-left transition-colors hover:bg-accent focus:outline-none focus:bg-accent"
            >
              <div className="flex items-center justify-between gap-2">
                <span className="truncate text-xs font-medium text-foreground">
                  <span className="mr-1 tabular-nums text-muted-foreground/60">{i + 1}.</span>
                  {it.matchup}
                </span>
                {it.edge != null && (
                  <span className={`shrink-0 text-[11px] tabular-nums ${it.edge > 0 ? 'text-positive' : 'text-muted-foreground'}`}>
                    {it.edge > 0 ? '+' : ''}{it.edge}
                  </span>
                )}
              </div>
              <div className="flex items-center justify-between gap-2">
                <span className="flex min-w-0 items-center gap-1">
                  {it.tag && (
                    <span className={`shrink-0 rounded-sm px-1 text-[8px] font-bold uppercase tracking-wider ${it.tag === 'grad' ? 'bg-emerald-500/25 text-emerald-200' : 'bg-amber-500/25 text-amber-200'}`}>
                      {it.tag === 'grad' ? 'grad' : 'lean'}
                    </span>
                  )}
                  <span className="truncate text-[10px] text-muted-foreground">{it.pick || 'no lean'}</span>
                </span>
                {it.final && (
                  <span className={`shrink-0 text-[9px] font-semibold uppercase ${it.result === 'W' ? 'text-positive' : it.result === 'L' ? 'text-negative' : 'text-muted-foreground'}`}>
                    {it.result === 'W' ? 'Won' : it.result === 'L' ? 'Lost' : 'Final'}
                  </span>
                )}
              </div>
            </button>
          ))}
        </div>
      </div>
    </aside>
  )
}
export default memo(GameRailImpl)
