import { memo } from 'react'

// Compact paper-shadow P/L panel for a single scope (full / 1st-inn /
// derivative). Same sky "paper" styling as PickHistory's lean panel, but
// standalone so it can sit under each tab's real tracker. Visible as soon as
// picks are being TRACKED (Austin 2026-09-19) — shows "Tracking N · 0 settled"
// before anything grades, instead of hiding until the first settle.
function cn(...xs) { return xs.filter(Boolean).join(' ') }

function PaperShadowPanelImpl({ summary, rows, title, pending }) {
  const s = summary || {}
  const pend = pending || 0
  const settled = s.total || 0
  const hasRows = Array.isArray(rows) && rows.length > 0
  if (!settled && !pend && !hasRows) return null
  return (
    <section className="rounded-lg border border-sky-500/30 bg-sky-500/[0.04] p-5">
      <div className="flex flex-col gap-1">
        <div className="flex items-center gap-2">
          <span className="rounded-sm bg-sky-500/25 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-sky-200">Paper</span>
          <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">{title || 'Paper shadow'}</div>
        </div>
        {settled > 0 ? (
          <div className={cn('text-3xl font-bold tabular-nums', s.profit > 0 ? 'text-positive' : s.profit < 0 ? 'text-negative' : 'text-foreground')}>
            {s.profit > 0 ? '+' : ''}${s.profit}
            <span className="ml-2 text-sm font-medium text-muted-foreground">{s.roi > 0 ? '+' : ''}{s.roi}% ROI</span>
          </div>
        ) : (
          <div className="text-2xl font-bold tabular-nums text-muted-foreground">
            Tracking {pend}
            <span className="ml-2 text-sm font-medium text-muted-foreground/70">&middot; 0 settled</span>
          </div>
        )}
        <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          {settled > 0 && (
            <>
              <span className="tabular-nums">{s.wins}-{s.losses}</span>
              <span className="text-border">&middot;</span>
              <span className="tabular-nums">{s.win_pct}% WR</span>
              <span className="text-border">&middot;</span>
              <span className="tabular-nums">{settled} settled</span>
            </>
          )}
          {pend > 0 && (
            <>
              {settled > 0 && <span className="text-border">&middot;</span>}
              <span className="tabular-nums">{pend} pending</span>
            </>
          )}
          {s.pushes > 0 && (<><span className="text-border">&middot;</span><span className="tabular-nums">{s.pushes} push</span></>)}
        </div>
      </div>
      {hasRows && (
        <div className="mt-3 border-t border-sky-500/15 pt-3 flex flex-col gap-1">
          {rows.slice(0, 6).map((r, i) => (
            <div key={i} className="flex items-center justify-between gap-2 text-[11px]">
              <span className="text-muted-foreground truncate">{r.matchup}</span>
              <span className="tabular-nums whitespace-nowrap">{r.bet_type} {r.pick}{r.line != null ? ' ' + r.line : ''}</span>
              <span className={cn('tabular-nums font-semibold w-16 text-right', r.result === 'W' ? 'text-positive' : r.result === 'L' ? 'text-negative' : 'text-muted-foreground')}>
                {r.result}{r.profit != null ? ' ' + (r.profit > 0 ? '+' : '') + r.profit : ''}
              </span>
            </div>
          ))}
        </div>
      )}
    </section>
  )
}
const PaperShadowPanel = memo(PaperShadowPanelImpl)
export default PaperShadowPanel
