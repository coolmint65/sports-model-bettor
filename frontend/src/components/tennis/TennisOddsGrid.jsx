/**
 * TennisOddsGrid — inline ML / Set / Total Games / Total Sets grid.
 *
 * Same chrome team-sport cards use (rounded-md border bg-background/30
 * with divide-y between rows). Extracted from TennisPanel 2026-07-03
 * so both TennisPanel and the shared MatchCard render odds identically.
 */

export default function TennisOddsGrid({ odds, match }) {
  const m = odds?.markets || {}
  const ml = m.ml
  const tg = m.total_games
  const ts = m.total_sets
  const ssAll = Array.isArray(m.set_spread) ? m.set_spread : []
  const ssP1 = ssAll.find(s => s.player === 'p1' && Math.abs(s.point) === 1.5)
  const ssP2 = ssAll.find(s => s.player === 'p2' && Math.abs(s.point) === 1.5)
  if (!ml && !tg && !ts && !(ssP1 && ssP2)) return null
  const fmt = (n) => n == null ? '-' : `${n > 0 ? '+' : ''}${Math.round(n)}`
  const ptFmt = (n) => n > 0 ? `+${n}` : `${n}`
  const lastName = (n) => (n || '').split(' ').slice(-1)[0]
  const p1Last = lastName(match.p1_name)
  const p2Last = lastName(match.p2_name)
  const Row = ({ label, p1, p2 }) => (
    <div className="grid grid-cols-[2.5rem_1fr_1fr] items-center gap-2 px-2.5 py-1.5">
      <span className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
        {label}
      </span>
      <span className="tabular-nums text-foreground/90 truncate">{p1}</span>
      <span className="tabular-nums text-foreground/90 truncate">{p2}</span>
    </div>
  )
  return (
    <div className="rounded-md border border-border/60 bg-background/30 divide-y divide-border/60 text-[11px]">
      {ml && (
        <Row label="ML"
             p1={`${p1Last} ${fmt(ml.p1_odds)}`}
             p2={`${p2Last} ${fmt(ml.p2_odds)}`} />
      )}
      {ssP1 && ssP2 && (
        <Row label="SET"
             p1={`${p1Last} ${ptFmt(ssP1.point)} (${fmt(ssP1.odds)})`}
             p2={`${p2Last} ${ptFmt(ssP2.point)} (${fmt(ssP2.odds)})`} />
      )}
      {tg && tg.line != null && (
        <Row label="O/U"
             p1={`o${tg.line} (${fmt(tg.over_odds)})`}
             p2={`u${tg.line} (${fmt(tg.under_odds)})`} />
      )}
      {ts && ts.line != null && (
        <Row label="Sets"
             p1={`o${ts.line} (${fmt(ts.over_odds)})`}
             p2={`u${ts.line} (${fmt(ts.under_odds)})`} />
      )}
    </div>
  )
}
