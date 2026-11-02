import { useMemo } from 'react'
import GameRail from './GameRail'
import { buildRailItems, railMatchup, shownPick } from '../../lib/rail'

// The best-first rail beside a sport panel's card grids -- ScoreboardShell's layout for the panels
// that render their own grids (Football / Basketball / Hockey / Soccer; Austin 2026-10-27 "add this
// 'best first' list to our other sports"). entries = the panel's adapted [{ game, bet }] rows.
export default function RailLayout({ entries, children }) {
  const items = useMemo(() => buildRailItems((entries || []).map(({ game, bet }) => ({
    id: game.id ?? game.game_id,
    matchup: railMatchup(game),
    pick: shownPick(game, bet),
    date: game.date,
    final: game.status?.state === 'post' || !!game.status?.completed,
  }))), [entries])
  return (
    <div className="flex gap-4 items-start">
      <GameRail items={items} />
      <div className="flex-1 min-w-0 space-y-3">{children}</div>
    </div>
  )
}

// A rail row's jump target; h-full so the card inside still fills its grid row.
export function CardAnchor({ id, children }) {
  return <div id={`sbcard-${id}`} className="scroll-mt-4 h-full">{children}</div>
}
