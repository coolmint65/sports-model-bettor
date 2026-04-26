/**
 * ScoreboardShell
 * ──────────────────────────────────────────────────────────────
 * Sport-agnostic scoreboard page shell: loading state, empty
 * state, active/final partition, edge-count header, and the two
 * game grids. The sport-specific bits flow in via props:
 *
 *   title          "Today's Games" | "NHL Games" | "NBA Games"
 *   loadingLabel   "Loading today's slate..." etc.
 *   emptyPrimary   "No NBA games scheduled today." etc.
 *   emptySub       Secondary message in the empty state.
 *   edgeLabel      "plays with edge" | "Q1 plays with edge"
 *   renderCard     ({ game, bet, onClick }) => ReactNode
 *   renderFinal    ({ game, bet, onClick }) => ReactNode (optional;
 *                  defaults to the shared <FinalRow>)
 *
 * Replaces three near-identical exported scoreboards in
 * Scoreboard.jsx / NHLScoreboard.jsx / NBAScoreboard.jsx.
 */

import { useCallback, useMemo } from 'react'
import FinalRow from './FinalRow'

export default function ScoreboardShell({
  games,
  loading,
  progress,
  onSelectGame,
  bestBets,
  title,
  loadingLabel,
  emptyPrimary,
  emptySub,
  edgeLabel = 'plays with edge',
  renderCard,
  renderFinal,
}) {
  // Derive betMap, partition, and sort in a single memo keyed on games
  // + bestBets. Previously ran on every parent re-render (every time
  // the 5-minute scoreboard refresh fired or any sibling state ticked),
  // mutating activeGames in-place and re-filtering for the edge count.
  const { betMap, activeGames, finalGames, edgeCount } = useMemo(() => {
    const bm = {}
    if (bestBets) {
      for (const b of bestBets) bm[b.game_id] = b
    }
    const active = []
    const final = []
    for (const g of games || []) {
      if (g.status?.state === 'post' || g.status?.completed) final.push(g)
      else active.push(g)
    }
    active.sort((a, b) => {
      const aEdge = bm[a.id]?.best_pick?.edge ?? -99
      const bEdge = bm[b.id]?.best_pick?.edge ?? -99
      return bEdge - aEdge
    })
    let ec = 0
    for (const g of active) {
      const c = bm[g.id]?.confidence
      if (c === 'strong' || c === 'moderate') ec++
    }
    return { betMap: bm, activeGames: active, finalGames: final, edgeCount: ec }
  }, [games, bestBets])

  // Stable handler per game id. The previous `game => () => ...`
  // closure allocated a new function for every card on every render,
  // defeating any shallow-prop equality the card might use.
  const handleClick = useCallback(
    game => () => { if (onSelectGame) onSelectGame(game) },
    [onSelectGame],
  )

  if (loading) {
    return <LoadingState progress={progress} fallback={loadingLabel} />
  }

  if (!games || games.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">{emptyPrimary}</div>
        <div className="mt-1 text-xs text-muted-foreground">{emptySub}</div>
      </div>
    )
  }

  return (
    <div className="space-y-3">
      <header className="flex items-baseline justify-between gap-3 pt-2">
        <h2 className="text-lg font-semibold tracking-tight text-foreground">
          {title}
          <span className="ml-2 text-sm font-normal tabular-nums text-muted-foreground">
            ({games.length})
          </span>
        </h2>
        {edgeCount > 0 && (
          <span className="rounded-full bg-positive/15 px-2 py-0.5 text-[11px] font-semibold tabular-nums text-positive">
            {edgeCount} {edgeLabel}
          </span>
        )}
      </header>

      {activeGames.length > 0 && (
        <>
          {finalGames.length > 0 && (
            <SectionHeader
              label={activeGames.some(g => g.status?.state === 'in') ? 'Live & Upcoming' : 'Upcoming'}
              count={activeGames.length}
            />
          )}
          <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {activeGames.map(game =>
              renderCard({
                game,
                bet: betMap[game.id],
                onClick: handleClick(game),
                key: game.id,
              })
            )}
          </div>
        </>
      )}

      {finalGames.length > 0 && (
        <>
          <SectionHeader label="Final" count={finalGames.length} />
          <div className="grid grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-3">
            {finalGames.map(game => {
              const props = {
                game,
                bet: betMap[game.id],
                onClick: handleClick(game),
                key: game.id,
              }
              return renderFinal
                ? renderFinal(props)
                : <FinalRow {...props} />
            })}
          </div>
        </>
      )}
    </div>
  )
}

function SectionHeader({ label, count }) {
  return (
    <div className="flex items-center gap-2 pt-2">
      <span className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
        {label}
      </span>
      <span className="text-[10px] tabular-nums text-muted-foreground/70">
        ({count})
      </span>
      <span className="flex-1 border-t border-border ml-1" aria-hidden="true" />
    </div>
  )
}

function LoadingState({ progress, fallback }) {
  const total = progress?.total || 0
  const rawDone = progress?.done || 0
  const done = total > 0 ? Math.min(rawDone, total) : rawDone
  const pct = total ? Math.min(100, Math.round((done / total) * 100)) : null
  const phase = progress?.phase
  let label = fallback
  if (phase === 'predicting' && total > 0) {
    label = `Computing predictions: ${done}/${total} games (${pct}%)`
  } else if (phase === 'building') {
    label = 'Assembling picks…'
  }
  return (
    <div className="flex items-center justify-center gap-3 py-16 text-sm text-muted-foreground">
      <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
      {label}
    </div>
  )
}
