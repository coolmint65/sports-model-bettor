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
  if (loading) {
    return <LoadingState progress={progress} fallback={loadingLabel} />
  }

  if (!games || games.length === 0) {
    return (
      <div className="no-games">
        <p>{emptyPrimary}</p>
        <p className="sub">{emptySub}</p>
      </div>
    )
  }

  const betMap = {}
  if (bestBets) {
    for (const b of bestBets) betMap[b.game_id] = b
  }

  const activeGames = []
  const finalGames = []
  for (const g of games) {
    if (g.status?.state === 'post' || g.status?.completed) finalGames.push(g)
    else activeGames.push(g)
  }

  activeGames.sort((a, b) => {
    const aEdge = betMap[a.id]?.best_pick?.edge ?? -99
    const bEdge = betMap[b.id]?.best_pick?.edge ?? -99
    return bEdge - aEdge
  })

  const edgeCount = activeGames.filter(g => {
    const c = betMap[g.id]?.confidence
    return c === 'strong' || c === 'moderate'
  }).length

  const handleClick = game => () => onSelectGame && onSelectGame(game)

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        {title} ({games.length})
        {edgeCount > 0 && <span className="edge-count">{edgeCount} {edgeLabel}</span>}
      </h2>

      {activeGames.length > 0 && (
        <>
          {finalGames.length > 0 && (
            <div className="games-section-header">
              {activeGames.some(g => g.status?.state === 'in') ? 'Live & Upcoming' : 'Upcoming'} ({activeGames.length})
            </div>
          )}
          <div className="games-feature-grid">
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
          <div className="games-section-header">Final ({finalGames.length})</div>
          <div className="games-finals-grid">
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

function LoadingState({ progress, fallback }) {
  const total = progress?.total || 0
  const rawDone = progress?.done || 0
  // Backend single-flight keeps done <= total, but clamp defensively.
  const done = total > 0 ? Math.min(rawDone, total) : rawDone
  const pct = total ? Math.min(100, Math.round((done / total) * 100)) : null
  const phase = progress?.phase
  let label = fallback
  if (phase === 'predicting' && total > 0) {
    label = `Computing predictions: ${done}/${total} games (${pct}%)`
  } else if (phase === 'building') {
    label = 'Assembling picks...'
  }
  return (
    <div className="loading">
      <div className="spinner" />
      <p>{label}</p>
    </div>
  )
}
