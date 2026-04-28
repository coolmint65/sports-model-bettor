import { useState, useMemo } from 'react'
import NBAScoreboard from './NBAScoreboard'
import PickOfDayHero from './PickOfDayHero'
import { cn } from '../lib/utils'

/**
 * NBA Bets-tab wrapper.
 *
 * Single Q1/Full toggle drives both the POTD card and every GameCard
 * underneath. Toggle defaults to Full Game (the higher-volume market
 * the model now ships). Switching to Q1 swaps each card's headline
 * pick to its highest-edge Q1_* candidate via the bet payload's
 * `best_pick_q1` / `best_pick_full` fields the backend ships per game.
 */
export default function NBABetsView({ games, loading, progress, onSelectGame, bestBets }) {
  const [view, setView] = useState('full')

  // Re-key each bet so its `best_pick` reflects the current view. The
  // GameCard primitive reads `bet.best_pick` and `bet.confidence`
  // unconditionally — we hand it a re-shaped bet rather than threading
  // a `view` prop through every GameCard child.
  //
  // No cross-view fallback: a Q1 pick must NEVER leak into Full view
  // (or vice versa). The toggle is a market-type filter — when Full
  // has no qualifying pick the card should render "No Pick" so the
  // user knows the model didn't find an edge in that market for that
  // game, not silently substitute a Q1 pick that looks like a Full
  // pick to the eye. Surfaced 2026-04-28 after the elimination-boost
  // halve dropped POR's Full picks below floor and Q1_SPREAD bled
  // through the toggle.
  const viewBets = useMemo(() => {
    if (!Array.isArray(bestBets)) return bestBets
    const key = view === 'full' ? 'best_pick_full' : 'best_pick_q1'
    return bestBets.map(b => {
      const chosen = b[key] || null
      return {
        ...b,
        best_pick: chosen,
        confidence: chosen?.confidence || 'lean',
      }
    })
  }, [bestBets, view])

  return (
    <>
      <div className="flex justify-end mb-2">
        <ViewToggle view={view} onChange={setView} />
      </div>
      <PickOfDayHero sport="nba" view={view} />
      <NBAScoreboard
        games={games}
        loading={loading}
        progress={progress}
        onSelectGame={onSelectGame}
        bestBets={viewBets}
      />
    </>
  )
}


function ViewToggle({ view, onChange }) {
  return (
    <div className="inline-flex rounded-md border border-border bg-card overflow-hidden shadow-sm">
      {[
        { id: 'full', label: 'Full Game' },
        { id: 'q1',   label: 'Q1' },
      ].map(opt => (
        <button
          key={opt.id}
          type="button"
          onClick={() => onChange(opt.id)}
          className={cn(
            'px-3 py-1.5 text-xs font-semibold tracking-wider uppercase transition-colors',
            view === opt.id
              ? 'bg-primary text-primary-foreground'
              : 'text-muted-foreground hover:bg-accent',
          )}
        >
          {opt.label}
        </button>
      ))}
    </div>
  )
}
