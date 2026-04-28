import { useState } from 'react'
import MarketToggle from './primitives/MarketToggle'
import PickOfDayHero from './PickOfDayHero'
import FirstInningPicks from './FirstInningPicks'
import Scoreboard from './Scoreboard'
import NHLScoreboard from './NHLScoreboard'
import NBAScoreboard from './NBAScoreboard'
import PropsPanel from './PropsPanel'
import DerivativeTracker from './DerivativeTracker'
import FirstInningTracker from './FirstInningTracker'

/**
 * BetsView — the consolidated Bets surface for a sport.
 *
 * Replaces the old top-level tab fan-out (Bets / Props / Derivatives /
 * 1st Inn) with a single Bets tab that holds a market-type toggle.
 * The user requested this shape on 2026-04-28: "instead of all the
 * picks being separate in their own tabs, we have it all in one place
 * with the toggles."
 *
 * Toggle options are sport-aware:
 *   MLB  → Full Game · 1st Inn · Derivatives · Player Props
 *   NHL  → Full Game · Derivatives · Player Props
 *   NBA  → Full Game · Q1 · Derivatives · Player Props
 *
 * The Q1/Full split that used to live inside NBABetsView is promoted
 * to the market-type toggle here — no more nested toggle inside the
 * scoreboard. NBA Q1 and Full are first-class peers next to
 * Derivatives and Props.
 *
 * Each market renders the existing component for that surface so the
 * refactor is structural, not a rewrite.
 *
 * Props:
 *   sport     — 'mlb' | 'nhl' | 'nba'
 *   data      — sport state passed down from App.jsx (games, bestBets,
 *               loading, progress, onSelectGame, etc.)
 *   api       — axios instance for DerivativeTracker
 */
export default function BetsView({ sport, data, api }) {
  const options = buildOptions(sport, data)
  const [market, setMarket] = useState(options[0]?.id || 'full')

  return (
    <>
      <div className="flex justify-end mb-2">
        <MarketToggle options={options} active={market} onChange={setMarket} />
      </div>
      <MarketContent sport={sport} market={market} data={data} api={api} />
    </>
  )
}


function buildOptions(sport, data) {
  const opts = [{ id: 'full', label: 'Full Game' }]
  if (sport === 'nba') opts.push({ id: 'q1', label: 'Q1' })
  if (sport === 'mlb') opts.push({ id: 'firstinning', label: '1st Inn' })
  opts.push({ id: 'derivatives', label: 'Derivatives' })
  opts.push({ id: 'props',       label: 'Player Props' })
  return opts
}


function MarketContent({ sport, market, data, api }) {
  if (market === 'derivatives') {
    return <DerivativeTracker sport={sport} api={api} />
  }
  if (market === 'props') {
    return <PropsPanel sport={sport} />
  }
  if (market === 'firstinning' && sport === 'mlb') {
    return <FirstInningTracker />
  }

  // 'full' or NBA 'q1' — main slate scoreboard.
  if (sport === 'mlb') {
    return (
      <>
        <PickOfDayHero sport="mlb" />
        <FirstInningPicks bestBets={data.bestBets} />
        <Scoreboard
          games={data.games}
          loading={data.loading}
          progress={data.progress}
          onSelectGame={data.onSelectGame}
          bestBets={data.bestBets}
        />
      </>
    )
  }
  if (sport === 'nhl') {
    return (
      <>
        <PickOfDayHero sport="nhl" />
        <NHLScoreboard
          games={data.games}
          loading={data.loading}
          progress={data.progress}
          onSelectGame={data.onSelectGame}
          bestBets={data.bestBets}
        />
      </>
    )
  }
  if (sport === 'nba') {
    // For NBA, the active market is either 'full' or 'q1'. Re-key each
    // bet's best_pick to the right view (no cross-view fallback —
    // same rule as the old NBABetsView introduced 2026-04-28).
    const view = market === 'q1' ? 'q1' : 'full'
    const key = view === 'q1' ? 'best_pick_q1' : 'best_pick_full'
    const bets = Array.isArray(data.bestBets)
      ? data.bestBets.map(b => {
          const chosen = b[key] || null
          return { ...b, best_pick: chosen, confidence: chosen?.confidence || 'lean' }
        })
      : data.bestBets
    return (
      <>
        <PickOfDayHero sport="nba" view={view} />
        <NBAScoreboard
          games={data.games}
          loading={data.loading}
          progress={data.progress}
          onSelectGame={data.onSelectGame}
          bestBets={bets}
        />
      </>
    )
  }
  return null
}
