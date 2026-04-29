import { useMemo, useState } from 'react'
import MarketToggle from './primitives/MarketToggle'
import PickHistory from './PickHistory'
import DerivativeTracker from './DerivativeTracker'
import PropsPanel from './PropsPanel'
import FirstInningTracker from './FirstInningTracker'

/**
 * TrackerView — sport's Tracker tab with a market-type toggle that
 * mirrors BetsView. Each market renders its own dedicated tracker
 * view (core PickHistory / DerivativeTracker / PropsPanel / 1st INN
 * tracker / Q1-only PickHistory) so the user can see per-market
 * P/L without flipping between tabs.
 *
 * Toggle options are sport-aware:
 *   MLB  → Full Game · 1st Inn · Derivatives · Player Props
 *   NHL  → Full Game · Derivatives · Player Props
 *   NBA  → Full Game · Q1 · Derivatives · Player Props
 *
 * "Full Game" filters PickHistory's tables to non-Q1, non-derivative
 * bet types (NBA only — for MLB / NHL the core PickHistory IS full
 * game). "Q1" (NBA) filters to Q1_* bet_types. PickHistory's hero +
 * per-bet-type tiles are recomputed from the filtered subset so the
 * P/L the user sees matches the toggle.
 *
 * Derivatives / Props / 1st Inn keep their own existing components.
 */

const Q1_TYPES = new Set(['Q1_ML', 'Q1_SPREAD', 'Q1_TOTAL'])
const FULL_NBA_TYPES = new Set(['ML', 'SPREAD', 'TOTAL', 'ALT SPREAD', 'ALT TOTAL'])
// MLB Full Game = everything except 1st INN. Without this filter,
// 1st INN picks bleed into the Full toggle (user reported 2026-04-28)
// because they share the same `picks` table and ride the unfiltered
// PickHistory. 1st INN has its own toggle option that routes to the
// dedicated FirstInningTracker view.
const MLB_NON_FULL_TYPES = new Set(['1st INN'])

export default function TrackerView({ sport, api, trackerProps }) {
  const options = buildOptions(sport)
  const [market, setMarket] = useState(options[0]?.id || 'full')

  return (
    <>
      <div className="flex justify-end mb-2">
        <MarketToggle options={options} active={market} onChange={setMarket} />
      </div>
      <MarketContent sport={sport} market={market} api={api} trackerProps={trackerProps} />
    </>
  )
}


function buildOptions(sport) {
  const opts = [{ id: 'full', label: 'Full Game' }]
  if (sport === 'nba') opts.push({ id: 'q1', label: 'Q1' })
  if (sport === 'mlb') opts.push({ id: 'firstinning', label: '1st Inn' })
  opts.push({ id: 'derivatives', label: 'Derivatives' })
  opts.push({ id: 'props',       label: 'Player Props' })
  return opts
}


function MarketContent({ sport, market, api, trackerProps }) {
  if (market === 'derivatives') {
    return <DerivativeTracker sport={sport} api={api} />
  }
  if (market === 'props') {
    return <PropsPanel sport={sport} />
  }
  if (market === 'firstinning' && sport === 'mlb') {
    return <FirstInningTracker />
  }
  // 'full' or NBA 'q1' — both render PickHistory, possibly filtered.
  return (
    <FilteredPickHistory
      sport={sport}
      market={market}
      {...trackerProps}
    />
  )
}


function FilteredPickHistory({ sport, market, summary, history, loading, onRecord, onSettle }) {
  // Filter strategy by sport + market:
  //   NBA + 'q1'         → only Q1_*
  //   NBA + 'full'       → only ML/SPREAD/TOTAL/ALT*
  //   MLB + 'full'       → exclude 1st INN (which has its own toggle option)
  //   NHL + 'full'       → no filter (single market) — pass through
  const filtered = useMemo(() => {
    if (sport === 'nba') {
      const target = market === 'q1' ? Q1_TYPES : FULL_NBA_TYPES
      const filteredHistory = (history || []).filter(p => target.has(p.bet_type))
      const subSummary = recomputeSummary(filteredHistory, summary?.by_type || {}, target)
      return { summary: subSummary, history: filteredHistory }
    }
    if (sport === 'mlb' && market === 'full') {
      const filteredHistory = (history || []).filter(p => !MLB_NON_FULL_TYPES.has(p.bet_type))
      // Build a mirrored by_type dict that drops the excluded buckets so
      // the per-tile P/L doesn't include 1st INN.
      const trimmedByType = Object.fromEntries(
        Object.entries(summary?.by_type || {}).filter(([k]) => !MLB_NON_FULL_TYPES.has(k))
      )
      const subSummary = recomputeSummary(filteredHistory, trimmedByType, null)
      return { summary: subSummary, history: filteredHistory }
    }
    return { summary, history }
  }, [sport, market, summary, history])

  return (
    <PickHistory
      summary={filtered.summary}
      history={filtered.history}
      loading={loading}
      onRecord={onRecord}
      onSettle={onSettle}
    />
  )
}


/**
 * Recompute the PickHistory summary shape from a filtered slice of
 * history. The backend ships ONE summary per sport with all bet_types
 * mixed together — for NBA's Q1/Full split the user wants per-view
 * P/L, so we rebuild from the filtered rows. by_type tiles are pulled
 * from the source summary's by_type object (already keyed by bet_type)
 * so the per-tile numbers stay accurate; the overall hero recomputes
 * from the rows we kept.
 */
function recomputeSummary(rows, byType, allowedTypes /* Set | null */) {
  let wins = 0, losses = 0, pushes = 0, pending = 0, profit = 0
  let clvSum = 0, clvN = 0
  for (const r of rows) {
    if (r.result === 'W') wins++
    else if (r.result === 'L') losses++
    else if (r.result === 'P') pushes++
    else if (r.result == null) pending++
    profit += Number(r.profit || 0)
    if (r.odds != null && r.closing_odds != null) {
      const betImp = r.odds < 0
        ? Math.abs(r.odds) / (Math.abs(r.odds) + 100)
        : 100 / (r.odds + 100)
      const clsImp = r.closing_odds < 0
        ? Math.abs(r.closing_odds) / (Math.abs(r.closing_odds) + 100)
        : 100 / (r.closing_odds + 100)
      clvSum += (clsImp - betImp) * 100
      clvN += 1
    }
  }
  const settled = wins + losses
  // Keep only by_type entries for the allowed bet_types. When
  // allowedTypes is null the caller already pre-filtered byType, so
  // we pass it through unchanged.
  const filteredByType = allowedTypes
    ? Object.fromEntries(
        Object.entries(byType).filter(([k]) => allowedTypes.has(k))
      )
    : byType
  return {
    overall: {
      total: rows.length,
      wins, losses, pushes, pending,
      profit: Math.round(profit * 100) / 100,
      win_pct: settled > 0 ? Math.round(wins / settled * 1000) / 10 : 0,
      avg_clv: clvN > 0 ? Math.round(clvSum / clvN * 100) / 100 : null,
      clv_sample: clvN,
    },
    by_type: filteredByType,
  }
}
