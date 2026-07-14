import { useMemo, useState } from 'react'
import MarketToggle from './primitives/MarketToggle'
import PickHistory from './PickHistory'
import DerivativeTracker from './DerivativeTracker'
import PropsPanel from './PropsPanel'
import FirstInningTracker from './FirstInningTracker'
import LiveTrackerPanel from './LiveTrackerPanel'

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
// NHL P1 bet types — shared with P2/P3, period disambiguated by pick
// prefix. The P1 toggle filters DerivativeTracker rows to those whose
// pick text starts with "P1 " so per-period P/L is its own view.
const NHL_PERIOD_TYPES = new Set(['Period Total', 'Period BTS', 'Period DNB'])

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
  // NHL P1 — same shape as NBA Q1 but routed through the existing
  // derivative-tracker data path (Period Total / DNB / BTS rows whose
  // pick text begins "P1 "). Adds a per-period view without forking
  // the picks pipeline.
  if (sport === 'nhl') opts.push({ id: 'p1', label: 'P1' })
  if (sport === 'mlb') opts.push({ id: 'firstinning', label: '1st Inn' })
  // Live tracker (Phase 3d) — NBA + NHL only.
  if (sport === 'nba' || sport === 'nhl') {
    opts.push({ id: 'live', label: 'Live' })
  }
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
  if (market === 'live' && (sport === 'nba' || sport === 'nhl')) {
    return <LiveTrackerPanel sport={sport} />
  }
  // NHL 'p1' reuses the DerivativeTracker component but pre-filters
  // its rows to Period* bet types whose pick text starts with "P1 ".
  // Implemented inside DerivativeTracker via the `pickFilter` prop so
  // the shared summary/POTD hero recompute from the filtered subset.
  if (market === 'p1' && sport === 'nhl') {
    return (
      <DerivativeTracker
        sport={sport}
        api={api}
        pickFilter={(row) => (
          NHL_PERIOD_TYPES.has(row.bet_type)
          && typeof row.pick === 'string'
          && row.pick.trim().startsWith('P1 ')
        )}
        title="P1 Period Picks"
      />
    )
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
      sport={sport}
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
  // Hero + tiles both pull from the FULL cumulative `by_type` summary
  // shipped by /api/tracker/summary, not the visible /api/tracker/history
  // slice (LIMIT 50). Per the user's preference: P/L numbers should
  // reflect every settled pick on file, not just what scrolled into view.
  //
  // Filter strategy: when `allowedTypes` is given (NBA q1/full split),
  // restrict to those bet_types; otherwise keep every type in byType.
  // For MLB the caller pre-filters byType (drops 1st INN), so allowedTypes
  // is null here and we trust the caller's keys.
  const filteredByType = allowedTypes
    ? Object.fromEntries(
        Object.entries(byType || {}).filter(([k]) => allowedTypes.has(k))
      )
    : (byType || {})

  // Aggregate the kept by_type cells back into the overall hero. F5
  // is a synthetic tile that already aggregates F5 ML/OU/RL on the
  // backend (engine/tracker/_summary.py), so skip its component keys
  // to avoid double-counting when both F5 and its parts are present.
  let wins = 0, losses = 0, pushes = 0, pending = 0, profit = 0, total = 0
  const hasAggF5 = Boolean(filteredByType['F5']
                            && filteredByType['F5'].total > 0)
  for (const [key, v] of Object.entries(filteredByType)) {
    if (hasAggF5 && (key === 'F5 ML' || key === 'F5 O/U' || key === 'F5 RL')) {
      continue
    }
    total += v.total || 0
    wins += v.wins || 0
    losses += v.losses || 0
    pushes += v.pushes || 0
    pending += v.pending || 0
    profit += Number(v.profit || 0)
  }
  const settled = wins + losses

  // CLV stays computed from the visible rows because /api/tracker/summary
  // ships avg_clv across ALL settled picks — we still want a CLV figure
  // for non-NBA paths that don't filter at all. Fall back to the
  // upstream avg_clv when the visible slice is too thin.
  let clvSum = 0, clvN = 0
  for (const r of rows) {
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

  return {
    overall: {
      total, wins, losses, pushes, pending,
      profit: Math.round(profit * 100) / 100,
      win_pct: settled > 0 ? Math.round(wins / settled * 1000) / 10 : 0,
      avg_clv: clvN > 0 ? Math.round(clvSum / clvN * 100) / 100 : null,
      clv_sample: clvN,
    },
    by_type: filteredByType,
  }
}
