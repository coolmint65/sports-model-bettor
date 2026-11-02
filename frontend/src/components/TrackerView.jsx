import { useMemo, useState } from 'react'
import { clvPct } from '../lib/clv'
import MarketToggle from './primitives/MarketToggle'
import PickHistory from './PickHistory'
import DerivativeTracker from './DerivativeTracker'
import PropsPanel from './PropsPanel'
import FirstInningTracker from './FirstInningTracker'
import P1PaperTracker from './P1PaperTracker'

/**
 * TrackerView — sport's Tracker tab with a market-type toggle that
 * mirrors BetsView. Each market renders its own dedicated tracker
 * view (core PickHistory / DerivativeTracker / PropsPanel / 1st INN
 * tracker / Q1-only PickHistory / P1PaperTracker) so the user can see
 * per-market P/L without flipping between tabs.
 *
 * Toggle options are sport-aware:
 *   MLB  → Full Game · 1st Inn · Derivatives · Player Props
 *   NHL  → Full Game · P1 · Derivatives · Player Props
 *   NBA  → Full Game · Q1 · Derivatives · Player Props
 *
 * "Full Game" filters PickHistory's tables to non-Q1, non-derivative
 * bet types (NBA only — for MLB / NHL the core PickHistory IS full
 * game). "Q1" (NBA) filters to Q1_* bet_types. PickHistory's hero +
 * per-bet-type tiles are recomputed from the filtered subset so the
 * P/L the user sees matches the toggle.
 *
 * Derivatives / Props / 1st Inn keep their own existing components.
 * NHL P1 also gets its own component (P1PaperTracker, Austin
 * 2026-10-01) — it renders through PickHistory itself rather than
 * DerivativeTracker, so the P1 tab reads as a sibling of the Full
 * Game paper tracker (same hero / table / "paper" labeling) instead
 * of the generic Derivatives-tab look. See P1PaperTracker's docstring.
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
  const opts = [{ id: 'full', label: 'Picks' }]
  // NHL P1 — a per-period paper tracker, sibling in look to the Full
  // Game paper section. Backed by its own isolated table
  // (nhl_p1_paper_picks) rather than forking the main picks pipeline
  // or sharing the generic derivative table — see P1PaperTracker.
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
  // NHL 'p1' renders through PickHistory (via P1PaperTracker) instead
  // of DerivativeTracker, so it reads as a sibling of the Full Game
  // paper tracker — same hero/table/"paper" labeling — rather than the
  // generic Derivatives-tab look. Self-contained: fetches its own
  // /nhl/p1-tracker/* endpoints, backed by the isolated
  // nhl_p1_paper_picks table (Austin 2026-10-01 P1 tab/tracker
  // consistency pass; previously reused DerivativeTracker + a
  // pickFilter over the shared, 4.0-floor-gated derivative table,
  // which silently dropped P1 leans the cards were already showing).
  if (market === 'p1' && sport === 'nhl') {
    return <P1PaperTracker api={api} />
  }
  // 'full' or NBA 'q1' — both render PickHistory, possibly filtered.
  // Merged tracker (Austin 2026-10-12): one view, not separate P1/Q1/Deriv/1st-Inn
  // tabs. Real full-game P/L up top; the PAPER markets render BELOW as their own
  // existing, correctly-segmented trackers so real + paper P/L never blend.
  const _Lbl = ({ t }) => (
    <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">{t}</div>
  )
  return (
    <div className="space-y-6">
      <FilteredPickHistory sport={sport} market="full" {...trackerProps} />
      {sport === 'nba' && (
        <div className="space-y-2"><_Lbl t="Q1 · paper" /><FilteredPickHistory sport={sport} market="q1" {...trackerProps} /></div>
      )}
      {sport === 'nhl' && (
        <div className="space-y-2"><_Lbl t="Period 1 · paper" /><P1PaperTracker api={api} /></div>
      )}
      {sport === 'mlb' && (
        <div className="space-y-2"><_Lbl t="First Inning · paper" /><FirstInningTracker /></div>
      )}
      <div className="space-y-2"><_Lbl t="Derivatives · paper" /><DerivativeTracker sport={sport} api={api} /></div>
    </div>
  )
}


function FilteredPickHistory({ sport, market, summary, history, loading, onRecord, onSettle, paper, paperRows, unstaked }) {
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
      paper={paper}
      paperRows={paperRows}
      loading={loading}
      onRecord={onRecord}
      onSettle={onSettle}
      sport={sport}
      unstaked={unstaked}
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
    const _c = clvPct(r.odds, r.closing_odds)
    if (_c != null) { clvSum += _c; clvN += 1 }
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
