import { useEffect, useState } from 'react'
import PickHistory from './PickHistory'
import { clvPct } from '../lib/clv'
import { cachedGet, peek, invalidate } from '../lib/apiCache'

const SUMMARY_URL = '/nhl/p1-tracker/summary'
const HISTORY_URL = '/nhl/p1-tracker/history'

/**
 * P1PaperTracker — NHL P1 tab's Tracker content (Austin 2026-10-01 P1
 * tab/tracker consistency pass).
 *
 * Renders through the SAME PickHistory component + "paper" hero/table
 * styling as the Full Game tab's calibrated-shadow paper tracker, so
 * the P1 tracker reads as a sibling of the other paper trackers
 * instead of the generic Derivatives-tab look (DerivativeTracker,
 * which this view used to reuse via a pickFilter prop).
 *
 * Data source: the isolated nhl_p1_paper_picks table
 * (engine/derivative_tracker.py's record_p1_paper_picks), recorded one
 * row per game via the SAME two-tier fallback the P1 card itself uses
 * (best_pick_p1, else the single paper_picks_p1 lean — see BetsView.jsx
 * and _p1_lean_for_bet's docstring), so the tracker agrees with what
 * the cards show. Deliberately a separate table from
 * nhl_derivative_picks (shared by the generic Derivatives tab and
 * floor-gated at 4.0% edge in aggregate across every period + Team
 * Total): mixing the two would either keep dropping P1 picks that
 * lose the per-game "top derivative overall" contest to a same-night
 * Team Total/P2/P3 pick, or leak sub-4% P1 rows into the Derivatives
 * tab's edge-gated view. See record_p1_paper_picks's docstring.
 *
 * `/nhl/p1-tracker/summary` reuses engine.derivative_tracker.get_summary
 * (the SAME per-bet-type summary function the Derivatives tab calls),
 * just pointed at the P1 table via its `table=` override — its `_grand`
 * already carries total/wins/losses/pushes/pending/profit/win_pct/roi,
 * the exact fields PickHistory's `paper` prop wants. Only avg_clv /
 * clv_sample are missing (get_summary doesn't compute them), so they're
 * filled in here client-side from the history rows — the same pattern
 * TrackerView's recomputeSummary already uses for the NBA Q1 / MLB Full
 * filtered views.
 */
export default function P1PaperTracker({ api }) {
  const [summary, setSummary] = useState(() => peek(SUMMARY_URL) ?? null)
  const [history, setHistory] = useState(() => peek(HISTORY_URL) ?? [])
  const [loading, setLoading] = useState(peek(SUMMARY_URL) === undefined)

  const refresh = () => {
    setLoading(true)
    Promise.all([
      cachedGet(SUMMARY_URL).then(d => setSummary(d)).catch(() => {}),
      cachedGet(HISTORY_URL).then(d => setHistory(d)).catch(() => {}),
    ]).finally(() => setLoading(false))
  }

  useEffect(() => { refresh() }, [])  // eslint-disable-line react-hooks/exhaustive-deps

  const settleNow = () => {
    setLoading(true)
    api.post('/nhl/p1-tracker/settle')
      .then(() => {
        invalidate(SUMMARY_URL)
        invalidate(HISTORY_URL)
        refresh()
      })
      .catch(() => setLoading(false))
  }

  const grand = summary?._grand || {}
  let clvSum = 0, clvN = 0
  for (const r of history) {
    const c = clvPct(r.odds, r.closing_odds)
    if (c != null) { clvSum += c; clvN += 1 }
  }
  const paper = {
    ...grand,
    avg_clv: clvN > 0 ? Math.round(clvSum / clvN * 100) / 100 : null,
    clv_sample: clvN,
  }
  // Flags every row paper/unstaked for PicksTable's styling (sky
  // left-border) — every row in this table is paper by construction,
  // there is no real-money P1 book.
  const paperRows = history.map(r => ({ ...r, is_shadow: 1 }))

  return (
    <PickHistory
      summary={undefined}
      history={[]}
      paper={paper}
      paperRows={paperRows}
      loading={loading}
      onSettle={settleNow}
      sport="nhl"
    />
  )
}
