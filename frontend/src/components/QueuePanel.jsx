/**
 * QueuePanel — cross-sport auto-eligible bet queue.
 *
 * Deliberately simple: no knobs, no threshold inputs, just "here are
 * today's picks that belong in cells the tracker has proven earn
 * money." Thresholds are hardcoded (n>=50 settled picks, stake-
 * weighted ROI >= 5%, 5u daily bankroll cap, $100/u display size).
 *
 * Two tabs:
 *   - Today   → the live queue for today+tomorrow
 *   - Tracker → historical performance for queue-eligible cells so
 *              the user can see the queue's earned track record
 *
 * Each queue row renders the sport's own card component — tennis
 * picks get the full TennisPanel MatchCard, team-sport picks get the
 * shared GameCard. useSportData fetches each sport's scheduled data
 * on demand so the queue reuses the sport's own cache/pipeline
 * instead of maintaining a parallel card layout.
 */
import { useMemo, useState } from 'react'
import PanelShell from './primitives/PanelShell'
import PickCard from './queue/PickCard'
import TrackerView from './queue/TrackerView'
import PlacementsView from './queue/PlacementsView'
import LiveFireToggle from './queue/LiveFireToggle'
import { useSportData } from './queue/useSportData'


// Hardcoded — no user-facing knobs. If we ever want to tune, edit here.
// unit_dollars=1 matches the auto-placer's authorized envelope (HR's
// $1 minimum stake). If we raise the unit size later, update
// engine/queue_placer/_config.py in lockstep so the daily/weekly caps
// scale with it.
const CFG = {
  min_n: 50,
  min_roi: 5,
  max_units: 10,
  unit_dollars: 1,
}

const TABS = [
  { id: 'today',      label: 'Today' },
  { id: 'tracker',    label: 'Tracker' },
  { id: 'placements', label: 'Placements' },
]


// Reject picks whose sport-native match/game has already tipped off.
// Two signals: (a) an explicit status field (tennis: 'in' / 'post',
// team sports: 'in-progress' / 'final'), (b) a start_time earlier
// than the current wall-clock. Falls through to KEEP when the sport
// data hasn't loaded yet — better to briefly show a soon-to-be-live
// pick than to hide a legit pending pick because the sport fetcher
// hasn't returned.
function hasStarted(pick, sportData) {
  if (!pick.event_id) return false
  const native = sportData[`${pick.sport}:${pick.event_id}`]
  if (!native) return false
  const status = String(native.status || '').toLowerCase()
  if (status === 'in' || status === 'post'
      || status === 'in-progress' || status === 'final'
      || status === 'live' || status === 'complete') return true
  const start = native.start_time || native.date
  if (start) {
    const t = Date.parse(start)
    if (!isNaN(t) && t < Date.now()) return true
  }
  return false
}


export default function QueuePanel({
  api, queue, tracker, loading, onRefresh,
}) {
  const [view, setView] = useState('today')
  // Data is hoisted to App level so switching away from this tab and
  // coming back doesn't remount empty — the picks are already there.
  // The 2-min poll runs at the App level too, so we don't need our
  // own timer here. `onRefresh` triggers a fresh cross-cutting fetch
  // (button click, etc.); it's a no-op if the poll just fired.
  const load = onRefresh

  const rawPicks = queue?.picks || []
  const trimmed = queue?.trimmed || []
  const sportData = useSportData(rawPicks, api)
  // Filter out live/in-progress and completed events. The queue's
  // whole point is manual placement on HR — once a game starts, the
  // pre-match odds are gone (or shifted) so surfacing those picks
  // wastes the user's time. Backend already reads only pre-match
  // picks tables (live_picks_<sport> isn't in _SOURCES); this filter
  // is the second half — drop any pick whose sport-native status
  // shows the game has already started.
  const picks = useMemo(
    () => rawPicks.filter(p => !hasStarted(p, sportData)),
    [rawPicks, sportData]
  )
  const totalDollars = useMemo(
    () => picks.reduce((s, p) => s + (p.stake_dollars || 0), 0),
    [picks]
  )

  const contextChips = useMemo(() => {
    const chips = []
    if (view === 'today' && picks.length > 0) {
      chips.push({
        icon: '🎯',
        text: `${picks.length} pick${picks.length === 1 ? '' : 's'} · $${totalDollars.toFixed(0)}`,
        key: 'n',
      })
    }
    if (view === 'tracker' && tracker?.summary?.n) {
      const s = tracker.summary
      chips.push({
        icon: '📈',
        text: `${s.n} settled · ${s.wr}% WR`,
        tone: s.roi > 0 ? 'positive' : 'warning',
        key: 'ns',
      })
    }
    const gen = view === 'today' ? queue?.generated_at : tracker?.generated_at
    if (gen) {
      chips.push({
        text: `Refreshed ${new Date(gen).toLocaleTimeString(
          [], { hour: 'numeric', minute: '2-digit' })}`,
        key: 'ts',
      })
    }
    return chips
  }, [view, picks.length, totalDollars, queue, tracker])

  return (
    <PanelShell
      title="Bet Queue"
      subtitle="High-confidence picks in markets that have earned it"
      contextChips={contextChips}
      tabs={TABS}
      active={view}
      onTabChange={setView}
      headerExtras={
        <div className="flex items-center gap-2">
          <LiveFireToggle api={api} />
          <button
            onClick={load}
            disabled={loading}
            className="rounded-md border border-border bg-card px-3 py-1.5 text-xs font-medium text-foreground hover:bg-accent disabled:opacity-50"
          >
            {loading ? 'Loading…' : 'Refresh'}
          </button>
        </div>
      }
    >
      {view === 'today' && (
        <TodayView
          picks={picks}
          trimmed={trimmed}
          loading={loading && !queue}
          sportData={sportData}
        />
      )}
      {view === 'tracker' && (
        <TrackerView tracker={tracker} loading={loading && !tracker} />
      )}
      {view === 'placements' && (
        <PlacementsView api={api} />
      )}
    </PanelShell>
  )
}


// ── Today view ──────────────────────────────────────────────

function TodayView({ picks, trimmed, loading, sportData }) {
  return (
    <>
      <PicksList picks={picks} loading={loading} sportData={sportData} />
      {trimmed.length > 0 && (
        <div className="rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-xs text-warning">
          {trimmed.length} more pick{trimmed.length === 1 ? '' : 's'} skipped
          today to stay under the {CFG.max_units}u daily bankroll cap.
        </div>
      )}
      <p className="text-[11px] text-muted-foreground leading-relaxed">
        Only picks in cells with at least {CFG.min_n} settled bets and a
        proven positive stake-weighted ROI ({CFG.min_roi}%+) enter the
        queue. Placement stays manual — copy the pick and paste into
        your Hard Rock bet slip.
      </p>
    </>
  )
}


function PicksList({ picks, loading, sportData }) {
  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card/50 px-4 py-6 text-sm text-muted-foreground">
        Loading queue…
      </div>
    )
  }
  if (!picks.length) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/40 px-6 py-12 text-center space-y-2">
        <div className="text-sm font-semibold text-foreground">
          No high-confidence picks right now.
        </div>
        <div className="text-xs text-muted-foreground max-w-md mx-auto">
          The queue only surfaces picks in markets our tracker has
          proven are earning money. Check back when the model finds
          something in a validated cell.
        </div>
      </div>
    )
  }
  // Grid layout — same shape TennisPanel + team-sport slates use, so
  // three picks slot into a single row on wide screens instead of
  // stacking vertically. Cards stretch to equal height so a strong
  // tennis card and a slim MLB row don't look mismatched side-by-side.
  return (
    <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3 items-stretch">
      {picks.map(p => {
        const key = p.event_id ? `${p.sport}:${p.event_id}` : null
        const native = key ? sportData[key] : null
        return (
          <PickCard
            key={`${p.sport}:${p.matchup}:${p.bet_type}:${p.pick}`}
            pick={p}
            native={native}
          />
        )
      })}
    </div>
  )
}
