import { useEffect, useState, useCallback } from 'react'
import axios from 'axios'
import { cachedGet, invalidate } from '../lib/apiCache'
import TeamRow from './primitives/TeamRow'
import EdgeBadge from './primitives/EdgeBadge'
import { cn } from '../lib/utils'

/**
 * LiveBetsPanel — Phase 3d.
 *
 * Reads /api/{sport}/live-picks and renders a card per in-progress
 * game showing candidate live picks ranked by edge. Each pick has a
 * "Lock" button that POSTs to /live-picks/record so the snapshot
 * (period/clock/score/remaining_s) is frozen into the live tracker.
 *
 * Polling cadence per Phase 3 spec: 15s NBA, 30s NHL. apiCache's TTL
 * lines up so background polls don't double-fetch.
 *
 * POTD intentionally not surfaced here — live lines are too volatile
 * for a single locked daily pick (the spec calls this out).
 */

const POLL_MS = {
  nba: 15000,
  nhl: 30000,
  // Basketball-framework sports backed by the live worker — match
  // their _POLL_INTERVAL_S cadence in services/live_worker/main.py so
  // the FE doesn't poll faster than the backend refreshes state.
  wnba: 30000,
  ncaam: 30000,
  afl: 30000,
}


export default function LiveBetsPanel({ sport }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [lockingId, setLockingId] = useState(null)
  const [lockedKeys, setLockedKeys] = useState(new Set())
  // pendingByGame: {game_id: [{bet_type, pick, odds, edge_pct, ...}]}
  // Carries already-locked-but-unsettled picks so the card can render
  // them alongside fresh candidates from the live engine. Without
  // this, a user who locked 4 picks at halftime sees only 1 chip on
  // the card (the engine's current top candidate), even though the
  // tracker correctly shows all 4 pending.
  const [pendingByGame, setPendingByGame] = useState({})

  const fetchData = useCallback(async () => {
    try {
      const res = await cachedGet(`/${sport}/live-picks`,
                                   { ttlMs: POLL_MS[sport] || 15000 })
      setData(res)
      setError(null)
    } catch (e) {
      setError(e.message || 'live picks fetch failed')
    } finally {
      setLoading(false)
    }
  }, [sport])

  // Hydrate lockedKeys from the live-picks history so a tab switch +
  // remount doesn't blow away "this is already locked" state. Only
  // pending rows count — a settled row from earlier in the day
  // shouldn't grey out a fresh opportunity at the same direction.
  const hydrateLockedKeys = useCallback(async () => {
    try {
      const res = await cachedGet(`/${sport}/live-picks/history`,
                                   { ttlMs: 30_000 })
      const rows = res?.rows || []
      const keys = new Set()
      const byGame = {}
      for (const r of rows) {
        if (r.result) continue   // only pending picks block re-lock
        keys.add(`${r.game_id}|${r.bet_type}|${r.pick}`)
        // Bucket pending picks by game so the card can render them.
        // Reshape to the same field names live-picks ships
        // (game_id, bet_type, pick, odds, edge_pct). game_id is
        // critical — pickKey() includes it, so omitting would cause
        // the dedup against engine candidates to fail (PHI@BOS Q3
        // TOTAL Over 50.5 was rendering twice — once locked, once
        // unlocked — because the locked row's key was missing
        // game_id and didn't match the candidate row's key).
        const gid = String(r.game_id)
        if (!byGame[gid]) byGame[gid] = []
        byGame[gid].push({
          game_id: r.game_id,
          bet_type: r.bet_type,
          pick: r.pick,
          odds: r.odds,
          edge_pct: r.edge_pct ?? r.edge ?? null,
          model_prob: r.model_prob,
          is_locked_pending: true,
        })
      }
      setLockedKeys(keys)
      setPendingByGame(byGame)
    } catch (e) {
      // History hydration is best-effort; the backend dedup gate is
      // the real safety net.
    }
  }, [sport])

  useEffect(() => {
    setLoading(true)
    fetchData()
    hydrateLockedKeys()
    const interval = setInterval(fetchData, POLL_MS[sport] || 15000)
    return () => clearInterval(interval)
  }, [sport, fetchData, hydrateLockedKeys])

  const handleLock = useCallback(async (pick) => {
    const key = pickKey(pick)
    setLockingId(key)
    try {
      await axios.post(`/api/${sport}/live-picks/record`, pick)
      setLockedKeys(prev => new Set(prev).add(key))
      // Invalidate history cache so the tracker view picks up the new
      // row, and so a future tab-flip rehydrates with this lock
      // already in the set.
      invalidate(`/${sport}/live-picks/history`)
    } catch (e) {
      alert(`Lock failed: ${e.response?.data?.detail || e.message}`)
    } finally {
      setLockingId(null)
    }
  }, [sport])

  const games = data?.games || []
  const totalEdges = games.reduce((s, g) => s + (g.picks?.length || 0), 0)

  return (
    <div className="space-y-5 py-4">
      {/* Page header — matches PropsPanel / DerivativeTracker rhythm */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="flex items-center gap-2 text-xl font-semibold tracking-tight text-foreground">
            {sport.toUpperCase()} Live Picks
            <span className="inline-flex items-center gap-1.5 rounded-full bg-negative/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-negative">
              <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-negative" />
              Live
            </span>
          </h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Polling every {POLL_MS[sport] / 1000}s ·{' '}
            {games.length} live game{games.length !== 1 ? 's' : ''} ·{' '}
            {totalEdges} candidate edge{totalEdges !== 1 ? 's' : ''}
          </p>
        </div>
        {lockedKeys.size > 0 && (
          <span className="rounded-md bg-positive/15 px-2.5 py-1 text-[11px] font-semibold text-positive">
            {lockedKeys.size} locked this session
          </span>
        )}
      </div>

      {loading && !data && (
        <div className="flex items-center justify-center gap-3 rounded-lg border border-border bg-card py-12 text-sm text-muted-foreground">
          <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
          Polling live games…
        </div>
      )}

      {error && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
          {error}
        </div>
      )}

      {!loading && !error && games.length === 0 && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm text-muted-foreground">
          No live {sport.toUpperCase()} games right now. Picks will
          appear here as games tip off.
        </div>
      )}

      {games.map(game => (
        <LiveGameCard
          key={game.game_id}
          game={game}
          sport={sport}
          onLock={handleLock}
          lockingId={lockingId}
          lockedKeys={lockedKeys}
          pendingPicks={pendingByGame[String(game.game_id)] || []}
        />
      ))}
    </div>
  )
}


// Confidence tiers map edge magnitude to the same visual language
// as the prematch GameCard / EdgeBadge (strong = positive green,
// moderate = primary blue, lean = muted). Uses the same EdgeBadge
// component for the pick chip so the live cards read like siblings
// of the prematch slate.
function confidenceFromEdge(edgePct) {
  if (edgePct >= 15) return 'strong'
  if (edgePct >= 8)  return 'moderate'
  return 'lean'
}


// LIVE accent on the left border, matching GameCard's pattern. The
// negative-color pulse + ring mirrors the prematch live-game treatment
// so the user's eye finds them in the same place.
function LiveGameCard({ game, sport, onLock, lockingId, lockedKeys,
                       pendingPicks = [] }) {
  const status = game.state || {}
  // Merge engine candidates + already-locked-pending picks. Dedup by
  // pickKey so a pick that's both currently emitted AND already
  // locked appears once (preferring the locked-pending entry so the
  // chip renders in the locked state).
  const seen = new Set()
  const merged = []
  for (const p of pendingPicks) {
    const k = pickKey(p)
    if (seen.has(k)) continue
    seen.add(k)
    merged.push(p)
  }
  for (const p of (game.picks || [])) {
    const k = pickKey(p)
    if (seen.has(k)) continue
    seen.add(k)
    merged.push(p)
  }
  const picks = merged.sort((a, b) => (b.edge_pct ?? 0) - (a.edge_pct ?? 0))

  // Backend ships full home/away dicts on each game payload — the
  // shape matches what TeamRow expects after light renaming.
  const homeRaw = game.home || {}
  const awayRaw = game.away || {}
  const homeScore = homeRaw.score
  const awayScore = awayRaw.score
  const homeWinner = homeScore != null && awayScore != null && homeScore > awayScore
  const awayWinner = homeScore != null && awayScore != null && awayScore > homeScore
  const homeTeam = {
    abbreviation: homeRaw.abbr || (game.matchup || ' @ ').split(' @ ')[1] || '',
    name: homeRaw.name || '',
    score: homeScore,
    winner: homeWinner,
  }
  const awayTeam = {
    abbreviation: awayRaw.abbr || (game.matchup || ' @ ').split(' @ ')[0] || '',
    name: awayRaw.name || '',
    score: awayScore,
    winner: awayWinner,
  }

  return (
    <div
      className={cn(
        'flex flex-col gap-3 rounded-xl border border-border border-l-4 bg-card p-4',
        'border-l-negative ring-1 ring-negative/30',
        'transition-colors',
      )}
    >
      {/* Top status row — LIVE badge + period detail. Mirrors
          GameCard's inline status pill so prematch + live cards have
          consistent header rhythm. */}
      <div className="flex items-center justify-between gap-2">
        <span className="inline-flex w-max items-center gap-1.5 rounded-full bg-negative/20 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-negative">
          <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-negative" />
          Live
        </span>
        <span className="text-[11px] font-semibold tabular-nums text-foreground">
          {status.detail || 'In progress'}
        </span>
      </div>

      {/* Team rows — same component as prematch GameCard so logos,
          alignment, and score formatting all match. */}
      <div className="flex flex-col gap-1">
        <TeamRow team={awayTeam} isLive isFinal={false} sport={sport} />
        <TeamRow team={homeTeam} isLive isFinal={false} sport={sport} />
      </div>

      {/* Pick list — each candidate gets an EdgeBadge-style chip + a
          Lock button. Same visual language as prematch best-pick
          chips; the only addition is the per-row Lock action. */}
      {picks.length === 0 ? (
        <div className="rounded-md border border-dashed border-border bg-card/50 px-3 py-2.5 text-center text-xs italic text-muted-foreground">
          No edges above floor right now.
        </div>
      ) : (
        <div className="flex flex-col gap-1.5">
          {picks.map(pick => {
            const key = pickKey(pick)
            const isLocked = lockedKeys.has(key)
            const isLocking = lockingId === key
            return (
              <LivePickRow
                key={key}
                pick={pick}
                onLock={() => onLock(pick)}
                state={isLocked ? 'locked' : isLocking ? 'locking' : 'idle'}
              />
            )
          })}
        </div>
      )}
    </div>
  )
}


// A single pick row inside a LiveGameCard. Wraps EdgeBadge for the
// chip and adds a Lock button to the right.
//
// `pick.is_best` triggers a star prefix + slight visual emphasis so
// when 5-8 picks land at once (common at NBA halftime) the user can
// see the highest-conviction play at a glance. Conviction = prob² ×
// min(edge,12)/30, same formula POTD uses, computed in
// engine.live._picks.conviction_score.
function LivePickRow({ pick, onLock, state }) {
  const conf = confidenceFromEdge(pick.edge_pct)
  const badgePick = {
    type: pick.bet_type,
    pick: pick.pick,
    edge: pick.edge_pct,
    odds: pick.odds,
  }
  const disabled = state !== 'idle'
  const oddsLabel = pick.odds > 0 ? `+${pick.odds}` : `${pick.odds}`
  const isBest = !!pick.is_best

  return (
    <div className={cn(
      'flex items-center gap-2',
      isBest && 'relative rounded-md ring-1 ring-primary/40 bg-primary/5 px-1.5 py-1 -mx-1.5',
    )}>
      {isBest && (
        <span
          className="flex-shrink-0 text-sm leading-none text-primary"
          title="Highest-conviction pick in this game"
          aria-label="Best bet"
        >
          ★
        </span>
      )}
      <div className="min-w-0 flex-1">
        <EdgeBadge pick={badgePick} confidence={conf} />
      </div>
      <span className="flex-shrink-0 min-w-[3.5rem] text-right text-xs font-semibold tabular-nums text-muted-foreground">
        {oddsLabel}
      </span>
      <button
        type="button"
        onClick={onLock}
        disabled={disabled}
        className={cn(
          'flex-shrink-0 rounded-md px-3 py-1.5 text-[11px] font-semibold uppercase tracking-wider transition-colors',
          state === 'locked'
            ? 'bg-positive/20 text-positive cursor-default'
            : state === 'locking'
              ? 'bg-secondary text-muted-foreground cursor-wait'
              : 'bg-primary text-primary-foreground hover:bg-primary/80',
        )}
      >
        {state === 'locked' ? 'Locked' : state === 'locking' ? '…' : 'Lock'}
      </button>
    </div>
  )
}


function pickKey(pick) {
  // Stable identity for dedup: (game_id, bet_type, pick text). Odds
  // are NOT part of the key — HR moves the price every tick, but a
  // pick already in the tracker should stay greyed out as Locked
  // even if the live price ticks from -110 to -115. The backend's
  // record_live_pick dedup uses the same triple, so frontend +
  // backend agree on what counts as "the same opportunity".
  return `${pick.game_id}|${pick.bet_type}|${pick.pick}`
}
