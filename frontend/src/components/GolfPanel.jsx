/**
 * GolfPanel v2 — built on PanelShell for parity with NBA/NHL/F1.
 *
 * Outright structure (one tournament, N-player field) — leaderboard
 * table is the focal element. v2 changes vs v1:
 *   - PanelShell standardizes the header / tabs / status pill.
 *   - Pick-of-the-Day hero replaces the flat pick strip.
 *   - Leaderboard rows highlight when the player has a live pick.
 *   - Tournament context strip (course, round, cut line when live).
 *   - Shared PickHistory primitive for the Tracker tab (NBA/NHL/MLB parity).
 *   - Country flags rendered on hero + leaderboard rows (F1-style).
 *   - Calibration tab placeholder (per-market hit rate once picks
 *     start settling).
 *   - skill_n badge surfaces thin-history players in the leaderboard.
 */
import { useEffect, useMemo, useState } from 'react'
import { useSlate } from '../lib/useSlate'
import PanelShell, {
  EmptyState, ErrorBox,
} from './primitives/PanelShell'
import PickHistory from './PickHistory'
import { cn } from '../lib/utils'

const TOUR_OPTIONS = [
  { key: 'pga',       label: 'PGA Tour' },
  { key: 'lpga',      label: 'LPGA Tour' },
  { key: 'kornferry', label: 'Korn Ferry' },
]


export default function GolfPanel({ api }) {
  const [tour, setTour] = useState('pga')
  const [view, setView] = useState('slate')
  const [tracker, setTracker] = useState(null)
  const [err, setErr] = useState(null)

  const slateParams = useMemo(() => ({}), [])
  const { data: slate, loading } =
    useSlate(`/golf/${tour}/today`, slateParams, api)

  const [trackerLoading, setTrackerLoading] = useState(false)
  const refreshTracker = () => {
    setTrackerLoading(true)
    api.get(`/golf/${tour}/tracker/history`)
      .then(r => setTracker(r.data))
      .catch(() => setTracker({ rows: [], summary: { overall: {}, by_type: {} } }))
      .finally(() => setTrackerLoading(false))
  }
  useEffect(() => {
    if (view !== 'tracker' || tracker) return
    refreshTracker()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [view, tour, tracker])

  // Header keeps a single status badge — tournament details live in
  // the TournamentContextCard inside the slate body so the top bar
  // stays uncluttered.
  const t = slate?.tournament
  const statusBadge = useMemo(() => {
    if (!t) return null
    if (t.status === 'in') return { label: 'live', tone: 'positive' }
    if (t.status === 'final') return { label: 'final', tone: 'pending_calibration' }
    return { label: 'beta', tone: 'beta' }
  }, [t])

  return (
    <PanelShell
      title={slate?.display_name || 'Golf'}
      statusBadge={statusBadge}
      headerExtras={
        <TourSelector value={tour} onChange={setTour} />
      }
      tabs={[
        { id: 'slate',       label: t?.status === 'in' ? 'Live' : 'Slate' },
        { id: 'tracker',     label: 'Tracker' },
        { id: 'calibration', label: 'Calibration' },
      ]}
      active={view}
      onTabChange={setView}
    >
      {view === 'slate' && (
        <SlateView slate={slate} loading={loading} err={err} />
      )}
      {view === 'tracker' && (
        <PickHistory
          summary={tracker?.summary}
          history={tracker?.rows || []}
          loading={trackerLoading}
          onRecord={() => api.post(`/golf/${tour}/tracker/record`).finally(refreshTracker)}
          onSettle={() => api.post(`/golf/${tour}/tracker/settle`).finally(refreshTracker)}
        />
      )}
      {view === 'calibration' && (
        <CalibrationView tracker={tracker} tour={tour} api={api} />
      )}
    </PanelShell>
  )
}


function TourSelector({ value, onChange }) {
  return (
    <select
      value={value}
      onChange={e => onChange(e.target.value)}
      className="text-xs bg-card border border-border rounded px-2 py-1 focus:outline-none focus-visible:ring-2 focus-visible:ring-ring"
    >
      {TOUR_OPTIONS.map(o => (
        <option key={o.key} value={o.key}>{o.label}</option>
      ))}
    </select>
  )
}


function formatTournamentDates(startIso, endIso) {
  if (!startIso) return ''
  const s = new Date(startIso)
  const e = endIso ? new Date(endIso) : null
  const sFmt = s.toLocaleDateString(undefined, { month: 'short', day: 'numeric' })
  if (!e) return sFmt
  const eFmt = e.toLocaleDateString(undefined, { day: 'numeric' })
  return `${sFmt}–${eFmt}`
}


// ── Slate view ────────────────────────────────────────────────

function SlateView({ slate, loading, err }) {
  const [showFullField, setShowFullField] = useState(false)
  // All hooks must run unconditionally — keep them above any early
  // returns. The .players passthrough handles slate=null cleanly.
  const players = slate?.players || []
  const playerById = useMemo(() => {
    const m = new Map()
    for (const p of players) m.set(p.id, p)
    return m
  }, [players])
  if (loading) return <EmptyState title="Loading slate…" />
  if (err) return <ErrorBox message={err} />
  if (!slate?.tournament) {
    return <EmptyState title="No upcoming tournament" icon="⛳"
                         message="Slate will populate when ESPN ships the next event." />
  }
  const t = slate.tournament
  const picks = slate.picks || []
  const pickPlayerIds = new Set(picks.map(p => p.player_id))
  // Hero (primary) + up to 3 secondary rows. Backend caps at 4 per
  // tournament; this .slice is the defensive ceiling.
  const [primaryPick, ...secondaryPicks] = picks.slice(0, 4)

  return (
    <div className="space-y-4">
      <TournamentContextCard tournament={t} />
      {primaryPick ? (
        <PickOfDayHero
          pick={primaryPick}
          secondaries={secondaryPicks}
          playerById={playerById}
        />
      ) : (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-4 py-3 text-xs text-muted-foreground text-center">
          No edges clear the per-market floor right now. Check back as
          odds move toward Round 1.
        </div>
      )}
      <ModelBoard
        players={players}
        pickPlayerIds={pickPlayerIds}
        showFullField={showFullField}
        onToggle={() => setShowFullField(v => !v)}
      />
    </div>
  )
}


function TournamentContextCard({ tournament }) {
  const t = tournament
  if (!t) return null
  // Compact info row that replaces the dense header chips. Course +
  // dates + major badge live in one card so the eye lands on them
  // without scanning the title bar.
  return (
    <div className="rounded-lg border border-border bg-card/60 p-4 flex flex-wrap items-center gap-x-6 gap-y-2">
      <div className="flex-1 min-w-0">
        <div className="text-lg font-bold text-foreground flex items-center gap-2">
          {t.name}
          {t.is_major && (
            <span className="text-[10px] font-bold uppercase tracking-wider
                              px-1.5 py-0.5 rounded bg-amber-500/20 text-amber-200">
              Major
            </span>
          )}
        </div>
        {t.course && (
          <div className="text-sm text-muted-foreground mt-0.5">
            ⛳ {t.course}
          </div>
        )}
      </div>
      <div className="text-right text-sm">
        <div className="text-foreground tabular-nums">
          {formatTournamentDates(t.start_date, t.end_date)}
        </div>
        <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
          {t.status === 'in' ? 'Live now'
            : t.status === 'final' ? 'Final'
            : 'Tee off ' + formatRelativeDate(t.start_date)}
        </div>
      </div>
    </div>
  )
}


function formatRelativeDate(iso) {
  if (!iso) return ''
  try {
    const target = new Date(iso)
    const now = new Date()
    const diffDays = Math.round((target - now) / 86400000)
    if (diffDays <= 0) return 'today'
    if (diffDays === 1) return 'tomorrow'
    if (diffDays < 7) return `in ${diffDays}d`
    return target.toLocaleDateString(undefined, { weekday: 'short', month: 'short', day: 'numeric' })
  } catch { return '' }
}


function PickOfDayHero({ pick, secondaries, playerById }) {
  const primaryPlayer = playerById?.get?.(pick.player_id) || null
  return (
    <div className="space-y-2">
      <PrimaryPickCard pick={pick} player={primaryPlayer} />
      {(secondaries || []).map(s => (
        <SecondaryPickRow
          key={`${s.bet_type}-${s.player_id}`}
          pick={s}
          player={playerById?.get?.(s.player_id) || null}
        />
      ))}
    </div>
  )
}


function FlagBadge({ player, size = 'sm' }) {
  // Small ISO flag rendered next to a player's name. Falls back to a
  // muted dot when ESPN's flag_url is missing so layout doesn't jump
  // between players with and without flags.
  if (!player) return null
  const url = player.flag_url || player.flagUrl
  const dims = size === 'lg' ? 'w-5 h-3.5' : 'w-4 h-3'
  if (!url) return null
  return (
    <img
      src={url}
      alt={player.country || ''}
      title={player.country || ''}
      className={cn(dims, 'inline-block align-middle rounded-sm object-cover shadow-sm shadow-black/20')}
      onError={e => { e.currentTarget.style.display = 'none' }}
    />
  )
}


function PrimaryPickCard({ pick, player }) {
  const headshot = player?.headshot_url
  const edge = Number(pick.edge ?? 0)
  const modelProb = Number(pick.model_prob ?? 0)
  const impliedProb = Number(pick.implied_prob ?? 0)
  const odds = Number(pick.odds ?? 0)
  return (
    <div className="rounded-xl border border-positive/30 bg-gradient-to-br from-positive/10 via-card to-card p-5 flex items-center gap-4">
      <div className="flex-shrink-0">
        {headshot ? (
          <img src={headshot} alt="" className="w-20 h-20 rounded-full bg-muted object-cover"
            onError={e => { e.currentTarget.style.display = 'none' }} />
        ) : (
          <div className="w-20 h-20 rounded-full bg-positive/10 flex items-center justify-center text-3xl">
            ⛳
          </div>
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-[10px] font-bold uppercase tracking-wider text-positive">
          Headline Pick
        </div>
        <div className="text-2xl font-bold text-foreground truncate flex items-center gap-2">
          <FlagBadge player={player} size="lg" />
          <span className="truncate">{pick.pick}</span>
        </div>
        <div className="text-sm text-muted-foreground">
          {pick.bet_type.replace('_', ' ')} ·{' '}
          <span className="font-semibold text-foreground tabular-nums">
            {odds > 0 ? '+' : ''}{odds}
          </span>
        </div>
      </div>
      <div className="flex-shrink-0 text-right">
        <div className="text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
          Edge
        </div>
        <div className="text-3xl font-bold tabular-nums text-positive">
          +{Number.isFinite(edge) ? edge.toFixed(1) : '0.0'}%
        </div>
        <div className="text-[10px] text-muted-foreground tabular-nums">
          {(modelProb * 100).toFixed(1)}% model · {(impliedProb * 100).toFixed(1)}% mkt
        </div>
      </div>
    </div>
  )
}


function SecondaryPickRow({ pick, player }) {
  const headshot = player?.headshot_url
  const edge = Number(pick.edge ?? 0)
  const odds = Number(pick.odds ?? 0)
  return (
    <div className="rounded-lg border border-border bg-card/60 p-3 flex items-center gap-3">
      <div className="flex-shrink-0">
        {headshot ? (
          <img src={headshot} alt="" className="w-10 h-10 rounded-full bg-muted object-cover"
            onError={e => { e.currentTarget.style.display = 'none' }} />
        ) : (
          <div className="w-10 h-10 rounded-full bg-muted/50" />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Secondary
        </div>
        <div className="text-sm font-semibold truncate flex items-center gap-2">
          <FlagBadge player={player} />
          <span className="truncate">{pick.pick}</span>
        </div>
      </div>
      <div className="text-right text-xs">
        <div className="text-muted-foreground">
          {pick.bet_type.replace('_', ' ')} ·{' '}
          <span className="text-foreground tabular-nums">
            {odds > 0 ? '+' : ''}{odds}
          </span>
        </div>
        <div className="text-positive font-semibold tabular-nums">
          +{Number.isFinite(edge) ? edge.toFixed(1) : '0.0'}% edge
        </div>
      </div>
    </div>
  )
}


function ModelBoard({ players, pickPlayerIds, showFullField, onToggle }) {
  // Compact view: top 20 by p_winner + any picks outside top 20. Hides
  // the dense 5-column prob/odds grid in favor of a focused 3-column
  // layout (Win / Top-10 / Make-Cut). Users who want the full panel
  // expand via the "Show full field" toggle.
  const top20 = players.slice(0, 20)
  const top20Ids = new Set(top20.map(p => p.id))
  const extraPicked = players.filter(
    p => pickPlayerIds.has(p.id) && !top20Ids.has(p.id)
  )
  const compactRows = [...top20, ...extraPicked]
  const rows = showFullField ? players : compactRows
  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-3 py-2 border-b border-border flex items-center justify-between">
        <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
          Model board · top {compactRows.length} of {players.length}
        </div>
        <button
          onClick={onToggle}
          className="text-[10px] uppercase tracking-wider text-primary
                     hover:text-primary/80 transition-colors"
        >
          {showFullField ? 'Show top 20' : `Show full field (${players.length})`}
        </button>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-[10px] uppercase tracking-wider text-muted-foreground bg-muted/30">
            <tr>
              <th className="text-right px-2 py-2 w-10">#</th>
              <th className="text-left  px-3 py-2">Player</th>
              <th className="text-right px-2 py-2 w-20">Win</th>
              <th className="text-right px-2 py-2 w-20">Top 10</th>
              <th className="text-right px-2 py-2 w-20 hidden sm:table-cell">Make Cut</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((p, i) => (
              <PlayerRow key={p.id || i} player={p} rank={i + 1}
                          isPicked={pickPlayerIds.has(p.id)} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}


function PlayerRow({ player, rank, isPicked }) {
  const p = player
  return (
    <tr className={cn(
      'border-t border-border/40 hover:bg-accent/30 transition-colors',
      isPicked && 'bg-positive/5',
    )}>
      <td className="px-2 py-1.5 text-right text-[10px] tabular-nums text-muted-foreground">
        {rank}
      </td>
      <td className="px-3 py-1.5">
        <div className="flex items-center gap-2">
          {p.headshot_url ? (
            <img src={p.headshot_url} alt=""
              className="w-7 h-7 rounded-full object-cover bg-muted"
              onError={e => { e.currentTarget.style.display = 'none' }} />
          ) : (
            <div className="w-7 h-7 rounded-full bg-muted/50" />
          )}
          <div className="min-w-0">
            <div className="font-medium truncate flex items-center gap-1.5">
              <FlagBadge player={p} />
              <span className="truncate">{p.name}</span>
            </div>
            {p.country && (
              <div className="text-[10px] text-muted-foreground truncate">
                {p.country}
              </div>
            )}
          </div>
        </div>
      </td>
      <ProbCell value={p.p_winner}   edge={p.edges?.WINNER} />
      <ProbCell value={p.p_top_10}   edge={p.edges?.TOP_10} />
      <ProbCell value={p.p_made_cut} edge={p.edges?.MAKE_CUT}
                  className="hidden sm:table-cell" />
    </tr>
  )
}


// Compact prob cell — just the model probability, optionally tinted
// when this player has an edge in that market. Dropped odds + edge
// subtitles — those clutter the leaderboard. The Headline Pick hero
// already exposes the exact odds + edge for the recommended bets.
function ProbCell({ value, edge, className }) {
  if (value == null) {
    return <td className={cn(
      'px-2 py-1.5 text-right text-muted-foreground/60',
      className,
    )}>—</td>
  }
  const pct = (value * 100).toFixed(1)
  return (
    <td className={cn(
      'px-2 py-1.5 text-right tabular-nums whitespace-nowrap text-sm',
      edge != null && 'bg-positive/10 text-positive font-semibold',
      className,
    )}>
      {pct}%
    </td>
  )
}


// ── Calibration ─────────────────────────────────────────────

function CalibrationView({ tracker, tour }) {
  // First-pass calibration: settled picks bucketed by bet_type, showing
  // the realized hit rate vs model probability. Empty until picks
  // settle (golf tournaments finalize ~Sunday evening).
  const byType = tracker?.summary?.by_type || {}
  const hasSettled = Object.values(byType).some(
    b => (b.wins ?? 0) + (b.losses ?? 0) > 0
  )
  if (!hasSettled) {
    return (
      <EmptyState
        title="No settled picks yet"
        message={`Calibration metrics populate after ${tour.toUpperCase()} picks settle (tournaments finalize Sunday evening).`}
        icon="📊"
      />
    )
  }
  const rows = Object.entries(byType)
    .filter(([_, b]) => (b.wins ?? 0) + (b.losses ?? 0) > 0)
    .map(([bt, b]) => ({ bt, ...b }))
  return (
    <div className="space-y-4">
      <div className="text-xs text-muted-foreground">
        Per-market realized hit rate vs settled picks. Add Brier / log-loss
        once we have more sample.
      </div>
      <div className="rounded-lg border border-border bg-card overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-[10px] uppercase tracking-wider text-muted-foreground bg-muted/30">
            <tr>
              <th className="text-left px-3 py-2">Market</th>
              <th className="text-right px-2 py-2">Settled</th>
              <th className="text-right px-2 py-2">Wins</th>
              <th className="text-right px-2 py-2">Hit Rate</th>
              <th className="text-right px-2 py-2">ROI / unit</th>
              <th className="text-right px-2 py-2">P/L</th>
            </tr>
          </thead>
          <tbody>
            {rows.map(r => {
              const settled = (r.wins ?? 0) + (r.losses ?? 0)
              return (
                <tr key={r.bt} className="border-t border-border/40">
                  <td className="px-3 py-1.5 font-mono text-[10px]">{r.bt}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{settled}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{r.wins ?? 0}</td>
                  <td className="px-2 py-1.5 text-right tabular-nums">{r.win_pct ?? 0}%</td>
                  <td className={cn(
                    'px-2 py-1.5 text-right tabular-nums',
                    (r.roi ?? 0) > 0 && 'text-positive',
                    (r.roi ?? 0) < 0 && 'text-negative',
                  )}>
                    {(r.roi ?? 0) > 0 ? '+' : ''}{(r.roi ?? 0).toFixed(1)}
                  </td>
                  <td className={cn(
                    'px-2 py-1.5 text-right tabular-nums',
                    (r.profit ?? 0) > 0 && 'text-positive',
                    (r.profit ?? 0) < 0 && 'text-negative',
                  )}>
                    ${(r.profit ?? 0).toFixed(0)}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
