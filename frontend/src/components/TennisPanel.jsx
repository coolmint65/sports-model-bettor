import { useEffect, useMemo, useState } from 'react'
import { User } from 'lucide-react'
import { cn } from '../lib/utils'
import EdgeBadge from './primitives/EdgeBadge'
import PickEventsBadge from './PickEventsBadge'
import PickOfDayHero from './PickOfDayHero'
import PicksTable from './primitives/PicksTable'
import PickHistory from './PickHistory'
import MatchCard, { TierChip } from './tennis/MatchCard'
import TournamentDropdownView from './tennis/TournamentDropdownView'
import TennisMatchDetail from './tennis/MatchDetail'

// Render a signed integer with explicit sign for + values. Replaces
// Python f-string format specs (`:+.0f`) which JSX can't parse.
function fmtSigned(n) {
  const v = Math.round(Number(n) || 0)
  return v > 0 ? `+${v}` : `${v}`
}

/**
 * TennisPanel — Phase 6 Tennis tab.
 *
 * 2026-05-12 reorg: each tournament now has its own sub-page (mirrors
 * the basketball/hockey framework pattern of leagues as drill-down
 * cards). Landing view is the tournament index — a sortable list of
 * active tournaments with match + edge counts. Clicking a tournament
 * opens the slate view filtered to that tournament's matches. The
 * previous "everything in one big flat slate" page is gone — Slams,
 * Masters, and ITF Futures no longer share a single scroll.
 *
 * Top-level tabs:
 *   - Tournaments (default) — index of active tournaments
 *   - Rankings              — top-25 by tour x surface (lookup tool)
 *   - Matchup               — ad-hoc two-player predictor (debug)
 *   - Tracker               — pending + settled tennis_picks rows
 *
 * Tournament drill-down replaces the Tournaments tab when a card is
 * clicked. Back button returns to the index.
 */
export default function TennisPanel({ api }) {
  const [tour, setTour] = useState('atp')
  const [surface, setSurface] = useState('Hard')
  const [view, setView] = useState('index') // index | rankings | matchup | tracker | calibration
  const [selectedMatch, setSelectedMatch] = useState(null)

  const tabs = [
    { id: 'index',       label: 'Tournaments' },
    { id: 'rankings',    label: 'Rankings'    },
    { id: 'matchup',     label: 'Matchup'     },
    { id: 'tracker',     label: 'Tracker'     },
    { id: 'calibration', label: 'Calibration' },
  ]

  return (
    <div className="space-y-4 py-4">
      <div className="flex items-center gap-3 flex-wrap">
        <div className="flex rounded-md border border-border overflow-hidden">
          {['atp','wta'].map(t => (
            <button
              key={t}
              onClick={() => setTour(t)}
              className={cn(
                'px-3 py-1.5 text-xs font-semibold uppercase tracking-wider',
                tour === t
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-card text-muted-foreground hover:text-foreground',
              )}
            >
              {t}
            </button>
          ))}
        </div>
        {/* Surface filter — used by Rankings + Matchup tools only. */}
        {(view === 'rankings' || view === 'matchup') && (
          <div className="flex rounded-md border border-border overflow-hidden">
            {['Hard','Clay','Grass','Carpet'].map(s => (
              <button
                key={s}
                onClick={() => setSurface(s)}
                className={cn(
                  'px-3 py-1.5 text-xs font-semibold',
                  surface === s
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-card text-muted-foreground hover:text-foreground',
                )}
              >
                {s}
              </button>
            ))}
          </div>
        )}
        <div className="ml-auto flex rounded-md border border-border overflow-hidden">
          {tabs.map(t => (
            <button
              key={t.id}
              onClick={() => setView(t.id)}
              className={cn(
                'px-3 py-1.5 text-xs font-semibold',
                view === t.id
                  ? 'bg-primary text-primary-foreground'
                  : 'bg-card text-muted-foreground hover:text-foreground',
              )}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {view === 'index' && (
        <TournamentDropdownView
          api={api}
          tour={tour}
          onOpenMatch={setSelectedMatch}
        />
      )}
      {view === 'rankings' && (
        <RankingsView api={api} tour={tour} surface={surface} />
      )}
      {view === 'matchup' && (
        <MatchupView api={api} tour={tour} surface={surface} />
      )}
      {view === 'tracker' && (
        <TrackerView api={api} tour={tour} />
      )}
      {view === 'calibration' && (
        <CalibrationView api={api} tour={tour} />
      )}
      {selectedMatch && (
        <TennisMatchDetail
          api={api}
          match={selectedMatch}
          onClose={() => setSelectedMatch(null)}
        />
      )}
    </div>
  )
}


// ── Calibration view ────────────────────────────────────────

function CalibrationView({ api, tour }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const [refreshing, setRefreshing] = useState(false)

  const refresh = () => {
    setLoading(true)
    api.get('/tennis/calibration')
      .then(r => setData(r.data))
      .catch(() => setData({ n: 0, per_market: [] }))
      .finally(() => { setLoading(false); setRefreshing(false) })
  }
  useEffect(() => { refresh() }, [])  // eslint-disable-line

  if (loading && !data) {
    return (
      <div className="text-center py-8 text-sm text-muted-foreground">
        Loading calibration…
      </div>
    )
  }
  if (!data || !data.n) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm text-muted-foreground">
        No settled tennis picks yet. Calibration fits on W/L outcomes — come back after a few matches resolve.
      </div>
    )
  }

  const filtered = (data.per_market || []).filter(
    m => !tour || m.tour === tour,
  )
  const improvement = data.improvement_pct || 0
  const tone = improvement >= 20 ? 'text-positive'
    : improvement >= 5 ? 'text-foreground'
    : 'text-warning'

  return (
    <section className="space-y-4">
      <header className="flex items-baseline justify-between gap-3 pt-2">
        <div>
          <h3 className="text-lg font-semibold tracking-tight text-foreground">
            Calibration
          </h3>
          <p className="text-[11px] text-muted-foreground mt-1">
            Bayesian beta-binomial bucket shrinkage on settled picks.
            Brier improvement &gt;20% is healthy.
          </p>
        </div>
        <button
          onClick={() => { setRefreshing(true); refresh() }}
          disabled={refreshing}
          className="rounded-md border border-border px-2 py-1 text-[10px] uppercase tracking-wider text-muted-foreground hover:text-foreground"
        >
          {refreshing ? 'Refreshing…' : 'Recalibrate'}
        </button>
      </header>

      <div className="grid grid-cols-2 md:grid-cols-4 gap-3 rounded-lg border border-border bg-card/60 px-4 py-3">
        <Stat label="Settled Picks" value={data.n} />
        <Stat label="Brier (Raw)" value={data.brier_raw?.toFixed(4) ?? '—'} />
        <Stat label="Brier (Calibrated)" value={data.brier_calibrated?.toFixed(4) ?? '—'} />
        <Stat
          label="Improvement"
          value={`${improvement.toFixed(1)}%`}
          valueClass={tone}
        />
      </div>

      <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
        <div className="px-3 py-2 border-b border-border bg-background/40">
          <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Per (Tour, Market) — Brier delta + Reliability
          </div>
        </div>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-background/20">
                <Th>Tour</Th>
                <Th>Market</Th>
                <Th align="right">N</Th>
                <Th align="right">W</Th>
                <Th align="right">Brier Raw</Th>
                <Th align="right">Brier Cal</Th>
                <Th align="right">Improvement</Th>
                <Th align="right">Reliability</Th>
                <Th align="right">ROI / Pick</Th>
              </tr>
            </thead>
            <tbody>
              {filtered.map(m => {
                const impTone = m.improvement_pct >= 20 ? 'text-positive'
                  : m.improvement_pct >= 5 ? 'text-foreground'
                  : 'text-warning'
                const relTone = m.reliability >= 0.6 ? 'text-positive'
                  : m.reliability >= 0.4 ? 'text-foreground'
                  : 'text-destructive'
                const roiTone = m.roi_per_pick > 0 ? 'text-positive'
                  : m.roi_per_pick < 0 ? 'text-destructive'
                  : 'text-muted-foreground'
                return (
                  <tr
                    key={`${m.tour}:${m.bet_type}`}
                    className="border-b border-border/60 hover:bg-accent/30 transition-colors"
                  >
                    <Td className="font-mono text-xs uppercase">{m.tour}</Td>
                    <Td className="font-semibold">{m.bet_type}</Td>
                    <Td align="right" className="tabular-nums">{m.n}</Td>
                    <Td align="right" className="tabular-nums">{m.wins}</Td>
                    <Td align="right" className="tabular-nums">{m.brier_raw.toFixed(4)}</Td>
                    <Td align="right" className="tabular-nums font-semibold">{m.brier_calibrated.toFixed(4)}</Td>
                    <Td align="right" className={`tabular-nums font-semibold ${impTone}`}>
                      {m.improvement_pct > 0 ? '+' : ''}{m.improvement_pct.toFixed(1)}%
                    </Td>
                    <Td align="right" className={`tabular-nums ${relTone}`}>{m.reliability.toFixed(3)}</Td>
                    <Td align="right" className={`tabular-nums ${roiTone}`}>
                      {m.roi_per_pick > 0 ? '+' : ''}{m.roi_per_pick.toFixed(2)}
                    </Td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>

      <div className="text-[11px] text-muted-foreground">
        Calibrated probability is fed to picks_core's edge gate, so the picks engine already
        consumes these adjustments. Reliability is the auto-tuned multiplier
        from realized ROI — bet types under 0.5 get demoted, over 0.5 boosted.
        Recalibrate to refit after new picks settle.
      </div>
    </section>
  )
}


function Stat({ label, value, valueClass = '' }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className={`text-lg font-semibold tabular-nums ${valueClass || 'text-foreground'}`}>{value}</div>
    </div>
  )
}


function Th({ children, align = 'left' }) {
  const a = align === 'right' ? 'text-right' : align === 'center' ? 'text-center' : 'text-left'
  return (
    <th scope="col" className={`px-3 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground ${a}`}>
      {children}
    </th>
  )
}

function Td({ children, align = 'left', className = '' }) {
  const a = align === 'right' ? 'text-right' : align === 'center' ? 'text-center' : 'text-left'
  return <td className={`px-3 py-2 ${a} ${className}`}>{children}</td>
}





// ── Rankings ──────────────────────────────────────────────────

function RankingsView({ api, tour, surface }) {
  const [rows, setRows] = useState([])
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    let cancelled = false
    setLoading(true)
    api.get(`/tennis/top-players`, { params: { tour, surface, limit: 25 }})
      .then(r => { if (!cancelled) setRows(r.data?.players || []) })
      .catch(() => { if (!cancelled) setRows([]) })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, tour, surface])

  return (
    <section className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between px-5 py-3 border-b border-border">
        <h3 className="text-sm font-semibold">{tour.toUpperCase()} {surface} — Top 25</h3>
        <span className="text-[10px] uppercase tracking-wider text-muted-foreground">
          {loading ? 'loading…' : `${rows.length} players`}
        </span>
      </div>
      <div className="divide-y divide-border">
        {rows.map((r, i) => (
          <div key={`${r.player_id}`} className="flex items-center gap-3 px-5 py-2.5 text-sm">
            <span className="w-8 text-right tabular-nums text-muted-foreground">{i+1}</span>
            <span className="flex-1 font-medium">{r.name || `#${r.player_id}`}</span>
            <span className="tabular-nums text-xs text-muted-foreground">{r.matches}m</span>
            <span className="w-14 text-right tabular-nums text-xs text-muted-foreground">RD {Math.round(r.rd)}</span>
            <span className="w-16 text-right font-bold tabular-nums">{Math.round(r.rating)}</span>
          </div>
        ))}
        {!loading && rows.length === 0 && (
          <div className="px-5 py-8 text-center text-sm text-muted-foreground">
            No ratings yet for this tour × surface.
          </div>
        )}
      </div>
    </section>
  )
}


// ── Matchup ───────────────────────────────────────────────────

function MatchupView({ api, tour, surface }) {
  const [bestOf, setBestOf] = useState(3)
  const [p1, setP1] = useState(null)
  const [p2, setP2] = useState(null)
  const [p1ML, setP1ML] = useState('')
  const [p2ML, setP2ML] = useState('')
  const [pred, setPred] = useState(null)
  const [picks, setPicks] = useState([])
  const [loading, setLoading] = useState(false)
  const [lockState, setLockState] = useState({})  // pickKey -> 'idle' | 'locking' | 'locked'

  // Re-predict whenever inputs change
  useEffect(() => {
    if (!p1 || !p2) {
      setPred(null); setPicks([]); return
    }
    let cancelled = false
    setLoading(true)
    const params = {
      tour, p1_id: p1.player_id, p2_id: p2.player_id,
      surface, best_of: bestOf,
    }
    if (p1ML) params.p1_ml = parseInt(p1ML, 10)
    if (p2ML) params.p2_ml = parseInt(p2ML, 10)
    api.get(`/tennis/picks/preview`, { params })
      .then(r => {
        if (cancelled) return
        setPred(r.data?.prediction || null)
        setPicks(r.data?.picks || [])
      })
      .catch(() => { if (!cancelled) { setPred(null); setPicks([]) } })
      .finally(() => { if (!cancelled) setLoading(false) })
    return () => { cancelled = true }
  }, [api, tour, surface, bestOf, p1, p2, p1ML, p2ML])

  const onLock = async (pick) => {
    const key = `${pick.type}|${pick.pick}`
    setLockState(s => ({ ...s, [key]: 'locking' }))
    try {
      const matchup = `${p1.name} vs ${p2.name}`
      // Synth match_id so the row has a stable key. Real match_id will
      // be set when HR live ingest pairs predictions to actual draws.
      const today = new Date().toISOString().slice(0,10)
      const match_id = `manual-${today}-${p1.player_id}-${p2.player_id}-${bestOf}`
      const payload = {
        tour, match_id, matchup,
        surface, best_of: bestOf, tourney_level: 'M',
        p1_id: p1.player_id, p2_id: p2.player_id,
        bet_type: pick.type, pick: pick.pick,
        model_prob: pick.model_prob, edge: pick.edge, odds: pick.odds,
        conviction_score: pick.conviction_score,
      }
      await api.post(`/tennis/tracker/record`, payload)
      setLockState(s => ({ ...s, [key]: 'locked' }))
    } catch {
      setLockState(s => ({ ...s, [key]: 'idle' }))
    }
  }

  return (
    <section className="space-y-4">
      <div className="rounded-lg border border-border bg-card p-5 space-y-4">
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          <PlayerPicker api={api} tour={tour} label="Player 1" value={p1} onChange={setP1} />
          <PlayerPicker api={api} tour={tour} label="Player 2" value={p2} onChange={setP2} />
        </div>
        <div className="flex items-center gap-3 flex-wrap">
          <span className="text-xs uppercase tracking-wider text-muted-foreground">Best of</span>
          <div className="flex rounded-md border border-border overflow-hidden">
            {[3,5].map(n => (
              <button
                key={n}
                onClick={() => setBestOf(n)}
                className={cn(
                  'px-3 py-1.5 text-xs font-semibold',
                  bestOf === n
                    ? 'bg-primary text-primary-foreground'
                    : 'bg-card text-muted-foreground hover:text-foreground',
                )}
              >{n}</button>
            ))}
          </div>
          <input
            type="text"
            placeholder="P1 ML odds (e.g. -140)"
            value={p1ML}
            onChange={e => setP1ML(e.target.value)}
            className="rounded-md border border-border bg-background px-3 py-1.5 text-xs w-40"
          />
          <input
            type="text"
            placeholder="P2 ML odds (e.g. +120)"
            value={p2ML}
            onChange={e => setP2ML(e.target.value)}
            className="rounded-md border border-border bg-background px-3 py-1.5 text-xs w-40"
          />
        </div>
      </div>

      {pred && (
        <div className="rounded-lg border border-border bg-card p-5 space-y-3">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground font-semibold">
            Prediction · {surface} · BO{bestOf} · {pred.surface_used}
          </div>
          <div className="grid grid-cols-2 gap-4">
            <PredCol name={p1.name} prob={pred.p1_win_prob} rating={pred.p1_rating} rd={pred.p1_rd} />
            <PredCol name={p2.name} prob={pred.p2_win_prob} rating={pred.p2_rating} rd={pred.p2_rd} />
          </div>
          <div className="text-[11px] text-muted-foreground">
            Elo gap {fmtSigned(pred.factors.elo_gap)}
            {' · '}form Δ {fmtSigned(pred.factors.p1_form_delta)} / {fmtSigned(pred.factors.p2_form_delta)}
            {' · '}fatigue Δ {fmtSigned(pred.factors.p1_fatigue_delta)} / {fmtSigned(pred.factors.p2_fatigue_delta)}
          </div>
        </div>
      )}

      {picks.length > 0 && (
        <div className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold">Edges</h3>
          </div>
          <div className="divide-y divide-border">
            {picks.map(p => {
              const key = `${p.type}|${p.pick}`
              const state = lockState[key] || 'idle'
              return (
                <div key={key} className="flex items-center gap-2 px-5 py-3">
                  <div className="min-w-0 flex-1">
                    <EdgeBadge
                      pick={{ type: p.type, pick: p.pick, edge: p.edge, odds: p.odds }}
                      confidence={p.confidence}
                    />
                  </div>
                  <span className="text-xs font-semibold tabular-nums text-muted-foreground w-16 text-right">
                    {p.odds > 0 ? `+${p.odds}` : p.odds}
                  </span>
                  <button
                    onClick={() => onLock(p)}
                    disabled={state !== 'idle'}
                    className={cn(
                      'rounded-md px-3 py-1.5 text-[11px] font-semibold uppercase tracking-wider',
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
            })}
          </div>
        </div>
      )}

      {loading && (!pred || picks.length === 0) && (
        <div className="text-center py-8 text-sm text-muted-foreground">
          Predicting…
        </div>
      )}
      {!loading && (!p1 || !p2) && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm text-muted-foreground">
          Pick two players to see a prediction.
        </div>
      )}
    </section>
  )
}


function PredCol({ name, prob, rating, rd }) {
  const pct = (prob * 100).toFixed(1)
  return (
    <div>
      <div className="text-sm font-semibold truncate">{name}</div>
      <div className="text-3xl font-bold tabular-nums">{pct}%</div>
      <div className="text-xs text-muted-foreground tabular-nums">
        Elo {Math.round(rating)} · RD {Math.round(rd)}
      </div>
    </div>
  )
}


function PlayerPicker({ api, tour, label, value, onChange }) {
  const [q, setQ] = useState('')
  const [results, setResults] = useState([])
  const [open, setOpen] = useState(false)

  useEffect(() => {
    if (!q || q.length < 2) {
      setResults([]); return
    }
    let cancelled = false
    api.get(`/tennis/players/search`, { params: { tour, q }})
      .then(r => { if (!cancelled) setResults(r.data?.results || []) })
      .catch(() => { if (!cancelled) setResults([]) })
    return () => { cancelled = true }
  }, [api, tour, q])

  // Reset on tour change
  useEffect(() => { onChange(null); setQ('') }, [tour])  // eslint-disable-line

  return (
    <div className="relative">
      <label className="text-[10px] uppercase tracking-wider text-muted-foreground font-semibold">
        {label}
      </label>
      <input
        type="text"
        placeholder={value ? value.name : 'Search…'}
        value={q}
        onChange={e => { setQ(e.target.value); setOpen(true) }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 150)}
        className="w-full rounded-md border border-border bg-background px-3 py-2 text-sm"
      />
      {open && results.length > 0 && (
        <div className="absolute left-0 right-0 z-10 mt-1 max-h-60 overflow-y-auto rounded-md border border-border bg-card shadow-lg">
          {results.map(r => (
            <button
              key={r.player_id}
              onClick={() => { onChange(r); setQ(''); setOpen(false) }}
              className="block w-full text-left px-3 py-2 text-sm hover:bg-secondary"
            >
              <span className="font-medium">{r.name}</span>
              {r.country && (
                <span className="ml-2 text-xs text-muted-foreground">{r.country}</span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}


// ── Tracker ───────────────────────────────────────────────────

// CLV from American odds → implied prob delta in pp.
// Same formula team-sport tracker uses.
function clvFor(r) {
  if (r.odds == null || r.closing_odds == null) return null
  const imp = (o) => o < 0 ? Math.abs(o) / (Math.abs(o) + 100) : 100 / (o + 100)
  return Math.round((imp(r.closing_odds) - imp(r.odds)) * 1000) / 10
}


function TrackerView({ api, tour }) {
  const [rows, setRows] = useState([])
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(false)

  const refresh = () => {
    setLoading(true)
    Promise.all([
      api.get(`/tennis/tracker/history`, { params: { tour, limit: 200 }}).then(r => setRows(r.data?.rows || [])),
      api.get(`/tennis/tracker/summary`, { params: { tour }}).then(r => setSummary(r.data)),
    ])
      .catch(() => {})
      .finally(() => setLoading(false))
  }
  useEffect(refresh, [api, tour])  // eslint-disable-line

  // Manual record/settle hooks — same surface team sports expose.
  const onSettle = () => {
    api.post(`/tennis/tracker/settle`).finally(refresh)
  }
  // Tennis "Record today" runs the schedule's pick generator + records
  // any new top picks across the slate. Refresh after for immediate
  // feedback. (Tennis records via the schedule daily, but the manual
  // button is preserved for parity + ad-hoc recovery.)
  const onRecord = () => {
    api.post(`/tennis/scheduled/refresh`).finally(refresh)
  }

  // Map tennis history rows into the team-sport pick shape PickHistory
  // already understands. The 🧪 prefix on paper rows lives inside the
  // matchup cell so PicksTable stays tour-agnostic.
  const decoratedHistory = rows.map(r => ({
    ...r,
    matchup: r.is_paper ? `🧪 ${r.matchup}` : r.matchup,
  }))

  return (
    <section className="space-y-4">
      {summary?.on_probation && (
        <div className="rounded-lg border border-warning/40 bg-warning/10 px-4 py-3">
          <div className="text-xs font-bold uppercase tracking-wider text-warning">
            🧪 PAPER-BET PROBATION
          </div>
          <div className="mt-1 text-xs text-foreground">
            Tennis picks are paper-only until {summary.probation_min_decided}+ settled
            picks accumulate AND model Brier ≤ {summary.probation_brier_ceiling}.
            Currently {(summary.wins || 0) + (summary.losses || 0)} decided.
          </div>
        </div>
      )}
      <PickHistory
        summary={summary}
        history={decoratedHistory}
        loading={loading}
        onRecord={onRecord}
        onSettle={onSettle}
      />
    </section>
  )
}


// ── Match-detail modal ───────────────────────────────────────
//
// Click-through pane analogous to NBAGameDetail / NHLGameDetail —
// surfaces the full prediction context, every market HR shipped, and
// the complete pick list (not just best_pick). Modal-style overlay
// because tennis matches don't deserve a route of their own and the
// overlay matches how the other sports' detail panes already feel.
