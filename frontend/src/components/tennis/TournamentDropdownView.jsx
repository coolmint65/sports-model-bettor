/**
 * TournamentDropdownView — single-page tennis slate with each
 * tournament as a collapsible section.
 *
 * Replaces the previous two-page flow (Tournament index → drill-down).
 * Matches basketball's expandable-group pattern: parent row shows
 * tournament name + tier + surface + match/edge counts, chevron
 * toggles the section, matches render inside when expanded.
 *
 * Default expansion:
 *   - Auto-expand any tournament with ≥1 edge (moderate/strong pick),
 *     or any live match. That's the actionable stuff.
 *   - Everything else stays collapsed so the user sees a compact
 *     directory instead of a wall of ITF/Challenger matches.
 *
 * Priority-tier dividers (Slams & Masters / Main Tour / Challengers /
 * ITF) still group tournaments visually so the top of the page reads
 * top-down by importance.
 */
import { useEffect, useMemo, useState } from 'react'
import { ChevronRight } from 'lucide-react'
import { cn } from '../../lib/utils'
import PaperPickOfDayHero from '../PaperPickOfDayHero'
import MatchCard, { TierChip } from './MatchCard'
import { useSlate } from '../../lib/useSlate'


export default function TournamentDropdownView({ api, tour }) {
  // Shared slate cache — instant on tab switches, background refresh
  // every 2 min. First mount kicks a fetch; every subsequent mount
  // (including cross-tour toggles) returns cached matches immediately.
  const slateParams = useMemo(() => ({ tour, days: 2 }), [tour])
  const { data, loading, refresh: refreshCache } =
    useSlate('/tennis/scheduled', slateParams, api)
  const matches = data?.matches || []
  const [refreshing, setRefreshing] = useState(false)

  // PAPER Pick of the Day (PickMint tennis parity Phase 2/3, Austin
  // 2026-10-10) — REPLACES the real-only <PickOfDayHero> that used to
  // sit here. Tennis is 100% on probation (on_probation=true for both
  // tours — no real pick has ever cleared the gate), so the real hero
  // was dead chrome: /api/pick-of-day/tennis always answered "No
  // qualifying picks today" and PickOfDayHero renders null on that
  // shape. The game-day-locked paper POTD (engine/tennis_paper_potd.py,
  // exposed at /api/tennis/paper-potd) is the thing that actually has
  // content every day. If tennis ever graduates off probation this
  // should go back to a real hero — not automatic, flag for a
  // follow-up rather than threading a live probation check through
  // here today.
  const [paperPotd, setPaperPotd] = useState(null)
  const [paperPotdRecord, setPaperPotdRecord] = useState(null)
  useEffect(() => {
    let alive = true
    api.get('/tennis/paper-potd', { params: { tour } })
      .then(r => {
        if (!alive) return
        setPaperPotd(r.data?.potd || null)
        setPaperPotdRecord(r.data?.potd_record || null)
      })
      .catch(() => { if (alive) { setPaperPotd(null); setPaperPotdRecord(null) } })
    return () => { alive = false }
  }, [api, tour])

  const refresh = (force = false) => {
    if (!force) { refreshCache(); return }
    setRefreshing(true)
    // Force refresh — bypass the cache TTL and force the backend to
    // re-fetch from ESPN. Non-force just uses the cache's own poll.
    api.get(`/tennis/scheduled`, { params: { tour, days: 2, refresh: true } })
      .then(() => refreshCache())
      .finally(() => setRefreshing(false))
  }

  // Flattened (Austin 2026-09-19): matches render as ONE time-sorted list
  // (MLB/NBA style) instead of per-tournament collapsible sections.
  const sortedMatches = useMemo(
    () => [...matches].sort((a, b) =>
      String(a.start_time || a.date || '').localeCompare(String(b.start_time || b.date || ''))),
    [matches])

  const totalEdges = useMemo(
    () => matches.reduce((n, m) => (
      m.confidence === 'strong' || m.confidence === 'moderate' ? n + 1 : n
    ), 0),
    [matches]
  )

  return (
    <section className="space-y-4">
      <PaperPickOfDayHero
        games={matches}
        sport="tennis"
        locked={(paperPotd) ?? null}
        summary={paperPotdRecord}
      />

      <header className="flex items-baseline justify-between gap-3 pt-2 flex-wrap">
        <div className="flex items-center gap-2 min-w-0">
          <h3 className="text-lg font-semibold tracking-tight text-foreground truncate">
            {tour.toUpperCase()} Tennis
            <span className="ml-2 text-sm font-normal tabular-nums text-muted-foreground">
              ({matches.length})
            </span>
          </h3>
        </div>
        <div className="flex items-center gap-2">
          {totalEdges > 0 && (
            <span className="rounded-full bg-positive/15 px-2 py-0.5 text-[11px] font-semibold tabular-nums text-positive">
              {totalEdges} plays with edge
            </span>
          )}
          <button
            onClick={() => refresh(true)}
            disabled={refreshing}
            className="rounded-md border border-border px-2 py-1 text-[10px] uppercase tracking-wider text-muted-foreground hover:text-foreground disabled:opacity-50"
          >
            {refreshing ? 'Refreshing…' : 'Refresh from ESPN'}
          </button>
        </div>
      </header>

      {loading && matches.length === 0 && (
        <div className="text-center py-8 text-sm text-muted-foreground">
          Loading slate…
        </div>
      )}
      {!loading && matches.length === 0 && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm text-muted-foreground">
          No {tour.toUpperCase()} matches found. Try Refresh from ESPN.
        </div>
      )}

      {/* 2-col grid (Phase 1 compact restyle, 2026-10-10) — matches
          BasketballPanel's GameCard grid (grid-cols-1 md:grid-cols-2
          gap-3) now that MatchCard is trimmed to the same density;
          the single-column stack this replaced was built for the old,
          taller card. */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        {sortedMatches.map(m => (
          <MatchCard key={m.match_id} match={m} />
        ))}
      </div>
    </section>
  )
}


// ── Collapsible tournament section ──────────────────────────

function TournamentSection({ tournament, onOpenMatch }) {
  // Auto-expand tournaments with picks OR live matches. Slams / Masters
  // stay expanded by default too — the top-of-page tournaments are
  // usually the ones the user opened the page to look at.
  const initiallyOpen = tournament.edges > 0
    || tournament.matches_live > 0
    || tournament.priority >= 80
  const [open, setOpen] = useState(initiallyOpen)

  const {
    label, level, surface, matches, matches_today,
    matches_live, edges, first_date, last_date,
  } = tournament

  return (
    <div className={cn(
      'rounded-lg border border-border overflow-hidden',
      matches_live > 0 && 'border-positive/40',
      edges > 0 && 'ring-1 ring-primary/20',
    )}>
      <button
        type="button"
        onClick={() => setOpen(v => !v)}
        aria-expanded={open}
        className={cn(
          'flex w-full items-center justify-between gap-3 px-4 py-3 text-left',
          'transition-colors hover:bg-accent/40 focus:outline-none focus-visible:ring-2 focus-visible:ring-ring',
          open && 'bg-accent/20 border-b border-border',
        )}
      >
        <div className="flex items-center gap-2 min-w-0 flex-1">
          <ChevronRight
            className={cn(
              'h-4 w-4 transition-transform flex-shrink-0 text-muted-foreground',
              open && 'rotate-90',
            )}
            aria-hidden="true"
          />
          <span className="text-sm font-semibold text-foreground truncate">
            {label}
          </span>
          {level && <TierChip level={level} />}
          <span className="text-[10px] uppercase tracking-wider text-muted-foreground/70">
            · {surface || '?'}
          </span>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0 text-[11px]">
          {matches_live > 0 && (
            <span className="rounded-full bg-positive/15 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-positive">
              Live · {matches_live}
            </span>
          )}
          {edges > 0 && (
            <span className="rounded-full bg-primary/15 px-1.5 py-0.5 font-semibold tabular-nums text-primary">
              {edges} edge{edges === 1 ? '' : 's'}
            </span>
          )}
          {matches_today > 0 && (
            <span className="text-foreground/85 tabular-nums">
              {matches_today} today
            </span>
          )}
          <span className="text-muted-foreground tabular-nums">
            {matches.length} match{matches.length === 1 ? '' : 'es'}
          </span>
          {first_date && (
            <span className="text-muted-foreground/70 tabular-nums hidden sm:inline">
              {first_date.slice(5)}
              {last_date && last_date !== first_date && ` → ${last_date.slice(5)}`}
            </span>
          )}
        </div>
      </button>

      {open && (
        <div className="p-3 grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
          {matches.map(m => (
            <MatchCard
              key={m.match_id}
              match={m}
              onOpen={() => onOpenMatch?.(m)}
            />
          ))}
        </div>
      )}
    </div>
  )
}


// ── Grouping ────────────────────────────────────────────────

function tierFor(priority) {
  if (priority >= 80) return { label: 'Top Tier · Slams & Masters', order: 1 }
  if (priority >= 50) return { label: 'Main Tour · 500 / Premier',  order: 2 }
  if (priority >= 25) return { label: 'Tour · 250 / A',              order: 3 }
  if (priority >= 10) return { label: 'Challengers',                 order: 4 }
  return                     { label: 'ITF & Futures',               order: 5 }
}

function groupIntoTiers(matches) {
  // First: bucket matches per tournament, sort matches within a
  // tournament by (with-pick first, then by date), and compute
  // per-tournament aggregates.
  const byTournament = new Map()
  for (const m of matches) {
    const key = m.tournament || '(unknown)'
    if (!byTournament.has(key)) {
      byTournament.set(key, {
        label: key,
        level: m.tournament_level,
        surface: m.surface,
        priority: m.tournament_priority ?? 30,
        matches: [],
        matches_today: 0,
        matches_live: 0,
        edges: 0,
        first_date: null,
        last_date: null,
      })
    }
    const bucket = byTournament.get(key)
    bucket.matches.push(m)
    if (m.status === 'in') bucket.matches_live += 1
    if (m.confidence === 'strong' || m.confidence === 'moderate') {
      bucket.edges += 1
    }
    const d = m.date || ''
    if (d) {
      if (!bucket.first_date || d < bucket.first_date) bucket.first_date = d
      if (!bucket.last_date  || d > bucket.last_date)  bucket.last_date = d
    }
    const today = new Date().toISOString().slice(0, 10)
    if (d === today) bucket.matches_today += 1
  }

  // Sort matches inside each tournament: live first, then picks, then
  // date. Same rhythm the previous BetsView produced with its buckets.
  const sortMatches = (list) => list.sort((a, b) => {
    const scoreOf = (m) => {
      if (m.status === 'in') return 0
      if (m.confidence === 'strong' || m.confidence === 'moderate') return 1
      if (m.confidence === 'lean') return 2
      if (m.status === 'post') return 4
      return 3
    }
    const sa = scoreOf(a), sb = scoreOf(b)
    if (sa !== sb) return sa - sb
    return (a.date || '').localeCompare(b.date || '')
  })
  for (const t of byTournament.values()) sortMatches(t.matches)

  // Now tier tournaments and sort them within each tier: with-picks
  // first, then priority, then alpha.
  const tiers = new Map()
  for (const t of byTournament.values()) {
    const tier = tierFor(t.priority)
    if (!tiers.has(tier.label)) {
      tiers.set(tier.label, { order: tier.order, tournaments: [] })
    }
    tiers.get(tier.label).tournaments.push(t)
  }
  for (const bucket of tiers.values()) {
    bucket.tournaments.sort((a, b) =>
      (b.edges - a.edges)
      || (b.matches_live - a.matches_live)
      || (b.priority - a.priority)
      || a.label.localeCompare(b.label)
    )
  }
  return Array.from(tiers.entries())
    .map(([label, { order, tournaments }]) => ({ label, order, tournaments }))
    .sort((a, b) => a.order - b.order)
}
