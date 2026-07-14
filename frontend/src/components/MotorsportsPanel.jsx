/**
 * MotorsportsPanel — F1 race-winner + podium framework panel.
 *
 * Layout mirrors HockeyPanel / BasketballPanel as closely as possible
 * (same Header shape, shared SubNav, PicksTable-driven tracker) so
 * the F1 surface feels like the same product as the other sports.
 * Body of the Race tab is bespoke (driver-table) since motorsports
 * is one outright per race rather than a 2-team H2H — the GameCard
 * primitive doesn't apply.
 */
import { useEffect, useMemo, useState } from 'react'
import { useSlate } from '../lib/useSlate'
import PanelShell from './primitives/PanelShell'
import PicksTable from './primitives/PicksTable'
import {
  flagForCountry, flagForNationality, teamColor,
} from '../lib/f1Flags'
import StandingsView from './motorsports/StandingsView'
import CalibrationView from './motorsports/CalibrationView'
import {
  RaceHero, FeaturedPick, PicksGrid,
} from './motorsports/RaceHeroCard'
import { TrackerHero, MarketTiles } from './motorsports/TrackerHero'
import { Th, Td, Stat, pct, fmtOdds, fmtRaceTime } from './motorsports/cells'
import { cn } from '../lib/utils'

// Standard tab ids (see PanelShell.STANDARD_TABS). Motorsports relabels
// 'slate' → 'Race' but keeps the id stable so cross-cutting code
// (POTD, deep-link routing) doesn't fork per sport.
const F1_TABS = [
  { id: 'slate',       label: 'Race' },
  { id: 'tracker',     label: 'Tracker' },
  { id: 'calibration', label: 'Calibration' },
  { id: 'standings',   label: 'Standings' },
]


export default function MotorsportsPanel({ api, seriesKey = 'f1', series = null }) {
  const [view, setView] = useState('slate')
  const [tracker, setTracker] = useState(null)
  const [standings, setStandings] = useState(null)
  const [calibration, setCalibration] = useState(null)
  const [err, setErr] = useState(null)

  // Pull the registry entry for the active series — drives the title,
  // status badge, and pending-data short-circuit (IndyCar / NASCAR
  // render a placeholder until their ingest lands).
  const seriesEntry = useMemo(
    () => (series || []).find(S => S.key === seriesKey),
    [series, seriesKey],
  )
  const isPendingData = seriesEntry?.status === 'pending_data'

  // Cached slate — instant on series switches once each has been
  // fetched. `pending_data` series short-circuit to a null slate so
  // the placeholder branch below still fires.
  const slateParams = useMemo(() => ({}), [])
  const { data: slateData, loading: slateLoading } =
    useSlate(seriesKey && !isPendingData
              ? `/motorsports/${seriesKey}/today`
              : null,
             slateParams, api)
  const slate = isPendingData ? null : slateData
  const loading = !!slateLoading && !isPendingData

  useEffect(() => {
    // Reset the sub-view state on series switch — tracker / standings /
    // calibration have their own endpoints that shouldn't flash stale
    // rows from the previous series.
    setTracker(null); setStandings(null); setCalibration(null)
    setErr(null)
  }, [seriesKey])

  useEffect(() => {
    if (view !== 'tracker' || !seriesKey || tracker) return
    api.get(`/motorsports/${seriesKey}/tracker`)
      .then(r => setTracker(r.data))
      .catch(() => setTracker({ rows: [], summary: {} }))
  }, [view, seriesKey, tracker, api])

  useEffect(() => {
    if (view !== 'standings' || !seriesKey || standings) return
    api.get(`/motorsports/${seriesKey}/standings`)
      .then(r => setStandings(r.data))
      .catch(() => setStandings({ drivers: [], constructors: [] }))
  }, [view, seriesKey, standings, api])

  useEffect(() => {
    if (view !== 'calibration' || !seriesKey || calibration) return
    api.get(`/motorsports/${seriesKey}/calibration`)
      .then(r => setCalibration(r.data))
      .catch(() => setCalibration({ n: 0, calibration: {} }))
  }, [view, seriesKey, calibration, api])

  const settled = tracker?.summary?.settled ?? 0
  const inSeason = slate?.in_season ?? true
  const status = seriesEntry?.status || slate?.status || 'beta'
  const title = seriesEntry?.display_name
    || slate?.display_name
    || 'Formula 1'
  const contextChips = useMemo(() => {
    const chips = []
    if (isPendingData) {
      chips.push({ tone: 'warning', text: 'Data pipeline pending', key: 'pending' })
      return chips
    }
    if (slate?.region) chips.push({ text: slate.region, key: 'region' })
    if (settled > 0) chips.push({ icon: '📜', text: `${settled} settled`, key: 'settled' })
    if (!inSeason) chips.push({ tone: 'warning', text: 'Off-season', key: 'off' })
    return chips
  }, [slate, settled, inSeason, isPendingData])

  if (isPendingData) {
    return (
      <PanelShell
        title={title}
        statusBadge={{ label: status, tone: status }}
        contextChips={contextChips}
        tabs={[{ id: 'slate', label: 'Race' }]}
        active="slate"
        onTabChange={() => {}}
      >
        <PendingDataView seriesName={title} />
      </PanelShell>
    )
  }

  return (
    <PanelShell
      title={title}
      statusBadge={{ label: status, tone: status }}
      contextChips={contextChips}
      tabs={F1_TABS}
      active={view}
      onTabChange={setView}
    >
      {view === 'slate' && (
        <RaceView slate={slate} loading={loading} err={err} />
      )}
      {view === 'standings' && (
        <StandingsView standings={standings} />
      )}
      {view === 'tracker' && (
        <TrackerView tracker={tracker} />
      )}
      {view === 'calibration' && (
        <CalibrationView calibration={calibration} />
      )}
    </PanelShell>
  )
}


function PendingDataView({ seriesName }) {
  return (
    <div className="rounded-lg border border-dashed border-border bg-card/40 px-6 py-12 text-center space-y-2">
      <div className="text-sm font-semibold text-foreground">
        {seriesName} coverage is on the roadmap.
      </div>
      <div className="text-xs text-muted-foreground max-w-md mx-auto">
        Race ingest, driver standings, and HR market wiring are not yet
        live for this series. The predictor framework (driver-rating
        model + walk-forward calibration) is already in place — only
        the data pipeline is pending.
      </div>
    </div>
  )
}


// ── Race view ───────────────────────────────────────────────

function RaceView({ slate, loading, err }) {
  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card/50 px-4 py-6 text-sm text-muted-foreground">
        Loading race…
      </div>
    )
  }
  if (err) {
    return (
      <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive">
        Failed to load: {err}
      </div>
    )
  }
  if (!slate?.race) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">
          No upcoming race in the next 21 days.
        </div>
      </div>
    )
  }
  const r = slate.race
  return (
    <div className="space-y-4">
      {slate.fell_forward && (
        <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[11px] font-medium text-amber-200">
          No race today — showing next race ({slate.date}).
        </div>
      )}

      {slate.odds_available === false && slate.drivers?.length > 0 && (
        <div className="rounded-md border border-sky-500/40 bg-sky-500/10 px-3 py-2 text-[11px] font-medium text-sky-200">
          HR hasn't posted race-day markets yet — showing model
          probabilities without odds. NASCAR + IndyCar race lines
          typically go up 24-48h before green flag; check back closer
          to the race.
        </div>
      )}

      {/* Race hero — big country flag, circuit image, countdown */}
      <RaceHero race={r} slate={slate} />

      {/* Featured pick — mint-tinted "pick of the race" hero */}
      <FeaturedPick drivers={slate.drivers} />

      {/* Race picks grid — WINNER + top-2 PODIUM compact cards */}
      <PicksGrid drivers={slate.drivers} />

      {/* Circuit info + last winners still live in the compact strip
          below the hero — they're supporting context, not header
          material. */}
      {(r.circuit_image_url || r.info?.length_km) && (
        <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
          <CircuitInfo race={r} />
        </div>
      )}

      {r.last_winners?.length > 0 && (
        <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
          <LastWinners winners={r.last_winners} />
        </div>
      )}

      {/* Full driver table — every driver's model probs + odds + edge */}
      <div>
        <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground mb-2">
          Driver Grid
        </div>
        <DriverTable drivers={slate.drivers} />
      </div>
    </div>
  )
}


function DriverTable({ drivers }) {
  if (!drivers?.length) return null
  return (
    <div className="rounded-lg border border-border bg-card/40 overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/40">
            <Th>Driver</Th>
            <Th>Team</Th>
            <Th align="center">Grid</Th>
            <Th align="right" colSpan={3}>Winner</Th>
            <Th align="right" colSpan={3}>Podium (Top 3)</Th>
          </tr>
          <tr className="border-b border-border bg-background/20 text-[9px]">
            <th className="px-3 py-1" colSpan={3} />
            <th className="px-3 py-1 text-right text-[9px] font-semibold uppercase tracking-wider text-muted-foreground/70">Model</th>
            <th className="px-3 py-1 text-right text-[9px] font-semibold uppercase tracking-wider text-muted-foreground/70">Odds</th>
            <th className="px-3 py-1 text-right text-[9px] font-semibold uppercase tracking-wider text-muted-foreground/70">Edge</th>
            <th className="px-3 py-1 text-right text-[9px] font-semibold uppercase tracking-wider text-muted-foreground/70">Model</th>
            <th className="px-3 py-1 text-right text-[9px] font-semibold uppercase tracking-wider text-muted-foreground/70">Odds</th>
            <th className="px-3 py-1 text-right text-[9px] font-semibold uppercase tracking-wider text-muted-foreground/70">Edge</th>
          </tr>
        </thead>
        <tbody>
          {drivers.map(d => {
            const anyPick = d.is_winner_pick || d.is_podium_pick
            return (
              <tr
                key={d.driver_id}
                className={cn(
                  'border-b border-border/60 hover:bg-accent/30 transition-colors',
                  anyPick && 'bg-primary/[0.03]',
                )}
                style={{ borderLeft: `3px solid ${teamColor(d.team)}` }}
              >
                <Td className="font-semibold text-foreground whitespace-nowrap">
                  <div className="flex items-center gap-2">
                    <span className="inline-flex items-center justify-center w-8 h-6 rounded bg-muted text-[10px] font-mono font-bold text-muted-foreground">
                      {d.abbrev}
                    </span>
                    {d.nationality && (
                      <span className="text-base leading-none" aria-label={d.nationality}>
                        {flagForNationality(d.nationality)}
                      </span>
                    )}
                    <span>{d.name}</span>
                  </div>
                </Td>
                <Td className="text-muted-foreground whitespace-nowrap text-xs">
                  <span className="inline-flex items-center gap-1.5">
                    <span
                      className="inline-block w-2.5 h-2.5 rounded-full"
                      style={{ backgroundColor: teamColor(d.team) }}
                      aria-hidden="true"
                    />
                    {d.team}
                  </span>
                </Td>
                <Td align="center" className="tabular-nums text-xs text-muted-foreground">
                  {d.grid_pos != null ? `P${d.grid_pos}` : '—'}
                </Td>
                <Td align="right" className="tabular-nums">
                  {pct(d.p_win)}
                </Td>
                <Td align="right" className="tabular-nums text-muted-foreground">
                  {fmtOdds(d.winner_odds)}
                </Td>
                <Td align="right" className="tabular-nums">
                  <EdgeCell edge={d.winner_edge} pick={d.is_winner_pick} label="WIN" />
                </Td>
                <Td align="right" className="tabular-nums">
                  {pct(d.p_podium)}
                </Td>
                <Td align="right" className="tabular-nums text-muted-foreground">
                  {fmtOdds(d.podium_odds)}
                </Td>
                <Td align="right" className="tabular-nums">
                  <EdgeCell edge={d.podium_edge} pick={d.is_podium_pick} label="P3" />
                </Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}


function EdgeCell({ edge, pick, label }) {
  if (edge == null) return <span className="text-muted-foreground">—</span>
  const ePct = (edge * 100).toFixed(1)
  const sign = edge > 0 ? '+' : ''
  if (pick) {
    return (
      <span className="inline-flex items-center gap-1 rounded px-1.5 py-0.5 bg-primary/20 text-primary font-semibold">
        {sign}{ePct}%
        <span className="text-[9px] uppercase tracking-wider">{label}</span>
      </span>
    )
  }
  const cls = edge > 0 ? 'text-positive' : 'text-muted-foreground'
  return <span className={cls}>{sign}{ePct}%</span>
}


// ── Tracker view (uses shared PicksTable primitive) ─────────

// PicksTable's default profitFormatter treats the row value as
// dollars. The motorsports tracker API ships row profit in units
// (a won +140 pick returns 1.4 to the caller, a lost 1u pick returns
// -1.0). Scale by $100/u so the P/L cell reads like every other
// tracker — 1u wins become +$100, 1u losses become -$100.
const _UNIT_DOLLARS = 100
function _fmtDollarsFromUnits(units) {
  return `$${Math.round(Number(units || 0) * _UNIT_DOLLARS)}`
}


function TrackerView({ tracker }) {
  if (!tracker) {
    return (
      <div className="rounded-lg border border-border bg-card/50 px-4 py-6 text-sm text-muted-foreground">
        Loading tracker…
      </div>
    )
  }
  const s = tracker.summary || {}
  const rows = tracker.rows || []
  return (
    <div className="space-y-4">
      <TrackerHero summary={s} />
      <MarketTiles byType={s.by_type} />
      {rows.length > 0 && (
        <section>
          <div className="flex items-baseline justify-between mb-2">
            <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
              Recent Picks
            </div>
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground/70 tabular-nums">
              {rows.length} shown
            </div>
          </div>
          <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
            <PicksTable
              picks={rows}
              typeColumnLabel="Market"
              sport="motorsports"
              profitFormatter={_fmtDollarsFromUnits}
            />
          </div>
        </section>
      )}
    </div>
  )
}


// ── Circuit info (track map + curated stats) ────────────────

function CircuitInfo({ race }) {
  const info = race.info || {}
  const stats = []
  if (info.length_km) stats.push({ label: 'Length', value: `${info.length_km.toFixed(3)} km` })
  if (info.laps) stats.push({ label: 'Laps', value: info.laps })
  if (info.distance_km) stats.push({ label: 'Race Distance', value: `${info.distance_km.toFixed(1)} km` })
  if (info.type) stats.push({ label: 'Type', value: info.type })
  return (
    <div className="p-4 grid grid-cols-1 md:grid-cols-[260px_1fr] gap-4">
      {race.circuit_image_url ? (
        <a
          href={race.circuit_wiki_url || race.circuit_image_url}
          target="_blank"
          rel="noopener noreferrer"
          className="block rounded-md overflow-hidden bg-white/95 p-2 hover:bg-white transition-colors"
          title="Open on Wikipedia"
        >
          <img
            src={race.circuit_image_url}
            alt={`${race.circuit} layout`}
            className="w-full h-auto max-h-48 object-contain"
            loading="lazy"
          />
        </a>
      ) : (
        <div className="rounded-md bg-muted/30 px-4 py-12 text-center text-xs text-muted-foreground">
          No track image available.
        </div>
      )}
      <div className="space-y-3">
        {stats.length > 0 && (
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
            {stats.map(s => (
              <div key={s.label}>
                <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
                  {s.label}
                </div>
                <div className="text-sm font-semibold text-foreground">{s.value}</div>
              </div>
            ))}
          </div>
        )}
        {info.lap_record_time && (
          <div>
            <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
              Lap Record
            </div>
            <div className="text-sm text-foreground">
              <span className="font-semibold tabular-nums">{info.lap_record_time}</span>
              <span className="ml-2 text-muted-foreground">
                {info.lap_record_holder}
                {info.lap_record_year ? ` (${info.lap_record_year})` : ''}
              </span>
            </div>
          </div>
        )}
      </div>
    </div>
  )
}


function LastWinners({ winners }) {
  return (
    <div className="px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground mb-1">
        Recent Winners Here
      </div>
      <div className="flex flex-wrap gap-2 text-xs">
        {winners.map(w => (
          <span
            key={w.season}
            className="inline-flex items-center gap-1 rounded px-2 py-1 bg-card/60 border border-border/60"
            style={{ borderLeft: `3px solid ${teamColor(w.team)}` }}
          >
            <span className="font-mono text-[10px] text-muted-foreground">{w.season}</span>
            <span className="font-semibold text-foreground">{w.abbrev}</span>
            <span className="text-muted-foreground">{w.name}</span>
          </span>
        ))}
      </div>
    </div>
  )
}



