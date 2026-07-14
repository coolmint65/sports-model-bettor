/**
 * Motorsports standings view — drivers + constructors championship.
 * Extracted from MotorsportsPanel 2026-07-09.
 */
import { flagForNationality, teamColor } from '../../lib/f1Flags'
import { Th, Td } from './cells'


export default function StandingsView({ standings }) {
  if (!standings) {
    return (
      <div className="rounded-lg border border-border bg-card/50 px-4 py-6 text-sm text-muted-foreground">
        Loading standings…
      </div>
    )
  }
  const drivers = standings.drivers || []
  const constructors = standings.constructors || []
  if (!drivers.length && !constructors.length) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">
          No completed races yet this season.
        </div>
      </div>
    )
  }
  const seasonLabel = standings.season ? `${standings.season} Season` : 'Season'
  // Constructors table renders only when the series has data. NASCAR
  // + IndyCar populate it via racing-team from vehicle metadata (see
  // engine.motorsports._espn_core_ingest), F1 via Ergast constructors.
  // Widen the drivers column to full width when constructors is empty
  // rather than leaving an awkward blank half-panel.
  const showConstructors = constructors.length > 0
  return (
    <>
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {seasonLabel}
      </div>
      <div className={`grid grid-cols-1 gap-4 ${
        showConstructors ? 'lg:grid-cols-2' : ''
      }`}>
        <DriversChampTable rows={drivers} />
        {showConstructors && <ConstructorsChampTable rows={constructors} />}
      </div>
    </>
  )
}



function DriversChampTable({ rows }) {
  // Series without a points feed (NASCAR / IndyCar don't ship
  // per-race points via the ESPN core endpoint) get a wins-based
  // ranking instead of a broken 0-pts column. F1's Ergast ingest
  // populates points so the column shows normally.
  const hasPoints = rows.some(d => (d.points || 0) > 0)
  return (
    <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
      <div className="px-3 py-2 border-b border-border bg-background/40">
        <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Drivers Championship
        </div>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/20">
            <Th align="right">#</Th>
            <Th>Driver</Th>
            <Th>Team</Th>
            {hasPoints && <Th align="right">Pts</Th>}
            <Th align="right">W</Th>
            <Th align="right">P3</Th>
            <Th align="right">Starts</Th>
          </tr>
        </thead>
        <tbody>
          {rows.map(d => {
            const leaderGap = hasPoints && rows[0]?.points && d.rank > 1
              ? `(-${(rows[0].points - d.points).toFixed(0)})` : ''
            return (
              <tr
                key={d.driver_id}
                className="border-b border-border/60 hover:bg-accent/30 transition-colors"
                style={{ borderLeft: `3px solid ${teamColor(d.team)}` }}
              >
                <Td align="right" className="tabular-nums text-muted-foreground">
                  {d.rank}
                </Td>
                <Td className="font-semibold text-foreground whitespace-nowrap">
                  <span className="font-mono text-[11px] text-muted-foreground mr-2">{d.abbrev}</span>
                  {d.nationality && (
                    <span className="mr-1.5 text-base leading-none">
                      {flagForNationality(d.nationality)}
                    </span>
                  )}
                  {d.name}
                </Td>
                <Td className="text-muted-foreground whitespace-nowrap">{d.team}</Td>
                {hasPoints && (
                  <Td align="right" className="tabular-nums font-semibold">
                    {d.points.toFixed(0)}
                    {leaderGap && (
                      <span className="ml-1 text-[10px] text-muted-foreground tabular-nums">
                        {leaderGap}
                      </span>
                    )}
                  </Td>
                )}
                <Td align="right" className="tabular-nums text-xs">{d.wins}</Td>
                <Td align="right" className="tabular-nums text-xs">{d.podiums}</Td>
                <Td align="right" className="tabular-nums text-xs text-muted-foreground">
                  {d.races}
                </Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}


function ConstructorsChampTable({ rows }) {
  return (
    <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
      <div className="px-3 py-2 border-b border-border bg-background/40">
        <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          Constructors Championship
        </div>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/20">
            <Th align="right">#</Th>
            <Th>Constructor</Th>
            <Th align="right">Pts</Th>
            <Th align="right">W</Th>
            <Th align="right">P3</Th>
          </tr>
        </thead>
        <tbody>
          {rows.map(c => {
            const leaderGap = rows[0]?.points && c.rank > 1
              ? `(-${(rows[0].points - c.points).toFixed(0)})` : ''
            return (
              <tr
                key={c.team_id}
                className="border-b border-border/60 hover:bg-accent/30 transition-colors"
                style={{ borderLeft: `3px solid ${teamColor(c.name)}` }}
              >
                <Td align="right" className="tabular-nums text-muted-foreground">
                  {c.rank}
                </Td>
                <Td className="font-semibold text-foreground whitespace-nowrap">
                  <span
                    className="inline-block w-2 h-2 rounded-full mr-2 align-middle"
                    style={{ backgroundColor: teamColor(c.name) }}
                    aria-hidden="true"
                  />
                  {c.name}
                </Td>
                <Td align="right" className="tabular-nums font-semibold">
                  {c.points.toFixed(0)}
                  {leaderGap && (
                    <span className="ml-1 text-[10px] text-muted-foreground tabular-nums">
                      {leaderGap}
                    </span>
                  )}
                </Td>
                <Td align="right" className="tabular-nums text-xs">{c.wins}</Td>
                <Td align="right" className="tabular-nums text-xs">{c.podiums}</Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

