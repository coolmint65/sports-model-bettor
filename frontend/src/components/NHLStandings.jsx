import StandingsTable from './primitives/StandingsTable'

/**
 * NHL Standings — composes the shared StandingsTable primitive with
 * NHL-specific column set (GP / W / L / OTL / PTS / GF / GA / DIFF /
 * L10 / STRK) and spot rule.
 */
const COLUMNS = [
  { label: 'GP',   value: t => t.wins + t.losses + (t.otl || 0) },
  { label: 'W',    value: t => t.wins },
  { label: 'L',    value: t => t.losses },
  { label: 'OTL',  value: t => t.otl || 0 },
  { label: 'PTS',  value: t => t.points, emphasis: true },
  { label: 'GF',   value: t => t.gf },
  { label: 'GA',   value: t => t.ga },
  { label: 'DIFF', value: t => t.diff, format: 'signed' },
  { label: 'L10',  value: t => t.l10 },
  { label: 'STRK', value: t => t.streak, format: 'streak' },
]

// NHL: top 3 in division = playoff spot, 4-5 = wildcard contention, 6+ = out.
const SPOT_RULE = (i) => i < 3 ? 'division' : i < 5 ? 'wildcard' : 'out'

const CONFERENCES = [
  { label: 'Eastern Conference', divisionNames: ['Atlantic', 'Metropolitan'] },
  { label: 'Western Conference', divisionNames: ['Central', 'Pacific'] },
]

export default function NHLStandings({ divisions, loading }) {
  return (
    <StandingsTable
      title="NHL Standings"
      divisions={divisions}
      loading={loading}
      loadingLabel="Loading NHL standings…"
      conferences={CONFERENCES}
      spotRule={SPOT_RULE}
      spotLegend={{ division: 'Division spot', wildcard: 'Wild card', out: 'Out of playoffs' }}
      columns={COLUMNS}
      showLogo
      emptyMessage="No NHL standings data available."
    />
  )
}
