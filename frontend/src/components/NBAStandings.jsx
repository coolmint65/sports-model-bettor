import StandingsTable from './primitives/StandingsTable'

/**
 * NBA Standings — composes the shared StandingsTable primitive with
 * NBA-specific column set + play-in spot rule.
 */
const COLUMNS = [
  { label: 'W',    value: t => t.wins },
  { label: 'L',    value: t => t.losses },
  { label: 'PCT',  value: t => t.pct || '.000', emphasis: true },
  { label: 'DIFF', value: t => t.diff || 0, format: 'signed' },
  { label: 'STRK', value: t => t.streak, format: 'streak' },
  { label: 'L10',  value: t => t.l10 },
]

// NBA: top 2 in division = direct playoff spot, 3-4 = play-in, 5+ = out.
// (Per-division tightening of the conference-wide 1-6 / 7-10 rule.)
const SPOT_RULE = (i) => i < 2 ? 'division' : i < 4 ? 'wildcard' : 'out'

const CONFERENCES = [
  { label: 'Eastern Conference', divisionNames: ['Atlantic', 'Central', 'Southeast'] },
  { label: 'Western Conference', divisionNames: ['Northwest', 'Pacific', 'Southwest'] },
]

export default function NBAStandings({ divisions, loading }) {
  return (
    <StandingsTable
      title="NBA Standings"
      divisions={divisions}
      loading={loading}
      loadingLabel="Loading NBA standings…"
      conferences={CONFERENCES}
      spotRule={SPOT_RULE}
      spotLegend={{ division: 'Playoff spot', wildcard: 'Play-in', out: 'Out' }}
      columns={COLUMNS}
      showLogo
      emptyMessage="No NBA standings data available."
    />
  )
}
