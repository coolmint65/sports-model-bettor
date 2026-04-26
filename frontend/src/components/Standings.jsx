import StandingsTable from './primitives/StandingsTable'

/**
 * MLB Standings — composes the shared StandingsTable primitive
 * with MLB-specific column set + spot rule.
 */
const COLUMNS = [
  { label: 'W',    value: t => t.wins },
  { label: 'L',    value: t => t.losses },
  { label: 'PCT',  value: t => t.pct, emphasis: true },
  { label: 'DIFF', value: t => t.run_diff, format: 'signed' },
  { label: 'L10',  value: t => t.last_10 },
  { label: 'STRK', value: t => t.streak, format: 'streak' },
]

// MLB: top 1 = division leader, 2-3 = wild card contention, 4+ = out.
const SPOT_RULE = (i) => i === 0 ? 'division' : i < 3 ? 'wildcard' : 'out'

export default function Standings({ divisions }) {
  // MLB groups by AL/NL via the `league` field rather than division name,
  // so build conferences dynamically from what's in the payload.
  const al = (divisions || []).filter(d => d.league === 'AL').map(d => d.division)
  const nl = (divisions || []).filter(d => d.league === 'NL').map(d => d.division)
  const conferences = []
  if (al.length) conferences.push({ label: 'American League', divisionNames: al })
  if (nl.length) conferences.push({ label: 'National League', divisionNames: nl })

  return (
    <StandingsTable
      title="MLB Standings"
      divisions={divisions}
      conferences={conferences}
      spotRule={SPOT_RULE}
      spotLegend={{
        division: 'Division leader',
        wildcard: 'Wild card contention',
        out:      'Out of playoffs',
      }}
      columns={COLUMNS}
      getDivKey={d => d.division}
      emptyMessage="No standings data available."
      emptyHint="Run sync.bat to pull standings from MLB."
    />
  )
}
