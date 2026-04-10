export default function NHLStandings({ divisions, loading }) {
  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Loading NHL standings...</p>
      </div>
    )
  }

  if (!divisions || divisions.length === 0) {
    return (
      <div className="no-games">
        <p>No NHL standings data available.</p>
      </div>
    )
  }

  // Group divisions by conference
  const EAST = ['Atlantic', 'Metropolitan']
  const WEST = ['Central', 'Pacific']

  const eastDivs = divisions.filter(d => EAST.includes(d.name))
  const westDivs = divisions.filter(d => WEST.includes(d.name))
  const other = divisions.filter(d => !EAST.includes(d.name) && !WEST.includes(d.name))

  return (
    <div className="standings-page">
      <h2 className="section-title">NHL Standings</h2>

      <div className="standings-legend">
        <div className="legend-item"><span className="dot spot-division" /> Division spot</div>
        <div className="legend-item"><span className="dot spot-wildcard" /> Wild card</div>
        <div className="legend-item"><span className="dot spot-out" /> Out of playoffs</div>
      </div>

      {eastDivs.length > 0 && (
        <>
          <div className="conference-header">Eastern Conference</div>
          <div className="standings-conf-grid">
            {eastDivs.map(div => <DivisionTable key={div.name} div={div} />)}
          </div>
        </>
      )}

      {westDivs.length > 0 && (
        <>
          <div className="conference-header">Western Conference</div>
          <div className="standings-conf-grid">
            {westDivs.map(div => <DivisionTable key={div.name} div={div} />)}
          </div>
        </>
      )}

      {other.length > 0 && (
        <div className="standings-conf-grid">
          {other.map(div => <DivisionTable key={div.name} div={div} />)}
        </div>
      )}
    </div>
  )
}


function DivisionTable({ div }) {
  return (
    <div className="standings-division-card">
      <div className="standings-division-title">{div.name}</div>
      <table className="standings-table-v2">
        <thead>
          <tr>
            <th className="rank-col">#</th>
            <th className="team-col">Team</th>
            <th>GP</th>
            <th>W</th>
            <th>L</th>
            <th>OTL</th>
            <th>PTS</th>
            <th>GF</th>
            <th>GA</th>
            <th>DIFF</th>
            <th>L10</th>
            <th>STRK</th>
          </tr>
        </thead>
        <tbody>
          {div.teams.map((team, i) => {
            const gp = team.wins + team.losses + (team.otl || 0)
            // Top 3 = division spot (green), 4-5 = wildcard contention (yellow), 6+ = out (red)
            const spotClass =
              i < 3 ? 'spot-division' :
              i < 5 ? 'spot-wildcard' :
              'spot-out'
            return (
              <tr key={team.abbreviation || i} className={spotClass}>
                <td className="rank-col">{i + 1}</td>
                <td className="team-col">
                  {team.logo && <img src={team.logo} alt="" className="team-logo-sm" />}
                  <span className="standing-abbr">{team.abbreviation}</span>
                  <span className="standing-name">{team.name}</span>
                </td>
                <td>{gp}</td>
                <td>{team.wins}</td>
                <td>{team.losses}</td>
                <td>{team.otl || 0}</td>
                <td className="pts-col">{team.points}</td>
                <td>{team.gf}</td>
                <td>{team.ga}</td>
                <td className={team.diff > 0 ? 'positive' : team.diff < 0 ? 'negative' : ''}>
                  {team.diff > 0 ? '+' : ''}{team.diff}
                </td>
                <td>{team.l10}</td>
                <td className={team.streak?.startsWith('W') ? 'positive' : team.streak?.startsWith('L') ? 'negative' : ''}>
                  {team.streak}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
