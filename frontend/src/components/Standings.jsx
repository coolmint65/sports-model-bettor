export default function Standings({ divisions }) {
  if (!divisions || divisions.length === 0) {
    return (
      <div className="no-games">
        <p>No standings data available.</p>
        <p className="sub">Run <code style={{ background: '#1e293b', padding: '2px 8px', borderRadius: 4 }}>sync.bat</code> to pull standings from MLB.</p>
      </div>
    )
  }

  const filled = divisions.filter(d => d.teams && d.teams.length > 0)
  if (filled.length === 0) {
    return (
      <div className="no-games">
        <p>Standings data is empty — season may not have started yet.</p>
        <p className="sub">Run <code style={{ background: '#1e293b', padding: '2px 8px', borderRadius: 4 }}>sync.bat</code> to pull the latest data.</p>
      </div>
    )
  }

  const al = filled.filter(d => d.league === 'AL')
  const nl = filled.filter(d => d.league === 'NL')

  return (
    <div className="standings-page">
      <h2 className="section-title">MLB Standings</h2>

      <div className="standings-legend">
        <div className="legend-item"><span className="dot spot-division" /> Division leader</div>
        <div className="legend-item"><span className="dot spot-wildcard" /> Wild card contention</div>
        <div className="legend-item"><span className="dot spot-out" /> Out of playoffs</div>
      </div>

      {al.length > 0 && (
        <>
          <div className="conference-header">American League</div>
          <div className="standings-conf-grid">
            {al.map(div => <MLBDivisionTable key={div.division} div={div} />)}
          </div>
        </>
      )}

      {nl.length > 0 && (
        <>
          <div className="conference-header">National League</div>
          <div className="standings-conf-grid">
            {nl.map(div => <MLBDivisionTable key={div.division} div={div} />)}
          </div>
        </>
      )}
    </div>
  )
}


function MLBDivisionTable({ div }) {
  return (
    <div className="standings-division-card">
      <div className="standings-division-title">{div.division}</div>
      <table className="standings-table-v2">
        <thead>
          <tr>
            <th className="rank-col">#</th>
            <th className="team-col">Team</th>
            <th>W</th>
            <th>L</th>
            <th>PCT</th>
            <th>DIFF</th>
            <th>L10</th>
            <th>STRK</th>
          </tr>
        </thead>
        <tbody>
          {div.teams.map((team, i) => {
            // Top 1 = division leader, 2-3 = wild card contention, 4+ = out
            const spotClass =
              i === 0 ? 'spot-division' :
              i < 3 ? 'spot-wildcard' :
              'spot-out'
            return (
              <tr key={team.id} className={spotClass}>
                <td className="rank-col">{i + 1}</td>
                <td className="team-col">
                  <span className="standing-abbr">{team.abbreviation}</span>
                  <span className="standing-name">{team.name}</span>
                </td>
                <td>{team.wins}</td>
                <td>{team.losses}</td>
                <td className="pts-col">{team.pct}</td>
                <td className={team.run_diff > 0 ? 'positive' : team.run_diff < 0 ? 'negative' : ''}>
                  {team.run_diff > 0 ? '+' : ''}{team.run_diff}
                </td>
                <td>{team.last_10}</td>
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
