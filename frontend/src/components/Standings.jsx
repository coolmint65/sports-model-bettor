export default function Standings({ divisions }) {
  if (!divisions || divisions.length === 0) {
    return (
      <div className="no-games">
        <p>No standings data available.</p>
        <p className="sub">Run <code style={{background:'#1e293b',padding:'2px 8px',borderRadius:4}}>sync.bat</code> to pull standings from MLB.</p>
      </div>
    )
  }

  // Filter out empty divisions (no teams)
  const filled = divisions.filter(d => d.teams && d.teams.length > 0)
  if (filled.length === 0) {
    return (
      <div className="no-games">
        <p>Standings data is empty — season may not have started yet.</p>
        <p className="sub">Run <code style={{background:'#1e293b',padding:'2px 8px',borderRadius:4}}>sync.bat</code> to pull the latest data.</p>
      </div>
    )
  }

  const al = filled.filter(d => d.league === 'AL')
  const nl = filled.filter(d => d.league === 'NL')

  return (
    <div className="standings-page">
      <h2 className="section-title">MLB Standings</h2>

      <div className="standings-leagues">
        {al.length > 0 && <LeagueStandings name="American League" divisions={al} />}
        {nl.length > 0 && <LeagueStandings name="National League" divisions={nl} />}
      </div>
    </div>
  )
}

function LeagueStandings({ name, divisions }) {
  return (
    <div className="league-standings">
      <h3 className="league-name">{name}</h3>
      {divisions.map(div => (
        <div key={`${div.league}-${div.division}`} className="division">
          <h4 className="division-name">{div.division}</h4>
          <table className="standings-table">
            <thead>
              <tr>
                <th className="team-col">Team</th>
                <th>W</th>
                <th>L</th>
                <th>PCT</th>
                <th>DIFF</th>
                <th>STRK</th>
                <th>L10</th>
              </tr>
            </thead>
            <tbody>
              {div.teams.map((team, i) => (
                <tr key={team.id} className={i === 0 ? 'division-leader' : ''}>
                  <td className="team-col">
                    <span className="standing-abbr">{team.abbreviation}</span>
                    <span className="standing-name">{team.name}</span>
                  </td>
                  <td>{team.wins}</td>
                  <td>{team.losses}</td>
                  <td>{team.pct}</td>
                  <td className={team.run_diff > 0 ? 'positive' : team.run_diff < 0 ? 'negative' : ''}>
                    {team.run_diff > 0 ? '+' : ''}{team.run_diff}
                  </td>
                  <td className={team.streak?.startsWith('W') ? 'positive' : team.streak?.startsWith('L') ? 'negative' : ''}>
                    {team.streak}
                  </td>
                  <td>{team.last_10}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ))}
    </div>
  )
}
