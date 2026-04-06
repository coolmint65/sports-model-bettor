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

  return (
    <div className="standings-page">
      <h2 className="section-title">NHL Standings</h2>

      <div className="standings-leagues">
        {divisions.map(div => (
          <div key={div.name} className="division" style={{marginBottom: 24}}>
            <h4 className="division-name">{div.name}</h4>
            <table className="standings-table">
              <thead>
                <tr>
                  <th className="team-col">Team</th>
                  <th>GP</th>
                  <th>W</th>
                  <th>L</th>
                  <th>OTL</th>
                  <th>PTS</th>
                  <th>GF</th>
                  <th>GA</th>
                  <th>DIFF</th>
                  <th>STRK</th>
                  <th>L10</th>
                </tr>
              </thead>
              <tbody>
                {div.teams.map((team, i) => {
                  const gp = team.wins + team.losses + (team.otl || 0)
                  return (
                    <tr key={team.abbreviation || i} className={i === 0 ? 'division-leader' : ''}>
                      <td className="team-col">
                        {team.logo && <img src={team.logo} alt="" className="team-logo-sm" style={{width:20,height:20,marginRight:6,verticalAlign:'middle'}} />}
                        <span className="standing-abbr">{team.abbreviation}</span>
                        <span className="standing-name">{team.name}</span>
                      </td>
                      <td>{gp}</td>
                      <td>{team.wins}</td>
                      <td>{team.losses}</td>
                      <td>{team.otl || 0}</td>
                      <td style={{fontWeight: 600}}>{team.points}</td>
                      <td>{team.gf}</td>
                      <td>{team.ga}</td>
                      <td className={team.diff > 0 ? 'positive' : team.diff < 0 ? 'negative' : ''}>
                        {team.diff > 0 ? '+' : ''}{team.diff}
                      </td>
                      <td className={team.streak?.startsWith('W') ? 'positive' : team.streak?.startsWith('L') ? 'negative' : ''}>
                        {team.streak}
                      </td>
                      <td>{team.l10}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        ))}
      </div>
    </div>
  )
}
