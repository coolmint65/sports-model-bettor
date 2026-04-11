export default function NBAStandings({ divisions, loading }) {
  if (loading) {
    return <div className="loading"><div className="spinner" /><p>Loading NBA standings...</p></div>
  }
  if (!divisions || divisions.length === 0) {
    return <div className="no-games"><p>No NBA standings data available.</p></div>
  }

  const EAST = ['Atlantic', 'Central', 'Southeast']
  const WEST = ['Northwest', 'Pacific', 'Southwest']
  const eastDivs = divisions.filter(d => EAST.includes(d.name))
  const westDivs = divisions.filter(d => WEST.includes(d.name))
  const other = divisions.filter(d => !EAST.includes(d.name) && !WEST.includes(d.name))

  return (
    <div className="standings-page">
      <h2 className="section-title">NBA Standings</h2>
      <div className="standings-legend">
        <div className="legend-item"><span className="dot spot-division" /> Playoff spot (1-6)</div>
        <div className="legend-item"><span className="dot spot-wildcard" /> Play-in (7-10)</div>
        <div className="legend-item"><span className="dot spot-out" /> Out</div>
      </div>
      {eastDivs.length > 0 && (
        <>
          <div className="conference-header">Eastern Conference</div>
          <div className="standings-conf-grid">
            {eastDivs.map(div => <DivTable key={div.name} div={div} />)}
          </div>
        </>
      )}
      {westDivs.length > 0 && (
        <>
          <div className="conference-header">Western Conference</div>
          <div className="standings-conf-grid">
            {westDivs.map(div => <DivTable key={div.name} div={div} />)}
          </div>
        </>
      )}
      {other.length > 0 && other.map(div => <DivTable key={div.name} div={div} />)}
    </div>
  )
}

function DivTable({ div }) {
  return (
    <div className="standings-division-card">
      <div className="standings-division-title">{div.name}</div>
      <table className="standings-table-v2">
        <thead>
          <tr>
            <th className="rank-col">#</th>
            <th className="team-col">Team</th>
            <th>W</th><th>L</th><th>PCT</th><th>DIFF</th><th>STRK</th><th>L10</th>
          </tr>
        </thead>
        <tbody>
          {div.teams.map((t, i) => (
            <tr key={t.abbreviation || i} className={i < 2 ? 'spot-division' : i < 4 ? 'spot-wildcard' : 'spot-out'}>
              <td className="rank-col">{i + 1}</td>
              <td className="team-col">
                {t.logo && <img src={t.logo} alt="" className="team-logo-sm" />}
                <span className="standing-abbr">{t.abbreviation}</span>
                <span className="standing-name">{t.name}</span>
              </td>
              <td>{t.wins}</td><td>{t.losses}</td>
              <td className="pts-col">{t.pct || '.000'}</td>
              <td className={t.diff > 0 ? 'positive' : t.diff < 0 ? 'negative' : ''}>{t.diff > 0 ? '+' : ''}{t.diff || 0}</td>
              <td className={t.streak?.startsWith('W') ? 'positive' : t.streak?.startsWith('L') ? 'negative' : ''}>{t.streak}</td>
              <td>{t.l10}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
