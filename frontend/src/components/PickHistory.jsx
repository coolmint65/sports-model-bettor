export default function PickHistory({ summary, history, loading, onRecord, onSettle }) {
  const pct = n => `${(n * 100).toFixed(1)}%`

  if (loading) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Loading pick history...</p>
      </div>
    )
  }

  const overall = summary?.overall || {}
  const byType = summary?.by_type || {}

  return (
    <div className="history-page">
      <h2 className="section-title">Pick History</h2>

      {/* Actions */}
      <div className="bt-controls">
        <button className="bt-run-btn" onClick={onRecord}>Record Today's Picks</button>
        <button className="bt-run-btn" onClick={onSettle} style={{background: '#059669'}}>Settle Completed</button>
      </div>

      {/* Running totals */}
      {overall.total > 0 && (
        <div className="result-card">
          <h2>Running Totals</h2>
          <div className="bt-summary">
            <div className="bt-summary-stat">
              <span className="bt-big">{overall.total}</span>
              <span className="bt-label">Picks</span>
            </div>
            <div className="bt-summary-stat">
              <span className="bt-big">{overall.wins}-{overall.losses}</span>
              <span className="bt-label">Record</span>
            </div>
            <div className="bt-summary-stat">
              <span className={`bt-big ${overall.profit > 0 ? 'positive' : overall.profit < 0 ? 'negative' : ''}`}>
                ${overall.profit}
              </span>
              <span className="bt-label">Profit</span>
            </div>
            <div className="bt-summary-stat">
              <span className="bt-big">{overall.win_pct}%</span>
              <span className="bt-label">Win Rate</span>
            </div>
            <div className="bt-summary-stat">
              <span className="bt-big">{overall.pending}</span>
              <span className="bt-label">Pending</span>
            </div>
          </div>
        </div>
      )}

      {/* Per-type breakdown */}
      {Object.keys(byType).length > 0 && (
        <div className="bt-grid" style={{marginTop: 16}}>
          {[
            {key: 'ML', label: 'Moneyline'},
            {key: 'O/U', label: 'Over/Under'},
            {key: '1st INN', label: '1st Inning'},
            {key: 'RL', label: 'Run Line'},
          ].map(({key, label}) => {
            const s = byType[key]
            if (!s || s.total === 0) return null
            return (
              <div key={key} className={`result-card bt-card ${s.profit > 0 ? 'bt-profitable' : ''}`}>
                <h2>{label}</h2>
                <div className="bt-row">
                  <span className="stat-label">Record</span>
                  <span className="stat-value">{s.wins}-{s.losses}</span>
                </div>
                <div className="bt-row">
                  <span className="stat-label">Profit</span>
                  <span className={`stat-value ${s.profit > 0 ? 'positive' : 'negative'}`}>${s.profit}</span>
                </div>
                <div className="bt-row">
                  <span className="stat-label">Pending</span>
                  <span className="stat-value">{s.pending}</span>
                </div>
              </div>
            )
          })}
        </div>
      )}

      {/* Recent picks list */}
      {history && history.length > 0 && (
        <div className="result-card" style={{marginTop: 16}}>
          <h2>Recent Picks</h2>
          <table className="standings-table">
            <thead>
              <tr>
                <th className="team-col">Date</th>
                <th className="team-col">Matchup</th>
                <th>Type</th>
                <th>Pick</th>
                <th>Odds</th>
                <th>Prob</th>
                <th>Edge</th>
                <th>Result</th>
                <th>P/L</th>
              </tr>
            </thead>
            <tbody>
              {history.map((p, i) => (
                <tr key={p.id || i}>
                  <td className="team-col">{p.date}</td>
                  <td className="team-col">{p.matchup}</td>
                  <td>{p.bet_type?.toUpperCase()}</td>
                  <td style={{fontWeight: 600}}>{p.pick}</td>
                  <td style={{color: '#94a3b8'}}>{p.odds ? `${p.odds > 0 ? '+' : ''}${p.odds}` : '-'}</td>
                  <td>{p.model_prob ? pct(p.model_prob) : '-'}</td>
                  <td className={p.edge > 4 ? 'positive' : ''}>{p.edge ? `+${p.edge.toFixed(1)}%` : '-'}</td>
                  <td>
                    {p.result === 'W' && <span className="positive" style={{fontWeight:700}}>W</span>}
                    {p.result === 'L' && <span className="negative" style={{fontWeight:700}}>L</span>}
                    {!p.result && <span style={{color:'#64748b'}}>PENDING</span>}
                  </td>
                  <td className={p.profit > 0 ? 'positive' : p.profit < 0 ? 'negative' : ''}>
                    {p.profit != null ? `$${p.profit > 0 ? '+' : ''}${p.profit}` : '-'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {(!history || history.length === 0) && (!summary || overall.total === 0) && (
        <div className="no-games" style={{marginTop: 20}}>
          <p>No picks recorded yet.</p>
          <p className="sub">Click "Record Today's Picks" to start tracking.</p>
        </div>
      )}
    </div>
  )
}
