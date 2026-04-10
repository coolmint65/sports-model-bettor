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

  // Split history: today's picks vs historical
  const today = new Date().toISOString().slice(0, 10)
  const todaysPicks = (history || []).filter(p => p.date === today)
  const pastPicks = (history || []).filter(p => p.date !== today)

  // Bet types that have data
  const typesWithData = [
    { key: 'ML', label: 'Moneyline' },
    { key: 'O/U', label: 'Over/Under' },
    { key: '1st INN', label: '1st Inning' },
    { key: 'RL', label: 'Run Line' },
    { key: 'PL', label: 'Puck Line' },
  ].filter(({ key }) => byType[key] && byType[key].total > 0)

  return (
    <div className="history-page">
      <div className="history-header">
        <h2 className="section-title">Pick Tracker</h2>
        <div className="bt-controls">
          <button className="bt-run-btn" onClick={onRecord}>Record Today's Picks</button>
          <button className="bt-run-btn" onClick={onSettle} style={{ background: '#059669' }}>Settle Completed</button>
        </div>
      </div>

      {/* Hero summary — big, prominent */}
      {overall.total > 0 && (
        <div className="pick-hero">
          <div className="pick-hero-main">
            <div className="pick-hero-profit-label">OVERALL P/L</div>
            <div className={`pick-hero-profit ${overall.profit > 0 ? 'positive' : overall.profit < 0 ? 'negative' : ''}`}>
              {overall.profit > 0 ? '+' : ''}${overall.profit}
            </div>
            <div className="pick-hero-meta">
              <span>{overall.wins}-{overall.losses}</span>
              <span className="sep">|</span>
              <span>{overall.win_pct}% WR</span>
              <span className="sep">|</span>
              <span>{overall.total} picks</span>
              {overall.pending > 0 && (
                <>
                  <span className="sep">|</span>
                  <span className="pending">{overall.pending} pending</span>
                </>
              )}
            </div>
          </div>

          {/* Compact bet type tiles */}
          {typesWithData.length > 0 && (
            <div className="pick-type-tiles">
              {typesWithData.map(({ key, label }) => {
                const s = byType[key]
                const profitable = s.profit > 0
                return (
                  <div key={key} className={`pick-type-tile ${profitable ? 'profitable' : s.profit < 0 ? 'losing' : ''}`}>
                    <div className="pick-type-label">{label}</div>
                    <div className={`pick-type-profit ${profitable ? 'positive' : s.profit < 0 ? 'negative' : ''}`}>
                      {s.profit > 0 ? '+' : ''}${s.profit}
                    </div>
                    <div className="pick-type-record">
                      {s.wins}-{s.losses} ({s.win_pct}%)
                    </div>
                  </div>
                )
              })}
            </div>
          )}
        </div>
      )}

      {/* Today's picks (live activity) */}
      {todaysPicks.length > 0 && (
        <div className="result-card">
          <h2>Today's Picks ({todaysPicks.length})</h2>
          <PicksTable picks={todaysPicks} pct={pct} />
        </div>
      )}

      {/* Historical picks */}
      {pastPicks.length > 0 && (
        <div className="result-card" style={{ marginTop: 16 }}>
          <h2>Recent History</h2>
          <PicksTable picks={pastPicks} pct={pct} />
        </div>
      )}

      {(!history || history.length === 0) && (!summary || overall.total === 0) && (
        <div className="no-games" style={{ marginTop: 20 }}>
          <p>No picks recorded yet.</p>
          <p className="sub">Click "Record Today's Picks" to start tracking.</p>
        </div>
      )}
    </div>
  )
}


function PicksTable({ picks, pct }) {
  return (
    <table className="picks-table">
      <thead>
        <tr>
          <th>Date</th>
          <th>Matchup</th>
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
        {picks.map((p, i) => {
          const resultClass = p.result === 'W' ? 'row-win' : p.result === 'L' ? 'row-loss' : 'row-pending'
          return (
            <tr key={p.id || i} className={resultClass}>
              <td className="col-date">{p.date?.slice(5)}</td>
              <td className="col-matchup">{p.matchup}</td>
              <td><span className="type-badge">{p.bet_type?.toUpperCase()}</span></td>
              <td style={{ fontWeight: 600 }}>{p.pick}</td>
              <td style={{ color: '#94a3b8' }}>{p.odds ? `${p.odds > 0 ? '+' : ''}${p.odds}` : '-'}</td>
              <td>{p.model_prob ? pct(p.model_prob) : '-'}</td>
              <td className={p.edge > 4 ? 'positive' : ''}>{p.edge ? `+${p.edge.toFixed(1)}%` : '-'}</td>
              <td>
                {p.result === 'W' && <span className="result-pill win">W</span>}
                {p.result === 'L' && <span className="result-pill loss">L</span>}
                {p.result === 'P' && <span className="result-pill push">P</span>}
                {!p.result && <span className="result-pill pending">PEND</span>}
              </td>
              <td className={p.profit > 0 ? 'positive' : p.profit < 0 ? 'negative' : ''} style={{ fontWeight: 600 }}>
                {p.profit != null ? `${p.profit > 0 ? '+' : ''}$${p.profit}` : '-'}
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}
