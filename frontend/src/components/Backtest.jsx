import { useState } from 'react'

export default function Backtest({ data, loading, onRun }) {
  const currentYear = new Date().getFullYear()
  const [season, setSeason] = useState(String(currentYear))
  const [days, setDays] = useState('')
  const [minEdge, setMinEdge] = useState('3')

  // Always show controls even when loading/empty
  const controls = (
    <div className="bt-controls">
      <div className="bt-control">
        <label>Season</label>
        <select value={season} onChange={e => setSeason(e.target.value)} className="bt-select">
          {[currentYear, currentYear - 1, currentYear - 2, currentYear - 3].map(y => (
            <option key={y} value={y}>{y}</option>
          ))}
        </select>
      </div>
      <div className="bt-control">
        <label>Last N days</label>
        <input
          type="number"
          placeholder="All"
          value={days}
          onChange={e => setDays(e.target.value)}
        />
      </div>
      <div className="bt-control">
        <label>Min edge %</label>
        <input
          type="number"
          placeholder="0"
          step="0.5"
          value={minEdge}
          onChange={e => setMinEdge(e.target.value)}
        />
      </div>
      <button
        className="bt-run-btn"
        onClick={() => onRun(days || 0, minEdge || 0, season)}
        disabled={loading}
      >
        {loading ? 'Running...' : 'Run Backtest'}
      </button>
    </div>
  )

  if (loading) {
    return (
      <div className="backtest-page">
        <h2 className="section-title">Model Performance</h2>
        {controls}
        <div className="loading">
          <div className="spinner" />
          <p>
            {parseInt(season) < currentYear
              ? `Loading ${season} season data and running backtest... This may take a minute.`
              : 'Running backtest against historical games...'}
          </p>
        </div>
      </div>
    )
  }

  if (!data || data.error) {
    return (
      <div className="backtest-page">
        <h2 className="section-title">Model Performance</h2>
        {controls}
        <div className="no-games">
          <p>{data?.error || 'Select a season and click Run Backtest.'}</p>
        </div>
      </div>
    )
  }

  const cats = [
    { key: 'moneyline', label: 'Moneyline' },
    { key: 'over_under', label: 'Over/Under' },
    { key: 'nrfi', label: 'NRFI / YRFI' },
    { key: 'run_line', label: 'Run Line' },
  ]

  // Use best-bet-per-game for summary (one bet per game, highest conviction)
  const bb = data.best_bet || {}
  const totalProfit = bb.profit || 0
  const totalBets = bb.total_bets || 0
  const totalWins = bb.wins || 0
  const totalLosses = bb.losses || 0

  return (
    <div className="backtest-page">
      <h2 className="section-title">Model Performance — {season} Season</h2>
      {controls}

      {/* Summary */}
      <div className="result-card">
        <h2>Summary</h2>
        <div className="bt-summary">
          <div className="bt-summary-stat">
            <span className="bt-big">{data.games_tested}</span>
            <span className="bt-label">Games</span>
          </div>
          <div className="bt-summary-stat">
            <span className="bt-big">{totalBets}</span>
            <span className="bt-label">Bets</span>
          </div>
          <div className="bt-summary-stat">
            <span className="bt-big">{totalWins}-{totalLosses}</span>
            <span className="bt-label">Record</span>
          </div>
          <div className="bt-summary-stat">
            <span className={`bt-big ${totalProfit > 0 ? 'positive' : totalProfit < 0 ? 'negative' : ''}`}>
              ${totalProfit.toFixed(0)}
            </span>
            <span className="bt-label">Profit</span>
          </div>
          <div className="bt-summary-stat">
            <span className={`bt-big ${totalProfit > 0 ? 'positive' : totalProfit < 0 ? 'negative' : ''}`}>
              {totalBets > 0 ? (totalProfit / totalBets).toFixed(1) : '0'}%
            </span>
            <span className="bt-label">ROI</span>
          </div>
        </div>
      </div>

      {/* Per-category breakdown */}
      <div className="bt-grid">
        {cats.map(({ key, label }) => {
          const bt = data[key]
          if (!bt || bt.total_bets === 0) return null
          const profitable = bt.profit > 0

          return (
            <div key={key} className={`result-card bt-card ${profitable ? 'bt-profitable' : ''}`}>
              <h2>{label}</h2>
              <div className="bt-card-stats">
                <div className="bt-row">
                  <span className="stat-label">Record</span>
                  <span className="stat-value">{bt.wins}-{bt.losses}</span>
                </div>
                <div className="bt-row">
                  <span className="stat-label">Win Rate</span>
                  <span className={`stat-value ${bt.win_pct > 55 ? 'positive' : bt.win_pct < 45 ? 'negative' : ''}`}>
                    {bt.win_pct}%
                  </span>
                </div>
                <div className="bt-row">
                  <span className="stat-label">Profit</span>
                  <span className={`stat-value ${profitable ? 'positive' : 'negative'}`}>
                    ${bt.profit.toFixed(0)}
                  </span>
                </div>
                <div className="bt-row">
                  <span className="stat-label">ROI</span>
                  <span className={`stat-value ${bt.roi > 0 ? 'positive' : 'negative'}`}>
                    {bt.roi > 0 ? '+' : ''}{bt.roi}%
                  </span>
                </div>
              </div>
              <div className={`bt-verdict ${profitable ? 'positive' : 'negative'}`}>
                {profitable ? 'PROFITABLE' : 'LOSING'}
              </div>
            </div>
          )
        })}
      </div>

      <div className="bt-note">
        Based on $100 flat bets. ML uses -150/+130 standard lines.
        O/U and Run Line use -110. NRFI uses -120. Past performance
        does not guarantee future results.
      </div>
    </div>
  )
}
