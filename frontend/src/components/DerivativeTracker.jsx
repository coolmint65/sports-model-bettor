import { useEffect, useMemo, useState } from 'react'
import { humanizeBetType } from '../lib/betType'

/**
 * DerivativeTracker
 * ──────────────────────────────────────────────────────────────
 * Paper-bet log view for Phase 1 derivative markets — separate from
 * the main Pick Tracker so derivative profitability can be evaluated
 * in isolation. Backend auto-records the best derivative pick per
 * game per day at 4%+ edge; this view shows the W/L/profit history
 * per bet_type plus a recent-pick list.
 *
 * Style mirrors PickHistory.jsx (same hero, same per-bet-type tiles,
 * same picks-table) so the two trackers feel like siblings.
 *
 * Props:
 *   sport — 'mlb' | 'nhl' | 'nba'
 *   api   — axios-shaped client with .get() / .post()
 */
export default function DerivativeTracker({ sport, api }) {
  const [summary, setSummary] = useState(null)
  const [history, setHistory] = useState([])
  const [potd, setPotd] = useState(null)
  const [loading, setLoading] = useState(true)

  const refresh = () => {
    setLoading(true)
    Promise.all([
      api.get(`/${sport}/derivative-tracker/summary`).then(r => setSummary(r.data)),
      api.get(`/${sport}/derivative-tracker/history`).then(r => setHistory(r.data)),
      api.get(`/${sport}/derivative-pick-of-day`)
        .then(r => setPotd(r.data && !r.data.message && !r.data.error ? r.data : null))
        .catch(() => setPotd(null)),
    ])
      .catch(() => {})
      .finally(() => setLoading(false))
  }

  useEffect(() => { refresh() }, [sport])

  const settleNow = () => {
    setLoading(true)
    api.post(`/${sport}/derivative-tracker/settle`)
      .then(() => refresh())
      .catch(() => setLoading(false))
  }

  const pct = n => n != null ? `${(n * 100).toFixed(1)}%` : '-'

  // Same today/past split as PickHistory.
  const { todaysPicks, pastPicks } = useMemo(() => {
    const now = new Date()
    const today = `${now.getFullYear()}-${String(now.getMonth()+1).padStart(2,'0')}-${String(now.getDate()).padStart(2,'0')}`
    const todays = []
    const past = []
    for (const p of history || []) {
      if (p.date === today) todays.push(p)
      else past.push(p)
    }
    return { todaysPicks: todays, pastPicks: past }
  }, [history])

  if (loading && !summary) {
    return (
      <div className="loading">
        <div className="spinner" />
        <p>Loading derivative tracker...</p>
      </div>
    )
  }

  const grand = summary?._grand || {}
  // Bet types with at least one row, sorted by ROI desc then volume.
  const typesWithData = summary
    ? Object.entries(summary)
        .filter(([k, v]) => k !== '_grand' && (v?.total || 0) > 0)
        .sort(([, a], [, b]) => {
          if ((b.roi || 0) !== (a.roi || 0)) return (b.roi || 0) - (a.roi || 0)
          return (b.total || 0) - (a.total || 0)
        })
    : []

  return (
    <div className="history-page">
      <div className="history-header">
        <h2 className="section-title">
          {sport.toUpperCase()} Derivative Tracker
          <span className="derivative-tracker-subtitle">
            paper-bet log, 4%+ edge, top 1 per game
          </span>
        </h2>
        <div className="bt-controls">
          <button className="bt-run-btn" onClick={settleNow}
                  style={{ background: '#059669' }}>
            Settle Completed
          </button>
        </div>
      </div>

      {potd && potd.matchup && (
        <div className="pick-hero" style={{ marginBottom: 16, borderColor: 'rgba(168, 85, 247, 0.45)' }}>
          <div className="pick-hero-main">
            <div className="pick-hero-profit-label" style={{ color: '#a855f7' }}>
              {sport.toUpperCase()} DERIVATIVE PICK OF THE DAY
            </div>
            <div style={{ fontSize: '1.4rem', fontWeight: 700, color: '#e2e8f0', margin: '6px 0' }}>
              {potd.matchup} — {humanizeBetType(potd.bet_type)}
            </div>
            <div style={{ fontSize: '1.1rem', color: '#a855f7', fontWeight: 600 }}>
              {potd.pick}
            </div>
            <div className="pick-hero-meta" style={{ marginTop: 6 }}>
              <span>edge {potd.edge != null ? `+${potd.edge.toFixed(1)}%` : '-'}</span>
              <span className="sep">|</span>
              <span>odds {potd.odds > 0 ? '+' : ''}{potd.odds}</span>
              <span className="sep">|</span>
              <span>prob {potd.model_prob ? `${(potd.model_prob * 100).toFixed(1)}%` : '-'}</span>
              {potd.kelly_pct != null && (
                <>
                  <span className="sep">|</span>
                  <span>Kelly {potd.kelly_pct}%</span>
                </>
              )}
            </div>
          </div>
        </div>
      )}

      {grand.total > 0 && (
        <div className="pick-hero">
          <div className="pick-hero-main">
            <div className="pick-hero-profit-label">DERIVATIVE P/L</div>
            <div className={`pick-hero-profit ${
              grand.profit > 0 ? 'positive'
              : grand.profit < 0 ? 'negative' : ''}`}>
              {grand.profit > 0 ? '+' : ''}${(grand.profit || 0).toFixed(2)}
            </div>
            <div className="pick-hero-meta">
              <span>{grand.wins || 0}-{grand.losses || 0}</span>
              <span className="sep">|</span>
              <span>{grand.win_pct || 0}% WR</span>
              <span className="sep">|</span>
              <span>{grand.total || 0} picks</span>
              {grand.pending > 0 && (
                <>
                  <span className="sep">|</span>
                  <span className="pending">{grand.pending} pending</span>
                </>
              )}
            </div>
          </div>

          {/* Per-bet-type tiles, same style as PickHistory's */}
          {typesWithData.length > 0 && (
            <div className="pick-type-tiles">
              {typesWithData.map(([key, s]) => {
                const profitable = s.profit > 0
                return (
                  <div key={key} className={`pick-type-tile ${
                    profitable ? 'profitable'
                    : s.profit < 0 ? 'losing' : ''}`}>
                    <div className="pick-type-label">
                      {humanizeBetType(key)}
                    </div>
                    <div className={`pick-type-profit ${
                      profitable ? 'positive'
                      : s.profit < 0 ? 'negative' : ''}`}>
                      {s.profit > 0 ? '+' : ''}${(s.profit || 0).toFixed(2)}
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

      {todaysPicks.length > 0 && (
        <div className="result-card">
          <h2>Today's Picks ({todaysPicks.length})</h2>
          <DerivativePicksTable picks={todaysPicks} pct={pct} />
        </div>
      )}

      {pastPicks.length > 0 && (
        <div className="result-card" style={{ marginTop: 16 }}>
          <h2>Recent History</h2>
          <DerivativePicksTable picks={pastPicks} pct={pct} />
        </div>
      )}

      {(!history || history.length === 0) && (!summary || grand.total === 0) && (
        <div className="no-games" style={{ marginTop: 20 }}>
          <p>No derivative picks logged yet.</p>
          <p className="sub">
            Auto-records on each best-bets refresh once today's slate
            has playable derivative edges.
          </p>
        </div>
      )}
    </div>
  )
}


function DerivativePicksTable({ picks, pct }) {
  return (
    <table className="picks-table">
      <thead>
        <tr>
          <th>Date</th>
          <th>Matchup</th>
          <th>Market</th>
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
          const resultClass = p.result === 'W' ? 'row-win'
            : p.result === 'L' ? 'row-loss' : 'row-pending'
          return (
            <tr key={p.id || i} className={resultClass}>
              <td className="col-date">{p.date?.slice(5)}</td>
              <td className="col-matchup">{p.matchup}</td>
              <td><span className="type-badge">{humanizeBetType(p.bet_type)}</span></td>
              <td style={{ fontWeight: 600 }}>{p.pick}</td>
              <td style={{ color: '#94a3b8' }}>
                {p.odds ? `${p.odds > 0 ? '+' : ''}${p.odds}` : '-'}
              </td>
              <td>{p.model_prob ? pct(p.model_prob) : '-'}</td>
              <td className={p.edge > 4 ? 'positive' : ''}>
                {p.edge ? `+${p.edge.toFixed(1)}%` : '-'}
              </td>
              <td>
                {p.result === 'W' && <span className="result-pill win">W</span>}
                {p.result === 'L' && <span className="result-pill loss">L</span>}
                {p.result === 'P' && <span className="result-pill push">P</span>}
                {!p.result && <span className="result-pill pending">PEND</span>}
              </td>
              <td className={p.profit > 0 ? 'positive' : p.profit < 0 ? 'negative' : ''}
                  style={{ fontWeight: 600 }}>
                {p.profit != null
                  ? `${p.profit > 0 ? '+' : ''}$${p.profit.toFixed(2)}`
                  : '-'}
              </td>
            </tr>
          )
        })}
      </tbody>
    </table>
  )
}
