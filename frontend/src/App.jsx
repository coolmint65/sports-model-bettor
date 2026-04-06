import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import Scoreboard from './components/Scoreboard'
import GameDetail from './components/GameDetail'
import Standings from './components/Standings'
import Backtest from './components/Backtest'
import BestBets from './components/BestBets'
import PickHistory from './components/PickHistory'
import NHLScoreboard from './components/NHLScoreboard'
import NHLStandings from './components/NHLStandings'

const api = axios.create({ baseURL: '/api' })

export default function App() {
  const [league, setLeague] = useState('MLB')
  const [view, setView] = useState('games')

  // MLB state
  const [games, setGames] = useState([])
  const [gamesLoading, setGamesLoading] = useState(true)
  const [selectedGame, setSelectedGame] = useState(null)
  const [prediction, setPrediction] = useState(null)
  const [predLoading, setPredLoading] = useState(false)
  const [standings, setStandings] = useState([])
  const [backtest, setBacktest] = useState(null)
  const [btLoading, setBtLoading] = useState(false)
  const [bestBets, setBestBets] = useState(null)
  const [bbLoading, setBbLoading] = useState(false)
  const [pickSummary, setPickSummary] = useState(null)
  const [pickHistory, setPickHistory] = useState(null)
  const [phLoading, setPhLoading] = useState(false)

  // NHL state
  const [nhlGames, setNhlGames] = useState([])
  const [nhlLoading, setNhlLoading] = useState(true)
  const [nhlBestBets, setNhlBestBets] = useState(null)
  const [nhlBbLoading, setNhlBbLoading] = useState(false)
  const [nhlStandings, setNhlStandings] = useState([])
  const [nhlStandingsLoading, setNhlStandingsLoading] = useState(false)
  const [nhlPrediction, setNhlPrediction] = useState(null)
  const [nhlPredLoading, setNhlPredLoading] = useState(false)
  const [nhlSelectedGame, setNhlSelectedGame] = useState(null)

  // Load MLB games on mount + auto-refresh every 5 min
  useEffect(() => {
    const fetchGames = () => {
      api.get('/scoreboard').then(r => setGames(r.data)).catch(() => {})
    }
    setGamesLoading(true)
    Promise.all([
      api.get('/scoreboard'),
      api.get('/best-bets'),
    ]).then(([g, b]) => {
      setGames(g.data)
      setBestBets(b.data)
    }).catch(() => setGames([]))
      .finally(() => setGamesLoading(false))
    const interval = setInterval(() => {
      fetchGames()
      api.get('/best-bets').then(r => setBestBets(r.data)).catch(() => {})
    }, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

  // Load NHL games on mount + auto-refresh
  useEffect(() => {
    setNhlLoading(true)
    Promise.all([
      api.get('/nhl/scoreboard'),
      api.get('/nhl/best-bets'),
    ]).then(([g, b]) => {
      setNhlGames(g.data)
      setNhlBestBets(b.data)
    }).catch(() => setNhlGames([]))
      .finally(() => setNhlLoading(false))
    const interval = setInterval(() => {
      api.get('/nhl/scoreboard').then(r => setNhlGames(r.data)).catch(() => {})
      api.get('/nhl/best-bets').then(r => setNhlBestBets(r.data)).catch(() => {})
    }, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

  // MLB handlers
  const selectGame = useCallback((game) => {
    setSelectedGame(game)
    setView('games')
    setPrediction(null)
    setPredLoading(true)

    const homeId = game.home.team_id
    const awayId = game.away.team_id
    if (!homeId || !awayId) { setPredLoading(false); return }

    const homePid = game.home_pitcher?.id ? parseInt(game.home_pitcher.id) : null
    const awayPid = game.away_pitcher?.id ? parseInt(game.away_pitcher.id) : null

    api.post('/predict', {
      home_team_id: homeId, away_team_id: awayId,
      home_pitcher_id: homePid, away_pitcher_id: awayPid,
      venue: game.venue || null,
    })
      .then(r => setPrediction(r.data))
      .catch(() => setPrediction(null))
      .finally(() => setPredLoading(false))
  }, [])

  // NHL handlers
  const selectNhlGame = useCallback((game) => {
    setNhlSelectedGame(game)
    setView('games')
    setNhlPrediction(null)
    setNhlPredLoading(true)

    // Map ESPN abbreviation to team key for prediction
    const h = game.home.abbreviation
    const a = game.away.abbreviation

    api.get(`/nhl/predict?home=${encodeURIComponent(h)}&away=${encodeURIComponent(a)}`)
      .then(r => setNhlPrediction(r.data))
      .catch(() => setNhlPrediction(null))
      .finally(() => setNhlPredLoading(false))
  }, [])

  const showStandings = useCallback(() => {
    setView('standings'); setSelectedGame(null); setNhlSelectedGame(null)
    if (league === 'MLB' && standings.length === 0) {
      api.get('/standings').then(r => setStandings(r.data)).catch(() => {})
    }
    if (league === 'NHL' && nhlStandings.length === 0) {
      setNhlStandingsLoading(true)
      api.get('/nhl/standings')
        .then(r => setNhlStandings(r.data))
        .catch(() => {})
        .finally(() => setNhlStandingsLoading(false))
    }
  }, [league, standings.length, nhlStandings.length])

  const showBacktest = useCallback(() => {
    setView('backtest'); setSelectedGame(null); setNhlSelectedGame(null)
  }, [])

  const runBacktest = useCallback((days, minEdge, season) => {
    setBtLoading(true); setBacktest(null)
    const params = new URLSearchParams()
    if (days) params.set('days', days)
    if (minEdge) params.set('min_edge', minEdge)
    if (season) params.set('season', season)
    api.get(`/backtest?${params}`)
      .then(r => setBacktest(r.data))
      .catch(() => setBacktest({ error: "Backtest failed. Try again." }))
      .finally(() => setBtLoading(false))
  }, [])

  const showBestBets = useCallback(() => {
    setView('best-bets'); setSelectedGame(null); setNhlSelectedGame(null)
    if (league === 'MLB') {
      setBbLoading(true)
      api.get('/best-bets')
        .then(r => setBestBets(r.data))
        .catch(() => setBestBets([]))
        .finally(() => setBbLoading(false))
    } else {
      setNhlBbLoading(true)
      api.get('/nhl/best-bets')
        .then(r => setNhlBestBets(r.data))
        .catch(() => setNhlBestBets([]))
        .finally(() => setNhlBbLoading(false))
    }
  }, [league])

  const showHistory = useCallback(() => {
    setView('history'); setSelectedGame(null); setNhlSelectedGame(null)
    setPhLoading(true)
    Promise.all([
      api.get('/tracker/summary'),
      api.get('/tracker/history'),
    ]).then(([s, h]) => {
      setPickSummary(s.data)
      setPickHistory(h.data)
    }).catch(() => {})
      .finally(() => setPhLoading(false))
  }, [])

  const recordPicks = useCallback(() => {
    api.post('/tracker/record').then(() => {
      api.get('/tracker/summary').then(r => setPickSummary(r.data))
      api.get('/tracker/history').then(r => setPickHistory(r.data))
    })
  }, [])

  const settlePicks = useCallback(() => {
    api.post('/tracker/settle').then(() => {
      api.get('/tracker/summary').then(r => setPickSummary(r.data))
      api.get('/tracker/history').then(r => setPickHistory(r.data))
    })
  }, [])

  const goBack = useCallback(() => {
    setSelectedGame(null); setNhlSelectedGame(null)
    setPrediction(null); setNhlPrediction(null)
    setView('games')
  }, [])

  const switchLeague = useCallback((l) => {
    setLeague(l)
    setView('games')
    setSelectedGame(null)
    setNhlSelectedGame(null)
    setPrediction(null)
    setNhlPrediction(null)
  }, [])

  const isMLB = league === 'MLB'
  const isNHL = league === 'NHL'

  return (
    <div className="app">
      <div className="header">
        <h1>{league} Prediction Engine</h1>
        <p className="subtitle">Data-driven {league} game predictions</p>
      </div>

      {/* League switcher */}
      <div className="league-switcher">
        <button
          className={`league-btn ${isMLB ? 'active' : ''}`}
          onClick={() => switchLeague('MLB')}
        >
          MLB
        </button>
        <button
          className={`league-btn ${isNHL ? 'active' : ''}`}
          onClick={() => switchLeague('NHL')}
        >
          NHL
        </button>
      </div>

      <nav className="nav-tabs">
        <button className={`nav-tab ${view === 'games' && !selectedGame && !nhlSelectedGame ? 'active' : ''}`} onClick={goBack}>
          Games
        </button>
        <button className={`nav-tab ${view === 'best-bets' ? 'active' : ''}`} onClick={showBestBets}>
          Best Bets
        </button>
        <button className={`nav-tab ${view === 'standings' ? 'active' : ''}`} onClick={showStandings}>
          Standings
        </button>
        {isMLB && (
          <>
            <button className={`nav-tab ${view === 'history' ? 'active' : ''}`} onClick={showHistory}>
              Pick Tracker
            </button>
            <button className={`nav-tab ${view === 'backtest' ? 'active' : ''}`} onClick={showBacktest}>
              Backtest
            </button>
          </>
        )}
      </nav>

      {/* ── MLB Views ── */}
      {isMLB && view === 'games' && !selectedGame && (
        <Scoreboard games={games} loading={gamesLoading} onSelectGame={selectGame} bestBets={bestBets} />
      )}

      {isMLB && selectedGame && (
        <GameDetail game={selectedGame} prediction={prediction} loading={predLoading} onBack={goBack} />
      )}

      {isMLB && view === 'best-bets' && (
        <BestBets bets={bestBets} loading={bbLoading} />
      )}

      {isMLB && view === 'standings' && (
        <Standings divisions={standings} />
      )}

      {isMLB && view === 'history' && (
        <PickHistory
          summary={pickSummary}
          history={pickHistory}
          loading={phLoading}
          onRecord={recordPicks}
          onSettle={settlePicks}
        />
      )}

      {isMLB && view === 'backtest' && (
        <Backtest data={backtest} loading={btLoading} onRun={runBacktest} />
      )}

      {/* ── NHL Views ── */}
      {isNHL && view === 'games' && !nhlSelectedGame && (
        <NHLScoreboard games={nhlGames} loading={nhlLoading} onSelectGame={selectNhlGame} bestBets={nhlBestBets} />
      )}

      {isNHL && nhlSelectedGame && (
        <NHLGameDetail game={nhlSelectedGame} prediction={nhlPrediction} loading={nhlPredLoading} onBack={goBack} />
      )}

      {isNHL && view === 'best-bets' && (
        <BestBets bets={nhlBestBets} loading={nhlBbLoading} />
      )}

      {isNHL && view === 'standings' && (
        <NHLStandings divisions={nhlStandings} loading={nhlStandingsLoading} />
      )}
    </div>
  )
}


// ── NHL Game Detail (inline) ──
function NHLGameDetail({ game, prediction, loading, onBack }) {
  const { home, away, status } = game
  const pred = prediction

  return (
    <div className="game-detail">
      <button className="back-btn" onClick={onBack}>Back to Games</button>

      <div className="detail-header">
        <div className="detail-team">
          {away.logo && <img src={away.logo} alt="" className="detail-logo" />}
          <div>
            <h2>{away.name}</h2>
            <span className="detail-record">{away.record}</span>
          </div>
        </div>
        <div className="detail-vs">
          {status.state === 'pre' ? 'vs' : `${away.score} - ${home.score}`}
        </div>
        <div className="detail-team">
          {home.logo && <img src={home.logo} alt="" className="detail-logo" />}
          <div>
            <h2>{home.name}</h2>
            <span className="detail-record">{home.record}</span>
          </div>
        </div>
      </div>

      <div className="detail-meta">
        <span>{new Date(game.date).toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })}</span>
        {game.venue && <span>{game.venue}</span>}
        {game.broadcast && <span>{game.broadcast}</span>}
      </div>

      {loading && (
        <div className="loading">
          <div className="spinner" />
          <p>Running prediction model...</p>
        </div>
      )}

      {pred && (
        <div className="prediction-results">
          {/* Win probabilities */}
          <div className="result-card">
            <h2>Win Probability</h2>
            <div className="wp-bar">
              <div className="wp-fill" style={{width: `${(pred.win_prob.away * 100).toFixed(1)}%`}}>
                {away.abbreviation || pred.away?.abbreviation} {(pred.win_prob.away * 100).toFixed(1)}%
              </div>
              <div className="wp-fill home" style={{width: `${(pred.win_prob.home * 100).toFixed(1)}%`}}>
                {home.abbreviation || pred.home?.abbreviation} {(pred.win_prob.home * 100).toFixed(1)}%
              </div>
            </div>
            {pred.regulation_draw_prob > 0 && (
              <p style={{color: '#94a3b8', fontSize: '0.8rem', marginTop: 4}}>
                Regulation draw: {(pred.regulation_draw_prob * 100).toFixed(1)}% (goes to OT)
              </p>
            )}
          </div>

          {/* Expected Score */}
          <div className="result-card">
            <h2>Expected Score</h2>
            <div className="bt-summary">
              <div className="bt-summary-stat">
                <span className="bt-big">{pred.expected_score.away}</span>
                <span className="bt-label">{away.abbreviation || pred.away?.abbreviation}</span>
              </div>
              <div className="bt-summary-stat">
                <span className="bt-big">{pred.expected_score.home}</span>
                <span className="bt-label">{home.abbreviation || pred.home?.abbreviation}</span>
              </div>
              <div className="bt-summary-stat">
                <span className="bt-big">{pred.total}</span>
                <span className="bt-label">Total</span>
              </div>
              <div className="bt-summary-stat">
                <span className="bt-big">{pred.spread > 0 ? '+' : ''}{pred.spread}</span>
                <span className="bt-label">Spread</span>
              </div>
            </div>
          </div>

          {/* Puck Line */}
          {pred.puck_line && (
            <div className="result-card">
              <h2>Puck Line</h2>
              <div className="bt-summary">
                <div className="bt-summary-stat">
                  <span className="bt-big">{(pred.puck_line.home_minus_1_5 * 100).toFixed(1)}%</span>
                  <span className="bt-label">{home.abbreviation} -1.5</span>
                </div>
                <div className="bt-summary-stat">
                  <span className="bt-big">{(pred.puck_line.away_plus_1_5 * 100).toFixed(1)}%</span>
                  <span className="bt-label">{away.abbreviation} +1.5</span>
                </div>
              </div>
            </div>
          )}

          {/* Periods */}
          {pred.periods && pred.periods.length > 0 && (
            <div className="result-card">
              <h2>Period Breakdown</h2>
              <table className="standings-table">
                <thead>
                  <tr>
                    <th>Period</th>
                    <th>{away.abbreviation}</th>
                    <th>{home.abbreviation}</th>
                    <th>Total</th>
                  </tr>
                </thead>
                <tbody>
                  {pred.periods.map(p => (
                    <tr key={p.period}>
                      <td style={{fontWeight:600}}>{p.period}</td>
                      <td>{p.away}</td>
                      <td>{p.home}</td>
                      <td>{p.total}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Key Factors */}
          {pred.factors && (
            <div className="result-card">
              <h2>Key Factors</h2>
              <div className="bt-grid" style={{gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))'}}>
                {pred.factors.home_pp != null && (
                  <div className="bt-row">
                    <span className="stat-label">Power Play</span>
                    <span className="stat-value">{away.abbreviation} {(pred.factors.away_pp * 100).toFixed(1)}% / {home.abbreviation} {(pred.factors.home_pp * 100).toFixed(1)}%</span>
                  </div>
                )}
                {pred.factors.home_pk != null && (
                  <div className="bt-row">
                    <span className="stat-label">Penalty Kill</span>
                    <span className="stat-value">{away.abbreviation} {(pred.factors.away_pk * 100).toFixed(1)}% / {home.abbreviation} {(pred.factors.home_pk * 100).toFixed(1)}%</span>
                  </div>
                )}
                {pred.factors.home_sv != null && (
                  <div className="bt-row">
                    <span className="stat-label">Save %</span>
                    <span className="stat-value">{away.abbreviation} {pred.factors.away_sv?.toFixed(3)} / {home.abbreviation} {pred.factors.home_sv?.toFixed(3)}</span>
                  </div>
                )}
                <div className="bt-row">
                  <span className="stat-label">Shots/Game</span>
                  <span className="stat-value">{away.abbreviation} {pred.factors.away_shots} / {home.abbreviation} {pred.factors.home_shots}</span>
                </div>
                <div className="bt-row">
                  <span className="stat-label">Faceoff %</span>
                  <span className="stat-value">{away.abbreviation} {(pred.factors.away_fo * 100).toFixed(1)}% / {home.abbreviation} {(pred.factors.home_fo * 100).toFixed(1)}%</span>
                </div>
              </div>
            </div>
          )}

          {/* Odds card */}
          {game.odds && (
            <div className="result-card">
              <h2>DraftKings Odds</h2>
              <div className="game-odds-grid" style={{fontSize: '0.95rem'}}>
                {game.odds.home_ml && (
                  <div className="odds-line">
                    <span className="odds-label">ML</span>
                    <span className="odds-val">{away.abbreviation} {game.odds.away_ml > 0 ? '+' : ''}{game.odds.away_ml}</span>
                    <span className="odds-val">{home.abbreviation} {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml}</span>
                  </div>
                )}
                {game.odds.over_under && (
                  <div className="odds-line">
                    <span className="odds-label">O/U</span>
                    <span className="odds-val">o{game.odds.over_under} ({game.odds.over_odds > 0 ? '+' : ''}{game.odds.over_odds})</span>
                    <span className="odds-val">u{game.odds.over_under} ({game.odds.under_odds > 0 ? '+' : ''}{game.odds.under_odds})</span>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Top correct scores */}
          {pred.correct_scores && (
            <div className="result-card">
              <h2>Most Likely Scores</h2>
              <div className="bt-summary">
                {pred.correct_scores.map((s, i) => (
                  <div key={i} className="bt-summary-stat">
                    <span className="bt-big">{s.score}</span>
                    <span className="bt-label">{(s.prob * 100).toFixed(1)}%</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
