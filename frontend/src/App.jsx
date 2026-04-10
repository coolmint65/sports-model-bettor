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
import NHLGameDetail from './components/NHLGameDetail'

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
  const [nhlPickSummary, setNhlPickSummary] = useState(null)
  const [nhlPickHistory, setNhlPickHistory] = useState(null)
  const [nhlPhLoading, setNhlPhLoading] = useState(false)

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
    const endpoint = league === 'NHL' ? '/nhl/backtest' : '/backtest'
    api.get(`${endpoint}?${params}`)
      .then(r => setBacktest(r.data))
      .catch(() => setBacktest({ error: "Backtest failed. Try again." }))
      .finally(() => setBtLoading(false))
  }, [league])

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
    if (league === 'MLB') {
      setPhLoading(true)
      Promise.all([
        api.get('/tracker/summary'),
        api.get('/tracker/history'),
      ]).then(([s, h]) => {
        setPickSummary(s.data)
        setPickHistory(h.data)
      }).catch(() => {})
        .finally(() => setPhLoading(false))
    } else {
      setNhlPhLoading(true)
      Promise.all([
        api.get('/nhl/tracker/summary'),
        api.get('/nhl/tracker/history'),
      ]).then(([s, h]) => {
        setNhlPickSummary(s.data)
        setNhlPickHistory(h.data)
      }).catch(() => {})
        .finally(() => setNhlPhLoading(false))
    }
  }, [league])

  const recordPicks = useCallback(() => {
    if (league === 'MLB') {
      api.post('/tracker/record').then(() => {
        api.get('/tracker/summary').then(r => setPickSummary(r.data))
        api.get('/tracker/history').then(r => setPickHistory(r.data))
      })
    } else {
      api.post('/nhl/tracker/record').then(() => {
        api.get('/nhl/tracker/summary').then(r => setNhlPickSummary(r.data))
        api.get('/nhl/tracker/history').then(r => setNhlPickHistory(r.data))
      })
    }
  }, [league])

  const settlePicks = useCallback(() => {
    if (league === 'MLB') {
      api.post('/tracker/settle').then(() => {
        api.get('/tracker/summary').then(r => setPickSummary(r.data))
        api.get('/tracker/history').then(r => setPickHistory(r.data))
      })
    } else {
      api.post('/nhl/tracker/settle').then(() => {
        api.get('/nhl/tracker/summary').then(r => setNhlPickSummary(r.data))
        api.get('/nhl/tracker/history').then(r => setNhlPickHistory(r.data))
      })
    }
  }, [league])

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
        <button className={`nav-tab ${view === 'history' ? 'active' : ''}`} onClick={showHistory}>
          Pick Tracker
        </button>
        <button className={`nav-tab ${view === 'backtest' ? 'active' : ''}`} onClick={showBacktest}>
          Backtest
        </button>
      </nav>

      {/* ── MLB Views ── */}
      {isMLB && view === 'games' && !selectedGame && (
        <>
          <PickOfDayCard sport="mlb" />
          <Scoreboard games={games} loading={gamesLoading} onSelectGame={selectGame} bestBets={bestBets} />
        </>
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
        <>
          <PickOfDayCard sport="nhl" />
          <NHLScoreboard games={nhlGames} loading={nhlLoading} onSelectGame={selectNhlGame} bestBets={nhlBestBets} />
        </>
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

      {isNHL && view === 'history' && (
        <PickHistory
          summary={nhlPickSummary}
          history={nhlPickHistory}
          loading={nhlPhLoading}
          onRecord={recordPicks}
          onSettle={settlePicks}
        />
      )}

      {isNHL && view === 'backtest' && (
        <Backtest data={backtest} loading={btLoading} onRun={runBacktest} />
      )}
    </div>
  )
}


// ── Pick of the Day card ──
function PickOfDayCard({ sport }) {
  const [potd, setPotd] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    if (loaded) return
    setLoaded(true)
    const a = axios.create({ baseURL: '/api' })
    Promise.all([
      a.get(`/pick-of-day/${sport}`),
      a.get(`/pick-of-day/${sport}/summary`),
    ]).then(([p, s]) => {
      setPotd(p.data)
      setSummary(s.data)
    }).catch(() => {})
  }, [sport, loaded])

  if (!potd || potd.error || potd.message) return null

  const s = summary || {}
  const odds = potd.odds
  const oddsStr = odds ? `${odds > 0 ? '+' : ''}${odds}` : ''

  return (
    <div style={{
      background: 'linear-gradient(135deg, rgba(52,211,153,0.06) 0%, #111827 60%)',
      border: '1px solid rgba(52,211,153,0.25)',
      borderRadius: 12,
      padding: '20px 24px',
      marginBottom: 16,
      position: 'relative',
      overflow: 'hidden',
    }}>
      {/* Accent bar */}
      <div style={{
        position: 'absolute', top: 0, left: 0, right: 0, height: 3,
        background: 'linear-gradient(90deg, #34d399, #60a5fa)',
      }} />

      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 20, flexWrap: 'wrap' }}>
        <div>
          <div style={{ fontSize: '0.68rem', fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.1em', color: '#34d399', marginBottom: 6 }}>
            {sport.toUpperCase()} PICK OF THE DAY
          </div>
          <div style={{ fontSize: '1.4rem', fontWeight: 800, color: '#f8fafc', lineHeight: 1.2 }}>
            {potd.pick} <span style={{ color: '#94a3b8', fontWeight: 500, fontSize: '1rem' }}>({oddsStr})</span>
          </div>
          <div style={{ fontSize: '0.85rem', color: '#94a3b8', marginTop: 4 }}>
            {potd.matchup} — <span style={{ color: '#60a5fa' }}>{potd.bet_type}</span>
          </div>
          <div style={{ fontSize: '0.78rem', color: '#64748b', marginTop: 6 }}>
            Model: {(potd.model_prob * 100).toFixed(1)}% | Edge: +{potd.edge?.toFixed(1)}% | Kelly: {potd.kelly_pct}%
          </div>
          {potd.reasoning && (
            <div style={{ fontSize: '0.75rem', color: '#94a3b8', marginTop: 8, fontStyle: 'italic', maxWidth: 500 }}>
              {potd.reasoning}
            </div>
          )}
        </div>

        {/* POTD running record */}
        {s.total > 0 && (
          <div style={{ textAlign: 'right', minWidth: 120 }}>
            <div style={{ fontSize: '0.62rem', fontWeight: 600, textTransform: 'uppercase', letterSpacing: '0.08em', color: '#64748b', marginBottom: 4 }}>
              POTD Record
            </div>
            <div style={{ fontSize: '1.5rem', fontWeight: 800, color: '#e2e8f0' }}>
              {s.wins}-{s.losses}
            </div>
            <div style={{
              fontSize: '1rem', fontWeight: 700, marginTop: 2,
              color: s.profit > 0 ? '#34d399' : s.profit < 0 ? '#ef4444' : '#94a3b8',
            }}>
              {s.profit > 0 ? '+' : ''}${s.profit}
            </div>
            <div style={{ fontSize: '0.7rem', color: '#64748b', marginTop: 2 }}>
              {s.win_pct}% WR | {s.total} picks
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
