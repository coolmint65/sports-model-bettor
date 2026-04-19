import { useState, useEffect, useCallback } from 'react'
import axios from 'axios'
import Scoreboard from './components/Scoreboard'
import GameDetail from './components/GameDetail'
import Standings from './components/Standings'
import Backtest from './components/Backtest'
import PickHistory from './components/PickHistory'
import NHLScoreboard from './components/NHLScoreboard'
import NHLStandings from './components/NHLStandings'
import NHLGameDetail from './components/NHLGameDetail'
import NBAScoreboard from './components/NBAScoreboard'
import NBAStandings from './components/NBAStandings'
import NBAGameDetail from './components/NBAGameDetail'
import PickOfDayHero from './components/PickOfDayHero'

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

  // NBA state
  const [nbaGames, setNbaGames] = useState([])
  const [nbaLoading, setNbaLoading] = useState(true)
  const [nbaBestBets, setNbaBestBets] = useState(null)
  const [nbaBbLoading, setNbaBbLoading] = useState(false)
  const [nbaStandings, setNbaStandings] = useState([])
  const [nbaStandingsLoading, setNbaStandingsLoading] = useState(false)
  const [nbaPrediction, setNbaPrediction] = useState(null)
  const [nbaPredLoading, setNbaPredLoading] = useState(false)
  const [nbaSelectedGame, setNbaSelectedGame] = useState(null)
  const [nbaPickSummary, setNbaPickSummary] = useState(null)
  const [nbaPickHistory, setNbaPickHistory] = useState(null)
  const [nbaPhLoading, setNbaPhLoading] = useState(false)
  // Live progress of in-flight /best-bets calls so the loading spinner
  // can render "Computing 8/15 games (53%)" instead of an indeterminate
  // spinner during the multi-minute cold load. One slot per sport so
  // the three parallel mount promises don't stomp on each other.
  const [bbProgress, setBbProgress] = useState(null)
  const [nhlBbProgress, setNhlBbProgress] = useState(null)
  const [nbaBbProgress, setNbaBbProgress] = useState(null)

  // Load MLB games on mount + auto-refresh every 5 min. Both the mount
  // load and each refresh pipe through /best-bets which can take minutes
  // cold, so we poll /best-bets/progress on both paths to keep the
  // spinner label live.
  useEffect(() => {
    const fetchGames = () => {
      api.get('/scoreboard').then(r => setGames(r.data)).catch(() => {})
    }

    const runBestBets = (onStart, onFinally) => {
      if (onStart) onStart()
      const pollHandle = setInterval(() => {
        api.get('/best-bets/progress')
          .then(r => setBbProgress(r.data))
          .catch(() => {})
      }, 750)
      return api.get('/best-bets')
        .then(r => setBestBets(r.data))
        .catch(() => {})
        .finally(() => {
          clearInterval(pollHandle)
          setBbProgress(null)
          if (onFinally) onFinally()
        })
    }

    setGamesLoading(true)
    Promise.all([
      api.get('/scoreboard').then(r => setGames(r.data)).catch(() => setGames([])),
      runBestBets(),
    ]).finally(() => setGamesLoading(false))

    const interval = setInterval(() => {
      fetchGames()
      runBestBets()
    }, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

  // Load NHL games on mount + auto-refresh
  useEffect(() => {
    const runBestBets = () => {
      const pollHandle = setInterval(() => {
        api.get('/nhl/best-bets/progress')
          .then(r => setNhlBbProgress(r.data))
          .catch(() => {})
      }, 750)
      return api.get('/nhl/best-bets')
        .then(r => setNhlBestBets(r.data))
        .catch(() => {})
        .finally(() => {
          clearInterval(pollHandle)
          setNhlBbProgress(null)
        })
    }

    setNhlLoading(true)
    Promise.all([
      api.get('/nhl/scoreboard').then(r => setNhlGames(r.data)).catch(() => setNhlGames([])),
      runBestBets(),
    ]).finally(() => setNhlLoading(false))

    const interval = setInterval(() => {
      api.get('/nhl/scoreboard').then(r => setNhlGames(r.data)).catch(() => {})
      runBestBets()
    }, 5 * 60 * 1000)
    return () => clearInterval(interval)
  }, [])

  // Load NBA games on mount + auto-refresh
  useEffect(() => {
    const runBestBets = () => {
      const pollHandle = setInterval(() => {
        api.get('/nba/best-bets/progress')
          .then(r => setNbaBbProgress(r.data))
          .catch(() => {})
      }, 750)
      return api.get('/nba/best-bets')
        .then(r => setNbaBestBets(Array.isArray(r.data) ? r.data : null))
        .catch(() => {})
        .finally(() => {
          clearInterval(pollHandle)
          setNbaBbProgress(null)
        })
    }

    setNbaLoading(true)
    Promise.all([
      api.get('/nba/scoreboard').then(r => setNbaGames(Array.isArray(r.data) ? r.data : [])).catch(() => setNbaGames([])),
      runBestBets(),
    ]).finally(() => setNbaLoading(false))

    const interval = setInterval(() => {
      api.get('/nba/scoreboard').then(r => setNbaGames(Array.isArray(r.data) ? r.data : [])).catch(() => {})
      runBestBets()
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

  // NBA handlers
  const selectNbaGame = useCallback((game) => {
    setNbaSelectedGame(game)
    setView('games')
    setNbaPrediction(null)
    setNbaPredLoading(true)

    const h = game.home.abbreviation
    const a = game.away.abbreviation

    api.get(`/nba/predict?home=${encodeURIComponent(h)}&away=${encodeURIComponent(a)}`)
      .then(r => setNbaPrediction(r.data))
      .catch(() => setNbaPrediction(null))
      .finally(() => setNbaPredLoading(false))
  }, [])

  const showStandings = useCallback(() => {
    setView('standings'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
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
    if (league === 'NBA' && nbaStandings.length === 0) {
      setNbaStandingsLoading(true)
      api.get('/nba/standings')
        .then(r => setNbaStandings(Array.isArray(r.data) ? r.data : []))
        .catch(() => {})
        .finally(() => setNbaStandingsLoading(false))
    }
  }, [league, standings.length, nhlStandings.length, nbaStandings.length])

  const showBacktest = useCallback(() => {
    setView('backtest'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
  }, [])

  const runBacktest = useCallback((days, minEdge, season) => {
    setBtLoading(true); setBacktest(null)
    const params = new URLSearchParams()
    if (days) params.set('days', days)
    if (minEdge) params.set('min_edge', minEdge)
    if (season) params.set('season', season)
    const endpoint = league === 'NHL' ? '/nhl/backtest' : league === 'NBA' ? '/nba/backtest' : '/backtest'
    api.get(`${endpoint}?${params}`)
      .then(r => setBacktest(r.data))
      .catch(() => setBacktest({ error: "Backtest failed. Try again." }))
      .finally(() => setBtLoading(false))
  }, [league])


  const showHistory = useCallback(() => {
    setView('history'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
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
    } else if (league === 'NHL') {
      setNhlPhLoading(true)
      Promise.all([
        api.get('/nhl/tracker/summary'),
        api.get('/nhl/tracker/history'),
      ]).then(([s, h]) => {
        setNhlPickSummary(s.data)
        setNhlPickHistory(h.data)
      }).catch(() => {})
        .finally(() => setNhlPhLoading(false))
    } else if (league === 'NBA') {
      setNbaPhLoading(true)
      Promise.all([
        api.get('/nba/tracker/summary'),
        api.get('/nba/tracker/history'),
      ]).then(([s, h]) => {
        setNbaPickSummary(s.data)
        setNbaPickHistory(Array.isArray(h.data) ? h.data : [])
      }).catch(() => {})
        .finally(() => setNbaPhLoading(false))
    }
  }, [league])

  const recordPicks = useCallback(() => {
    if (league === 'MLB') {
      api.post('/tracker/record').then(() => {
        api.get('/tracker/summary').then(r => setPickSummary(r.data))
        api.get('/tracker/history').then(r => setPickHistory(r.data))
      })
    } else if (league === 'NHL') {
      api.post('/nhl/tracker/record').then(() => {
        api.get('/nhl/tracker/summary').then(r => setNhlPickSummary(r.data))
        api.get('/nhl/tracker/history').then(r => setNhlPickHistory(r.data))
      })
    } else if (league === 'NBA') {
      api.post('/nba/tracker/record').then(() => {
        api.get('/nba/tracker/summary').then(r => setNbaPickSummary(r.data))
        api.get('/nba/tracker/history').then(r => setNbaPickHistory(r.data))
      })
    }
  }, [league])

  const settlePicks = useCallback(() => {
    if (league === 'MLB') {
      api.post('/tracker/settle').then(() => {
        api.get('/tracker/summary').then(r => setPickSummary(r.data))
        api.get('/tracker/history').then(r => setPickHistory(r.data))
      })
    } else if (league === 'NHL') {
      api.post('/nhl/tracker/settle').then(() => {
        api.get('/nhl/tracker/summary').then(r => setNhlPickSummary(r.data))
        api.get('/nhl/tracker/history').then(r => setNhlPickHistory(r.data))
      })
    } else if (league === 'NBA') {
      api.post('/nba/tracker/settle').then(() => {
        api.get('/nba/tracker/summary').then(r => setNbaPickSummary(r.data))
        api.get('/nba/tracker/history').then(r => setNbaPickHistory(r.data))
      })
    }
  }, [league])

  const goBack = useCallback(() => {
    setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)
    setPrediction(null); setNhlPrediction(null); setNbaPrediction(null)
    setView('games')
  }, [])

  const switchLeague = useCallback((l) => {
    setLeague(l)
    setView('games')
    setSelectedGame(null)
    setNhlSelectedGame(null)
    setNbaSelectedGame(null)
    setPrediction(null)
    setNhlPrediction(null)
    setNbaPrediction(null)
  }, [])

  const isMLB = league === 'MLB'
  const isNHL = league === 'NHL'
  const isNBA = league === 'NBA'

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
        <button
          className={`league-btn ${isNBA ? 'active' : ''}`}
          onClick={() => switchLeague('NBA')}
        >
          NBA
        </button>
      </div>

      <nav className="nav-tabs">
        <button className={`nav-tab ${view === 'games' && !selectedGame && !nhlSelectedGame && !nbaSelectedGame ? 'active' : ''}`} onClick={goBack}>
          Games
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
          <PickOfDayHero sport="mlb" />
          <Scoreboard games={games} loading={gamesLoading} progress={bbProgress} onSelectGame={selectGame} bestBets={bestBets} />
        </>
      )}

      {isMLB && selectedGame && (
        <GameDetail game={selectedGame} prediction={prediction} loading={predLoading} onBack={goBack} />
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
          <PickOfDayHero sport="nhl" />
          <NHLScoreboard games={nhlGames} loading={nhlLoading} progress={nhlBbProgress} onSelectGame={selectNhlGame} bestBets={nhlBestBets} />
        </>
      )}

      {isNHL && nhlSelectedGame && (
        <NHLGameDetail game={nhlSelectedGame} prediction={nhlPrediction} loading={nhlPredLoading} onBack={goBack} />
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

      {/* ── NBA Views ── */}
      {isNBA && view === 'games' && !nbaSelectedGame && (
        <>
          <PickOfDayHero sport="nba" />
          <NBAScoreboard games={nbaGames} loading={nbaLoading} progress={nbaBbProgress} onSelectGame={selectNbaGame} bestBets={nbaBestBets} />
        </>
      )}

      {isNBA && nbaSelectedGame && (
        <NBAGameDetail game={nbaSelectedGame} prediction={nbaPrediction} loading={nbaPredLoading} onBack={goBack} />
      )}

      {isNBA && view === 'standings' && (
        <NBAStandings divisions={nbaStandings} loading={nbaStandingsLoading} />
      )}

      {isNBA && view === 'history' && (
        <PickHistory
          summary={nbaPickSummary}
          history={nbaPickHistory}
          loading={nbaPhLoading}
          onRecord={recordPicks}
          onSettle={settlePicks}
        />
      )}

      {isNBA && view === 'backtest' && (
        <Backtest data={backtest} loading={btLoading} onRun={runBacktest} />
      )}
    </div>
  )
}
