import GameCard from './primitives/GameCard'

export default function NBAScoreboard({ games, loading, progress, onSelectGame, bestBets }) {
  if (loading) {
    const total = progress?.total || 0
    const rawDone = progress?.done || 0
    const done = total > 0 ? Math.min(rawDone, total) : rawDone
    const pct = total ? Math.min(100, Math.round((done / total) * 100)) : null
    const phase = progress?.phase
    let label = 'Loading NBA games...'
    if (phase === 'predicting' && total > 0) {
      label = `Computing predictions: ${done}/${total} games (${pct}%)`
    } else if (phase === 'building') {
      label = 'Assembling picks...'
    }
    return (
      <div className="loading">
        <div className="spinner" />
        <p>{label}</p>
      </div>
    )
  }

  if (!games || games.length === 0) {
    return (
      <div className="no-games">
        <p>No NBA games scheduled today.</p>
        <p className="sub">Check back for the next slate.</p>
      </div>
    )
  }

  const betMap = {}
  if (bestBets) {
    for (const b of bestBets) {
      betMap[b.game_id] = b
    }
  }

  // Split games into active (pregame/live) and finals
  const activeGames = []
  const finalGames = []
  for (const g of games) {
    if (g.status?.state === 'post' || g.status?.completed) {
      finalGames.push(g)
    } else {
      activeGames.push(g)
    }
  }

  // Sort active games by edge (highest first)
  activeGames.sort((a, b) => {
    const aEdge = betMap[a.id]?.best_pick?.edge || -99
    const bEdge = betMap[b.id]?.best_pick?.edge || -99
    return bEdge - aEdge
  })

  const edgeCount = activeGames.filter(g =>
    betMap[g.id]?.confidence === 'strong' || betMap[g.id]?.confidence === 'moderate'
  ).length

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        NBA Games ({games.length})
        {edgeCount > 0 && <span className="edge-count">{edgeCount} Q1 plays with edge</span>}
      </h2>

      {activeGames.length > 0 && (
        <>
          {finalGames.length > 0 && (
            <div className="games-section-header">
              {activeGames.some(g => g.status?.state === 'in') ? 'Live & Upcoming' : 'Upcoming'} ({activeGames.length})
            </div>
          )}
          <div className="games-feature-grid">
            {activeGames.map(game => (
              <NBAGameCard
                key={game.id}
                game={game}
                bet={betMap[game.id]}
                onClick={() => onSelectGame && onSelectGame(game)}
              />
            ))}
          </div>
        </>
      )}

      {finalGames.length > 0 && (
        <>
          <div className="games-section-header">Final ({finalGames.length})</div>
          <div className="games-finals-grid">
            {finalGames.map(game => (
              <NBAFinalRow
                key={game.id}
                game={game}
                bet={betMap[game.id]}
                onClick={() => onSelectGame && onSelectGame(game)}
              />
            ))}
          </div>
        </>
      )}
    </div>
  )
}


function NBAFinalRow({ game, bet, onClick }) {
  const { home, away } = game
  const hs = parseInt(home.score) || 0
  const as = parseInt(away.score) || 0
  const homeWon = hs > as
  const q1 = game.q1 || {}

  return (
    <div className="game-final-row" onClick={onClick}>
      <span className="final-label">FINAL</span>
      <div className="final-teams">
        <div className="final-team">
          {away.logo && <img src={away.logo} alt="" />}
          <span className={`final-abbr ${!homeWon ? 'winner' : ''}`}>{away.abbreviation}</span>
        </div>
        <span className={`final-score ${!homeWon ? 'winner' : ''}`}>{as}</span>
        <span className="final-dash">-</span>
        <span className={`final-score ${homeWon ? 'winner' : ''}`}>{hs}</span>
        <div className="final-team">
          {home.logo && <img src={home.logo} alt="" />}
          <span className={`final-abbr ${homeWon ? 'winner' : ''}`}>{home.abbreviation}</span>
        </div>
      </div>
      {/* Q1 score in final row */}
      {q1.home != null && q1.away != null && (
        <div className="q1-final-line">
          Q1: {away.abbreviation} {q1.away} - {home.abbreviation} {q1.home}
        </div>
      )}
    </div>
  )
}


function NBAGameCard({ game, bet, onClick }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const q1 = game.q1 || {}

  const liveExtras = (
    <>
      {(isLive || isFinal) && q1.home != null && q1.away != null && (
        <div className="q1-score-display">
          <span className="q1-label">Q1</span>
          <span className="q1-score-away">{away.abbreviation} {q1.away}</span>
          <span className="q1-separator">-</span>
          <span className="q1-score-home">{home.abbreviation} {q1.home}</span>
        </div>
      )}
      {isLive && (
        <div className="live-quarter-indicator">{status.detail}</div>
      )}
    </>
  )

  const odds = game.odds ? <NBAOddsGrid odds={game.odds} home={home} away={away} /> : null

  return (
    <GameCard
      game={game}
      bet={bet}
      onClick={onClick}
      pickAccent="q1"
      restTiredLabel="B2B"
      liveExtras={liveExtras}
      odds={odds}
    />
  )
}

function NBAOddsGrid({ odds, home, away }) {
  return (
    <div className="game-odds-grid">
      {(odds.home_spread_point != null || odds.away_spread_point != null) && (
        <div className="odds-line">
          <span className="odds-label">SPR</span>
          <span className="odds-val">
            {away.abbreviation} {odds.away_spread_point > 0 ? '+' : ''}{odds.away_spread_point || '-'}
            {odds.away_spread_odds ? ` (${odds.away_spread_odds > 0 ? '+' : ''}${Math.round(odds.away_spread_odds)})` : ''}
          </span>
          <span className="odds-val">
            {home.abbreviation} {odds.home_spread_point > 0 ? '+' : ''}{odds.home_spread_point || '-'}
            {odds.home_spread_odds ? ` (${odds.home_spread_odds > 0 ? '+' : ''}${Math.round(odds.home_spread_odds)})` : ''}
          </span>
        </div>
      )}
      {odds.over_under && (
        <div className="odds-line">
          <span className="odds-label">O/U</span>
          <span className="odds-val">o{odds.over_under} {odds.over_odds ? `(${Math.round(odds.over_odds) > 0 ? '+' : ''}${Math.round(odds.over_odds)})` : ''}</span>
          <span className="odds-val">u{odds.over_under} {odds.under_odds ? `(${Math.round(odds.under_odds) > 0 ? '+' : ''}${Math.round(odds.under_odds)})` : ''}</span>
        </div>
      )}
      {(odds.home_ml || odds.away_ml) && (
        <div className="odds-line">
          <span className="odds-label">ML</span>
          <span className="odds-val">{away.abbreviation} {odds.away_ml > 0 ? '+' : ''}{odds.away_ml || '-'}</span>
          <span className="odds-val">{home.abbreviation} {odds.home_ml > 0 ? '+' : ''}{odds.home_ml || '-'}</span>
        </div>
      )}
    </div>
  )
}


