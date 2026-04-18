import GameCard from './primitives/GameCard'

export default function Scoreboard({ games, loading, progress, onSelectGame, bestBets }) {
  if (loading) {
    // Progress is the live snapshot from /api/best-bets/progress -- shows
    // X / N games and a percent so the cold-load wait isn't a black box.
    const total = progress?.total || 0
    const rawDone = progress?.done || 0
    // Backend single-flight should keep done <= total now, but defense
    // in depth: clamp the display in case two predates collide.
    const done = total > 0 ? Math.min(rawDone, total) : rawDone
    const pct = total ? Math.min(100, Math.round((done / total) * 100)) : null
    const phase = progress?.phase
    let label = "Loading today's slate..."
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

  if (games.length === 0) {
    return (
      <div className="no-games">
        <p>No games scheduled today.</p>
        <p className="sub">Check back tomorrow for the next slate.</p>
      </div>
    )
  }

  // Build lookup: game id -> best bet data
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

  // Sort active by edge
  activeGames.sort((a, b) => {
    const aEdge = betMap[a.id]?.best_pick?.edge || -99
    const bEdge = betMap[b.id]?.best_pick?.edge || -99
    return bEdge - aEdge
  })

  const edgeCount = activeGames.filter(g => betMap[g.id]?.confidence === 'strong' || betMap[g.id]?.confidence === 'moderate').length

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        Today's Games ({games.length})
        {edgeCount > 0 && <span className="edge-count">{edgeCount} plays with edge</span>}
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
              <MLBGameCard
                key={game.id}
                game={game}
                bet={betMap[game.id]}
                onClick={() => onSelectGame(game)}
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
              <MLBFinalRow
                key={game.id}
                game={game}
                onClick={() => onSelectGame(game)}
              />
            ))}
          </div>
        </>
      )}
    </div>
  )
}


function MLBFinalRow({ game, onClick }) {
  const { home, away } = game
  const hs = parseInt(home.score) || 0
  const as = parseInt(away.score) || 0
  const homeWon = hs > as
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
    </div>
  )
}

function MLBGameCard({ game, bet, onClick }) {
  const { home, away } = game

  const starters = (game.home_pitcher || game.away_pitcher) ? (
    <div className="game-pitchers">
      <span className="pitcher">{game.away_pitcher?.name || 'TBD'}</span>
      <span className="vs">vs</span>
      <span className="pitcher">{game.home_pitcher?.name || 'TBD'}</span>
    </div>
  ) : null

  const odds = game.odds ? <MLBOddsGrid odds={game.odds} home={home} away={away} /> : null

  const insight = <MLBCardInsight bet={bet} home={home} away={away} game={game} />

  return (
    <GameCard
      game={game}
      bet={bet}
      onClick={onClick}
      insight={insight}
      starters={starters}
      odds={odds}
    />
  )
}

function MLBOddsGrid({ odds, home, away }) {
  // MLB runline fallback: when sportsbook hasn't posted a real RL we
  // synthesize ±1.5 off the ML side so the user still sees a spread line.
  const hasReal = odds.away_spread_point != null || odds.home_spread_point != null
  const homeFav = odds.home_ml && odds.away_ml && odds.home_ml < odds.away_ml
  const dAwayPt = hasReal ? odds.away_spread_point : (homeFav ? 1.5 : -1.5)
  const dHomePt = hasReal ? odds.home_spread_point : (homeFav ? -1.5 : 1.5)
  const dAwayOdds = odds.away_spread_odds || (dAwayPt > 0 ? -140 : 120)
  const dHomeOdds = odds.home_spread_odds || (dHomePt > 0 ? -140 : 120)

  return (
    <div className="game-odds-grid">
      {(odds.home_ml || odds.away_ml) && (
        <div className="odds-line">
          <span className="odds-label">ML</span>
          <span className="odds-val">{away.abbreviation} {odds.away_ml > 0 ? '+' : ''}{odds.away_ml || '-'}</span>
          <span className="odds-val">{home.abbreviation} {odds.home_ml > 0 ? '+' : ''}{odds.home_ml || '-'}</span>
        </div>
      )}
      {odds.over_under && (
        <div className="odds-line">
          <span className="odds-label">O/U</span>
          <span className="odds-val">o{odds.over_under} {odds.over_odds ? `(${Math.round(odds.over_odds) > 0 ? '+' : ''}${Math.round(odds.over_odds)})` : ''}</span>
          <span className="odds-val">u{odds.over_under} {odds.under_odds ? `(${Math.round(odds.under_odds) > 0 ? '+' : ''}${Math.round(odds.under_odds)})` : ''}</span>
        </div>
      )}
      <div className="odds-line">
        <span className="odds-label">RL</span>
        <span className="odds-val">
          {away.abbreviation} {dAwayPt > 0 ? '+' : ''}{dAwayPt}
          {` (${dAwayOdds > 0 ? '+' : ''}${Math.round(dAwayOdds)})`}
        </span>
        <span className="odds-val">
          {home.abbreviation} {dHomePt > 0 ? '+' : ''}{dHomePt}
          {` (${dHomeOdds > 0 ? '+' : ''}${Math.round(dHomeOdds)})`}
        </span>
      </div>
    </div>
  )
}


function MLBCardInsight({ bet, home, away, game }) {
  // One-liner explaining WHY the model picked this side. MLB-specific
  // signals: pitcher ERA gap, wRC+ gap, form, injuries, home/road
  // splits. Mirrors NHL's CardInsight in structure.
  const f = bet.factors || {}
  const reasons = []

  // Pitcher matchup
  const hP = game.home_pitcher || {}
  const aP = game.away_pitcher || {}
  if (hP.era > 0 && aP.era > 0) {
    const diff = Math.abs(hP.era - aP.era)
    if (diff >= 1.0) {
      const better = hP.era < aP.era ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: diff * 5,
        text: <><strong>{better}</strong> has SP ERA edge ({Math.min(hP.era, aP.era).toFixed(2)})</>
      })
    }
  }

  // Offense gap via wRC+
  const hWrc = f.home_wrc_plus
  const aWrc = f.away_wrc_plus
  if (hWrc != null && aWrc != null && Math.abs(hWrc - aWrc) > 15) {
    const better = hWrc > aWrc ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(hWrc - aWrc) / 3,
      text: <><strong>{better}</strong> wRC+ edge ({Math.max(hWrc, aWrc).toFixed(0)})</>
    })
  }

  // Recent form gap
  const hForm = f.home_form
  const aForm = f.away_form
  if (hForm != null && aForm != null && Math.abs(hForm - aForm) > 0.05) {
    const hotter = hForm > aForm ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(hForm - aForm) * 40,
      text: <><strong>{hotter}</strong> running hot ({((hForm > aForm ? hForm : aForm) * 100).toFixed(0)}%)</>
    })
  }

  // Injuries - plain English
  const hImp = bet.injuries?.home_impact
  const aImp = bet.injuries?.away_impact
  if (hImp != null && hImp < 0.92) {
    reasons.push({
      weight: (1 - hImp) * 50,
      text: <><strong>{home.abbreviation}</strong> shorthanded ({Math.round((1 - hImp) * 100)}% weaker)</>
    })
  }
  if (aImp != null && aImp < 0.92) {
    reasons.push({
      weight: (1 - aImp) * 50,
      text: <><strong>{away.abbreviation}</strong> shorthanded ({Math.round((1 - aImp) * 100)}% weaker)</>
    })
  }

  // Park factor (distinctive venues only)
  const park = f.park_factor
  if (park != null && Math.abs(park - 1.0) >= 0.04) {
    const tag = park > 1.04 ? 'hitter-friendly park' : 'pitcher-friendly park'
    reasons.push({
      weight: Math.abs(park - 1.0) * 20,
      text: <>Venue: {tag} ({park.toFixed(2)})</>
    })
  }

  if (reasons.length === 0) return null

  reasons.sort((a, b) => b.weight - a.weight)
  return (
    <div className="card-insight">
      {reasons[0].text}
    </div>
  )
}
