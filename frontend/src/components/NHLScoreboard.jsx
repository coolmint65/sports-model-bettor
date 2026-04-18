import GameCard from './primitives/GameCard'

// League-average NHL SV% is ~0.905. Color + tooltip labels anchor the
// user's eye so a ".915" starter is recognized as above average without
// needing to remember the league baseline.
function svPctColor(sv) {
  if (sv >= 0.915) return '#34d399'   // elite green
  if (sv >= 0.905) return '#94a3b8'   // average slate
  if (sv >= 0.895) return '#fbbf24'   // below-average amber
  return '#ef4444'                    // poor red
}
function svPctLabel(sv) {
  if (sv >= 0.920) return 'is elite (top ~10%)'
  if (sv >= 0.910) return 'is above average'
  if (sv >= 0.900) return 'is league average'
  if (sv >= 0.890) return 'is below average'
  return 'has struggled this season'
}


export default function NHLScoreboard({ games, loading, progress, onSelectGame, bestBets }) {
  if (loading) {
    const total = progress?.total || 0
    const rawDone = progress?.done || 0
    const done = total > 0 ? Math.min(rawDone, total) : rawDone
    const pct = total ? Math.min(100, Math.round((done / total) * 100)) : null
    const phase = progress?.phase
    let label = 'Loading NHL games...'
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
        <p>No NHL games scheduled today.</p>
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

  // Sort finals by most recent first (natural order usually works)
  const edgeCount = activeGames.filter(g =>
    betMap[g.id]?.confidence === 'strong' || betMap[g.id]?.confidence === 'moderate'
  ).length

  return (
    <div className="scoreboard">
      <h2 className="section-title">
        NHL Games ({games.length})
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
              <NHLGameCard
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
              <NHLFinalRow
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


function NHLFinalRow({ game, bet, onClick }) {
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

function NHLGameCard({ game, bet, onClick }) {
  const { home, away } = game

  const starters = (game.away_goalie || game.home_goalie) ? (
    <div className="game-pitchers">
      <GoalieSlot g={game.away_goalie} />
      <span className="vs">vs</span>
      <GoalieSlot g={game.home_goalie} />
    </div>
  ) : null

  const odds = game.odds ? <NHLOddsGrid odds={game.odds} home={home} away={away} /> : null

  const insight = <CardInsight bet={bet} home={home} away={away} game={game} />

  return (
    <GameCard
      game={game}
      bet={bet}
      onClick={onClick}
      insight={insight}
      starters={starters}
      odds={odds}
      restTiredLabel="B2B"
    />
  )
}

function GoalieSlot({ g }) {
  if (!g?.name) return <span className="pitcher">TBD</span>
  return (
    <span className="pitcher">
      {g.name}
      {g.status === 'confirmed' && (
        <span
          className="goalie-mark goalie-confirmed"
          title="Confirmed starter (DailyFaceoff)"
          aria-label="Confirmed starting goalie"
        >✓</span>
      )}
      {g.status === 'expected' && (
        <span
          className="goalie-mark goalie-expected"
          title="Projected starter (not yet confirmed)"
          aria-label="Projected starting goalie, not confirmed"
        >~</span>
      )}
      {g.save_pct > 0 && (
        <span
          className="goalie-svpct"
          style={{ color: svPctColor(g.save_pct) }}
          title={`League average: ~0.905. This starter ${svPctLabel(g.save_pct)}.`}
        >
          {g.save_pct.toFixed(3)} SV%
        </span>
      )}
    </span>
  )
}

function NHLOddsGrid({ odds, home, away }) {
  const hasReal = odds.away_spread_point != null || odds.home_spread_point != null
  const homeFav = odds.home_ml && odds.away_ml && odds.home_ml < odds.away_ml
  const dAwayPt = hasReal ? odds.away_spread_point : (homeFav ? 1.5 : -1.5)
  const dHomePt = hasReal ? odds.home_spread_point : (homeFav ? -1.5 : 1.5)
  const dAwayOdds = odds.away_spread_odds || (dAwayPt > 0 ? -180 : 150)
  const dHomeOdds = odds.home_spread_odds || (dHomePt > 0 ? -180 : 150)

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
        <span className="odds-label">PL</span>
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


function CardInsight({ bet, home, away, game }) {
  // Generate a short "why" reason from the strongest factor
  const f = bet.factors || {}
  const wp = bet.win_prob || {}
  const ctx = bet.season_context || {}
  const rest = bet.rest || {}

  const reasons = []

  // Goalie matchup
  if (game.home_goalie?.save_pct > 0 && game.away_goalie?.save_pct > 0) {
    const diff = Math.abs(game.home_goalie.save_pct - game.away_goalie.save_pct)
    if (diff >= 0.010) {
      const better = game.home_goalie.save_pct > game.away_goalie.save_pct
        ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: diff * 100,
        text: <><strong>{better}</strong> has goalie edge ({(Math.max(game.home_goalie.save_pct, game.away_goalie.save_pct)).toFixed(3)})</>
      })
    }
  }

  // Form / L10 gap
  const hL10 = ctx.home?.l10_pts_pct
  const aL10 = ctx.away?.l10_pts_pct
  if (hL10 != null && aL10 != null && Math.abs(hL10 - aL10) > 0.2) {
    const hotter = hL10 > aL10 ? home.abbreviation : away.abbreviation
    const hotPct = Math.max(hL10, aL10)
    reasons.push({
      weight: Math.abs(hL10 - aL10) * 20,
      text: <><strong>{hotter}</strong> hot in L10 ({(hotPct * 100).toFixed(0)}%)</>
    })
  }

  // Rest advantage
  if (rest.home_b2b || rest.away_b2b) {
    const tired = rest.home_b2b ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: 5,
      text: <><strong>{tired}</strong> on back-to-back (fatigue penalty)</>
    })
  }

  // PP mismatch
  const hPP = f.home_pp, aPP = f.away_pp
  if (hPP != null && aPP != null) {
    const ppDiff = Math.abs(hPP - aPP) * 100
    if (ppDiff > 4) {
      const better = hPP > aPP ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: ppDiff,
        text: <><strong>{better}</strong> has {ppDiff.toFixed(1)}% PP edge</>
      })
    }
  }

  // Motivation gap
  const hM = ctx.home?.motivation, aM = ctx.away?.motivation
  if (hM != null && aM != null && Math.abs(hM - aM) > 0.25) {
    const more = hM > aM ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(hM - aM) * 10,
      text: <><strong>{more}</strong> has playoff-race motivation</>
    })
  }

  // Injuries - plain English
  const hImp = bet.injuries?.home_impact
  const aImp = bet.injuries?.away_impact
  if (hImp != null && hImp < 0.92) {
    const pct = Math.round((1 - hImp) * 100)
    reasons.push({
      weight: (1 - hImp) * 50,
      text: <><strong>{home.abbreviation}</strong> severely shorthanded ({pct}% weaker)</>
    })
  }
  if (aImp != null && aImp < 0.92) {
    reasons.push({
      weight: (1 - aImp) * 50,
      text: <><strong>{away.abbreviation}</strong> severely shorthanded ({Math.round((1 - aImp) * 100)}% weaker)</>
    })
  }

  if (reasons.length === 0) return null

  // Show strongest
  reasons.sort((a, b) => b.weight - a.weight)
  return (
    <div className="card-insight">
      {reasons[0].text}
    </div>
  )
}
