import LineMovementBadge from './LineMovementBadge'

/**
 * The shared top section of a game detail page.
 *
 * Handles the pieces that are identical between MLB and NHL:
 *   - back button
 *   - live / final status badges
 *   - team matchup row (logos, names, records, live scores)
 *   - venue / broadcast / start-time row
 *   - odds chips + line movement badge
 *
 * Sport-specific sections plug in via render props / children so each
 * wrapper still controls its own pitcher-vs-pitcher, goalie, rest, and
 * confirmation badges. Keeping this as a slot-based component lets us
 * refactor without changing any rendered HTML.
 *
 * Props:
 *   game:            raw game object (home, away, status, venue, odds, ...)
 *   onBack:          back-button handler
 *   matchupExtras:   ReactNode rendered below the detail-matchup block
 *                    (pitcher / goalie cards, confirmation badges,
 *                    rest-advantage pills, etc.)
 */
export default function SharedGameHeader({ game, onBack, matchupExtras }) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'

  const lm = game.line_movement
  const lmSignificant = lm && lm.significance && lm.significance !== 'none'

  return (
    <>
      <button className="back-btn" onClick={onBack}>
        <span className="back-arrow">&larr;</span> Back to games
      </button>

      <div className="detail-header">
        {isLive && <div className="live-badge">LIVE</div>}
        {isFinal && <div className="final-badge">FINAL</div>}

        <div className="detail-matchup">
          <div className="detail-team">
            {away.logo && <img src={away.logo} alt="" className="detail-logo" />}
            <div className="detail-team-name">{away.name}</div>
            <div className="detail-team-record">{away.record}</div>
            {(isLive || isFinal) && (
              <div className={`detail-score ${away.winner ? 'winner' : ''}`}>{away.score}</div>
            )}
          </div>

          <div className="detail-at">@</div>

          <div className="detail-team">
            {home.logo && <img src={home.logo} alt="" className="detail-logo" />}
            <div className="detail-team-name">{home.name}</div>
            <div className="detail-team-record">{home.record}</div>
            {(isLive || isFinal) && (
              <div className={`detail-score ${home.winner ? 'winner' : ''}`}>{home.score}</div>
            )}
          </div>
        </div>

        {matchupExtras}

        <div className="detail-info">
          {game.venue && <span>{game.venue}</span>}
          {game.broadcast && <span>{game.broadcast}</span>}
          {status.state === 'pre' && (
            <span>{new Date(game.date).toLocaleString([], {
              weekday: 'short', month: 'short', day: 'numeric',
              hour: 'numeric', minute: '2-digit',
            })}</span>
          )}
          {isLive && <span className="live-clock">{status.detail}</span>}
        </div>

        {game.odds && (
          <div className="detail-odds">
            {game.odds.home_ml && (
              <span className="odds-chip ml">
                {home.abbreviation} {game.odds.home_ml > 0 ? '+' : ''}{game.odds.home_ml}
              </span>
            )}
            {game.odds.away_ml && (
              <span className="odds-chip ml">
                {away.abbreviation} {game.odds.away_ml > 0 ? '+' : ''}{game.odds.away_ml}
              </span>
            )}
            {game.odds.over_under && <span className="odds-chip">O/U {game.odds.over_under}</span>}
            {lmSignificant && <LineMovementBadge lm={lm} home={home} away={away} />}
          </div>
        )}
      </div>
    </>
  )
}
