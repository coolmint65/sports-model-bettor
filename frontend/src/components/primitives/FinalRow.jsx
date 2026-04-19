/**
 * FinalRow
 * ──────────────────────────────────────────────────────────────
 * Compact "final score" row used in the Final section of every
 * scoreboard. NBA appends a Q1 recap line via the `extra` slot;
 * MLB and NHL pass nothing.
 */

export default function FinalRow({ game, onClick, extra }) {
  const { home, away } = game
  const hs = parseInt(home.score) || 0
  const as = parseInt(away.score) || 0
  const homeWon = hs > as

  return (
    <div className="game-final-row" onClick={onClick} role="button" tabIndex={0}>
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
      {extra}
    </div>
  )
}
