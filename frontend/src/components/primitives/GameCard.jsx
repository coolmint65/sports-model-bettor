/**
 * GameCard
 * ──────────────────────────────────────────────────────────────
 * Sport-agnostic scorecard shell. Provides the shared skeleton
 * (live/final badge, one-pick EdgeBadge, rest chips, line-moved
 * chip, team rows, win-prob bar, game meta) and accepts render
 * props for the sport-specific sections that don't share a shape:
 *
 *   insight       -> node rendered between win-prob bar and starters
 *                     (MLB: MLBCardInsight, NHL: CardInsight)
 *   starters      -> node for pitchers/goalies line
 *   odds          -> node for the .game-odds-grid block
 *   liveExtras    -> node rendered above win-prob for live games
 *                     (NBA uses this to show Q1 score + quarter)
 *
 * Props:
 *   pickAccent = 'q1'  -> amber EdgeBadge styling for NBA
 *   restTiredLabel     -> "tired" (MLB) or "B2B" (NHL/NBA)
 */

import EdgeBadge from './EdgeBadge'
import WinProbBar from './WinProbBar'
import TeamRow from './TeamRow'
import RestChips from './RestChips'
import LineMovedChip from './LineMovedChip'

export default function GameCard({
  game,
  bet,
  onClick,
  insight,
  starters,
  odds,
  liveExtras,
  pickAccent,
  restTiredLabel = 'tired',
}) {
  const { home, away, status } = game
  const isLive = status.state === 'in'
  const isFinal = status.state === 'post'
  const isPre = status.state === 'pre'
  const conf = bet?.confidence || 'skip'
  const hasPick = bet?.best_pick && conf !== 'skip'

  return (
    <div
      className={`game-card ${isLive ? 'live' : ''} card-${conf}`}
      onClick={onClick}
      role="button"
      tabIndex={0}
    >
      {isLive && <div className="live-badge">LIVE</div>}
      {isFinal && <div className="final-badge">FINAL</div>}

      {isPre && (
        hasPick
          ? <EdgeBadge pick={bet.best_pick} confidence={conf} accent={pickAccent} />
          : <EdgeBadge empty />
      )}

      {isPre && <RestChips rest={bet?.rest} home={home} away={away} tiredLabel={restTiredLabel} />}
      {isPre && <LineMovedChip lm={game.line_movement} />}

      {game.series?.in_series && (
        <div className="series-badge" style={{
          fontSize: '0.7rem', color: '#94a3b8', textAlign: 'center',
          padding: '2px 0', letterSpacing: '0.02em',
        }}>
          Game {game.series.game_number}
          {game.series.game_number > 1 && (
            <> &middot; {
              game.series.is_tied
                ? `Tied ${game.series.home_wins}-${game.series.away_wins}`
                : game.series.series_leader === 'home'
                  ? `${home.abbreviation} leads ${game.series.home_wins}-${game.series.away_wins}`
                  : `${away.abbreviation} leads ${game.series.away_wins}-${game.series.home_wins}`
            }</>
          )}
          {game.series.is_elimination && <> &middot; <span style={{ color: '#ef4444' }}>ELIMINATION</span></>}
        </div>
      )}

      <div className="game-teams">
        <TeamRow team={away} isLive={isLive} isFinal={isFinal} />
        <div className="game-at">@</div>
        <TeamRow team={home} isLive={isLive} isFinal={isFinal} />
      </div>

      {liveExtras}

      {isPre && bet?.win_prob?.home != null && (
        <WinProbBar wp={bet.win_prob} home={home} away={away} />
      )}

      {isPre && hasPick && insight}
      {isPre && starters}
      {odds}

      <div className="game-meta">
        {isPre && (
          <span className="game-time">
            {new Date(game.date).toLocaleTimeString([], { hour: 'numeric', minute: '2-digit' })}
          </span>
        )}
        {isLive && <span className="game-inning">{status.detail}</span>}
        {game.broadcast && <span className="game-broadcast">{game.broadcast}</span>}
      </div>
    </div>
  )
}
