import { memo } from 'react'
import GameCard from './primitives/GameCard'
import { OddsGrid, OddsRow } from './primitives/OddsGrid'
import ScoreboardShell from './primitives/ScoreboardShell'
import FinalRow from './primitives/FinalRow'

function NBAScoreboardImpl(props) {
  return (
    <ScoreboardShell
      {...props}
      title="NBA Games"
      loadingLabel="Loading NBA games..."
      emptyPrimary="No NBA games scheduled today."
      emptySub="Check back for the next slate."
      edgeLabel="Q1 plays with edge"
      renderCard={({ game, bet, onClick, key }) => (
        <NBAGameCard key={key} game={game} bet={bet} onClick={onClick} sport="nba" />
      )}
      renderFinal={({ game, onClick, key }) => {
        const q1 = game.q1 || {}
        const extra = q1.home != null && q1.away != null ? (
          <div className="q1-final-line">
            Q1: {game.away.abbreviation} {q1.away} - {game.home.abbreviation} {q1.home}
          </div>
        ) : null
        return <FinalRow key={key} game={game} onClick={onClick} extra={extra} />
      }}
    />
  )
}

const NBAScoreboard = memo(NBAScoreboardImpl)
export default NBAScoreboard


function NBAGameCard({ game, bet, onClick, sport }) {
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
      sport={sport}
    />
  )
}

function NBAOddsGrid({ odds, home, away }) {
  return (
    <OddsGrid>
      {(odds.home_spread_point != null || odds.away_spread_point != null) && (
        <OddsRow label="SPR"
          away={`${away.abbreviation} ${odds.away_spread_point > 0 ? '+' : ''}${odds.away_spread_point || '-'}${odds.away_spread_odds ? ` (${odds.away_spread_odds > 0 ? '+' : ''}${Math.round(odds.away_spread_odds)})` : ''}`}
          home={`${home.abbreviation} ${odds.home_spread_point > 0 ? '+' : ''}${odds.home_spread_point || '-'}${odds.home_spread_odds ? ` (${odds.home_spread_odds > 0 ? '+' : ''}${Math.round(odds.home_spread_odds)})` : ''}`}
        />
      )}
      {odds.over_under && (
        <OddsRow label="O/U"
          away={`o${odds.over_under}${odds.over_odds ? ` (${Math.round(odds.over_odds) > 0 ? '+' : ''}${Math.round(odds.over_odds)})` : ''}`}
          home={`u${odds.over_under}${odds.under_odds ? ` (${Math.round(odds.under_odds) > 0 ? '+' : ''}${Math.round(odds.under_odds)})` : ''}`}
        />
      )}
      {(odds.home_ml || odds.away_ml) && (
        <OddsRow label="ML"
          away={`${away.abbreviation} ${odds.away_ml > 0 ? '+' : ''}${odds.away_ml || '-'}`}
          home={`${home.abbreviation} ${odds.home_ml > 0 ? '+' : ''}${odds.home_ml || '-'}`}
        />
      )}
    </OddsGrid>
  )
}


