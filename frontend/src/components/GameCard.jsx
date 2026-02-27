import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Clock, MapPin, ChevronRight, TrendingUp } from 'lucide-react';
import { format, parseISO } from 'date-fns';

function getConfidenceColor(confidence) {
  if (confidence >= 75) return '#00ff88';
  if (confidence >= 60) return '#4fc3f7';
  if (confidence >= 45) return '#ffd700';
  return '#ff5252';
}

function getStatusDisplay(game) {
  const status = game.status || game.game_state || '';
  const statusLower = status.toLowerCase();

  if (statusLower === 'final' || statusLower === 'completed' || statusLower === 'off') {
    return {
      label: 'Final',
      className: 'status-final',
      showScore: true,
    };
  }
  if (statusLower === 'live' || statusLower === 'in_progress' || statusLower === 'active') {
    return {
      label: 'LIVE',
      className: 'status-live',
      showScore: true,
    };
  }
  return {
    label: null,
    className: 'status-scheduled',
    showScore: false,
  };
}

function formatGameTime(game) {
  try {
    const dateStr = game.start_time || game.datetime || game.date;
    if (!dateStr) return 'TBD';
    const date = typeof dateStr === 'string' ? parseISO(dateStr) : new Date(dateStr);
    return format(date, 'h:mm a');
  } catch {
    return game.time || 'TBD';
  }
}

function GameCard({ game }) {
  const navigate = useNavigate();
  const gameId = game.game_id || game.id;
  const statusInfo = getStatusDisplay(game);

  const awayTeam = game.away_team || game.teams?.away?.name || 'Away';
  const homeTeam = game.home_team || game.teams?.home?.name || 'Home';
  const awayAbbrev = game.away_abbreviation || game.teams?.away?.abbreviation || awayTeam.substring(0, 3).toUpperCase();
  const homeAbbrev = game.home_abbreviation || game.teams?.home?.abbreviation || homeTeam.substring(0, 3).toUpperCase();
  const awayScore = game.away_score ?? game.score?.away ?? null;
  const homeScore = game.home_score ?? game.score?.home ?? null;
  const venue = game.venue || game.arena || '';
  const confidence = game.top_confidence || game.confidence || game.prediction_confidence || null;

  const handleClick = () => {
    if (gameId) {
      navigate(`/games/${gameId}`);
    }
  };

  return (
    <div
      className={`game-card ${statusInfo.className}`}
      onClick={handleClick}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => e.key === 'Enter' && handleClick()}
    >
      {statusInfo.label && (
        <div className={`game-status-badge ${statusInfo.className}`}>
          {statusInfo.label === 'LIVE' && <span className="live-dot"></span>}
          {statusInfo.label}
        </div>
      )}

      <div className="game-card-body">
        {/* Away Team */}
        <div className="game-team">
          <div className="team-abbrev">{awayAbbrev}</div>
          <div className="team-name">{awayTeam}</div>
          {statusInfo.showScore && awayScore !== null && (
            <div className={`team-score ${awayScore > homeScore ? 'score-winning' : ''}`}>
              {awayScore}
            </div>
          )}
        </div>

        {/* VS / Time Divider */}
        <div className="game-divider">
          {statusInfo.showScore ? (
            <span className="vs-label">{statusInfo.label}</span>
          ) : (
            <>
              <Clock size={14} />
              <span className="game-time">{formatGameTime(game)}</span>
            </>
          )}
        </div>

        {/* Home Team */}
        <div className="game-team">
          <div className="team-abbrev">{homeAbbrev}</div>
          <div className="team-name">{homeTeam}</div>
          {statusInfo.showScore && homeScore !== null && (
            <div className={`team-score ${homeScore > awayScore ? 'score-winning' : ''}`}>
              {homeScore}
            </div>
          )}
        </div>
      </div>

      {/* Footer: Venue + Confidence */}
      <div className="game-card-footer">
        {venue && (
          <div className="game-venue">
            <MapPin size={12} />
            <span>{venue}</span>
          </div>
        )}
        {confidence && (
          <div className="game-confidence" title={`Top prediction confidence: ${confidence}%`}>
            <TrendingUp size={12} />
            <span
              className="confidence-text"
              style={{ color: getConfidenceColor(confidence) }}
            >
              {typeof confidence === 'number' ? `${confidence.toFixed(0)}%` : confidence}
            </span>
          </div>
        )}
      </div>

      <div className="game-card-arrow">
        <ChevronRight size={18} />
      </div>
    </div>
  );
}

export default GameCard;
