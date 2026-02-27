import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Clock, MapPin, ChevronRight, TrendingUp } from 'lucide-react';
import { format } from 'date-fns';
import { teamName, teamAbbrev, confidencePct, parseAsUTC } from '../utils/teams';

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
    const dateStr = game.start_time || game.datetime;
    if (!dateStr) return 'TBD';
    const date = parseAsUTC(dateStr);
    if (!date || isNaN(date.getTime())) return 'TBD';
    return format(date, 'h:mm a');
  } catch {
    return game.time || 'TBD';
  }
}

function GameCard({ game }) {
  const navigate = useNavigate();
  const gameId = game.game_id || game.id;
  const statusInfo = getStatusDisplay(game);

  const awayName = teamName(game.away_team, 'Away');
  const homeName = teamName(game.home_team, 'Home');
  const awayAbbr = teamAbbrev(game.away_team, 'AWY');
  const homeAbbr = teamAbbrev(game.home_team, 'HME');
  const awayScore = game.away_score ?? game.score?.away ?? null;
  const homeScore = game.home_score ?? game.score?.home ?? null;
  const venue = game.venue || game.arena || '';
  const rawConf = game.top_confidence || game.confidence || game.prediction_confidence || null;
  const confidence = rawConf != null ? confidencePct(rawConf) : null;

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
          <div className="team-abbrev">{awayAbbr}</div>
          <div className="team-name">{awayName}</div>
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
          <div className="team-abbrev">{homeAbbr}</div>
          <div className="team-name">{homeName}</div>
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
        {confidence != null && (
          <div className="game-confidence" title={`Top prediction confidence: ${confidence.toFixed(0)}%`}>
            <TrendingUp size={12} />
            <span
              className="confidence-text"
              style={{ color: getConfidenceColor(confidence) }}
            >
              {confidence.toFixed(0)}%
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
