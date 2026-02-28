import React, { useState, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { Clock, MapPin, ChevronRight, TrendingUp, Target } from 'lucide-react';
import { format } from 'date-fns';
import { teamName, teamAbbrev, teamLogo, confidencePct, parseAsUTC, formatBetType, formatPredictionValue } from '../utils/teams';

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
      isLive: false,
    };
  }
  if (statusLower === 'live' || statusLower === 'in_progress' || statusLower === 'active') {
    return {
      label: 'LIVE',
      className: 'status-live',
      showScore: true,
      isLive: true,
    };
  }
  return {
    label: null,
    className: 'status-scheduled',
    showScore: false,
    isLive: false,
  };
}

function formatPeriod(game) {
  const period = game.period;
  const periodType = game.period_type;
  const clock = game.clock;

  if (!period) return { label: null, clock: null };

  let periodLabel;
  if (periodType === 'OT') {
    periodLabel = 'OT';
  } else if (periodType === 'SO') {
    periodLabel = 'SO';
  } else if (period === 1) {
    periodLabel = '1st';
  } else if (period === 2) {
    periodLabel = '2nd';
  } else if (period === 3) {
    periodLabel = '3rd';
  } else {
    periodLabel = `${period}th`;
  }

  return { label: periodLabel, clock: clock || null };
}

function Countdown({ startTime }) {
  const [timeLeft, setTimeLeft] = useState('');

  useEffect(() => {
    function update() {
      const now = new Date();
      const start = parseAsUTC(startTime);
      if (!start || isNaN(start.getTime())) {
        setTimeLeft('TBD');
        return;
      }

      const diff = start.getTime() - now.getTime();
      if (diff <= 0) {
        setTimeLeft('Starting');
        return;
      }

      const hours = Math.floor(diff / (1000 * 60 * 60));
      const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));

      if (hours > 24) {
        const days = Math.floor(hours / 24);
        setTimeLeft(`${days}d ${hours % 24}h`);
      } else if (hours > 0) {
        setTimeLeft(`${hours}h ${mins}m`);
      } else {
        const secs = Math.floor((diff % (1000 * 60)) / 1000);
        setTimeLeft(`${mins}m ${secs}s`);
      }
    }

    update();
    const interval = setInterval(update, 1000);
    return () => clearInterval(interval);
  }, [startTime]);

  return <span className="game-countdown">{timeLeft}</span>;
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

function TeamLogo({ team, size = 36 }) {
  const logo = teamLogo(team);
  if (!logo) return null;
  return (
    <img
      className="team-logo"
      src={logo}
      alt=""
      width={size}
      height={size}
      loading="lazy"
      onError={(e) => { e.target.style.display = 'none'; }}
    />
  );
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
  const topPick = game.top_pick || null;
  const rawConf = topPick?.confidence || game.top_confidence || game.confidence || game.prediction_confidence || null;
  const confidence = rawConf != null ? confidencePct(rawConf) : null;
  const hasBadge = !!statusInfo.label;
  const startTime = game.start_time || game.datetime;

  // Live game period info
  const periodInfo = statusInfo.isLive ? formatPeriod(game) : { label: null, clock: null };

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
      {hasBadge && (
        <div className={`game-status-badge ${statusInfo.className}`}>
          {statusInfo.label === 'LIVE' && <span className="live-dot"></span>}
          {statusInfo.label}
        </div>
      )}

      <div className={`game-card-body ${hasBadge ? 'has-badge' : ''}`}>
        {/* Away Team */}
        <div className="game-team">
          <TeamLogo team={game.away_team} size={36} />
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
          {statusInfo.isLive ? (
            <div className="live-divider">
              <span className="vs-label live-label">{periodInfo.label || 'LIVE'}</span>
              <span className="live-clock">{periodInfo.clock || '--:--'}</span>
              {(game.home_shots != null || game.away_shots != null) && (
                <span className="live-shots">SOG: {game.away_shots ?? 0}-{game.home_shots ?? 0}</span>
              )}
            </div>
          ) : statusInfo.showScore ? (
            <span className="vs-label">{statusInfo.label}</span>
          ) : (
            <div className="scheduled-divider">
              <div className="game-time-row">
                <Clock size={14} />
                <span className="game-time">{formatGameTime(game)}</span>
              </div>
              {startTime && (
                <Countdown startTime={startTime} />
              )}
            </div>
          )}
        </div>

        {/* Home Team */}
        <div className="game-team">
          <TeamLogo team={game.home_team} size={36} />
          <div className="team-abbrev">{homeAbbr}</div>
          <div className="team-name">{homeName}</div>
          {statusInfo.showScore && homeScore !== null && (
            <div className={`team-score ${homeScore > awayScore ? 'score-winning' : ''}`}>
              {homeScore}
            </div>
          )}
        </div>
      </div>

      {/* Footer: Top Pick or Venue + Confidence */}
      <div className="game-card-footer">
        {topPick ? (
          <div className="game-top-pick">
            <Target size={12} />
            <span className="top-pick-type">{formatBetType(topPick.bet_type)}</span>
            <span className="top-pick-value">{formatPredictionValue(topPick.prediction_value)}</span>
          </div>
        ) : venue ? (
          <div className="game-venue">
            <MapPin size={12} />
            <span>{venue}</span>
          </div>
        ) : <div />}
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
