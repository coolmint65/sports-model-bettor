import { useState, useEffect, useRef, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Clock,
  ChevronRight,
  TrendingUp,
  Calendar,
  Minus,
  Target,
  Plus,
  Check,
} from 'lucide-react';
import {
  teamName,
  teamAbbrev,
  confidencePct,
  formatBetType,
} from '../utils/teams';
import { formatAmericanOdds, formatGameDate, formatGameTime } from '../utils/formatting';
import { trackBet } from '../utils/api';
import TeamLogo from './shared/TeamLogo';

function getStatusDisplay(game) {
  const status = game.status || game.game_state || '';
  const statusLower = status.toLowerCase();

  if (statusLower === 'final' || statusLower === 'completed' || statusLower === 'off') {
    return { label: 'Final', className: 'status-final', showScore: true, isLive: false, isFinal: true };
  }
  if (statusLower === 'live' || statusLower === 'in_progress' || statusLower === 'active') {
    return { label: 'LIVE', className: 'status-live', showScore: true, isLive: true, isFinal: false };
  }
  return { label: null, className: 'status-scheduled', showScore: false, isLive: false, isFinal: false };
}

function formatPeriod(game) {
  const period = game.period;
  const periodType = game.period_type;
  const clock = game.clock;
  const inIntermission = game.in_intermission;

  if (!period) return { label: null, clock: null, intermission: false };

  let periodLabel;
  if (periodType === 'OT') periodLabel = 'OT';
  else if (periodType === 'SO') periodLabel = 'SO';
  else if (period === 1) periodLabel = '1st';
  else if (period === 2) periodLabel = '2nd';
  else if (period === 3) periodLabel = '3rd';
  else periodLabel = `${period}th`;

  if (inIntermission) {
    return { label: `End ${periodLabel}`, clock: null, intermission: true };
  }
  return { label: periodLabel, clock: clock || null, intermission: false };
}

function LiveClock({ serverClock, running }) {
  const hasClock = serverClock != null && serverClock !== '';
  const [seconds, setSeconds] = useState(() => parseClock(serverClock));
  const prevServer = useRef(serverClock);

  useEffect(() => {
    if (serverClock !== prevServer.current) {
      prevServer.current = serverClock;
      setSeconds(parseClock(serverClock));
    }
  }, [serverClock]);

  useEffect(() => {
    if (!running || seconds <= 0) return;
    const id = setInterval(() => {
      setSeconds((s) => Math.max(0, s - 1));
    }, 1000);
    return () => clearInterval(id);
  }, [running, seconds > 0]); // eslint-disable-line react-hooks/exhaustive-deps

  if (!hasClock) {
    return <span className="live-clock">--:--</span>;
  }

  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  const display = `${String(mins).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
  return <span className="live-clock">{display}</span>;
}

function parseClock(str) {
  if (!str) return 0;
  const parts = str.split(':');
  if (parts.length !== 2) return 0;
  return (parseInt(parts[0], 10) || 0) * 60 + (parseInt(parts[1], 10) || 0);
}


function getConfidenceBadge(confidence) {
  if (confidence == null) return null;
  if (confidence >= 70) return { label: 'GOOD BET', className: 'badge-good', icon: TrendingUp };
  if (confidence >= 60) return { label: 'LEAN', className: 'badge-borderline', icon: Minus };
  return { label: 'SKIP', className: 'badge-skip', icon: Minus };
}

const MEDAL_STYLES = {
  gold: { className: 'rank-gold', label: '#1' },
  silver: { className: 'rank-silver', label: '#2' },
  bronze: { className: 'rank-bronze', label: '#3' },
};

const SPORT_LABELS = {
  nhl: { sport: 'Hockey', league: 'NHL' },
  nba: { sport: 'Basketball', league: 'NBA' },
  nfl: { sport: 'Football', league: 'NFL' },
  mlb: { sport: 'Baseball', league: 'MLB' },
};

function GameCard({ game, section, medal }) {
  const navigate = useNavigate();
  const [tracked, setTracked] = useState(false);
  const [tracking, setTracking] = useState(false);
  const gameId = game.game_id || game.id;
  const statusInfo = getStatusDisplay(game);
  const sportKey = (game.sport || 'nhl').toLowerCase();
  const sportLabel = SPORT_LABELS[sportKey] || SPORT_LABELS.nhl;

  const awayName = teamName(game.away_team, 'Away');
  const homeName = teamName(game.home_team, 'Home');
  const awayAbbr = teamAbbrev(game.away_team, 'AWY');
  const homeAbbr = teamAbbrev(game.home_team, 'HME');
  const awayScore = game.away_score ?? game.score?.away ?? null;
  const homeScore = game.home_score ?? game.score?.home ?? null;
  const topPick = game.top_pick || null;
  // Prefer bet_confidence (signal-based) over confidence (win probability)
  const rawConf = topPick?.bet_confidence || topPick?.confidence || game.top_confidence || game.confidence || game.prediction_confidence || null;
  const confidence = rawConf != null ? confidencePct(rawConf) : null;
  const odds = game.odds || null;
  const periodInfo = statusInfo.isLive ? formatPeriod(game) : { label: null, clock: null };

  const handleClick = () => {
    if (gameId) navigate(`/games/${gameId}`);
  };

  // For live games, keep compact card style
  const medalStyle = medal ? MEDAL_STYLES[medal] : null;
  if (statusInfo.isLive) {
    return (
      <div
        className={`game-card ${statusInfo.className}`}
        onClick={handleClick}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => e.key === 'Enter' && handleClick()}
      >
        {medalStyle && (
          <div className={`dc-rank ${medalStyle.className}`}>{medalStyle.label}</div>
        )}
        <div className="game-status-badge status-live">
          <span className="live-dot"></span>
          LIVE
        </div>

        <div className="game-card-body has-badge">
          <div className="game-team">
            <TeamLogo team={game.away_team} size={36} />
            <div className="team-abbrev">{awayAbbr}</div>
            <div className="team-name">{awayName}</div>
            {awayScore !== null && (
              <div className={`team-score ${awayScore > homeScore ? 'score-winning' : ''}`}>
                {awayScore}
              </div>
            )}
          </div>

          <div className="game-divider">
            <div className="live-divider">
              <span className={`vs-label live-label ${periodInfo.intermission ? 'intermission-label' : ''}`}>
                {periodInfo.label || 'LIVE'}
              </span>
              {periodInfo.intermission ? (
                <span className="live-intermission">Intermission</span>
              ) : (
                <LiveClock serverClock={periodInfo.clock} running={game.clock_running !== false} />
              )}
              {(game.home_shots != null || game.away_shots != null) && (
                <span className="live-shots">SOG: {game.away_shots ?? 0}-{game.home_shots ?? 0}</span>
              )}
            </div>
          </div>

          <div className="game-team">
            <TeamLogo team={game.home_team} size={36} />
            <div className="team-abbrev">{homeAbbr}</div>
            <div className="team-name">{homeName}</div>
            {homeScore !== null && (
              <div className={`team-score ${homeScore > awayScore ? 'score-winning' : ''}`}>
                {homeScore}
              </div>
            )}
          </div>
        </div>

        <div className="game-card-arrow">
          <ChevronRight size={18} />
        </div>
      </div>
    );
  }

  // Final/completed game card — similar to live but with "Final" badge
  if (statusInfo.isFinal) {
    const otLabel = game.went_to_overtime ? (game.period_type === 'SO' ? 'SO' : 'OT') : null;
    return (
      <div
        className={`game-card ${statusInfo.className}`}
        onClick={handleClick}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => e.key === 'Enter' && handleClick()}
      >
        {medalStyle && (
          <div className={`dc-rank ${medalStyle.className}`}>{medalStyle.label}</div>
        )}
        <div className="game-status-badge status-final-badge">
          Final{otLabel ? ` (${otLabel})` : ''}
        </div>

        <div className="game-card-body has-badge">
          <div className="game-team">
            <TeamLogo team={game.away_team} size={36} />
            <div className="team-abbrev">{awayAbbr}</div>
            <div className="team-name">{awayName}</div>
            {awayScore !== null && (
              <div className={`team-score ${awayScore > homeScore ? 'score-winning' : ''}`}>
                {awayScore}
              </div>
            )}
          </div>

          <div className="game-divider">
            <span className="vs-label">Final</span>
          </div>

          <div className="game-team">
            <TeamLogo team={game.home_team} size={36} />
            <div className="team-abbrev">{homeAbbr}</div>
            <div className="team-name">{homeName}</div>
            {homeScore !== null && (
              <div className={`team-score ${homeScore > awayScore ? 'score-winning' : ''}`}>
                {homeScore}
              </div>
            )}
          </div>
        </div>

        <div className="game-card-arrow">
          <ChevronRight size={18} />
        </div>
      </div>
    );
  }

  // New dashboard card design for scheduled/prematch games
  const badge = getConfidenceBadge(confidence);
  const gameDate = formatGameDate(game);
  const gameTime = formatGameTime(game);

  const awayML = odds?.away_moneyline;
  const homeML = odds?.home_moneyline;
  const spreadLine = odds?.home_spread_line;
  const awaySpreadLine = odds?.away_spread_line;
  const ouLine = odds?.over_under_line;

  // Find the single best pick to display
  const topPicks = game.top_picks || [];
  let bestPick = topPick;
  if (topPicks.length > 0) {
    const positivePicks = topPicks.filter((p) => p.edge == null || p.edge >= 0);
    bestPick = positivePicks.length > 0
      ? positivePicks.reduce((best, p) => {
          const score = (s) => (s?.composite_score ?? 0) || ((s?.edge ?? 0) * 100 + (s?.confidence ?? 0));
          return score(p) > score(best) ? p : best;
        }, positivePicks[0])
      : topPicks[0];
  }

  const pickValue = (bestPick?.prediction_value || '').toLowerCase();
  const pickBetType = (bestPick?.bet_type || '').toLowerCase();
  const pickIsHome = pickValue === 'home' || pickValue.includes(homeAbbr.toLowerCase());
  const pickIsAway = pickValue === 'away' || pickValue.includes(awayAbbr.toLowerCase());

  // Format the best pick label
  const formatPickLabel = () => {
    if (!bestPick || !pickBetType) return null;
    const team = pickIsHome ? homeAbbr : pickIsAway ? awayAbbr : '';
    if (pickBetType === 'ml') return `${team} ML`;
    if (pickBetType === 'spread') {
      const line = pickIsHome ? spreadLine : awaySpreadLine;
      return `${team} ${line != null ? (line > 0 ? '+' : '') + line : 'Spread'}`;
    }
    if (pickBetType === 'total') {
      const isOver = pickValue.includes('over');
      return `${isOver ? 'Over' : 'Under'} ${ouLine || ''}`;
    }
    return formatBetType(pickBetType, sportKey);
  };

  const bestPickLabel = formatPickLabel();
  const bestPickEdge = bestPick?.edge != null ? bestPick.edge * 100 : null;
  const bestPickOdds = bestPick?.odds_display;
  const bestPickConf = bestPick?.bet_confidence != null
    ? confidencePct(bestPick.bet_confidence)
    : (bestPick?.confidence != null ? confidencePct(bestPick.confidence) : null);

  return (
    <div className="dc-card" onClick={handleClick} role="button" tabIndex={0} onKeyDown={(e) => e.key === 'Enter' && handleClick()}>
      {/* Rank badge */}
      {medalStyle && (
        <div className={`dc-rank ${medalStyle.className}`}>{medalStyle.label}</div>
      )}

      {/* Top tags row */}
      <div className="dc-tags">
        <span className="dc-tag dc-tag-sport">{sportLabel.sport}</span>
        <span className="dc-tag dc-tag-league">{sportLabel.league}</span>
        {badge && (
          <span className={`dc-tag dc-tag-confidence ${badge.className}`}>
            <badge.icon size={12} />
            {badge.label} {confidence != null ? `${Math.round(confidence)}%` : ''}
          </span>
        )}
      </div>

      {/* Date/Time row */}
      <div className="dc-datetime">
        {gameDate && (
          <>
            <Calendar size={12} />
            <span>{gameDate}</span>
          </>
        )}
        {gameTime && (
          <>
            <Clock size={12} />
            <span>{gameTime}</span>
          </>
        )}
      </div>

      {/* Team matchup — abbreviations primary, logos smaller */}
      <div className="dc-matchup">
        <div className="dc-team">
          <TeamLogo team={game.away_team} size={40} />
          <div className="dc-team-abbr">{awayAbbr}</div>
          {awayML != null && (
            <div className="dc-ml-sm">{formatAmericanOdds(awayML)}</div>
          )}
        </div>

        <div className="dc-vs-section">
          <span className="dc-vs">VS</span>
        </div>

        <div className="dc-team">
          <TeamLogo team={game.home_team} size={40} />
          <div className="dc-team-abbr">{homeAbbr}</div>
          {homeML != null && (
            <div className="dc-ml-sm">{formatAmericanOdds(homeML)}</div>
          )}
        </div>
      </div>

      {/* Single best pick highlight */}
      {bestPickLabel && (
        <div className="dc-best-pick">
          <Target size={13} />
          <span className="dc-best-pick-label">{bestPickLabel}</span>
          {bestPickOdds != null && (
            <span className="dc-best-pick-odds">{formatAmericanOdds(bestPickOdds)}</span>
          )}
          {bestPickEdge != null && (
            <span className={`dc-best-pick-edge${bestPickEdge < 0 ? ' dc-edge-neg' : ''}`}>
              {bestPickEdge >= 0 ? '+' : ''}{bestPickEdge.toFixed(1)}%
            </span>
          )}
          {bestPickConf != null && (
            <span className="dc-best-pick-conf">{Math.round(bestPickConf)}%</span>
          )}
          {bestPick?.prediction_id && (
            <button
              className={`dc-track-btn ${tracked ? 'dc-track-btn-tracked' : ''}`}
              onClick={(e) => {
                e.stopPropagation();
                if (tracked || tracking) return;
                setTracking(true);
                trackBet(bestPick.prediction_id)
                  .then(() => { setTracked(true); })
                  .catch((err) => {
                    if (err?.response?.status === 409) setTracked(true);
                    else console.error('Track failed:', err);
                  })
                  .finally(() => setTracking(false));
              }}
              disabled={tracked || tracking}
              title={tracked ? 'Already tracked' : 'Track this bet'}
            >
              {tracked ? <Check size={12} /> : <Plus size={12} />}
              {tracked ? 'Tracked' : 'Track'}
            </button>
          )}
        </div>
      )}

      {/* Footer - just Details link */}
      <div className="dc-footer">
        <span className="dc-details-link">
          Details <ChevronRight size={14} />
        </span>
      </div>
    </div>
  );
}

export default GameCard;
