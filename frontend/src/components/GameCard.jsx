import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import { Clock, ChevronRight, TrendingUp, Zap, Info } from 'lucide-react';
import { format } from 'date-fns';
import { teamName, teamAbbrev, teamLogo, confidencePct, parseAsUTC, formatBetType, formatPredictionValue } from '../utils/teams';
import { getConfidenceColor, formatAmericanOdds } from '../utils/formatting';

function getStatusDisplay(game) {
  const status = game.status || game.game_state || '';
  const statusLower = status.toLowerCase();

  if (statusLower === 'final' || statusLower === 'completed' || statusLower === 'off') {
    return { label: 'Final', className: 'status-final', showScore: true, isLive: false };
  }
  if (statusLower === 'live' || statusLower === 'in_progress' || statusLower === 'active') {
    return { label: 'LIVE', className: 'status-live', showScore: true, isLive: true };
  }
  return { label: null, className: 'status-scheduled', showScore: false, isLive: false };
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

function Countdown({ startTime }) {
  const [timeLeft, setTimeLeft] = useState('');

  useEffect(() => {
    function update() {
      const now = new Date();
      const start = parseAsUTC(startTime);
      if (!start || isNaN(start.getTime())) { setTimeLeft('TBD'); return; }
      const diff = start.getTime() - now.getTime();
      if (diff <= 0) { setTimeLeft('Starting'); return; }
      const hours = Math.floor(diff / (1000 * 60 * 60));
      const mins = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
      if (hours > 24) setTimeLeft(`${Math.floor(hours / 24)}d ${hours % 24}h`);
      else if (hours > 0) setTimeLeft(`${hours}h ${mins}m`);
      else {
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

function getConfidenceTier(confidence) {
  if (confidence >= 75) return { label: 'HIGH CONFIDENCE', className: 'tier-high' };
  if (confidence >= 60) return { label: 'MEDIUM CONFIDENCE', className: 'tier-medium' };
  if (confidence >= 45) return { label: 'LOW CONFIDENCE', className: 'tier-low' };
  return { label: 'SPECULATIVE', className: 'tier-spec' };
}

/**
 * Parse reasoning into individual bullet points.
 * The backend returns clean, semicolon-separated signal text.
 * Strips any leftover "(Odds: ...)" fragments from legacy data.
 * Extracts {{tooltip:...}} markers into a separate tooltip field.
 */
function parseReasons(reasoning) {
  if (!reasoning) return [];

  // Strip any "(Odds: ...)" fragments from legacy reasoning
  let cleaned = reasoning.replace(/\s*\(Odds:\s*[^)]*\)/g, '').trim();
  if (!cleaned) return [];

  // Extract all {{team:...}} markers (team abbreviations for logos)
  // and {{tooltip:...}} markers BEFORE splitting, since tooltips
  // may contain periods/semicolons that would break the line splitter.
  const teamMarkers = [];
  const tooltips = [];
  const TM_PH = '\x00TM';
  const TT_PH = '\x00TT';
  cleaned = cleaned.replace(/\{\{team:([^}]+)\}\}\s*/g, (_match, abbr) => {
    teamMarkers.push(abbr.trim().toUpperCase());
    return TM_PH + (teamMarkers.length - 1) + ' ';
  });
  cleaned = cleaned.replace(/\s*\{\{tooltip:([\s\S]*?)\}\}/g, (_match, tip) => {
    tooltips.push(tip.trim());
    return TT_PH + (tooltips.length - 1);
  });

  // Try splitting by numbered items (1. xxx 2. xxx) or newlines or semicolons
  let lines = cleaned
    .split(/(?:\d+\.\s+|\n|;\s*)/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  if (lines.length <= 1) {
    // Fall back: split by periods for multi-sentence reasoning
    lines = cleaned
      .split(/\.\s+/)
      .map((s) => s.trim().replace(/\.$/, ''))
      .filter((s) => s.length > 5);
  }

  // Re-attach team markers and tooltips from placeholders
  const tmRe = new RegExp(TM_PH + '(\\d+)\\s*', 'g');
  const ttRe = new RegExp(TT_PH + '(\\d+)', 'g');
  return lines.slice(0, 7).map((line) => {
    let tooltip = null;
    let team = null;
    let text = line.replace(tmRe, (_m, idx) => {
      team = teamMarkers[parseInt(idx, 10)] || null;
      return '';
    });
    text = text.replace(ttRe, (_m, idx) => {
      tooltip = tooltips[parseInt(idx, 10)] || null;
      return '';
    }).trim();
    return { text, tooltip, team };
  });
}

const MEDAL_CONFIG = {
  gold:   { icon: '🥇', label: '#1 Pick' },
  silver: { icon: '🥈', label: '#2 Pick' },
  bronze: { icon: '🥉', label: '#3 Pick' },
};

function GameCard({ game, section, medal }) {
  const navigate = useNavigate();
  const gameId = game.game_id || game.id;
  const statusInfo = getStatusDisplay(game);

  const awayName = teamName(game.away_team, 'Away');
  const homeName = teamName(game.home_team, 'Home');
  const awayAbbr = teamAbbrev(game.away_team, 'AWY');
  const homeAbbr = teamAbbrev(game.home_team, 'HME');
  const awayScore = game.away_score ?? game.score?.away ?? null;
  const homeScore = game.home_score ?? game.score?.home ?? null;
  const topPick = game.top_pick || null;
  const rawConf = topPick?.confidence || game.top_confidence || game.confidence || game.prediction_confidence || null;
  const confidence = rawConf != null ? confidencePct(rawConf) : null;
  const startTime = game.start_time || game.datetime;
  const odds = game.odds || null;

  const periodInfo = statusInfo.isLive ? formatPeriod(game) : { label: null, clock: null };

  const handleClick = () => {
    if (gameId) navigate(`/games/${gameId}`);
  };

  // For live games, keep the original compact card style
  if (statusInfo.isLive) {
    return (
      <div
        className={`game-card ${statusInfo.className}`}
        onClick={handleClick}
        role="button"
        tabIndex={0}
        onKeyDown={(e) => e.key === 'Enter' && handleClick()}
      >
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

  // Buddy's Analysis style card for scheduled games
  const tier = confidence != null ? getConfidenceTier(confidence) : null;
  const confColor = confidence != null ? getConfidenceColor(confidence) : null;
  const betType = topPick ? formatBetType(topPick.bet_type) : null;
  const pickValue = topPick
    ? formatPredictionValue(topPick.prediction_value, homeAbbr, awayAbbr, topPick.bet_type)
    : null;
  const oddsDisplay = topPick?.odds_display != null
    ? formatAmericanOdds(topPick.odds_display)
    : (odds?.home_moneyline != null || odds?.away_moneyline != null)
      ? null // we'll show it differently
      : null;

  const reasoning = topPick?.reasoning || topPick?.reason || topPick?.analysis || '';
  const reasons = parseReasons(reasoning);

  // Map team abbreviation → team object for inline logos in analysis
  const teamByAbbr = {};
  if (game.home_team) teamByAbbr[teamAbbrev(game.home_team).toUpperCase()] = game.home_team;
  if (game.away_team) teamByAbbr[teamAbbrev(game.away_team).toUpperCase()] = game.away_team;

  // Determine which team is the pick
  const pickTeamName = topPick?.pick_team || null;

  return (
    <div className={`pick-card ${tier ? tier.className : ''}`}>
      {/* Confidence tier banner */}
      {tier && (
        <div className="pick-card-tier" style={{ borderColor: confColor }}>
          <Zap size={14} />
          <span>{tier.label}</span>
        </div>
      )}

      {/* Main card content */}
      <div className="pick-card-body" onClick={handleClick} role="button" tabIndex={0}>
        {/* Team matchup header */}
        <div className="pick-card-matchup">
          <div className="pick-card-teams">
            <TeamLogo team={game.away_team} size={40} />
            <TeamLogo team={game.home_team} size={40} />
            <div className="pick-card-team-names">
              <span className="pick-matchup-text">{awayName} @ {homeName}</span>
              <span className="pick-game-time">
                <Clock size={12} />
                {formatGameTime(game)}
                {startTime && (
                  <>
                    {' '}&middot;{' '}
                    <Countdown startTime={startTime} />
                  </>
                )}
              </span>
            </div>
          </div>
          {confidence != null && (
            <div className="pick-card-confidence" style={{ color: confColor }}>
              {confidence.toFixed(0)}%
            </div>
          )}
        </div>

        {/* Pick details */}
        {topPick && (
          <div className="pick-card-selection">
            <div className="pick-card-badges">
              {betType && <span className="pick-badge pick-badge-type">{betType}</span>}
              {medal && MEDAL_CONFIG[medal] && (
                <span className={`pick-badge-medal medal-${medal}`}>
                  <span className="medal-icon">{MEDAL_CONFIG[medal].icon}</span>
                  {MEDAL_CONFIG[medal].label}
                </span>
              )}
            </div>
            <div className="pick-card-value">{pickValue}</div>
            {oddsDisplay && <div className="pick-card-odds">{oddsDisplay}</div>}
          </div>
        )}

        {/* Analysis reasons — always visible */}
        {reasons.length > 0 && (
          <div className="pick-card-analysis">
            <div className="pick-analysis-header">
              <TrendingUp size={14} />
              <span>Analysis ({reasons.length})</span>
            </div>
            <ol className="pick-analysis-list">
              {reasons.map((reason, i) => {
                const reasonTeam = reason.team ? teamByAbbr[reason.team] : null;
                const reasonLogo = reasonTeam ? teamLogo(reasonTeam) : null;
                return (
                  <li key={i}>
                    {reasonLogo && (
                      <img
                        className="pick-analysis-team-logo"
                        src={reasonLogo}
                        alt={reason.team}
                        width={18}
                        height={18}
                        loading="lazy"
                        onError={(e) => { e.target.style.display = 'none'; }}
                      />
                    )}
                    {reason.text}
                    {reason.tooltip && (
                      <span className="pick-analysis-tooltip" title={reason.tooltip}>
                        <Info size={13} />
                      </span>
                    )}
                  </li>
                );
              })}
            </ol>
          </div>
        )}

        {/* No pick fallback */}
        {!topPick && (
          <div className="pick-card-no-pick">
            <p>No analysis available yet</p>
          </div>
        )}
      </div>

      {/* Odds bar at bottom */}
      {odds && (
        <div className="pick-card-odds-bar">
          {odds.home_moneyline != null && (
            <div className="pick-odds-item">
              <span className="pick-odds-label">ML</span>
              <span className="pick-odds-values">
                {awayAbbr} {formatAmericanOdds(odds.away_moneyline) || '—'}
                <span className="pick-odds-sep">/</span>
                {homeAbbr} {formatAmericanOdds(odds.home_moneyline) || '—'}
              </span>
            </div>
          )}
          {odds.over_under_line != null && (
            <div className="pick-odds-item">
              <span className="pick-odds-label">O/U</span>
              <span className="pick-odds-values">{odds.over_under_line}</span>
            </div>
          )}
          {odds.home_spread_line != null && (
            <div className="pick-odds-item">
              <span className="pick-odds-label">PL</span>
              <span className="pick-odds-values">
                {odds.home_spread_line > 0 ? '+' : ''}{odds.home_spread_line}
              </span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default GameCard;
