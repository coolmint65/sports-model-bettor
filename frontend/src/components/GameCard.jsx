import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Clock, ChevronRight, TrendingUp, Target, Radio,
  CheckCircle, XCircle, MinusCircle, Calendar, AlertTriangle,
  Users, Zap, Activity,
} from 'lucide-react';
import { format } from 'date-fns';
import {
  teamName, teamAbbrev, teamLogo, confidencePct,
  parseAsUTC, isLiveStatus,
} from '../utils/teams';
import { formatAmericanOdds } from '../utils/formatting';

/* ── Utility functions ── */

function getStatusDisplay(game) {
  const s = (game.status || game.game_state || '').toLowerCase();
  if (s === 'final' || s === 'completed' || s === 'off')
    return { label: 'Final', className: 'status-final', showScore: true, isLive: false };
  if (s === 'live' || s === 'in_progress' || s === 'active')
    return { label: 'LIVE', className: 'status-live', showScore: true, isLive: true };
  return { label: null, className: 'status-scheduled', showScore: false, isLive: false };
}

function formatPeriod(game) {
  const { period, period_type, clock, in_intermission } = game;
  if (!period) return { label: null, clock: null, intermission: false };
  let p;
  if (period_type === 'OT') p = 'OT';
  else if (period_type === 'SO') p = 'SO';
  else if (period === 1) p = '1st';
  else if (period === 2) p = '2nd';
  else if (period === 3) p = '3rd';
  else p = `${period}th`;
  if (in_intermission) return { label: `End ${p}`, clock: null, intermission: true };
  return { label: p, clock: clock || null, intermission: false };
}

function parseClock(str) {
  if (!str) return 0;
  const parts = str.split(':');
  if (parts.length !== 2) return 0;
  return (parseInt(parts[0], 10) || 0) * 60 + (parseInt(parts[1], 10) || 0);
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
    const id = setInterval(() => setSeconds((s) => Math.max(0, s - 1)), 1000);
    return () => clearInterval(id);
  }, [running, seconds > 0]); // eslint-disable-line react-hooks/exhaustive-deps
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return <span className="gc-live-clock">{String(mins).padStart(2, '0')}:{String(secs).padStart(2, '0')}</span>;
}

function TeamLogo({ team, size = 52 }) {
  const logo = teamLogo(team);
  if (!logo) return null;
  return (
    <img
      className="gc-team-logo"
      src={logo} alt="" width={size} height={size}
      loading="lazy"
      onError={(e) => { e.target.style.display = 'none'; }}
    />
  );
}

function formatGameDateTime(game) {
  try {
    const dateStr = game.start_time || game.datetime;
    if (!dateStr) return { date: 'TBD', time: '' };
    const dt = parseAsUTC(dateStr);
    if (!dt || isNaN(dt.getTime())) return { date: 'TBD', time: '' };
    return { date: format(dt, 'EEE, MMM d'), time: format(dt, 'h:mm a') };
  } catch {
    return { date: 'TBD', time: '' };
  }
}

/* ── Main GameCard ── */

function GameCard({ game, rank }) {
  const navigate = useNavigate();
  const gameId = game.game_id || game.id;
  const statusInfo = getStatusDisplay(game);

  const awayName = teamName(game.away_team, 'Away');
  const homeName = teamName(game.home_team, 'Home');
  const awayAbbr = teamAbbrev(game.away_team, 'AWY');
  const homeAbbr = teamAbbrev(game.home_team, 'HME');
  const awayScore = game.away_score ?? null;
  const homeScore = game.home_score ?? null;
  const topPick = game.top_pick || null;
  const rawConf = topPick?.confidence || null;
  const confidence = rawConf != null ? confidencePct(rawConf) : null;
  const odds = game.odds || null;
  const startTime = game.start_time || game.datetime;
  const periodInfo = statusInfo.isLive ? formatPeriod(game) : { label: null, clock: null, intermission: false };
  const { date: dateDisplay, time: timeDisplay } = formatGameDateTime(game);

  const isFallback = topPick?.is_fallback;
  const hasQualifiedPick = topPick && !isFallback;
  const betQuality = hasQualifiedPick ? 'good' : (topPick ? 'borderline' : null);

  const handleClick = () => { if (gameId) navigate(`/games/${gameId}`); };

  return (
    <div
      className={`gc-card ${statusInfo.className}`}
      onClick={handleClick} role="button" tabIndex={0}
      onKeyDown={(e) => e.key === 'Enter' && handleClick()}
    >
      {/* Ranking badge */}
      {rank != null && rank <= 3 && (
        <div className={`gc-rank gc-rank-${rank}`}>#{rank}</div>
      )}

      {/* Outcome overlay for final games */}
      {topPick?.outcome && (
        <div className={`gc-outcome gc-outcome-${topPick.outcome}`}>
          {topPick.outcome === 'win' && <CheckCircle size={22} />}
          {topPick.outcome === 'loss' && <XCircle size={22} />}
          {topPick.outcome === 'push' && <MinusCircle size={22} />}
        </div>
      )}

      {/* Badges */}
      <div className="gc-badges">
        <span className="gc-badge gc-badge-sport">Hockey</span>
        <span className="gc-badge gc-badge-league"><Users size={11} /> NHL</span>
        {betQuality === 'good' && (
          <span className="gc-badge gc-badge-good"><TrendingUp size={11} /> GOOD BET</span>
        )}
        {betQuality === 'borderline' && (
          <span className="gc-badge gc-badge-borderline"><AlertTriangle size={11} /> BORDERLINE</span>
        )}
      </div>

      {/* Date / Time / Live */}
      <div className="gc-datetime">
        {statusInfo.isLive ? (
          <>
            <Radio size={12} className="gc-pulse" />
            <span className="gc-live-label">LIVE</span>
            {periodInfo.label && <span className="gc-period-badge">{periodInfo.label}</span>}
            {periodInfo.clock && !periodInfo.intermission && (
              <LiveClock serverClock={periodInfo.clock} running={game.clock_running !== false} />
            )}
            {periodInfo.intermission && <span className="gc-intermission-label">Intermission</span>}
          </>
        ) : statusInfo.label === 'Final' ? (
          <span className="gc-final-label">Final{game.went_to_overtime ? ' (OT)' : ''}</span>
        ) : (
          <>
            <Calendar size={12} />
            <span>{dateDisplay}</span>
            <Clock size={12} />
            <span>{timeDisplay}</span>
          </>
        )}
      </div>

      {/* Team Matchup */}
      <div className="gc-matchup">
        <div className="gc-team">
          <TeamLogo team={game.away_team} size={52} />
          <span className="gc-team-name">{awayName}</span>
          {statusInfo.showScore && awayScore != null ? (
            <span className={`gc-score ${awayScore > homeScore ? 'gc-winning' : ''}`}>{awayScore}</span>
          ) : odds?.away_moneyline != null ? (
            <span className={`gc-ml ${odds.away_moneyline > 0 ? 'gc-ml-plus' : 'gc-ml-minus'}`}>
              {formatAmericanOdds(odds.away_moneyline)}
            </span>
          ) : null}
        </div>

        <div className="gc-center">
          <span className="gc-vs">VS</span>
          {statusInfo.isLive ? (
            (game.away_shots != null || game.home_shots != null) && (
              <span className="gc-shots">SOG {game.away_shots ?? 0}-{game.home_shots ?? 0}</span>
            )
          ) : !statusInfo.showScore ? (
            <span className="gc-status-text">Scheduled</span>
          ) : null}
        </div>

        <div className="gc-team">
          <TeamLogo team={game.home_team} size={52} />
          <span className="gc-team-name">{homeName}</span>
          {statusInfo.showScore && homeScore != null ? (
            <span className={`gc-score ${homeScore > awayScore ? 'gc-winning' : ''}`}>{homeScore}</span>
          ) : odds?.home_moneyline != null ? (
            <span className={`gc-ml ${odds.home_moneyline > 0 ? 'gc-ml-plus' : 'gc-ml-minus'}`}>
              {formatAmericanOdds(odds.home_moneyline)}
            </span>
          ) : null}
        </div>
      </div>

      {/* Compact Odds Pills */}
      {odds && (odds.home_moneyline != null || odds.home_spread_line != null || odds.over_under_line != null) && (
        <div className="gc-odds-pills">
          {(odds.home_moneyline != null || odds.away_moneyline != null) && (
            <div className="gc-pill">
              <Target size={11} />
              <span className="gc-pill-label">ML</span>
              <span className="gc-pill-val">
                {formatAmericanOdds(odds.away_moneyline) || '—'} / {formatAmericanOdds(odds.home_moneyline) || '—'}
              </span>
            </div>
          )}
          {odds.home_spread_line != null && (
            <div className="gc-pill">
              <Activity size={11} />
              <span className="gc-pill-label">SPREAD</span>
              <span className="gc-pill-val">
                {odds.away_spread_line > 0 ? '+' : ''}{odds.away_spread_line} / {odds.home_spread_line > 0 ? '+' : ''}{odds.home_spread_line}
              </span>
            </div>
          )}
          {odds.over_under_line != null && (
            <div className="gc-pill">
              <Zap size={11} />
              <span className="gc-pill-label">O/U</span>
              <span className="gc-pill-val">O {odds.over_under_line}</span>
            </div>
          )}
        </div>
      )}

      {/* Value indicator */}
      {hasQualifiedPick && (
        <div className="gc-value">
          <TrendingUp size={14} />
          Good Parlay Value
        </div>
      )}

      {/* Footer */}
      <div className="gc-footer">
        <div className="gc-footer-left">
          {confidence != null && (
            <span className="gc-confidence">
              <Target size={12} /> Confidence: {Math.round(confidence)}
            </span>
          )}
        </div>
        <span className="gc-details">
          Details <ChevronRight size={14} />
        </span>
      </div>
    </div>
  );
}

export default GameCard;
