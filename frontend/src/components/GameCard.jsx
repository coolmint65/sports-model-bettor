import { useState, useEffect, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import {
  Clock,
  ChevronRight,
  TrendingUp,
  Calendar,
  Minus,
  Target,
} from 'lucide-react';
import {
  teamName,
  teamAbbrev,
  confidencePct,
  formatBetType,
  formatPredictionValue,
} from '../utils/teams';
import { formatAmericanOdds, formatGameDate, formatGameTime } from '../utils/formatting';
import TeamLogo from './shared/TeamLogo';

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


function getConfidenceBadge(confidence) {
  if (confidence == null) return null;
  if (confidence >= 80) return { label: 'STRONG BET', className: 'badge-good', icon: TrendingUp };
  if (confidence >= 70) return { label: 'GOOD BET', className: 'badge-good', icon: TrendingUp };
  if (confidence >= 60) return { label: 'LEAN', className: 'badge-borderline', icon: Minus };
  if (confidence >= 50) return { label: 'SPECULATIVE', className: 'badge-skip', icon: Minus };
  return { label: 'NO EDGE', className: 'badge-skip', icon: Minus };
}

function getPickTier(conf, isBest, hasEdge, hasGoodJuice) {
  const unfaded = conf != null && conf >= 60 && hasGoodJuice;
  if (isBest) return { tier: 'dc-pick-chip-best', tierLabel: 'BEST' };
  if (conf != null && conf >= 75 && (hasEdge || unfaded)) return { tier: 'dc-pick-chip-good', tierLabel: 'STRONG' };
  if (conf != null && conf >= 60 && (hasEdge || unfaded)) return { tier: 'dc-pick-chip-borderline', tierLabel: 'LEAN' };
  return { tier: 'dc-pick-chip-skip', tierLabel: 'SKIP' };
}

function PickChips({ mlPick, spreadPick, totalPick, formatMarketPick }) {
  const picks = [mlPick, spreadPick, totalPick].filter(Boolean);
  const positivePicks = picks.filter((p) => p.edge == null || p.edge >= 0);
  const bestPick = positivePicks.length > 0
    ? positivePicks.reduce((best, p) => {
        const score = (s) => (s?.composite_score ?? 0) || ((s?.edge ?? 0) * 100 + (s?.confidence ?? 0));
        return score(p) > score(best) ? p : best;
      }, positivePicks[0])
    : null;

  const annotated = picks.map((pick) => {
    const betConf = pick.bet_confidence != null
      ? confidencePct(pick.bet_confidence)
      : (pick.confidence != null ? confidencePct(pick.confidence) : null);
    const isBest = bestPick && pick === bestPick && positivePicks.length > 1;
    const hasEdge = pick.edge == null || pick.edge >= 0;
    return { pick, conf: betConf, isBest, hasEdge };
  });

  annotated.sort((a, b) => {
    if (a.isBest !== b.isBest) return a.isBest ? -1 : 1;
    if (a.hasEdge !== b.hasEdge) return a.hasEdge ? -1 : 1;
    return (b.conf ?? 0) - (a.conf ?? 0);
  });

  return annotated.map(({ pick, conf, isBest, hasEdge }) => {
    const label = formatMarketPick(pick);
    const edgeVal = pick.edge != null ? pick.edge * 100 : null;
    const edgePct = edgeVal != null ? `${edgeVal >= 0 ? '+' : ''}${edgeVal.toFixed(1)}%` : null;
    const hasGoodJuice = pick.odds_display == null || pick.odds_display > 0 || pick.odds_display >= -170;
    const { tier, tierLabel } = getPickTier(conf, isBest, hasEdge, hasGoodJuice);

    return (
      <div key={pick.bet_type} className={`dc-pick-chip ${tier}`}>
        <Target size={11} />
        <span className="dc-pick-chip-label">{label}</span>
        {pick.odds_display != null && (
          <span className="dc-pick-chip-odds">{formatAmericanOdds(pick.odds_display)}</span>
        )}
        {edgePct != null && (
          <span className={`dc-pick-chip-edge${edgeVal < 0 ? ' dc-pick-chip-edge-neg' : ''}`}>Edge {edgePct}</span>
        )}
        {conf != null && (
          <span className="dc-pick-chip-conf">{Math.round(conf)}%</span>
        )}
        <span className="dc-pick-chip-badge">{tierLabel}</span>
      </div>
    );
  });
}

const MEDAL_STYLES = {
  gold: { className: 'rank-gold', label: '#1' },
  silver: { className: 'rank-silver', label: '#2' },
  bronze: { className: 'rank-bronze', label: '#3' },
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
  // Prefer bet_confidence (signal-based) over confidence (win probability)
  const rawConf = topPick?.bet_confidence || topPick?.confidence || game.top_confidence || game.confidence || game.prediction_confidence || null;
  const confidence = rawConf != null ? confidencePct(rawConf) : null;
  const odds = game.odds || null;
  const periodInfo = statusInfo.isLive ? formatPeriod(game) : { label: null, clock: null };

  const handleClick = () => {
    if (gameId) navigate(`/games/${gameId}`);
  };

  // For live games, keep compact card style
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

  // New dashboard card design for scheduled/prematch games
  const badge = getConfidenceBadge(confidence);
  const medalStyle = medal ? MEDAL_STYLES[medal] : null;
  const gameDate = formatGameDate(game);
  const gameTime = formatGameTime(game);

  const awayML = odds?.away_moneyline;
  const homeML = odds?.home_moneyline;
  const spreadLine = odds?.home_spread_line;
  const awaySpreadLine = odds?.away_spread_line;
  const ouLine = odds?.over_under_line;

  // Per-market picks (best ML, best Spread, best O/U)
  const topPicks = game.top_picks || [];
  const pickByMarket = {};
  for (const p of topPicks) {
    pickByMarket[p.bet_type] = p;
  }
  const mlPick = pickByMarket['ml'] || null;
  const spreadPick = pickByMarket['spread'] || null;
  const totalPick = pickByMarket['total'] || null;

  // Determine which side the single top_pick is on (for odds pill highlighting)
  const pickValue = (topPick?.prediction_value || '').toLowerCase();
  const pickBetType = (topPick?.bet_type || '').toLowerCase();
  const pickIsHome = pickValue === 'home' || pickValue.includes(homeAbbr.toLowerCase());
  const pickIsAway = pickValue === 'away' || pickValue.includes(awayAbbr.toLowerCase());

  // Detect over/under pick
  const pickIsOver = pickBetType === 'total' && pickValue.includes('over');
  const pickIsUnder = pickBetType === 'total' && pickValue.includes('under');

  // Helper to format a per-market pick label
  const formatMarketPick = (pick) => {
    if (!pick) return null;
    const val = (pick.prediction_value || '').toLowerCase();
    const bt = (pick.bet_type || '').toLowerCase();
    const isHome = val === 'home' || val.includes(homeAbbr.toLowerCase());
    const isAway = val === 'away' || val.includes(awayAbbr.toLowerCase());
    const team = isHome ? homeAbbr : isAway ? awayAbbr : '';

    if (bt === 'ml') return `${team} ML`;
    if (bt === 'spread') {
      const line = isHome ? spreadLine : awaySpreadLine;
      return `${team} ${line != null ? (line > 0 ? '+' : '') + line : 'PL'}`;
    }
    if (bt === 'total') {
      const isOver = val.includes('over');
      return `${isOver ? 'Over' : 'Under'} ${ouLine || ''}`;
    }
    return formatBetType(bt);
  };

  return (
    <div className="dc-card" onClick={handleClick} role="button" tabIndex={0} onKeyDown={(e) => e.key === 'Enter' && handleClick()}>
      {/* Rank badge */}
      {medalStyle && (
        <div className={`dc-rank ${medalStyle.className}`}>{medalStyle.label}</div>
      )}

      {/* Top tags row */}
      <div className="dc-tags">
        <span className="dc-tag dc-tag-sport">Hockey</span>
        <span className="dc-tag dc-tag-league">NHL</span>
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

      {/* Team matchup */}
      <div className="dc-matchup">
        <div className="dc-team">
          <TeamLogo team={game.away_team} size={48} />
          <div className="dc-team-name">{awayName}</div>
          {awayML != null && (
            <div className={`dc-ml ${pickIsAway && pickBetType === 'ml' ? 'dc-ml-pick' : ''}`}>
              {formatAmericanOdds(awayML)}
            </div>
          )}
        </div>

        <div className="dc-vs-section">
          <span className="dc-vs">VS</span>
          <span className="dc-status">Scheduled</span>
        </div>

        <div className="dc-team">
          <TeamLogo team={game.home_team} size={48} />
          <div className="dc-team-name">{homeName}</div>
          {homeML != null && (
            <div className={`dc-ml ${pickIsHome && pickBetType === 'ml' ? 'dc-ml-pick' : ''}`}>
              {formatAmericanOdds(homeML)}
            </div>
          )}
        </div>
      </div>

      {/* Per-market pick bars (best ML, best Spread, best O/U) */}
      {topPicks.length > 0 ? (
        <div className="dc-picks-multi">
          <PickChips
            mlPick={mlPick}
            spreadPick={spreadPick}
            totalPick={totalPick}
            formatMarketPick={formatMarketPick}
          />
        </div>
      ) : topPick && pickBetType ? (
        <div className="dc-pick-bar">
          <Target size={13} />
          <span className="dc-pick-bar-text">
            <strong>
              {pickIsHome ? homeAbbr : pickIsAway ? awayAbbr : ''}{' '}
              {pickBetType === 'ml' ? 'ML' : pickBetType === 'spread' ? 'Spread' : pickBetType === 'total' ? (pickIsOver ? 'Over' : 'Under') + ' ' + (ouLine || '') : formatBetType(pickBetType) + (topPick?.prediction_value ? ' ' + formatPredictionValue(topPick.prediction_value, homeAbbr, awayAbbr, pickBetType) : '')}
            </strong>
          </span>
          {confidence != null && (
            <span className="dc-pick-bar-conf">{Math.round(confidence)}%</span>
          )}
        </div>
      ) : null}

      {/* Odds summary pills - flat horizontal row */}
      {odds && (
        <div className="dc-odds-row">
          {awayML != null && homeML != null && (
            <div className={`dc-odds-pill ${pickBetType === 'ml' ? 'dc-odds-pill-active' : ''}`}>
              <span className="dc-odds-label">ML</span>
              <span className="dc-odds-val">
                <span className={pickIsAway && pickBetType === 'ml' ? 'dc-pick-highlight' : ''}>{formatAmericanOdds(awayML)}</span>
                <span className="dc-odds-sep">/</span>
                <span className={pickIsHome && pickBetType === 'ml' ? 'dc-pick-highlight' : ''}>{formatAmericanOdds(homeML)}</span>
              </span>
            </div>
          )}
          <div className={`dc-odds-pill ${pickBetType === 'spread' ? 'dc-odds-pill-active' : ''}`}>
            <span className="dc-odds-label">PL</span>
            <span className="dc-odds-val">
              {spreadLine != null ? (
                <>
                  <span className={pickIsAway && pickBetType === 'spread' ? 'dc-pick-highlight' : ''}>{awaySpreadLine != null ? (awaySpreadLine > 0 ? '+' : '') + awaySpreadLine : `-${Math.abs(spreadLine)}`}</span>
                  <span className="dc-odds-sep">/</span>
                  <span className={pickIsHome && pickBetType === 'spread' ? 'dc-pick-highlight' : ''}>{spreadLine > 0 ? '+' : ''}{spreadLine}</span>
                </>
              ) : (
                <>
                  <span className={pickIsAway && pickBetType === 'spread' ? 'dc-pick-highlight' : ''}>+1.5</span>
                  <span className="dc-odds-sep">/</span>
                  <span className={pickIsHome && pickBetType === 'spread' ? 'dc-pick-highlight' : ''}>-1.5</span>
                </>
              )}
            </span>
          </div>
          {ouLine != null && (
            <div className={`dc-odds-pill ${pickBetType === 'total' ? 'dc-odds-pill-active' : ''}`}>
              <span className="dc-odds-label">
                <span className={pickIsOver ? 'dc-pick-highlight' : ''}>O</span>
                /
                <span className={pickIsUnder ? 'dc-pick-highlight' : ''}>U</span>
              </span>
              <span className="dc-odds-val">{ouLine}</span>
            </div>
          )}
        </div>
      )}

      {/* Starting Goalies — only show when at least one name is available */}
      {(game.home_starter?.name || game.away_starter?.name) && (
        <div className="dc-goalies">
          <div className="dc-goalie-row">
            <span className="dc-goalie-name">
              {game.away_starter?.name || '—'}
            </span>
            <span className="dc-goalie-label">Goalies</span>
            <span className="dc-goalie-name">
              {game.home_starter?.name || '—'}
            </span>
          </div>
          <div className="dc-goalie-row dc-goalie-status-row">
            <span className={`dc-goalie-status ${game.away_starter?.confirmed ? 'dc-confirmed' : game.away_starter?.status?.toLowerCase() === 'unconfirmed' ? 'dc-unconfirmed' : game.away_starter ? 'dc-likely' : ''}`}>
              {game.away_starter?.status || (game.away_starter?.confirmed ? 'Confirmed' : game.away_starter ? 'Expected' : '')}
            </span>
            <span />
            <span className={`dc-goalie-status ${game.home_starter?.confirmed ? 'dc-confirmed' : game.home_starter?.status?.toLowerCase() === 'unconfirmed' ? 'dc-unconfirmed' : game.home_starter ? 'dc-likely' : ''}`}>
              {game.home_starter?.status || (game.home_starter?.confirmed ? 'Confirmed' : game.home_starter ? 'Expected' : '')}
            </span>
          </div>
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
