import { useState, useEffect, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeft,
  Clock,
  MapPin,
  BarChart3,
  Target,
  Users,
  TrendingUp,
  TrendingDown,
  Layers,
  DollarSign,
  Radio,
  AlertTriangle,
  Lock,
  Shield,
  Activity,
  Calendar,
  ChevronRight,
  CheckSquare,
  Info,
  Zap,
  Award,
  Cloud,
  User,
} from 'lucide-react';
import { format, formatDistanceToNowStrict } from 'date-fns';
import { BarChart, Bar, XAxis, YAxis, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import { fetchGameDetails, fetchGameInjuries } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { useWebSocketEvent } from '../hooks/useWebSocket';
import PredictionCard from './PredictionCard';
import { teamName, teamAbbrev, teamLogo, parseAsUTC, isLiveStatus, confidencePct } from '../utils/teams';
import { formatAmericanOddsOrDash, getConfidenceColor } from '../utils/formatting';

const LIVE_POLL_INTERVAL = 30_000;

function formatGameDate(game) {
  try {
    const dateStr = game.start_time || game.datetime;
    if (!dateStr) return 'TBD';
    const date = parseAsUTC(dateStr);
    if (!date || isNaN(date.getTime())) return 'TBD';
    return format(date, 'EEEE, MMMM d, yyyy');
  } catch {
    return 'TBD';
  }
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

const formatAmericanOdds = formatAmericanOddsOrDash;

function getConfidenceLabel(confidence) {
  if (confidence >= 85) return 'EXCELLENT';
  if (confidence >= 70) return 'GOOD';
  if (confidence >= 55) return 'FAIR';
  return 'LOW';
}

function getRiskLevel(confidence, edge) {
  const edgePct = edge != null ? confidencePct(edge) : 0;
  if (confidence >= 75 && edgePct > 5) return { label: 'Low', color: 'var(--accent-green)', score: 25 };
  if (confidence >= 60 && edgePct > 2) return { label: 'Medium', color: 'var(--accent-gold)', score: 45 };
  if (confidence >= 45) return { label: 'High', color: 'var(--accent-orange, #ff9800)', score: 70 };
  return { label: 'Very High', color: 'var(--accent-red)', score: 85 };
}

function getStakeLevel(confidence) {
  if (confidence >= 80) return 'Heavy';
  if (confidence >= 65) return 'Medium';
  if (confidence >= 50) return 'Light';
  return 'Skip';
}

function formatPeriodLabel(game) {
  const period = game.period;
  const periodType = game.period_type;
  if (!period) return 'LIVE';
  if (periodType === 'OT') return 'OT';
  if (periodType === 'SO') return 'SO';
  if (period === 1) return '1st';
  if (period === 2) return '2nd';
  if (period === 3) return '3rd';
  return `${period}th`;
}

/* ──────────────────── Header Section ──────────────────── */
function GameHeader({ game, awayAbbr, homeAbbr, awayTeamLabel, homeTeamLabel, confidence, isLive, venue, pickIsHome, pickIsAway }) {
  const confLabel = confidence != null ? getConfidenceLabel(confidence) : null;

  return (
    <div className="gd-header">
      <div className="gd-header-main">
        {/* Away team badge (left) */}
        <div className="gd-team-badge gd-team-away">
          <div className={`gd-badge-box ${pickIsAway ? 'gd-badge-picked' : ''}`}>
            {teamLogo(game.away_team || game.away_team_form) ? (
              <img
                src={teamLogo(game.away_team || game.away_team_form) || game.away_team_form?.logo_url}
                alt={awayAbbr}
                width={48}
                height={48}
                className="gd-badge-logo"
                onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling && (e.target.nextSibling.style.display = 'block'); }}
              />
            ) : null}
            <span className="gd-badge-abbr">{awayAbbr}</span>
          </div>
          <div className="gd-team-info">
            <div className="gd-team-fullname">{awayTeamLabel}</div>
            <div className="gd-team-label">Away</div>
          </div>
        </div>

        {/* VS divider or score */}
        <div className="gd-center">
          {isLive ? (
            <div className="gd-live-center">
              <div className="detail-live-badge">
                <Radio size={14} className="live-icon-pulse" />
                LIVE
              </div>
              <div className="detail-live-score">
                <span className={`detail-score ${game.away_score > game.home_score ? 'score-winning' : ''}`}>
                  {game.away_score ?? 0}
                </span>
                <span className="detail-score-sep">-</span>
                <span className={`detail-score ${game.home_score > game.away_score ? 'score-winning' : ''}`}>
                  {game.home_score ?? 0}
                </span>
              </div>
              <div className="detail-live-period">
                {formatPeriodLabel(game)} {game.clock || '--:--'}
              </div>
            </div>
          ) : game.status === 'final' ? (
            <div className="gd-final-center">
              <div className="detail-final-badge">Final{game.overtime ? ' (OT)' : ''}</div>
              <div className="detail-live-score">
                <span className={`detail-score ${game.away_score > game.home_score ? 'score-winning' : ''}`}>
                  {game.away_score ?? 0}
                </span>
                <span className="detail-score-sep">-</span>
                <span className={`detail-score ${game.home_score > game.away_score ? 'score-winning' : ''}`}>
                  {game.home_score ?? 0}
                </span>
              </div>
            </div>
          ) : (
            <span className="gd-vs">VS</span>
          )}
        </div>

        {/* Home team badge (right) */}
        <div className="gd-team-badge gd-team-home">
          <div className={`gd-badge-box ${pickIsHome ? 'gd-badge-picked' : ''}`}>
            {teamLogo(game.home_team || game.home_team_form) ? (
              <img
                src={teamLogo(game.home_team || game.home_team_form) || game.home_team_form?.logo_url}
                alt={homeAbbr}
                width={48}
                height={48}
                className="gd-badge-logo"
                onError={(e) => { e.target.style.display = 'none'; e.target.nextSibling && (e.target.nextSibling.style.display = 'block'); }}
              />
            ) : null}
            <span className="gd-badge-abbr">{homeAbbr}</span>
          </div>
          <div className="gd-team-info">
            <div className="gd-team-fullname">{homeTeamLabel}</div>
            <div className="gd-team-label">Home</div>
          </div>
        </div>
      </div>

      {/* Right side: tags + meta */}
      <div className="gd-header-meta">
        <div className="gd-header-tags">
          <span className="dc-tag dc-tag-sport">Hockey</span>
          {confLabel && (
            <span className={`dc-tag dc-tag-confidence ${confidence >= 70 ? 'badge-good' : confidence >= 55 ? 'badge-borderline' : 'badge-low'}`}>
              <TrendingUp size={12} />
              {confLabel} - {Math.round(confidence)}%
            </span>
          )}
        </div>
        <div className="gd-meta-items">
          <div className="gd-meta-item">
            <Calendar size={14} />
            {formatGameDate(game)}
          </div>
          <div className="gd-meta-item">
            <Clock size={14} />
            {formatGameTime(game)}
          </div>
          {venue && (
            <div className="gd-meta-item">
              <MapPin size={14} />
              {venue}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

/* ──────────────────── Odds Cards ──────────────────── */
function OddsCards({ game, homeAbbr, awayAbbr, pickBetType, pickIsHome, pickIsAway, pickIsOver, pickIsUnder }) {
  const odds = game.odds;
  if (!odds) return null;

  const isOT = game.period_type === 'OT' || game.period_type === 'SO';
  const live = isLiveStatus(game.status);
  const locked = live && isOT;

  return (
    <div className="gd-odds-row">
      {/* Moneyline */}
      {(odds.home_moneyline != null || odds.away_moneyline != null) && (
        <div className={`gd-odds-card ${pickBetType === 'ml' ? 'gd-odds-card-picked' : ''}`}>
          <div className="gd-odds-card-header">
            <Target size={14} />
            <span>Moneyline</span>
          </div>
          <div className="gd-odds-card-body">
            <div className={`gd-odds-side ${pickIsHome && pickBetType === 'ml' ? 'gd-odds-side-picked' : ''}`}>
              <span className="gd-odds-team">{homeAbbr}</span>
              <span className="gd-odds-big">{formatAmericanOdds(odds.home_moneyline)}</span>
            </div>
            <span className="gd-odds-vs">VS</span>
            <div className={`gd-odds-side ${pickIsAway && pickBetType === 'ml' ? 'gd-odds-side-picked' : ''}`}>
              <span className="gd-odds-team">{awayAbbr}</span>
              <span className="gd-odds-big">{formatAmericanOdds(odds.away_moneyline)}</span>
            </div>
          </div>
        </div>
      )}

      {/* Spread */}
      <div className={`gd-odds-card ${locked ? 'gd-odds-locked' : ''} ${pickBetType === 'spread' ? 'gd-odds-card-picked' : ''}`}>
        <div className="gd-odds-card-header">
          <TrendingUp size={14} />
          <span>Spread</span>
        </div>
        {locked ? (
          <div className="gd-odds-locked-body"><Lock size={16} /></div>
        ) : (
          <div className="gd-odds-card-body">
            <div className={`gd-odds-side ${pickIsHome && pickBetType === 'spread' ? 'gd-odds-side-picked' : ''}`}>
              <span className="gd-odds-team">{homeAbbr}</span>
              <span className="gd-odds-big">
                {odds.home_spread_line != null
                  ? `${odds.home_spread_line > 0 ? '+' : ''}${odds.home_spread_line}`
                  : '-1.5'}
              </span>
              {odds.home_spread_price != null
                ? <span className="gd-odds-price">({formatAmericanOdds(odds.home_spread_price)})</span>
                : <span className="gd-odds-price gd-odds-pending">TBD</span>}
            </div>
            <span className="gd-odds-vs">VS</span>
            <div className={`gd-odds-side ${pickIsAway && pickBetType === 'spread' ? 'gd-odds-side-picked' : ''}`}>
              <span className="gd-odds-team">{awayAbbr}</span>
              <span className="gd-odds-big">
                {odds.away_spread_line != null
                  ? `${odds.away_spread_line > 0 ? '+' : ''}${odds.away_spread_line}`
                  : '+1.5'}
              </span>
              {odds.away_spread_price != null
                ? <span className="gd-odds-price">({formatAmericanOdds(odds.away_spread_price)})</span>
                : <span className="gd-odds-price gd-odds-pending">TBD</span>}
            </div>
          </div>
        )}
      </div>

      {/* Total O/U */}
      {odds.over_under_line != null && (
        <div className={`gd-odds-card ${locked ? 'gd-odds-locked' : ''} ${pickBetType === 'total' ? 'gd-odds-card-picked' : ''}`}>
          <div className="gd-odds-card-header">
            <Zap size={14} />
            <span>Total (O/U)</span>
          </div>
          {locked ? (
            <div className="gd-odds-locked-body"><Lock size={16} /></div>
          ) : (
            <div className="gd-odds-card-body">
              <div className={`gd-odds-side ${pickIsOver ? 'gd-odds-side-picked' : ''}`}>
                <span className="gd-odds-team">Over</span>
                <span className="gd-odds-big">{odds.over_under_line}</span>
                <span className="gd-odds-price">({formatAmericanOdds(odds.over_price)})</span>
              </div>
              <span className="gd-odds-vs">/</span>
              <div className={`gd-odds-side ${pickIsUnder ? 'gd-odds-side-picked' : ''}`}>
                <span className="gd-odds-team">Under</span>
                <span className="gd-odds-big">{odds.over_under_line}</span>
                <span className="gd-odds-price">({formatAmericanOdds(odds.under_price)})</span>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ──────────────────── Game Props ──────────────────── */
function GamePropsSection({ game, homeAbbr, awayAbbr }) {
  const gp = game.game_props;
  if (!gp) return null;

  const hasBTTS = gp.btts_yes_price != null || gp.btts_no_price != null;
  const hasReg = gp.reg_home_price != null || gp.reg_away_price != null;
  const hasP1 = gp.period1_home_ml != null || gp.period1_total_line != null;

  if (!hasBTTS && !hasReg && !hasP1) return null;

  const homeLabel = game.home_team_form?.team_name || homeAbbr;
  const awayLabel = game.away_team_form?.team_name || awayAbbr;

  return (
    <div className="gd-section-card">
      <div className="gd-section-header">
        <Layers size={16} />
        <h3>Game Props</h3>
      </div>
      <div className="gd-game-props-grid">
        {hasBTTS && (
          <div className="gd-game-prop-card">
            <div className="gd-game-prop-title">Both Teams to Score</div>
            <div className="gd-game-prop-odds">
              <div className="gd-game-prop-side">
                <span className="gd-game-prop-label">Yes</span>
                <span className="gd-game-prop-price">{formatAmericanOdds(gp.btts_yes_price)}</span>
              </div>
              <div className="gd-game-prop-side">
                <span className="gd-game-prop-label">No</span>
                <span className="gd-game-prop-price">{formatAmericanOdds(gp.btts_no_price)}</span>
              </div>
            </div>
          </div>
        )}

        {hasReg && (
          <div className="gd-game-prop-card">
            <div className="gd-game-prop-title">Regulation Winner</div>
            <div className="gd-game-prop-odds gd-game-prop-3way">
              <div className="gd-game-prop-side">
                <span className="gd-game-prop-label">{homeAbbr}</span>
                <span className="gd-game-prop-price">{formatAmericanOdds(gp.reg_home_price)}</span>
              </div>
              <div className="gd-game-prop-side">
                <span className="gd-game-prop-label">Draw</span>
                <span className="gd-game-prop-price">{formatAmericanOdds(gp.reg_draw_price)}</span>
              </div>
              <div className="gd-game-prop-side">
                <span className="gd-game-prop-label">{awayAbbr}</span>
                <span className="gd-game-prop-price">{formatAmericanOdds(gp.reg_away_price)}</span>
              </div>
            </div>
          </div>
        )}

        {hasP1 && (
          <div className="gd-game-prop-card">
            <div className="gd-game-prop-title">1st Period</div>
            <div className="gd-game-prop-subgrid">
              {gp.period1_home_ml != null && (
                <div className="gd-game-prop-row">
                  <span className="gd-game-prop-market">ML</span>
                  <span>{homeAbbr} {formatAmericanOdds(gp.period1_home_ml)}</span>
                  <span>{awayAbbr} {formatAmericanOdds(gp.period1_away_ml)}</span>
                  {gp.period1_draw_price != null && <span>Draw {formatAmericanOdds(gp.period1_draw_price)}</span>}
                </div>
              )}
              {gp.period1_total_line != null && (
                <div className="gd-game-prop-row">
                  <span className="gd-game-prop-market">O/U {gp.period1_total_line}</span>
                  <span>O {formatAmericanOdds(gp.period1_over_price)}</span>
                  <span>U {formatAmericanOdds(gp.period1_under_price)}</span>
                </div>
              )}
              {gp.period1_spread_line != null && (
                <div className="gd-game-prop-row">
                  <span className="gd-game-prop-market">PL</span>
                  <span>{homeAbbr} {gp.period1_spread_line > 0 ? '+' : ''}{gp.period1_spread_line} ({formatAmericanOdds(gp.period1_home_spread_price)})</span>
                  <span>{awayAbbr} ({formatAmericanOdds(gp.period1_away_spread_price)})</span>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

/* ──────────────────── Key Injuries ──────────────────── */
function KeyInjuries({ injuries, homeTeamLabel, awayTeamLabel }) {
  if (!injuries) return null;
  const home = injuries.home_injuries || [];
  const away = injuries.away_injuries || [];
  if (home.length === 0 && away.length === 0) return null;

  const totalOut = [...home, ...away].filter(
    (i) => i.status?.toLowerCase() === 'out' || i.status?.toLowerCase() === 'ir'
  ).length;

  const statusBadge = (status) => {
    const s = (status || '').toLowerCase();
    if (s === 'out' || s === 'ir') return <span className="gd-injury-status gd-injury-out">Out</span>;
    if (s === 'day-to-day' || s === 'dtd') return <span className="gd-injury-status gd-injury-dtd">Day-to-Day</span>;
    if (s === 'questionable') return <span className="gd-injury-status gd-injury-q">Questionable</span>;
    return <span className="gd-injury-status gd-injury-prob">{status}</span>;
  };

  const renderTeam = (players, teamLabel) => {
    if (players.length === 0) return null;
    return (
      <div className="gd-injury-team">
        <div className="gd-injury-team-header">
          <TrendingUp size={14} />
          <strong>{teamLabel}</strong>
          <span className="gd-injury-count">{players.length}</span>
        </div>
        {players.map((p, i) => (
          <div key={i} className="gd-injury-player">
            <div className="gd-injury-name">
              <User size={13} />
              {p.player_name}
              {p.position && <span className="gd-injury-pos">({p.position})</span>}
            </div>
            <div className="gd-injury-detail">
              {p.injury_type && <span className="gd-injury-type">{p.injury_type}</span>}
              {statusBadge(p.status)}
            </div>
          </div>
        ))}
      </div>
    );
  };

  return (
    <div className="gd-section-card">
      <div className="gd-section-header">
        <AlertTriangle size={16} />
        <h3>Key Injuries</h3>
        {totalOut > 0 && <span className="gd-injury-status gd-injury-out">{totalOut} Out</span>}
      </div>
      {renderTeam(home, homeTeamLabel)}
      {renderTeam(away, awayTeamLabel)}
    </div>
  );
}

/**
 * Strip {{team:...}} and {{tooltip:...}} markers from reasoning text,
 * returning clean human-readable text.
 */
function cleanReasoningText(raw) {
  if (!raw) return '';
  return raw
    .replace(/\s*\(Odds:\s*[^)]*\)/g, '')
    .replace(/\{\{team:[^}]+\}\}\s*/g, '')
    .replace(/\s*\{\{tooltip:[^}]*\}\}/g, '')
    .trim();
}

/* ──────────────────── AI Match Analysis ──────────────────── */
function AIAnalysis({ game, homeAbbr, awayAbbr, homeTeamLabel, awayTeamLabel }) {
  const predictions = game.predictions || game.bets || [];
  // Use the same top_pick the dashboard computed so both pages always
  // agree on which bet to display.  Fall back to the old heuristic
  // only when the backend didn't provide top_pick.
  const topPick = game.top_pick
    ? {
        bet_type: game.top_pick.bet_type,
        prediction_value: game.top_pick.prediction_value,
        confidence: game.top_pick.confidence,
        edge: game.top_pick.edge,
        reasoning: game.top_pick.reasoning,
        recommended: !game.top_pick.is_fallback,
      }
    : predictions.find((p) => p.recommended) || predictions[0];
  if (!topPick) return null;

  const confidence = confidencePct(topPick.confidence);
  const edge = confidencePct(topPick.edge || 0);
  const risk = getRiskLevel(confidence, topPick.edge);
  const stake = getStakeLevel(confidence);
  const confColor = getConfidenceColor(confidence);
  const isQualified = confidence >= 70 && edge > 3;

  const rawReasoning = topPick.reasoning || topPick.reason || topPick.analysis || '';
  const cleaned = cleanReasoningText(rawReasoning);
  const reasons = cleaned
    .split(/(?:\d+\.\s+|\n|;\s*)/)
    .map((s) => s.trim())
    .filter((s) => s.length > 5);

  // If splitting by numbered lines didn't work well, try sentence splitting
  if (reasons.length <= 1 && cleaned.length > 20) {
    const bySentence = cleaned
      .split(/\.\s+/)
      .map((s) => s.trim().replace(/\.$/, ''))
      .filter((s) => s.length > 5);
    if (bySentence.length > reasons.length) {
      reasons.length = 0;
      reasons.push(...bySentence);
    }
  }

  // Categorize reasons into positive/negative/neutral
  const negativeKeywords = ['losing', 'loss', 'struggle', 'injury', 'missing', 'without', 'concern', 'decline', 'losing streak'];
  const positiveKeywords = ['strong', 'winning', 'advantage', 'record', 'home-ice', 'dominant', 'superior', 'hot streak', 'edge', 'dominates'];
  const categorized = reasons.slice(0, 6).map((r) => {
    const lower = r.toLowerCase();
    if (negativeKeywords.some((k) => lower.includes(k))) return { text: r, type: 'negative' };
    if (positiveKeywords.some((k) => lower.includes(k))) return { text: r, type: 'positive' };
    return { text: r, type: 'neutral' };
  });

  // Build pick team name and human-readable pick label
  const pickValue = topPick.prediction_value || '';
  const pickLower = pickValue.toLowerCase();
  const pickBetType = (topPick.bet_type || '').toLowerCase();
  const pickIsHomeTeam = pickLower.includes('home') || pickValue.includes(homeAbbr);
  const pickIsAwayTeam = pickLower.includes('away') || pickValue.includes(awayAbbr);

  const odds = game.odds || {};
  let pickTeam, pickSide;
  if (pickBetType === 'total') {
    const isOver = pickLower.includes('over');
    const line = pickValue.replace(/^(over|under)[_\s]?/i, '');
    pickTeam = `${isOver ? 'Over' : 'Under'} ${line}`;
    pickSide = '(Total)';
  } else if (pickBetType === 'spread') {
    // Spread pick - show team name + spread line
    if (pickIsHomeTeam) {
      const line = odds.home_spread_line;
      pickTeam = `${homeTeamLabel} ${line != null ? (line > 0 ? '+' : '') + line : ''}`.trim();
      pickSide = '(Spread)';
    } else if (pickIsAwayTeam) {
      const line = odds.away_spread_line;
      pickTeam = `${awayTeamLabel} ${line != null ? (line > 0 ? '+' : '') + line : ''}`.trim();
      pickSide = '(Spread)';
    } else {
      pickTeam = pickValue.replace(/_/g, ' ');
      pickSide = '(Spread)';
    }
  } else if (pickIsHomeTeam) {
    pickTeam = homeTeamLabel;
    pickSide = '(Moneyline)';
  } else if (pickIsAwayTeam) {
    pickTeam = awayTeamLabel;
    pickSide = '(Moneyline)';
  } else {
    pickTeam = pickValue.replace(/_/g, ' ');
    pickSide = '';
  }

  return (
    <div className="gd-section-card gd-analysis-card">
      <div className="gd-section-header">
        <Target size={16} />
        <h3>Match Analysis</h3>
        {isQualified && (
          <span className="gd-qualified-badge">
            <CheckSquare size={12} />
            QUALIFIED BET
          </span>
        )}
      </div>

      {/* Main narrative */}
      {reasons.length > 0 && (
        <div className="gd-analysis-narrative">
          <p className="gd-analysis-main">{reasons[0]}</p>
          {reasons.length > 1 && (
            <p className="gd-analysis-sub">{reasons.slice(1, 3).join('. ')}.</p>
          )}
        </div>
      )}

      {/* Key Insight callout */}
      {reasons.length > 3 && (
        <div className="gd-callout gd-callout-insight">
          <Zap size={14} />
          <div>
            <strong>Key Insight:</strong> {reasons[3]}
          </div>
        </div>
      )}

      {/* Analysis Factors */}
      {categorized.length > 0 && (
        <>
          <h4 className="gd-factors-title">
            <CheckSquare size={14} />
            Analysis Factors
          </h4>
          <div className="gd-factors-grid">
            {categorized.map((factor, i) => (
              <div key={i} className={`gd-factor gd-factor-${factor.type}`}>
                {factor.type === 'positive' && <CheckSquare size={13} />}
                {factor.type === 'negative' && <TrendingDown size={13} />}
                {factor.type === 'neutral' && <AlertTriangle size={13} />}
                <span>{factor.text}</span>
              </div>
            ))}
          </div>
        </>
      )}

      {/* Injury Impact callout */}
      {reasons.some((r) => r.toLowerCase().includes('injur')) && (
        <div className="gd-callout gd-callout-warning">
          <AlertTriangle size={14} />
          <div>
            <strong>Injury Impact:</strong> {reasons.find((r) => r.toLowerCase().includes('injur'))}
          </div>
        </div>
      )}

      {/* Confidence + Risk */}
      <div className="gd-confidence-row">
        <div className="gd-confidence-big">
          <span className="gd-confidence-pct" style={{ color: confColor }}>
            {Math.round(confidence)}%
          </span>
          <span className="gd-confidence-label">
            <Info size={12} />
            AI Confidence
          </span>
        </div>
        <span className={`gd-risk-badge`} style={{ color: risk.color, borderColor: risk.color }}>
          {risk.label} Risk
        </span>
      </div>

      {/* Suggested Action */}
      <div className="gd-suggested-action">
        <div className="gd-suggested-header">
          <TrendingUp size={14} />
          <span>Suggested Action</span>
        </div>
        <div className="gd-suggested-grid">
          <div className="gd-suggested-item">
            <span className="gd-suggested-label">Pick</span>
            <strong>{pickTeam}</strong>
            <span className="gd-suggested-sub">{pickSide}</span>
          </div>
          <div className="gd-suggested-item">
            <span className="gd-suggested-label">Confidence</span>
            <strong>{Math.round(confidence)}%</strong>
          </div>
          <div className="gd-suggested-item">
            <span className="gd-suggested-label">Risk Level</span>
            <strong style={{ color: risk.color }}>{risk.label}</strong>
          </div>
          <div className="gd-suggested-item">
            <span className="gd-suggested-label">Suggested Stake</span>
            <strong>{stake}</strong>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ──────────────────── Risk Assessment + Market Interest ──────────────────── */
function RiskAndMarket({ game, homeAbbr, awayAbbr, homeTeamLabel, awayTeamLabel }) {
  const predictions = game.predictions || game.bets || [];
  const topPick = predictions.find((p) => p.recommended) || predictions[0];
  if (!topPick) return null;

  const confidence = confidencePct(topPick.confidence);
  const risk = getRiskLevel(confidence, topPick.edge);

  // Derive market interest from odds differential
  const odds = game.odds || {};
  const homeML = odds.home_moneyline || 0;
  const awayML = odds.away_moneyline || 0;
  const homeImplied = homeML < 0 ? Math.abs(homeML) / (Math.abs(homeML) + 100) : 100 / (homeML + 100);
  const awayImplied = awayML < 0 ? Math.abs(awayML) / (Math.abs(awayML) + 100) : 100 / (awayML + 100);
  // Normalize to remove vig (raw implied probs sum > 100%)
  const totalImplied = homeImplied + awayImplied || 1;
  const homePct = Math.round((homeImplied / totalImplied) * 100);
  const awayPct = 100 - homePct;

  const underdogTeam = homePct < awayPct ? homeTeamLabel : awayTeamLabel;
  const underdogAbbr = homePct < awayPct ? homeAbbr : awayAbbr;

  return (
    <div className="gd-two-col">
      {/* Risk Assessment */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <Shield size={16} />
          <h3>Risk Assessment</h3>
        </div>
        <div className="gd-risk-content">
          <div className="gd-risk-info">
            <span>Risk Level: {risk.label}</span>
            <span>{risk.score}/100</span>
          </div>
          <div className="gd-risk-bar">
            <div
              className="gd-risk-bar-fill"
              style={{ width: `${risk.score}%`, backgroundColor: risk.color }}
            />
          </div>
          <div className="gd-risk-note">
            <AlertTriangle size={13} />
            {risk.score <= 30 ? 'Strong edge detected — favorable risk/reward.' :
              risk.score <= 60 ? 'Very close spread - coin flip territory.' :
                'High variance — proceed with caution.'}
          </div>

          {/* Risk factors breakdown */}
          <div className="gd-risk-factors">
            <div className="gd-risk-factor-row">
              <span className="gd-risk-factor-label">Model Confidence</span>
              <span className="gd-risk-factor-val" style={{ color: confidence >= 65 ? 'var(--accent-green)' : confidence >= 55 ? 'var(--accent-gold)' : 'var(--accent-red)' }}>
                {Math.round(confidence)}%
              </span>
            </div>
            <div className="gd-risk-factor-row">
              <span className="gd-risk-factor-label">Edge vs Market</span>
              <span className="gd-risk-factor-val" style={{ color: confidencePct(topPick.edge || 0) > 5 ? 'var(--accent-green)' : confidencePct(topPick.edge || 0) > 2 ? 'var(--accent-gold)' : 'var(--text-muted)' }}>
                {confidencePct(topPick.edge || 0).toFixed(1)}%
              </span>
            </div>
            <div className="gd-risk-factor-row">
              <span className="gd-risk-factor-label">Line Value</span>
              <span className="gd-risk-factor-val">
                {(() => {
                  const e = confidencePct(topPick.edge || 0);
                  if (e > 8) return 'Excellent';
                  if (e > 5) return 'Good';
                  if (e > 2) return 'Fair';
                  return 'Thin';
                })()}
              </span>
            </div>
            <div className="gd-risk-factor-row">
              <span className="gd-risk-factor-label">Suggested Stake</span>
              <span className="gd-risk-factor-val">
                {confidence >= 80 ? '3-5 units (Heavy)' : confidence >= 65 ? '2-3 units (Medium)' : confidence >= 50 ? '1 unit (Light)' : 'Pass'}
              </span>
            </div>
          </div>

          {/* Risk context note */}
          <div className="gd-risk-context">
            {confidence >= 70 && confidencePct(topPick.edge || 0) > 5
              ? 'High-confidence pick with significant edge over the market. This is a strong play.'
              : confidence >= 60
                ? 'Moderate confidence. The model sees value but the margin is slim — consider sizing down.'
                : 'Lower-confidence play. Treat as a speculative pick and limit exposure.'}
          </div>
        </div>
      </div>

      {/* Implied Probability */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <DollarSign size={16} />
          <h3>Implied Probability</h3>
        </div>
        <p className="gd-section-desc">
          The odds imply how likely the market thinks each team is to win. These percentages are derived from the moneyline.
        </p>
        <div className="gd-market-content">
          <div className="gd-market-pcts">
            <div className="gd-market-side">
              <span className="gd-market-abbr">{homeAbbr}</span>
              <span className="gd-market-pct">{homePct}%</span>
              <span className="gd-market-ml">{formatAmericanOdds(homeML)}</span>
            </div>
            <div className="gd-market-bar-wrap">
              <div className="gd-market-bar">
                <div className="gd-market-bar-home" style={{ width: `${homePct}%` }} />
              </div>
            </div>
            <div className="gd-market-side">
              <span className="gd-market-abbr">{awayAbbr}</span>
              <span className="gd-market-pct">{awayPct}%</span>
              <span className="gd-market-ml">{formatAmericanOdds(awayML)}</span>
            </div>
          </div>
          <div className="gd-market-explainer">
            <Info size={13} />
            <span>
              {homePct > awayPct
                ? `${homeTeamLabel} is the market favorite (${homePct}% implied win probability).`
                : awayPct > homePct
                  ? `${awayTeamLabel} is the market favorite (${awayPct}% implied win probability).`
                  : 'Market sees this as a toss-up.'}
              {Math.abs(homePct - awayPct) > 15
                ? ` There may be value on ${underdogTeam} as the underdog.`
                : ''}
            </span>
          </div>
        </div>
      </div>
    </div>
  );
}

/**
 * Format a stat value — handles both decimal (0.222) and already-percentage (22.2) formats.
 * For percentage stats, if value <= 1 it's a decimal (multiply by 100), otherwise display as-is.
 */
function fmtStat(val, isPct = false, decimals = 2) {
  if (val == null) return '-';
  if (typeof val !== 'number') return val;
  if (isPct) {
    const display = val <= 1 ? val * 100 : val;
    return `${display.toFixed(1)}%`;
  }
  return val.toFixed(decimals);
}

/* ──────────────────── Season Stats & Standings + Betting Trends ──────────────────── */
function StatsAndTrends({ game, homeAbbr, awayAbbr }) {
  const home = game.home_team_form || {};
  const away = game.away_team_form || {};
  const homeLabel = home.team_name || 'Home';
  const awayLabel = away.team_name || 'Away';
  const homeLogo = home.logo_url || teamLogo(game.home_team) || teamLogo(game.home_team_form);
  const awayLogo = away.logo_url || teamLogo(game.away_team) || teamLogo(game.away_team_form);
  const homeRecord = `${home.wins || 0}-${home.losses || 0}-${home.ot_losses || 0}`;
  const awayRecord = `${away.wins || 0}-${away.losses || 0}-${away.ot_losses || 0}`;
  const homeWinPct = home.games_played ? `${Math.round((home.wins / home.games_played) * 100)}%` : '-';
  const awayWinPct = away.games_played ? `${Math.round((away.wins / away.games_played) * 100)}%` : '-';
  const ouLine = game.odds?.over_under_line || 5.5;

  // Streak from recent games
  const homeRecent = game.home_recent_games || [];
  const awayRecent = game.away_recent_games || [];
  const getStreak = (games) => {
    if (!games.length) return 'N/A';
    let count = 1;
    const first = games[0]?.result;
    for (let i = 1; i < games.length; i++) {
      if (games[i].result === first) count++;
      else break;
    }
    return `${first === 'W' ? 'W' : first === 'L' ? 'L' : 'OTL'}${count}`;
  };

  // Division rank from points
  const homeRank = home.division_rank || '-';
  const awayRank = away.division_rank || '-';
  const homeDivLabel = home.division_name ? `${home.division_name}` : '';
  const awayDivLabel = away.division_name ? `${away.division_name}` : '';
  const homeDivSize = home.division_size || '';
  const awayDivSize = away.division_size || '';

  const lg = game.league_averages || {};
  const homeRanks = lg.home_ranks || {};
  const awayRanks = lg.away_ranks || {};
  const totalTeams = lg.total_teams || 32;

  // Color-code stats vs league average: green = favorable, red = unfavorable
  const statColor = (val, avg, higherIsBetter = true) => {
    if (val == null || avg == null) return '';
    const diff = val - avg;
    if (higherIsBetter) return diff >= 0 ? 'gd-stat-good' : 'gd-stat-bad';
    return diff <= 0 ? 'gd-stat-good' : 'gd-stat-bad';
  };

  const fmtRank = (rank) => rank != null ? `#${rank}` : '';

  const makeStat = (label, key, isPct = false, higherIsBetter = true) => ({
    label,
    homeVal: fmtStat(home[key], isPct),
    awayVal: fmtStat(away[key], isPct),
    homeClass: statColor(home[key], lg[key], higherIsBetter),
    awayClass: statColor(away[key], lg[key], higherIsBetter),
    homeRank: fmtRank(homeRanks[key]),
    awayRank: fmtRank(awayRanks[key]),
    avg: fmtStat(lg[key], isPct),
  });

  const perfStats = [
    makeStat('Goals For/Game', 'goals_for_per_game'),
    makeStat('Goals Against/Game', 'goals_against_per_game', false, false),
    makeStat('Power Play %', 'power_play_pct', true),
    makeStat('Penalty Kill %', 'penalty_kill_pct', true),
    makeStat('Shots For/Game', 'shots_for_per_game'),
    makeStat('Shots Against/Game', 'shots_against_per_game', false, false),
    makeStat('Faceoff Win %', 'faceoff_win_pct', true),
  ];

  // Compute ATS-like record from recent games
  const computeATS = (recent) => {
    if (!recent || recent.length === 0) return null;
    const w = recent.filter((g) => g.result === 'W').length;
    const l = recent.filter((g) => g.result === 'L').length;
    const otl = recent.filter((g) => g.result === 'OTL').length;
    return `${w}-${l}-${otl}`;
  };

  return (
    <div className="gd-two-col">
      {/* Season Stats & Standings */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <BarChart3 size={16} />
          <h3>Season Stats & Standings</h3>
        </div>
        <div className="gd-stats-teams">
          <div className="gd-stats-team">
            {awayLogo && <img src={awayLogo} alt="" width={48} height={48} className="gd-stats-logo" onError={(e) => { e.target.style.display = 'none'; }} />}
            <div className="gd-stats-team-name">{awayLabel}</div>
            <div className="gd-stats-record">{awayRecord}</div>
            <div className="gd-stats-badges">
              <span className={`gd-streak-badge ${getStreak(awayRecent).startsWith('W') ? 'streak-win' : 'streak-loss'}`}>
                {getStreak(awayRecent).startsWith('W') ? <TrendingUp size={11} /> : <TrendingDown size={11} />}
                {getStreak(awayRecent)}
              </span>
              {awayRank !== '-' && (
                <span className="gd-rank-badge">
                  <Award size={11} />
                  #{awayRank}{awayDivSize ? `/${awayDivSize}` : ''}{awayDivLabel ? ` ${awayDivLabel}` : ''}
                </span>
              )}
            </div>
            <div className="gd-stats-winpct">Win%: {awayWinPct}</div>
          </div>
          <div className="gd-stats-team">
            {homeLogo && <img src={homeLogo} alt="" width={48} height={48} className="gd-stats-logo" onError={(e) => { e.target.style.display = 'none'; }} />}
            <div className="gd-stats-team-name">{homeLabel}</div>
            <div className="gd-stats-record">{homeRecord}</div>
            <div className="gd-stats-badges">
              <span className={`gd-streak-badge ${getStreak(homeRecent).startsWith('W') ? 'streak-win' : 'streak-loss'}`}>
                {getStreak(homeRecent).startsWith('W') ? <TrendingUp size={11} /> : <TrendingDown size={11} />}
                {getStreak(homeRecent)}
              </span>
              {homeRank !== '-' && (
                <span className="gd-rank-badge">
                  <Award size={11} />
                  #{homeRank}{homeDivSize ? `/${homeDivSize}` : ''}{homeDivLabel ? ` ${homeDivLabel}` : ''}
                </span>
              )}
            </div>
            <div className="gd-stats-winpct">Win%: {homeWinPct}</div>
          </div>
        </div>

        <div className="gd-perf-section">
          <h4 className="gd-perf-title">KEY PERFORMANCE STATS</h4>
          <table className="gd-perf-table">
            <thead>
              <tr>
                <th className="gd-perf-th gd-perf-th-team">{awayLabel}</th>
                <th className="gd-perf-th gd-perf-th-stat">Stat</th>
                <th className="gd-perf-th gd-perf-th-avg">Lg Avg</th>
                <th className="gd-perf-th gd-perf-th-team">{homeLabel}</th>
              </tr>
            </thead>
            <tbody>
              {perfStats.map((s) => (
                <tr key={s.label}>
                  <td className={`gd-perf-td gd-perf-td-val ${s.awayClass || ''}`}>
                    {s.awayVal}
                    {s.awayRank && <span className="gd-perf-rank">{s.awayRank}</span>}
                  </td>
                  <td className="gd-perf-td gd-perf-td-label">{s.label}</td>
                  <td className="gd-perf-td gd-perf-td-avg">{s.avg || '-'}</td>
                  <td className={`gd-perf-td gd-perf-td-val ${s.homeClass || ''}`}>
                    {s.homeVal}
                    {s.homeRank && <span className="gd-perf-rank">{s.homeRank}</span>}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Betting Trends */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <TrendingUp size={16} />
          <h3>Betting Trends</h3>
        </div>
        <div className="gd-trends-content">
          <div className="gd-trends-teams">
            <div className="gd-trends-team">
              <div className="gd-trends-team-label">{awayLabel}</div>
              <div className="gd-trends-rows">
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Overall</span>
                  <span className="gd-trends-val">{away.wins || 0}-{away.losses || 0}-{away.ot_losses || 0}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Away</span>
                  <span className="gd-trends-val">{away.away_record || '-'}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Last 10</span>
                  <span className="gd-trends-val">{away.record_last_10 || computeATS(awayRecent.slice(0, 10)) || '-'}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Last 5</span>
                  <span className="gd-trends-val">{computeATS(awayRecent.slice(0, 5)) || '-'}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Streak</span>
                  <span className="gd-trends-val">{getStreak(awayRecent)}</span>
                </div>
              </div>
              {/* Scoring trends */}
              {awayRecent.length > 0 && (
                <div className="gd-trends-scoring">
                  <div className="gd-trends-scoring-title">Scoring (Last {Math.min(awayRecent.length, 10)})</div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Avg Goals For</span>
                    <span className="gd-trends-val">
                      {(awayRecent.slice(0, 10).reduce((s, g) => s + (g.goals_for || 0), 0) / Math.min(awayRecent.length, 10)).toFixed(1)}
                    </span>
                  </div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Avg Goals Against</span>
                    <span className="gd-trends-val">
                      {(awayRecent.slice(0, 10).reduce((s, g) => s + (g.goals_against || 0), 0) / Math.min(awayRecent.length, 10)).toFixed(1)}
                    </span>
                  </div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Avg Total</span>
                    <span className="gd-trends-val">
                      {(awayRecent.slice(0, 10).reduce((s, g) => s + (g.goals_for || 0) + (g.goals_against || 0), 0) / Math.min(awayRecent.length, 10)).toFixed(1)}
                    </span>
                  </div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Over {ouLine}</span>
                    <span className="gd-trends-val">
                      {awayRecent.slice(0, 10).filter((g) => (g.goals_for || 0) + (g.goals_against || 0) > ouLine).length}-{awayRecent.slice(0, 10).filter((g) => (g.goals_for || 0) + (g.goals_against || 0) <= ouLine).length}
                    </span>
                  </div>
                </div>
              )}
            </div>
            <div className="gd-trends-team">
              <div className="gd-trends-team-label">{homeLabel}</div>
              <div className="gd-trends-rows">
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Overall</span>
                  <span className="gd-trends-val">{home.wins || 0}-{home.losses || 0}-{home.ot_losses || 0}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Home</span>
                  <span className="gd-trends-val">{home.home_record || '-'}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Last 10</span>
                  <span className="gd-trends-val">{home.record_last_10 || computeATS(homeRecent.slice(0, 10)) || '-'}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Last 5</span>
                  <span className="gd-trends-val">{computeATS(homeRecent.slice(0, 5)) || '-'}</span>
                </div>
                <div className="gd-trends-row">
                  <span className="gd-trends-label">Streak</span>
                  <span className="gd-trends-val">{getStreak(homeRecent)}</span>
                </div>
              </div>
              {/* Scoring trends */}
              {homeRecent.length > 0 && (
                <div className="gd-trends-scoring">
                  <div className="gd-trends-scoring-title">Scoring (Last {Math.min(homeRecent.length, 10)})</div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Avg Goals For</span>
                    <span className="gd-trends-val">
                      {(homeRecent.slice(0, 10).reduce((s, g) => s + (g.goals_for || 0), 0) / Math.min(homeRecent.length, 10)).toFixed(1)}
                    </span>
                  </div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Avg Goals Against</span>
                    <span className="gd-trends-val">
                      {(homeRecent.slice(0, 10).reduce((s, g) => s + (g.goals_against || 0), 0) / Math.min(homeRecent.length, 10)).toFixed(1)}
                    </span>
                  </div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Avg Total</span>
                    <span className="gd-trends-val">
                      {(homeRecent.slice(0, 10).reduce((s, g) => s + (g.goals_for || 0) + (g.goals_against || 0), 0) / Math.min(homeRecent.length, 10)).toFixed(1)}
                    </span>
                  </div>
                  <div className="gd-trends-row">
                    <span className="gd-trends-label">Over {ouLine}</span>
                    <span className="gd-trends-val">
                      {homeRecent.slice(0, 10).filter((g) => (g.goals_for || 0) + (g.goals_against || 0) > ouLine).length}-{homeRecent.slice(0, 10).filter((g) => (g.goals_for || 0) + (g.goals_against || 0) <= ouLine).length}
                    </span>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ──────────────────── Goalie Matchup ──────────────────── */
function GoalieMatchup({ game }) {
  const homeGoalies = game.home_goalies || [];
  const awayGoalies = game.away_goalies || [];
  if (homeGoalies.length === 0 && awayGoalies.length === 0) return null;

  const homeStarter = game.home_starter;
  const awayStarter = game.away_starter;

  const renderGoalie = (g, starter) => {
    const isStarter = starter && starter.name && g.name && starter.name.toLowerCase() === g.name.toLowerCase();
    return (
      <div key={g.player_id} className={`gd-goalie-card ${isStarter ? 'gd-goalie-starter' : ''}`}>
        <div className="gd-goalie-name-row">
          <Shield size={14} />
          <strong>{g.name}</strong>
          {isStarter && (
            <span className={`gd-goalie-badge ${starter.confirmed ? 'gd-goalie-confirmed' : ''}`}>
              {starter.confirmed ? 'Starting' : 'Projected'}
            </span>
          )}
        </div>
        <div className="gd-goalie-stats-grid">
          <div className="gd-goalie-stat">
            <span className="gd-goalie-stat-val">{g.games_played || 0}</span>
            <span className="gd-goalie-stat-label">GP</span>
          </div>
          <div className="gd-goalie-stat">
            <span className="gd-goalie-stat-val">{g.wins || 0}-{g.losses || 0}-{g.ot_losses || 0}</span>
            <span className="gd-goalie-stat-label">Record</span>
          </div>
          <div className="gd-goalie-stat">
            <span className="gd-goalie-stat-val">{g.save_pct != null ? (g.save_pct <= 1 ? (g.save_pct * 100).toFixed(1) : g.save_pct.toFixed(1)) + '%' : '-'}</span>
            <span className="gd-goalie-stat-label">SV%</span>
          </div>
          <div className="gd-goalie-stat">
            <span className="gd-goalie-stat-val">{g.gaa != null ? g.gaa.toFixed(2) : '-'}</span>
            <span className="gd-goalie-stat-label">GAA</span>
          </div>
          <div className="gd-goalie-stat">
            <span className="gd-goalie-stat-val">{g.shutouts || 0}</span>
            <span className="gd-goalie-stat-label">SO</span>
          </div>
        </div>
      </div>
    );
  };

  const homeLabel = game.home_team_form?.team_name || 'Home';
  const awayLabel = game.away_team_form?.team_name || 'Away';

  return (
    <div className="gd-section-card">
      <div className="gd-section-header">
        <Shield size={16} />
        <h3>Goalie Matchup</h3>
      </div>
      <div className="gd-goalie-matchup">
        <div className="gd-goalie-team">
          <div className="gd-goalie-team-label">{awayLabel}</div>
          {awayGoalies.map((g) => renderGoalie(g, awayStarter))}
        </div>
        <div className="gd-goalie-team">
          <div className="gd-goalie-team-label">{homeLabel}</div>
          {homeGoalies.map((g) => renderGoalie(g, homeStarter))}
        </div>
      </div>
    </div>
  );
}

/* ──────────────────── Venue & Conditions ──────────────────── */
function VenueSection({ game }) {
  const venue = game.venue || game.arena || '';
  if (!venue) return null;

  // Derive city from venue or team info
  const homeForm = game.home_team_form || {};
  const city = homeForm.city || '';

  return (
    <div className="gd-section-card">
      <div className="gd-section-header">
        <MapPin size={16} />
        <h3>Venue & Conditions</h3>
      </div>
      <div className="gd-venue-content">
        <div className="gd-venue-grid">
          <div className="gd-venue-item">
            <MapPin size={16} />
            <span className="gd-venue-label">Venue</span>
            <strong>{venue}</strong>
            {city && <span className="gd-venue-sub">{city}</span>}
          </div>
          <div className="gd-venue-item">
            <Cloud size={16} />
            <span className="gd-venue-label">Setting</span>
            <strong>Indoor</strong>
          </div>
        </div>
        <p className="gd-venue-note">Game played at {venue}.</p>
      </div>
    </div>
  );
}

/* ──────────────────── Recent Form & H2H ──────────────────── */
function RecentFormAndH2H({ game, homeAbbr, awayAbbr }) {
  const homeForm = game.home_recent_games || [];
  const awayForm = game.away_recent_games || [];
  const homeLabel = game.home_team_form?.team_name || 'Home';
  const awayLabel = game.away_team_form?.team_name || 'Away';
  const h2h = game.head_to_head || game.h2h || null;

  const last5Home = homeForm.slice(0, 5);
  const last5Away = awayForm.slice(0, 5);

  const calcRecord = (games) => {
    const w = games.filter((g) => g.result === 'W').length;
    const l = games.filter((g) => g.result === 'L').length;
    return `${w}-${l}`;
  };

  // Determine trending direction from last 5 games
  const getFormTrend = (games) => {
    if (games.length < 2) return 'neutral';
    const wins = games.filter((g) => g.result === 'W').length;
    if (wins >= 4) return 'hot';
    if (wins >= 3) return 'up';
    if (wins <= 1) return 'down';
    return 'neutral';
  };
  const homeTrend = getFormTrend(last5Home);
  const awayTrend = getFormTrend(last5Away);

  const homeId = game.home_team_form?.team_id ?? game.home_team?.id;
  const team1IsHome = h2h?.team1_id === homeId;

  // Use backend-provided H2H games (all matchups between these teams)
  // Falls back to filtering recent games if backend data not available
  const backendH2H = (game.h2h_games || []).map((g) => ({ ...g, perspective: 'home' }));
  let h2hGames;
  if (backendH2H.length > 0) {
    h2hGames = backendH2H;
  } else {
    // Fallback: filter recent games for opponent matchups
    const h2hFromHome = homeForm.filter((g) => {
      const opp = (g.opponent_abbrev || g.opponent_name || '').toUpperCase();
      return opp === awayAbbr.toUpperCase();
    });
    const h2hFromAway = awayForm.filter((g) => {
      const opp = (g.opponent_abbrev || g.opponent_name || '').toUpperCase();
      return opp === homeAbbr.toUpperCase();
    });
    const h2hGamesMap = new Map();
    for (const g of h2hFromHome) {
      const key = g.game_date || g.date;
      if (key) h2hGamesMap.set(key, { ...g, perspective: 'home' });
    }
    for (const g of h2hFromAway) {
      const key = g.game_date || g.date;
      if (key && !h2hGamesMap.has(key)) h2hGamesMap.set(key, { ...g, perspective: 'away' });
    }
    h2hGames = [...h2hGamesMap.values()]
      .sort((a, b) => new Date(b.game_date || b.date) - new Date(a.game_date || a.date))
      .slice(0, 10);
  }

  return (
    <div className="gd-two-col">
      {/* Recent Form & H2H */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <Users size={16} />
          <h3>{h2hGames.length > 0 ? 'Recent Form & Head-to-Head' : 'Recent Form'}</h3>
        </div>

        {/* Recent Form */}
        <div className="gd-form-teams">
          <div className="gd-form-team">
            <div className="gd-form-team-header">
              {awayTrend === 'down' ? <TrendingDown size={13} className="form-trend-down" /> : <TrendingUp size={13} className={awayTrend === 'hot' ? 'form-trend-hot' : awayTrend === 'up' ? 'form-trend-up' : 'form-trend-neutral'} />}
              <strong>{awayLabel}</strong>
              <span className="gd-form-record-badge">{calcRecord(last5Away)} Last 5</span>
            </div>
            <div className="gd-form-dots">
              {[...last5Away].reverse().map((g, i) => (
                <span key={i} className={`gd-form-dot ${g.result === 'W' ? 'result-win' : g.result === 'OTL' ? 'result-otl' : 'result-loss'}`}>
                  {g.result === 'W' ? 'W' : 'L'}
                </span>
              ))}
            </div>
            <div className="gd-form-games">
              {last5Away.map((g, i) => (
                <div key={i} className="gd-form-game-row">
                  <span className="gd-form-game-date">{g.game_date ? format(new Date(g.game_date), 'MMM d') : ''}</span>
                  <span>vs {g.opponent_abbrev || g.opponent_name}</span>
                  <span className={g.result === 'W' ? 'gd-form-win' : 'gd-form-loss'}>
                    {g.result} {g.score_display}{g.overtime ? ' (OT)' : ''}
                  </span>
                </div>
              ))}
            </div>
          </div>

          <div className="gd-form-team">
            <div className="gd-form-team-header">
              {homeTrend === 'down' ? <TrendingDown size={13} className="form-trend-down" /> : <TrendingUp size={13} className={homeTrend === 'hot' ? 'form-trend-hot' : homeTrend === 'up' ? 'form-trend-up' : 'form-trend-neutral'} />}
              <strong>{homeLabel}</strong>
              <span className="gd-form-record-badge">{calcRecord(last5Home)} Last 5</span>
            </div>
            <div className="gd-form-dots">
              {[...last5Home].reverse().map((g, i) => (
                <span key={i} className={`gd-form-dot ${g.result === 'W' ? 'result-win' : g.result === 'OTL' ? 'result-otl' : 'result-loss'}`}>
                  {g.result === 'W' ? 'W' : 'L'}
                </span>
              ))}
            </div>
            <div className="gd-form-games">
              {last5Home.map((g, i) => (
                <div key={i} className="gd-form-game-row">
                  <span className="gd-form-game-date">{g.game_date ? format(new Date(g.game_date), 'MMM d') : ''}</span>
                  <span>vs {g.opponent_abbrev || g.opponent_name}</span>
                  <span className={g.result === 'W' ? 'gd-form-win' : 'gd-form-loss'}>
                    {g.result} {g.score_display}{g.overtime ? ' (OT)' : ''}
                  </span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* Takeaway */}
        {last5Home.length > 0 && last5Away.length > 0 && (
          <div className="gd-form-takeaway">
            <p>
              {(() => {
                const homeWins = last5Home.filter((g) => g.result === 'W').length;
                const awayWins = last5Away.filter((g) => g.result === 'W').length;
                if (homeWins > awayWins) return `Takeaway: ${homeLabel} has marginally better recent results, though both sides are inconsistent.`;
                if (awayWins > homeWins) return `Takeaway: ${awayLabel} has marginally better recent results, though both sides are inconsistent.`;
                return `Takeaway: Both teams have similar recent form.`;
              })()}
            </p>
          </div>
        )}

        {/* No H2H notice */}
        {h2hGames.length === 0 && (
          <div className="gd-no-h2h-notice">
            <Info size={14} />
            <span>These teams have not played each other this season yet.</span>
          </div>
        )}

        {/* H2H Summary */}
        {h2h && h2hGames.length > 0 && (
          <div className="gd-h2h-section">
            <div className="gd-h2h-title">
              <strong>Head-to-Head</strong>
              {(h2h.games_played < 3 && h2hGames.length < 3) && (
                <span className="gd-h2h-limited">
                  <AlertTriangle size={11} />
                  Limited Data
                </span>
              )}
            </div>

            <div className="gd-h2h-scores">
              <div className="gd-h2h-side">
                <span className="gd-h2h-big" style={{ color: 'var(--accent-blue)' }}>
                  {team1IsHome ? h2h.team2_wins : h2h.team1_wins}
                </span>
                <span>{awayLabel}</span>
              </div>
              <span className="gd-h2h-vs">vs</span>
              <div className="gd-h2h-side">
                <span className="gd-h2h-big" style={{ color: 'var(--accent-blue)' }}>
                  {team1IsHome ? h2h.team1_wins : h2h.team2_wins}
                </span>
                <span>{homeLabel}</span>
              </div>
            </div>

            {h2h.last_meeting && (
              <div className="gd-h2h-last">Last meeting: {format(new Date(h2h.last_meeting), 'yyyy-MM-dd')}</div>
            )}
          </div>
        )}

        {/* H2H Individual Game Details */}
        {h2hGames.length > 0 && (
          <div className="gd-h2h-games">
            <div className="gd-h2h-games-title">
              <Calendar size={13} />
              <strong>Recent Matchups ({h2hGames.length})</strong>
            </div>
            {h2hGames.map((g, i) => {
              const dateStr = g.game_date || g.date;
              const dateDisplay = dateStr ? format(new Date(dateStr), 'MMM d, yyyy') : '';
              const isHomePerspective = g.perspective === 'home';
              const winnerLabel = g.result === 'W'
                ? (isHomePerspective ? homeLabel : awayLabel)
                : (isHomePerspective ? awayLabel : homeLabel);
              return (
                <div key={i} className="gd-h2h-game-row">
                  <span className="gd-h2h-game-date">{dateDisplay}</span>
                  <span className="gd-h2h-game-teams">
                    {awayAbbr} @ {homeAbbr}
                  </span>
                  <span className="gd-h2h-game-score">
                    {g.score_display}{g.overtime ? ' (OT)' : ''}
                  </span>
                  <span className={`gd-h2h-game-winner ${g.result === 'W' && isHomePerspective ? 'gd-form-win' : g.result === 'W' ? 'gd-form-loss' : isHomePerspective ? 'gd-form-loss' : 'gd-form-win'}`}>
                    {winnerLabel} W
                  </span>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Performance Analysis — H2H Matchups or Independent Scoring */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <BarChart3 size={16} />
          <h3>{h2hGames.length > 0 ? 'Head-to-Head Performance' : 'Independent Scoring'}</h3>
        </div>

        {h2hGames.length > 0 ? (
          <>
            {/* H2H Record boxes */}
            <div className="gd-perf-boxes">
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{awayLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-red)' }}>
                  {h2hGames.filter((g) => (g.perspective === 'away' && g.result === 'W') || (g.perspective === 'home' && g.result !== 'W')).length}W
                </span>
                <span className="gd-perf-box-sub">vs {homeAbbr}</span>
              </div>
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{homeLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-green)' }}>
                  {h2hGames.filter((g) => (g.perspective === 'home' && g.result === 'W') || (g.perspective === 'away' && g.result !== 'W')).length}W
                </span>
                <span className="gd-perf-box-sub">vs {awayAbbr}</span>
              </div>
            </div>

            {/* H2H Scoring Chart */}
            <div className="gd-chart-section">
              <h4 className="gd-chart-title">
                <TrendingUp size={13} />
                H2H Scoring
              </h4>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart
                  data={[...h2hGames].reverse().map((g, i) => {
                    const dateStr = g.game_date || g.date;
                    const label = dateStr ? format(new Date(dateStr), 'M/d') : `G${i + 1}`;
                    return {
                      name: label,
                      [homeLabel]: g.perspective === 'home' ? (g.goals_for ?? 0) : (g.goals_against ?? 0),
                      [awayLabel]: g.perspective === 'away' ? (g.goals_for ?? 0) : (g.goals_against ?? 0),
                    };
                  })}
                  margin={{ top: 5, right: 5, left: -20, bottom: 5 }}
                >
                  <XAxis dataKey="name" tick={{ fill: '#a0a0b8', fontSize: 11 }} />
                  <YAxis tick={{ fill: '#a0a0b8', fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                    labelStyle={{ color: '#e8e8f0' }}
                  />
                  <Legend wrapperStyle={{ fontSize: 11, color: '#a0a0b8' }} />
                  <Bar dataKey={awayLabel} fill="#ff5252" radius={[3, 3, 0, 0]} />
                  <Bar dataKey={homeLabel} fill="#00ff88" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </>
        ) : (
          <>
            {/* Independent recent form (no H2H data this season) */}
            <div className="gd-perf-boxes">
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{awayLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-red)' }}>
                  {calcRecord(last5Away)}
                </span>
                <span className="gd-perf-box-sub">Last 5 (all opponents)</span>
              </div>
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{homeLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-green)' }}>
                  {calcRecord(last5Home)}
                </span>
                <span className="gd-perf-box-sub">Last 5 (all opponents)</span>
              </div>
            </div>

            <div className="gd-chart-section">
              <h4 className="gd-chart-title">
                <TrendingUp size={13} />
                Goals Scored (Last 5, All Opponents)
              </h4>
              <ResponsiveContainer width="100%" height={200}>
                <BarChart
                  data={[...Array(Math.max(last5Home.length, last5Away.length))].map((_, i) => ({
                    name: `G${i + 1}`,
                    [homeLabel]: last5Home[last5Home.length - 1 - i]?.goals_for ?? 0,
                    [awayLabel]: last5Away[last5Away.length - 1 - i]?.goals_for ?? 0,
                  }))}
                  margin={{ top: 5, right: 5, left: -20, bottom: 5 }}
                >
                  <XAxis dataKey="name" tick={{ fill: '#a0a0b8', fontSize: 11 }} />
                  <YAxis tick={{ fill: '#a0a0b8', fontSize: 11 }} />
                  <Tooltip
                    contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                    labelStyle={{ color: '#e8e8f0' }}
                  />
                  <Legend wrapperStyle={{ fontSize: 11, color: '#a0a0b8' }} />
                  <Bar dataKey={awayLabel} fill="#ff5252" radius={[3, 3, 0, 0]} />
                  <Bar dataKey={homeLabel} fill="#00ff88" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </>
        )}
      </div>
    </div>
  );
}

/* ──────────────────── Predictions Tab ──────────────────── */
const MARKET_BET_TYPES = new Set(['ml', 'total', 'spread']);

function PredictionsTab({ game }) {
  const predictions = game.predictions || game.bets || [];
  const homeAbbr = teamAbbrev(game.home_team || game.home_team_form);
  const awayAbbr = teamAbbrev(game.away_team || game.away_team_form);

  if (predictions.length === 0) {
    return (
      <div className="tab-content">
        <div className="empty-state">
          <Target size={48} />
          <p>No predictions available for this game yet.</p>
        </div>
      </div>
    );
  }

  const market = predictions.filter((p) => MARKET_BET_TYPES.has(p.bet_type));
  const props = predictions.filter((p) => !MARKET_BET_TYPES.has(p.bet_type));
  const topPicks = market.filter((p) => p.recommended);
  const heavyJuice = market.filter((p) => p.is_fallback && !p.recommended);
  const otherMarket = market.filter((p) => !p.recommended && !p.is_fallback);

  return (
    <div className="tab-content predictions-tab">
      {topPicks.length > 0 && (
        <div className="predictions-section">
          <h3 className="predictions-section-title">
            <Target size={16} />
            Top Picks
          </h3>
          <div className="predictions-list">
            {topPicks.map((pred, index) => (
              <PredictionCard key={pred.id || index} prediction={pred} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
            ))}
          </div>
        </div>
      )}

      {heavyJuice.length > 0 && (
        <div className="predictions-section">
          <h3 className="predictions-section-title predictions-section-title-fallback">
            <AlertTriangle size={16} />
            Heavy Juice Picks
          </h3>
          <p className="predictions-section-desc">
            These picks have real edge but are on heavy favourite lines. Proceed with caution.
          </p>
          <div className="predictions-list">
            {heavyJuice.map((pred, index) => (
              <PredictionCard key={pred.id || index} prediction={pred} isFallback homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
            ))}
          </div>
        </div>
      )}

      {otherMarket.length > 0 && (
        <div className="predictions-section">
          <h3 className="predictions-section-title predictions-section-title-other">
            Market Analysis
          </h3>
          <div className="predictions-list">
            {otherMarket.map((pred, index) => (
              <PredictionCard key={pred.id || index} prediction={pred} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
            ))}
          </div>
        </div>
      )}

      {props.length > 0 && (
        <div className="predictions-section">
          <h3 className="predictions-section-title predictions-section-title-props">
            <Layers size={16} />
            Props
          </h3>
          <div className="predictions-list">
            {props.map((pred, index) => (
              <PredictionCard key={pred.id || `prop-${index}`} prediction={pred} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

/* ──────────────────── Main GameDetail Component ──────────────────── */
function GameDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('overview');
  const [injuries, setInjuries] = useState(null);

  const { data: game, loading, error, refetch, silentRefetch } = useApi(fetchGameDetails, [id]);

  // Fetch injuries
  useEffect(() => {
    if (!id) return;
    fetchGameInjuries(id)
      .then((res) => setInjuries(res.data))
      .catch(() => setInjuries(null));
  }, [id]);

  // Auto-poll for live games
  const isLive = game && isLiveStatus(game.status);
  const intervalRef = useRef(null);
  useEffect(() => {
    if (isLive) {
      intervalRef.current = setInterval(() => {
        silentRefetch();
      }, LIVE_POLL_INTERVAL);
    }
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [isLive, silentRefetch]);

  useWebSocketEvent('odds_update', useCallback((data) => {
    const changedIds = (data?.changed_games || []).map((g) => g.game_id);
    if (changedIds.includes(Number(id))) {
      silentRefetch();
    }
  }, [id, silentRefetch]));

  if (loading) {
    return (
      <div className="game-detail-page">
        <div className="loading-container large">
          <div className="loading-spinner"></div>
          <p>Loading game analysis...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="game-detail-page">
        <button className="btn btn-back" onClick={() => navigate(-1)}>
          <ArrowLeft size={18} />
          Back
        </button>
        <div className="error-container">
          <p>Failed to load game details: {error}</p>
        </div>
      </div>
    );
  }

  if (!game) {
    return (
      <div className="game-detail-page">
        <button className="btn btn-back" onClick={() => navigate(-1)}>
          <ArrowLeft size={18} />
          Back
        </button>
        <div className="empty-state">
          <p>Game not found</p>
        </div>
      </div>
    );
  }

  const awayForm = game.away_team_form || {};
  const homeForm = game.home_team_form || {};
  const awayTeamLabel = awayForm.team_name || teamName(game.away_team, 'Away');
  const homeTeamLabel = homeForm.team_name || teamName(game.home_team, 'Home');
  const awayAbbr = awayForm.abbreviation || teamAbbrev(game.away_team, 'AWY');
  const homeAbbr = homeForm.abbreviation || teamAbbrev(game.home_team, 'HME');
  const venue = game.venue || game.arena || '';

  // Confidence from top pick — use the same top_pick the dashboard uses
  const predictions = game.predictions || game.bets || [];
  const topPick = game.top_pick || predictions.find((p) => p.recommended) || predictions[0];
  const confidence = topPick ? confidencePct(topPick.confidence) : null;

  // Determine which team the AI picked
  const pickValue = (topPick?.prediction_value || '').toLowerCase();
  const detailPickBetType = (topPick?.bet_type || '').toLowerCase();
  const pickIsHome = pickValue === 'home' || pickValue.includes(homeAbbr.toLowerCase());
  const pickIsAway = pickValue === 'away' || pickValue.includes(awayAbbr.toLowerCase());
  const pickIsOver = detailPickBetType === 'total' && pickValue.includes('over');
  const pickIsUnder = detailPickBetType === 'total' && pickValue.includes('under');

  const TABS = [
    { id: 'overview', label: 'Overview', icon: BarChart3 },
    { id: 'predictions', label: 'Predictions', icon: Target },
  ];

  return (
    <div className="game-detail-page">
      <button className="btn btn-back" onClick={() => navigate(-1)}>
        <ArrowLeft size={18} />
        Back
      </button>

      {/* Game Header */}
      <GameHeader
        game={game}
        awayAbbr={awayAbbr}
        homeAbbr={homeAbbr}
        awayTeamLabel={awayTeamLabel}
        homeTeamLabel={homeTeamLabel}
        confidence={confidence}
        isLive={isLive}
        venue={venue}
        pickIsHome={pickIsHome}
        pickIsAway={pickIsAway}
      />

      {/* Odds Cards */}
      <OddsCards game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} pickBetType={detailPickBetType} pickIsHome={pickIsHome} pickIsAway={pickIsAway} pickIsOver={pickIsOver} pickIsUnder={pickIsUnder} />

      {/* Tab Navigation */}
      <div className="game-detail-tabs">
        {TABS.map((tab) => {
          const Icon = tab.icon;
          return (
            <button
              key={tab.id}
              className={`tab-btn ${activeTab === tab.id ? 'tab-active' : ''}`}
              onClick={() => setActiveTab(tab.id)}
            >
              <Icon size={16} />
              <span>{tab.label}</span>
            </button>
          );
        })}
      </div>

      {/* Tab Content */}
      <div className="gd-content">
        {activeTab === 'overview' ? (
          <>
            <GamePropsSection game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
            <KeyInjuries injuries={injuries} homeTeamLabel={homeTeamLabel} awayTeamLabel={awayTeamLabel} />
            <AIAnalysis game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} homeTeamLabel={homeTeamLabel} awayTeamLabel={awayTeamLabel} />
            <RiskAndMarket game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} homeTeamLabel={homeTeamLabel} awayTeamLabel={awayTeamLabel} />
            <StatsAndTrends game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
            <GoalieMatchup game={game} />
            <VenueSection game={game} />
            <RecentFormAndH2H game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
          </>
        ) : (
          <PredictionsTab game={game} />
        )}
      </div>
    </div>
  );
}

export default GameDetail;
