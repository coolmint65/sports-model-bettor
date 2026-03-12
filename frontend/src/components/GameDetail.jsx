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
        {/* Home team badge */}
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

        {/* VS divider or score */}
        <div className="gd-center">
          {isLive ? (
            <div className="gd-live-center">
              <div className="detail-live-badge">
                <Radio size={14} className="live-icon-pulse" />
                LIVE
              </div>
              <div className="detail-live-score">
                <span className={`detail-score ${game.home_score > game.away_score ? 'score-winning' : ''}`}>
                  {game.home_score ?? 0}
                </span>
                <span className="detail-score-sep">-</span>
                <span className={`detail-score ${game.away_score > game.home_score ? 'score-winning' : ''}`}>
                  {game.away_score ?? 0}
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
                <span className={`detail-score ${game.home_score > game.away_score ? 'score-winning' : ''}`}>
                  {game.home_score ?? 0}
                </span>
                <span className="detail-score-sep">-</span>
                <span className={`detail-score ${game.away_score > game.home_score ? 'score-winning' : ''}`}>
                  {game.away_score ?? 0}
                </span>
              </div>
            </div>
          ) : (
            <span className="gd-vs">VS</span>
          )}
        </div>

        {/* Away team badge */}
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
function OddsCards({ game, homeAbbr, awayAbbr }) {
  const odds = game.odds;
  if (!odds) return null;

  const isOT = game.period_type === 'OT' || game.period_type === 'SO';
  const live = isLiveStatus(game.status);
  const locked = live && isOT;

  return (
    <div className="gd-odds-row">
      {/* Moneyline */}
      {(odds.home_moneyline != null || odds.away_moneyline != null) && (
        <div className="gd-odds-card">
          <div className="gd-odds-card-header">
            <Target size={14} />
            <span>Moneyline</span>
          </div>
          <div className="gd-odds-card-body">
            <div className="gd-odds-side">
              <span className="gd-odds-team">{homeAbbr}</span>
              <span className="gd-odds-big">{formatAmericanOdds(odds.home_moneyline)}</span>
            </div>
            <span className="gd-odds-vs">VS</span>
            <div className="gd-odds-side">
              <span className="gd-odds-team">{awayAbbr}</span>
              <span className="gd-odds-big">{formatAmericanOdds(odds.away_moneyline)}</span>
            </div>
          </div>
        </div>
      )}

      {/* Spread */}
      {odds.home_spread_line != null && (
        <div className={`gd-odds-card ${locked ? 'gd-odds-locked' : ''}`}>
          <div className="gd-odds-card-header">
            <TrendingUp size={14} />
            <span>Spread</span>
          </div>
          {locked ? (
            <div className="gd-odds-locked-body"><Lock size={16} /></div>
          ) : (
            <div className="gd-odds-card-body">
              <div className="gd-odds-side">
                <span className="gd-odds-team">{homeAbbr}</span>
                <span className="gd-odds-big">
                  {odds.home_spread_line > 0 ? '+' : ''}{odds.home_spread_line}
                </span>
                <span className="gd-odds-price">({formatAmericanOdds(odds.home_spread_price)})</span>
              </div>
              <span className="gd-odds-vs">VS</span>
              <div className="gd-odds-side">
                <span className="gd-odds-team">{awayAbbr}</span>
                <span className="gd-odds-big">
                  {odds.away_spread_line != null ? ((odds.away_spread_line > 0 ? '+' : '') + odds.away_spread_line) : ''}
                </span>
                <span className="gd-odds-price">({formatAmericanOdds(odds.away_spread_price)})</span>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Total O/U */}
      {odds.over_under_line != null && (
        <div className={`gd-odds-card ${locked ? 'gd-odds-locked' : ''}`}>
          <div className="gd-odds-card-header">
            <Zap size={14} />
            <span>Total (O/U)</span>
          </div>
          {locked ? (
            <div className="gd-odds-locked-body"><Lock size={16} /></div>
          ) : (
            <div className="gd-odds-card-body">
              <div className="gd-odds-side">
                <span className="gd-odds-team">Over</span>
                <span className="gd-odds-big">{odds.over_under_line}</span>
                <span className="gd-odds-price">({formatAmericanOdds(odds.over_price)})</span>
              </div>
              <span className="gd-odds-vs">/</span>
              <div className="gd-odds-side">
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
  const topPick = predictions.find((p) => p.recommended) || predictions[0];
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

  // Build pick team name
  const pickValue = topPick.prediction_value || '';
  const pickTeam = pickValue.toLowerCase().includes('home') || pickValue.includes(homeAbbr)
    ? homeTeamLabel
    : pickValue.toLowerCase().includes('away') || pickValue.includes(awayAbbr)
      ? awayTeamLabel
      : pickValue;
  const pickSide = pickValue.toLowerCase().includes('home') || pickValue.includes(homeAbbr) ? '(Home)' : '(Away)';

  return (
    <div className="gd-section-card gd-analysis-card">
      <div className="gd-section-header">
        <Target size={16} />
        <h3>AI Match Analysis</h3>
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
  const homeImplied = homeML < 0 ? Math.abs(homeML) / (Math.abs(homeML) + 100) : 100 / (awayML + 100);
  const awayImplied = 1 - homeImplied;
  const homePct = Math.round(homeImplied * 100);
  const awayPct = Math.round(awayImplied * 100);

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
              risk.score <= 60 ? 'Very close spread - coin flip territory' :
                'High variance — proceed with caution.'}
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
  const homeRecord = `${home.wins || 0}-${home.losses || 0}`;
  const awayRecord = `${away.wins || 0}-${away.losses || 0}`;
  const homeWinPct = home.games_played ? `${Math.round((home.wins / home.games_played) * 100)}%` : '-';
  const awayWinPct = away.games_played ? `${Math.round((away.wins / away.games_played) * 100)}%` : '-';

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

  const perfStats = [
    { label: 'GF/G', homeVal: fmtStat(home.goals_for_per_game), awayVal: fmtStat(away.goals_for_per_game) },
    { label: 'GAA', homeVal: fmtStat(home.goals_against_per_game), awayVal: fmtStat(away.goals_against_per_game) },
    { label: 'PP%', homeVal: fmtStat(home.power_play_pct, true), awayVal: fmtStat(away.power_play_pct, true) },
    { label: 'PK%', homeVal: fmtStat(home.penalty_kill_pct, true), awayVal: fmtStat(away.penalty_kill_pct, true) },
    { label: 'SF/G', homeVal: fmtStat(home.shots_for_per_game), awayVal: fmtStat(away.shots_for_per_game) },
    { label: 'SA/G', homeVal: fmtStat(home.shots_against_per_game), awayVal: fmtStat(away.shots_against_per_game) },
    { label: 'FO%', homeVal: fmtStat(home.faceoff_win_pct, true), awayVal: fmtStat(away.faceoff_win_pct, true) },
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
            {homeLogo && <img src={homeLogo} alt="" width={28} height={28} className="gd-stats-logo" onError={(e) => { e.target.style.display = 'none'; }} />}
            <div className="gd-stats-team-name">{homeLabel}</div>
            <div className="gd-stats-record">{homeRecord}</div>
            <div className="gd-stats-badges">
              <span className="gd-streak-badge">
                <TrendingDown size={11} />
                {getStreak(homeRecent)}
              </span>
              {homeRank !== '-' && (
                <span className="gd-rank-badge">
                  <Award size={11} />
                  #{homeRank}
                </span>
              )}
            </div>
            <div className="gd-stats-winpct">Win%: {homeWinPct}</div>
          </div>
          <div className="gd-stats-team">
            {awayLogo && <img src={awayLogo} alt="" width={28} height={28} className="gd-stats-logo" onError={(e) => { e.target.style.display = 'none'; }} />}
            <div className="gd-stats-team-name">{awayLabel}</div>
            <div className="gd-stats-record">{awayRecord}</div>
            <div className="gd-stats-badges">
              <span className="gd-streak-badge">
                <TrendingDown size={11} />
                {getStreak(awayRecent)}
              </span>
              {awayRank !== '-' && (
                <span className="gd-rank-badge">
                  <Award size={11} />
                  #{awayRank}
                </span>
              )}
            </div>
            <div className="gd-stats-winpct">Win%: {awayWinPct}</div>
          </div>
        </div>

        <div className="gd-perf-section">
          <h4 className="gd-perf-title">KEY PERFORMANCE STATS</h4>
          <div className="gd-perf-grid">
            <div className="gd-perf-col">
              <div className="gd-perf-team-label">{homeLabel}</div>
              {perfStats.map((s) => (
                <div key={s.label} className="gd-perf-row">
                  <span className="gd-perf-stat-label">{s.label}</span>
                  <span className="gd-perf-stat-val">{s.homeVal}</span>
                </div>
              ))}
            </div>
            <div className="gd-perf-col">
              <div className="gd-perf-team-label">{awayLabel}</div>
              {perfStats.map((s) => (
                <div key={s.label} className="gd-perf-row">
                  <span className="gd-perf-stat-label">{s.label}</span>
                  <span className="gd-perf-stat-val">{s.awayVal}</span>
                </div>
              ))}
            </div>
          </div>
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
            </div>
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
            </div>
          </div>
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

  const homeId = game.home_team_form?.team_id ?? game.home_team?.id;
  const team1IsHome = h2h?.team1_id === homeId;

  // Find H2H matchups by filtering each team's recent games for the opponent
  const h2hFromHome = homeForm.filter((g) => {
    const opp = (g.opponent_abbrev || g.opponent_name || '').toUpperCase();
    return opp === awayAbbr.toUpperCase();
  });
  const h2hFromAway = awayForm.filter((g) => {
    const opp = (g.opponent_abbrev || g.opponent_name || '').toUpperCase();
    return opp === homeAbbr.toUpperCase();
  });

  // Merge and deduplicate H2H games by date, preferring home team's perspective
  const h2hGamesMap = new Map();
  for (const g of h2hFromHome) {
    const key = g.game_date || g.date;
    if (key) h2hGamesMap.set(key, { ...g, perspective: 'home' });
  }
  for (const g of h2hFromAway) {
    const key = g.game_date || g.date;
    if (key && !h2hGamesMap.has(key)) h2hGamesMap.set(key, { ...g, perspective: 'away' });
  }
  const h2hGames = [...h2hGamesMap.values()]
    .sort((a, b) => new Date(b.game_date || b.date) - new Date(a.game_date || a.date))
    .slice(0, 5);

  return (
    <div className="gd-two-col">
      {/* Recent Form & H2H */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <Users size={16} />
          <h3>Recent Form & Head-to-Head</h3>
        </div>

        {/* Recent Form */}
        <div className="gd-form-teams">
          <div className="gd-form-team">
            <div className="gd-form-team-header">
              <TrendingDown size={13} />
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

          <div className="gd-form-team">
            <div className="gd-form-team-header">
              <TrendingDown size={13} />
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

        {/* H2H Summary */}
        {h2h && (
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
                  {team1IsHome ? h2h.team1_wins : h2h.team2_wins}
                </span>
                <span>{homeLabel}</span>
              </div>
              <span className="gd-h2h-vs">vs</span>
              <div className="gd-h2h-side">
                <span className="gd-h2h-big" style={{ color: 'var(--accent-blue)' }}>
                  {team1IsHome ? h2h.team2_wins : h2h.team1_wins}
                </span>
                <span>{awayLabel}</span>
              </div>
            </div>

            {h2h.last_meeting && (
              <div className="gd-h2h-last">Last meeting: {h2h.last_meeting}</div>
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
                    {homeAbbr} vs {awayAbbr}
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

      {/* Performance Analysis — H2H Matchups */}
      <div className="gd-section-card">
        <div className="gd-section-header">
          <BarChart3 size={16} />
          <h3>Performance Analysis</h3>
        </div>

        {h2hGames.length > 0 ? (
          <>
            {/* H2H Record boxes */}
            <div className="gd-perf-boxes">
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{homeLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-green)' }}>
                  {h2hGames.filter((g) => (g.perspective === 'home' && g.result === 'W') || (g.perspective === 'away' && g.result !== 'W')).length}W
                </span>
                <span className="gd-perf-box-sub">vs {awayAbbr}</span>
              </div>
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{awayLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-red)' }}>
                  {h2hGames.filter((g) => (g.perspective === 'away' && g.result === 'W') || (g.perspective === 'home' && g.result !== 'W')).length}W
                </span>
                <span className="gd-perf-box-sub">vs {homeAbbr}</span>
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
                  <Bar dataKey={homeLabel} fill="#00ff88" radius={[3, 3, 0, 0]} />
                  <Bar dataKey={awayLabel} fill="#ff5252" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>
          </>
        ) : (
          <>
            {/* Fallback: show independent recent games if no H2H data */}
            <div className="gd-perf-boxes">
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{homeLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-green)' }}>
                  {calcRecord(last5Home)}
                </span>
                <span className="gd-perf-box-sub">Last 5</span>
              </div>
              <div className="gd-perf-box">
                <span className="gd-perf-box-label">{awayLabel}</span>
                <span className="gd-perf-box-record" style={{ color: 'var(--accent-red)' }}>
                  {calcRecord(last5Away)}
                </span>
                <span className="gd-perf-box-sub">Last 5</span>
              </div>
            </div>

            <div className="gd-chart-section">
              <h4 className="gd-chart-title">
                <TrendingUp size={13} />
                Recent Scoring
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
                  <Bar dataKey={homeLabel} fill="#00ff88" radius={[3, 3, 0, 0]} />
                  <Bar dataKey={awayLabel} fill="#ff5252" radius={[3, 3, 0, 0]} />
                </BarChart>
              </ResponsiveContainer>
            </div>

            <div className="gd-h2h-warning" style={{ marginTop: '0.75rem' }}>
              No head-to-head matchups found in recent games. Showing independent form.
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

  // Confidence from top pick
  const predictions = game.predictions || game.bets || [];
  const topPick = predictions.find((p) => p.recommended) || predictions[0];
  const confidence = topPick ? confidencePct(topPick.confidence) : null;

  // Determine which team the AI picked
  const pickValue = (topPick?.prediction_value || '').toLowerCase();
  const pickIsHome = pickValue === 'home' || pickValue.includes(homeAbbr.toLowerCase());
  const pickIsAway = pickValue === 'away' || pickValue.includes(awayAbbr.toLowerCase());

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
      <OddsCards game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />

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
            <KeyInjuries injuries={injuries} homeTeamLabel={homeTeamLabel} awayTeamLabel={awayTeamLabel} />
            <AIAnalysis game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} homeTeamLabel={homeTeamLabel} awayTeamLabel={awayTeamLabel} />
            <RiskAndMarket game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} homeTeamLabel={homeTeamLabel} awayTeamLabel={awayTeamLabel} />
            <StatsAndTrends game={game} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
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
