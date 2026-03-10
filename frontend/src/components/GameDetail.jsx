import { useState, useEffect, useRef, useCallback } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeft, Clock, MapPin, BarChart3, Target, TrendingUp,
  Activity, DollarSign, Radio, Shield, Zap, CheckCircle,
  AlertTriangle, Info, Users, Star, ChevronRight, Cloud,
  HeartPulse, GitBranch,
} from 'lucide-react';
import { format, formatDistanceToNowStrict } from 'date-fns';
import { BarChart, Bar, LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';
import { fetchGameDetails, fetchLineMovement, fetchGameInjuries } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { useWebSocketEvent } from '../hooks/useWebSocket';
import { teamName, teamAbbrev, teamLogo, parseAsUTC, isLiveStatus, confidencePct, formatBetType, formatPredictionValue } from '../utils/teams';
import { formatAmericanOddsOrDash, formatAmericanOdds, getConfidenceColor } from '../utils/formatting';

const LIVE_POLL_INTERVAL = 30_000;

/* ════════════════════════════════════════════════════════════
   Utility functions
   ════════════════════════════════════════════════════════════ */

function americanToImplied(odds) {
  if (odds == null) return null;
  if (odds < 0) return Math.abs(odds) / (Math.abs(odds) + 100);
  return 100 / (odds + 100);
}

function getConfidenceRating(conf) {
  if (conf >= 75) return { label: 'EXCELLENT', cls: 'gd-rating-excellent' };
  if (conf >= 65) return { label: 'GOOD', cls: 'gd-rating-good' };
  if (conf >= 55) return { label: 'FAIR', cls: 'gd-rating-fair' };
  return { label: 'CAUTION', cls: 'gd-rating-caution' };
}

function computeRisk(conf, edge) {
  if (conf >= 70 && edge >= 7) return { score: Math.max(20, 35 - edge), level: 'Low', color: '#00ff88' };
  if (conf >= 60 && edge >= 5) return { score: Math.max(40, 55 - edge), level: 'Medium', color: '#ffd700' };
  return { score: Math.min(80, 60 + (100 - conf) * 0.25), level: 'High', color: '#ff5252' };
}

function getSuggestedStake(conf, edge) {
  if (conf >= 72 && edge >= 8) return 'Heavy';
  if (conf >= 62 && edge >= 5) return 'Medium';
  return 'Light';
}

function generateFactors(homeForm, awayForm, h2h, homeRecent, awayRecent) {
  const factors = [];
  const hGP = (homeForm.wins || 0) + (homeForm.losses || 0) + (homeForm.ot_losses || 0);
  const aGP = (awayForm.wins || 0) + (awayForm.losses || 0) + (awayForm.ot_losses || 0);

  if (hGP > 0 && (homeForm.wins / hGP) >= 0.55) {
    factors.push({ text: `${homeForm.team_name}'s overall strong season record (${homeForm.wins}-${homeForm.losses})`, type: 'positive' });
  }
  if (aGP > 0 && (awayForm.wins / aGP) >= 0.55) {
    factors.push({ text: `${awayForm.team_name}'s strong season record (${awayForm.wins}-${awayForm.losses})`, type: 'positive' });
  }
  if (hGP > 0 && (homeForm.wins / hGP) < 0.42) {
    factors.push({ text: `${homeForm.team_name}'s struggling season (${homeForm.wins}-${homeForm.losses})`, type: 'warning' });
  }
  if (aGP > 0 && (awayForm.wins / aGP) < 0.42) {
    factors.push({ text: `${awayForm.team_name}'s struggling season (${awayForm.wins}-${awayForm.losses})`, type: 'warning' });
  }

  const hLast5 = (homeRecent || []).slice(0, 5);
  const aLast5 = (awayRecent || []).slice(0, 5);
  const hW = hLast5.filter(g => g.result === 'W').length;
  const hL = hLast5.length - hW;
  const aW = aLast5.filter(g => g.result === 'W').length;
  const aL = aLast5.length - aW;

  if (hLast5.length >= 3) {
    if (hW >= 4) factors.push({ text: `${homeForm.team_name}'s hot streak in recent form (${hW}W-${hL}L)`, type: 'trend' });
    else if (hW <= 1) factors.push({ text: `${homeForm.team_name}'s current losing streak in recent form (${hW}W-${hL}L)`, type: 'trend' });
  }
  if (aLast5.length >= 3) {
    if (aW >= 4) factors.push({ text: `${awayForm.team_name}'s hot recent form (${aW}W-${aL}L)`, type: 'trend' });
    else if (aW <= 1) factors.push({ text: `${awayForm.team_name}'s poor recent form (${aW}W-${aL}L)`, type: 'trend' });
    else if (aW >= 3) factors.push({ text: `${awayForm.team_name}'s positive recent form (${aW}W-${aL}L)`, type: 'positive' });
  }

  if (h2h && h2h.games_played >= 2) {
    const homeIsT1 = h2h.team1_id === homeForm.team_id;
    const homeH2H = homeIsT1 ? h2h.team1_wins : h2h.team2_wins;
    const awayH2H = homeIsT1 ? h2h.team2_wins : h2h.team1_wins;
    if (awayH2H > homeH2H) {
      factors.push({ text: `${awayForm.team_name} winning ${awayH2H > 1 ? `${awayH2H}` : 'recent'} head-to-head matchups`, type: 'warning' });
    } else if (homeH2H > awayH2H) {
      factors.push({ text: `${homeForm.team_name} dominant in head-to-head matchups`, type: 'positive' });
    }
  }

  return factors.slice(0, 6);
}

/* ════════════════════════════════════════════════════════════
   Section Components
   ════════════════════════════════════════════════════════════ */

function GameHeader({ game, homeForm, awayForm, homeAbbr, awayAbbr, topPred, venue }) {
  const isLive = isLiveStatus(game.status);
  const isFinal = game.status === 'final';
  const conf = topPred ? confidencePct(topPred.confidence) : null;
  const rating = conf != null ? getConfidenceRating(conf) : null;

  let dateStr = '', timeStr = '';
  try {
    const dt = parseAsUTC(game.start_time);
    if (dt && !isNaN(dt.getTime())) {
      dateStr = format(dt, 'EEEE, MMMM d, yyyy');
      timeStr = format(dt, 'h:mm a');
    }
  } catch {}

  return (
    <div className="gd-header">
      <div className="gd-header-teams">
        <div className="gd-header-team">
          <div className="gd-team-box">
            {homeForm.logo_url ? (
              <img src={homeForm.logo_url} alt="" width={40} height={40} onError={e => e.target.style.display='none'} />
            ) : (
              <span className="gd-team-box-abbr">{homeAbbr}</span>
            )}
          </div>
          <div className="gd-team-label">{homeForm.team_name || 'Home'}</div>
          <div className="gd-team-role">Home</div>
        </div>

        <div className="gd-header-center">
          {isLive ? (
            <div className="gd-live-center">
              <div className="gd-live-badge"><Radio size={14} /> LIVE</div>
              <div className="gd-live-score">
                <span className={game.away_score > game.home_score ? 'gd-score-lead' : ''}>{game.away_score ?? 0}</span>
                <span className="gd-score-sep">-</span>
                <span className={game.home_score > game.away_score ? 'gd-score-lead' : ''}>{game.home_score ?? 0}</span>
              </div>
            </div>
          ) : isFinal ? (
            <div className="gd-final-center">
              <div className="gd-final-badge">Final{game.overtime ? ' (OT)' : ''}</div>
              <div className="gd-live-score">
                <span className={game.away_score > game.home_score ? 'gd-score-lead' : ''}>{game.away_score ?? 0}</span>
                <span className="gd-score-sep">-</span>
                <span className={game.home_score > game.away_score ? 'gd-score-lead' : ''}>{game.home_score ?? 0}</span>
              </div>
            </div>
          ) : (
            <span className="gd-vs-text">VS</span>
          )}
        </div>

        <div className="gd-header-team">
          <div className="gd-team-box">
            {awayForm.logo_url ? (
              <img src={awayForm.logo_url} alt="" width={40} height={40} onError={e => e.target.style.display='none'} />
            ) : (
              <span className="gd-team-box-abbr">{awayAbbr}</span>
            )}
          </div>
          <div className="gd-team-label">{awayForm.team_name || 'Away'}</div>
          <div className="gd-team-role">Away</div>
        </div>
      </div>

      <div className="gd-header-meta">
        <div className="gd-header-badges">
          <span className="gc-badge gc-badge-sport">Hockey</span>
          {rating && (
            <span className={`gc-badge ${rating.cls}`}>
              <TrendingUp size={12} /> {rating.label} - {Math.round(conf)}%
            </span>
          )}
        </div>
        <div className="gd-header-info">
          {dateStr && <div><Calendar size={14} /> {dateStr}</div>}
          {timeStr && <div><Clock size={14} /> {timeStr}</div>}
          {venue && <div><MapPin size={14} /> {venue}</div>}
        </div>
      </div>
    </div>
  );
}

function OddsSection({ odds, homeAbbr, awayAbbr }) {
  if (!odds) return null;
  return (
    <div className="gd-odds-row">
      {(odds.home_moneyline != null || odds.away_moneyline != null) && (
        <div className="gd-odds-card">
          <div className="gd-odds-card-title"><Target size={14} /> Moneyline</div>
          <div className="gd-odds-card-body">
            <div className="gd-odds-side">
              <span className="gd-odds-team">{homeAbbr}</span>
              <span className="gd-odds-val gd-odds-primary">{formatAmericanOddsOrDash(odds.home_moneyline)}</span>
            </div>
            <span className="gd-odds-vs">VS</span>
            <div className="gd-odds-side">
              <span className="gd-odds-team">{awayAbbr}</span>
              <span className="gd-odds-val gd-odds-primary">{formatAmericanOddsOrDash(odds.away_moneyline)}</span>
            </div>
          </div>
        </div>
      )}
      {odds.home_spread_line != null && (
        <div className="gd-odds-card">
          <div className="gd-odds-card-title"><Activity size={14} /> Spread</div>
          <div className="gd-odds-card-body">
            <div className="gd-odds-side">
              <span className="gd-odds-team">{homeAbbr}</span>
              <span className="gd-odds-val gd-odds-primary">{odds.home_spread_line > 0 ? '+' : ''}{odds.home_spread_line}</span>
              {odds.home_spread_price != null && <span className="gd-odds-price">({formatAmericanOddsOrDash(odds.home_spread_price)})</span>}
            </div>
            <span className="gd-odds-vs">VS</span>
            <div className="gd-odds-side">
              <span className="gd-odds-team">{awayAbbr}</span>
              <span className="gd-odds-val gd-odds-primary">{odds.away_spread_line > 0 ? '+' : ''}{odds.away_spread_line}</span>
              {odds.away_spread_price != null && <span className="gd-odds-price">({formatAmericanOddsOrDash(odds.away_spread_price)})</span>}
            </div>
          </div>
        </div>
      )}
      {odds.over_under_line != null && (
        <div className="gd-odds-card">
          <div className="gd-odds-card-title"><Zap size={14} /> Total (O/U)</div>
          <div className="gd-odds-card-body">
            <div className="gd-odds-side">
              <span className="gd-odds-team">Over</span>
              <span className="gd-odds-val gd-odds-primary">{odds.over_under_line}</span>
              {odds.over_price != null && <span className="gd-odds-price">({formatAmericanOddsOrDash(odds.over_price)})</span>}
            </div>
            <span className="gd-odds-vs">/</span>
            <div className="gd-odds-side">
              <span className="gd-odds-team">Under</span>
              <span className="gd-odds-val gd-odds-primary">{odds.over_under_line}</span>
              {odds.under_price != null && <span className="gd-odds-price">({formatAmericanOddsOrDash(odds.under_price)})</span>}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function MatchAnalysis({ topPred, predictions, homeForm, awayForm, h2h, homeRecent, awayRecent, homeAbbr, awayAbbr }) {
  const hasPred = predictions && predictions.length > 0;
  const recommended = predictions?.find(p => p.recommended);
  const pick = recommended || topPred || (predictions && predictions[0]);
  if (!pick) return null;

  const conf = confidencePct(pick.confidence);
  const edge = confidencePct(pick.edge);
  const risk = computeRisk(conf, edge);
  const stake = getSuggestedStake(conf, edge);
  const isQualified = pick.recommended || (conf >= 58 && edge >= 5);
  const factors = generateFactors(homeForm, awayForm, h2h, homeRecent, awayRecent);
  const reasoning = pick.reasoning || '';

  // Derive pick team name
  const pickValue = formatPredictionValue(pick.prediction_value, homeAbbr, awayAbbr);
  const betType = formatBetType(pick.bet_type);
  const isHome = pick.prediction_value === 'home' || (pick.prediction_value && pick.prediction_value.includes(homeAbbr));
  const pickTeamName = isHome ? homeForm.team_name : awayForm.team_name;

  return (
    <div className="gd-section gd-analysis">
      <div className="gd-section-header">
        <h3><BarChart3 size={18} /> AI Match Analysis</h3>
        {isQualified ? (
          <span className="gd-badge-qualified"><CheckCircle size={14} /> QUALIFIED BET</span>
        ) : (
          <span className="gd-badge-nobet">No Qualified Bet</span>
        )}
      </div>

      {/* Main narrative */}
      {reasoning && (
        <div className="gd-narrative">
          <p>{reasoning}</p>
        </div>
      )}

      {/* Key Insight */}
      {reasoning && (
        <div className="gd-callout gd-callout-gold">
          <Star size={16} />
          <div>
            <strong>Key Insight:</strong> {
              edge >= 5
                ? `The model sees ${edge.toFixed(1)}% edge on this line — the market may be undervaluing ${pickTeamName || pickValue}.`
                : `Close market line with limited edge. Proceed with caution.`
            }
          </div>
        </div>
      )}

      {/* Analysis Factors */}
      {factors.length > 0 && (
        <>
          <h4 className="gd-factors-title"><CheckCircle size={14} /> Analysis Factors</h4>
          <div className="gd-factors-grid">
            {factors.map((f, i) => (
              <div key={i} className={`gd-factor gd-factor-${f.type}`}>
                {f.type === 'positive' && <CheckCircle size={13} />}
                {f.type === 'trend' && <TrendingUp size={13} />}
                {f.type === 'warning' && <AlertTriangle size={13} />}
                {f.text}
              </div>
            ))}
          </div>
        </>
      )}

      {/* AI Confidence */}
      <div className="gd-confidence-row">
        <div className="gd-confidence-big">
          <span className="gd-conf-pct">{Math.round(conf)}%</span>
          <span className="gd-conf-label"><Info size={12} /> AI Confidence</span>
        </div>
        <span className={`gd-risk-badge gd-risk-${risk.level.toLowerCase()}`}>{risk.level} Risk</span>
      </div>

      {/* Suggested Action */}
      {isQualified && (
        <div className="gd-action">
          <div className="gd-action-title"><TrendingUp size={16} /> Suggested Action</div>
          <div className="gd-action-grid">
            <div className="gd-action-cell">
              <span className="gd-action-label">Pick</span>
              <span className="gd-action-value">{pickValue}</span>
              <span className="gd-action-sub">{betType}</span>
            </div>
            <div className="gd-action-cell">
              <span className="gd-action-label">Confidence</span>
              <span className="gd-action-value">{Math.round(conf)}%</span>
            </div>
            <div className="gd-action-cell">
              <span className="gd-action-label">Risk Level</span>
              <span className="gd-action-value" style={{ color: risk.color }}>{risk.level}</span>
            </div>
            <div className="gd-action-cell">
              <span className="gd-action-label">Suggested Stake</span>
              <span className="gd-action-value">{stake}</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

function RiskAssessment({ conf, edge, odds }) {
  const risk = computeRisk(conf || 0, edge || 0);
  const score = Math.round(risk.score);
  let description = 'Limited data available';
  if (conf >= 70 && edge >= 7) description = 'Strong model confidence with significant edge';
  else if (conf >= 60 && edge >= 5) description = 'Moderate confidence with reasonable edge';
  else if (odds?.home_spread_line && Math.abs(odds.home_spread_line) <= 1.5) description = 'Very close spread - coin flip territory';
  else description = 'Lower confidence or minimal edge';

  return (
    <div className="gd-section gd-risk">
      <h3><Shield size={16} /> Risk Assessment</h3>
      <div className="gd-risk-header">
        <span>Risk Level: {risk.level}</span>
        <span>{score}/100</span>
      </div>
      <div className="gd-risk-bar">
        <div className="gd-risk-fill" style={{ width: `${score}%`, backgroundColor: risk.color }} />
      </div>
      <p className="gd-risk-desc"><AlertTriangle size={13} /> {description}</p>
    </div>
  );
}

function MarketInterest({ odds, homeForm, awayForm }) {
  const homeImpl = americanToImplied(odds?.home_moneyline);
  const awayImpl = americanToImplied(odds?.away_moneyline);
  if (!homeImpl && !awayImpl) return (
    <div className="gd-section gd-market">
      <h3><TrendingUp size={16} /> Market Interest Index</h3>
      <p className="gd-muted">No odds data available</p>
    </div>
  );
  const total = (homeImpl || 0.5) + (awayImpl || 0.5);
  const homePct = Math.round(((homeImpl || 0.5) / total) * 100);
  const awayPct = 100 - homePct;
  const underdog = homePct < awayPct ? homeForm : awayForm;
  const underdogPct = Math.min(homePct, awayPct);

  return (
    <div className="gd-section gd-market">
      <h3><TrendingUp size={16} /> Market Interest Index</h3>
      <div className="gd-market-pcts">
        <div className="gd-market-team">
          <span className="gd-market-abbr">{homeForm.abbreviation}</span>
          <span className="gd-market-pct">{homePct}%</span>
        </div>
        <div className="gd-market-team">
          <span className="gd-market-abbr">{awayForm.abbreviation}</span>
          <span className="gd-market-pct">{awayPct}%</span>
        </div>
      </div>
      {underdogPct >= 25 && (
        <div className="gd-market-insight">
          <Info size={14} />
          <div>
            <strong>Clear underdog value on {underdog.team_name}</strong>
            <p>Derived from league importance, team popularity, and event timing.</p>
          </div>
        </div>
      )}
    </div>
  );
}

function SeasonStatsCard({ homeForm, awayForm }) {
  const homeRecord = `${homeForm.wins || 0}-${homeForm.losses || 0}`;
  const awayRecord = `${awayForm.wins || 0}-${awayForm.losses || 0}`;
  const hGP = (homeForm.wins || 0) + (homeForm.losses || 0) + (homeForm.ot_losses || 0);
  const aGP = (awayForm.wins || 0) + (awayForm.losses || 0) + (awayForm.ot_losses || 0);
  const hWinPct = hGP > 0 ? Math.round((homeForm.wins / hGP) * 100) : 0;
  const aWinPct = aGP > 0 ? Math.round((awayForm.wins / aGP) * 100) : 0;

  // Streak from recent record
  const parseStreak = (rec) => {
    if (!rec) return null;
    const match = rec.match(/^(\d+)-(\d+)/);
    if (!match) return null;
    const w = parseInt(match[1]), l = parseInt(match[2]);
    if (w > l) return { label: `W${w - l > 1 ? w : ''}`, cls: 'gd-streak-w' };
    if (l > w) return { label: `L${l - w > 1 ? l : ''}`, cls: 'gd-streak-l' };
    return null;
  };

  const hStreak = parseStreak(homeForm.record_last_5);
  const aStreak = parseStreak(awayForm.record_last_5);

  const stats = [
    { label: 'GF/G', home: homeForm.goals_for_per_game, away: awayForm.goals_for_per_game, higher: true },
    { label: 'GAA', home: homeForm.goals_against_per_game, away: awayForm.goals_against_per_game, higher: false },
    { label: 'PP%', home: homeForm.power_play_pct, away: awayForm.power_play_pct, higher: true },
    { label: 'PK%', home: homeForm.penalty_kill_pct, away: awayForm.penalty_kill_pct, higher: true },
  ];

  return (
    <div className="gd-section gd-stats">
      <h3><BarChart3 size={16} /> Season Stats & Standings</h3>
      <div className="gd-stats-teams">
        <div className="gd-stats-team">
          <span className="gd-stats-name">{homeForm.team_name}</span>
          <span className="gd-stats-record">{homeRecord}</span>
          <div className="gd-stats-badges">
            {hStreak && <span className={`gd-mini-badge ${hStreak.cls}`}>{hStreak.label}</span>}
          </div>
          <span className="gd-stats-winpct">Win%: {hWinPct}%</span>
        </div>
        <div className="gd-stats-team">
          <span className="gd-stats-name">{awayForm.team_name}</span>
          <span className="gd-stats-record">{awayRecord}</span>
          <div className="gd-stats-badges">
            {aStreak && <span className={`gd-mini-badge ${aStreak.cls}`}>{aStreak.label}</span>}
          </div>
          <span className="gd-stats-winpct">Win%: {aWinPct}%</span>
        </div>
      </div>
      <div className="gd-key-stats">
        <h4>KEY PERFORMANCE STATS</h4>
        <div className="gd-key-stats-grid">
          <div className="gd-key-stats-col">
            <span className="gd-key-stats-header">{homeForm.team_name}</span>
            {stats.map(s => (
              <div key={s.label} className="gd-stat-row">
                <span className="gd-stat-label">{s.label}</span>
                <span className={`gd-stat-val ${s.home != null && s.away != null && ((s.higher && s.home > s.away) || (!s.higher && s.home < s.away)) ? 'gd-stat-better' : ''}`}>
                  {s.home != null ? (typeof s.home === 'number' ? s.home.toFixed(s.label.includes('%') ? 1 : 2) : s.home) : '-'}
                </span>
              </div>
            ))}
          </div>
          <div className="gd-key-stats-col">
            <span className="gd-key-stats-header">{awayForm.team_name}</span>
            {stats.map(s => (
              <div key={s.label} className="gd-stat-row">
                <span className="gd-stat-label">{s.label}</span>
                <span className={`gd-stat-val ${s.home != null && s.away != null && ((s.higher && s.away > s.home) || (!s.higher && s.away < s.home)) ? 'gd-stat-better' : ''}`}>
                  {s.away != null ? (typeof s.away === 'number' ? s.away.toFixed(s.label.includes('%') ? 1 : 2) : s.away) : '-'}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function BettingTrendsCard({ homeForm, awayForm, homeRecent, awayRecent }) {
  const homeRec = homeForm.home_record || homeForm.away_record;
  const overallHome = `${homeForm.wins || 0}-${homeForm.losses || 0}-${homeForm.ot_losses || 0}`;

  return (
    <div className="gd-section gd-trends">
      <h3><TrendingUp size={16} /> Betting Trends</h3>
      <div className="gd-trends-teams">
        <div className="gd-trends-team">
          <span className="gd-trends-name">{homeForm.team_name}</span>
          {homeRec && <div className="gd-trends-ats">Record: <strong>{homeRec}</strong></div>}
          <div className="gd-trends-overall">Overall record {overallHome}</div>
        </div>
        <div className="gd-trends-team">
          <span className="gd-trends-name">{awayForm.team_name}</span>
          {awayForm.away_record && <div className="gd-trends-ats">Away: <strong>{awayForm.away_record}</strong></div>}
          <div className="gd-trends-overall">Overall record {awayForm.wins || 0}-{awayForm.losses || 0}-{awayForm.ot_losses || 0}</div>
        </div>
      </div>
    </div>
  );
}

function VenueSection({ venue }) {
  if (!venue) return null;
  return (
    <div className="gd-section gd-venue">
      <h3><MapPin size={16} /> Venue & Conditions</h3>
      <div className="gd-venue-grid">
        <div className="gd-venue-item">
          <MapPin size={18} />
          <div>
            <span className="gd-venue-label">Venue</span>
            <strong>{venue}</strong>
          </div>
        </div>
        <div className="gd-venue-item">
          <Cloud size={18} />
          <div>
            <span className="gd-venue-label">Setting</span>
            <strong>Indoor</strong>
          </div>
        </div>
      </div>
    </div>
  );
}

function FormAndH2H({ homeForm, awayForm, h2h, homeRecent, awayRecent }) {
  const hLast5 = (homeRecent || []).slice(0, 5);
  const aLast5 = (awayRecent || []).slice(0, 5);
  const hW = hLast5.filter(g => g.result === 'W').length;
  const hL = hLast5.length - hW;
  const aW = aLast5.filter(g => g.result === 'W').length;
  const aL = aLast5.length - aW;
  const hLast3 = (homeRecent || []).slice(0, 3);
  const aLast3 = (awayRecent || []).slice(0, 3);

  const homeIsT1 = h2h ? h2h.team1_id === homeForm.team_id : true;
  const homeH2HWins = h2h ? (homeIsT1 ? h2h.team1_wins : h2h.team2_wins) : 0;
  const awayH2HWins = h2h ? (homeIsT1 ? h2h.team2_wins : h2h.team1_wins) : 0;

  return (
    <div className="gd-section gd-form">
      <h3><Users size={16} /> Recent Form & Head-to-Head</h3>

      {/* Last 5 W/L pills */}
      <div className="gd-form-teams">
        <div className="gd-form-team">
          <div className="gd-form-team-header">
            <TrendingUp size={14} /> {homeForm.team_name}
            <span className={`gd-form-record ${hW > hL ? 'gd-form-pos' : 'gd-form-neg'}`}>{hW}-{hL} Last 5</span>
          </div>
          <div className="gd-form-pills">
            {hLast5.map((g, i) => (
              <span key={i} className={`gd-form-pill ${g.result === 'W' ? 'gd-pill-w' : g.result === 'OTL' ? 'gd-pill-otl' : 'gd-pill-l'}`}>
                {g.result}
              </span>
            ))}
          </div>
          <div className="gd-form-games">
            {hLast3.map((g, i) => (
              <div key={i} className="gd-form-game">
                <span>{g.home_away === 'away' ? '@' : 'vs'} {g.opponent_abbrev || g.opponent_name}</span>
                <span className={g.result === 'W' ? 'gd-form-w' : 'gd-form-l'}>{g.result} {g.score_display}{g.overtime ? ' (OT)' : ''}</span>
              </div>
            ))}
          </div>
        </div>
        <div className="gd-form-team">
          <div className="gd-form-team-header">
            <TrendingUp size={14} /> {awayForm.team_name}
            <span className={`gd-form-record ${aW > aL ? 'gd-form-pos' : 'gd-form-neg'}`}>{aW}-{aL} Last 5</span>
          </div>
          <div className="gd-form-pills">
            {aLast5.map((g, i) => (
              <span key={i} className={`gd-form-pill ${g.result === 'W' ? 'gd-pill-w' : g.result === 'OTL' ? 'gd-pill-otl' : 'gd-pill-l'}`}>
                {g.result}
              </span>
            ))}
          </div>
          <div className="gd-form-games">
            {aLast3.map((g, i) => (
              <div key={i} className="gd-form-game">
                <span>{g.home_away === 'away' ? '@' : 'vs'} {g.opponent_abbrev || g.opponent_name}</span>
                <span className={g.result === 'W' ? 'gd-form-w' : 'gd-form-l'}>{g.result} {g.score_display}{g.overtime ? ' (OT)' : ''}</span>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* H2H */}
      <div className="gd-h2h">
        <div className="gd-h2h-header">
          <strong>Head-to-Head (Last 5)</strong>
          {h2h && h2h.games_played < 3 && <span className="gd-h2h-limited"><AlertTriangle size={12} /> Limited Data</span>}
        </div>
        {!h2h || h2h.games_played === 0 ? (
          <p className="gd-muted">No head-to-head data available.</p>
        ) : (
          <div className="gd-h2h-record">
            <div className="gd-h2h-wins">
              <span className="gd-h2h-count">{homeH2HWins}</span>
              <span>{homeForm.team_name}</span>
            </div>
            <span className="gd-h2h-vs">vs</span>
            <div className="gd-h2h-wins">
              <span className="gd-h2h-count">{awayH2HWins}</span>
              <span>{awayForm.team_name}</span>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

function PerformanceAnalysis({ homeForm, awayForm, homeRecent, awayRecent }) {
  const hLast5 = (homeRecent || []).slice(0, 5);
  const aLast5 = (awayRecent || []).slice(0, 5);
  const hW = hLast5.filter(g => g.result === 'W').length;
  const hL = hLast5.length - hW;
  const aW = aLast5.filter(g => g.result === 'W').length;
  const aL = aLast5.length - aW;

  // Build chart data (most recent 5 games, reversed so oldest first)
  const maxLen = Math.min(hLast5.length, aLast5.length, 5);
  const chartData = [];
  for (let i = maxLen - 1; i >= 0; i--) {
    chartData.push({
      name: `G${maxLen - i}`,
      [homeForm.abbreviation || 'Home']: hLast5[i]?.goals_for || 0,
      [awayForm.abbreviation || 'Away']: aLast5[i]?.goals_for || 0,
    });
  }

  return (
    <div className="gd-section gd-perf">
      <h3><BarChart3 size={16} /> Performance Analysis</h3>
      <div className="gd-perf-records">
        <div className={`gd-perf-box ${hW > hL ? 'gd-perf-pos' : 'gd-perf-neg'}`}>
          <span className="gd-perf-name">{homeForm.team_name}</span>
          <span className="gd-perf-rec">{hW}-{hL}</span>
          <span className="gd-perf-sub">Last 5</span>
        </div>
        <div className={`gd-perf-box ${aW > aL ? 'gd-perf-pos' : 'gd-perf-neg'}`}>
          <span className="gd-perf-name">{awayForm.team_name}</span>
          <span className="gd-perf-rec">{aW}-{aL}</span>
          <span className="gd-perf-sub">Last 5</span>
        </div>
      </div>

      {chartData.length > 0 && (
        <>
          <h4 className="gd-chart-title"><TrendingUp size={14} /> Recent Scoring</h4>
          <div className="gd-chart-wrap">
            <ResponsiveContainer width="100%" height={200}>
              <BarChart data={chartData} barGap={4}>
                <XAxis dataKey="name" stroke="#6a6a88" fontSize={12} />
                <YAxis stroke="#6a6a88" fontSize={12} />
                <Tooltip
                  contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                  labelStyle={{ color: '#e8e8f0' }}
                />
                <Bar dataKey={homeForm.abbreviation || 'Home'} fill="#00ff88" radius={[4, 4, 0, 0]} />
                <Bar dataKey={awayForm.abbreviation || 'Away'} fill="#ff5252" radius={[4, 4, 0, 0]} opacity={0.6} />
              </BarChart>
            </ResponsiveContainer>
            <div className="gd-chart-legend">
              <span><span className="gd-legend-dot" style={{ background: '#00ff88' }} /> {homeForm.team_name}</span>
              <span><span className="gd-legend-dot" style={{ background: '#ff5252' }} /> {awayForm.team_name}</span>
            </div>
          </div>
        </>
      )}
    </div>
  );
}

/* ── Line Movement Section ── */

function LineMovementSection({ gameId, homeAbbr, awayAbbr }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    fetchLineMovement(gameId)
      .then(res => { if (!cancelled) setData(res.data); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [gameId]);

  if (loading) return null;
  if (!data || !data.snapshots || data.snapshots.length === 0) return null;

  const snapshots = data.snapshots;
  const opening = data.opening;
  const current = data.current;

  // Build chart data for moneyline movement
  const chartData = snapshots.map((s, i) => {
    let label;
    try {
      const dt = new Date(s.captured_at);
      label = format(dt, 'HH:mm');
    } catch {
      label = `#${i + 1}`;
    }
    return {
      time: label,
      [homeAbbr]: s.home_moneyline,
      [awayAbbr]: s.away_moneyline,
      ou: s.over_under_line,
    };
  });

  const mlMoved = opening && current &&
    (opening.home_moneyline !== current.home_moneyline || opening.away_moneyline !== current.away_moneyline);
  const ouMoved = opening && current &&
    opening.over_under_line !== current.over_under_line;

  return (
    <div className="gd-section gd-line-movement">
      <h3><GitBranch size={16} /> Line Movement</h3>
      <div className="gd-lm-summary">
        <div className="gd-lm-box">
          <span className="gd-lm-label">Opening ML</span>
          <span className="gd-lm-value">
            {homeAbbr} {formatAmericanOddsOrDash(opening?.home_moneyline)} / {awayAbbr} {formatAmericanOddsOrDash(opening?.away_moneyline)}
          </span>
        </div>
        <div className="gd-lm-box">
          <span className="gd-lm-label">Current ML</span>
          <span className={`gd-lm-value ${mlMoved ? 'gd-lm-moved' : ''}`}>
            {homeAbbr} {formatAmericanOddsOrDash(current?.home_moneyline)} / {awayAbbr} {formatAmericanOddsOrDash(current?.away_moneyline)}
          </span>
        </div>
        {opening?.over_under_line != null && (
          <>
            <div className="gd-lm-box">
              <span className="gd-lm-label">Opening O/U</span>
              <span className="gd-lm-value">{opening.over_under_line}</span>
            </div>
            <div className="gd-lm-box">
              <span className="gd-lm-label">Current O/U</span>
              <span className={`gd-lm-value ${ouMoved ? 'gd-lm-moved' : ''}`}>{current?.over_under_line ?? '—'}</span>
            </div>
          </>
        )}
      </div>

      {chartData.length > 1 && (
        <div className="gd-chart-wrap">
          <ResponsiveContainer width="100%" height={180}>
            <LineChart data={chartData}>
              <XAxis dataKey="time" stroke="#6a6a88" fontSize={11} />
              <YAxis stroke="#6a6a88" fontSize={11} />
              <Tooltip
                contentStyle={{ background: '#1a1a2e', border: '1px solid #2a2a4a', borderRadius: 8 }}
                labelStyle={{ color: '#e8e8f0' }}
              />
              <Line type="monotone" dataKey={homeAbbr} stroke="#00ff88" strokeWidth={2} dot={false} />
              <Line type="monotone" dataKey={awayAbbr} stroke="#ff5252" strokeWidth={2} dot={false} />
            </LineChart>
          </ResponsiveContainer>
          <div className="gd-chart-legend">
            <span><span className="gd-legend-dot" style={{ background: '#00ff88' }} /> {homeAbbr} ML</span>
            <span><span className="gd-legend-dot" style={{ background: '#ff5252' }} /> {awayAbbr} ML</span>
          </div>
        </div>
      )}

      <p className="gd-lm-snapshots">{snapshots.length} snapshot{snapshots.length !== 1 ? 's' : ''} captured</p>
    </div>
  );
}

/* ── Injury Report Section ── */

function InjuryReportSection({ gameId, homeForm, awayForm }) {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    fetchGameInjuries(gameId)
      .then(res => { if (!cancelled) setData(res.data); })
      .catch(() => {})
      .finally(() => { if (!cancelled) setLoading(false); });
    return () => { cancelled = true; };
  }, [gameId]);

  if (loading) return null;

  const homeInj = data?.home_injuries || [];
  const awayInj = data?.away_injuries || [];
  const hasInjuries = homeInj.length > 0 || awayInj.length > 0;

  const statusColor = (status) => {
    const s = (status || '').toLowerCase();
    if (s === 'out' || s.includes('ir')) return '#ff5252';
    if (s === 'day-to-day' || s === 'questionable') return '#ffd700';
    return '#6a6a88';
  };

  const renderTeamInjuries = (injuries, teamName) => (
    <div className="gd-inj-team">
      <h4>{teamName}</h4>
      {injuries.length === 0 ? (
        <p className="gd-inj-healthy"><CheckCircle size={14} /> No injuries reported</p>
      ) : (
        <div className="gd-inj-list">
          {injuries.map((inj, i) => (
            <div key={i} className="gd-inj-row">
              <span className="gd-inj-status" style={{ color: statusColor(inj.status) }}>{inj.status}</span>
              <span className="gd-inj-name">{inj.player_name}</span>
              <span className="gd-inj-pos">{inj.position || ''}</span>
              <span className="gd-inj-type">{inj.injury_type || ''}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  );

  return (
    <div className="gd-section gd-injuries">
      <h3>
        <HeartPulse size={16} /> Injury Report
        {hasInjuries && <span className="gd-inj-count">{homeInj.length + awayInj.length} player{homeInj.length + awayInj.length !== 1 ? 's' : ''}</span>}
      </h3>
      <div className="gd-inj-grid">
        {renderTeamInjuries(homeInj, homeForm.team_name || 'Home')}
        {renderTeamInjuries(awayInj, awayForm.team_name || 'Away')}
      </div>
    </div>
  );
}

/* ════════════════════════════════════════════════════════════
   Main GameDetail Component
   ════════════════════════════════════════════════════════════ */

function GameDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const { data: game, loading, error, refetch } = useApi(fetchGameDetails, [id]);

  const isLive = game && isLiveStatus(game.status);
  const intervalRef = useRef(null);
  useEffect(() => {
    if (isLive) {
      intervalRef.current = setInterval(() => refetch(), LIVE_POLL_INTERVAL);
    }
    return () => { if (intervalRef.current) { clearInterval(intervalRef.current); intervalRef.current = null; } };
  }, [isLive, refetch]);

  useWebSocketEvent('odds_update', useCallback((data) => {
    const changedIds = (data?.changed_games || []).map((g) => g.game_id);
    if (changedIds.includes(Number(id))) refetch();
  }, [id, refetch]));

  if (loading) return (
    <div className="game-detail-page">
      <div className="loading-container large"><div className="loading-spinner"></div><p>Loading game analysis...</p></div>
    </div>
  );
  if (error) return (
    <div className="game-detail-page">
      <button className="btn btn-back" onClick={() => navigate(-1)}><ArrowLeft size={18} /> Back</button>
      <div className="error-container"><p>Failed to load game details: {error}</p></div>
    </div>
  );
  if (!game) return (
    <div className="game-detail-page">
      <button className="btn btn-back" onClick={() => navigate(-1)}><ArrowLeft size={18} /> Back</button>
      <div className="empty-state"><p>Game not found</p></div>
    </div>
  );

  const homeForm = game.home_team_form || {};
  const awayForm = game.away_team_form || {};
  const homeAbbr = homeForm.abbreviation || teamAbbrev(game.home_team, 'HME');
  const awayAbbr = awayForm.abbreviation || teamAbbrev(game.away_team, 'AWY');
  const venue = game.venue || '';
  const odds = game.odds || null;
  const predictions = game.predictions || [];
  const topPred = predictions.find(p => p.recommended) || predictions[0] || null;
  const topConf = topPred ? confidencePct(topPred.confidence) : 0;
  const topEdge = topPred ? confidencePct(topPred.edge) : 0;
  const homeRecent = game.home_recent_games || [];
  const awayRecent = game.away_recent_games || [];
  const h2h = game.head_to_head || null;

  return (
    <div className="game-detail-page gd-page">
      <button className="btn btn-back" onClick={() => navigate(-1)}>
        <ArrowLeft size={18} /> Back
      </button>

      <GameHeader game={game} homeForm={homeForm} awayForm={awayForm}
        homeAbbr={homeAbbr} awayAbbr={awayAbbr} topPred={topPred} venue={venue} />

      <OddsSection odds={odds} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />

      <div className="gd-two-col">
        <LineMovementSection gameId={id} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
        <InjuryReportSection gameId={id} homeForm={homeForm} awayForm={awayForm} />
      </div>

      <MatchAnalysis topPred={topPred} predictions={predictions}
        homeForm={homeForm} awayForm={awayForm} h2h={h2h}
        homeRecent={homeRecent} awayRecent={awayRecent}
        homeAbbr={homeAbbr} awayAbbr={awayAbbr} />

      <div className="gd-two-col">
        <RiskAssessment conf={topConf} edge={topEdge} odds={odds} />
        <MarketInterest odds={odds} homeForm={homeForm} awayForm={awayForm} />
      </div>

      <div className="gd-two-col">
        <SeasonStatsCard homeForm={homeForm} awayForm={awayForm} />
        <BettingTrendsCard homeForm={homeForm} awayForm={awayForm}
          homeRecent={homeRecent} awayRecent={awayRecent} />
      </div>

      <VenueSection venue={venue} />

      <div className="gd-two-col">
        <FormAndH2H homeForm={homeForm} awayForm={awayForm} h2h={h2h}
          homeRecent={homeRecent} awayRecent={awayRecent} />
        <PerformanceAnalysis homeForm={homeForm} awayForm={awayForm}
          homeRecent={homeRecent} awayRecent={awayRecent} />
      </div>
    </div>
  );
}

export default GameDetail;
