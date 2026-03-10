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
  Layers,
  DollarSign,
  Radio,
  AlertTriangle,
  Lock,
} from 'lucide-react';
import { format, formatDistanceToNowStrict } from 'date-fns';
import { fetchGameDetails } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { useWebSocketEvent } from '../hooks/useWebSocket';
import PredictionCard from './PredictionCard';
import { teamName, teamAbbrev, teamLogo, parseAsUTC, isLiveStatus } from '../utils/teams';
import { formatAmericanOddsOrDash } from '../utils/formatting';

const LIVE_POLL_INTERVAL = 30_000;

const TABS = [
  { id: 'overview', label: 'Overview', icon: BarChart3 },
  { id: 'predictions', label: 'Predictions', icon: Target },
];

function formatGameDateTime(game) {
  try {
    const dateStr = game.start_time || game.datetime;
    if (!dateStr) return 'TBD';
    const date = parseAsUTC(dateStr);
    if (!date || isNaN(date.getTime())) return 'TBD';
    return format(date, 'EEEE, MMM d, yyyy - h:mm a');
  } catch {
    return game.time || 'TBD';
  }
}

function StatComparison({ label, awayValue, homeValue, higherIsBetter = true, format: formatFn }) {
  const awayNum = parseFloat(awayValue) || 0;
  const homeNum = parseFloat(homeValue) || 0;
  const awayBetter = higherIsBetter ? awayNum > homeNum : awayNum < homeNum;
  const homeBetter = higherIsBetter ? homeNum > awayNum : homeNum < awayNum;
  const displayAway = formatFn ? formatFn(awayValue) : awayValue;
  const displayHome = formatFn ? formatFn(homeValue) : homeValue;

  return (
    <div className="stat-comparison-row">
      <span className={`stat-value stat-away ${awayBetter ? 'stat-better' : ''}`}>
        {displayAway ?? '-'}
      </span>
      <span className="stat-label">{label}</span>
      <span className={`stat-value stat-home ${homeBetter ? 'stat-better' : ''}`}>
        {displayHome ?? '-'}
      </span>
    </div>
  );
}

const formatAmericanOdds = formatAmericanOddsOrDash;

function RecordPill({ label, value }) {
  if (!value) return null;
  return (
    <div className="record-pill">
      <span className="record-pill-label">{label}</span>
      <span className="record-pill-value">{value}</span>
    </div>
  );
}

function OverviewTab({ game }) {
  const away = game.away_team_form || {};
  const home = game.home_team_form || {};
  const awayRecord = `${away.wins || 0}-${away.losses || 0}-${away.ot_losses || 0}`;
  const homeRecord = `${home.wins || 0}-${home.losses || 0}-${home.ot_losses || 0}`;
  const odds = game.odds;

  const awayLogo = away.logo_url || teamLogo(game.away_team) || teamLogo(game.away_team_form);
  const homeLogo = home.logo_url || teamLogo(game.home_team) || teamLogo(game.home_team_form);

  const fmtDiff = (v) => {
    if (v == null) return '-';
    return v > 0 ? `+${v}` : `${v}`;
  };
  const fmtPct = (v) => {
    if (v == null) return '-';
    return typeof v === 'number' ? `${(v * 100).toFixed(1)}%` : v;
  };

  const stats = [
    { label: 'Goals/Game', away: away.goals_for_per_game, home: home.goals_for_per_game, higher: true },
    { label: 'Goals Against/Game', away: away.goals_against_per_game, home: home.goals_against_per_game, higher: false },
    { label: 'Goal Differential', away: away.goal_diff, home: home.goal_diff, higher: true, fmt: fmtDiff },
    { label: 'Power Play %', away: away.power_play_pct, home: home.power_play_pct, higher: true },
    { label: 'Penalty Kill %', away: away.penalty_kill_pct, home: home.penalty_kill_pct, higher: true },
    { label: 'Shots/Game', away: away.shots_for_per_game, home: home.shots_for_per_game, higher: true },
    { label: 'Shots Against/Game', away: away.shots_against_per_game, home: home.shots_against_per_game, higher: false },
    { label: 'Faceoff Win %', away: away.faceoff_win_pct, home: home.faceoff_win_pct, higher: true },
    { label: 'Points %', away: away.points_pct, home: home.points_pct, higher: true, fmt: fmtPct },
  ];

  const hasAnyStats = stats.some(s => s.away != null || s.home != null);

  return (
    <div className="tab-content overview-tab">
      {/* Odds — show pregame + live separately for live games */}
      {(() => {
        const pregame = game.pregame_odds;
        const live = isLiveStatus(game.status);
        const hasLiveOdds = live && pregame && odds;

        const isOT = game.period_type === 'OT' || game.period_type === 'SO';

        const renderOddsGrid = (o, showLocks = false) => {
          const locked = showLocks && isOT;
          return (
            <div className="odds-grid">
              {(o.home_moneyline != null || o.away_moneyline != null) && (
                <div className="odds-card">
                  <span className="odds-label">Moneyline</span>
                  <div className="odds-values">
                    <div className="odds-team-line">
                      <span className="odds-team-name">{away.abbreviation || 'Away'}</span>
                      <span className="odds-value">{formatAmericanOdds(o.away_moneyline)}</span>
                    </div>
                    <div className="odds-team-line">
                      <span className="odds-team-name">{home.abbreviation || 'Home'}</span>
                      <span className="odds-value">{formatAmericanOdds(o.home_moneyline)}</span>
                    </div>
                  </div>
                </div>
              )}
              {o.over_under_line != null && (
                <div className={`odds-card ${locked ? 'odds-card-locked' : ''}`}>
                  <span className="odds-label">Over/Under</span>
                  {locked ? (
                    <div className="odds-values odds-locked-overlay">
                      <Lock size={16} className="odds-lock-icon" />
                    </div>
                  ) : (
                    <div className="odds-values">
                      <div className="odds-team-line">
                        <span className="odds-team-name">O {o.over_under_line}</span>
                        <span className="odds-value">{formatAmericanOdds(o.over_price)}</span>
                      </div>
                      <div className="odds-team-line">
                        <span className="odds-team-name">U {o.over_under_line}</span>
                        <span className="odds-value">{formatAmericanOdds(o.under_price)}</span>
                      </div>
                    </div>
                  )}
                </div>
              )}
              {o.home_spread_line != null && (
                <div className={`odds-card ${locked ? 'odds-card-locked' : ''}`}>
                  <span className="odds-label">Puck Line</span>
                  {locked ? (
                    <div className="odds-values odds-locked-overlay">
                      <Lock size={16} className="odds-lock-icon" />
                    </div>
                  ) : (
                    <div className="odds-values">
                      <div className="odds-team-line">
                        <span className="odds-team-name">{away.abbreviation} {o.away_spread_line != null ? (o.away_spread_line > 0 ? '+' : '') + o.away_spread_line : ''}</span>
                        <span className="odds-value">{formatAmericanOdds(o.away_spread_price)}</span>
                      </div>
                      <div className="odds-team-line">
                        <span className="odds-team-name">{home.abbreviation} {o.home_spread_line > 0 ? '+' : ''}{o.home_spread_line}</span>
                        <span className="odds-value">{formatAmericanOdds(o.home_spread_price)}</span>
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        };

        if (hasLiveOdds) {
          // Show both sections for live games with pregame snapshot
          return (
            <>
              <div className="odds-section">
                <h3 className="subsection-title">
                  <DollarSign size={16} />
                  Pregame Lines
                </h3>
                {renderOddsGrid(pregame)}
              </div>
              <div className="odds-section odds-section-live">
                <h3 className="subsection-title">
                  <DollarSign size={16} />
                  Live Lines
                  {odds.odds_updated_at && (
                    <span className="odds-updated-ago-detail">
                      {(() => {
                        try {
                          const dt = new Date(odds.odds_updated_at);
                          if (isNaN(dt.getTime())) return '';
                          return formatDistanceToNowStrict(dt, { addSuffix: true });
                        } catch { return ''; }
                      })()}
                    </span>
                  )}
                </h3>
                {renderOddsGrid(odds, true)}
              </div>
            </>
          );
        }

        if (!odds) return null;

        // Single section for pregame or when no pregame snapshot
        return (
          <div className="odds-section">
            <h3 className="subsection-title">
              <DollarSign size={16} />
              {live ? 'Pregame Lines' : 'Sportsbook Odds'}
              {odds.odds_updated_at && (
                <span className="odds-updated-ago-detail">
                  {(() => {
                    try {
                      const dt = new Date(odds.odds_updated_at);
                      if (isNaN(dt.getTime())) return '';
                      return formatDistanceToNowStrict(dt, { addSuffix: true });
                    } catch { return ''; }
                  })()}
                </span>
              )}
            </h3>
            {renderOddsGrid(odds)}
          </div>
        );
      })()}

      {/* Team Records Section */}
      <div className="overview-records-section">
        <div className="overview-team-records">
          <div className="overview-team-header">
            {awayLogo && <img className="overview-team-logo" src={awayLogo} alt="" width={28} height={28} onError={(e) => { e.target.style.display = 'none'; }} />}
            <span className="overview-team-name">{away.abbreviation || 'Away'}</span>
            <span className="overview-team-record">{awayRecord}</span>
          </div>
          <div className="record-pills">
            <RecordPill label="L5" value={away.record_last_5} />
            <RecordPill label="L10" value={away.record_last_10} />
            <RecordPill label="L20" value={away.record_last_20} />
            <RecordPill label="Home" value={away.home_record} />
            <RecordPill label="Away" value={away.away_record} />
          </div>
        </div>
        <div className="overview-team-records">
          <div className="overview-team-header">
            {homeLogo && <img className="overview-team-logo" src={homeLogo} alt="" width={28} height={28} onError={(e) => { e.target.style.display = 'none'; }} />}
            <span className="overview-team-name">{home.abbreviation || 'Home'}</span>
            <span className="overview-team-record">{homeRecord}</span>
          </div>
          <div className="record-pills">
            <RecordPill label="L5" value={home.record_last_5} />
            <RecordPill label="L10" value={home.record_last_10} />
            <RecordPill label="L20" value={home.record_last_20} />
            <RecordPill label="Home" value={home.home_record} />
            <RecordPill label="Away" value={home.away_record} />
          </div>
        </div>
      </div>

      {hasAnyStats ? (
        <div className="stat-comparison-table">
          {stats.map(
            (stat) =>
              (stat.away != null || stat.home != null) && (
                <StatComparison
                  key={stat.label}
                  label={stat.label}
                  awayValue={stat.away}
                  homeValue={stat.home}
                  higherIsBetter={stat.higher}
                  format={stat.fmt || ((v) => (v != null ? (typeof v === 'number' ? v.toFixed(2) : v) : '-'))}
                />
              )
          )}
        </div>
      ) : (
        <div className="empty-state">
          <BarChart3 size={48} />
          <p>No team stats available for this game yet.</p>
        </div>
      )}

      {/* Head-to-Head */}
      <OverviewH2H game={game} />

      {/* Recent Form */}
      <OverviewForm game={game} />
    </div>
  );
}

function OverviewH2H({ game }) {
  const h2h = game.head_to_head || game.h2h || null;
  if (!h2h) return null;

  const awayLabel = game.away_team_form?.team_name || 'Away';
  const homeLabel = game.home_team_form?.team_name || 'Home';
  const homeId = game.home_team_form?.team_id ?? game.home_team?.id;
  const team1IsHome = h2h.team1_id === homeId;
  const label1 = team1IsHome ? homeLabel : awayLabel;
  const label2 = team1IsHome ? awayLabel : homeLabel;

  return (
    <div className="overview-h2h-section">
      <h3 className="subsection-title">
        <Users size={16} />
        Head-to-Head
      </h3>
      <div className="h2h-summary-grid">
        {h2h.games_played != null && (
          <div className="h2h-stat-box">
            <span className="h2h-stat-value">{h2h.games_played}</span>
            <span className="h2h-stat-label">Games Played</span>
          </div>
        )}
        {h2h.team1_wins != null && (
          <div className="h2h-stat-box">
            <span className="h2h-stat-value">{h2h.team1_wins}</span>
            <span className="h2h-stat-label">{label1} Wins</span>
          </div>
        )}
        {h2h.team2_wins != null && (
          <div className="h2h-stat-box">
            <span className="h2h-stat-value">{h2h.team2_wins}</span>
            <span className="h2h-stat-label">{label2} Wins</span>
          </div>
        )}
        {h2h.team1_goals != null && h2h.team2_goals != null && h2h.games_played > 0 && (
          <>
            <div className="h2h-stat-box">
              <span className="h2h-stat-value">
                {((h2h.team1_goals + h2h.team2_goals) / h2h.games_played).toFixed(1)}
              </span>
              <span className="h2h-stat-label">Avg Total Goals</span>
            </div>
            <div className="h2h-stat-box">
              <span className="h2h-stat-value">
                {(h2h.team1_goals / h2h.games_played).toFixed(1)} - {(h2h.team2_goals / h2h.games_played).toFixed(1)}
              </span>
              <span className="h2h-stat-label">Avg Goals ({label1} - {label2})</span>
            </div>
          </>
        )}
        {h2h.last_meeting && (
          <div className="h2h-stat-box">
            <span className="h2h-stat-value">{h2h.last_meeting}</span>
            <span className="h2h-stat-label">Last Meeting</span>
          </div>
        )}
      </div>
    </div>
  );
}

function OverviewForm({ game }) {
  const awayForm = game.away_recent_games || game.away_form || game.away_recent || [];
  const homeForm = game.home_recent_games || game.home_form || game.home_recent || [];
  const awayTeamLabel = game.away_team_form?.team_name || teamName(game.away_team, 'Away');
  const homeTeamLabel = game.home_team_form?.team_name || teamName(game.home_team, 'Home');

  if (awayForm.length === 0 && homeForm.length === 0) return null;

  const calcRecord = (games) => {
    const wins = games.filter((g) => g.result === 'W').length;
    const losses = games.filter((g) => g.result === 'L').length;
    const otl = games.filter((g) => g.result === 'OTL').length;
    return `${wins}-${losses}${otl > 0 ? `-${otl}` : ''}`;
  };

  const renderTeamForm = (form, label) => {
    if (!form || form.length === 0) return null;
    const last10 = form.slice(0, 10);

    return (
      <div className="overview-form-team">
        <h4 className="overview-form-team-name">{label}</h4>
        <div className="overview-form-record">
          L10: {calcRecord(last10)}
        </div>
        <div className="form-results-strip">
          {[...last10].reverse().map((g, i) => {
            const resultClass =
              g.result === 'W'
                ? 'result-win'
                : g.result === 'OTL'
                  ? 'result-otl'
                  : 'result-loss';
            const title = `${g.result} ${g.score_display || ''} ${g.home_away === 'home' ? 'vs' : '@'} ${g.opponent_abbrev || ''}${g.overtime ? ' (OT)' : ''}`;
            return (
              <div key={i} className={`form-result-dot ${resultClass}`} title={title}>
                {g.result}
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="overview-form-section">
      <h3 className="subsection-title">
        <TrendingUp size={16} />
        Recent Form
      </h3>
      <div className="overview-form-grid">
        {renderTeamForm(awayForm, awayTeamLabel)}
        {renderTeamForm(homeForm, homeTeamLabel)}
      </div>
    </div>
  );
}

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

  // Within market bets: recommended > fallback > rest
  const topPicks = market.filter((p) => p.recommended);
  const heavyJuice = market.filter((p) => p.is_fallback && !p.recommended);
  const otherMarket = market.filter((p) => !p.recommended && !p.is_fallback);

  return (
    <div className="tab-content predictions-tab">
      {/* --- Top Picks (recommended market bets) --- */}
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

      {/* --- Heavy Juice (market bets with edge but steep lines) --- */}
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

      {/* --- Other market bets (no edge or below threshold) --- */}
      {otherMarket.length > 0 && (
        <div className="predictions-section">
          <h3 className="predictions-section-title predictions-section-title-other">
            Market Analysis
          </h3>
          <p className="predictions-section-desc">
            {topPicks.length === 0 && heavyJuice.length === 0
              ? 'No actionable edge found for this game.'
              : 'Other market predictions below threshold.'}
          </p>
          <div className="predictions-list">
            {otherMarket.map((pred, index) => (
              <PredictionCard key={pred.id || index} prediction={pred} homeAbbr={homeAbbr} awayAbbr={awayAbbr} />
            ))}
          </div>
        </div>
      )}

      {/* --- Props --- */}
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

function GameDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('overview');

  const { data: game, loading, error, refetch, silentRefetch } = useApi(fetchGameDetails, [id]);

  // Auto-poll for live games (fallback)
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

  // Instantly refetch when WebSocket pushes odds update for this game
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
  const awayRecord = awayForm.wins != null ? `${awayForm.wins}-${awayForm.losses}-${awayForm.ot_losses}` : '';
  const homeRecord = homeForm.wins != null ? `${homeForm.wins}-${homeForm.losses}-${homeForm.ot_losses}` : '';
  const venue = game.venue || game.arena || '';

  // Team logos - check multiple possible sources
  const awayLogoUrl = awayForm.logo_url || teamLogo(game.away_team) || teamLogo(game.away_team_form);
  const homeLogoUrl = homeForm.logo_url || teamLogo(game.home_team) || teamLogo(game.home_team_form);

  const renderTabContent = () => {
    switch (activeTab) {
      case 'overview':
        return <OverviewTab game={game} />;
      case 'predictions':
        return <PredictionsTab game={game} />;
      default:
        return <OverviewTab game={game} />;
    }
  };

  return (
    <div className="game-detail-page">
      <button className="btn btn-back" onClick={() => navigate(-1)}>
        <ArrowLeft size={18} />
        Back
      </button>

      {/* Game Header */}
      <div className="game-detail-header">
        <div className="game-detail-team away-team-detail">
          {awayLogoUrl && (
            <img
              className="detail-team-logo"
              src={awayLogoUrl}
              alt={awayTeamLabel}
              width={56}
              height={56}
              onError={(e) => { e.target.style.display = 'none'; }}
            />
          )}
          <div className="detail-team-abbrev">{awayAbbr}</div>
          <div className="detail-team-name">{awayTeamLabel}</div>
          {awayRecord && <div className="detail-team-record">{awayRecord}</div>}
        </div>

        <div className="game-detail-center">
          {isLive ? (
            <div className="game-detail-live-center">
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
              {(game.away_shots != null || game.home_shots != null) && (
                <div className="detail-live-shots">
                  SOG: {game.away_shots ?? 0} - {game.home_shots ?? 0}
                </div>
              )}
            </div>
          ) : game.status === 'final' ? (
            <div className="game-detail-final-center">
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
              <div className="game-detail-meta">
                {venue && (
                  <span className="game-detail-venue">
                    <MapPin size={14} />
                    {venue}
                  </span>
                )}
              </div>
            </div>
          ) : (
            <>
              <div className="game-detail-vs">VS</div>
              <div className="game-detail-meta">
                <span className="game-detail-time">
                  <Clock size={14} />
                  {formatGameDateTime(game)}
                </span>
                {venue && (
                  <span className="game-detail-venue">
                    <MapPin size={14} />
                    {venue}
                  </span>
                )}
              </div>
            </>
          )}
        </div>

        <div className="game-detail-team home-team-detail">
          {homeLogoUrl && (
            <img
              className="detail-team-logo"
              src={homeLogoUrl}
              alt={homeTeamLabel}
              width={56}
              height={56}
              onError={(e) => { e.target.style.display = 'none'; }}
            />
          )}
          <div className="detail-team-abbrev">{homeAbbr}</div>
          <div className="detail-team-name">{homeTeamLabel}</div>
          {homeRecord && <div className="detail-team-record">{homeRecord}</div>}
        </div>
      </div>

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
      <div className="game-detail-content">
        {renderTabContent()}
      </div>
    </div>
  );
}

export default GameDetail;
