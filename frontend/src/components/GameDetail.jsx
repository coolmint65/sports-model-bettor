import { useState, useEffect, useRef } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import {
  ArrowLeft,
  Clock,
  MapPin,
  BarChart3,
  Target,
  Users,
  TrendingUp,
  Activity,
  Layers,
  DollarSign,
  Radio,
  AlertTriangle,
} from 'lucide-react';
import { format } from 'date-fns';
import { fetchGameDetails } from '../utils/api';
import { useApi } from '../hooks/useApi';
import PredictionCard from './PredictionCard';
import { teamName, teamAbbrev, teamLogo, parseAsUTC, isLiveStatus } from '../utils/teams';

const LIVE_POLL_INTERVAL = 30_000;

const TABS = [
  { id: 'overview', label: 'Overview', icon: BarChart3 },
  { id: 'predictions', label: 'Predictions', icon: Target },
  { id: 'h2h', label: 'H2H', icon: Users },
  { id: 'form', label: 'Form', icon: TrendingUp },
  { id: 'periods', label: 'Periods', icon: Layers },
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

function formatAmericanOdds(odds) {
  if (odds == null) return '-';
  const rounded = Math.round(odds);
  return rounded > 0 ? `+${rounded}` : `${rounded}`;
}

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
      {odds && (
        <div className="odds-section">
          <h3 className="subsection-title">
            <DollarSign size={16} />
            Sportsbook Odds
          </h3>
          <div className="odds-grid">
            {(odds.home_moneyline != null || odds.away_moneyline != null) && (
              <div className="odds-card">
                <span className="odds-label">Moneyline</span>
                <div className="odds-values">
                  <div className="odds-team-line">
                    <span className="odds-team-name">{away.abbreviation || 'Away'}</span>
                    <span className="odds-value">{formatAmericanOdds(odds.away_moneyline)}</span>
                  </div>
                  <div className="odds-team-line">
                    <span className="odds-team-name">{home.abbreviation || 'Home'}</span>
                    <span className="odds-value">{formatAmericanOdds(odds.home_moneyline)}</span>
                  </div>
                </div>
              </div>
            )}
            {odds.over_under_line != null && (
              <div className="odds-card">
                <span className="odds-label">Over/Under</span>
                <div className="odds-values">
                  <div className="odds-team-line">
                    <span className="odds-team-name">O {odds.over_under_line}</span>
                    <span className="odds-value">{formatAmericanOdds(odds.over_price)}</span>
                  </div>
                  <div className="odds-team-line">
                    <span className="odds-team-name">U {odds.over_under_line}</span>
                    <span className="odds-value">{formatAmericanOdds(odds.under_price)}</span>
                  </div>
                </div>
              </div>
            )}
            {odds.home_spread_line != null && (
              <div className="odds-card">
                <span className="odds-label">Puck Line</span>
                <div className="odds-values">
                  <div className="odds-team-line">
                    <span className="odds-team-name">{away.abbreviation} {odds.away_spread_line != null ? (odds.away_spread_line > 0 ? '+' : '') + odds.away_spread_line : ''}</span>
                    <span className="odds-value">{formatAmericanOdds(odds.away_spread_price)}</span>
                  </div>
                  <div className="odds-team-line">
                    <span className="odds-team-name">{home.abbreviation} {odds.home_spread_line > 0 ? '+' : ''}{odds.home_spread_line}</span>
                    <span className="odds-value">{formatAmericanOdds(odds.home_spread_price)}</span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

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
    </div>
  );
}

const MARKET_BET_TYPES = new Set(['ml', 'total', 'spread']);

function PredictionsTab({ game }) {
  const predictions = game.predictions || game.bets || [];

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

  // Split market bets from props
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
              <PredictionCard key={pred.id || index} prediction={pred} />
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
              <PredictionCard key={pred.id || index} prediction={pred} isFallback />
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
              <PredictionCard key={pred.id || index} prediction={pred} />
            ))}
          </div>
        </div>
      )}

      {/* --- Props / Exotics --- */}
      {props.length > 0 && (
        <div className="predictions-section">
          <h3 className="predictions-section-title predictions-section-title-other">
            Props
          </h3>
          <div className="predictions-list">
            {props.map((pred, index) => (
              <PredictionCard key={pred.id || index} prediction={pred} compact />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

function H2HTab({ game }) {
  const h2h = game.head_to_head || game.h2h || null;
  const awayLabel = game.away_team_form?.team_name || 'Away';
  const homeLabel = game.home_team_form?.team_name || 'Home';

  if (!h2h) {
    return (
      <div className="tab-content h2h-tab">
        <div className="empty-state">
          <Users size={48} />
          <p>No head-to-head history available. Sync historical data to populate matchup records.</p>
        </div>
      </div>
    );
  }

  const homeId = game.home_team_form?.team_id ?? game.home_team?.id;
  const team1IsHome = h2h.team1_id === homeId;
  const label1 = team1IsHome ? homeLabel : awayLabel;
  const label2 = team1IsHome ? awayLabel : homeLabel;

  return (
    <div className="tab-content h2h-tab">
      <div className="h2h-summary">
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
    </div>
  );
}

function FormTab({ game }) {
  const awayForm = game.away_recent_games || game.away_form || game.away_recent || [];
  const homeForm = game.home_recent_games || game.home_form || game.home_recent || [];
  const awayTeamLabel = game.away_team_form?.team_name || teamName(game.away_team, 'Away');
  const homeTeamLabel = game.home_team_form?.team_name || teamName(game.home_team, 'Home');

  const renderFormList = (form, label) => {
    if (!form || form.length === 0) {
      return (
        <div className="empty-state-small">
          <p>No recent form data for {label}. Sync historical data to populate.</p>
        </div>
      );
    }

    const last5 = form.slice(0, 5);
    const last10 = form.slice(0, 10);
    const last20 = form.slice(0, 20);

    const calcRecord = (games) => {
      const wins = games.filter((g) => g.result === 'W').length;
      const losses = games.filter((g) => g.result === 'L').length;
      const otl = games.filter((g) => g.result === 'OTL').length;
      return `${wins}-${losses}${otl > 0 ? `-${otl}` : ''}`;
    };

    const calcAvgGoals = (games) => {
      if (games.length === 0) return '0.0';
      const total = games.reduce((sum, g) => sum + (g.goals_for || 0), 0);
      return (total / games.length).toFixed(1);
    };

    const calcAvgGA = (games) => {
      if (games.length === 0) return '0.0';
      const total = games.reduce((sum, g) => sum + (g.goals_against || 0), 0);
      return (total / games.length).toFixed(1);
    };

    return (
      <div className="form-section">
        <div className="form-summary-grid">
          {last5.length > 0 && (
            <div className="form-period">
              <span className="form-period-label">Last 5</span>
              <span className="form-period-record">{calcRecord(last5)}</span>
              <span className="form-period-goals">{calcAvgGoals(last5)} GF/G | {calcAvgGA(last5)} GA/G</span>
            </div>
          )}
          {last10.length >= 6 && (
            <div className="form-period">
              <span className="form-period-label">Last 10</span>
              <span className="form-period-record">{calcRecord(last10)}</span>
              <span className="form-period-goals">{calcAvgGoals(last10)} GF/G | {calcAvgGA(last10)} GA/G</span>
            </div>
          )}
          {last20.length >= 11 && (
            <div className="form-period">
              <span className="form-period-label">Last 20</span>
              <span className="form-period-record">{calcRecord(last20)}</span>
              <span className="form-period-goals">{calcAvgGoals(last20)} GF/G | {calcAvgGA(last20)} GA/G</span>
            </div>
          )}
        </div>

        <div className="form-results-strip">
          {last10.map((g, i) => {
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

        {/* Recent Games Table */}
        <div className="form-games-table">
          <div className="form-games-header">
            <span>Date</span>
            <span>Opp</span>
            <span>Score</span>
            <span>Result</span>
          </div>
          {last10.map((g, i) => {
            const resultClass =
              g.result === 'W' ? 'result-win' : g.result === 'OTL' ? 'result-otl' : 'result-loss';
            return (
              <div key={i} className="form-games-row">
                <span className="form-game-date">{g.game_date}</span>
                <span className="form-game-opp">
                  {g.home_away === 'away' ? '@' : 'vs'} {g.opponent_abbrev}
                </span>
                <span className="form-game-score">{g.score_display}{g.overtime ? ' OT' : ''}</span>
                <span className={`form-game-result ${resultClass}`}>{g.result}</span>
              </div>
            );
          })}
        </div>
      </div>
    );
  };

  return (
    <div className="tab-content form-tab">
      <div className="form-teams-grid">
        <div className="form-team-section">
          <h3 className="form-team-title">{awayTeamLabel}</h3>
          {renderFormList(awayForm, awayTeamLabel)}
        </div>
        <div className="form-team-section">
          <h3 className="form-team-title">{homeTeamLabel}</h3>
          {renderFormList(homeForm, homeTeamLabel)}
        </div>
      </div>
    </div>
  );
}

function PeriodsTab({ game }) {
  const homePeriod = game.home_period_scoring || {};
  const awayPeriod = game.away_period_scoring || {};
  const hasPeriodData = homePeriod.period_1_avg != null || awayPeriod.period_1_avg != null;
  const periodData = hasPeriodData ? [
    { period: '1st', away: awayPeriod.period_1_avg, home: homePeriod.period_1_avg },
    { period: '2nd', away: awayPeriod.period_2_avg, home: homePeriod.period_2_avg },
    { period: '3rd', away: awayPeriod.period_3_avg, home: homePeriod.period_3_avg },
  ] : (game.period_analysis || game.period_scoring || game.periods || null);
  const awayTeamLabel = game.away_team_form?.team_name || teamName(game.away_team, 'Away');
  const homeTeamLabel = game.home_team_form?.team_name || teamName(game.home_team, 'Home');

  if (!periodData) {
    return (
      <div className="tab-content">
        <div className="empty-state">
          <Layers size={48} />
          <p>No period analysis data available.</p>
        </div>
      </div>
    );
  }

  const periods = Array.isArray(periodData)
    ? periodData
    : [
        { period: '1st', away: periodData.first?.away || periodData.p1_away, home: periodData.first?.home || periodData.p1_home },
        { period: '2nd', away: periodData.second?.away || periodData.p2_away, home: periodData.second?.home || periodData.p2_home },
        { period: '3rd', away: periodData.third?.away || periodData.p3_away, home: periodData.third?.home || periodData.p3_home },
      ];

  return (
    <div className="tab-content periods-tab">
      <h3 className="subsection-title">Period-by-Period Scoring Averages</h3>
      <div className="periods-table">
        <div className="periods-table-header">
          <span>Period</span>
          <span>{awayTeamLabel}</span>
          <span>{homeTeamLabel}</span>
        </div>
        {periods.map((p, index) => {
          const periodLabel = p.period || p.label || `Period ${index + 1}`;
          const awayVal = p.away ?? p.away_goals ?? p.away_avg ?? '-';
          const homeVal = p.home ?? p.home_goals ?? p.home_avg ?? '-';

          return (
            <div className="periods-table-row" key={index}>
              <span className="period-label">{periodLabel}</span>
              <span className="period-value">{typeof awayVal === 'number' ? awayVal.toFixed(2) : awayVal}</span>
              <span className="period-value">{typeof homeVal === 'number' ? homeVal.toFixed(2) : homeVal}</span>
            </div>
          );
        })}
      </div>

      {(periodData.over_rate || periodData.scoring_patterns) && (
        <div className="period-insights">
          <h3 className="subsection-title">Scoring Patterns</h3>
          <div className="insights-grid">
            {periodData.over_rate && (
              <div className="insight-card">
                <Activity size={18} />
                <span>Over Rate: {periodData.over_rate}%</span>
              </div>
            )}
            {periodData.first_period_over_rate && (
              <div className="insight-card">
                <Activity size={18} />
                <span>1st Period Scoring Rate: {periodData.first_period_over_rate}%</span>
              </div>
            )}
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

  const { data: game, loading, error, refetch } = useApi(fetchGameDetails, [id]);

  // Auto-poll for live games
  const isLive = game && isLiveStatus(game.status);
  const intervalRef = useRef(null);
  useEffect(() => {
    if (isLive) {
      intervalRef.current = setInterval(() => {
        refetch();
      }, LIVE_POLL_INTERVAL);
    }
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [isLive, refetch]);

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
      case 'h2h':
        return <H2HTab game={game} />;
      case 'form':
        return <FormTab game={game} />;
      case 'periods':
        return <PeriodsTab game={game} />;
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
