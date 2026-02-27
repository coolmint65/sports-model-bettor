import React, { useState } from 'react';
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
} from 'lucide-react';
import { format } from 'date-fns';
import { fetchGameDetails } from '../utils/api';
import { useApi } from '../hooks/useApi';
import PredictionCard from './PredictionCard';
import { teamName, teamAbbrev, parseAsUTC } from '../utils/teams';

const TABS = [
  { id: 'overview', label: 'Overview', icon: BarChart3 },
  { id: 'predictions', label: 'Predictions', icon: Target },
  { id: 'h2h', label: 'H2H', icon: Users },
  { id: 'form', label: 'Form', icon: TrendingUp },
  { id: 'periods', label: 'Periods', icon: Layers },
];

function getConfidenceColor(confidence) {
  if (confidence >= 75) return '#00ff88';
  if (confidence >= 60) return '#4fc3f7';
  if (confidence >= 45) return '#ffd700';
  return '#ff5252';
}

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

function OverviewTab({ game }) {
  const away = game.away_team_form || game.away_stats || game.away_team_stats || {};
  const home = game.home_team_form || game.home_stats || game.home_team_stats || {};
  const awayRecord = `${away.wins || 0}-${away.losses || 0}-${away.ot_losses || 0}`;
  const homeRecord = `${home.wins || 0}-${home.losses || 0}-${home.ot_losses || 0}`;

  const stats = [
    { label: 'Goals/Game', away: away.goals_for_per_game, home: home.goals_for_per_game, higher: true },
    { label: 'Goals Against/Game', away: away.goals_against_per_game, home: home.goals_against_per_game, higher: false },
    { label: 'Power Play %', away: away.power_play_pct, home: home.power_play_pct, higher: true },
    { label: 'Penalty Kill %', away: away.penalty_kill_pct, home: home.penalty_kill_pct, higher: true },
    { label: 'Shots/Game', away: away.shots_for_per_game, home: home.shots_for_per_game, higher: true },
    { label: 'Shots Against/Game', away: away.shots_against_per_game, home: home.shots_against_per_game, higher: false },
  ];

  const hasAnyStats = stats.some(s => s.away != null || s.home != null);

  return (
    <div className="tab-content overview-tab">
      {awayRecord || homeRecord ? (
        <div className="records-bar">
          <span className="record-label">{awayRecord || 'N/A'}</span>
          <span className="record-vs">Records</span>
          <span className="record-label">{homeRecord || 'N/A'}</span>
        </div>
      ) : null}

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
                  format={(v) => (v != null ? (typeof v === 'number' ? v.toFixed(2) : v) : '-')}
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

  return (
    <div className="tab-content predictions-tab">
      <div className="predictions-list">
        {predictions.map((pred, index) => (
          <PredictionCard key={pred.id || index} prediction={pred} />
        ))}
      </div>
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
          <p>No head-to-head history available.</p>
        </div>
      </div>
    );
  }

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
              <span className="h2h-stat-label">{homeLabel} Wins</span>
            </div>
          )}
          {h2h.team2_wins != null && (
            <div className="h2h-stat-box">
              <span className="h2h-stat-value">{h2h.team2_wins}</span>
              <span className="h2h-stat-label">{awayLabel} Wins</span>
            </div>
          )}
          {h2h.team1_goals != null && h2h.team2_goals != null && h2h.games_played > 0 && (
            <div className="h2h-stat-box">
              <span className="h2h-stat-value">
                {((h2h.team1_goals + h2h.team2_goals) / h2h.games_played).toFixed(1)}
              </span>
              <span className="h2h-stat-label">Avg Total Goals</span>
            </div>
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
  const awayForm = game.away_form || game.away_recent || [];
  const homeForm = game.home_form || game.home_recent || [];
  const awayTeamLabel = game.away_team_form?.team_name || teamName(game.away_team, 'Away');
  const homeTeamLabel = game.home_team_form?.team_name || teamName(game.home_team, 'Home');

  const renderFormList = (form, label) => {
    if (!form || form.length === 0) {
      return (
        <div className="empty-state-small">
          <p>No recent form data for {label}</p>
        </div>
      );
    }

    const last5 = form.slice(0, 5);
    const last10 = form.slice(0, 10);
    const last20 = form.slice(0, 20);

    const calcRecord = (games) => {
      const wins = games.filter((g) => g.result === 'W' || g.win).length;
      const losses = games.filter((g) => g.result === 'L' || g.loss).length;
      const otl = games.filter((g) => g.result === 'OTL' || g.otl).length;
      return `${wins}-${losses}${otl > 0 ? `-${otl}` : ''}`;
    };

    const calcAvgGoals = (games) => {
      if (games.length === 0) return '0.0';
      const total = games.reduce((sum, g) => sum + (g.goals_for || g.score || 0), 0);
      return (total / games.length).toFixed(1);
    };

    return (
      <div className="form-section">
        <div className="form-summary-grid">
          {last5.length > 0 && (
            <div className="form-period">
              <span className="form-period-label">Last 5</span>
              <span className="form-period-record">{calcRecord(last5)}</span>
              <span className="form-period-goals">{calcAvgGoals(last5)} GF/G</span>
            </div>
          )}
          {last10.length >= 6 && (
            <div className="form-period">
              <span className="form-period-label">Last 10</span>
              <span className="form-period-record">{calcRecord(last10)}</span>
              <span className="form-period-goals">{calcAvgGoals(last10)} GF/G</span>
            </div>
          )}
          {last20.length >= 11 && (
            <div className="form-period">
              <span className="form-period-label">Last 20</span>
              <span className="form-period-record">{calcRecord(last20)}</span>
              <span className="form-period-goals">{calcAvgGoals(last20)} GF/G</span>
            </div>
          )}
        </div>

        <div className="form-results-strip">
          {last10.map((g, i) => {
            const resultClass =
              g.result === 'W' || g.win
                ? 'result-win'
                : g.result === 'OTL' || g.otl
                  ? 'result-otl'
                  : 'result-loss';
            return (
              <div key={i} className={`form-result-dot ${resultClass}`} title={`${g.result || (g.win ? 'W' : 'L')} ${g.score_display || ''}`}>
                {g.result || (g.win ? 'W' : 'L')}
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

function GameDetail() {
  const { id } = useParams();
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('overview');

  const { data: game, loading, error } = useApi(fetchGameDetails, [id]);

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
          <div className="detail-team-abbrev">{awayAbbr}</div>
          <div className="detail-team-name">{awayTeamLabel}</div>
          {awayRecord && <div className="detail-team-record">{awayRecord}</div>}
        </div>

        <div className="game-detail-center">
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
        </div>

        <div className="game-detail-team home-team-detail">
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
