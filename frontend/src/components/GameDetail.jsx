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
import { format, parseISO } from 'date-fns';
import { fetchGameDetails } from '../utils/api';
import { useApi } from '../hooks/useApi';
import PredictionCard from './PredictionCard';

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
    const dateStr = game.start_time || game.datetime || game.date;
    if (!dateStr) return 'TBD';
    const date = typeof dateStr === 'string' ? parseISO(dateStr) : new Date(dateStr);
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
  const away = game.away_stats || game.away_team_stats || {};
  const home = game.home_stats || game.home_team_stats || {};
  const awayRecord = game.away_record || away.record || '';
  const homeRecord = game.home_record || home.record || '';

  const stats = [
    { label: 'Goals/Game', away: away.goals_per_game, home: home.goals_per_game, higher: true },
    { label: 'Goals Against/Game', away: away.goals_against_per_game, home: home.goals_against_per_game, higher: false },
    { label: 'Power Play %', away: away.pp_pct || away.power_play_pct, home: home.pp_pct || home.power_play_pct, higher: true },
    { label: 'Penalty Kill %', away: away.pk_pct || away.penalty_kill_pct, home: home.pk_pct || home.penalty_kill_pct, higher: true },
    { label: 'Shots/Game', away: away.shots_per_game, home: home.shots_per_game, higher: true },
    { label: 'Shots Against/Game', away: away.shots_against_per_game, home: home.shots_against_per_game, higher: false },
    { label: 'Faceoff %', away: away.faceoff_pct, home: home.faceoff_pct, higher: true },
    { label: 'Save %', away: away.save_pct, home: home.save_pct, higher: true },
    { label: 'Corsi For %', away: away.corsi_for_pct || away.cf_pct, home: home.corsi_for_pct || home.cf_pct, higher: true },
    { label: 'xGF/Game', away: away.xgf_per_game || away.expected_goals_for, home: home.xgf_per_game || home.expected_goals_for, higher: true },
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
  const h2h = game.h2h || game.head_to_head || [];
  const h2hSummary = game.h2h_summary || null;

  return (
    <div className="tab-content h2h-tab">
      {h2hSummary && (
        <div className="h2h-summary">
          <div className="h2h-summary-grid">
            {h2hSummary.total_games != null && (
              <div className="h2h-stat-box">
                <span className="h2h-stat-value">{h2hSummary.total_games}</span>
                <span className="h2h-stat-label">Games</span>
              </div>
            )}
            {h2hSummary.away_wins != null && (
              <div className="h2h-stat-box">
                <span className="h2h-stat-value">{h2hSummary.away_wins}</span>
                <span className="h2h-stat-label">Away Wins</span>
              </div>
            )}
            {h2hSummary.home_wins != null && (
              <div className="h2h-stat-box">
                <span className="h2h-stat-value">{h2hSummary.home_wins}</span>
                <span className="h2h-stat-label">Home Wins</span>
              </div>
            )}
            {h2hSummary.avg_total_goals != null && (
              <div className="h2h-stat-box">
                <span className="h2h-stat-value">{h2hSummary.avg_total_goals.toFixed(1)}</span>
                <span className="h2h-stat-label">Avg Total Goals</span>
              </div>
            )}
          </div>
        </div>
      )}

      {h2h.length > 0 ? (
        <div className="h2h-games-list">
          <h3 className="subsection-title">Recent Meetings</h3>
          <div className="h2h-table">
            <div className="h2h-table-header">
              <span>Date</span>
              <span>Matchup</span>
              <span>Score</span>
            </div>
            {h2h.map((meeting, index) => (
              <div className="h2h-table-row" key={index}>
                <span className="h2h-date">
                  {meeting.date
                    ? format(parseISO(meeting.date), 'MMM d, yyyy')
                    : 'N/A'}
                </span>
                <span className="h2h-matchup">
                  {meeting.away_team || 'Away'} @ {meeting.home_team || 'Home'}
                </span>
                <span className="h2h-score">
                  {meeting.away_score ?? '-'} - {meeting.home_score ?? '-'}
                </span>
              </div>
            ))}
          </div>
        </div>
      ) : (
        <div className="empty-state">
          <Users size={48} />
          <p>No head-to-head history available.</p>
        </div>
      )}
    </div>
  );
}

function FormTab({ game }) {
  const awayForm = game.away_form || game.away_recent || [];
  const homeForm = game.home_form || game.home_recent || [];
  const awayTeam = game.away_team || 'Away';
  const homeTeam = game.home_team || 'Home';

  const renderFormList = (form, teamName) => {
    if (!form || form.length === 0) {
      return (
        <div className="empty-state-small">
          <p>No recent form data for {teamName}</p>
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
          <h3 className="form-team-title">{awayTeam}</h3>
          {renderFormList(awayForm, awayTeam)}
        </div>
        <div className="form-team-section">
          <h3 className="form-team-title">{homeTeam}</h3>
          {renderFormList(homeForm, homeTeam)}
        </div>
      </div>
    </div>
  );
}

function PeriodsTab({ game }) {
  const periodData = game.period_analysis || game.period_scoring || game.periods || null;
  const awayTeam = game.away_team || 'Away';
  const homeTeam = game.home_team || 'Home';

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
          <span>{awayTeam}</span>
          <span>{homeTeam}</span>
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

  const awayTeam = game.away_team || game.teams?.away?.name || 'Away';
  const homeTeam = game.home_team || game.teams?.home?.name || 'Home';
  const awayAbbrev = game.away_abbreviation || game.teams?.away?.abbreviation || awayTeam.substring(0, 3).toUpperCase();
  const homeAbbrev = game.home_abbreviation || game.teams?.home?.abbreviation || homeTeam.substring(0, 3).toUpperCase();
  const awayRecord = game.away_record || game.away_stats?.record || '';
  const homeRecord = game.home_record || game.home_stats?.record || '';
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
          <div className="detail-team-abbrev">{awayAbbrev}</div>
          <div className="detail-team-name">{awayTeam}</div>
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
          <div className="detail-team-abbrev">{homeAbbrev}</div>
          <div className="detail-team-name">{homeTeam}</div>
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
