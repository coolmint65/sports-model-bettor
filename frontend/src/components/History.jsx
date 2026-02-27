import React, { useState, useMemo } from 'react';
import {
  BarChart3,
  TrendingUp,
  DollarSign,
  Target,
  Award,
  CheckCircle,
  XCircle,
  Percent,
  Activity,
  Filter,
} from 'lucide-react';
import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Area,
  AreaChart,
  ReferenceLine,
} from 'recharts';
import { format, parseISO } from 'date-fns';
import { fetchPredictionHistory, fetchPredictionStats } from '../utils/api';
import { useApi } from '../hooks/useApi';
import PredictionCard from './PredictionCard';
import { teamName, confidencePct, formatBetType, formatPredictionValue } from '../utils/teams';

function StatCard({ icon: Icon, label, value, subValue, color, className }) {
  return (
    <div className={`history-stat-card ${className || ''}`}>
      <div className="stat-card-icon" style={{ color: color || '#4fc3f7' }}>
        <Icon size={22} />
      </div>
      <div className="stat-card-content">
        <span className="stat-card-value" style={{ color: color || '#fff' }}>
          {value}
        </span>
        <span className="stat-card-label">{label}</span>
        {subValue && <span className="stat-card-sub">{subValue}</span>}
      </div>
    </div>
  );
}

function CustomTooltip({ active, payload, label }) {
  if (active && payload && payload.length) {
    return (
      <div className="chart-tooltip">
        <p className="chart-tooltip-label">{label}</p>
        {payload.map((entry, index) => (
          <p key={index} className="chart-tooltip-value" style={{ color: entry.color }}>
            {entry.name}: {typeof entry.value === 'number' ? entry.value.toFixed(2) : entry.value}
          </p>
        ))}
      </div>
    );
  }
  return null;
}

function History() {
  const { data: historyData, loading: historyLoading, error: historyError } = useApi(fetchPredictionHistory);
  const { data: statsData, loading: statsLoading, error: statsError } = useApi(fetchPredictionStats);
  const [betTypeFilter, setBetTypeFilter] = useState('all');

  const stats = statsData || {};
  const history = historyData?.entries || historyData?.predictions || historyData?.history || (Array.isArray(historyData) ? historyData : []);

  const betTypes = useMemo(() => {
    const types = new Set();
    history.forEach((pred) => {
      const type = pred.bet_type || pred.type;
      if (type) types.add(type);
    });
    return ['all', ...Array.from(types)];
  }, [history]);

  const filteredHistory = useMemo(() => {
    if (betTypeFilter === 'all') return history;
    return history.filter((pred) => (pred.bet_type || pred.type) === betTypeFilter);
  }, [history, betTypeFilter]);

  const chartData = useMemo(() => {
    let cumulativeProfit = 0;
    let totalBets = 0;
    let wins = 0;

    return filteredHistory
      .filter((pred) => pred.outcome || pred.result)
      .sort((a, b) => {
        const dateA = a.date || a.game_date || '';
        const dateB = b.date || b.game_date || '';
        return dateA.localeCompare(dateB);
      })
      .map((pred) => {
        const outcome = (pred.outcome || pred.result || '').toLowerCase();
        const profit = pred.profit || pred.units || 0;
        const isWin = outcome === 'win' || outcome === 'correct' || outcome === 'hit';

        cumulativeProfit += profit;
        totalBets += 1;
        if (isWin) wins += 1;

        const dateStr = pred.date || pred.game_date || '';
        let displayDate = dateStr;
        try {
          if (dateStr) {
            displayDate = format(parseISO(dateStr), 'MMM d');
          }
        } catch {
          // keep original
        }

        return {
          date: displayDate,
          profit: parseFloat(cumulativeProfit.toFixed(2)),
          winRate: totalBets > 0 ? parseFloat(((wins / totalBets) * 100).toFixed(1)) : 0,
          betNumber: totalBets,
        };
      });
  }, [filteredHistory]);

  const rawWinRate = stats.hit_rate || stats.win_rate || stats.overall_win_rate || 0;
  const winRate = rawWinRate <= 1 ? rawWinRate * 100 : rawWinRate;
  const totalBets = stats.total_predictions || stats.total_bets || history.length || 0;
  const totalProfit = stats.total_profit || stats.profit || stats.total_units || 0;
  const rawRoi = stats.roi || stats.return_on_investment || 0;
  const roi = rawRoi <= 1 && rawRoi !== 0 ? rawRoi * 100 : rawRoi;
  const rawAvgConf = stats.avg_confidence || stats.average_confidence || 0;
  const avgConfidence = rawAvgConf <= 1 ? rawAvgConf * 100 : rawAvgConf;
  const bestStreak = stats.best_streak || stats.longest_win_streak || 0;

  const loading = historyLoading || statsLoading;
  const hasError = historyError || statsError;

  return (
    <div className="history-page">
      <div className="history-header">
        <h1 className="history-title">
          <BarChart3 size={28} />
          Performance History
        </h1>
        <p className="history-subtitle">Track the model's prediction accuracy and profitability over time</p>
      </div>

      {loading && (
        <div className="loading-container large">
          <div className="loading-spinner"></div>
          <p>Loading performance data...</p>
        </div>
      )}

      {hasError && (
        <div className="error-container">
          <p>Failed to load performance data: {historyError || statsError}</p>
        </div>
      )}

      {!loading && !hasError && (
        <>
          {/* Stats Overview Cards */}
          <section className="history-stats-section">
            <div className="history-stats-grid">
              <StatCard
                icon={Percent}
                label="Win Rate"
                value={`${typeof winRate === 'number' ? winRate.toFixed(1) : winRate}%`}
                subValue={`${totalBets} total bets`}
                color={winRate >= 55 ? '#00ff88' : winRate >= 50 ? '#ffd700' : '#ff5252'}
              />
              <StatCard
                icon={DollarSign}
                label="Total Profit"
                value={`${totalProfit >= 0 ? '+' : ''}${typeof totalProfit === 'number' ? totalProfit.toFixed(2) : totalProfit}u`}
                subValue={`${typeof roi === 'number' ? roi.toFixed(1) : roi}% ROI`}
                color={totalProfit >= 0 ? '#00ff88' : '#ff5252'}
              />
              <StatCard
                icon={Target}
                label="Total Bets"
                value={totalBets}
                color="#4fc3f7"
              />
              <StatCard
                icon={Activity}
                label="Avg Confidence"
                value={`${typeof avgConfidence === 'number' ? avgConfidence.toFixed(1) : avgConfidence}%`}
                color="#4fc3f7"
              />
              {bestStreak > 0 && (
                <StatCard
                  icon={Award}
                  label="Best Streak"
                  value={`${bestStreak}W`}
                  color="#ffd700"
                />
              )}
            </div>
          </section>

          {/* Profit Chart */}
          {chartData.length > 0 && (
            <section className="history-chart-section">
              <div className="section-header">
                <h2 className="section-title">
                  <TrendingUp size={20} />
                  Cumulative Profit
                </h2>
              </div>
              <div className="chart-container">
                <ResponsiveContainer width="100%" height={350}>
                  <AreaChart data={chartData} margin={{ top: 10, right: 30, left: 10, bottom: 10 }}>
                    <defs>
                      <linearGradient id="profitGradient" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#00ff88" stopOpacity={0.3} />
                        <stop offset="95%" stopColor="#00ff88" stopOpacity={0} />
                      </linearGradient>
                      <linearGradient id="profitGradientNeg" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#ff5252" stopOpacity={0} />
                        <stop offset="95%" stopColor="#ff5252" stopOpacity={0.3} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#2a2a4a" />
                    <XAxis
                      dataKey="date"
                      stroke="#666"
                      tick={{ fill: '#888', fontSize: 12 }}
                      interval="preserveStartEnd"
                    />
                    <YAxis
                      stroke="#666"
                      tick={{ fill: '#888', fontSize: 12 }}
                      tickFormatter={(val) => `${val}u`}
                    />
                    <Tooltip content={<CustomTooltip />} />
                    <ReferenceLine y={0} stroke="#555" strokeDasharray="3 3" />
                    <Area
                      type="monotone"
                      dataKey="profit"
                      name="Profit"
                      stroke="#00ff88"
                      strokeWidth={2}
                      fill="url(#profitGradient)"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </section>
          )}

          {/* Bet Type Filter */}
          {betTypes.length > 1 && (
            <div className="history-filter-bar">
              <Filter size={16} />
              <span className="filter-label">Filter by type:</span>
              {betTypes.map((type) => (
                <button
                  key={type}
                  className={`filter-btn ${betTypeFilter === type ? 'filter-active' : ''}`}
                  onClick={() => setBetTypeFilter(type)}
                >
                  {type === 'all' ? 'All Bets' : formatBetType(type)}
                </button>
              ))}
            </div>
          )}

          {/* Past Predictions Table */}
          <section className="history-table-section">
            <div className="section-header">
              <h2 className="section-title">Prediction History</h2>
              <span className="bet-count">{filteredHistory.length} Predictions</span>
            </div>

            {filteredHistory.length === 0 ? (
              <div className="empty-state">
                <BarChart3 size={48} />
                <p>No prediction history available yet.</p>
              </div>
            ) : (
              <div className="history-table">
                <div className="history-table-header">
                  <span className="col-date">Date</span>
                  <span className="col-game">Game</span>
                  <span className="col-type">Type</span>
                  <span className="col-pick">Pick</span>
                  <span className="col-confidence">Conf.</span>
                  <span className="col-result">Result</span>
                  <span className="col-profit">Profit</span>
                </div>
                <div className="history-table-body">
                  {filteredHistory.map((pred, index) => {
                    const outcome = (pred.outcome || pred.result || '').toLowerCase();
                    const isWin = outcome === 'win' || outcome === 'correct' || outcome === 'hit';
                    const isLoss = outcome === 'loss' || outcome === 'incorrect' || outcome === 'miss';
                    const profit = pred.profit || pred.units || 0;
                    const confidence = confidencePct(pred.confidence);
                    const dateStr = pred.date || pred.game_date || '';
                    let displayDate = dateStr;
                    try {
                      if (dateStr) {
                        displayDate = format(parseISO(String(dateStr)), 'MMM d');
                      }
                    } catch {
                      // keep original
                    }

                    return (
                      <div
                        className={`history-table-row ${isWin ? 'row-win' : isLoss ? 'row-loss' : ''}`}
                        key={pred.id || index}
                      >
                        <span className="col-date">{displayDate}</span>
                        <span className="col-game">
                          {teamName(pred.away_team, '?')} @{' '}
                          {teamName(pred.home_team, '?')}
                        </span>
                        <span className="col-type">{formatBetType(pred.bet_type || pred.type)}</span>
                        <span className="col-pick">{formatPredictionValue(pred.prediction_value || pred.pick || pred.selection)}</span>
                        <span className="col-confidence">
                          <span
                            className="confidence-dot"
                            style={{
                              backgroundColor: confidence >= 70 ? '#00ff88' : confidence >= 55 ? '#ffd700' : '#ff5252',
                            }}
                          ></span>
                          {confidence.toFixed(0)}%
                        </span>
                        <span className={`col-result ${isWin ? 'result-win' : isLoss ? 'result-loss' : 'result-pending'}`}>
                          {isWin && <CheckCircle size={14} />}
                          {isLoss && <XCircle size={14} />}
                          {pred.outcome || pred.result || 'Pending'}
                        </span>
                        <span className={`col-profit ${profit >= 0 ? 'profit-positive' : 'profit-negative'}`}>
                          {profit >= 0 ? '+' : ''}{typeof profit === 'number' ? profit.toFixed(2) : profit}u
                        </span>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </section>
        </>
      )}
    </div>
  );
}

export default History;
