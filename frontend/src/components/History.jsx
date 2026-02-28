import React, { useState, useMemo } from 'react';
import {
  BarChart3,
  TrendingUp,
  DollarSign,
  Target,
  Award,
  CheckCircle,
  XCircle,
  Clock,
  Percent,
  Activity,
  Filter,
} from 'lucide-react';
import {
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

function formatAmericanOdds(odds) {
  if (odds == null) return null;
  const v = Math.round(odds);
  return v > 0 ? `+${v}` : `${v}`;
}

function History() {
  const { data: historyData, loading: historyLoading, error: historyError } = useApi(fetchPredictionHistory);
  const { data: statsData, loading: statsLoading, error: statsError } = useApi(fetchPredictionStats);
  const [betTypeFilter, setBetTypeFilter] = useState('all');

  const stats = statsData || {};
  const bets = historyData?.bets || [];

  const betTypes = useMemo(() => {
    const types = new Set();
    bets.forEach((bet) => {
      if (bet.bet_type) types.add(bet.bet_type);
    });
    return ['all', ...Array.from(types)];
  }, [bets]);

  const filteredBets = useMemo(() => {
    if (betTypeFilter === 'all') return bets;
    return bets.filter((bet) => bet.bet_type === betTypeFilter);
  }, [bets, betTypeFilter]);

  // Chart data: cumulative profit over settled bets
  const chartData = useMemo(() => {
    let cumulativeProfit = 0;
    let totalSettled = 0;
    let wins = 0;

    return filteredBets
      .filter((bet) => bet.outcome === 'Win' || bet.outcome === 'Loss')
      .reverse() // oldest first for chart
      .map((bet) => {
        const isWin = bet.outcome === 'Win';
        const profit = bet.profit || 0;
        cumulativeProfit += profit;
        totalSettled += 1;
        if (isWin) wins += 1;

        const dateStr = bet.game_date || '';
        let displayDate = dateStr;
        try {
          if (dateStr) {
            displayDate = format(parseISO(String(dateStr)), 'MMM d');
          }
        } catch {
          // keep original
        }

        return {
          date: displayDate,
          profit: parseFloat(cumulativeProfit.toFixed(2)),
          winRate: totalSettled > 0 ? parseFloat(((wins / totalSettled) * 100).toFixed(1)) : 0,
          betNumber: totalSettled,
        };
      });
  }, [filteredBets]);

  const totalBets = historyData?.total_bets || bets.length || 0;
  const winsCount = historyData?.wins || 0;
  const lossesCount = historyData?.losses || 0;
  const pendingCount = historyData?.pending || 0;
  const totalGraded = winsCount + lossesCount;
  const winRate = totalGraded > 0 ? (winsCount / totalGraded) * 100 : 0;
  const totalProfit = historyData?.total_profit || 0;
  const roi = totalGraded > 0 ? (totalProfit / totalGraded) * 100 : 0;

  // Compute best streak from settled bets
  const bestStreak = useMemo(() => {
    let streak = 0;
    let maxStreak = 0;
    for (const bet of bets) {
      if (bet.outcome === 'Win') {
        streak += 1;
        if (streak > maxStreak) maxStreak = streak;
      } else if (bet.outcome === 'Loss') {
        streak = 0;
      }
    }
    return maxStreak;
  }, [bets]);

  const loading = historyLoading || statsLoading;
  const hasError = historyError || statsError;

  return (
    <div className="history-page">
      <div className="history-header">
        <h1 className="history-title">
          <BarChart3 size={28} />
          Performance History
        </h1>
        <p className="history-subtitle">Best bet per game — tracking the model's top pick and its results</p>
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
                value={`${winRate.toFixed(1)}%`}
                subValue={`${winsCount}W - ${lossesCount}L`}
                color={winRate >= 55 ? '#00ff88' : winRate >= 50 ? '#ffd700' : '#ff5252'}
              />
              <StatCard
                icon={DollarSign}
                label="Total Profit"
                value={`${totalProfit >= 0 ? '+' : ''}${totalProfit.toFixed(2)}u`}
                subValue={`${roi.toFixed(1)}% ROI`}
                color={totalProfit >= 0 ? '#00ff88' : '#ff5252'}
              />
              <StatCard
                icon={Target}
                label="Total Bets"
                value={totalBets}
                subValue={pendingCount > 0 ? `${pendingCount} pending` : undefined}
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

          {/* Best Bets History Table */}
          <section className="history-table-section">
            <div className="section-header">
              <h2 className="section-title">Best Bet Per Game</h2>
              <span className="bet-count">{filteredBets.length} Bets</span>
            </div>

            {filteredBets.length === 0 ? (
              <div className="empty-state">
                <BarChart3 size={48} />
                <p>No bet history available yet.</p>
              </div>
            ) : (
              <div className="history-table">
                <div className="history-table-header">
                  <span className="col-date">Date</span>
                  <span className="col-game">Game</span>
                  <span className="col-type">Type</span>
                  <span className="col-pick">Pick</span>
                  <span className="col-odds">Odds</span>
                  <span className="col-confidence">Conf.</span>
                  <span className="col-result">Result</span>
                  <span className="col-profit">Profit</span>
                </div>
                <div className="history-table-body">
                  {filteredBets.map((bet, index) => {
                    const isWin = bet.outcome === 'Win';
                    const isLoss = bet.outcome === 'Loss';
                    const isPending = !isWin && !isLoss;
                    const profit = bet.profit || 0;
                    const confidence = confidencePct(bet.confidence);
                    const dateStr = bet.game_date || '';
                    let displayDate = dateStr;
                    try {
                      if (dateStr) {
                        displayDate = format(parseISO(String(dateStr)), 'MMM d');
                      }
                    } catch {
                      // keep original
                    }

                    const oddsStr = formatAmericanOdds(bet.odds_display);

                    return (
                      <div
                        className={`history-table-row ${isWin ? 'row-win' : isLoss ? 'row-loss' : ''}`}
                        key={bet.id || index}
                      >
                        <span className="col-date">{displayDate}</span>
                        <span className="col-game">
                          {teamName(bet.away_team, '?')} @{' '}
                          {teamName(bet.home_team, '?')}
                        </span>
                        <span className="col-type">{formatBetType(bet.bet_type)}</span>
                        <span className="col-pick">{formatPredictionValue(bet.prediction_value)}</span>
                        <span className="col-odds">{oddsStr || '—'}</span>
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
                          {isPending && <Clock size={14} />}
                          {bet.outcome || 'Pending'}
                        </span>
                        <span className={`col-profit ${!isPending && profit >= 0 ? 'profit-positive' : !isPending ? 'profit-negative' : ''}`}>
                          {isPending ? '—' : `${profit >= 0 ? '+' : ''}${typeof profit === 'number' ? profit.toFixed(2) : profit}u`}
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
