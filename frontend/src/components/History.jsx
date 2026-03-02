import { useState, useMemo, useEffect, useCallback } from 'react';
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
  Filter,
  Trash2,
  RefreshCw,
  Layers,
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
import { fetchTrackedBets, deleteTrackedBet, settleTrackedBets, clearAllTrackedBets } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { confidencePct, formatBetType, formatPredictionValue } from '../utils/teams';

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
  const { data, loading, error, refetch, silentRefetch } = useApi(fetchTrackedBets);
  const [betTypeFilter, setBetTypeFilter] = useState('all');
  const [settling, setSettling] = useState(false);
  const [clearing, setClearing] = useState(false);

  // Listen for data syncs to auto-settle
  useEffect(() => {
    const onSynced = () => silentRefetch();
    window.addEventListener('data-synced', onSynced);
    return () => window.removeEventListener('data-synced', onSynced);
  }, [silentRefetch]);

  const bets = data?.bets || [];
  const totalBets = data?.total_bets || 0;
  const winsCount = data?.wins || 0;
  const lossesCount = data?.losses || 0;
  const pushesCount = data?.pushes || 0;
  const pendingCount = data?.pending || 0;
  const totalProfit = data?.total_profit || 0;
  const totalUnitsWagered = data?.total_units_wagered || 0;

  const totalGraded = winsCount + lossesCount;
  const winRate = totalGraded > 0 ? (winsCount / totalGraded) * 100 : 0;
  const roi = totalUnitsWagered > 0 ? (totalProfit / totalUnitsWagered) * 100 : 0;

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
    let settledCount = 0;
    let wins = 0;

    return filteredBets
      .filter((bet) => bet.result === 'win' || bet.result === 'loss')
      .reverse()
      .map((bet) => {
        const isWin = bet.result === 'win';
        const profit = bet.profit_loss || 0;
        cumulativeProfit += profit;
        settledCount += 1;
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
          winRate: settledCount > 0 ? parseFloat(((wins / settledCount) * 100).toFixed(1)) : 0,
          betNumber: settledCount,
        };
      });
  }, [filteredBets]);

  const bestStreak = useMemo(() => {
    let streak = 0;
    let maxStreak = 0;
    for (const bet of bets) {
      if (bet.result === 'win') {
        streak += 1;
        if (streak > maxStreak) maxStreak = streak;
      } else if (bet.result === 'loss') {
        streak = 0;
      }
    }
    return maxStreak;
  }, [bets]);

  const handleSettle = useCallback(async () => {
    setSettling(true);
    try {
      await settleTrackedBets();
      await refetch();
    } catch (err) {
      console.error('Failed to settle bets:', err);
    } finally {
      setSettling(false);
    }
  }, [refetch]);

  const handleClearAll = useCallback(async () => {
    if (!window.confirm('Clear all tracked bets? This cannot be undone.')) return;
    setClearing(true);
    try {
      await clearAllTrackedBets();
      await refetch();
    } catch (err) {
      console.error('Failed to clear bets:', err);
    } finally {
      setClearing(false);
    }
  }, [refetch]);

  const handleDelete = useCallback(async (id) => {
    try {
      await deleteTrackedBet(id);
      await silentRefetch();
    } catch (err) {
      console.error('Failed to delete bet:', err);
    }
  }, [silentRefetch]);

  return (
    <div className="history-page">
      <div className="history-header">
        <div>
          <h1 className="history-title">
            <BarChart3 size={28} />
            Bet Tracker
          </h1>
          <p className="history-subtitle">Track your picks from the dashboard and monitor performance</p>
        </div>
        <div className="history-actions">
          {pendingCount > 0 && (
            <button
              className="btn btn-settle"
              onClick={handleSettle}
              disabled={settling}
            >
              <RefreshCw size={14} className={settling ? 'spin' : ''} />
              {settling ? 'Settling...' : `Settle (${pendingCount})`}
            </button>
          )}
          {totalBets > 0 && (
            <button
              className="btn btn-clear"
              onClick={handleClearAll}
              disabled={clearing}
            >
              <Trash2 size={14} />
              Clear All
            </button>
          )}
        </div>
      </div>

      {loading && (
        <div className="loading-container large">
          <div className="loading-spinner"></div>
          <p>Loading tracked bets...</p>
        </div>
      )}

      {error && (
        <div className="error-container">
          <p>Failed to load tracked bets: {error}</p>
        </div>
      )}

      {!loading && !error && (
        <>
          {/* Stats Overview */}
          {totalBets > 0 && (
            <section className="history-stats-section">
              <div className="history-stats-grid">
                <StatCard
                  icon={Percent}
                  label="Win Rate"
                  value={totalGraded > 0 ? `${winRate.toFixed(1)}%` : '—'}
                  subValue={totalGraded > 0 ? `${winsCount}W - ${lossesCount}L` : 'No settled bets yet'}
                  color={winRate >= 55 ? '#00ff88' : winRate >= 50 ? '#ffd700' : totalGraded > 0 ? '#ff5252' : '#4fc3f7'}
                />
                <StatCard
                  icon={DollarSign}
                  label="Total Profit"
                  value={`${totalProfit >= 0 ? '+' : ''}${totalProfit.toFixed(2)}u`}
                  subValue={totalUnitsWagered > 0 ? `${roi.toFixed(1)}% ROI on ${totalUnitsWagered.toFixed(1)}u wagered` : undefined}
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
          )}

          {/* Profit Chart */}
          {chartData.length > 1 && (
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
                      {(() => {
                        const profits = chartData.map(d => d.profit);
                        const maxP = Math.max(...profits, 0);
                        const minP = Math.min(...profits, 0);
                        const range = maxP - minP || 1;
                        // zeroOffset = fraction of the Y-axis where 0 sits (0=top, 1=bottom)
                        const zeroOffset = maxP / range;
                        return (
                          <>
                            <linearGradient id="profitGradientSplit" x1="0" y1="0" x2="0" y2="1">
                              <stop offset="0%" stopColor="#00ff88" stopOpacity={0.35} />
                              <stop offset={`${(zeroOffset * 100).toFixed(1)}%`} stopColor="#00ff88" stopOpacity={0.05} />
                              <stop offset={`${(zeroOffset * 100).toFixed(1)}%`} stopColor="#ff5252" stopOpacity={0.05} />
                              <stop offset="100%" stopColor="#ff5252" stopOpacity={0.35} />
                            </linearGradient>
                            <linearGradient id="profitStrokeSplit" x1="0" y1="0" x2="0" y2="1">
                              <stop offset={`${(zeroOffset * 100).toFixed(1)}%`} stopColor="#00ff88" stopOpacity={1} />
                              <stop offset={`${(zeroOffset * 100).toFixed(1)}%`} stopColor="#ff5252" stopOpacity={1} />
                            </linearGradient>
                          </>
                        );
                      })()}
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
                      stroke="url(#profitStrokeSplit)"
                      strokeWidth={2}
                      fill="url(#profitGradientSplit)"
                    />
                  </AreaChart>
                </ResponsiveContainer>
              </div>
            </section>
          )}

          {/* Filter */}
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

          {/* Bet History Table */}
          <section className="history-table-section">
            <div className="section-header">
              <h2 className="section-title">Tracked Bets</h2>
              <span className="bet-count">{filteredBets.length} Bet{filteredBets.length !== 1 ? 's' : ''}</span>
            </div>

            {filteredBets.length === 0 ? (
              <div className="empty-state">
                <BarChart3 size={48} />
                <p>No tracked bets yet. Use the "Track" button on best bets to start tracking your picks.</p>
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
                  <span className="col-units">Units</span>
                  <span className="col-phase">Phase</span>
                  <span className="col-result">Result</span>
                  <span className="col-profit">Profit</span>
                  <span className="col-action"></span>
                </div>
                <div className="history-table-body">
                  {filteredBets.map((bet) => {
                    const isWin = bet.result === 'win';
                    const isLoss = bet.result === 'loss';
                    const isPending = !bet.result;
                    const profit = bet.profit_loss || 0;
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

                    const oddsStr = formatAmericanOdds(bet.odds);
                    const phase = bet.phase || 'prematch';

                    return (
                      <div
                        className={`history-table-row ${isWin ? 'row-win' : isLoss ? 'row-loss' : ''}`}
                        key={bet.id}
                      >
                        <span className="col-date">{displayDate}</span>
                        <span className="col-game">
                          {bet.away_team_abbr || bet.away_team_name || '?'} @{' '}
                          {bet.home_team_abbr || bet.home_team_name || '?'}
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
                        <span className="col-units">{(bet.units || 1).toFixed(1)}u</span>
                        <span className={`col-phase phase-tag phase-${phase}`}>
                          <Layers size={10} />
                          {phase === 'live' ? 'Live' : 'Pre'}
                        </span>
                        <span className={`col-result ${isWin ? 'result-win' : isLoss ? 'result-loss' : 'result-pending'}`}>
                          {isWin && <CheckCircle size={14} />}
                          {isLoss && <XCircle size={14} />}
                          {isPending && <Clock size={14} />}
                          {isWin ? 'Win' : isLoss ? 'Loss' : 'Pending'}
                        </span>
                        <span className={`col-profit ${!isPending && profit >= 0 ? 'profit-positive' : !isPending ? 'profit-negative' : ''}`}>
                          {isPending ? '—' : `${profit >= 0 ? '+' : ''}${typeof profit === 'number' ? profit.toFixed(2) : profit}u`}
                        </span>
                        <span className="col-action">
                          <button
                            className="btn-icon btn-delete"
                            onClick={() => handleDelete(bet.id)}
                            title="Remove bet"
                          >
                            <Trash2 size={14} />
                          </button>
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
