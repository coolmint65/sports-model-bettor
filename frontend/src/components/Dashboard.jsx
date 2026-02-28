import React, { useEffect, useRef } from 'react';
import { Calendar, TrendingUp } from 'lucide-react';
import { format } from 'date-fns';
import BestBets from './BestBets';
import GameCard from './GameCard';
import { fetchTodaySchedule } from '../utils/api';
import { useApi } from '../hooks/useApi';

const LIVE_POLL_INTERVAL = 30_000; // 30 seconds

function Dashboard() {
  const {
    data: scheduleData,
    loading: scheduleLoading,
    error: scheduleError,
    refetch,
  } = useApi(fetchTodaySchedule);

  const today = format(new Date(), 'EEEE, MMMM d, yyyy');
  const games = scheduleData?.games || scheduleData || [];

  // Auto-poll for live score updates when any game is in progress
  const hasLive = games.some((g) => {
    const s = (g.status || '').toLowerCase();
    return s === 'in_progress' || s === 'live' || s === 'active';
  });

  const intervalRef = useRef(null);
  useEffect(() => {
    if (hasLive) {
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
  }, [hasLive, refetch]);

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <div className="dashboard-title-section">
          <h1 className="dashboard-title">
            <TrendingUp size={28} />
            Today's Action
          </h1>
          <p className="dashboard-date">
            <Calendar size={16} />
            {today}
          </p>
        </div>
      </div>

      {/* Best Bets Section */}
      <section className="section">
        <BestBets />
      </section>

      {/* Today's Schedule */}
      <section className="section">
        <div className="section-header">
          <h2 className="section-title">Today's Schedule</h2>
          <span className="game-count">
            {games.length} {games.length === 1 ? 'Game' : 'Games'}
          </span>
        </div>

        {scheduleLoading && (
          <div className="loading-container">
            <div className="loading-spinner"></div>
            <p>Loading schedule...</p>
          </div>
        )}

        {scheduleError && (
          <div className="error-container">
            <p>Failed to load schedule: {scheduleError}</p>
          </div>
        )}

        {!scheduleLoading && !scheduleError && games.length === 0 && (
          <div className="empty-state">
            <Calendar size={48} />
            <p>No games scheduled for today</p>
          </div>
        )}

        {!scheduleLoading && !scheduleError && games.length > 0 && (
          <div className="games-grid">
            {games.map((game) => (
              <GameCard key={game.game_id || game.id} game={game} />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

export default Dashboard;
