import { useEffect, useRef, useState, useCallback } from 'react';
import { Calendar, TrendingUp, Radio } from 'lucide-react';
import { format } from 'date-fns';
import BestBets from './BestBets';
import GameCard from './GameCard';
import { fetchTodaySchedule, fetchLiveGames } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { useWebSocketEvent } from '../hooks/useWebSocket';
import { isLiveStatus } from '../utils/teams';

const LIVE_POLL_INTERVAL = 5_000; // 5 seconds when live
const IDLE_POLL_INTERVAL = 60_000; // 1 minute when no live games

function Dashboard() {
  const {
    data: scheduleData,
    loading: scheduleLoading,
    error: scheduleError,
    silentRefetch,
  } = useApi(fetchTodaySchedule);

  const [liveGames, setLiveGames] = useState([]);

  const pollLive = useCallback(async () => {
    try {
      const res = await fetchLiveGames();
      const games = res?.data?.games || res?.data || [];
      setLiveGames(games);
    } catch {
      // Silently fail — live section just won't show
    }
  }, []);

  // Instantly refetch when WebSocket pushes odds/predictions updates
  useWebSocketEvent('odds_update', useCallback(() => {
    silentRefetch();
    pollLive();
  }, [silentRefetch, pollLive]));

  const today = format(new Date(), 'EEEE, MMMM d, yyyy');
  const games = scheduleData?.games || scheduleData || [];

  const todayHasLive = games.some((g) => isLiveStatus(g.status));
  const hasAnyLive = liveGames.length > 0 || todayHasLive;

  // Always poll — faster when live, slower when idle.
  // This replaces the manual sync-only model.
  const intervalRef = useRef(null);
  useEffect(() => {
    pollLive();

    const interval = hasAnyLive ? LIVE_POLL_INTERVAL : IDLE_POLL_INTERVAL;
    intervalRef.current = setInterval(() => {
      silentRefetch();
      pollLive();
    }, interval);

    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [hasAnyLive, silentRefetch, pollLive]);

  // Also refresh immediately on manual sync
  useEffect(() => {
    const onSynced = () => silentRefetch();
    window.addEventListener('data-synced', onSynced);
    return () => window.removeEventListener('data-synced', onSynced);
  }, [silentRefetch]);

  // Build a lookup of live game data (from /schedule/live which has
  // the freshest odds via live odds sync).  Prefer this data over
  // /schedule/today for live games so odds timestamps stay current.
  const liveGameMap = new Map();
  for (const g of liveGames) {
    const gid = g.id || g.game_id;
    if (gid) liveGameMap.set(gid, g);
  }

  const todayGameIds = new Set(games.map((g) => g.id || g.game_id));
  const extraLiveGames = liveGames.filter(
    (g) => !todayGameIds.has(g.id) && !todayGameIds.has(g.game_id)
  );

  // For live games in today's schedule, prefer the /schedule/live data
  // which carries freshly-synced odds.
  const allLive = [
    ...games
      .filter((g) => isLiveStatus(g.status))
      .map((g) => liveGameMap.get(g.id || g.game_id) || g),
    ...extraLiveGames,
  ];

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

      {/* Live Games Section */}
      {allLive.length > 0 && (
        <section className="section live-section">
          <div className="section-header">
            <h2 className="section-title live-section-title">
              <Radio size={20} className="live-icon" />
              Live Now
            </h2>
            <span className="game-count live-count">
              {allLive.length} {allLive.length === 1 ? 'Game' : 'Games'}
            </span>
          </div>
          <div className="games-grid">
            {allLive.map((game) => (
              <GameCard key={game.game_id || game.id} game={game} />
            ))}
          </div>
        </section>
      )}

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
