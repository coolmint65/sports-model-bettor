import { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import { Calendar, Radio, RefreshCw } from 'lucide-react';
import { format } from 'date-fns';
import GameCard from './GameCard';
import { fetchTodaySchedule, fetchLiveGames, regeneratePredictions } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { useWebSocketEvent } from '../hooks/useWebSocket';
import { isLiveStatus } from '../utils/teams';

const LIVE_POLL_INTERVAL = 5_000;
const IDLE_POLL_INTERVAL = 60_000;

function Dashboard() {
  const {
    data: scheduleData,
    loading: scheduleLoading,
    error: scheduleError,
    silentRefetch,
  } = useApi(fetchTodaySchedule);

  const [liveGames, setLiveGames] = useState([]);
  const [regenerating, setRegenerating] = useState(false);
  const [regenMessage, setRegenMessage] = useState('');
  const regeneratingRef = useRef(false);

  const pollLive = useCallback(async () => {
    try {
      const res = await fetchLiveGames();
      const games = res?.data?.games || res?.data || [];
      setLiveGames(games);
    } catch {
      // Silently fail
    }
  }, []);

  useWebSocketEvent('odds_update', useCallback(() => {
    silentRefetch();
    pollLive();
  }, [silentRefetch, pollLive]));

  useWebSocketEvent('predictions_update', useCallback(() => {
    silentRefetch();
  }, [silentRefetch]));

  const today = format(new Date(), 'EEEE, MMMM d, yyyy');
  const rawGames = scheduleData?.games || scheduleData || [];

  const prematchPickCache = useRef(new Map());
  const games = useMemo(() => {
    const currentGameIds = new Set(
      rawGames.map((g) => g.id || g.game_id).filter(Boolean)
    );
    for (const key of prematchPickCache.current.keys()) {
      const gid = parseInt(key.split(':')[0], 10);
      if (!currentGameIds.has(gid)) {
        prematchPickCache.current.delete(key);
      }
    }

    return rawGames.map((g) => {
      const gid = g.id || g.game_id;
      if (!gid) return g;

      if (g.top_pick) {
        prematchPickCache.current.set(`${gid}:pick`, g.top_pick);
      }
      if (g.top_prop) {
        prematchPickCache.current.set(`${gid}:prop`, g.top_prop);
      }

      const cachedPick = prematchPickCache.current.get(`${gid}:pick`);
      const cachedProp = prematchPickCache.current.get(`${gid}:prop`);

      if (!g.top_pick && cachedPick) {
        return { ...g, top_pick: cachedPick, top_prop: g.top_prop || cachedProp };
      }
      if (!g.top_prop && cachedProp) {
        return { ...g, top_prop: cachedProp };
      }
      return g;
    });
  }, [rawGames]);

  const todayHasLive = games.some((g) => isLiveStatus(g.status));
  const hasAnyLive = liveGames.length > 0 || todayHasLive;

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

  useEffect(() => {
    const onSynced = () => silentRefetch();
    window.addEventListener('data-synced', onSynced);
    return () => window.removeEventListener('data-synced', onSynced);
  }, [silentRefetch]);

  const liveGameMap = new Map();
  const scheduleMap = new Map();
  for (const g of games) {
    const gid = g.id || g.game_id;
    if (gid) scheduleMap.set(gid, g);
  }
  for (const g of liveGames) {
    const gid = g.id || g.game_id;
    if (gid) {
      const scheduleGame = scheduleMap.get(gid);
      if (!g.top_pick && scheduleGame?.top_pick) {
        g.top_pick = scheduleGame.top_pick;
      }
      if (!g.top_prop && scheduleGame?.top_prop) {
        g.top_prop = scheduleGame.top_prop;
      }
      liveGameMap.set(gid, g);
    }
  }

  const todayGameIds = new Set(games.map((g) => g.id || g.game_id));
  const extraLiveGames = liveGames.filter(
    (g) => !todayGameIds.has(g.id) && !todayGameIds.has(g.game_id)
  );

  const allLive = [
    ...games
      .filter((g) => isLiveStatus(g.status))
      .map((g) => liveGameMap.get(g.id || g.game_id) || g),
    ...extraLiveGames,
  ];

  // Prematch games only (not live, not final)
  const prematchGames = games.filter((g) => {
    const status = (g.status || '').toLowerCase();
    return !isLiveStatus(status) && status !== 'final' && status !== 'completed' && status !== 'off';
  });

  const handleRegenerate = useCallback(async () => {
    if (regeneratingRef.current) return;
    regeneratingRef.current = true;
    setRegenerating(true);
    setRegenMessage('Syncing schedule & odds...');
    try {
      const resp = await regeneratePredictions();
      const count = resp.data?.predictions_generated ?? 0;
      const msg = count > 0
        ? `Regenerated ${count} predictions`
        : 'No predictions generated (check data sync)';
      setRegenMessage(msg);
      await silentRefetch();
      window.dispatchEvent(new Event('data-synced'));
      setTimeout(() => setRegenMessage(''), 6000);
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || '';
      setRegenMessage(`Regeneration failed${detail ? `: ${detail}` : ''}`);
      setTimeout(() => setRegenMessage(''), 6000);
    } finally {
      regeneratingRef.current = false;
      setRegenerating(false);
    }
  }, [silentRefetch]);

  return (
    <div className="dashboard">
      <div className="dashboard-header">
        <div className="dashboard-title-section">
          <h1 className="dashboard-title">Today's Action</h1>
          <p className="dashboard-date">
            <Calendar size={16} />
            {today}
          </p>
        </div>
        <div className="dashboard-actions">
          <button
            className="btn btn-regen"
            onClick={handleRegenerate}
            disabled={regenerating}
            title="Sync schedule, fetch latest odds, and regenerate all predictions"
          >
            <RefreshCw size={14} className={regenerating ? 'spin' : ''} />
            {regenerating ? (regenMessage || 'Regenerating...') : 'Regenerate'}
          </button>
          {regenMessage && !regenerating && (
            <span className="regen-message">{regenMessage}</span>
          )}
        </div>
      </div>

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
              <GameCard key={game.game_id || game.id} game={game} section="live" />
            ))}
          </div>
        </section>
      )}

      {/* Today's Schedule */}
      <section className="section">
        <div className="section-header">
          <h2 className="section-title">Today's Schedule</h2>
          <span className="game-count">
            {prematchGames.length} {prematchGames.length === 1 ? 'Game' : 'Games'}
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

        {!scheduleLoading && !scheduleError && prematchGames.length === 0 && (
          <div className="empty-state">
            <Calendar size={48} />
            <p>No upcoming games scheduled for today</p>
          </div>
        )}

        {!scheduleLoading && !scheduleError && prematchGames.length > 0 && (
          <div className="games-grid">
            {prematchGames.map((game) => (
              <GameCard key={game.game_id || game.id} game={game} section="schedule" />
            ))}
          </div>
        )}
      </section>
    </div>
  );
}

export default Dashboard;
