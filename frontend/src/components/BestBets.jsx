import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Trophy, TrendingUp, Target, Star, ChevronRight, Radio, Plus, Check, Layers, RefreshCw } from 'lucide-react';
import { fetchBestBets, trackBet, fetchTrackedBets, regeneratePredictions } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { useWebSocketEvent } from '../hooks/useWebSocket';
import { teamName, teamAbbrev, confidencePct, formatBetType, formatPredictionValue, isLiveStatus } from '../utils/teams';
import { formatAmericanOdds, formatOddsFromProb, getConfidenceColor } from '../utils/formatting';

const BEST_BETS_POLL_INTERVAL = 60_000; // 60 seconds

function BestBetCard({ bet, rank, isFeatured, onTrack, tracked }) {
  const navigate = useNavigate();
  const confidence = confidencePct(bet.confidence);
  const edge = confidencePct(bet.edge);
  const confColor = getConfidenceColor(confidence);
  const live = isLiveStatus(bet.game_status);
  const phase = bet.phase || 'prematch';
  const oddsDisplay = bet.odds_display != null
    ? formatAmericanOdds(bet.odds_display)
    : formatOddsFromProb(bet.odds_implied_prob);

  const awayName = teamName(bet.away_team, 'Away');
  const homeName = teamName(bet.home_team, 'Home');
  const homeAbbr = teamAbbrev(bet.home_team);
  const awayAbbr = teamAbbrev(bet.away_team);

  const handleClick = () => {
    if (bet.game_id) {
      navigate(`/games/${bet.game_id}`);
    }
  };

  const handleTrack = (e) => {
    e.stopPropagation();
    if (!tracked && onTrack) {
      onTrack(bet);
    }
  };

  return (
    <div
      className={`best-bet-card ${isFeatured ? 'best-bet-featured' : ''}`}
      onClick={handleClick}
      role="button"
      tabIndex={0}
    >
      <div className="best-bet-badges">
        {isFeatured && (
          <div className="best-bet-badge">
            <Star size={14} />
            BEST BET
          </div>
        )}

        {live && (
          <div className="best-bet-live-badge">
            <Radio size={12} />
            LIVE
          </div>
        )}

        <div className={`best-bet-phase-badge phase-${phase}`}>
          <Layers size={11} />
          {phase === 'live' ? 'LIVE PICK' : 'PREMATCH'}
        </div>
      </div>

      <div className="best-bet-rank">
        <span className="rank-number">#{rank}</span>
      </div>

      <div className="best-bet-content">
        <div className="best-bet-matchup">
          <span className="best-bet-teams">
            {awayName} @ {homeName}
          </span>
        </div>

        <div className="best-bet-pick">
          <Target size={16} />
          <span className="pick-type">{formatBetType(bet.bet_type || bet.type)}</span>
        </div>

        <div className="best-bet-selection">
          {bet.line_display || formatPredictionValue(bet.prediction_value || bet.pick || bet.selection, homeAbbr, awayAbbr)}
        </div>

        <div className="best-bet-metrics">
          <div className="metric">
            <span className="metric-label">Confidence</span>
            <div className="confidence-bar-container">
              <div
                className="confidence-bar"
                style={{
                  width: `${Math.min(confidence, 100)}%`,
                  backgroundColor: confColor,
                }}
              ></div>
            </div>
            <span className="metric-value" style={{ color: confColor }}>
              {confidence.toFixed(1)}%
            </span>
          </div>

          {bet.edge != null && (
          <div className="metric">
            <span className="metric-label">Edge</span>
            <span className="metric-value edge-value">
              <TrendingUp size={14} />
              {edge > 0 ? '+' : ''}
              {edge.toFixed(1)}%
            </span>
          </div>
          )}

          <div className="metric">
            <span className="metric-label">Odds</span>
            <span className="metric-value odds-value">
              {oddsDisplay || '—'}
            </span>
          </div>

        </div>

        {bet.reasoning && (
          <p className="best-bet-reasoning">
            {bet.reasoning}
          </p>
        )}
      </div>

      <div className="best-bet-actions">
        <button
          className={`btn-track ${tracked ? 'btn-tracked' : ''}`}
          onClick={handleTrack}
          disabled={tracked}
          title={tracked ? 'Already tracked' : 'Track this bet'}
        >
          {tracked ? <Check size={14} /> : <Plus size={14} />}
          {tracked ? 'Tracked' : 'Track'}
        </button>
        <div className="best-bet-arrow">
          <ChevronRight size={20} />
        </div>
      </div>
    </div>
  );
}

const TABS = [
  { key: 'all', label: 'Top Picks' },
  { key: 'ml', label: 'Moneyline' },
  { key: 'spread', label: 'Spread' },
  { key: 'total', label: 'Totals' },
];

function BestBets() {
  const { data, loading, error, silentRefetch } = useApi(fetchBestBets);
  const [activeTab, setActiveTab] = useState('all');
  const [trackedIds, setTrackedIds] = useState(new Set());
  // Track by game_id+bet_type as fallback (handles prediction ID changes)
  const [trackedKeys, setTrackedKeys] = useState(new Set());
  const [trackingId, setTrackingId] = useState(null);
  const [regenerating, setRegenerating] = useState(false);
  const [regenMessage, setRegenMessage] = useState('');

  // Load already-tracked bets so the Track button is disabled
  const refreshTrackedState = useCallback(async () => {
    try {
      const resp = await fetchTrackedBets();
      const bets = resp.data?.bets || resp.data || [];
      const ids = new Set(
        bets.map((b) => b.prediction_id).filter(Boolean)
      );
      const keys = new Set(
        bets.map((b) => `${b.game_id}:${b.bet_type}:${b.prediction_value}`)
      );
      setTrackedIds(ids);
      setTrackedKeys(keys);
    } catch {
      // non-critical
    }
  }, []);

  useEffect(() => {
    refreshTrackedState();
  }, [refreshTrackedState]);

  // Refetch when predictions are regenerated (separate from odds updates)
  useWebSocketEvent('predictions_update', useCallback(() => {
    silentRefetch();
    refreshTrackedState();
  }, [silentRefetch, refreshTrackedState]));

  // Also refetch on odds updates in case line movements affect display
  useWebSocketEvent('odds_update', useCallback(() => {
    silentRefetch();
  }, [silentRefetch]));

  // Fallback: poll best bets every 60 seconds
  useEffect(() => {
    const interval = setInterval(() => {
      silentRefetch();
    }, BEST_BETS_POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [silentRefetch]);

  // Also refresh on manual data sync — refresh both bets and tracked state
  useEffect(() => {
    const onSynced = () => {
      silentRefetch();
      refreshTrackedState();
    };
    window.addEventListener('data-synced', onSynced);
    return () => window.removeEventListener('data-synced', onSynced);
  }, [silentRefetch, refreshTrackedState]);

  const isBetTracked = useCallback((bet) => {
    const predId = bet.prediction_id || bet.id;
    if (predId && trackedIds.has(predId)) return true;
    const key = `${bet.game_id}:${bet.bet_type}:${bet.prediction_value}`;
    return trackedKeys.has(key);
  }, [trackedIds, trackedKeys]);

  const handleTrack = useCallback(async (bet) => {
    const predId = bet.prediction_id || bet.id;
    if (!predId || isBetTracked(bet)) return;
    setTrackingId(predId);
    try {
      await trackBet(predId);
      setTrackedIds((prev) => new Set(prev).add(predId));
      const key = `${bet.game_id}:${bet.bet_type}:${bet.prediction_value}`;
      setTrackedKeys((prev) => new Set(prev).add(key));
    } catch (err) {
      if (err?.response?.status === 409) {
        // Already tracked server-side — mark as tracked locally
        setTrackedIds((prev) => new Set(prev).add(predId));
      }
      console.error('Failed to track bet:', err);
    } finally {
      setTrackingId(null);
    }
  }, [trackedIds]);

  const handleRegenerate = useCallback(async () => {
    if (regenerating) return;
    setRegenerating(true);
    setRegenMessage('Syncing schedule & odds...');
    try {
      const resp = await regeneratePredictions();
      const count = resp.data?.predictions_generated ?? 0;
      const msg = count > 0
        ? `Regenerated ${count} predictions`
        : 'No predictions generated (check data sync)';
      setRegenMessage(msg);
      // Refresh the best bets data
      await silentRefetch();
      window.dispatchEvent(new Event('data-synced'));
      setTimeout(() => setRegenMessage(''), 6000);
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || '';
      setRegenMessage(`Regeneration failed${detail ? `: ${detail}` : ''}`);
      console.error('Failed to regenerate predictions:', err);
      setTimeout(() => setRegenMessage(''), 6000);
    } finally {
      setRegenerating(false);
    }
  }, [regenerating, silentRefetch]);

  let allBets = data?.best_bets || data?.bets || (Array.isArray(data) ? data : []);
  const mlBets = data?.ml_bets || [];
  const spreadBets = data?.spread_bets || [];
  const totalBets = data?.total_bets || [];

  // If the overall best_bets is empty but categorized tabs have data,
  // synthesize a top picks view from the categorized bets so the
  // "Top Picks" tab is never empty when picks exist.
  if (allBets.length === 0 && (mlBets.length > 0 || spreadBets.length > 0 || totalBets.length > 0)) {
    allBets = [...mlBets, ...spreadBets, ...totalBets]
      .sort((a, b) => (b.confidence || 0) - (a.confidence || 0))
      .slice(0, 3);
  }

  const currentBets = {
    all: allBets,
    ml: mlBets,
    spread: spreadBets,
    total: totalBets,
  }[activeTab] || [];

  const tabCounts = {
    all: allBets.length,
    ml: mlBets.length,
    spread: spreadBets.length,
    total: totalBets.length,
  };

  if (loading) {
    return (
      <div className="best-bets-section">
        <div className="section-header">
          <h2 className="section-title">
            <Trophy size={22} className="gold-icon" />
            Best Bets
          </h2>
        </div>
        <div className="loading-container">
          <div className="loading-spinner"></div>
          <p>Analyzing today's best opportunities...</p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="best-bets-section">
        <div className="section-header">
          <h2 className="section-title">
            <Trophy size={22} className="gold-icon" />
            Best Bets
          </h2>
        </div>
        <div className="error-container">
          <p>Unable to load best bets: {error}</p>
        </div>
      </div>
    );
  }

  const hasBets = allBets.length > 0 || mlBets.length > 0 || spreadBets.length > 0 || totalBets.length > 0;

  if (!hasBets) {
    return (
      <div className="best-bets-section">
        <div className="section-header">
          <h2 className="section-title">
            <Trophy size={22} className="gold-icon" />
            Best Bets
          </h2>
        </div>
        <div className="empty-state">
          <Trophy size={48} />
          <p>No best bets available yet. Click "Sync Data" to pull today's games, then predictions will be generated.</p>
        </div>
      </div>
    );
  }

  return (
    <div className="best-bets-section">
      <div className="section-header">
        <h2 className="section-title">
          <Trophy size={22} className="gold-icon" />
          Best Bets
        </h2>
        <div className="section-header-actions">
          <span className="bet-count">{currentBets.length} Pick{currentBets.length !== 1 ? 's' : ''}</span>
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

      <div className="best-bets-tabs">
        {TABS.map((tab) => (
          <button
            key={tab.key}
            className={`best-bets-tab ${activeTab === tab.key ? 'tab-active' : ''}`}
            onClick={() => setActiveTab(tab.key)}
          >
            {tab.label}
            {tabCounts[tab.key] > 0 && (
              <span className="tab-count">{tabCounts[tab.key]}</span>
            )}
          </button>
        ))}
      </div>

      {currentBets.length === 0 ? (
        <div className="empty-state small">
          <p>No {TABS.find(t => t.key === activeTab)?.label || ''} picks available today.</p>
        </div>
      ) : (
        <div className="best-bets-grid">
          {currentBets.slice(0, 3).map((bet, index) => (
            <BestBetCard
              key={bet.prediction_id || bet.id || bet.game_id || index}
              bet={bet}
              rank={index + 1}
              isFeatured={index === 0}
              onTrack={handleTrack}
              tracked={isBetTracked(bet)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default BestBets;
