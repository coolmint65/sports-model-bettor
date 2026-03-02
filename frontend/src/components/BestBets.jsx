import { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { Trophy, TrendingUp, Target, Star, ChevronRight, Radio, Plus, Check, Layers } from 'lucide-react';
import { fetchBestBets, trackBet, fetchTrackedBets } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { teamName, teamAbbrev, confidencePct, formatBetType, formatPredictionValue } from '../utils/teams';

const BEST_BETS_POLL_INTERVAL = 60_000; // 60 seconds

function formatAmericanOdds(odds) {
  if (odds == null) return null;
  const v = Math.round(odds);
  return v > 0 ? `+${v}` : `${v}`;
}

function formatOddsFromProb(impliedProb) {
  if (!impliedProb || impliedProb <= 0 || impliedProb >= 1) return null;
  if (impliedProb > 0.5) {
    const odds = Math.round(-(impliedProb / (1 - impliedProb)) * 100);
    return odds.toString();
  } else {
    const odds = Math.round(((1 - impliedProb) / impliedProb) * 100);
    return `+${odds}`;
  }
}

function getConfidenceColor(confidence) {
  if (confidence >= 75) return '#00ff88';
  if (confidence >= 60) return '#4fc3f7';
  if (confidence >= 45) return '#ffd700';
  return '#ff5252';
}

function isLiveGame(status) {
  if (!status) return false;
  const s = status.toLowerCase();
  return s === 'in_progress' || s === 'live' || s === 'in progress';
}

function BestBetCard({ bet, rank, isFeatured, onTrack, tracked }) {
  const navigate = useNavigate();
  const confidence = confidencePct(bet.confidence);
  const edge = confidencePct(bet.edge);
  const confColor = getConfidenceColor(confidence);
  const live = isLiveGame(bet.game_status);
  const phase = bet.phase || 'prematch';
  const units = bet.units || 1;

  const oddsDisplay = bet.odds_display != null
    ? formatAmericanOdds(bet.odds_display)
    : formatOddsFromProb(bet.odds_implied_prob);

  const awayName = teamName(bet.away_team, 'Away');
  const homeName = teamName(bet.home_team, 'Home');

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
          {formatPredictionValue(bet.prediction_value || bet.pick || bet.selection)}
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

          <div className="metric">
            <span className="metric-label">Edge</span>
            <span className="metric-value edge-value">
              <TrendingUp size={14} />
              {edge > 0 ? '+' : ''}
              {edge.toFixed(1)}%
            </span>
          </div>

          {oddsDisplay && (
            <div className="metric">
              <span className="metric-label">Odds</span>
              <span className="metric-value odds-value">
                {oddsDisplay}
              </span>
            </div>
          )}

          <div className="metric">
            <span className="metric-label">Units</span>
            <span className="metric-value units-value">
              {units.toFixed(1)}u
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
  const [trackingId, setTrackingId] = useState(null);

  // Load already-tracked prediction IDs so the Track button is disabled
  useEffect(() => {
    (async () => {
      try {
        const resp = await fetchTrackedBets();
        const bets = resp.data?.bets || resp.data || [];
        const ids = new Set(
          bets.map((b) => b.prediction_id).filter(Boolean)
        );
        if (ids.size) setTrackedIds(ids);
      } catch {
        // non-critical
      }
    })();
  }, []);

  // Auto-poll best bets every 60 seconds for seamless updates
  useEffect(() => {
    const interval = setInterval(() => {
      silentRefetch();
    }, BEST_BETS_POLL_INTERVAL);
    return () => clearInterval(interval);
  }, [silentRefetch]);

  // Also refresh on manual data sync
  useEffect(() => {
    const onSynced = () => silentRefetch();
    window.addEventListener('data-synced', onSynced);
    return () => window.removeEventListener('data-synced', onSynced);
  }, [silentRefetch]);

  const handleTrack = useCallback(async (bet) => {
    const predId = bet.prediction_id || bet.id;
    if (!predId || trackedIds.has(predId)) return;
    setTrackingId(predId);
    try {
      await trackBet(predId, bet.units);
      setTrackedIds((prev) => new Set(prev).add(predId));
    } catch (err) {
      console.error('Failed to track bet:', err);
    } finally {
      setTrackingId(null);
    }
  }, [trackedIds]);

  const allBets = data?.best_bets || data?.bets || (Array.isArray(data) ? data : []);
  const mlBets = data?.ml_bets || [];
  const spreadBets = data?.spread_bets || [];
  const totalBets = data?.total_bets || [];

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
        <span className="bet-count">{currentBets.length} Pick{currentBets.length !== 1 ? 's' : ''}</span>
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
              tracked={trackedIds.has(bet.prediction_id || bet.id)}
            />
          ))}
        </div>
      )}
    </div>
  );
}

export default BestBets;
