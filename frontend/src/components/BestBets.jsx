import React from 'react';
import { useNavigate } from 'react-router-dom';
import { Trophy, TrendingUp, Target, Star, ChevronRight } from 'lucide-react';
import { fetchBestBets } from '../utils/api';
import { useApi } from '../hooks/useApi';

function getConfidenceColor(confidence) {
  if (confidence >= 75) return '#00ff88';
  if (confidence >= 60) return '#4fc3f7';
  if (confidence >= 45) return '#ffd700';
  return '#ff5252';
}

function getConfidenceLabel(confidence) {
  if (confidence >= 75) return 'Very High';
  if (confidence >= 60) return 'High';
  if (confidence >= 45) return 'Medium';
  return 'Low';
}

function BestBetCard({ bet, rank, isFeatured }) {
  const navigate = useNavigate();
  const confidence = bet.confidence || bet.confidence_pct || 0;
  const edge = bet.edge || bet.edge_pct || 0;
  const confColor = getConfidenceColor(confidence);

  const handleClick = () => {
    if (bet.game_id) {
      navigate(`/games/${bet.game_id}`);
    }
  };

  return (
    <div
      className={`best-bet-card ${isFeatured ? 'best-bet-featured' : ''}`}
      onClick={handleClick}
      role="button"
      tabIndex={0}
    >
      {isFeatured && (
        <div className="best-bet-badge">
          <Star size={14} />
          BEST BET
        </div>
      )}

      <div className="best-bet-rank">
        <span className="rank-number">#{rank}</span>
      </div>

      <div className="best-bet-content">
        <div className="best-bet-matchup">
          <span className="best-bet-teams">
            {bet.away_team || bet.teams?.away || 'Away'} @{' '}
            {bet.home_team || bet.teams?.home || 'Home'}
          </span>
        </div>

        <div className="best-bet-pick">
          <Target size={16} />
          <span className="pick-type">{bet.bet_type || bet.type || 'Prediction'}</span>
        </div>

        <div className="best-bet-selection">
          {bet.pick || bet.selection || bet.prediction || 'N/A'}
        </div>

        <div className="best-bet-metrics">
          <div className="metric">
            <span className="metric-label">Confidence</span>
            <div className="confidence-bar-container">
              <div
                className="confidence-bar"
                style={{
                  width: `${confidence}%`,
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
        </div>

        {(bet.reasoning || bet.reason) && (
          <p className="best-bet-reasoning">
            {bet.reasoning || bet.reason}
          </p>
        )}
      </div>

      <div className="best-bet-arrow">
        <ChevronRight size={20} />
      </div>
    </div>
  );
}

function BestBets() {
  const { data, loading, error } = useApi(fetchBestBets);

  const bets = data?.best_bets || data?.bets || (Array.isArray(data) ? data : []);

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

  if (bets.length === 0) {
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
          <p>No best bets available yet. Check back closer to game time.</p>
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
        <span className="bet-count">{bets.length} Pick{bets.length !== 1 ? 's' : ''}</span>
      </div>
      <div className="best-bets-grid">
        {bets.slice(0, 3).map((bet, index) => (
          <BestBetCard
            key={bet.id || bet.game_id || index}
            bet={bet}
            rank={index + 1}
            isFeatured={index === 0}
          />
        ))}
      </div>
    </div>
  );
}

export default BestBets;
