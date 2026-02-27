import React from 'react';
import { Target, TrendingUp, Star, CheckCircle, XCircle, AlertCircle } from 'lucide-react';

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

function getOutcomeIcon(outcome) {
  if (!outcome) return null;
  const outcomeLower = outcome.toLowerCase();
  if (outcomeLower === 'win' || outcomeLower === 'correct' || outcomeLower === 'hit') {
    return <CheckCircle size={16} className="outcome-win" />;
  }
  if (outcomeLower === 'loss' || outcomeLower === 'incorrect' || outcomeLower === 'miss') {
    return <XCircle size={16} className="outcome-loss" />;
  }
  return <AlertCircle size={16} className="outcome-push" />;
}

function PredictionCard({ prediction, showGame = false, compact = false }) {
  const confidence = prediction.confidence || prediction.confidence_pct || 0;
  const edge = prediction.edge || prediction.edge_pct || 0;
  const confColor = getConfidenceColor(confidence);
  const confLabel = getConfidenceLabel(confidence);
  const isBestBet = prediction.is_best_bet || prediction.best_bet || false;
  const outcome = prediction.outcome || prediction.result || null;
  const betType = prediction.bet_type || prediction.type || 'Prediction';
  const pick = prediction.pick || prediction.selection || prediction.prediction || 'N/A';
  const reasoning = prediction.reasoning || prediction.reason || prediction.analysis || '';

  return (
    <div className={`prediction-card ${isBestBet ? 'prediction-best-bet' : ''} ${compact ? 'prediction-compact' : ''} ${outcome ? `prediction-${outcome.toLowerCase()}` : ''}`}>
      {isBestBet && (
        <div className="prediction-badge">
          <Star size={12} />
          BEST BET
        </div>
      )}

      <div className="prediction-header">
        <div className="prediction-type">
          <Target size={16} />
          <span>{betType}</span>
        </div>
        {outcome && (
          <div className={`prediction-outcome prediction-outcome-${outcome.toLowerCase()}`}>
            {getOutcomeIcon(outcome)}
            <span>{outcome}</span>
          </div>
        )}
      </div>

      {showGame && (prediction.away_team || prediction.home_team) && (
        <div className="prediction-game-info">
          {prediction.away_team || 'Away'} @ {prediction.home_team || 'Home'}
        </div>
      )}

      <div className="prediction-pick">
        {pick}
      </div>

      <div className="prediction-metrics">
        <div className="prediction-metric">
          <span className="prediction-metric-label">Confidence</span>
          <div className="prediction-confidence-bar-wrap">
            <div className="prediction-confidence-bar-bg">
              <div
                className="prediction-confidence-bar-fill"
                style={{
                  width: `${Math.min(confidence, 100)}%`,
                  backgroundColor: confColor,
                }}
              />
            </div>
            <span className="prediction-confidence-value" style={{ color: confColor }}>
              {confidence.toFixed(1)}% - {confLabel}
            </span>
          </div>
        </div>

        {edge !== 0 && (
          <div className="prediction-metric">
            <span className="prediction-metric-label">Edge</span>
            <span className={`prediction-edge-value ${edge > 0 ? 'edge-positive' : 'edge-negative'}`}>
              <TrendingUp size={14} />
              {edge > 0 ? '+' : ''}{edge.toFixed(1)}%
            </span>
          </div>
        )}
      </div>

      {reasoning && !compact && (
        <div className="prediction-reasoning">
          <p>{reasoning}</p>
        </div>
      )}
    </div>
  );
}

export default PredictionCard;
