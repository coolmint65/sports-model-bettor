import { Target, TrendingUp, Star, CheckCircle, XCircle, AlertCircle, AlertTriangle, Info } from 'lucide-react';
import { useState } from 'react';
import { confidencePct, formatBetType, formatPredictionValue, teamAbbrev } from '../utils/teams';
import { getConfidenceColor } from '../utils/formatting';

/**
 * Parse reasoning text, stripping {{team:...}} and {{tooltip:...}} markers.
 * Returns an array of { text, team, tooltip } objects.
 */
function parseReasoning(reasoning) {
  if (!reasoning) return [];

  let cleaned = reasoning.replace(/\s*\(Odds:\s*[^)]*\)/g, '').trim();
  if (!cleaned) return [];

  const teamMarkers = [];
  const tooltips = [];
  const TM_PH = '\x00TM';
  const TT_PH = '\x00TT';
  cleaned = cleaned.replace(/\{\{team:([^}]+)\}\}\s*/g, (_match, abbr) => {
    teamMarkers.push(abbr.trim().toUpperCase());
    return TM_PH + (teamMarkers.length - 1) + ' ';
  });
  cleaned = cleaned.replace(/\s*\{\{tooltip:([\s\S]*?)\}\}/g, (_match, tip) => {
    tooltips.push(tip.trim());
    return TT_PH + (tooltips.length - 1);
  });

  let lines = cleaned
    .split(/(?:\d+\.\s+|\n|;\s*)/)
    .map((s) => s.trim())
    .filter((s) => s.length > 0);

  if (lines.length <= 1) {
    lines = cleaned
      .split(/\.\s+/)
      .map((s) => s.trim().replace(/\.$/, ''))
      .filter((s) => s.length > 5);
  }

  const tmRe = new RegExp(TM_PH + '(\\d+)\\s*', 'g');
  const ttRe = new RegExp(TT_PH + '(\\d+)', 'g');
  return lines.slice(0, 7).map((line) => {
    let tooltip = null;
    let team = null;
    let text = line.replace(tmRe, (_m, idx) => {
      team = teamMarkers[parseInt(idx, 10)] || null;
      return '';
    });
    text = text.replace(ttRe, (_m, idx) => {
      tooltip = tooltips[parseInt(idx, 10)] || null;
      return '';
    }).trim();
    return { text, tooltip, team };
  });
}

function ReasoningLine({ item }) {
  const [showTooltip, setShowTooltip] = useState(false);

  return (
    <li className="reasoning-bullet">
      {item.team && <span className="reasoning-team-tag">{item.team}</span>}
      <span>{item.text}</span>
      {item.tooltip && (
        <span
          className="reasoning-tooltip-trigger"
          onMouseEnter={() => setShowTooltip(true)}
          onMouseLeave={() => setShowTooltip(false)}
        >
          <Info size={12} />
          {showTooltip && (
            <span className="reasoning-tooltip-popup">{item.tooltip}</span>
          )}
        </span>
      )}
    </li>
  );
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

function PredictionCard({ prediction, showGame = false, compact = false, isFallback = false, homeAbbr, awayAbbr }) {
  const confidence = confidencePct(prediction.confidence);
  const edge = confidencePct(prediction.edge);
  const confColor = isFallback ? '#ff9800' : getConfidenceColor(confidence);
  const confLabel = isFallback ? 'Heavy Juice' : getConfidenceLabel(confidence);
  const isBestBet = prediction.is_best_bet || prediction.best_bet || false;
  const outcome = prediction.outcome || prediction.result || null;
  const betType = formatBetType(prediction.bet_type || prediction.type);
  // Use provided abbreviations, or derive from prediction's team objects
  const hAbbr = homeAbbr || teamAbbrev(prediction.home_team, null);
  const aAbbr = awayAbbr || teamAbbrev(prediction.away_team, null);
  const pick = formatPredictionValue(prediction.prediction_value || prediction.pick || prediction.selection, hAbbr, aAbbr, prediction.bet_type || prediction.type);
  const rawReasoning = prediction.reasoning || prediction.reason || prediction.analysis || '';
  const reasoningItems = parseReasoning(rawReasoning);

  const cardClasses = [
    'prediction-card',
    isBestBet ? 'prediction-best-bet' : '',
    isFallback ? 'prediction-fallback' : '',
    compact ? 'prediction-compact' : '',
    outcome ? `prediction-${outcome.toLowerCase()}` : '',
  ].filter(Boolean).join(' ');

  return (
    <div className={cardClasses}>
      {isBestBet && (
        <div className="prediction-badge">
          <Star size={12} />
          BEST BET
        </div>
      )}
      {isFallback && !isBestBet && (
        <div className="prediction-badge prediction-badge-fallback">
          <AlertTriangle size={12} />
          HEAVY JUICE
        </div>
      )}

      <div className="prediction-header">
        <div className="prediction-type">
          {isFallback ? <AlertTriangle size={16} /> : <Target size={16} />}
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
          {typeof prediction.away_team === 'object' ? prediction.away_team?.name : prediction.away_team || 'Away'} @{' '}
          {typeof prediction.home_team === 'object' ? prediction.home_team?.name : prediction.home_team || 'Home'}
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

      {reasoningItems.length > 0 && !compact && (
        <div className="prediction-reasoning">
          <ul className="reasoning-list">
            {reasoningItems.map((item, i) => (
              <ReasoningLine key={i} item={item} />
            ))}
          </ul>
        </div>
      )}
    </div>
  );
}

export default PredictionCard;
