import { useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Layers, Target } from 'lucide-react';
import { fetchTodayParlays } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { formatAmericanOdds } from '../utils/formatting';

function ParlayLegCard({ leg }) {
  const confPct = Math.round(
    leg.confidence > 1 ? leg.confidence : leg.confidence * 100
  );
  const isProp = leg.type === 'prop';

  return (
    <div className={`parlay-leg-card ${isProp ? 'parlay-leg-prop' : ''}`}>
      <div className="parlay-leg-card-header">
        <Target size={13} />
        <span className="parlay-leg-card-label">{leg.label}</span>
        <span className="parlay-leg-card-odds">
          {formatAmericanOdds(leg.odds)}
        </span>
      </div>
      <div className="parlay-leg-card-meta">
        <span className="parlay-leg-card-matchup">{leg.matchup}</span>
        <span className="parlay-leg-card-conf">{confPct}%</span>
      </div>
    </div>
  );
}

function ParlayCard({ parlay, legCount }) {
  if (!parlay) return null;

  return (
    <div className="parlay-sidebar-card">
      <div className="parlay-sidebar-card-header">
        <span className="parlay-sidebar-legs-badge">{legCount}-Leg Parlay</span>
        {parlay.combined_odds != null && (
          <span className="parlay-sidebar-combined-odds">
            {formatAmericanOdds(parlay.combined_odds)}
          </span>
        )}
      </div>
      <div className="parlay-sidebar-legs">
        {parlay.legs.map((leg, i) => (
          <ParlayLegCard key={i} leg={leg} />
        ))}
      </div>
    </div>
  );
}

function ParlaysSidebar() {
  const { sport } = useParams();
  const currentSport = sport || 'nhl';
  const fetchParlays = useCallback(
    () => fetchTodayParlays(currentSport),
    [currentSport]
  );
  const { data: parlayData } = useApi(fetchParlays);

  if (!parlayData?.two_leg && !parlayData?.three_leg) {
    return null;
  }

  return (
    <aside className="parlays-sidebar">
      <div className="parlays-sidebar-header">
        <Layers size={18} />
        <h3 className="parlays-sidebar-title">Top Parlays</h3>
      </div>
      <div className="parlays-sidebar-content">
        <ParlayCard parlay={parlayData.two_leg} legCount={2} />
        <ParlayCard parlay={parlayData.three_leg} legCount={3} />
      </div>
    </aside>
  );
}

export default ParlaysSidebar;
