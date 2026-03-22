import { useCallback } from 'react';
import { useParams } from 'react-router-dom';
import { Layers, Target, Crosshair, Shield, Star, Award, Zap } from 'lucide-react';
import { fetchTodayParlays } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { formatAmericanOdds } from '../utils/formatting';

const MARKET_CONFIG = {
  player_goal_scorer_anytime: { shortLabel: 'ATG', icon: Target, color: '#00ff88' },
  player_shots_on_goal: { shortLabel: 'SOG', icon: Crosshair, color: '#4fc3f7' },
  player_total_saves: { shortLabel: 'SVS', icon: Shield, color: '#e040fb' },
  player_points: { shortLabel: 'PTS', icon: Star, color: '#ffd700' },
  player_assists: { shortLabel: 'AST', icon: Award, color: '#ff9800' },
  player_rebounds: { shortLabel: 'REB', icon: Shield, color: '#4fc3f7' },
  player_threes: { shortLabel: '3PM', icon: Zap, color: '#bb86fc' },
};

function getNhlSeason() {
  const now = new Date();
  const year = now.getMonth() >= 9 ? now.getFullYear() : now.getFullYear() - 1;
  return `${year}${year + 1}`;
}

function PlayerHeadshot({ playerExtId, playerName, teamAbbrev, sport }) {
  if (!playerExtId) return null;
  let headshotUrl;
  if (sport === 'nba') {
    headshotUrl = `https://cdn.nba.com/headshots/nba/latest/260x190/${playerExtId}.png`;
  } else {
    if (!teamAbbrev) return null;
    const season = getNhlSeason();
    headshotUrl = `https://assets.nhle.com/mugs/nhl/${season}/${teamAbbrev}/${playerExtId}.png`;
  }
  return (
    <img
      className="prop-pick-headshot"
      src={headshotUrl}
      alt={playerName}
      loading="lazy"
      onError={(e) => { e.target.style.display = 'none'; }}
    />
  );
}

/** Game-line parlay leg (ML, spread, total) */
function GameLegCard({ leg }) {
  const confPct = Math.round(
    leg.confidence > 1 ? leg.confidence : leg.confidence * 100
  );

  return (
    <div className="parlay-leg-card">
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

/** Player-prop parlay leg — styled like PropPickCard */
function PropLegCard({ leg }) {
  const config = MARKET_CONFIG[leg.market] || {};
  const Icon = config.icon || Target;
  const edgeVal = (leg.edge || 0) * 100;
  const edgePct = `${edgeVal >= 0 ? '+' : ''}${edgeVal.toFixed(1)}%`;
  const confPct = Math.round(
    leg.confidence > 1 ? leg.confidence : leg.confidence * 100
  );

  const pickLabel = leg.market === 'player_goal_scorer_anytime'
    ? 'Anytime Goal'
    : `${(leg.pick_side || 'over') === 'over' ? 'Over' : 'Under'} ${leg.line}`;

  return (
    <div className="prop-pick-card parlay-prop-leg">
      <PlayerHeadshot
        playerExtId={leg.player_ext_id}
        playerName={leg.player_name}
        teamAbbrev={leg.team_abbrev}
        sport={leg.sport}
      />
      <div className="prop-pick-content">
        <div className="prop-pick-header">
          <div className="prop-pick-player">
            <Icon size={14} style={{ color: config.color }} />
            <span className="prop-pick-name">{leg.player_name}</span>
            {leg.jersey_number != null && <span className="prop-pick-number">#{leg.jersey_number}</span>}
            {leg.team_abbrev && <span className="prop-pick-team">{leg.team_abbrev}</span>}
          </div>
          <div className="prop-pick-edge" style={{ color: edgeVal >= 0 ? '#00ff88' : 'var(--accent-red, #ff5252)' }}>
            Edge {edgePct}
          </div>
        </div>
        <div className="prop-pick-details">
          <span className="prop-pick-label" style={{ color: config.color }}>
            {config.shortLabel}: {pickLabel}
          </span>
          <span className="prop-pick-odds">
            {formatAmericanOdds(leg.odds)}
          </span>
          <span className="prop-pick-conf">{confPct}%</span>
        </div>
        <div className="parlay-prop-leg-matchup">{leg.matchup}</div>
      </div>
    </div>
  );
}

function ParlayCard({ parlay, legCount }) {
  if (!parlay) return null;

  const hasProps = parlay.legs.some((l) => l.type === 'prop');
  const hasGameLegs = parlay.legs.some((l) => l.type !== 'prop');

  return (
    <div className="parlay-inline-card">
      <div className="parlay-inline-card-header">
        <span className="parlay-inline-legs-badge">{legCount}-Leg Parlay</span>
        {parlay.combined_odds != null && (
          <span className="parlay-inline-combined-odds">
            {formatAmericanOdds(parlay.combined_odds)}
          </span>
        )}
      </div>
      <div className="parlay-inline-legs">
        {parlay.legs.map((leg, i) =>
          leg.type === 'prop' ? (
            <PropLegCard key={i} leg={leg} />
          ) : (
            <GameLegCard key={i} leg={leg} />
          )
        )}
      </div>
    </div>
  );
}

function ParlaysSection() {
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
    <section className="section parlays-section">
      <div className="section-header">
        <h2 className="section-title parlays-section-title">
          <Layers size={20} />
          Top Parlays
        </h2>
      </div>
      <div className="parlays-inline-grid">
        <ParlayCard parlay={parlayData.two_leg} legCount={2} />
        <ParlayCard parlay={parlayData.three_leg} legCount={3} />
      </div>
    </section>
  );
}

export default ParlaysSection;
