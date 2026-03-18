import { useState, useMemo } from 'react';
import { Users, Target, Crosshair, Star, Shield, Award, Zap, Check, X } from 'lucide-react';
import { fetchTodayPropPicks } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { formatAmericanOdds } from '../utils/formatting';

const MARKET_CONFIG = {
  player_goal_scorer_anytime: {
    label: 'Anytime Goal Scorer',
    shortLabel: 'ATG',
    icon: Target,
    color: '#00ff88',
  },
  player_shots_on_goal: {
    label: 'Shots on Goal',
    shortLabel: 'SOG',
    icon: Crosshair,
    color: '#4fc3f7',
  },
  player_points: {
    label: 'Points',
    shortLabel: 'PTS',
    icon: Star,
    color: '#ffd700',
  },
  player_assists: {
    label: 'Assists',
    shortLabel: 'AST',
    icon: Award,
    color: '#ff9800',
  },
  player_total_saves: {
    label: 'Goalie Saves',
    shortLabel: 'SVS',
    icon: Shield,
    color: '#e040fb',
  },
};

const MARKET_ORDER = [
  'player_goal_scorer_anytime',
  'player_shots_on_goal',
  'player_points',
  'player_assists',
  'player_total_saves',
];

/**
 * Classify a reasoning segment as positive, negative, or neutral
 * relative to whether it helps the pick hit.
 */
function classifySegment(segment, pickSide, market) {
  const s = segment.toLowerCase().trim();
  const isOver = pickSide === 'over' || pickSide === 'yes';

  // Opponent defense: "more" = more goals/shots allowed = good for over, bad for under
  if (s.includes('more goals than avg') || s.includes('more shots than avg')) {
    if (market === 'player_total_saves') {
      // For saves: opponent generates more shots = good for over saves
      return isOver ? 'positive' : 'negative';
    }
    return isOver ? 'positive' : 'negative';
  }
  if (s.includes('fewer goals than avg') || s.includes('fewer shots than avg')) {
    if (market === 'player_total_saves') {
      return isOver ? 'negative' : 'positive';
    }
    return isOver ? 'negative' : 'positive';
  }

  // Fatigue — always a concern
  if (s.includes('b2b fatigue')) {
    return 'negative';
  }

  // Rest — neutral context, already factored into model
  if (s.includes('days rest')) {
    return 'neutral';
  }

  // Line moved against
  if (s.includes('line moved against')) {
    return 'negative';
  }

  // PP goals / physical / active — supplementary positives for over
  if (s.includes('pp goals') || s.includes('physical') || s.includes('active defensively')) {
    return isOver ? 'positive' : 'neutral';
  }

  // Neutral: "Playing at home", "Playing away", "Matchup:", base stats
  return 'neutral';
}

function PropReasoning({ reasoning, pickSide, market }) {
  if (!reasoning) return null;

  const segments = reasoning.split(' | ');
  // First segment is always the base stat line — keep neutral
  const base = segments[0];
  const factors = segments.slice(1);

  return (
    <>
      <span className="prop-reason-segment prop-reason-neutral">{base}</span>
      {factors.map((seg, i) => {
        const cls = classifySegment(seg, pickSide, market);
        return (
          <span key={i} className={`prop-reason-segment prop-reason-${cls}`}>
            {' | '}{seg}
          </span>
        );
      })}
    </>
  );
}

function PropPickCard({ pick, rank }) {
  const config = MARKET_CONFIG[pick.market] || {};
  const Icon = config.icon || Target;
  const edgeVal = pick.edge * 100;
  const edgePct = `${edgeVal >= 0 ? '+' : ''}${edgeVal.toFixed(1)}%`;
  const confPct = (pick.confidence * 100).toFixed(0);
  const outcome = pick.outcome; // true = hit, false = miss, null/undefined = pending

  const pickLabel = pick.market === 'player_goal_scorer_anytime'
    ? 'Anytime Goal'
    : `${pick.pick_side === 'over' ? 'Over' : 'Under'} ${pick.line}`;

  const outcomeClass = outcome === true ? 'prop-pick-hit' : outcome === false ? 'prop-pick-miss' : '';

  return (
    <div className={`prop-pick-card ${outcomeClass}`}>
      <div className="prop-pick-header">
        <div className="prop-pick-player">
          {outcome === true ? (
            <Check size={16} className="prop-outcome-icon prop-outcome-hit" />
          ) : outcome === false ? (
            <X size={16} className="prop-outcome-icon prop-outcome-miss" />
          ) : (
            <span className="prop-pick-rank">#{rank}</span>
          )}
          <Icon size={14} style={{ color: config.color }} />
          <span className="prop-pick-name">{pick.player_name}</span>
          {pick.jersey_number != null && <span className="prop-pick-number">#{pick.jersey_number}</span>}
          {pick.team_abbrev && <span className="prop-pick-team">{pick.team_abbrev}</span>}
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
          {formatAmericanOdds(pick.odds)}
        </span>
        <span className="prop-pick-conf">{confPct}%</span>
      </div>
      <div className="prop-pick-reasoning">
        <PropReasoning reasoning={pick.reasoning} pickSide={pick.pick_side} market={pick.market} />
      </div>
    </div>
  );
}

function PlayerProps() {
  const {
    data: picksData,
    loading: picksLoading,
    error: picksError,
  } = useApi(fetchTodayPropPicks);

  const [activeMarket, setActiveMarket] = useState('all');

  const picksGames = picksData?.games || [];
  const totalPicks = picksData?.total_picks || 0;

  // Collect available markets from picks
  const availableMarkets = useMemo(() => {
    const markets = new Set();
    for (const game of picksGames) {
      for (const pick of game.picks || []) {
        markets.add(pick.market);
      }
    }
    return MARKET_ORDER.filter((m) => markets.has(m));
  }, [picksGames]);

  // Filter picks by selected market
  const filteredGames = useMemo(() => {
    if (activeMarket === 'all') return picksGames;
    return picksGames.map((game) => ({
      ...game,
      picks: (game.picks || []).filter((p) => p.market === activeMarket),
      pick_count: (game.picks || []).filter((p) => p.market === activeMarket).length,
    })).filter((g) => g.pick_count > 0);
  }, [picksGames, activeMarket]);

  if (picksLoading) {
    return (
      <div className="loading-container">
        <div className="loading-spinner"></div>
        <p>Loading player prop picks...</p>
      </div>
    );
  }

  if (picksError) {
    return (
      <div className="error-container">
        <p>Failed to load player prop picks: {picksError}</p>
      </div>
    );
  }

  if (totalPicks === 0) {
    return (
      <div className="coming-soon-section">
        <Users size={48} />
        <h2>Player Props</h2>
        <p>No player prop picks available yet. Picks are generated when player stats and prop odds are both available.</p>
      </div>
    );
  }

  return (
    <div className="player-props">
      <div className="props-header">
        <div className="props-title-row">
          <h2 className="props-title">
            <Zap size={20} />
            Player Prop Picks
          </h2>
          <span className="props-count">{totalPicks} picks</span>
        </div>

        {/* Market filter pills */}
        {availableMarkets.length > 1 && (
          <div className="props-market-filters">
            <button
              className={`props-filter-pill ${activeMarket === 'all' ? 'props-filter-active' : ''}`}
              onClick={() => setActiveMarket('all')}
            >
              All
            </button>
            {availableMarkets.map((market) => {
              const config = MARKET_CONFIG[market];
              if (!config) return null;
              return (
                <button
                  key={market}
                  className={`props-filter-pill ${activeMarket === market ? 'props-filter-active' : ''}`}
                  onClick={() => setActiveMarket(market)}
                  style={activeMarket === market ? { borderColor: config.color, color: config.color } : {}}
                >
                  {config.shortLabel}
                </button>
              );
            })}
          </div>
        )}
      </div>

      <div className="props-picks-tab">
        {filteredGames.filter((g) => g.pick_count > 0).map((game) => (
          <div key={game.game_id} className="props-picks-game">
            <div className="props-picks-game-header">
              <span className="props-team-abbr">{game.away_team}</span>
              <span className="props-at">@</span>
              <span className="props-team-abbr">{game.home_team}</span>
              {game.status && ['final', 'completed', 'official', 'off'].includes(game.status.toLowerCase()) && (
                <span className="props-game-status props-game-final">Final</span>
              )}
              {game.status && ['in_progress', 'live', 'active'].includes(game.status.toLowerCase()) && (
                <span className="props-game-status props-game-live">Live</span>
              )}
              <span className="props-game-count">{game.pick_count} picks</span>
            </div>
            <div className="props-picks-list">
              {game.picks.map((pick, idx) => (
                <PropPickCard key={`${pick.player_name}-${pick.market}-${idx}`} pick={pick} rank={idx + 1} />
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

export default PlayerProps;
