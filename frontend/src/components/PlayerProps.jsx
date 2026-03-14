import { useState, useCallback, useMemo } from 'react';
import { Users, ChevronDown, ChevronRight, Target, Crosshair, Star, Shield, Award } from 'lucide-react';
import { fetchTodayProps } from '../utils/api';
import { useApi } from '../hooks/useApi';
import { useWebSocketEvent } from '../hooks/useWebSocket';
import { formatAmericanOdds } from '../utils/formatting';
import { teamAbbrev } from '../utils/teams';

const MARKET_CONFIG = {
  player_goal_scorer_anytime: {
    label: 'Anytime Goal Scorer',
    shortLabel: 'ATG',
    icon: Target,
    color: '#00ff88',
    isYesNo: true,
  },
  player_shots_on_goal: {
    label: 'Shots on Goal',
    shortLabel: 'SOG',
    icon: Crosshair,
    color: '#4fc3f7',
    isYesNo: false,
  },
  player_points: {
    label: 'Points',
    shortLabel: 'PTS',
    icon: Star,
    color: '#ffd700',
    isYesNo: false,
  },
  player_assists: {
    label: 'Assists',
    shortLabel: 'AST',
    icon: Award,
    color: '#ff9800',
    isYesNo: false,
  },
  player_total_saves: {
    label: 'Goalie Saves',
    shortLabel: 'SVS',
    icon: Shield,
    color: '#e040fb',
    isYesNo: false,
  },
};

const MARKET_ORDER = [
  'player_goal_scorer_anytime',
  'player_shots_on_goal',
  'player_points',
  'player_assists',
  'player_total_saves',
];

function PlayerProps() {
  const {
    data: propsData,
    loading,
    error,
    silentRefetch,
  } = useApi(fetchTodayProps);

  const [activeMarket, setActiveMarket] = useState('all');
  const [expandedGames, setExpandedGames] = useState(new Set());

  useWebSocketEvent('odds_update', useCallback(() => {
    silentRefetch();
  }, [silentRefetch]));

  const games = propsData?.games || [];
  const availableMarkets = propsData?.markets || [];
  const totalProps = propsData?.total_props || 0;

  // Filter props by selected market
  const filteredGames = useMemo(() => {
    if (activeMarket === 'all') return games;
    return games.map((game) => ({
      ...game,
      props: game.props.filter((p) => p.market === activeMarket),
      prop_count: game.props.filter((p) => p.market === activeMarket).length,
    })).filter((g) => g.prop_count > 0);
  }, [games, activeMarket]);

  const toggleGame = (gameId) => {
    setExpandedGames((prev) => {
      const next = new Set(prev);
      if (next.has(gameId)) {
        next.delete(gameId);
      } else {
        next.add(gameId);
      }
      return next;
    });
  };

  // Auto-expand all games on first load
  const allExpanded = expandedGames.size === 0 && filteredGames.length > 0;

  if (loading) {
    return (
      <div className="loading-container">
        <div className="loading-spinner"></div>
        <p>Loading player props...</p>
      </div>
    );
  }

  if (error) {
    return (
      <div className="error-container">
        <p>Failed to load player props: {error}</p>
      </div>
    );
  }

  if (totalProps === 0) {
    return (
      <div className="coming-soon-section">
        <Users size={48} />
        <h2>Player Props</h2>
        <p>No player props available yet. Props sync every 30 minutes on game days.</p>
      </div>
    );
  }

  return (
    <div className="player-props">
      <div className="props-header">
        <div className="props-title-row">
          <h2 className="props-title">
            <Users size={20} />
            Player Props
          </h2>
          <span className="props-count">{totalProps} lines</span>
        </div>

        {/* Market filter pills */}
        <div className="props-market-filters">
          <button
            className={`props-filter-pill ${activeMarket === 'all' ? 'props-filter-active' : ''}`}
            onClick={() => setActiveMarket('all')}
          >
            All
          </button>
          {MARKET_ORDER.filter((m) => availableMarkets.includes(m)).map((market) => {
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
      </div>

      {/* Games with props */}
      <div className="props-games-list">
        {filteredGames.map((game) => {
          const isExpanded = allExpanded || expandedGames.has(game.game_id);
          const propsByMarket = groupByMarket(game.props);

          return (
            <div key={game.game_id} className="props-game-card">
              <button
                className="props-game-header"
                onClick={() => toggleGame(game.game_id)}
              >
                <div className="props-game-matchup">
                  <span className="props-team-abbr">{game.away_team}</span>
                  <span className="props-at">@</span>
                  <span className="props-team-abbr">{game.home_team}</span>
                </div>
                <div className="props-game-meta">
                  <span className="props-game-count">{game.prop_count} props</span>
                  {isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                </div>
              </button>

              {isExpanded && (
                <div className="props-game-body">
                  {MARKET_ORDER.filter((m) => propsByMarket[m]).map((market) => {
                    const config = MARKET_CONFIG[market];
                    const props = propsByMarket[market];
                    if (!config || !props?.length) return null;
                    const Icon = config.icon;

                    return (
                      <div key={market} className="props-market-section">
                        <div className="props-market-header" style={{ color: config.color }}>
                          <Icon size={14} />
                          <span>{config.label}</span>
                          <span className="props-market-count">{props.length}</span>
                        </div>

                        {config.isYesNo ? (
                          <div className="props-atg-grid">
                            {props
                              .sort((a, b) => (a.over_price || 9999) - (b.over_price || 9999))
                              .map((prop) => (
                                <div key={prop.id} className="props-atg-row">
                                  <span className="props-player-name">{prop.player_name}</span>
                                  <span className="props-odds-value" style={{ color: config.color }}>
                                    {formatAmericanOdds(prop.over_price) || '-'}
                                  </span>
                                </div>
                              ))}
                          </div>
                        ) : (
                          <div className="props-ou-table">
                            <div className="props-ou-header-row">
                              <span className="props-ou-col-player">Player</span>
                              <span className="props-ou-col-line">Line</span>
                              <span className="props-ou-col-odds">Over</span>
                              <span className="props-ou-col-odds">Under</span>
                            </div>
                            {props
                              .sort((a, b) => (b.line || 0) - (a.line || 0))
                              .map((prop) => (
                                <div key={prop.id} className="props-ou-row">
                                  <span className="props-ou-col-player">{prop.player_name}</span>
                                  <span className="props-ou-col-line">{prop.line ?? '-'}</span>
                                  <span className="props-ou-col-odds props-over">
                                    {formatAmericanOdds(prop.over_price) || '-'}
                                  </span>
                                  <span className="props-ou-col-odds props-under">
                                    {formatAmericanOdds(prop.under_price) || '-'}
                                  </span>
                                </div>
                              ))}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function groupByMarket(props) {
  const grouped = {};
  for (const prop of props) {
    if (!grouped[prop.market]) {
      grouped[prop.market] = [];
    }
    grouped[prop.market].push(prop);
  }
  return grouped;
}

export default PlayerProps;
