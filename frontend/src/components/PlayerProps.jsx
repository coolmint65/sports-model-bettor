import { useState, useCallback, useMemo } from 'react';
import { Users, ChevronDown, ChevronRight, Target, Crosshair, Star, Shield, Award, TrendingUp, Zap } from 'lucide-react';
import { fetchTodayProps, fetchTodayPropPicks } from '../utils/api';
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

function PropPickCard({ pick }) {
  const config = MARKET_CONFIG[pick.market] || {};
  const Icon = config.icon || Target;
  const edgePct = (pick.edge * 100).toFixed(1);
  const confPct = (pick.confidence * 100).toFixed(0);

  const pickLabel = pick.market === 'player_goal_scorer_anytime'
    ? 'Anytime Goal'
    : `${pick.pick_side === 'over' ? 'Over' : 'Under'} ${pick.line}`;

  return (
    <div className="prop-pick-card">
      <div className="prop-pick-header">
        <div className="prop-pick-player">
          <Icon size={14} style={{ color: config.color }} />
          <span className="prop-pick-name">{pick.player_name}</span>
        </div>
        <div className="prop-pick-edge" style={{ color: '#00ff88' }}>
          +{edgePct}% edge
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
      <div className="prop-pick-reasoning">{pick.reasoning}</div>
    </div>
  );
}

function PlayerProps() {
  const {
    data: propsData,
    loading: propsLoading,
    error: propsError,
    silentRefetch: silentRefetchProps,
  } = useApi(fetchTodayProps);

  const {
    data: picksData,
    loading: picksLoading,
  } = useApi(fetchTodayPropPicks);

  const [activeTab, setActiveTab] = useState('picks');
  const [activeMarket, setActiveMarket] = useState('all');
  const [expandedGames, setExpandedGames] = useState(new Set());

  useWebSocketEvent('odds_update', useCallback(() => {
    silentRefetchProps();
  }, [silentRefetchProps]));

  const games = propsData?.games || [];
  const availableMarkets = propsData?.markets || [];
  const totalProps = propsData?.total_props || 0;

  const picksGames = picksData?.games || [];
  const totalPicks = picksData?.total_picks || 0;

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

  const loading = propsLoading || picksLoading;

  if (loading) {
    return (
      <div className="loading-container">
        <div className="loading-spinner"></div>
        <p>Loading player props...</p>
      </div>
    );
  }

  if (propsError) {
    return (
      <div className="error-container">
        <p>Failed to load player props: {propsError}</p>
      </div>
    );
  }

  if (totalProps === 0 && totalPicks === 0) {
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
      {/* Tab switcher */}
      <div className="props-tabs">
        <button
          className={`props-tab ${activeTab === 'picks' ? 'props-tab-active' : ''}`}
          onClick={() => setActiveTab('picks')}
        >
          <Zap size={14} />
          Picks ({totalPicks})
        </button>
        <button
          className={`props-tab ${activeTab === 'odds' ? 'props-tab-active' : ''}`}
          onClick={() => setActiveTab('odds')}
        >
          <TrendingUp size={14} />
          All Lines ({totalProps})
        </button>
      </div>

      {activeTab === 'picks' ? (
        /* ---- PICKS TAB ---- */
        <div className="props-picks-tab">
          {totalPicks === 0 ? (
            <div className="coming-soon-section" style={{ padding: '2rem' }}>
              <Target size={36} />
              <h3>No prop picks yet</h3>
              <p>Picks are generated when player stats and prop odds are both available.</p>
            </div>
          ) : (
            picksGames.filter((g) => g.pick_count > 0).map((game) => (
              <div key={game.game_id} className="props-picks-game">
                <div className="props-picks-game-header">
                  <span className="props-team-abbr">{game.away_team}</span>
                  <span className="props-at">@</span>
                  <span className="props-team-abbr">{game.home_team}</span>
                  <span className="props-game-count">{game.pick_count} picks</span>
                </div>
                <div className="props-picks-list">
                  {game.picks.map((pick, idx) => (
                    <PropPickCard key={`${pick.player_name}-${pick.market}-${idx}`} pick={pick} />
                  ))}
                </div>
              </div>
            ))
          )}
        </div>
      ) : (
        /* ---- ODDS TAB (existing) ---- */
        <div className="props-odds-tab">
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
      )}
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
