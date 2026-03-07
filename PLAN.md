# Model Enhancement Plan — Implemented

## What Changed

### 1. Configurable Model Constants (`ModelConfig`)
All prediction model weights and thresholds are now centralized in `config.py`
under `ModelConfig`, `InjuryConfig`, and `MatchupConfig`. No more hard-coded
constants scattered through the analytics code.

**Key settings:** league averages, form weights, factor multipliers (H2H, goalie,
skater talent, lineup depletion, injuries, matchups, schedule fatigue, special
teams), blending ratios, xG bounds, and feature extraction windows.

**Runtime API:** `GET /api/config/model` to view, `PUT /api/config/model` to
update at runtime, `POST /api/config/model/reset` to restore defaults.

### 2. Injury Reports
- **Model:** `InjuryReport` — tracks player injuries with status, type, dates,
  and snapshotted production metrics (PPG, GPG, TOI).
- **Scraper:** `injury_scraper.py` — fetches from NHL API roster data, maps
  injury designations to severity levels, auto-deactivates stale reports.
- **Feature:** `get_injury_impact()` — computes xG reduction from active injuries
  weighted by status severity and position importance.
- **API:** `GET /api/injuries/{team_abbr}`, `POST /api/injuries/refresh`.
- **Scheduler:** Injuries refresh automatically during full data sync.

### 3. Player vs Team Matchup Tracking
- **Model:** `PlayerMatchupStats` — per (player, opponent_team, season) stats
  with PPG/GPG deviation from overall average.
- **Engine:** `MatchupEngine.compute_player_matchup()` aggregates a player's
  box score stats in games against a specific opponent.
- **Feature:** `get_team_player_matchup_impact()` — weighted matchup boost/penalty
  for a team's key players against the opponent.
- **API:** `GET /api/matchups/player/{id}/vs/{team_abbr}`.

### 4. Enhanced Team vs Team Matchup Analytics
- **Model:** `TeamMatchupProfile` — scoring patterns (avg total goals, variance),
  period-level trends, OT rate, pace indicator between team pairs.
- **Engine:** `MatchupEngine.compute_team_matchup_profile()` analyzes H2H games
  for deeper tendencies beyond basic win/loss.
- **Feature:** `get_team_matchup_features()` feeds scoring tendency data into
  the xG calculation (TEAM_MATCHUP_SCORING_FACTOR).
- **API:** `GET /api/matchups/team/{t1}/vs/{t2}`, `POST /api/matchups/refresh/{t1}/vs/{t2}`.

### 5. Schedule Fatigue (Back-to-Back, Rest, Road Trips)
- **Feature:** `get_schedule_context()` detects B2B games, rest days, games in
  last 7 days, and consecutive road games.
- **xG Impact:** B2B penalty (-0.15 xG), rest advantage (+0.05/day, capped),
  road trip fatigue (-0.02/game after threshold).

### 6. Special Teams Matchup Integration
- **Feature:** `get_special_teams_matchup()` pulls PP% and PK% from season stats.
- **xG Impact:** Compares team PP vs opponent PK (and vice versa), adjusts xG
  based on special teams advantage/disadvantage.

## New Files
- `backend/app/models/injury.py` — InjuryReport model
- `backend/app/models/matchup.py` — PlayerMatchupStats, TeamMatchupProfile
- `backend/app/scrapers/injury_scraper.py` — NHL injury data fetcher
- `backend/app/analytics/matchups.py` — MatchupEngine
- `backend/app/api/injuries.py` — Injury API endpoints
- `backend/app/api/matchups.py` — Matchup API endpoints
- `backend/app/api/model_config.py` — Config API endpoints

## Modified Files
- `backend/app/config.py` — ModelConfig, InjuryConfig, MatchupConfig classes
- `backend/app/analytics/models.py` — Config-driven constants + new xG adjustments
- `backend/app/analytics/features.py` — New feature methods + build_game_features integration
- `backend/app/analytics/predictions.py` — Extended features_summary
- `backend/app/models/__init__.py` — Register new models
- `backend/app/database.py` — Import new model modules for table creation
- `backend/app/api/__init__.py` — Register new API routers
- `backend/app/live.py` — Injury sync in scheduler

## Future Expansion Points
The architecture is designed for easy extension:
- Add new sports by adding SportConfig entries and sport-specific FeatureEngine subclasses
- Add new bet types by extending BET_TYPES and adding prediction methods
- Add ML model training by reading features from the DB and training against outcomes
- Add advanced metrics (Corsi, Fenwick, xGF) by extending GamePlayerStats and FeatureEngine
- Add prop bet odds by connecting a prop odds data source
