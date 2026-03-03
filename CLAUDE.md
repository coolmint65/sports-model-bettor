# CLAUDE.md

## Project Overview

Sports Model Bettor — a full-stack application that scrapes NHL game data and odds, runs predictive analytics models, and surfaces betting recommendations through a React dashboard.

## Architecture

- **Backend**: Python / FastAPI (async) with SQLite (via SQLAlchemy + aiosqlite)
- **Frontend**: React 18 + Vite, with Recharts for visualizations and React Router for navigation
- **Data Sources**: NHL API (`api-web.nhle.com/v1`) for game/team/player data, The Odds API for live odds
- **ML Stack**: scikit-learn, pandas, numpy, scipy for predictions and feature engineering

## Project Structure

```
backend/
  app/
    api/          # FastAPI route modules (games, predictions, stats, schedule, data)
    analytics/    # ML models, feature engineering, prediction logic
    models/       # SQLAlchemy ORM models (game, team, player, prediction)
    scrapers/     # Data scrapers (NHL API, odds APIs)
    config.py     # Pydantic settings (ports, thresholds, sport configs)
    database.py   # Async SQLAlchemy engine/session setup
    main.py       # FastAPI app factory, middleware, lifespan
    constants.py  # Shared constants
  run.py          # Uvicorn entry point
  requirements.txt
  .env.example    # ODDS_API_KEY template

frontend/
  src/
    components/   # React components (Dashboard, GameCard, BestBets, History, etc.)
    hooks/        # Custom hooks (useApi)
    utils/        # API client, team helpers
    styles/       # CSS
  vite.config.js  # Dev server on :3000, proxies /api to :8000
```

## Development Setup

### Backend
```bash
cd backend
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # Add your ODDS_API_KEY
python run.py          # Starts on http://localhost:8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev            # Starts on http://localhost:3000
```

The Vite dev server proxies `/api` requests to the backend at `localhost:8000`.

## Key Configuration

- **Backend port**: 8000 (configured in `backend/app/config.py`)
- **Frontend port**: 3000 (configured in `frontend/vite.config.js`)
- **Database**: SQLite file at `backend/data/sports_betting.db` (auto-created on startup)
- **API docs**: http://localhost:8000/docs (Swagger UI)
- **Environment variables**: `ODDS_API_KEY` in `backend/.env`

## Prediction Thresholds (in `config.py`)

- `min_confidence`: 0.55 — minimum model confidence to surface a prediction
- `min_edge`: 0.03 — minimum edge over implied probability
- `best_bet_edge`: 0.08 — threshold for "best bet" designation
- `best_bet_max_favorite`: -170 — steepest favorite line for best bets
- `best_bet_max_implied`: 0.63 — max implied probability for best bets

## Common Tasks

- **Add a new API route**: Create a module in `backend/app/api/`, define a router, then register it in `backend/app/api/__init__.py`
- **Add a new DB model**: Define in `backend/app/models/`, import in `backend/app/models/__init__.py`
- **Add a new scraper**: Extend `backend/app/scrapers/base.py`
- **Frontend component**: Add to `frontend/src/components/`, wire into `App.jsx` routes

## Code Style

- Backend: Python with type hints, async/await, Pydantic models for schemas
- Frontend: React JSX (not TypeScript), functional components with hooks
- No linter/formatter configs are currently checked in — follow existing patterns
