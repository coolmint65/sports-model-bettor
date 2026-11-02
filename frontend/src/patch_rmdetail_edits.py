#!/usr/bin/env python3
"""
Remove the game-details click-through screen (MLB/NHL/NBA/Basketball/Tennis)
from PickMint's frontend. Exact-string replacements with per-file .bak
backups. Aborts the ENTIRE run on the first mismatch (does not leave a
partially-patched file, and stops before touching later files).

Usage: python patch_rmdetail_edits.py [--check] [--root PATH]
  --check   dry run: verify every expected string count, write nothing.
  --root    base dir containing the frontend/src tree (default: cwd).
"""
import os
import sys

BAK_SUFFIX = ".bak-rmdetail-20261022"

CHECK_ONLY = "--check" in sys.argv
ROOT = "."
for i, a in enumerate(sys.argv):
    if a == "--root" and i + 1 < len(sys.argv):
        ROOT = sys.argv[i + 1]


def detect_newline(raw):
    crlf = raw.count(b"\r\n")
    lf_only = raw.count(b"\n") - crlf
    if crlf and not lf_only:
        return "\r\n"
    if lf_only and not crlf:
        return "\n"
    if crlf and lf_only:
        raise ValueError("mixed newlines - handle manually")
    return "\n"


def patch_file(relpath, replacements):
    """replacements: list of (old, new, expected_count) authored with
    plain \\n; auto-adapted to the file's real newline convention."""
    path = os.path.join(ROOT, relpath)
    with open(path, "rb") as f:
        raw = f.read()
    nl = detect_newline(raw)
    content = raw.decode("utf-8")
    original = content
    for idx, (old_n, new_n, expected) in enumerate(replacements):
        old = old_n.replace("\n", nl) if nl != "\n" else old_n
        new = new_n.replace("\n", nl) if nl != "\n" else new_n
        count = content.count(old)
        if count != expected:
            print(f"FAIL {relpath} edit#{idx}: expected {expected} occurrence(s), found {count}")
            print("----- OLD (first 400 chars) -----")
            print(old[:400])
            sys.exit(1)
        content = content.replace(old, new)
    if CHECK_ONLY:
        print(f"CHECK-OK {relpath}: {len(replacements)} edit(s) would apply cleanly")
        return
    bak_path = path + BAK_SUFFIX
    if not os.path.exists(bak_path):
        with open(bak_path, "wb") as f:
            f.write(raw)
    with open(path, "wb") as f:
        f.write(content.encode("utf-8"))
    print(f"OK {relpath}: {len(replacements)} edit(s) applied, backup at {relpath}{BAK_SUFFIX}")


# ─────────────────────────────────────────────────────────────────────
# App.jsx
# ─────────────────────────────────────────────────────────────────────
patch_file("App.jsx", [
    ("import GameDetail from './components/GameDetail'\n", "", 1),
    ("import NHLGameDetail from './components/NHLGameDetail'\n", "", 1),
    ("import NBAGameDetail from './components/NBAGameDetail'\n", "", 1),
    (
        "  // Sub-view state that isn't part of the shared slate/best-bets hook —\n"
        "  // detail panes, prediction popovers, tracker slices — stays local so\n"
        "  // each sport's selectGame handler can mutate them freely.\n"
        "  const [selectedGame, setSelectedGame] = useState(null)\n"
        "  const [prediction, setPrediction] = useState(null)\n"
        "  const [predLoading, setPredLoading] = useState(false)\n"
        "  const [standings, setStandings] = useState([])\n",
        "  // Sub-view state that isn't part of the shared slate/best-bets hook —\n"
        "  // tracker slices — stays local per sport.\n"
        "  const [standings, setStandings] = useState([])\n",
        1,
    ),
    (
        "  const [nhlStandingsLoading, setNhlStandingsLoading] = useState(false)\n"
        "  const [nhlPrediction, setNhlPrediction] = useState(null)\n"
        "  const [nhlPredLoading, setNhlPredLoading] = useState(false)\n"
        "  const [nhlSelectedGame, setNhlSelectedGame] = useState(null)\n"
        "  const [nhlPickSummary, setNhlPickSummary] = useState(null)\n",
        "  const [nhlStandingsLoading, setNhlStandingsLoading] = useState(false)\n"
        "  const [nhlPickSummary, setNhlPickSummary] = useState(null)\n",
        1,
    ),
    (
        "  const [nbaStandingsLoading, setNbaStandingsLoading] = useState(false)\n"
        "  const [nbaPrediction, setNbaPrediction] = useState(null)\n"
        "  const [nbaPredLoading, setNbaPredLoading] = useState(false)\n"
        "  const [nbaSelectedGame, setNbaSelectedGame] = useState(null)\n"
        "  const [nbaPickSummary, setNbaPickSummary] = useState(null)\n",
        "  const [nbaStandingsLoading, setNbaStandingsLoading] = useState(false)\n"
        "  const [nbaPickSummary, setNbaPickSummary] = useState(null)\n",
        1,
    ),
    (
        "  // MLB handlers\n"
        "  const selectGame = useCallback((game) => {\n"
        "    setSelectedGame(game)\n"
        "    setView('bets')\n"
        "    setPrediction(null)\n"
        "    setPredLoading(true)\n"
        "\n"
        "    const homeId = game.home.team_id\n"
        "    const awayId = game.away.team_id\n"
        "    if (!homeId || !awayId) { setPredLoading(false); return }\n"
        "\n"
        "    const homePid = game.home_pitcher?.id ? parseInt(game.home_pitcher.id) : null\n"
        "    const awayPid = game.away_pitcher?.id ? parseInt(game.away_pitcher.id) : null\n"
        "\n"
        "    api.post('/predict', {\n"
        "      home_team_id: homeId, away_team_id: awayId,\n"
        "      home_pitcher_id: homePid, away_pitcher_id: awayPid,\n"
        "      venue: game.venue || null,\n"
        "    })\n"
        "      .then(r => setPrediction(r.data))\n"
        "      .catch(() => setPrediction(null))\n"
        "      .finally(() => setPredLoading(false))\n"
        "  }, [])\n"
        "\n"
        "  // NHL handlers\n"
        "  const selectNhlGame = useCallback((game) => {\n"
        "    setNhlSelectedGame(game)\n"
        "    setView('bets')\n"
        "    setNhlPrediction(null)\n"
        "    setNhlPredLoading(true)\n"
        "\n"
        "    // Map ESPN abbreviation to team key for prediction\n"
        "    const h = game.home.abbreviation\n"
        "    const a = game.away.abbreviation\n"
        "\n"
        "    api.get(`/nhl/predict?home=${encodeURIComponent(h)}&away=${encodeURIComponent(a)}`)\n"
        "      .then(r => setNhlPrediction(r.data))\n"
        "      .catch(() => setNhlPrediction(null))\n"
        "      .finally(() => setNhlPredLoading(false))\n"
        "  }, [])\n"
        "\n"
        "  // NBA handlers\n"
        "  const selectNbaGame = useCallback((game) => {\n"
        "    setNbaSelectedGame(game)\n"
        "    setView('bets')\n"
        "    setNbaPrediction(null)\n"
        "    setNbaPredLoading(true)\n"
        "\n"
        "    const h = game.home.abbreviation\n"
        "    const a = game.away.abbreviation\n"
        "\n"
        "    api.get(`/nba/predict?home=${encodeURIComponent(h)}&away=${encodeURIComponent(a)}`)\n"
        "      .then(r => setNbaPrediction(r.data))\n"
        "      .catch(() => setNbaPrediction(null))\n"
        "      .finally(() => setNbaPredLoading(false))\n"
        "  }, [])\n"
        "\n"
        "  const showStandings = useCallback(() => {\n",
        "  const showStandings = useCallback(() => {\n",
        1,
    ),
    (
        "    setView('standings'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)\n",
        "    setView('standings')\n",
        1,
    ),
    (
        "    setView('backtest'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)\n",
        "    setView('backtest')\n",
        1,
    ),
    (
        "    setView('history'); setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)\n",
        "    setView('history')\n",
        1,
    ),
    (
        "  const goBack = useCallback(() => {\n"
        "    setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)\n"
        "    setPrediction(null); setNhlPrediction(null); setNbaPrediction(null)\n"
        "    setView('bets')\n"
        "  }, [])\n"
        "\n"
        "  const switchLeague = useCallback((l) => {\n"
        "    setLeague(l)\n"
        "    setSurface(l.toLowerCase())\n"
        "    setView('bets')\n"
        "    setSelectedGame(null)\n"
        "    setNhlSelectedGame(null)\n"
        "    setNbaSelectedGame(null)\n"
        "    setPrediction(null)\n"
        "    setNhlPrediction(null)\n"
        "    setNbaPrediction(null)\n"
        "  }, [])\n",
        "  const switchLeague = useCallback((l) => {\n"
        "    setLeague(l)\n"
        "    setSurface(l.toLowerCase())\n"
        "    setView('bets')\n"
        "  }, [])\n",
        1,
    ),
    (
        "      setSelectedGame(null)\n"
        "      setNhlSelectedGame(null)\n"
        "      setNbaSelectedGame(null)\n"
        "      return\n",
        "      return\n",
        6,
    ),
    (
        "    setSelectedGame(null); setNhlSelectedGame(null); setNbaSelectedGame(null)\n"
        "    setView(tabId)\n",
        "    setView(tabId)\n",
        1,
    ),
    (
        "      {/* ── MLB Views ── */}\n"
        "      {isMLB && view === 'bets' && !selectedGame && (\n"
        "        <BetsView\n"
        "          sport=\"mlb\"\n"
        "          api={api}\n"
        "          data={{\n"
        "            games,\n"
        "            loading: gamesLoading,\n"
        "            progress: bbProgress,\n"
        "            onSelectGame: selectGame,\n"
        "            bestBets,\n"
        "          }}\n"
        "        />\n"
        "      )}\n"
        "\n"
        "      {isMLB && selectedGame && (\n"
        "        <GameDetail game={selectedGame} prediction={prediction} loading={predLoading} onBack={goBack} />\n"
        "      )}\n"
        "\n"
        "      {isMLB && view === 'standings' && <Standings divisions={standings} />}\n",
        "      {/* ── MLB Views ── */}\n"
        "      {isMLB && view === 'bets' && (\n"
        "        <BetsView\n"
        "          sport=\"mlb\"\n"
        "          api={api}\n"
        "          data={{\n"
        "            games,\n"
        "            loading: gamesLoading,\n"
        "            progress: bbProgress,\n"
        "            bestBets,\n"
        "          }}\n"
        "        />\n"
        "      )}\n"
        "\n"
        "      {isMLB && view === 'standings' && <Standings divisions={standings} />}\n",
        1,
    ),
    (
        "      {/* ── NHL Views ── */}\n"
        "      {isNHL && view === 'bets' && !nhlSelectedGame && (\n"
        "        <BetsView\n"
        "          sport=\"nhl\"\n"
        "          api={api}\n"
        "          data={{\n"
        "            games: nhlGames,\n"
        "            loading: nhlLoading,\n"
        "            progress: nhlBbProgress,\n"
        "            onSelectGame: selectNhlGame,\n"
        "            bestBets: nhlBestBets,\n"
        "          }}\n"
        "        />\n"
        "      )}\n"
        "\n"
        "      {isNHL && nhlSelectedGame && (\n"
        "        <NHLGameDetail game={nhlSelectedGame} prediction={nhlPrediction} loading={nhlPredLoading} onBack={goBack} />\n"
        "      )}\n"
        "\n"
        "      {isNHL && view === 'standings' && <NHLStandings divisions={nhlStandings} loading={nhlStandingsLoading} />}\n",
        "      {/* ── NHL Views ── */}\n"
        "      {isNHL && view === 'bets' && (\n"
        "        <BetsView\n"
        "          sport=\"nhl\"\n"
        "          api={api}\n"
        "          data={{\n"
        "            games: nhlGames,\n"
        "            loading: nhlLoading,\n"
        "            progress: nhlBbProgress,\n"
        "            bestBets: nhlBestBets,\n"
        "          }}\n"
        "        />\n"
        "      )}\n"
        "\n"
        "      {isNHL && view === 'standings' && <NHLStandings divisions={nhlStandings} loading={nhlStandingsLoading} />}\n",
        1,
    ),
    (
        "      {/* ── NBA Views ── */}\n"
        "      {isNBA && view === 'bets' && !nbaSelectedGame && (\n"
        "        <BetsView\n"
        "          sport=\"nba\"\n"
        "          api={api}\n"
        "          data={{\n"
        "            games: nbaGames,\n"
        "            loading: nbaLoading,\n"
        "            progress: nbaBbProgress,\n"
        "            onSelectGame: selectNbaGame,\n"
        "            bestBets: nbaBestBets,\n"
        "          }}\n"
        "        />\n"
        "      )}\n"
        "\n"
        "      {isNBA && nbaSelectedGame && (\n"
        "        <NBAGameDetail game={nbaSelectedGame} prediction={nbaPrediction} loading={nbaPredLoading} onBack={goBack} />\n"
        "      )}\n"
        "\n"
        "      {isNBA && view === 'standings' && <NBAStandings divisions={nbaStandings} loading={nbaStandingsLoading} />}\n",
        "      {/* ── NBA Views ── */}\n"
        "      {isNBA && view === 'bets' && (\n"
        "        <BetsView\n"
        "          sport=\"nba\"\n"
        "          api={api}\n"
        "          data={{\n"
        "            games: nbaGames,\n"
        "            loading: nbaLoading,\n"
        "            progress: nbaBbProgress,\n"
        "            bestBets: nbaBestBets,\n"
        "          }}\n"
        "        />\n"
        "      )}\n"
        "\n"
        "      {isNBA && view === 'standings' && <NBAStandings divisions={nbaStandings} loading={nbaStandingsLoading} />}\n",
        1,
    ),
])

# ─────────────────────────────────────────────────────────────────────
# components/BetsView.jsx
# ─────────────────────────────────────────────────────────────────────
patch_file("components/BetsView.jsx", [
    ("          onSelectGame={data.onSelectGame}\n", "", 3),
])

# ─────────────────────────────────────────────────────────────────────
# components/primitives/ScoreboardShell.jsx
# ─────────────────────────────────────────────────────────────────────
patch_file("components/primitives/ScoreboardShell.jsx", [
    ("  onSelectGame,\n", "", 1),
    (
        "  // Stable handler per game id. The previous `game => () => ...`\n"
        "  // closure allocated a new function for every card on every render,\n"
        "  // defeating any shallow-prop equality the card might use.\n"
        "  const handleClick = useCallback(\n"
        "    game => () => { if (onSelectGame) onSelectGame(game) },\n"
        "    [onSelectGame],\n"
        "  )\n"
        "\n"
        "  const jumpTo = useCallback((gid) => {\n",
        "  const jumpTo = useCallback((gid) => {\n",
        1,
    ),
    ("                  onClick: handleClick(game),\n", "", 1),
    ("                onClick: handleClick(game),\n", "", 1),
])

# ─────────────────────────────────────────────────────────────────────
# components/primitives/GameCard.jsx
# ─────────────────────────────────────────────────────────────────────
patch_file("components/primitives/GameCard.jsx", [
    (
        "      className={cn(\n"
        "        'group relative flex flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4 cursor-pointer',\n"
        "        'transition-all duration-150',\n"
        "        'hover:border-border hover:bg-accent/40 hover:-translate-y-px hover:shadow-lg',\n"
        "        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',\n"
        "        CONF_ACCENT[accentKey] || CONF_ACCENT.lean,\n"
        "        isLive && 'ring-1 ring-negative/30',\n"
        "      )}\n"
        "      onClick={onClick}\n"
        "      role=\"button\"\n"
        "      tabIndex={0}\n"
        "    >\n",
        "      className={cn(\n"
        "        'group relative flex flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4',\n"
        "        'transition-all duration-150',\n"
        "        'hover:border-border hover:bg-accent/40',\n"
        "        onClick && 'cursor-pointer hover:-translate-y-px hover:shadow-lg',\n"
        "        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',\n"
        "        CONF_ACCENT[accentKey] || CONF_ACCENT.lean,\n"
        "        isLive && 'ring-1 ring-negative/30',\n"
        "      )}\n"
        "      onClick={onClick}\n"
        "      role={onClick ? 'button' : undefined}\n"
        "      tabIndex={onClick ? 0 : undefined}\n"
        "    >\n",
        1,
    ),
])

# ─────────────────────────────────────────────────────────────────────
# components/BasketballPanel.jsx
# ─────────────────────────────────────────────────────────────────────
patch_file("components/BasketballPanel.jsx", [
    ("import BasketballGameDetail from './BasketballGameDetail'\n", "", 1),
    ("  const [selectedGameId, setSelectedGameId] = useState(null)\n", "", 1),
    (
        "  // Detail-pane lookup: when a card is clicked, find the matching raw\n"
        "  // game and render BasketballGameDetail in place of the slate grid.\n"
        "  const selectedRaw = selectedGameId\n"
        "    ? games.find(g => g.game_id === selectedGameId)\n"
        "    : null\n"
        "  if (selectedRaw) {\n"
        "    return (\n"
        "      <BasketballGameDetail\n"
        "        rawGame={selectedRaw}\n"
        "        league={leagueKey}\n"
        "        onBack={() => setSelectedGameId(null)}\n"
        "      />\n"
        "    )\n"
        "  }\n"
        "\n"
        "  // Props / Live tabs route to dedicated panels — they own their own\n",
        "  // Props / Live tabs route to dedicated panels — they own their own\n",
        1,
    ),
    (
        "                        onClick={() => setSelectedGameId(raw.game_id)}\n"
        "                        isPotd={!!raw.is_potd}\n",
        "                        isPotd={!!raw.is_potd}\n",
        1,
    ),
    (
        "                          onClick={() => setSelectedGameId(raw.game_id)}\n"
        "                        />\n"
        "                      )\n"
        "                    })}\n"
        "                  </div>\n"
        "                </>\n"
        "              )}",
        "                        />\n"
        "                      )\n"
        "                    })}\n"
        "                  </div>\n"
        "                </>\n"
        "              )}",
        1,
    ),
])

# ─────────────────────────────────────────────────────────────────────
# components/TennisPanel.jsx  (CRLF file — handled transparently)
# ─────────────────────────────────────────────────────────────────────
patch_file("components/TennisPanel.jsx", [
    ("import TennisMatchDetail from './tennis/MatchDetail'\n", "", 1),
    (
        "  const [view, setView] = useState('index') // index | rankings | matchup | tracker | calibration\n"
        "  const [selectedMatch, setSelectedMatch] = useState(null)\n",
        "  const [view, setView] = useState('index') // index | rankings | matchup | tracker | calibration\n",
        1,
    ),
    (
        "      {view === 'index' && (\n"
        "        <TournamentDropdownView\n"
        "          api={api}\n"
        "          tour={tour}\n"
        "          onOpenMatch={setSelectedMatch}\n"
        "        />\n"
        "      )}\n",
        "      {view === 'index' && (\n"
        "        <TournamentDropdownView\n"
        "          api={api}\n"
        "          tour={tour}\n"
        "        />\n"
        "      )}\n",
        1,
    ),
    (
        "      {view === 'calibration' && (\n"
        "        <CalibrationView api={api} tour={tour} />\n"
        "      )}\n"
        "      {selectedMatch && (\n"
        "        <TennisMatchDetail\n"
        "          api={api}\n"
        "          match={selectedMatch}\n"
        "          onClose={() => setSelectedMatch(null)}\n"
        "        />\n"
        "      )}\n"
        "    </div>\n"
        "  )\n"
        "}\n",
        "      {view === 'calibration' && (\n"
        "        <CalibrationView api={api} tour={tour} />\n"
        "      )}\n"
        "    </div>\n"
        "  )\n"
        "}\n",
        1,
    ),
])

# ─────────────────────────────────────────────────────────────────────
# components/tennis/TournamentDropdownView.jsx  (CRLF)
# ─────────────────────────────────────────────────────────────────────
patch_file("components/tennis/TournamentDropdownView.jsx", [
    (
        "export default function TournamentDropdownView({ api, tour, onOpenMatch }) {\n",
        "export default function TournamentDropdownView({ api, tour }) {\n",
        1,
    ),
    (
        "          <MatchCard key={m.match_id} match={m} onOpen={() => onOpenMatch?.(m)} />\n",
        "          <MatchCard key={m.match_id} match={m} />\n",
        1,
    ),
])

# ─────────────────────────────────────────────────────────────────────
# components/tennis/MatchCard.jsx  (CRLF)
# ─────────────────────────────────────────────────────────────────────
patch_file("components/tennis/MatchCard.jsx", [
    (
        "  return (\n"
        "    <div\n"
        "      className={cn(\n"
        "        'group relative flex flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4 cursor-pointer',\n"
        "        'transition-all duration-150',\n"
        "        'hover:border-border hover:bg-accent/40 hover:-translate-y-px hover:shadow-lg',\n"
        "        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',\n"
        "        accentClass,\n"
        "        isLive && 'ring-1 ring-negative/30',\n"
        "      )}\n"
        "      onClick={onOpen}\n"
        "      role=\"button\"\n"
        "      tabIndex={0}\n"
        "      onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpen?.() } }}\n"
        "    >\n",
        "  return (\n"
        "    <div\n"
        "      className={cn(\n"
        "        'group relative flex flex-col gap-2 rounded-xl border border-border border-l-4 bg-card p-4',\n"
        "        'transition-all duration-150',\n"
        "        'hover:border-border hover:bg-accent/40',\n"
        "        onOpen && 'cursor-pointer hover:-translate-y-px hover:shadow-lg',\n"
        "        'focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',\n"
        "        accentClass,\n"
        "        isLive && 'ring-1 ring-negative/30',\n"
        "      )}\n"
        "      onClick={onOpen}\n"
        "      role={onOpen ? 'button' : undefined}\n"
        "      tabIndex={onOpen ? 0 : undefined}\n"
        "      onKeyDown={onOpen ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onOpen?.() } } : undefined}\n"
        "    >\n",
        1,
    ),
])

print("\nALL EDITS APPLIED SUCCESSFULLY" if not CHECK_ONLY else "\nALL CHECKS PASSED (no files written)")
