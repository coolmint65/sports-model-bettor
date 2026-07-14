/**
 * BasketballPanel — landing for framework-managed basketball leagues
 * (WNBA / NCAAM / Euroleague / 27 international leagues).
 *
 * Uses the NBA primitives (GameCard, SubNav, PickHistory) so swapping
 * NBA → WNBA in the sidebar feels like a season swap, not a different
 * product. NBA itself stays on its existing isNBA path in App.jsx with
 * the full feature surface.
 */

import { useEffect, useMemo, useState } from 'react'
import { useSlate } from '../lib/useSlate'
import GameCard from './primitives/GameCard'
import { OddsGrid, OddsRow, CardInsight as CardInsightShell } from './primitives/OddsGrid'
import MarketToggle from './primitives/MarketToggle'
import PanelShell from './primitives/PanelShell'
import PickHistory from './PickHistory'
import BasketballGameDetail from './BasketballGameDetail'
import PickOfDayHero from './PickOfDayHero'
import PropsPanel from './PropsPanel'
import LiveBetsPanel from './LiveBetsPanel'
import LiveTrackerPanel from './LiveTrackerPanel'

// Mirrors NBA's SubNav: Bets / Tracker / Calibration / Standings.
// Live tab is N/A (Phase 3 — NBA + NHL only).
const BB_TABS = [
  { id: 'bets',        label: 'Bets' },
  { id: 'tracker',     label: 'Tracker' },
  { id: 'calibration', label: 'Calibration' },
  { id: 'standings',   label: 'Standings' },
]

export default function BasketballPanel({ api, leagueKey, leagues }) {
  const cfg = useMemo(
    () => (leagues || []).find(L => L.key === leagueKey),
    [leagues, leagueKey],
  )
  const [view, setView] = useState('bets')

  if (!leagueKey || !cfg) {
    return (
      <section className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-12 text-center">
        <div className="text-sm font-semibold text-foreground">
          Pick a league from the sidebar.
        </div>
      </section>
    )
  }

  const contextChips = useMemo(() => {
    const chips = []
    if (cfg.region) chips.push({ text: cfg.region, key: 'region' })
    if (cfg.in_season === false) {
      chips.push({ tone: 'warning', text: 'Off-season', key: 'season' })
    }
    return chips
  }, [cfg])

  return (
    <PanelShell
      title={cfg.display_name}
      statusBadge={{ label: cfg.status, tone: cfg.status }}
      contextChips={contextChips}
      tabs={BB_TABS}
      active={view}
      onTabChange={setView}
    >
      {view === 'bets' && (
        <>
          <PickOfDayHero sport={leagueKey} />
          <BetsView api={api} leagueKey={leagueKey} cfg={cfg} />
        </>
      )}
      {view === 'tracker' && <TrackerView api={api} leagueKey={leagueKey} />}
      {view === 'calibration' && <CalibrationView api={api} leagueKey={leagueKey} />}
      {view === 'standings' && <StandingsView api={api} leagueKey={leagueKey} />}
    </PanelShell>
  )
}




// ── Bets view ─────────────────────────────────────────────

// Same edge-based confidence tiers tennis uses; matches NBA's
// 12/7/lean cutoffs so the EdgeBadge color story is consistent.
function confidenceFor(edgePct) {
  if (edgePct == null) return 'skip'
  if (edgePct >= 12) return 'strong'
  if (edgePct >= 7) return 'moderate'
  if (edgePct >= 4) return 'lean'
  return 'skip'
}


function adaptGameShape(g) {
  const stateMap = { final: 'post', in: 'in', scheduled: 'pre' }
  // Real start_time only — no synthesized T19:00 placeholder. Spec
  // forbids fake times (standard-layout #7). GameCard handles null
  // date gracefully (renders date-only or no time string).
  if (!g.start_time && !g.date) {
    if (typeof console !== 'undefined' && console.warn) {
      console.warn('adaptGameShape: missing date for game_id', g.game_id)
    }
  }
  const date = g.start_time || g.date || null
  // Detect playoff context — series strip renders when present.
  const gtype = (g.game_type || g.season_type || '').toLowerCase()
  const isPlayoff = gtype.includes('playoff') || gtype.includes('postseason')
  return {
    game_id: g.game_id,
    date,
    home: {
      abbreviation: g.home_abbr,
      name: g.home_name,
      score: g.home_score,
      logo: g.home_logo,
    },
    away: {
      abbreviation: g.away_abbr,
      name: g.away_name,
      score: g.away_score,
      logo: g.away_logo,
    },
    status: {
      state: stateMap[g.status] || 'pre',
      detail: g.status === 'final' ? 'Final' : '',
    },
    // Series metadata — backend's `series` block when present mirrors
    // the NBA/NHL shape (game_number, home_wins, away_wins, etc).
    // GameCard renders the series strip iff series.in_series is true.
    series: g.series || (isPlayoff ? { in_series: true } : null),
    broadcast: g.broadcast || g.tv || '',
    line_movement: g.line_movement || null,
  }
}


function _topReason(g) {
  // Surface the strongest reason chip from the predictor's reasoning
  // array. Mirrors NHL/NBA's CardInsight pattern. Falls through to
  // empty string when no reasoning is shipped.
  const r = g.prediction?.reasoning
  if (!Array.isArray(r) || !r.length) return ''
  // Filter out placeholder messages ("constants not yet fitted...")
  const meaningful = r.filter(x => x && !/not yet fitted|prior/i.test(x))
  return (meaningful[0] || r[0] || '').trim()
}


function BasketballCardInsight({ bet, home, away, raw }) {
  // Mirrors HockeyCardInsight / NHLCardInsight: rank the strongest
  // signals we can derive and surface the top one as a chip. Returns
  // null when nothing meaningful — GameCard renders no insight strip,
  // matching the NHL behaviour for low-info games.
  const reasons = []
  const pred = raw?.prediction || {}
  const factors = pred.factors || {}

  // Recent-form margin gap — reads from _recent_form() output stored
  // on the prediction.factors block by engine.basketball._predict.
  const hr = factors.home_recent || {}
  const ar = factors.away_recent || {}
  if (hr.recent_margin != null && ar.recent_margin != null) {
    const diff = hr.recent_margin - ar.recent_margin
    if (Math.abs(diff) >= 4.0) {
      const better = diff > 0 ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: Math.abs(diff),
        text: <><strong>{better}</strong> +{Math.abs(diff).toFixed(1)} L10 margin gap</>,
      })
    }
  }

  // Predicted-margin lean (when ≥ 5pt).
  if (pred.predicted_margin != null && Math.abs(pred.predicted_margin) >= 5.0) {
    const fav = pred.predicted_margin > 0 ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(pred.predicted_margin) * 0.5,
      text: <><strong>{fav}</strong> projected by {Math.abs(pred.predicted_margin).toFixed(1)}</>,
    })
  }

  // Win-prob lean fallback (≥ 20pp from coinflip).
  const wp = bet?.win_prob
  if (wp?.home != null && Math.abs(wp.home - 0.5) >= 0.20) {
    const fav = wp.home > 0.5 ? home.abbreviation : away.abbreviation
    const pct = Math.round(Math.max(wp.home, wp.away) * 100)
    reasons.push({
      weight: Math.abs(wp.home - 0.5) * 10,
      text: <><strong>{fav}</strong> model favorite ({pct}%)</>,
    })
  }

  if (reasons.length === 0) return null
  reasons.sort((a, b) => b.weight - a.weight)
  return <CardInsightShell>{reasons[0].text}</CardInsightShell>
}


function adaptBetShape(g) {
  const best = g.best_pick
  const conf = best ? confidenceFor(best.edge) : 'skip'
  return {
    game_id: g.game_id,
    best_pick: best,
    confidence: conf,
    win_prob: g.prediction?.ml_home != null
      ? { home: g.prediction.ml_home, away: 1 - g.prediction.ml_home }
      : null,
    // Pass-through slots that GameCard renders conditionally — empty
    // values degrade silently (no chip) per the NHL pattern.
    rest: g.rest || g.prediction?.factors?.rest || null,
  }
}


// Per-league market sections. WNBA gets Q1/Live/Props parity with NBA;
// AFL gets Q1 (HR ships AUSSIE_RULES:P:* per-quarter lines and the
// AFL constants table has q1_* sigmas fitted from 1374 finals).
// NCAAM uses HALVES not quarters — the 'q1' tab id is reused but
// labeled "1st Half" since q1_share for NCAAM = 0.47 (first-half share)
// fitted from the home_q1/away_q1 columns that for NCAAM store first-
// half scores. HR ships H1 markets, which the parser routes into the
// same q1 sub-dict via _Q1_PERIODS = ('Q1', 'H1').
// Every other framework league stays on Full Game only until its own
// markets land. Order matches NBA's BetsView (Full · Q1 · Live · Props).
// Special-case leagues that have extra tabs beyond Full + Q1.
// Everything else in _Q1_LEAGUES gets a plain "Q1" tab appended.
const _MARKET_OPTIONS_SPECIAL = {
  wnba:  [{ id: 'q1', label: 'Q1' },
          { id: 'live',  label: 'Live' },
          { id: 'props', label: 'Player Props' }],
  ncaam: [{ id: 'q1',   label: '1st Half' },
          { id: 'live', label: 'Live' }],
}

// Every framework league with fitted q1_margin_std + q1_home_boost +
// q1_avg_total constants gets a "Q1" tab. Kept as a set so adding a
// new league is one-line: add its key + a Q1 entry to LEAGUE_REGISTRY.
const _Q1_LEAGUES = new Set([
  'afl', 'euroleague', 'ncaaw',
  'china_cba', 'bulgaria_nbl', 'czech_nbl', 'germany_bbl',
  'denmark_basketligaen', 'finland_korisliiga', 'france_pro_b',
  'greece_a1', 'hungary_nb1',
  'iceland_urvalsdeild', 'iceland_urvalsdeild_w', 'israel_super',
  'latvia_lbl', 'lithuania_lkl',
  'slovakia_extraliga', 'slovenia_skl', 'sweden_ligan',
  'argentina_lnb', 'brazil_lbf_w', 'brazil_nbb', 'dominican_lnb',
  'puerto_rico_bsn', 'japan_b2', 'nz_nbl', 'australia_nbl',
  'korea_kbl', 'nba_summer_league',
])

function buildMarketOptions(leagueKey) {
  const opts = [{ id: 'full', label: 'Full Game' }]
  if (_MARKET_OPTIONS_SPECIAL[leagueKey]) {
    opts.push(..._MARKET_OPTIONS_SPECIAL[leagueKey])
  } else if (_Q1_LEAGUES.has(leagueKey)) {
    opts.push({ id: 'q1', label: 'Q1' })
  }
  return opts
}


function BetsView({ api, leagueKey, cfg }) {
  const [err, setErr] = useState(null)
  const [recording, setRecording] = useState(false)
  const [selectedGameId, setSelectedGameId] = useState(null)
  const marketOptions = useMemo(() => buildMarketOptions(leagueKey), [leagueKey])
  const [market, setMarket] = useState(marketOptions[0]?.id || 'full')
  // Reset to Full Game when switching leagues — other leagues don't have
  // the same toggles, and a stale market id would render empty.
  useEffect(() => { setMarket(marketOptions[0]?.id || 'full') },
            [leagueKey, marketOptions])

  // Cached slate — switching between basketball leagues no longer
  // triggers a spinner as long as the target league's slate has been
  // fetched at least once in the last 30 min.
  const slateParams = useMemo(() => ({}), [])
  const { data: slate, loading, refresh: refreshCache } =
    useSlate(`/basketball/${leagueKey}/today`, slateParams, api)
  const refresh = () => { setErr(null); refreshCache() }

  const onRecord = () => {
    setRecording(true)
    api.post(`/basketball/${leagueKey}/tracker/record`)
      .then(() => refresh())
      .catch(() => {})
      .finally(() => setRecording(false))
  }

  const games = slate?.games || []
  // Q1 view re-keys each game's best_pick to best_pick_q1; Full keeps
  // best_pick_full. Cards then show the right pick per market without
  // refetching. Mirrors NBA's BetsView pattern.
  //
  // Earlier this fell back to flat `g.best_pick` when `best_pick_full`
  // was missing — that fix was for WNBA showing no badge when the
  // route only emitted `best_pick`. The fallback was too aggressive
  // though: when a game had Q1 picks but NO full-game pick that
  // cleared all gates, the fallback surfaced the Q1 pick on the Full
  // Game tab (user flagged WNBA MIN @ PHX showing "Q1 TOTAL Over 43.5"
  // on the Full tab 2026-06-01). Now: Full tab shows ONLY full-game
  // candidates; if none, the card renders without a pick badge. The
  // route also writes `best_pick_full` directly so the WNBA fallback
  // is no longer load-bearing.
  const viewKey = market === 'q1' ? 'best_pick_q1' : 'best_pick_full'
  const gamesForView = (market === 'full' || market === 'q1')
    ? games.map(g => {
        const chosen = g[viewKey] != null ? g[viewKey] : null
        return { ...g, best_pick: chosen, picks: chosen ? [chosen] : [] }
      })
    : games
  const totalPicks = gamesForView.reduce((n, g) => n + (g.picks?.length || 0), 0)
  // Partition: scheduled / in-progress on top, finals at bottom — same
  // UX pattern as NHL/NBA/MLB ScoreboardShell. Inside each bucket sort
  // by start_time.
  const activeRaw = gamesForView.filter(g => g.status !== 'final')
  const finalRaw = gamesForView.filter(g => g.status === 'final')
  const sortByStart = (a, b) => (a.start_time || '').localeCompare(b.start_time || '')
  activeRaw.sort(sortByStart)
  finalRaw.sort(sortByStart)
  const adaptedActive = activeRaw.map(g => ({
    game: adaptGameShape(g), bet: adaptBetShape(g), raw: g,
  }))
  const adaptedFinal = finalRaw.map(g => ({
    game: adaptGameShape(g), bet: adaptBetShape(g), raw: g,
  }))

  // Detail-pane lookup: when a card is clicked, find the matching raw
  // game and render BasketballGameDetail in place of the slate grid.
  const selectedRaw = selectedGameId
    ? games.find(g => g.game_id === selectedGameId)
    : null
  if (selectedRaw) {
    return (
      <BasketballGameDetail
        rawGame={selectedRaw}
        league={leagueKey}
        onBack={() => setSelectedGameId(null)}
      />
    )
  }

  // Props / Live tabs route to dedicated panels — they own their own
  // loaders + layouts. Same pattern NBA uses in BetsView. Skip the
  // slate scaffold when those tabs are active.
  if (market === 'props') {
    return (
      <div className="space-y-4">
        {marketOptions.length > 1 && (
          <div className="flex justify-end">
            <MarketToggle options={marketOptions} active={market} onChange={setMarket} />
          </div>
        )}
        <PropsPanel sport={leagueKey} />
      </div>
    )
  }
  if (market === 'live') {
    return (
      <div className="space-y-4">
        {marketOptions.length > 1 && (
          <div className="flex justify-end">
            <MarketToggle options={marketOptions} active={market} onChange={setMarket} />
          </div>
        )}
        <LiveBetsPanel sport={leagueKey} />
      </div>
    )
  }

  return (
    <div className="space-y-4">
      {marketOptions.length > 1 && (
        <div className="flex justify-end">
          <MarketToggle options={marketOptions} active={market} onChange={setMarket} />
        </div>
      )}
      {loading && (
        <div className="rounded-lg border border-border bg-card px-6 py-10 text-center text-sm text-muted-foreground">
          Loading {cfg.display_name} slate…
        </div>
      )}
      {err && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-5 py-4 text-sm text-destructive">
          Failed to load slate: {err}
        </div>
      )}
      {slate && !loading && (
        <>
          {slate.fell_forward && (
            <div className="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[11px] font-medium text-amber-200">
              No games today — showing next slate ({slate.date}).
            </div>
          )}
          <div className="flex items-center justify-between">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {slate.date} · {games.length} game{games.length === 1 ? '' : 's'}
              {totalPicks > 0 && (
                <span className="ml-2 text-primary">
                  · {totalPicks} pick{totalPicks === 1 ? '' : 's'}
                </span>
              )}
            </div>
            {totalPicks > 0 && (
              <button
                onClick={onRecord}
                disabled={recording}
                className="text-[11px] font-semibold uppercase tracking-wider px-3 py-1 rounded border border-border hover:bg-accent disabled:opacity-50"
              >
                {recording ? 'Recording…' : 'Record picks'}
              </button>
            )}
          </div>
          {games.length === 0 ? (
            <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
              <div className="text-sm font-semibold text-foreground">
                No games on this slate.
              </div>
            </div>
          ) : (
            <>
              {adaptedActive.length > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {adaptedActive.map(({ game, bet, raw }) => {
                    const home = game.home, away = game.away
                    return (
                      <GameCard
                        key={game.game_id}
                        game={game}
                        bet={bet}
                        sport={leagueKey}
                        odds={<HrOddsRow raw={raw} />}
                        insight={<BasketballCardInsight bet={bet} home={home} away={away} raw={raw} />}
                        onClick={() => setSelectedGameId(raw.game_id)}
                        isPotd={!!raw.is_potd}
                        restTiredLabel="B2B"
                      />
                    )
                  })}
                </div>
              )}
              {adaptedFinal.length > 0 && (
                <>
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground/70 pt-3">
                    Final · {adaptedFinal.length}
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3 opacity-80">
                    {adaptedFinal.map(({ game, bet, raw }) => {
                      const home = game.home, away = game.away
                      return (
                        <GameCard
                          key={game.game_id}
                          game={game}
                          bet={bet}
                          sport={leagueKey}
                          odds={<HrOddsRow raw={raw} />}
                          insight={<BasketballCardInsight bet={bet} home={home} away={away} raw={raw} />}
                          onClick={() => setSelectedGameId(raw.game_id)}
                        />
                      )
                    })}
                  </div>
                </>
              )}
            </>
          )}
          <div className="text-[10px] text-muted-foreground italic px-1">
            Beta — paper-bet only until ROI accumulates.
            Calibration applied per-bucket from {slate.date}'s walk-forward seed.
          </div>
        </>
      )}
    </div>
  )
}


function HrOddsRow({ raw }) {
  // Match NBA / NHL / MLB OddsGrid rhythm exactly:
  //   ML   AWAY-ABBR <signed-odds>          HOME-ABBR <signed-odds>
  //   SPR  AWAY-ABBR ±pt (<signed-odds>)    HOME-ABBR ±pt (<signed-odds>)
  //   O/U  o<line> (<signed-odds>)           u<line> (<signed-odds>)
  // Discrepancies vs that standard before this fix:
  //   - ML / Spread cells dropped the team abbreviation
  //   - Spread row labelled "Spread" instead of "SPR"
  //   - O/U swapped under to the away side and used uppercase "U " /
  //     "O " with a space ("U 220.5" vs "u220.5")
  // All three made the basketball card visibly out-of-rhythm with NBA's.
  const odds = raw.odds
  if (!odds) return null
  const fmt = (n) => n == null ? '—' : (n > 0 ? `+${n}` : `${n}`)
  const ptFmt = (pt) => pt == null ? '—' : (pt > 0 ? `+${pt}` : `${pt}`)
  const ml = odds.ml || {}
  const sp = odds.spread || {}
  const tot = odds.total || {}
  const homeAbbr = raw.home_abbr || ''
  const awayAbbr = raw.away_abbr || ''
  const mlCell = (abbr, val) =>
    val == null ? `${abbr} —` : `${abbr} ${fmt(val)}`
  const spreadCell = (abbr, pt, oddsVal) => {
    if (pt == null) return `${abbr} —`
    return `${abbr} ${ptFmt(pt)} (${fmt(oddsVal)})`
  }
  return (
    <OddsGrid>
      {(ml.home != null || ml.away != null) && (
        <OddsRow
          label="ML"
          away={mlCell(awayAbbr, ml.away)}
          home={mlCell(homeAbbr, ml.home)}
        />
      )}
      {sp.home_pt != null && (
        <OddsRow
          label="SPR"
          away={spreadCell(awayAbbr, sp.away_pt, sp.away_odds)}
          home={spreadCell(homeAbbr, sp.home_pt, sp.home_odds)}
        />
      )}
      {tot.line != null && (
        <OddsRow
          label="O/U"
          away={`o${tot.line} (${fmt(tot.over_odds)})`}
          home={`u${tot.line} (${fmt(tot.under_odds)})`}
        />
      )}
    </OddsGrid>
  )
}


// ── Tracker view ─────────────────────────────────────────

// ── Standings view ────────────────────────────────────────

function StandingsView({ api, leagueKey }) {
  const [rows, setRows] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    api.get(`/basketball/${leagueKey}/standings`)
      .then(r => setRows(Array.isArray(r.data) ? r.data : []))
      .catch(() => setRows([]))
      .finally(() => setLoading(false))
  }, [api, leagueKey])

  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card px-6 py-10 text-center text-sm text-muted-foreground">
        Loading standings…
      </div>
    )
  }
  if (!rows || rows.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm">
        <div className="font-semibold text-foreground">No standings available.</div>
        <div className="mt-1 text-xs text-muted-foreground">
          League hasn't accumulated enough finalized games this season.
        </div>
      </div>
    )
  }

  // Group by conference/division when present (mirrors NBA layout);
  // fall back to single bucket for international leagues.
  const groups = {}
  for (const r of rows) {
    const k = r.conference || r.division || 'Standings'
    if (!groups[k]) groups[k] = []
    groups[k].push(r)
  }

  return (
    <section className="space-y-4">
      {Object.entries(groups).map(([grp, teams]) => (
        <div key={grp} className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold">{grp}</h3>
          </div>
          <table className="w-full text-sm">
            <thead className="bg-muted/40">
              <tr className="text-[10px] uppercase tracking-wider text-muted-foreground">
                <th className="text-left px-4 py-2">#</th>
                <th className="text-left px-4 py-2">Team</th>
                <th className="text-right px-4 py-2">W</th>
                <th className="text-right px-4 py-2">L</th>
                <th className="text-right px-4 py-2">PCT</th>
                <th className="text-right px-4 py-2">GB</th>
                <th className="text-right px-4 py-2">PPG</th>
                <th className="text-right px-4 py-2">PAPG</th>
                <th className="text-right px-4 py-2">DIFF</th>
                <th className="text-right px-4 py-2">L10</th>
                <th className="text-right px-4 py-2">STRK</th>
              </tr>
            </thead>
            <tbody>
              {teams.map((t, i) => {
                const diff = (t.points_for || 0) - (t.points_against || 0)
                return (
                  <tr key={t.team_id} className="border-t border-border/40">
                    <td className="px-4 py-2 text-muted-foreground tabular-nums">{i + 1}</td>
                    <td className="px-4 py-2">
                      <span className="inline-flex items-center gap-2">
                        {t.logo && (
                          <img src={t.logo} alt="" className="h-4 w-4" loading="lazy" />
                        )}
                        <span className="font-bold">{t.abbreviation}</span>
                        <span className="text-muted-foreground">{t.name}</span>
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.wins}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.losses}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{(t.win_pct ?? 0).toFixed(3)}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.gb ?? '-'}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.ppg ?? '-'}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.papg ?? '-'}</td>
                    <td className={`px-4 py-2 text-right tabular-nums font-semibold ${
                      diff > 0 ? 'text-positive' : diff < 0 ? 'text-negative' : ''
                    }`}>
                      {diff > 0 ? '+' : ''}{diff}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums text-muted-foreground">{t.l10 || '0-0'}</td>
                    <td className="px-4 py-2 text-right tabular-nums font-semibold">{t.streak || '-'}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ))}
    </section>
  )
}


// ── Calibration view ──────────────────────────────────────

function CalibrationView({ api, leagueKey }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    api.get(`/basketball/${leagueKey}/calibration`)
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [api, leagueKey])

  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card px-6 py-10 text-center text-sm text-muted-foreground">
        Loading calibration…
      </div>
    )
  }
  if (!data) return null
  const buckets = (data.buckets?.ML || [])

  return (
    <section className="space-y-4">
      <header className="rounded-lg border border-border bg-card px-5 py-4">
        <h3 className="text-sm font-semibold">Calibration</h3>
        <p className="mt-1 text-xs text-muted-foreground">
          Walk-forward seed from {data.n_processed || 0} holdout games
          {data.test_start_date && <> (test start {data.test_start_date})</>}.
          Method: <span className="font-semibold">{data.method || 'none'}</span>.
        </p>
      </header>

      <section className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="px-5 py-3 border-b border-border">
          <h4 className="text-sm font-semibold">ML Buckets</h4>
          <p className="mt-0.5 text-[11px] text-muted-foreground">
            Predicted probability bucket vs realized win rate. Calibration
            applies when n ≥ 5; sparser buckets fall through to raw.
          </p>
        </div>
        <table className="w-full text-sm">
          <thead className="bg-muted/40">
            <tr className="text-[10px] uppercase tracking-wider text-muted-foreground">
              <th className="text-left px-4 py-2">Bucket</th>
              <th className="text-right px-4 py-2">N</th>
              <th className="text-right px-4 py-2">Avg Pred</th>
              <th className="text-right px-4 py-2">Realized WR</th>
              <th className="text-right px-4 py-2">Δ</th>
            </tr>
          </thead>
          <tbody>
            {buckets.map((b, i) => {
              const lo = b.bucket?.[0]
              const hi = b.bucket?.[1]
              const delta = (b.avg_pred != null && b.realized_wr != null)
                ? (b.realized_wr - b.avg_pred) * 100
                : null
              return (
                <tr key={i} className="border-t border-border/40">
                  <td className="px-4 py-2 tabular-nums">[{lo?.toFixed(2)}, {hi?.toFixed(2)})</td>
                  <td className="px-4 py-2 text-right tabular-nums text-muted-foreground">
                    {b.n}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums">
                    {b.avg_pred != null ? `${(b.avg_pred * 100).toFixed(1)}%` : '—'}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums font-semibold">
                    {b.realized_wr != null ? `${(b.realized_wr * 100).toFixed(1)}%` : '—'}
                  </td>
                  <td className={`px-4 py-2 text-right tabular-nums ${
                    delta == null ? 'text-muted-foreground'
                      : Math.abs(delta) > 5
                        ? (delta > 0 ? 'text-positive' : 'text-negative')
                        : 'text-foreground'
                  }`}>
                    {delta != null ? `${delta > 0 ? '+' : ''}${delta.toFixed(1)}pp` : '—'}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </section>

      {data.deferred && data.deferred.length > 0 && (
        <div className="rounded-lg border border-warning/30 bg-warning/5 px-4 py-3 text-xs text-warning">
          {data.deferred.map((d, i) => <div key={i}>· {d}</div>)}
        </div>
      )}
    </section>
  )
}


// WNBA / AFL tracker market sections — mirrors NBA's TrackerView so
// per-market P/L doesn't blend Q1 + full + live + props into one muddled
// number. Other framework leagues stay on the single full-game tracker
// until they grow their own market footprint.
const Q1_BET_TYPES = new Set(['Q1_ML', 'Q1_SPREAD', 'Q1_TOTAL'])
const FULL_BET_TYPES = new Set(['ML', 'SPREAD', 'TOTAL'])


// Tracker view uses the same market options as the bets view. Keeping
// them separate lets us diverge later (e.g. if the tracker gains a
// tab the bets view doesn't need), but until then delegate.
const buildTrackerOptions = buildMarketOptions


function _recomputeTrackerSummary(rows, byType, allowedTypes) {
  // Hero + tiles both pull from the FULL cumulative `by_type` summary,
  // not the visible /tracker/history slice. Per user directive
  // (2026-05-17): P/L numbers must reflect every settled pick on file,
  // not just what scrolled into view. CLV stays computed from the
  // visible rows because the backend's summary doesn't ship per-market
  // CLV; the visible-slice average is the closest fallback.
  const filteredByType = allowedTypes
    ? Object.fromEntries(
        Object.entries(byType || {}).filter(([k]) => allowedTypes.has(k))
      )
    : (byType || {})
  let wins = 0, losses = 0, pushes = 0, pending = 0, profit = 0, total = 0
  for (const v of Object.values(filteredByType)) {
    total += v.total || 0
    wins += v.wins || 0
    losses += v.losses || 0
    pushes += v.pushes || 0
    pending += v.pending || 0
    profit += Number(v.profit || 0)
  }
  let clvSum = 0, clvN = 0
  for (const r of rows) {
    if (r.odds != null && r.closing_odds != null) {
      const betImp = r.odds < 0
        ? Math.abs(r.odds) / (Math.abs(r.odds) + 100)
        : 100 / (r.odds + 100)
      const clsImp = r.closing_odds < 0
        ? Math.abs(r.closing_odds) / (Math.abs(r.closing_odds) + 100)
        : 100 / (r.closing_odds + 100)
      clvSum += (clsImp - betImp) * 100
      clvN += 1
    }
  }
  const settled = wins + losses
  return {
    overall: {
      total,
      wins, losses, pushes, pending,
      profit: Math.round(profit * 100) / 100,
      win_pct: settled > 0 ? Math.round(wins / settled * 1000) / 10 : 0,
      avg_clv: clvN > 0 ? Math.round(clvSum / clvN * 100) / 100 : null,
      clv_sample: clvN,
    },
    by_type: filteredByType,
  }
}


function TrackerView({ api, leagueKey }) {
  const [rows, setRows] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(false)
  const [settling, setSettling] = useState(false)
  const trackerOptions = useMemo(() => buildTrackerOptions(leagueKey), [leagueKey])
  const [market, setMarket] = useState(trackerOptions[0]?.id || 'full')
  useEffect(() => { setMarket(trackerOptions[0]?.id || 'full') },
            [leagueKey, trackerOptions])

  const refresh = () => {
    setLoading(true)
    api.get(`/basketball/${leagueKey}/tracker/history`)
      .then(r => {
        setRows(r.data?.rows || [])
        setSummary(r.data?.summary || null)
      })
      .catch(() => { setRows([]); setSummary(null) })
      .finally(() => setLoading(false))
  }
  useEffect(refresh, [api, leagueKey])  // eslint-disable-line

  const onSettle = () => {
    setSettling(true)
    api.post(`/basketball/${leagueKey}/tracker/settle`)
      .then(() => refresh())
      .catch(() => {})
      .finally(() => setSettling(false))
  }
  const onRecord = () => {
    api.post(`/basketball/${leagueKey}/tracker/record`).finally(refresh)
  }

  // Props / Live tabs delegate to their own panels (which carry their
  // own P/L summaries and history sections).
  if (market === 'props') {
    return (
      <div className="space-y-4">
        {trackerOptions.length > 1 && (
          <div className="flex justify-end">
            <MarketToggle options={trackerOptions} active={market} onChange={setMarket} />
          </div>
        )}
        <PropsPanel sport={leagueKey} />
      </div>
    )
  }
  if (market === 'live') {
    // Tracker → Live shows the history panel (pending + settled live
    // picks with snapshot context). The Bets → Live tab uses the active
    // LiveBetsPanel instead. Mirrors NBA's split.
    return (
      <div className="space-y-4">
        {trackerOptions.length > 1 && (
          <div className="flex justify-end">
            <MarketToggle options={trackerOptions} active={market} onChange={setMarket} />
          </div>
        )}
        <LiveTrackerPanel sport={leagueKey} />
      </div>
    )
  }

  // Full vs Q1 filter rebuilds the summary so the hero P/L + per-tile
  // by_type numbers reflect only the selected market. Any league with
  // a Q1 tab (wnba/afl today) needs the Full filter too so the Full
  // P/L number doesn't sweep Q1 picks back in.
  const hasQ1Tab = trackerOptions.some(o => o.id === 'q1')
  const allowedTypes = market === 'q1' ? Q1_BET_TYPES
                     : market === 'full' && hasQ1Tab ? FULL_BET_TYPES
                     : null
  const filteredHistory = allowedTypes
    ? (rows || []).filter(p => allowedTypes.has(p.bet_type))
    : (rows || [])
  const filteredSummary = allowedTypes
    ? _recomputeTrackerSummary(filteredHistory, summary?.by_type, allowedTypes)
    : summary

  return (
    <div className="space-y-4">
      {trackerOptions.length > 1 && (
        <div className="flex justify-end">
          <MarketToggle options={trackerOptions} active={market} onChange={setMarket} />
        </div>
      )}
      <PickHistory
        summary={filteredSummary}
        history={filteredHistory}
        loading={loading}
        onRecord={onRecord}
        onSettle={onSettle}
        sport={leagueKey}
      />
    </div>
  )
}
