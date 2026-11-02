/**
 * SoccerPanel — landing for the multi-league soccer framework.
 *
 * Mirrors BasketballPanel + HockeyPanel exactly so every soccer
 * competition (MLS / Big-5 / UEFA / CONMEBOL / FIFA / WC) matches the
 * card chrome users already know from the other team-sport panels.
 * The sidebar holds league selection (SoccerGroup); this panel renders
 * one league at a time via the leagueKey + leagues props.
 */
import { useEffect, useMemo, useState } from 'react'
import { clvPct } from '../lib/clv'
import { useSlate } from '../lib/useSlate'
import GameCard from './primitives/GameCard'
import RailLayout, { CardAnchor } from './primitives/RailLayout'
import MarketToggle from './primitives/MarketToggle'
import { OddsGrid, OddsRow, CardInsight as CardInsightShell } from './primitives/OddsGrid'
import PanelShell from './primitives/PanelShell'
import PickHistory from './PickHistory'
import UnifiedTracker from './UnifiedTracker'
import PickOfDayHero from './PickOfDayHero'
import PaperPickOfDayHero from './PaperPickOfDayHero'

const SOC_TABS = [
  { id: 'bets',        label: 'Bets' },
  { id: 'tracker',     label: 'Tracker' },
  { id: 'calibration', label: 'Calibration' },
]


// Per-league market segments inside the Bets view. Soccer leagues that
// have HT scores backfilled (h1_share fitted) + HR ships SOCCER:P:*
// markets get an H1 segment alongside Full Game — matches the
// Q1-tab pattern basketball uses inside BetsView. Leagues without HT
// data stay Full-only until the backfill lands.
//
// New leagues should be added here as their H1 calibration lands. WC
// 2026 + NWSL added 2026-05-29 — both emit H1_* picks via the shared
// soccer picker but were missing from this whitelist, so the H1 tab
// silently never rendered.
const H1_LEAGUES = new Set([
  'mls', 'ger_bundesliga', 'esp_laliga', 'fra_ligue1', 'eng_premier',
  'ita_seriea', 'uefa_champions', 'uefa_europa', 'uefa_conference',
  'usl_championship', 'arg_lpf', 'bra_seriea',
  'conmebol_libertadores', 'us_open_cup',
  'us_nwsl',
])

function buildMarketOptions(leagueKey) {
  // H1 merged (Austin 2026-10-12): H1 tab dropped; tracker shows full-game
  // + the H1 paper rows together (segmented), not a separate tab.
  const opts = [{ id: 'best', label: 'Best' }]
  return opts
}


// Same shape as Bets options — tracker mirrors the Bets segments so the
// same per-market split applies to settled history + P/L numbers.
function buildTrackerOptions(leagueKey) {
  return buildMarketOptions(leagueKey)
}


// Soccer bet-type partitions for the tracker per-market filter. H1 covers
// every H1_* market type; FULL is the residual (ML / OU / DC / DNB /
// BTTS / AH). Mirrors basketball's Q1_BET_TYPES / FULL_BET_TYPES pattern
// so per-market P/L doesn't blend Full + H1 into one muddled number.
const SOC_H1_BET_TYPES = new Set([
  'H1_ML', 'H1_OU', 'H1_TOTAL', 'H1_DC', 'H1_DNB', 'H1_BTTS',
])
const SOC_FULL_BET_TYPES = new Set([
  'ML', 'OU', 'TOTAL', 'DC', 'DNB', 'BTTS', 'AH',
])


function _recomputeSoccerTrackerSummary(rows, byType, allowedTypes) {
  // Hero + tiles pull from the FULL cumulative `by_type` summary (not
  // the visible /tracker/history slice). Per user directive 2026-05-17:
  // P/L numbers must reflect every settled pick on file, not just what
  // scrolled into view. CLV stays computed from the visible rows.
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
    const _c = clvPct(r.odds, r.closing_odds)
    if (_c != null) { clvSum += _c; clvN += 1 }
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


export default function SoccerPanel({ api, leagueKey, leagues }) {
  const cfg = useMemo(
    () => (leagues || []).find(L => L.key === leagueKey),
    [leagues, leagueKey],
  )
  const [view, setView] = useState('bets')

  if (!leagueKey || !cfg) {
    return (
      <section className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-12 text-center">
        <div className="text-sm font-semibold text-foreground">
          Pick a soccer competition from the sidebar.
        </div>
        <div className="mt-1 text-xs text-muted-foreground">
          {leagues?.length || 0} leagues across UEFA / CONMEBOL / FIFA /
          CONCACAF available.
        </div>
      </section>
    )
  }

  const contextChips = useMemo(() => {
    const chips = []
    if (cfg.confederation) chips.push({ text: cfg.confederation, key: 'conf' })
    if (cfg.country && cfg.country !== 'International') {
      chips.push({ text: cfg.country, key: 'country' })
    }
    if (cfg.competition_type === 'cup') {
      chips.push({ text: 'Cup', key: 'cup' })
    }
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
      tabs={SOC_TABS}
      active={view}
      onTabChange={setView}
    >
      {view === 'bets'        && <BetsView api={api} leagueKey={leagueKey} cfg={cfg} />}
      {view === 'tracker'     && <UnifiedTracker sport={`soccer/${leagueKey}`} api={api} />}
      {view === 'calibration' && <CalibrationView api={api} leagueKey={leagueKey} />}
    </PanelShell>
  )
}


function CalibrationView({ api, leagueKey }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  useEffect(() => {
    setLoading(true)
    api.get(`/soccer/${leagueKey}/calibration`)
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
  if (!data) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm">
        <div className="font-semibold text-foreground">No calibration data.</div>
      </div>
    )
  }
  const c = data.constants || {}
  const fmt = (v, prefix = '') => {
    if (v == null) return '—'
    if (typeof v === 'number') return `${prefix && v >= 0 ? prefix : ''}${v.toFixed(3)}`
    return String(v)
  }
  return (
    <section className="space-y-3">
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="px-5 py-3 border-b border-border">
          <h3 className="text-sm font-semibold">Fitted constants</h3>
          <p className="text-[10px] text-muted-foreground mt-0.5">
            Dixon-Coles bivariate Poisson fit from the league's recent
            finalized matches.
          </p>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-px bg-border">
          <Stat label="Avg home goals" value={fmt(c.avg_home_goals)} />
          <Stat label="Avg away goals" value={fmt(c.avg_away_goals)} />
          <Stat label="Home advantage" value={fmt(c.home_advantage, '+')} />
          <Stat label="Dixon-Coles ρ" value={fmt(c.dc_rho)} />
          <Stat label="Fitted matches" value={c.fitted_n ?? '—'} />
          <Stat label="Settled picks" value={data.n_settled ?? 0} />
        </div>
      </div>
    </section>
  )
}


function Stat({ label, value }) {
  return (
    <div className="bg-card px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className="font-bold tabular-nums text-foreground mt-0.5">
        {value}
      </div>
    </div>
  )
}


// ── Bets view ─────────────────────────────────────────────

function confidenceFor(edgePct) {
  if (edgePct == null) return 'skip'
  if (edgePct >= 12) return 'strong'
  if (edgePct >= 7)  return 'moderate'
  if (edgePct >= 4)  return 'lean'
  return 'skip'
}


function adaptMatchToGameShape(m) {
  // routes_soccer/today returns Dixon-Coles-shaped rows. Map to the
  // GameCard primitive shape every other team-sport panel uses so the
  // logo strip, time label, and bet badge render with the same chrome.
  // status/score come from the backend's matches row (2026-10-31: cards sat on "pre" forever)
  const st = String(m.status || 'scheduled').toLowerCase()
  const state = st === 'final' ? 'post' : (st === 'live' || st === 'halftime') ? 'in' : 'pre'
  const showScore = state !== 'pre'
  return {
    game_id: m.match_id,
    id: m.match_id,
    date: m.start_time || null,
    home: {
      abbreviation: m.home?.abbr || '',
      name: m.home?.name || '',
      logo: m.home?.logo || null,
      score: showScore ? (m.home_score ?? null) : null,
      odds: m.odds?.ml?.home ?? null,  // per-side ML (overhaul step 1)
    },
    away: {
      abbreviation: m.away?.abbr || '',
      name: m.away?.name || '',
      logo: m.away?.logo || null,
      score: showScore ? (m.away_score ?? null) : null,
      odds: m.odds?.ml?.away ?? null,
    },
    status: { state, detail: state === 'post' ? 'FT' : (m.status_detail || ''), completed: state === 'post' },
    series: null,
    odds: m.odds || null,
    broadcast: null,
    venue: null,
    line_movement: null,
  }
}


function adaptMatchToBetShape(m, scope = 'full') {
  // Top pick = highest-edge from m.picks filtered by scope (full vs H1).
  // Mirror the basketball best_pick shape so GameCard's bet-badge surface
  // renders identically. Picks engine emits at most one full + one H1
  // per match (see engine.soccer._picks.generate_picks_for_match) so
  // we pull the first match in the scope-family.
  const wantH1 = scope === 'h1'
  const picks = (m.picks || []).filter(p => {
    const t = String(p.type || '').toUpperCase()
    return wantH1 ? t.startsWith('H1_') : true  // 'full' scope = best of ALL markets incl H1/team-total (card picks any, like NHL/CFB) Austin 2026-10-17
  })
  // Step 3 (Pick Authority): read the BACKEND's authoritative pick (apply_selected);
  // fall back to the frontend select only if the backend hasn't set one (pre-restart). Austin 2026-10-17
  const top = (!wantH1 && m.best_pick) ? m.best_pick
    : (!wantH1 && m.card_lean) ? m.card_lean : (picks[0] || null)
  const topGraduated = !wantH1 && !!m.best_pick && top === m.best_pick   // lean vs graduated badge (Austin 2026-10-26)
  const best = top ? {
    graduated: topGraduated || top.graduated === true,
    lean: !topGraduated && top.graduated !== true,
    type: top.type,
    pick: top.pick,
    odds: top.odds,
    edge: top.edge,
    prob: top.raw_prob ?? top.model_prob ?? null,
    confidence: top.confidence || confidenceFor(top.edge),
    // Stake units — must pass through to GameCard's EdgeBadge so soccer
    // cards show the recommended-unit tag (1u / 0.75u / 0.5u). Missing
    // until 2026-05-29; cards rendered without the stake pill.
    stake_units: top.stake_units,
    result: top.result || null,   // held lean's W/L/P once the match is final (2026-10-31)
  } : null
  const conf = best ? best.confidence : 'skip'
  const pred = m.prediction || {}
  // 1X2 win probabilities — feed home/away into the GameCard ProbBar
  // and stash p_draw in the insight slot so we don't lose it.
  const winProb = m.card_win_prob ?? ((pred.p_home != null || pred.p_away != null) ? {
    home: pred.p_home,
    away: pred.p_away,
    draw: pred.p_draw,
  } : null)
  return {
    game_id: m.match_id,
    best_pick: best,
    // GameCard renders bet.paper_pick (paper-only mode), not best_pick, so soccer
    // cards showed NO PICK without this. display = the full pick text (soccer types
    // like OU/DC/DNB don't match PaperBadge's label construction). MintyBot 2026-10-16.
    paper_pick: best ? { ...best, display: best.pick } : null,
    confidence: conf,
    win_prob: winProb,
    factors: {},
    season_context: {},
    rest: {},
    injuries: {},
  }
}


function BetsView({ api, leagueKey, cfg }) {
  const [err, setErr] = useState(null)
  const marketOptions = useMemo(() => buildMarketOptions(leagueKey), [leagueKey])
  const [market, setMarket] = useState(marketOptions[0]?.id || 'full')
  // Reset market when switching leagues — leagues without H1 toggle
  // would otherwise show a stale 'h1' selection with no picks rendered.
  useEffect(() => { setMarket(marketOptions[0]?.id || 'full') },
            [leagueKey, marketOptions])

  const slateParams = useMemo(() => ({}), [])
  const { data: slate, loading, refresh: refreshCache } =
    useSlate(`/soccer/${leagueKey}/today`, slateParams, api)
  const refresh = () => { setErr(null); refreshCache() }

  // POTD-OWN record (Austin 2026-09-19): the Pick-of-the-Day's own running
  // W-L / profit from /pick-of-day/{league}/summary. Falls back to the
  // per-family tracker aggregate for leagues with no POTD logged yet.
  const [paperRec, setPaperRec] = useState(null)
  const [lockedPotd, setLockedPotd] = useState(null)
  useEffect(() => {
    let alive = true
    const aggFallback = () => api.get(`/soccer/${leagueKey}/tracker/history`)
      .then(r => {
        if (!alive) return
        const bt = r.data?.summary?.by_type || {}
        const agg = (typeSet) => {
          let wins = 0, losses = 0, pushes = 0, profit = 0
          for (const [k, b] of Object.entries(bt)) {
            if (!typeSet.has(k)) continue
            wins += b.wins || 0; losses += b.losses || 0
            pushes += b.pushes || 0; profit += b.profit || 0
          }
          const dec = wins + losses
          return { wins, losses, pushes, total: wins + losses + pushes,
                   profit: Math.round(profit * 100) / 100,
                   win_pct: dec ? Math.round(1000 * wins / dec) / 10 : 0,
                   roi: dec ? Math.round(10 * profit / dec) / 10 : 0 }
        }
        setPaperRec({ full: agg(SOC_FULL_BET_TYPES), h1: agg(SOC_H1_BET_TYPES) })
      })
      .catch(() => { if (alive) setPaperRec(null) })
    api.get(`/pick-of-day/${leagueKey}/summary`)
      .then(r => {
        if (!alive) return
        const s = r.data || {}
        if (s && !s.error && (s.total || 0) > 0) {
          setPaperRec({ full: { ...s }, h1: { ...s } })
        } else { aggFallback() }
      })
      .catch(() => { aggFallback() })
    return () => { alive = false }
  }, [api, leagueKey])

  // EPL hero (Austin 2026-10-17 issue B): HOLD the backend's locked full-game POTD
  // (stable per day, skips H1) instead of re-deriving the hero from the rolling
  // slate's max-edge pick, which flipped + headlined H1 picks mid-day.
  useEffect(() => {
    let alive = true
    api.get(`/soccer/${leagueKey}/potd`)
      .then(r => { if (!alive) return; const pd = r.data; setLockedPotd(pd && !pd.error && pd.pick ? pd : null) })
      .catch(() => { if (alive) setLockedPotd(null) })
    return () => { alive = false }
  }, [api, leagueKey])

  const matches = slate?.matches || []
  // Count picks within the active scope only — full vs H1.
  const scope = market
  const wantH1 = scope === 'h1'
  const totalPicks = matches.reduce((n, m) => {
    const filtered = (m.picks || []).filter(p => {
      const t = String(p.type || '').toUpperCase()
      return wantH1 ? t.startsWith('H1_') : true  // 'full' scope = best of ALL markets incl H1/team-total (card picks any, like NHL/CFB) Austin 2026-10-17
    })
    return n + filtered.length
  }, 0)
  // Sort by start time ascending.
  const sorted = [...matches].sort(
    (a, b) => (a.start_time || '').localeCompare(b.start_time || ''),
  )
  const adapted = sorted.map(m => ({
    game: adaptMatchToGameShape(m),
    bet: adaptMatchToBetShape(m, scope),
    raw: m,
  }))

  const paperGames = useMemo(() => (matches || []).map(m => {
    const b = adaptMatchToBetShape(m, scope).best_pick
    return b ? { home_abbr: m.home?.abbr, away_abbr: m.away?.abbr, home_logo: m.home?.logo, away_logo: m.away?.logo, paper_picks: [{ type: b.type, display: b.pick, odds: b.odds, edge: b.edge, prob: b.prob, matchup: m.matchup }] } : null
  }).filter(Boolean), [matches, scope])

  return (
    <div className="space-y-4">
      <PaperPickOfDayHero games={paperGames} sport="soccer" summary={scope === 'h1' ? paperRec?.h1 : paperRec?.full} locked={scope === 'h1' ? undefined : (lockedPotd ?? null)} />
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
          <div className="flex items-center justify-between">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {slate.date} · {matches.length} match{matches.length === 1 ? '' : 'es'}
              {totalPicks > 0 && (
                <span className="ml-2 text-primary">
                  · {totalPicks} pick{totalPicks === 1 ? '' : 's'}
                </span>
              )}
            </div>
          </div>
          {matches.length === 0 ? (
            <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
              <div className="text-sm font-semibold text-foreground">
                No matches on this slate.
              </div>
              <div className="mt-1 text-xs text-muted-foreground">
                ESPN scoreboard will pick up the next kickoff window.
              </div>
            </div>
          ) : (
            <RailLayout entries={adapted}>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
              {adapted.map(({ game, bet, raw }) => (
                <CardAnchor key={game.game_id} id={game.id ?? game.game_id}>
                <SoccerGameCard
                  game={game}
                  bet={bet}
                  raw={raw}
                  sport={`soccer/${leagueKey}`}
                />
                </CardAnchor>
              ))}
            </div>
            </RailLayout>
          )}
          <UpcomingMatches
            upcoming={slate?.upcoming || []}
            leagueKey={leagueKey}
            scope={scope}
          />
          <div className="text-[10px] text-muted-foreground italic px-1">
            Beta — Dixon-Coles bivariate Poisson with fitted goal rates
            for {cfg.display_name}. Paper-bet until 14-day live ROI
            window opens.
          </div>
        </>
      )}
    </div>
  )
}


function UpcomingMatches({ upcoming, leagueKey, scope }) {
  if (!upcoming || upcoming.length === 0) return null
  // Backend ships the next day's slate in the same match shape as the
  // active slate, so we can run them through the same adapter and
  // render with the same SoccerGameCard primitive — full odds grid,
  // pick badge, insight tag. Single date (tomorrow), so we just need
  // one section header.
  const fmtDate = (iso) => {
    if (!iso) return ''
    try {
      return new Date(iso + 'T12:00:00').toLocaleDateString(undefined,
        { weekday: 'short', month: 'short', day: 'numeric' })
    } catch { return iso }
  }
  const sorted = [...upcoming].sort(
    (a, b) => (a.start_time || '').localeCompare(b.start_time || ''),
  )
  const adapted = sorted.map(m => ({
    game: adaptMatchToGameShape(m),
    bet:  adaptMatchToBetShape(m, scope || 'full'),
    raw:  m,
  }))
  const tomorrowDate = sorted[0]?.date
  return (
    <section className="mt-6">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground mb-2">
        Tomorrow · {fmtDate(tomorrowDate)} · {upcoming.length} match{upcoming.length === 1 ? '' : 'es'}
      </div>
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 opacity-90">
        {adapted.map(({ game, bet, raw }) => (
          <SoccerGameCard
            key={game.game_id}
            game={game}
            bet={bet}
            raw={raw}
            sport={`soccer/${leagueKey}`}
          />
        ))}
      </div>
    </section>
  )
}


// ── SoccerGameCard ───────────────────────────────────────
//
// Mirror of HockeyGameCard / NHLGameCard so soccer cards render with
// the same primitives: logo strip, score row (TBD pre-game), odds grid
// for 1X2 / OU / BTTS, plus a single-line insight tag.

function SoccerGameCard({ game, bet, raw, sport }) {
  const { home, away } = game

  const odds = (raw?.odds && (raw.odds.ml || raw.odds.total || raw.odds.btts))
    ? <SoccerOddsGrid odds={raw.odds} home={home} away={away} />
    : null
  const insight = <SoccerCardInsight bet={bet} home={home} away={away} raw={raw} />

  return (
    <GameCard
      game={game}
      bet={bet}
      insight={insight}
      odds={odds}
      restTiredLabel={null}
      sport={sport}
      isPotd={!!raw?.is_potd}
    />
  )
}


function SoccerOddsGrid({ odds, home, away }) {
  // 1X2 row: home / draw / away moneylines.
  // O/U row: line + over/under odds.
  // BTTS row: yes / no when shipped.
  const ml = odds.ml || {}
  const tot = odds.total || {}
  const btts = odds.btts || {}
  const fmtOdds = (o) => (o == null ? '—' : (o > 0 ? `+${o}` : `${o}`))

  return (
    <OddsGrid>
      {(ml.home != null || ml.away != null) && (
        <OddsRow label="1X2"
          away={`${away.abbreviation} ${fmtOdds(ml.away)}`}
          center={ml.draw != null
            ? `Draw ${fmtOdds(ml.draw)}`
            : null}
          home={`${home.abbreviation} ${fmtOdds(ml.home)}`}
        />
      )}
      {tot.line != null && (
        <OddsRow label="O/U"
          away={`o${tot.line}${tot.over_odds != null ? ` (${fmtOdds(tot.over_odds)})` : ''}`}
          home={`u${tot.line}${tot.under_odds != null ? ` (${fmtOdds(tot.under_odds)})` : ''}`}
        />
      )}
      {(btts.yes != null || btts.no != null) && (
        <OddsRow label="BTTS"
          away={`Yes ${fmtOdds(btts.yes)}`}
          home={`No ${fmtOdds(btts.no)}`}
        />
      )}
    </OddsGrid>
  )
}


function SoccerCardInsight({ bet, home, away, raw }) {
  // Mirrors HockeyCardInsight's "strongest reason" pattern. Soccer
  // surfaces: lambda gap (goal-rate edge) → 1X2 win-prob lean → draw
  // lean (significant when p_draw is the modal outcome). Returns null
  // when nothing meaningful; GameCard renders no strip in that case.
  const reasons = []
  const pred = raw?.prediction || {}

  if (pred.lambda_home != null && pred.lambda_away != null) {
    const diff = pred.lambda_home - pred.lambda_away
    if (Math.abs(diff) >= 0.5) {
      const fav = diff > 0 ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: Math.abs(diff) * 4,
        text: <><strong>{fav}</strong> projected +{Math.abs(diff).toFixed(2)} goal edge</>,
      })
    }
  }

  if (pred.p_home != null && pred.p_away != null) {
    const lean = pred.p_home - pred.p_away
    if (Math.abs(lean) >= 0.20) {
      const fav = lean > 0 ? home.abbreviation : away.abbreviation
      const pct = Math.round(Math.max(pred.p_home, pred.p_away) * 100)
      reasons.push({
        weight: Math.abs(lean) * 10,
        text: <><strong>{fav}</strong> model favorite ({pct}%)</>,
      })
    }
  }

  if (pred.p_draw != null && pred.p_draw >= 0.32) {
    reasons.push({
      weight: pred.p_draw * 5,
      text: <>Draw a real threat ({Math.round(pred.p_draw * 100)}% model)</>,
    })
  }

  if (reasons.length === 0) return null
  reasons.sort((a, b) => b.weight - a.weight)
  return <CardInsightShell>{reasons[0].text}</CardInsightShell>
}


// ── Tracker view ─────────────────────────────────────────

function TrackerView({ api, leagueKey }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)
  const trackerOptions = useMemo(() => buildTrackerOptions(leagueKey),
                                  [leagueKey])
  const [market, setMarket] = useState(trackerOptions[0]?.id || 'full')
  useEffect(() => { setMarket(trackerOptions[0]?.id || 'full') },
            [leagueKey, trackerOptions])

  const refresh = () => {
    setLoading(true)
    api.get(`/soccer/${leagueKey}/tracker/history`)
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }
  useEffect(refresh, [api, leagueKey])  // eslint-disable-line

  const onRecord = () => {
    api.post(`/soccer/${leagueKey}/tracker/record`).finally(refresh)
  }
  const onSettle = () => {
    api.post(`/soccer/${leagueKey}/tracker/settle`).finally(refresh)
  }

  // Full vs H1 filter — when a league has the H1 tab we also filter the
  // Full P/L to exclude H1_* picks so the two numbers don't double-count.
  const rows = data?.rows || []
  const hasH1Tab = trackerOptions.some(o => o.id === 'h1')
  const allowedTypes = market === 'h1' ? SOC_H1_BET_TYPES
                     : market === 'best' && hasH1Tab ? SOC_FULL_BET_TYPES
                     : null
  const filteredHistory = allowedTypes
    ? rows.filter(p => allowedTypes.has(p.bet_type))
    : rows
  const filteredSummary = allowedTypes
    ? _recomputeSoccerTrackerSummary(filteredHistory,
                                       data?.summary?.by_type, allowedTypes)
    : data?.summary

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
