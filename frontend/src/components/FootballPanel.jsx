/**
 * FootballPanel — landing for football-framework leagues (UFL today,
 * NFL/NCAAF later).
 *
 * Mirrors HockeyPanel line-for-line for parity: same PanelShell tabs,
 * same active/final partition, same OddsGrid styling, same
 * CalibrationView shape. NHL/NBA-style insight strip swapped for
 * football-specific reasons (margin edge, total lean, win-prob
 * favorite). UFL doesn't have a goalie/starter equivalent so
 * starters=null.
 */
import { useEffect, useMemo, useState } from 'react'
import { cn } from '../lib/utils'
import GameCard from './primitives/GameCard'
import RailLayout, { CardAnchor } from './primitives/RailLayout'
import { OddsGrid, OddsRow, CardInsight as CardInsightShell } from './primitives/OddsGrid'
import MarketToggle from './primitives/MarketToggle'
import PanelShell from './primitives/PanelShell'
import PickHistory from './PickHistory'
import UnifiedTracker from './UnifiedTracker'
import PaperPickOfDayHero from './PaperPickOfDayHero'
import PickOfDayHero from './PickOfDayHero'
import PropsPanel from './PropsPanel'
import CLVBadge from './CLVBadge'

// Outer tabs mirror WNBA's BB_TABS exactly (Bets · Tracker · Calibration ·
// Standings). Player-Props is no longer a top-level tab —
// they moved INSIDE the Bets/Tracker views as MarketToggle options
// (Full Game · Anytime TDs · Player Props), matching how WNBA nests
// Full · Q1 · Player Props under a single Bets tab. See buildMarketOptions.
const FB_TABS = [
  { id: 'bets',        label: 'Bets' },
  { id: 'tracker',     label: 'Tracker' },
  { id: 'calibration', label: 'Calibration' },
  { id: 'standings',   label: 'Standings' },
]


export default function FootballPanel({ api, leagueKey = 'ufl' }) {
  const [cfg, setCfg] = useState(null)
  const [view, setView] = useState('bets')

  useEffect(() => {
    api.get('/football/leagues')
      .then(r => {
        const entry = (r.data?.leagues || []).find(L => L.key === leagueKey)
        setCfg(entry || null)
      })
      .catch(() => setCfg(null))
  }, [api, leagueKey])

  if (!leagueKey || !cfg) {
    return (
      <PanelShell title="Football" tabs={FB_TABS} active={view} onTabChange={setView}>
        <section className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-12 text-center">
          <div className="text-sm font-semibold text-foreground">Loading league…</div>
        </section>
      </PanelShell>
    )
  }

  const contextChips = []
  if (cfg.region) contextChips.push({ text: cfg.region, key: 'region' })
  if (cfg.in_season === false) {
    contextChips.push({ tone: 'warning', text: 'Off-season', key: 'season' })
  }

  return (
    <PanelShell
      title={cfg.display_name}
      statusBadge={{ label: cfg.status, tone: cfg.status }}
      contextChips={contextChips}
      tabs={FB_TABS}
      active={view}
      onTabChange={setView}
    >
      {view === 'bets' && (
        <BetsView api={api} leagueKey={leagueKey} cfg={cfg} />
      )}
      {view === 'tracker'     && <UnifiedTracker sport={`football/${leagueKey}`} api={api} />}
      {view === 'calibration' && <CalibrationView api={api} leagueKey={leagueKey} />}
      {view === 'standings'   && <StandingsView api={api} leagueKey={leagueKey} />}
    </PanelShell>
  )
}


function StandingsView({ api, leagueKey }) {
  const [rows, setRows] = useState(null)
  const [loading, setLoading] = useState(false)
  useEffect(() => {
    setLoading(true)
    api.get(`/football/${leagueKey}/standings`)
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
  return (
    <section className="rounded-lg border border-border bg-card overflow-hidden">
      <table className="w-full text-sm">
        <thead className="bg-muted/40">
          <tr className="text-[10px] uppercase tracking-wider text-muted-foreground">
            <th className="text-left px-4 py-2">#</th>
            <th className="text-left px-4 py-2">Team</th>
            <th className="text-right px-4 py-2">W</th>
            <th className="text-right px-4 py-2">L</th>
            <th className="text-right px-4 py-2">T</th>
            <th className="text-right px-4 py-2">PCT</th>
            <th className="text-right px-4 py-2">PF</th>
            <th className="text-right px-4 py-2">PA</th>
            <th className="text-right px-4 py-2">DIFF</th>
            <th className="text-right px-4 py-2">L10</th>
            <th className="text-right px-4 py-2">STRK</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((t, i) => (
            <tr key={t.team_id ?? i}
                className={cn('border-t border-border/60',
                  i < 3 && 'bg-primary/5')}>
              <td className="px-4 py-2 tabular-nums text-muted-foreground">{i + 1}</td>
              <td className="px-4 py-2">
                <span className="flex items-center gap-2">
                  {t.logo_url && (
                    <img src={t.logo_url} alt="" className="h-5 w-5" />
                  )}
                  <span className="font-medium">{t.name || t.abbreviation}</span>
                </span>
              </td>
              <td className="px-4 py-2 text-right tabular-nums">{t.wins ?? 0}</td>
              <td className="px-4 py-2 text-right tabular-nums">{t.losses ?? 0}</td>
              <td className="px-4 py-2 text-right tabular-nums">{t.ties ?? 0}</td>
              <td className="px-4 py-2 text-right tabular-nums">
                {(t.pct ?? 0).toFixed(3)}
              </td>
              <td className="px-4 py-2 text-right tabular-nums">{t.points_for ?? 0}</td>
              <td className="px-4 py-2 text-right tabular-nums">{t.points_against ?? 0}</td>
              <td className={cn('px-4 py-2 text-right tabular-nums',
                (t.point_diff ?? 0) > 0 ? 'text-positive'
                : (t.point_diff ?? 0) < 0 ? 'text-negative' : '')}>
                {(t.point_diff ?? 0) > 0 ? '+' : ''}{t.point_diff ?? 0}
              </td>
              <td className="px-4 py-2 text-right tabular-nums text-muted-foreground">
                {t.l10 ?? '—'}
              </td>
              <td className="px-4 py-2 text-right tabular-nums font-medium">
                {t.streak ?? '—'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}


// ── Bets view ────────────────────────────────────────────────

function confidenceFor(edgePct) {
  if (edgePct == null) return 'skip'
  if (edgePct >= 6) return 'strong'
  if (edgePct >= 3) return 'moderate'
  if (edgePct >= 1) return 'lean'
  return 'skip'
}


function adaptGameShape(g) {
  // routes_football returns rows with status: scheduled/live/final and
  // score in home_score/away_score. Map to the GameCard envelope NHL/
  // basketball use.
  const stateMap = { final: 'post', live: 'in', scheduled: 'pre' }
  return {
    game_id: g.game_id,
    id: g.game_id,
    date: g.start_time || null,
    home: {
      abbreviation: g.home_abbr,
      name: g.home_name,
      score: g.home_score,
      logo: g.home_logo,
      odds: (g.odds && g.odds.home_ml != null) ? g.odds.home_ml : null,  // per-side ML on the card (overhaul step 1)
    },
    away: {
      abbreviation: g.away_abbr,
      name: g.away_name,
      score: g.away_score,
      logo: g.away_logo,
      odds: (g.odds && g.odds.away_ml != null) ? g.odds.away_ml : null,
    },
    status: {
      state: stateMap[g.status] || 'pre',
      detail: g.status === 'final' ? 'Final' : (g.status_detail || ''),   // live clock, e.g. "9:15 - 3rd" (2026-10-31)
    },
    series: null,
    odds: g.odds || null,
    broadcast: null,
    venue: g.venue || null,
    line_movement: g.line_movement || null,
  }
}


function adaptBetShape(g) {
  const best = g.best_pick
  const conf = best ? (best.confidence || confidenceFor(best.edge)) : 'skip'
  const pred = g.prediction || {}
  return {
    game_id: g.game_id,
    best_pick: best,
    paper_pick: (() => { const pp = g.paper_picks || []; return pp.length ? { ...pp.reduce((a, b) => ((b.edge ?? -99) > (a.edge ?? -99) ? b : a)), graduated: !!g.best_pick } : (g.card_lean || null); })(),  // lean fallback (Austin 2026-10-26)
    confidence: conf,
    win_prob: g.card_win_prob ?? (pred.p_home != null
      ? { home: pred.p_home, away: pred.p_away ?? (1 - pred.p_home) }
      : null),
    factors: {},
    season_context: {},
    rest: {},
    injuries: {},
  }
}


// Per-league market sections — mirrors WNBA's _MARKET_OPTIONS_SPECIAL +
// buildMarketOptions. Player-Props is NFL-only (the models
// are nflverse-backed and NFL-gated on the backend), so only NFL gets the
// extra market toggles; CFB/UFL show "Full Game" alone (no toggle renders
// when there's a single option, same as a WNBA league with no Q1/props).
// Adding a league to either market is one line here.
const _FB_MARKET_SPECIAL = {
  nfl: [{ id: 'props',      label: 'Player Props' }],
}

function buildMarketOptions(leagueKey) {
  const opts = [{ id: 'best', label: 'Best' }]
  if (_FB_MARKET_SPECIAL[leagueKey]) {
    opts.push(..._FB_MARKET_SPECIAL[leagueKey])
  }
  return opts
}


function BetsView({ api, leagueKey, cfg }) {
  const marketOptions = useMemo(() => buildMarketOptions(leagueKey), [leagueKey])
  const [market, setMarket] = useState(marketOptions[0]?.id || 'full')
  // Reset to Full Game when switching leagues — a stale market id (e.g.
  // 'props' carried over to a league without it) would render empty.
  useEffect(() => { setMarket(marketOptions[0]?.id || 'full') },
            [leagueKey, marketOptions])
  const [slate, setSlate] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)
  const [paperRec, setPaperRec] = useState(null)  // paper-shadow aggregate for the POTD record

  const refresh = (silent = false) => {
    // Austin 2026-10-17: background polls (the 30s interval + the cold-retry)
    // pass silent=true so they DON'T flash the loading state. setLoading(true)
    // was swapping the whole slate out for a spinner every 30s and bouncing you
    // back to the top of the page; silent polls update the slate in place.
    if (!silent) setLoading(true)
    setErr(null)
    api.get(`/football/${leagueKey}/today`)
      .then(r => setSlate(r.data))
      .catch(e => setErr(e?.message || String(e)))
      .finally(() => { if (!silent) setLoading(false) })
  }
  useEffect(refresh, [api, leagueKey])  // eslint-disable-line
  // Keep the slate ticking while the tab stays open — refresh() above only
  // fires once per mount/league-change, so without this a live card (score,
  // clock, status) freezes at whatever was loaded, even though the backend
  // keeps moving. Mirrors LiveBetsPanel's setInterval (Austin 2026-10-10).
  useEffect(() => {
    const interval = setInterval(() => refresh(true), 30000)
    return () => clearInterval(interval)
  }, [api, leagueKey])  // eslint-disable-line
  // POTD running record (paper-shadow aggregate) — Austin 2026-09-19.
  useEffect(() => {
    let alive = true
    api.get(`/football/${leagueKey}/tracker/history`)
      .then(r => { if (alive) setPaperRec(r.data?.paper?.potd_record || null) })  // POTD-own record (Austin 2026-09-19)
      .catch(() => { if (alive) setPaperRec(null) })
    return () => { alive = false }
  }, [api, leagueKey])
  // Cold-cache backend returns {loading:true} + computes in the background;
  // poll every few seconds until the real slate lands (Austin 2026-09-19 perf).
  useEffect(() => {
    if (!slate?.loading) return
    const t = setTimeout(() => refresh(true), 4000)
    return () => clearTimeout(t)
  }, [slate])  // eslint-disable-line

  const games = slate?.games || []
  const totalPicks = games.reduce((n, g) => n + (g.picks?.length || 0), 0)
  const activeRaw = games.filter(g => g.status !== 'final')
  const finalRaw = games.filter(g => g.status === 'final')
  const sortByStart = (a, b) => (a.start_time || '').localeCompare(b.start_time || '')
  activeRaw.sort(sortByStart)
  finalRaw.sort(sortByStart)
  const adaptedActive = activeRaw.map(g => ({
    game: adaptGameShape(g), bet: adaptBetShape(g), raw: g,
  }))
  const adaptedFinal = finalRaw.map(g => ({
    game: adaptGameShape(g), bet: adaptBetShape(g), raw: g,
  }))

  // The Player-Props market delegates to its own panel (each
  // owns its loader + persist/settle history), same as WNBA's props/live
  // early returns — skip the full-game slate scaffold entirely.
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

  return (
    <div className="space-y-4">
      {marketOptions.length > 1 && (
        <div className="flex justify-end">
          <MarketToggle options={marketOptions} active={market} onChange={setMarket} />
        </div>
      )}
      {(loading || slate?.loading) && (
        <div className="rounded-lg border border-border bg-card px-6 py-10 text-center text-sm text-muted-foreground">
          Loading {cfg.display_name} slate…
        </div>
      )}
      {err && (
        <div className="rounded-lg border border-destructive/40 bg-destructive/10 px-5 py-4 text-sm text-destructive">
          Failed to load slate: {err}
        </div>
      )}
      {slate && !loading && !slate.loading && (
        <>
          <PickOfDayHero sport={leagueKey} />
          <div className="flex items-center justify-between">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {slate.date} · {games.length} game{games.length === 1 ? '' : 's'}
              {totalPicks > 0 && (
                <span className="ml-2 text-primary">
                  · {totalPicks} pick{totalPicks === 1 ? '' : 's'}
                </span>
              )}
            </div>
          </div>
          {games.length === 0 ? (
            <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
              <div className="text-sm font-semibold text-foreground">
                No games on this slate.
              </div>
              <div className="text-xs text-muted-foreground mt-1">
                {leagueKey === 'ufl'
                  ? 'UFL spring season runs March–June. Slate populates as ESPN posts the upcoming week.'
                  : 'Slate populates as ESPN posts upcoming games — check back on game day.'}
              </div>
            </div>
          ) : (
            <RailLayout entries={[...adaptedActive, ...adaptedFinal]}>
              {adaptedActive.length > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {adaptedActive.map(({ game, bet, raw }) => (
                    <CardAnchor key={game.game_id} id={game.id ?? game.game_id}>
                    <FootballGameCard
                      game={game}
                      bet={bet}
                      raw={raw}
                      sport={`football/${leagueKey}`}
                    />
                    </CardAnchor>
                  ))}
                </div>
              )}
              {adaptedFinal.length > 0 && (
                <>
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground/70 pt-3">
                    Final · {adaptedFinal.length}
                  </div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3 opacity-80">
                    {adaptedFinal.map(({ game, bet, raw }) => (
                      <CardAnchor key={game.game_id} id={game.id ?? game.game_id}>
                      <FootballGameCard
                        game={game}
                        bet={bet}
                        raw={raw}
                        sport={`football/${leagueKey}`}
                      />
                      </CardAnchor>
                    ))}
                  </div>
                </>
              )}
            </RailLayout>
          )}
          <div className="text-[10px] text-muted-foreground italic px-1">
            Beta — Elo + Monte Carlo ensemble. GBM leg gated until the
            league accumulates ~500 finalized games.
          </div>
        </>
      )}
    </div>
  )
}


// ── FootballGameCard ─────────────────────────────────────────

function FootballGameCard({ game, bet, raw, sport }) {
  const { home, away } = game
  const odds = game.odds ? <FootballOddsGrid odds={game.odds} home={home} away={away} /> : null
  const insight = <FootballCardInsight bet={bet} home={home} away={away} raw={raw} />

  return (
    <GameCard
      game={game}
      bet={bet}
      insight={insight}
      odds={odds}
      sport={sport}
    />
  )
}


function FootballOddsGrid({ odds, home, away }) {
  // Football is ML + SPRD + OU — same trio NHL has, just with "SPRD"
  // instead of "PL" since football spreads aren't fixed at ±1.5 the
  // way puck lines are.
  return (
    <OddsGrid>
      {(odds.home_ml || odds.away_ml) && (
        <OddsRow
          label="ML"
          away={`${away.abbreviation} ${odds.away_ml > 0 ? '+' : ''}${odds.away_ml || '-'}`}
          home={`${home.abbreviation} ${odds.home_ml > 0 ? '+' : ''}${odds.home_ml || '-'}`}
        />
      )}
      {odds.over_under && (
        <OddsRow
          label="O/U"
          away={`o${odds.over_under}${odds.over_odds ? ` (${Math.round(odds.over_odds) > 0 ? '+' : ''}${Math.round(odds.over_odds)})` : ''}`}
          home={`u${odds.over_under}${odds.under_odds ? ` (${Math.round(odds.under_odds) > 0 ? '+' : ''}${Math.round(odds.under_odds)})` : ''}`}
        />
      )}
      {odds.home_spread_point != null && (
        <OddsRow
          label="SPRD"
          away={`${away.abbreviation} ${odds.away_spread_point > 0 ? '+' : ''}${odds.away_spread_point}${odds.away_spread_odds ? ` (${odds.away_spread_odds > 0 ? '+' : ''}${odds.away_spread_odds})` : ''}`}
          home={`${home.abbreviation} ${odds.home_spread_point > 0 ? '+' : ''}${odds.home_spread_point}${odds.home_spread_odds ? ` (${odds.home_spread_odds > 0 ? '+' : ''}${odds.home_spread_odds})` : ''}`}
        />
      )}
    </OddsGrid>
  )
}


function FootballCardInsight({ bet, home, away, raw }) {
  // Mirrors NHL/Hockey insight — surface the "strongest reason" the
  // pick fires (or the model leans), so even no-pick games tell the
  // user *why* the model has nothing meaningful.
  const reasons = []
  const pred = raw?.prediction || {}

  if (pred.expected_margin != null && Math.abs(pred.expected_margin) >= 4) {
    const fav = pred.expected_margin > 0 ? home.abbreviation : away.abbreviation
    reasons.push({
      weight: Math.abs(pred.expected_margin),
      text: <><strong>{fav}</strong> projected +{Math.abs(pred.expected_margin).toFixed(1)} pt margin</>,
    })
  }

  if (pred.expected_total != null && raw?.odds?.over_under != null) {
    const diff = pred.expected_total - raw.odds.over_under
    if (Math.abs(diff) >= 1.5) {
      const lean = diff > 0 ? 'Over' : 'Under'
      reasons.push({
        weight: Math.abs(diff) * 2,
        text: <><strong>{lean}</strong> lean — model {pred.expected_total.toFixed(1)} vs line {raw.odds.over_under}</>,
      })
    }
  }

  const wp = bet?.win_prob
  if (wp?.home != null && Math.abs(wp.home - 0.5) >= 0.20) {
    const fav = wp.home > 0.5 ? home.abbreviation : away.abbreviation
    const pct = Math.round(Math.max(wp.home, wp.away) * 100)
    reasons.push({
      weight: Math.abs(wp.home - 0.5) * 10,
      text: <><strong>{fav}</strong> model favorite ({pct}%)</>,
    })
  }

  // One-bet-per-card (Austin 2026-09-19): the paper lean used to render here IN
  // ADDITION to the gated EdgeBadge (this insight only shows when there IS a gated
  // pick), so a gated game showed green + a redundant paper badge. The single
  // headline bet lives in GameCard (gated EdgeBadge, or PaperBadge when there's no
  // gated pick); the POTD carries the best paper lean. Insight = strongest reason.
  if (reasons.length === 0) return null
  reasons.sort((a, b) => b.weight - a.weight)
  return (
    <div className="space-y-1">
      <CardInsightShell>{reasons[0].text}</CardInsightShell>
    </div>
  )
}


// ── Tracker view ─────────────────────────────────────────────


function fmtAmerican(o) {
  if (o == null) return '—'
  return o > 0 ? `+${o}` : `${o}`
}

function PlayerPropCard({ p }) {
  const [imgOk, setImgOk] = useState(true)
  const pct = Math.round((p.model_prob || 0) * 100)
  const edge = p.edge
  const showEv = p.plus_ev && edge != null && edge <= 30
  const initials = String(p.player_name || '?')
    .split(' ').map(s => s[0]).filter(Boolean).slice(0, 2).join('')
  const accent = (edge != null && edge >= 6) ? 'border-l-positive'
    : (edge != null && edge > 0) ? 'border-l-primary' : 'border-l-border'
  const thr = Math.round((p.line ?? 0))
  return (
    <div className={cn(
      'flex items-center gap-3 rounded-xl border border-border border-l-4 bg-card p-3',
      'transition-colors hover:bg-accent/40', accent)}>
      <div className="relative h-12 w-12 flex-shrink-0">
        <div className="h-12 w-12 overflow-hidden rounded-full bg-muted ring-1 ring-border">
          {imgOk && p.headshot
            ? <img src={p.headshot} alt="" className="h-full w-full object-cover"
                   onError={() => setImgOk(false)} />
            : <div className="flex h-full w-full items-center justify-center text-xs font-bold text-muted-foreground">
                {initials}
              </div>}
        </div>
        {p.team_logo && (
          <img src={p.team_logo} alt="" loading="lazy"
               className="absolute -bottom-1 -right-1 h-5 w-5 rounded-full bg-card p-0.5 ring-1 ring-border" />
        )}
      </div>
      <div className="min-w-0 flex-1">
        <div className="truncate font-semibold text-foreground">{p.player_name}</div>
        <div className="text-[11px] text-muted-foreground">{p.position || '—'} · {p.team}</div>
        <div className="mt-1 text-sm font-semibold text-foreground">
          {p.bet_type} <span className="text-primary">{thr}+</span>
          {p.projection != null && (
            <span className="ml-2 text-[11px] font-normal text-muted-foreground">
              proj {p.projection}
            </span>
          )}
        </div>
      </div>
      <div className="flex-shrink-0 text-right">
        <div className="text-xl font-bold leading-none tabular-nums text-foreground">{pct}%</div>
        <div className="mt-0.5 text-[9px] uppercase tracking-wider text-muted-foreground">to clear</div>
        <div className="mt-1 flex items-center justify-end gap-2">
          <span className="text-xs tabular-nums text-muted-foreground">
            {p.odds != null ? fmtAmerican(p.odds) : '—'}
          </span>
          {edge != null && (
            <span className={cn('text-xs font-semibold tabular-nums',
              edge > 30 ? 'text-amber-500'
                : edge > 0 ? 'text-positive' : 'text-muted-foreground')}>
              {edge > 0 ? '+' : ''}{edge.toFixed(1)}%
            </span>
          )}
          {showEv && (
            <span className="rounded bg-positive/15 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-positive">
              +EV
            </span>
          )}
        </div>
        <div className="mt-1 flex justify-end">
          <CLVBadge entryOdds={p.odds} closingOdds={p.closing_odds} />
        </div>
      </div>
    </div>
  )
}

function PlayerPropsView({ api, leagueKey }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    api.get(`/${leagueKey}/player-props/today`)
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [api, leagueKey])

  if (loading) {
    return (
      <div className="rounded-xl border border-border bg-card px-6 py-10 text-center text-sm text-muted-foreground">
        Loading player props…
      </div>
    )
  }
  const picks = data?.picks || []
  if (!picks.length) {
    return (
      <div className="rounded-xl border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm">
        <div className="font-semibold text-foreground">No player props yet.</div>
        <div className="mt-1 text-xs text-muted-foreground">
          {data?.message
            ? data.message
            : 'Populates when Hard Rock posts NFL player-prop lines for the upcoming slate.'}
        </div>
      </div>
    )
  }
  return (
    <section className="space-y-2">
      <div className="px-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        {data.date} · {picks.length} prop{picks.length === 1 ? '' : 's'} · passing / rushing / receiving yards + receptions · ranked by edge
      </div>
      <div className="space-y-2">
        {picks.map(p => (
          <PlayerPropCard key={`${p.player_id}-${p.bet_type}-${p.line}`} p={p} />
        ))}
      </div>
      <div className="px-1 pt-1 text-[10px] italic text-muted-foreground">
        Paper model · projects each player's per-game distribution from nflverse
        play-by-play → P(stat ≥ line), priced against Hard Rock's alt-line
        thresholds. Edge = model − HR implied.
      </div>
    </section>
  )
}


// Tracker uses the same market options as the Bets view (WNBA keeps them
// separate to allow later divergence, but delegates for now). Full-game
// tracker = PickHistory; the Player-Props market delegates to
// the same panels the Bets view uses, each of which carries its own
// settled/W-L history (so they double as their own tracker).
const buildTrackerOptions = buildMarketOptions


function TrackerView({ api, leagueKey }) {
  const trackerOptions = useMemo(() => buildTrackerOptions(leagueKey), [leagueKey])
  const [market, setMarket] = useState(trackerOptions[0]?.id || 'full')
  useEffect(() => { setMarket(trackerOptions[0]?.id || 'full') },
            [leagueKey, trackerOptions])
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  const refresh = () => {
    setLoading(true)
    api.get(`/football/${leagueKey}/tracker/history`)
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }
  useEffect(refresh, [api, leagueKey])  // eslint-disable-line

  const onSettle = () => {
    api.post(`/football/${leagueKey}/tracker/settle`)
      .catch(() => null)
      .finally(refresh)
  }

  // The Player-Props tracker market delegates to its own panel
  // (they show settled picks with WON/LOST inline), mirroring how WNBA's
  // tracker routes props → PropsPanel.
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

  return (
    <div className="space-y-4">
      {trackerOptions.length > 1 && (
        <div className="flex justify-end">
          <MarketToggle options={trackerOptions} active={market} onChange={setMarket} />
        </div>
      )}
      <PickHistory
      summary={data?.summary}
      paper={data?.paper?.summary}
      paperRows={Object.values((data?.paper?.rows || []).reduce((acc, r) => {
        // One play per game (Austin 2026-09-19, opt B): keep the highest-edge
        // pick per matchup so the tracker reads like a card, not 3 correlated
        // bets — the shadow DB still records spread+total+ML for the market eval.
        const _k = (r.date || '') + '|' + (r.matchup || '')
        if (Math.abs(r.edge ?? 0) > 15) return acc  // |edge|<=15 ceiling — same best-lean rule as POTD/card/count (Austin 2026-09-21)
        if (!acc[_k] || (r.edge ?? -Infinity) > (acc[_k].edge ?? -Infinity)) acc[_k] = r  // max POSITIVE edge, not biggest fade (Austin 2026-09-21)
        return acc
      }, {})).map(r => {
        // Show the line on spread/total picks (Austin 2026-09-19): the paper rows
        // store pick + line separately, so combine them for the PICK column.
        const bt = String(r.bet_type || '').toUpperCase()
        const pick = bt === 'TOTAL'
          ? `${r.pick} ${r.line}`
          : (bt === 'SPREAD' || bt === 'RUN_LINE' || bt === 'PUCK_LINE') && r.line != null
            ? `${r.pick} ${r.line > 0 ? '+' : ''}${r.line}`
            : r.pick
        return { ...r, type: r.bet_type, prob: r.model_prob, pick }
      })}
      history={data?.rows || []}
      loading={loading}
      onSettle={onSettle}
      sport={leagueKey}
      />
    </div>
  )
}


// ── Calibration view ─────────────────────────────────────────

function CalibrationView({ api, leagueKey }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    api.get(`/football/${leagueKey}/calibration`)
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
  const e = data.ensemble || {}
  return (
    <section className="space-y-3">
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="px-5 py-3 border-b border-border">
          <h3 className="text-sm font-semibold">Fitted constants</h3>
          <p className="text-[10px] text-muted-foreground mt-0.5">
            Derived from the league's finalized-game history.
          </p>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-px bg-border">
          <Stat label="Avg total points" value={fmt(c.league_avg_total)} />
          <Stat label="Home edge (points)" value={fmt(c.home_advantage_pts, '+')} />
          <Stat label="Margin σ" value={fmt(c.margin_sigma)} />
          <Stat label="Total σ" value={fmt(c.total_sigma)} />
          <Stat label="Fitted games" value={c.fitted_n ?? '—'} />
          <Stat label="Settled picks" value={data.n_settled ?? 0} />
        </div>
      </div>
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="px-5 py-3 border-b border-border">
          <h3 className="text-sm font-semibold">Ensemble weights</h3>
          <p className="text-[10px] text-muted-foreground mt-0.5">
            Factor (Elo + Normal) / Monte Carlo / Gradient-boosted blend.
            GBM gated until the league accumulates enough training data.
          </p>
        </div>
        <div className="grid grid-cols-3 gap-px bg-border">
          <Stat label="Factor weight" value={fmt(e?.ml?.factor)} />
          <Stat label="MC weight" value={fmt(e?.ml?.mc)} />
          <Stat label="GBM weight" value={e?.gbm_trained ? fmt(e?.ml?.gbm) : 'gated'} />
        </div>
      </div>
      {(!data.buckets || data.buckets.length === 0) && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-5 py-4 text-xs text-muted-foreground">
          Per-bucket Brier / hit-rate metrics appear here once enough
          picks settle. Until then we show the fitted constants and
          ensemble weights only.
        </div>
      )}
    </section>
  )
}


function fmt(v, prefix = '') {
  if (v == null) return '—'
  if (typeof v === 'number') {
    return `${prefix && v >= 0 ? prefix : ''}${v.toFixed(2)}`
  }
  return String(v)
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
