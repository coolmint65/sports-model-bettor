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
import { OddsGrid, OddsRow, CardInsight as CardInsightShell } from './primitives/OddsGrid'
import PanelShell from './primitives/PanelShell'
import PickHistory from './PickHistory'
import PickOfDayHero from './PickOfDayHero'

const FB_TABS = [
  { id: 'bets',        label: 'Bets' },
  { id: 'tracker',     label: 'Tracker' },
  { id: 'standings',   label: 'Standings' },
  { id: 'calibration', label: 'Calibration' },
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
        <>
          <PickOfDayHero sport={leagueKey} />
          <BetsView api={api} leagueKey={leagueKey} cfg={cfg} />
        </>
      )}
      {view === 'tracker'     && <TrackerView api={api} leagueKey={leagueKey} />}
      {view === 'standings'   && <StandingsView api={api} leagueKey={leagueKey} />}
      {view === 'calibration' && <CalibrationView api={api} leagueKey={leagueKey} />}
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
    confidence: conf,
    win_prob: pred.p_home != null
      ? { home: pred.p_home, away: pred.p_away ?? (1 - pred.p_home) }
      : null,
    factors: {},
    season_context: {},
    rest: {},
    injuries: {},
  }
}


function BetsView({ api, leagueKey, cfg }) {
  const [slate, setSlate] = useState(null)
  const [loading, setLoading] = useState(false)
  const [err, setErr] = useState(null)

  const refresh = () => {
    setLoading(true)
    setErr(null)
    api.get(`/football/${leagueKey}/today`)
      .then(r => setSlate(r.data))
      .catch(e => setErr(e?.message || String(e)))
      .finally(() => setLoading(false))
  }
  useEffect(refresh, [api, leagueKey])  // eslint-disable-line

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

  return (
    <div className="space-y-4">
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
                UFL spring season runs March–June. Slate populates as ESPN posts the upcoming week.
              </div>
            </div>
          ) : (
            <>
              {adaptedActive.length > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {adaptedActive.map(({ game, bet, raw }) => (
                    <FootballGameCard
                      key={game.game_id}
                      game={game}
                      bet={bet}
                      raw={raw}
                      sport={`football/${leagueKey}`}
                    />
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
                      <FootballGameCard
                        key={game.game_id}
                        game={game}
                        bet={bet}
                        raw={raw}
                        sport={`football/${leagueKey}`}
                      />
                    ))}
                  </div>
                </>
              )}
            </>
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

  if (reasons.length === 0) return null
  reasons.sort((a, b) => b.weight - a.weight)
  return <CardInsightShell>{reasons[0].text}</CardInsightShell>
}


// ── Tracker view ─────────────────────────────────────────────

function TrackerView({ api, leagueKey }) {
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

  return (
    <PickHistory
      summary={data?.summary}
      history={data?.rows || []}
      loading={loading}
      onSettle={onSettle}
      sport={leagueKey}
    />
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
          <Stat label="Home edge (points)" value={fmt(c.home_advantage, '+')} />
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
