/**
 * HockeyPanel — landing for framework-managed hockey leagues (AHL, PWHL).
 *
 * Mirrors BasketballPanel exactly so AHL/PWHL match NHL's layout and
 * primitives — same SubNav, same GameCard, same TrackerView, same
 * StandingsView shape. NHL itself stays on its dedicated isNHL path
 * in App.jsx (heavy single-flight pipeline + intermission predictor);
 * this panel renders only when the user picks AHL/PWHL.
 */
import { useEffect, useMemo, useState } from 'react'
import { useSlate } from '../lib/useSlate'
import { CalibrationView, StandingsView } from './hockey/subviews'
import GameCard from './primitives/GameCard'
import { OddsGrid, OddsRow, CardInsight as CardInsightShell } from './primitives/OddsGrid'
import PanelShell from './primitives/PanelShell'
import PickHistory from './PickHistory'
import PickOfDayHero from './PickOfDayHero'

const HK_TABS = [
  { id: 'bets',        label: 'Bets' },
  { id: 'tracker',     label: 'Tracker' },
  { id: 'calibration', label: 'Calibration' },
  { id: 'standings',   label: 'Standings' },
]

export default function HockeyPanel({ api, leagueKey, leagues }) {
  const cfg = useMemo(
    () => (leagues || []).find(L => L.key === leagueKey),
    [leagues, leagueKey],
  )
  const [view, setView] = useState('bets')

  if (!leagueKey || !cfg) {
    return (
      <section className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-12 text-center">
        <div className="text-sm font-semibold text-foreground">
          Pick a hockey league from the sidebar.
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
      tabs={HK_TABS}
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
      {view === 'calibration' && <CalibrationView api={api} leagueKey={leagueKey} />}
      {view === 'standings'   && <StandingsView api={api} leagueKey={leagueKey} />}
    </PanelShell>
  )
}


// ── Bets view ─────────────────────────────────────────────

// Same edge-cutoffs the backend uses (engine/_pick_helpers.tag_confidence).
function confidenceFor(edgePct) {
  if (edgePct == null) return 'skip'
  if (edgePct >= 12) return 'strong'
  if (edgePct >= 7) return 'moderate'
  if (edgePct >= 4) return 'lean'
  return 'skip'
}


function adaptGameShape(g) {
  // routes_hockey/today returns theScore-shaped rows (status:
  // pre_game/in_progress/final). Map to the GameCard shape NHL/basketball use.
  const stateMap = { final: 'post', in_progress: 'in', pre_game: 'pre' }
  const date = g.start_time || null
  // Mirror NHL's series shape so the GameCard playoff-context strip
  // renders for AHL/PWHL postseason games. theScore uses
  // 'Postseason Tournament' (or 'Playoffs' on some leagues) for
  // postseason; either pattern triggers the playoff badge. game_number
  // / series_leader stay placeholder until theScore ships round metadata.
  const gt = (g.game_type || '').toLowerCase()
  const inSeries = gt.includes('playoff') || gt.includes('postseason')
  const series = inSeries ? {
    in_series: true,
    game_number: 1,
    is_tied: false,
    home_wins: 0, away_wins: 0,
    series_leader: null,
    is_elimination: false,
  } : null
  return {
    game_id: g.id,
    id: g.id,
    date,
    home: {
      abbreviation: g.home_short || g.home_name,
      name: g.home_name,
      score: g.home_score,
      logo: g.home_logo,
    },
    away: {
      abbreviation: g.away_short || g.away_name,
      name: g.away_name,
      score: g.away_score,
      logo: g.away_logo,
    },
    status: {
      state: stateMap[g.status] || 'pre',
      detail: g.status === 'final' ? 'Final' : '',
    },
    series,
    // Surface the raw odds + goalie + venue so HockeyGameCard's NHL-
    // mirror render slots can pick them up without re-shape gymnastics.
    odds: g.odds || null,
    home_goalie: g.home_goalie || null,
    away_goalie: g.away_goalie || null,
    broadcast: g.broadcast || null,
    venue: g.venue || null,
    line_movement: g.line_movement || null,
  }
}


function adaptBetShape(g) {
  const best = g.best_pick
  const conf = best ? (best.confidence || confidenceFor(best.edge)) : 'skip'
  return {
    game_id: g.id,
    best_pick: best,
    confidence: conf,
    win_prob: g.prediction?.ml_home != null
      ? { home: g.prediction.ml_home, away: 1 - g.prediction.ml_home }
      : null,
    // NHL-shaped supporting fields. AHL/PWHL ingest doesn't yet
    // produce factors / season_context / rest / injuries — pass
    // empty objects so NHLCardInsight returns null gracefully and
    // RestChips renders nothing instead of crashing.
    factors: {},
    season_context: {},
    rest: {},
    injuries: {},
  }
}


function BetsView({ api, leagueKey, cfg }) {
  const [err, setErr] = useState(null)
  const slateParams = useMemo(() => ({}), [])
  const { data: slate, loading, refresh: refreshCache } =
    useSlate(`/hockey/${leagueKey}/today`, slateParams, api)
  const refresh = () => { setErr(null); refreshCache() }

  const games = slate?.games || []
  const totalPicks = games.reduce((n, g) => n + (g.picks?.length || 0), 0)
  // Partition: active games (pre/in-progress) on top, finals at the
  // bottom — same UX pattern as NHL/NBA/MLB ScoreboardShell. Inside
  // each bucket, sort by start_time ascending.
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
            </div>
          ) : (
            <>
              {adaptedActive.length > 0 && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                  {adaptedActive.map(({ game, bet, raw }) => (
                    <HockeyGameCard
                      key={game.game_id}
                      game={game}
                      bet={bet}
                      raw={raw}
                      sport={`hockey/${leagueKey}`}
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
                      <HockeyGameCard
                        key={game.game_id}
                        game={game}
                        bet={bet}
                        raw={raw}
                        sport={`hockey/${leagueKey}`}
                      />
                    ))}
                  </div>
                </>
              )}
            </>
          )}
          <div className="text-[10px] text-muted-foreground italic px-1">
            Beta — paper-bet only until ROI accumulates. Predictions
            from theScore-backed Poisson model with calibrated
            home-boost from {cfg.display_name} backfill.
          </div>
        </>
      )}
    </div>
  )
}


// ── HockeyGameCard ───────────────────────────────────────
//
// Mirror of NHLGameCard line-for-line (NHLScoreboard.jsx:NHLGameCard)
// so AHL + PWHL render with the same primitives, the same odds grid,
// the same insight slot, and the same goalie starters slot. Anything
// AHL/PWHL doesn't yet have (rest data, line moves, full injuries)
// degrades to null — the parent GameCard renders nothing for those
// slots so the layout stays clean.

function HockeyGameCard({ game, bet, raw, sport }) {
  const { home, away } = game

  const starters = (game.away_goalie || game.home_goalie) ? (
    <div className="flex items-center justify-center gap-2 text-xs">
      <GoalieSlot g={game.away_goalie} />
      <span className="text-muted-foreground">vs</span>
      <GoalieSlot g={game.home_goalie} />
    </div>
  ) : null

  const odds = game.odds ? <NHLStyleOddsGrid odds={game.odds} home={home} away={away} /> : null
  const insight = <HockeyCardInsight bet={bet} home={home} away={away} game={game} raw={raw} />

  return (
    <GameCard
      game={game}
      bet={bet}
      insight={insight}
      starters={starters}
      odds={odds}
      restTiredLabel="B2B"
      sport={sport}
    />
  )
}


function GoalieSlot({ g }) {
  if (!g?.name) return <span className="font-medium text-foreground/70">TBD</span>
  return (
    <span className="inline-flex items-center gap-1 font-medium text-foreground/85">
      {g.name}
      {g.status === 'confirmed' && (
        <span className="text-positive" title="Confirmed starter">✓</span>
      )}
      {g.status === 'expected' && (
        <span className="text-warning" title="Projected starter">~</span>
      )}
      {g.save_pct > 0 && (
        <span
          className="ml-1 text-[10px] tabular-nums"
          style={{ color: svPctColor(g.save_pct) }}
          title={`League SV% varies; this starter ${svPctLabel(g.save_pct)}.`}
        >
          {g.save_pct.toFixed(3)} SV%
        </span>
      )}
    </span>
  )
}


// AHL/PWHL goalie SV% uses the same league-baseline color story as
// NHL — minor-league goalies ride similar averages (~0.900-0.910).
function svPctColor(sv) {
  if (sv >= 0.915) return '#34d399'
  if (sv >= 0.905) return '#94a3b8'
  if (sv >= 0.895) return '#fbbf24'
  return '#ef4444'
}
function svPctLabel(sv) {
  if (sv >= 0.920) return 'is elite (top ~10%)'
  if (sv >= 0.910) return 'is above average'
  if (sv >= 0.900) return 'is league average'
  if (sv >= 0.890) return 'is below average'
  return 'has struggled this season'
}


function NHLStyleOddsGrid({ odds, home, away }) {
  // Identical layout to NHLOddsGrid (NHLScoreboard.jsx:NHLOddsGrid) —
  // ML row, O/U row, PL row. PL falls back to a synthetic +/-1.5
  // when HR doesn't post the spread market explicitly.
  const hasReal = odds.away_spread_point != null || odds.home_spread_point != null
  const homeFav = odds.home_ml && odds.away_ml && odds.home_ml < odds.away_ml
  const dAwayPt = hasReal ? odds.away_spread_point : (homeFav ? 1.5 : -1.5)
  const dHomePt = hasReal ? odds.home_spread_point : (homeFav ? -1.5 : 1.5)
  const dAwayOdds = odds.away_spread_odds || (dAwayPt > 0 ? -180 : 150)
  const dHomeOdds = odds.home_spread_odds || (dHomePt > 0 ? -180 : 150)

  return (
    <OddsGrid>
      {(odds.home_ml || odds.away_ml) && (
        <OddsRow label="ML"
          away={`${away.abbreviation} ${odds.away_ml > 0 ? '+' : ''}${odds.away_ml || '-'}`}
          home={`${home.abbreviation} ${odds.home_ml > 0 ? '+' : ''}${odds.home_ml || '-'}`}
        />
      )}
      {odds.over_under && (
        <OddsRow label="O/U"
          away={`o${odds.over_under}${odds.over_odds ? ` (${Math.round(odds.over_odds) > 0 ? '+' : ''}${Math.round(odds.over_odds)})` : ''}`}
          home={`u${odds.over_under}${odds.under_odds ? ` (${Math.round(odds.under_odds) > 0 ? '+' : ''}${Math.round(odds.under_odds)})` : ''}`}
        />
      )}
      <OddsRow label="PL"
        away={`${away.abbreviation} ${dAwayPt > 0 ? '+' : ''}${dAwayPt} (${dAwayOdds > 0 ? '+' : ''}${Math.round(dAwayOdds)})`}
        home={`${home.abbreviation} ${dHomePt > 0 ? '+' : ''}${dHomePt} (${dHomeOdds > 0 ? '+' : ''}${Math.round(dHomeOdds)})`}
      />
    </OddsGrid>
  )
}


function HockeyCardInsight({ bet, home, away, game, raw }) {
  // Mirrors NHLCardInsight's "strongest reason" pattern. AHL/PWHL
  // ingest doesn't yet expose the same factors/season_context/rest/
  // injuries surface NHL has, so we lean on what we DO have:
  //
  //   1. Goalie SV% gap (when starters known)
  //   2. Predicted xG gap (from the Poisson model)
  //   3. Win-prob lean (>=20pp) as the fallback signal
  //
  // Returns null when nothing meaningful — GameCard renders no
  // insight strip, which matches NHL's behaviour for low-info games.
  const reasons = []

  if (game.home_goalie?.save_pct > 0 && game.away_goalie?.save_pct > 0) {
    const diff = Math.abs(game.home_goalie.save_pct - game.away_goalie.save_pct)
    if (diff >= 0.010) {
      const better = game.home_goalie.save_pct > game.away_goalie.save_pct
        ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: diff * 100,
        text: <><strong>{better}</strong> has goalie edge ({Math.max(game.home_goalie.save_pct, game.away_goalie.save_pct).toFixed(3)})</>,
      })
    }
  }

  const pred = raw?.prediction
  if (pred?.home_expected != null && pred?.away_expected != null) {
    const diff = Math.abs(pred.home_expected - pred.away_expected)
    if (diff >= 0.6) {
      const better = pred.home_expected > pred.away_expected ? home.abbreviation : away.abbreviation
      reasons.push({
        weight: diff * 5,
        text: <><strong>{better}</strong> projected +{diff.toFixed(1)} goal edge</>,
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


// ── Tracker view ─────────────────────────────────────────

function TrackerView({ api, leagueKey }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  const refresh = () => {
    setLoading(true)
    api.get(`/hockey/${leagueKey}/tracker/history`)
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }
  useEffect(refresh, [api, leagueKey])  // eslint-disable-line

  const onRecord = () => {
    api.post(`/hockey/${leagueKey}/tracker/record`).finally(refresh)
  }
  const onSettle = () => {
    api.post(`/hockey/${leagueKey}/tracker/settle`).finally(refresh)
  }

  return (
    <PickHistory
      summary={data?.summary}
      history={data?.history || []}
      loading={loading}
      onRecord={onRecord}
      onSettle={onSettle}
      sport={leagueKey}
    />
  )
}


// ── Calibration view ──────────────────────────────────────

