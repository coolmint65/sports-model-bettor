import { useState, useEffect } from 'react'
import MarketToggle from './primitives/MarketToggle'
import PickOfDayHero from './PickOfDayHero'
import PaperPickOfDayHero from './PaperPickOfDayHero'
import PaperShadowPanel from './PaperShadowPanel'
import FirstInningPicks from './FirstInningPicks'
import DerivativePicks from './DerivativePicks'
import Scoreboard from './Scoreboard'
import NHLScoreboard from './NHLScoreboard'
import NBAScoreboard from './NBAScoreboard'
import PropsPanel from './PropsPanel'
import LiveBetsPanel from './LiveBetsPanel'
import NHLLivePanel from './NHLLivePanel'
import DerivativeTracker from './DerivativeTracker'
import FirstInningTracker from './FirstInningTracker'
import PickHistory from './PickHistory'

/**
 * BetsView — the consolidated Bets surface for a sport.
 *
 * Replaces the old top-level tab fan-out (Bets / Props / Derivatives /
 * 1st Inn) with a single Bets tab that holds a market-type toggle.
 * The user requested this shape on 2026-04-28: "instead of all the
 * picks being separate in their own tabs, we have it all in one place
 * with the toggles."
 *
 * Toggle options are sport-aware:
 *   MLB  → Full Game · 1st Inn · Derivatives · Player Props
 *   NHL  → Full Game · Derivatives · Player Props
 *   NBA  → Full Game · Q1 · Derivatives · Player Props
 *
 * The Q1/Full split that used to live inside NBABetsView is promoted
 * to the market-type toggle here — no more nested toggle inside the
 * scoreboard. NBA Q1 and Full are first-class peers next to
 * Derivatives and Props.
 *
 * Each market renders the existing component for that surface so the
 * refactor is structural, not a rewrite.
 *
 * Props:
 *   sport     — 'mlb' | 'nhl' | 'nba'
 *   data      — sport state passed down from App.jsx (games, bestBets,
 *               loading, progress, onSelectGame, etc.)
 *   api       — axios instance for DerivativeTracker
 */
// Paper tracker in the rich PickHistory style (Austin 2026-10-04 "gone entirely"):
// the MLB 1st-inn + derivative tabs show their PAPER shadow data (flat summary +
// rows) through the same Pick-Tracker shell as the full-game tracker, replacing
// the old active FirstInningTracker / DerivativeTracker.
function PaperTrackerFromShadow({ shadow, sport }) {
  if (!shadow) return null
  const s = shadow.summary || {}
  const summary = { overall: { ...s, pending: shadow.pending }, by_type: {} }
  const history = (shadow.rows || []).map(r => {
    const bt = String(r.bet_type || '').toUpperCase()
    const pick = bt.includes('TOTAL') ? `${r.pick} ${r.line ?? ''}`.trim()
      : (bt === 'SPREAD' || bt === 'RUN_LINE' || bt === 'RL' || bt === 'PL') && r.line != null
        ? `${r.pick} ${r.line > 0 ? '+' : ''}${r.line}` : r.pick
    return { ...r, type: r.bet_type, prob: r.model_prob, pick }
  })
  return <PickHistory summary={summary} history={history} unstaked sport={sport} />
}


export default function BetsView({ sport, data, api }) {
  const options = buildOptions(sport, data)
  const [market, setMarket] = useState(options[0]?.id || 'full')

  return (
    <>
      <div className="flex justify-end mb-2">
        <MarketToggle options={options} active={market} onChange={setMarket} />
      </div>
      <MarketContent sport={sport} market={market} data={data} api={api} />
    </>
  )
}


function buildOptions(sport, data) {
  const opts = []
  opts.push({ id: 'best', label: 'Best' })
  // NHL live (in-game) model, 2026-10-28: one lean per game per period (engine.sports.nhl.live_model)
  if (sport === 'nhl') opts.push({ id: 'live', label: 'Live' })
  // MLB DISABLED 2026-10-22 (Austin: World Series / off-season; restore this line to revive)
  if (sport !== 'mlb') opts.push({ id: 'props',       label: 'Player Props' })
  return opts
}


function MarketContent({ sport, market, data, api }) {
  // MLB paper-shadow (Austin 2026-09-19): per-scope paper P/L → the POTD
  // records + the per-tab shadow panels. MLB-only endpoint, isolated from real P/L.
  const [paperShadow, setPaperShadow] = useState(null)
  useEffect(() => {
    if (sport !== 'mlb' || !api) return
    let alive = true
    api.get('/tracker/paper')
      .then(r => { if (alive) setPaperShadow(r.data || null) })
      .catch(() => { if (alive) setPaperShadow(null) })
    return () => { alive = false }
  }, [sport, api])

  // NHL paper-first (Austin 2026-09-22): calibrated-shadow paper layer, same
  // presentation as WNBA/BasketballPanel. `nhlPaper` = /api/nhl/tracker/paper
  // ({full:{summary,rows,pending,potd,potd_record}}) → the full-game PAPER
  // Pick-of-the-Day hero (locked pick + its W-L record). `nhlDeriv` still feeds
  // the derivatives-tab POTD hero from the generic derivative-POTD endpoints;
  // props POTD is self-fetched inside PropsPanel.
  const [nhlPaper, setNhlPaper] = useState(null)
  const [nhlDeriv, setNhlDeriv] = useState(null)
  // P1 POTD record (Austin 2026-10-01): the hero itself recomputes its best
  // pick from `bets`/`paper_picks_p1` client-side (same pattern as every
  // other scope), so this fetch only supplies the running W-L/profit line —
  // best-effort, never blocks the hero from rendering.
  const [nhlP1, setNhlP1] = useState(null)
  useEffect(() => {
    if (sport !== 'nhl' || !api) return
    let alive = true
    api.get('/nhl/tracker/paper')
      .then(r => { if (alive) setNhlPaper(r.data || null) })
      .catch(() => { if (alive) setNhlPaper(null) })
    Promise.all([
      api.get('/nhl/derivative-pick-of-day').catch(() => ({ data: null })),
      api.get('/nhl/derivative-pick-of-day/summary').catch(() => ({ data: null })),
    ]).then(([p, s]) => {
      if (alive) setNhlDeriv({ potd: p.data || null, summary: s.data || null })
    })
    api.get('/pick-of-day/nhl/summary?view=p1')
      .then(r => { if (alive) setNhlP1(r.data || null) })
      .catch(() => { if (alive) setNhlP1(null) })
    return () => { alive = false }
  }, [sport, api])

  // NBA paper POTD (Austin 2026-10-03, game-day lock): /api/nba/tracker/paper
  // returns {full:{potd,potd_record},q1:{...}} - the locked paper POTD per view
  // feeds the hero's `locked`/`summary`, so it shows a STABLE day pick instead
  // of recomputing live.
  const [nbaPaper, setNbaPaper] = useState(undefined)  // undefined = fetch in flight; null/object = loaded
  useEffect(() => {
    if (sport !== 'nba' || !api) return
    let alive = true
    api.get('/nba/tracker/paper')
      .then(r => { if (alive) setNbaPaper(r.data || null) })
      .catch(() => { if (alive) setNbaPaper(null) })
    return () => { alive = false }
  }, [sport, api])
  if (market === 'derivatives') {
    return (
      <div className="space-y-5">
        <PaperPickOfDayHero
          games={data.games} sport={sport} scope="derivs"
          summary={sport === 'nhl' ? nhlDeriv?.summary : paperShadow?.derivative?.potd_record}
          locked={(sport === 'nhl' ? nhlDeriv?.potd : paperShadow?.derivative?.potd) ?? null}
        />
        <DerivativePicks games={data.games} />
        {sport === 'mlb'
          ? <PaperTrackerFromShadow shadow={paperShadow?.derivative} sport="mlb" />
          : <DerivativeTracker sport={sport} api={api} />}
      </div>
    )
  }
  if (market === 'live') {
    return sport === 'nhl' ? <NHLLivePanel /> : <LiveBetsPanel sport={sport} />
  }
  if (market === 'props') {
    return <PropsPanel sport={sport} />
  }
  if (market === 'firstinning' && sport === 'mlb') {
    // Card system (Austin 2026-09-19): lead with per-game 1st-inning cards for today's slate
    // (gated 1st-inn recs + the paper NRFI/YRFI lean via game.first_inning_paper), then the
    // tracker log below. Live/final games keep whatever 1st-inning pick is on the game object.
    return (
      <div className="space-y-5">
        <PaperPickOfDayHero games={data.games} sport="mlb" scope="1stinn" summary={paperShadow?.first_inning?.potd_record} locked={(paperShadow?.first_inning?.potd) ?? null} />
        <FirstInningPicks bestBets={data.bestBets} games={data.games} />
        <PaperTrackerFromShadow shadow={paperShadow?.first_inning} sport="mlb" />
      </div>
    )
  }

  // 'full' or NBA 'q1' — main slate scoreboard.
  if (sport === 'mlb') {
    return (
      <>
        <PickOfDayHero sport="mlb" />
        <Scoreboard
          games={data.games}
          loading={data.loading}
          progress={data.progress}
          bestBets={data.bestBets}
        />
      </>
    )
  }
  if (sport === 'nhl') {
    // NHL paper-first (Austin 2026-09-22), mirroring WNBA/BasketballPanel:
    //   • one bet per card — the gated real pick if it cleared the floor,
    //     else the best calibrated PAPER lean (paper_pick = max-edge entry of
    //     b.paper_picks). GameCard renders the EdgeBadge or PaperBadge itself,
    //     so EVERY card shows a play instead of "NO PICK".
    //   • the full-game view leads with the PAPER Pick-of-the-Day hero (the
    //     locked calibrated-shadow lean + its W-L record). Real picks + the
    //     real POTD live on the Tracker tab.
    // The P1 tab keeps its own P1 (derivative) POTD hero.
    const merged = market === 'best'  // 'Best' = cross-market headline (market-merge 2026-10-11)
    const view = market === 'p1' ? 'p1' : 'full'  // scoreboard odds grid; 'best' uses full
    const key = view === 'p1' ? 'best_pick_p1' : 'best_pick_full'
    const maxEdge = (arr) => (arr.length ? arr.reduce((a, c) => ((c.edge ?? -99) > (a.edge ?? -99) ? c : a)) : null)
    const bets = Array.isArray(data.bestBets)
      ? data.bestBets.map(b => {
          const chosen = merged
            ? (b.best_pick || null)
            : (b[key] || (key === 'best_pick_full' ? b.best_pick : null))
          // Full-game paper leans (ML/O/U/PL) show on Full Game; P1 gets its
          // OWN period-1 lean (paper_picks_p1) so the two markets never mix
          // (Austin 2026-09-29).
          const pp = merged
            ? [...(b.paper_picks || []), ...(b.paper_picks_p1 || [])]
            : view === 'p1'
              ? (b.paper_picks_p1 || [])
              : (b.paper_picks || [])
          const _pp = maxEdge(pp)
          // 'Best' view: the headline IS the held/recorded authority pick -> show it (graduated), not the
          // max-edge live candidate (Austin 2026-11-01: card pill disagreed with the tracker).
          const paper_pick = (merged && chosen) ? { ...chosen, graduated: true }
            : _pp
            ? { ..._pp, graduated: !!(chosen && chosen.type === _pp.type && chosen.pick === _pp.pick) }
            : (view !== 'p1' ? (b.card_lean || null) : null)   // lean fallback (Austin 2026-10-26)
          return { ...b, best_pick: chosen, paper_pick,
                   confidence: chosen?.confidence || 'lean' }
        })
      : data.bestBets
    return (
      <>
        {view === 'p1'
          ? <PaperPickOfDayHero
              games={bets} sport="nhl" scope="p1"
              summary={nhlP1}
            />
          : <PickOfDayHero sport="nhl" />}
        <NHLScoreboard
          games={data.games}
          loading={data.loading}
          progress={data.progress}
          bestBets={bets}
          view={view}
        />
      </>
    )
  }
  if (sport === 'nba') {
    // For NBA, the active market is either 'full' or 'q1'. Re-key each
    // bet's best_pick to the right view (no cross-view fallback —
    // same rule as the old NBABetsView introduced 2026-04-28).
    const merged = market === 'best'  // 'Best' = cross-market headline (market-merge 2026-10-11)
    const view = market === 'q1' ? 'q1' : 'full'  // scoreboard odds grid; 'best' uses full
    const key = view === 'q1' ? 'best_pick_q1' : 'best_pick_full'
    const maxEdge = (arr) => (arr.length ? arr.reduce((a, c) => ((c.edge ?? -99) > (a.edge ?? -99) ? c : a)) : null)
    const bets = Array.isArray(data.bestBets)
      ? data.bestBets.map(b => {
          const chosen = merged
            ? (b.best_pick || null)
            : (b[key] || null)
          // Paper lean per view (Austin 2026-10-03, mirroring NHL/WNBA): the
          // max-edge entry of this view's paper_picks, so GameCard shows a
          // PaperBadge on every card instead of "NO PICK" when the real gate
          // suppressed the pick.
          const pp = merged
            ? [...(b.paper_picks || []), ...(b.paper_picks_q1 || [])]
            : (view === 'q1' ? (b.paper_picks_q1 || []) : (b.paper_picks || []))
          const _pp = maxEdge(pp)
          // 'Best' view: the headline IS the held/recorded authority pick -> show it (graduated), not the
          // max-edge live candidate (Austin 2026-11-01: card pill disagreed with the tracker).
          const paper_pick = (merged && chosen) ? { ...chosen, graduated: true }
            : _pp
            ? { ..._pp, graduated: !!(chosen && chosen.type === _pp.type && chosen.pick === _pp.pick) }
            : (view !== 'q1' ? (b.card_lean || null) : null)   // lean fallback (Austin 2026-10-26)
          return { ...b, best_pick: chosen, paper_pick, confidence: chosen?.confidence || 'lean' }
        })
      : data.bestBets
    return (
      <>
        {view === 'q1'
          ? <PaperPickOfDayHero games={bets} sport="nba" scope="q1" summary={nbaPaper?.q1?.potd_record} locked={(nbaPaper?.q1?.potd) ?? null} loading={nbaPaper === undefined} />
          : <PickOfDayHero sport="nba" />}
        <NBAScoreboard
          games={data.games}
          loading={data.loading}
          progress={data.progress}
          bestBets={bets}
          view={view}
        />
      </>
    )
  }
  return null
}
