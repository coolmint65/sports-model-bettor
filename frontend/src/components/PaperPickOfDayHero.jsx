import { memo } from 'react'
import PotdHero from './PotdHero'

// PAPER Pick of the Day (Austin 2026-09-19) — the single highest-edge PAPER lean across a sport's
// slate, rendered through the SAME shared <PotdHero> shell as the real POTD (gradient, edge
// headline, model-vs-market bar, logos), with a sky 'paper' accent. Computes from the games prop
// (game.paper_picks / paper_picks_q1 for the framework, or game.paper_pick for MLB) — no backend
// endpoint needed, so it always tracks the live slate.
function pickText(p) {
  const t = p.type || ''
  const isQ1 = t.startsWith('Q1_')
  const base = isQ1 ? t.slice(3) : t
  const s = (base === 'ML' || base === '1ST_INN')
    ? String(p.pick)                                    // ML side; NRFI/YRFI is an Under-0.5 total, not a spread
    : base.includes('TOTAL')
      ? p.pick + ' ' + p.line                           // Over/Under X.5 (match / F5 / team totals) - unsigned
      : p.pick + ' ' + (p.line > 0 ? '+' : '') + p.line // spread / run line - signed
  return isQ1 ? s + ' Q1' : s
}

// Model overconfidence: the biggest "edges" are the model's WORST picks
// (proven across tennis/soccer; in CFB it's the Elo blind to the FBS-vs-FCS
// gap — a "47% edge" on an FCS dog). Feature the best REALISTIC edge instead
// of blindly grabbing the max. Recording stays RAW so the shadow can measure
// the true edge buckets and we tune this from data. Austin 2026-09-19.
const POTD_EDGE_CEILING = { nfl: 20, cfb: 20, ufl: 20 } // aligned to the cards' 20pp football ceiling so POTD == card (Austin 2026-10-01)

// Football (NFL/CFB/UFL) plays WEEKLY, not nightly, unlike every other sport
// this hero serves. The live recompute below ("POTD = the current best CARD
// lean") assumes "today's slate" == "tonight's games" — true for NHL/MLB/
// NBA/WNBA/soccer (a game most days) — but a football pick is locked days
// before its game is played (see backend routes_football.py
// get_locked_potd), so the per-game `date`/`time` is almost never "today"
// and the recompute's today-only eligibility gate below finds zero
// candidates on nearly every day of the week. (It even misses on an actual
// game day: a bare "YYYY-MM-DD" `date` with no time-of-day parses as UTC
// midnight, which America/New_York then rolls back to the PREVIOUS calendar
// day, so it still can't match `_todayET` either.) Net effect: the football
// hero rendered nothing at all despite a confirmed, correctly-locked backend
// POTD (Austin/Claude 2026-10-02 root-cause — the original theory was a
// missing paper_picks field; the field is present and populated, the
// today-only date gate was silently discarding every candidate).
//
// Football already freezes exactly one pick/day server-side for this exact
// reason, and FootballPanel already plumbs it through as `locked` (it was
// previously ignored here via `void locked`) — so for these leagues, render
// it directly instead of recomputing from `games`.
const WEEKLY_SPORTS = new Set(Object.keys(POTD_EDGE_CEILING))

function PaperPickOfDayHeroImpl({ games, sport, scope, summary, locked, loading }) {
  const sportKey = String(sport || '').toLowerCase()

  // GAME-DAY LOCK (Austin 2026-10-03, paper-only POTD): prefer the backend's
  // game-day-LOCKED paper POTD for EVERY sport. The daily-sport hero used to
  // recompute the best card lean live, so the pick shuffled through the game day;
  // the paper POTD (record_paper_potd / record_nba_paper_potd) is idempotent per
  // date, so it freezes once the day arrives. Reading `locked` makes the hero
  // stable on game day -- like the weekly football hero already did. Falls
  // through to the live recompute only when no locked POTD exists yet (early in
  // the day, or a scope/sport without a paper-POTD record, e.g. NHL p1).
  const _kind = scope === 'q1' ? 'Q1 PAPER Pick of the Day'
    : scope === '1stinn' ? '1st-Inn PAPER Pick of the Day'
    : scope === 'derivs' ? 'Derivative PAPER Pick of the Day'
    : scope === 'p1' ? 'P1 PAPER Pick of the Day'
    : 'PAPER Pick of the Day'
  const _label = String(sport || '').replace(/_/g, ' ').toUpperCase() + ' · ' + _kind
  if (locked) {
    const lg = (games || []).find(g => g.game_id === locked.game_id) || {}
    const _lt = locked.bet_type || locked.type || ''
    const _lq1 = String(_lt).startsWith('Q1_')
    const pick = {
      pick: locked.pick,
      odds: locked.odds,
      bet_type: _lq1 ? 'Q1 ' + _lt.slice(3) : _lt,
      model_prob: locked.model_prob ?? locked.prob,
      matchup: locked.matchup,
      away_logo: lg.away_logo || lg.away?.logo || null,
      home_logo: lg.home_logo || lg.home?.logo || null,
      edge: locked.edge,
      confidence: 'paper',
    }
    return <PotdHero label={_label} sport={sport} pick={pick} accent="sky" summary={summary} />
  }
  // Lock-authoritative heroes: a caller that wires a paper/real POTD source passes
  // `locked` as null (not undefined) while that fetch is in flight, or when the
  // backend locked nothing eligible. Such a hero must NOT fall through to the raw
  // client-side max-edge recompute below -- that (a) FLICKERS (shows a different
  // pick for the ~1s until `locked` resolves, then swaps to it), and (b) headlines
  // an UNCALIBRATED max-edge pick the backend calibration gate would reject. Show
  // the backend pick (above) or nothing. Only a scope/sport with NO lock source
  // (NHL p1, soccer) passes `locked === undefined` and keeps the recompute below.
  // (Austin 2026-10-13: the "starts ANA -1.5, switches to EDM -1.5" flicker.)
  if (locked === null) {
    // Loaded-empty (backend locked nothing eligible) -> NO PICK; still in-flight -> null.
    if (loading === false) return <PotdHero label={_label} sport={sport} pick={null} noPick accent="sky" summary={summary} />
    return null
  }
  // Weekly football can't recompute from a 7-day board (today-only gate finds
  // nothing) -- without a lock there's nothing to show.
  if (WEEKLY_SPORTS.has(sportKey)) return null

  const edgeCeil = POTD_EDGE_CEILING[sportKey] ?? Infinity
  // POTD = the current best CARD lean (Austin 2026-09-29): compute from the
  // SAME games/paper_picks the cards render so the hero always matches its
  // game's card. Previously it displayed the server-`locked` pick, which only
  // refreshed its PRICE and never re-picked — so when a different market became
  // the best (e.g. spread -> ML) the hero went stale vs the card. The backend
  // live-freeze already freezes the games' paper picks at game start, so the
  // hero inherits that freeze for live/final games. `summary` still carries
  // the historical POTD record. (Weekly-cadence football branches out above
  // and uses `locked` directly instead — see note above.)
  let best = null
  let bg = null
  // Only TODAY's slate is eligible. The games board carries a 7-day
  // lookahead, so without this the hero surfaces the max-edge pick from a
  // FUTURE day (Austin 2026-09-30: NHL POTD hero showed tomorrow's game).
  // Mirrors the backend POTD, which locks to today ET. Undated rows
  // (HR-fallback -> '') are treated as today so they're never dropped.
  const _todayET = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' })
  const _etDate = (iso) => {
    if (!iso) return ''
    if (/^\d{4}-\d{2}-\d{2}$/.test(String(iso))) return String(iso)  // bare YYYY-MM-DD is already the backend ET game-date; Date() would parse it as UTC midnight and roll back a day in ET (hid the WNBA POTD). Austin/Claude 2026-10-02
    const d = new Date(iso)
    return isNaN(d.getTime()) ? '' : d.toLocaleDateString('en-CA', { timeZone: 'America/New_York' })
  }
  for (const g of games || []) {
    const _gd = _etDate(g.date || g.time)
    if (_gd && _gd !== _todayET) continue
    // scope: 'full' = full-game only, 'q1' = Q1 only, else both (Austin 2026-09-19 split)
    let cands
    if (scope === 'q1') {
      cands = [...(g.paper_picks_q1 || [])]
    } else if (scope === '1stinn') {
      cands = g.first_inning_paper ? [g.first_inning_paper] : []
    } else if (scope === 'derivs') {
      cands = g.derivative_paper ? [g.derivative_paper] : []
    } else if (scope === 'p1') {
      cands = [...(g.paper_picks_p1 || [])]
    } else if (scope === 'full') {
      cands = [...(g.paper_picks || [])]
      if (g.paper_pick) cands.push(g.paper_pick)
    } else {
      cands = [...(g.paper_picks || []), ...(g.paper_picks_q1 || [])]
      if (g.paper_pick) cands.push(g.paper_pick)
    }
    for (const p of cands) {
      if (p && (p.edge ?? -99) <= edgeCeil && (best == null || (p.edge ?? -99) > (best.edge ?? -99))) { best = p; bg = g }
    }
  }
  if (!best) return <PotdHero label={_label} sport={sport} pick={null} noPick accent="sky" summary={summary} />
  const isQ1 = String(best.type || '').startsWith('Q1_')
  const g = bg || {}
  const awayAbbr = g.away_abbr || g.away?.abbr || g.away?.abbreviation || (typeof g.away === 'string' ? g.away : '') || best.away_abbr || ''
  const homeAbbr = g.home_abbr || g.home?.abbr || g.home?.abbreviation || (typeof g.home === 'string' ? g.home : '') || best.home_abbr || ''
  const pick = {
    pick: best.display || pickText(best),
    odds: best.odds,
    bet_type: isQ1 ? 'Q1 ' + best.type.slice(3) : best.type,
    model_prob: best.prob,
    matchup: best.matchup || ((awayAbbr && homeAbbr) ? (awayAbbr + ' @ ' + homeAbbr) : ''),
    away_logo: g.away_logo || g.away?.logo || g.away?.logo_url || best.away_logo || null,
    home_logo: g.home_logo || g.home?.logo || g.home?.logo_url || best.home_logo || null,
    edge: best.edge,
    confidence: 'paper',
  }
  const kind = scope === 'q1' ? 'Q1 PAPER Pick of the Day'
    : scope === '1stinn' ? '1st-Inn PAPER Pick of the Day'
    : scope === 'derivs' ? 'Derivative PAPER Pick of the Day'
    : scope === 'p1' ? 'P1 PAPER Pick of the Day'
    : 'PAPER Pick of the Day'
  const label = String(sport || '').replace(/_/g, ' ').toUpperCase() + ' · ' + kind
  return <PotdHero label={label} sport={sport} pick={pick} accent="sky" summary={summary} />
}
const PaperPickOfDayHero = memo(PaperPickOfDayHeroImpl)
export default PaperPickOfDayHero
