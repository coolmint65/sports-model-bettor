/**
 * PickOfDayHero — fetches the core POTD + summary and renders the
 * shared <PotdHero>. The visual implementation (gradient, edge
 * headline, model-vs-market bar, team logos) lives in PotdHero so
 * the derivative POTD card and the props PotdHero share the same
 * shell.
 *
 * Reads through `apiCache` so the response survives tab-unmount.
 * Without that, every Bets-tab visit kicked off two fresh /pick-of-day
 * requests and the card "flashed empty" each time the user came back
 * from Props or History.
 */

import { useEffect, useState } from 'react'
import { cachedGet, peek } from '../lib/apiCache'
import PotdHero from './PotdHero'

// Sport keys that have core /api/pick-of-day/{sport} routes — the
// historical surface (mlb / nhl / nba / tennis). Anything else routes
// through its sport-framework scoped path.
const _CORE_SPORTS = new Set(['mlb', 'nhl', 'nba', 'tennis'])
// Hockey framework leagues use /api/hockey/{league}/potd. Baseball
// framework leagues use /api/baseball/{league}/potd. Soccer framework
// leagues use /api/soccer/{league}/potd. Any league key not in
// CORE / HOCKEY / BASEBALL / SOCCER falls back to basketball.
const _HOCKEY_LEAGUES = new Set(['ahl', 'pwhl'])
const _BASEBALL_LEAGUES = new Set(['college'])
const _SOCCER_LEAGUES = new Set([
  'mls', 'usl_championship', 'us_nwsl', 'us_open_cup',
  'eng_premier', 'esp_laliga', 'ita_seriea', 'ger_bundesliga',
  'fra_ligue1', 'uefa_champions', 'uefa_europa', 'uefa_conference',
  'bra_seriea', 'arg_lpf', 'conmebol_libertadores',
  'fifa_internationals', 'fifa_world_cup',
])


export default function PickOfDayHero({ sport, view, tour }) {
  const isCore = _CORE_SPORTS.has(sport)
  const isHockey = _HOCKEY_LEAGUES.has(sport)
  const isBaseball = _BASEBALL_LEAGUES.has(sport)
  const isSoccer = _SOCCER_LEAGUES.has(sport)
  // Tennis-only: include the active tour in the URL so ATP and WTA
  // POTDs are scoped separately. Cache key derives from URL so the
  // ATP/WTA toggle invalidates correctly without a second hook.
  const potdUrl = sport === 'nba'
    ? `/pick-of-day/${sport}?view=both`
    : sport === 'tennis' && tour
      ? `/pick-of-day/${sport}?tour=${tour}`
      : isCore
        ? `/pick-of-day/${sport}`
        : isHockey
          ? `/hockey/${sport}/potd`
          : isBaseball
            ? `/baseball/${sport}/potd`
            : isSoccer
              ? `/soccer/${sport}/potd`
              : `/basketball/${sport}/potd`
  const summaryUrl = isCore
    ? `/pick-of-day/${sport}/summary`
    : isHockey
      ? `/hockey/${sport}/potd/summary`
      : isBaseball
        ? `/baseball/${sport}/potd/summary`
        : isSoccer
          ? `/soccer/${sport}/potd/summary`
          : `/basketball/${sport}/potd/summary`

  // Initial state hydrates synchronously from the cache when warm,
  // so a remount on tab-switch shows the POTD immediately rather than
  // flashing null. Cold mounts still resolve via the async effect.
  const [data, setData] = useState(() => peek(potdUrl) ?? null)
  const [summary, setSummary] = useState(() => peek(summaryUrl) ?? null)

  useEffect(() => {
    let cancelled = false
    Promise.all([cachedGet(potdUrl), cachedGet(summaryUrl)])
      .then(([p, s]) => {
        if (cancelled) return
        setData(p)
        setSummary(s)
      })
      .catch(() => {})
    return () => { cancelled = true }
  }, [potdUrl, summaryUrl])

  if (!data) return null

  // NBA: pluck the right view; everything else: data IS the POTD.
  // No cross-view fallback — when the user is on the Full Game tab and
  // there's no full POTD, render nothing rather than leaking the Q1
  // POTD onto a card labelled "Full Game POTD" (and vice versa).
  let potd
  if (sport === 'nba' && data && (data.q1 !== undefined || data.full !== undefined)) {
    const v = view === 'q1' ? 'q1' : 'full'
    potd = data[v]
  } else {
    potd = data
  }
  if (!potd || potd.error || potd.message) return null

  const labelSuffix = sport === 'nba' && view
    ? ` · ${view === 'q1' ? 'Q1' : 'Full Game'} POTD`
    : ' · Pick of the Day'
  // Tennis: prefix the label with the tour so the user can see at a
  // glance which side this POTD is for.
  // Framework league keys carry underscores (china_cba, korea_kbl,
  // brazil_lbf_w...). Replace with spaces so the header reads
  // "CHINA CBA" rather than "CHINA_CBA". Existing single-token sport
  // keys (nba, mlb, wnba, etc.) are unaffected.
  const sportLabel = sport.replace(/_/g, ' ').toUpperCase()
  const labelText = sport === 'tennis' && tour
    ? `${tour.toUpperCase()} TENNIS${labelSuffix}`
    : `${sportLabel}${labelSuffix}`

  return (
    <PotdHero
      label={labelText}
      sport={sport}
      pick={potd}
      summary={summary}
      accent="primary"
    />
  )
}
