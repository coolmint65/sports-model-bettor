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

export default function PickOfDayHero({ sport, view }) {
  const potdUrl = sport === 'nba'
    ? `/pick-of-day/${sport}?view=both`
    : `/pick-of-day/${sport}`
  const summaryUrl = `/pick-of-day/${sport}/summary`

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
  let potd
  if (sport === 'nba' && data && (data.q1 !== undefined || data.full !== undefined)) {
    const v = view === 'q1' ? 'q1' : 'full'
    potd = data[v] || data.q1 || data.full
  } else {
    potd = data
  }
  if (!potd || potd.error || potd.message) return null

  const labelSuffix = sport === 'nba' && view
    ? ` · ${view === 'q1' ? 'Q1' : 'Full Game'} POTD`
    : ' · Pick of the Day'

  return (
    <PotdHero
      label={`${sport.toUpperCase()}${labelSuffix}`}
      sport={sport}
      pick={potd}
      summary={summary}
      accent="primary"
    />
  )
}
