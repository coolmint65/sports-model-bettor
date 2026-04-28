/**
 * PickOfDayHero — fetches the core POTD + summary and renders the
 * shared <PotdHero>. The visual implementation (gradient, edge
 * headline, model-vs-market bar, team logos) lives in PotdHero so
 * the derivative POTD card and the props PotdHero share the same
 * shell.
 */

import { useEffect, useState } from 'react'
import axios from 'axios'
import PotdHero from './PotdHero'

export default function PickOfDayHero({ sport, view }) {
  // For NBA the dashboard ships a Q1/Full toggle; everything else is
  // single-view. When view is supplied (NBA), we hit ?view=both once
  // and pluck the right sub-block based on the prop. That avoids a
  // re-fetch every time the user flips the toggle.
  const [data, setData] = useState(null)        // {q1: ..., full: ...} for NBA, single POTD otherwise
  const [summary, setSummary] = useState(null)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    if (loaded) return
    setLoaded(true)
    const a = axios.create({ baseURL: '/api' })
    const potdUrl = sport === 'nba'
      ? `/pick-of-day/${sport}?view=both`
      : `/pick-of-day/${sport}`
    Promise.all([
      a.get(potdUrl),
      a.get(`/pick-of-day/${sport}/summary`),
    ]).then(([p, s]) => {
      setData(p.data)
      setSummary(s.data)
    }).catch(() => {})
  }, [sport, loaded])

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
