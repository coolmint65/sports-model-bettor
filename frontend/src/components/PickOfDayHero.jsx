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

export default function PickOfDayHero({ sport }) {
  const [potd, setPotd] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    if (loaded) return
    setLoaded(true)
    const a = axios.create({ baseURL: '/api' })
    Promise.all([
      a.get(`/pick-of-day/${sport}`),
      a.get(`/pick-of-day/${sport}/summary`),
    ]).then(([p, s]) => {
      setPotd(p.data)
      setSummary(s.data)
    }).catch(() => {})
  }, [sport, loaded])

  if (!potd || potd.error || potd.message) return null

  return (
    <PotdHero
      label={`${sport.toUpperCase()} · Pick of the Day`}
      sport={sport}
      pick={potd}
      summary={summary}
      accent="primary"
    />
  )
}
