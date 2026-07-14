/**
 * useSportData — fetch the sport-native scheduled/best-bets payload
 * for every unique sport represented in the queue picks.
 *
 * Returns a lookup map `{ "<sport>:<event_id>": nativePayload }` so
 * queue rows can render each sport's actual card component (tennis
 * MatchCard, team-sport GameCard, etc.) instead of maintaining a
 * parallel card layout in the queue.
 *
 * Each sport carries a lightweight "fetcher" here — same endpoints
 * the sport's own panel hits so any caching/prewarm they've done gets
 * shared with the queue.
 */
import { useEffect, useState } from 'react'


// Per-sport fetcher. Each returns Promise<Array<{event_id, payload}>>
// where `payload` is the shape the sport's card component expects.
const SPORT_FETCHERS = {
  tennis: async (api, tours) => {
    const out = []
    const targets = tours.length ? Array.from(new Set(tours)) : ['atp', 'wta']
    await Promise.all(targets.map(async (tour) => {
      try {
        const r = await api.get('/tennis/scheduled', { params: { tour, days: 2 } })
        for (const m of (r.data?.matches || [])) {
          if (m.match_id != null) {
            out.push({ event_id: String(m.match_id), payload: m })
          }
        }
      } catch (_) { /* swallow — falls back to slim row */ }
    }))
    return out
  },
  mlb: async (api) => {
    try {
      const r = await api.get('/best-bets')
      const rows = Array.isArray(r.data) ? r.data : []
      return rows.filter(g => g.game_id != null)
        .map(g => ({ event_id: String(g.game_id), payload: g }))
    } catch (_) { return [] }
  },
  nhl: async (api) => {
    try {
      const r = await api.get('/nhl/best-bets')
      const rows = Array.isArray(r.data) ? r.data : []
      return rows.filter(g => g.game_id != null)
        .map(g => ({ event_id: String(g.game_id), payload: g }))
    } catch (_) { return [] }
  },
  nba: async (api) => {
    try {
      const r = await api.get('/nba/best-bets')
      const rows = Array.isArray(r.data) ? r.data : []
      return rows.filter(g => g.game_id != null)
        .map(g => ({ event_id: String(g.game_id), payload: g }))
    } catch (_) { return [] }
  },
}


export function useSportData(picks, api) {
  const [lookup, setLookup] = useState({})

  useEffect(() => {
    if (!picks || !picks.length) return
    // Group event_ids by sport so we only fire one request per sport
    // regardless of how many picks it has.
    const bySport = new Map()
    for (const p of picks) {
      if (!p.event_id) continue
      if (!bySport.has(p.sport)) bySport.set(p.sport, { tours: new Set(), ids: new Set() })
      const g = bySport.get(p.sport)
      g.ids.add(String(p.event_id))
      if (p.tour) g.tours.add(p.tour)
    }
    if (!bySport.size) return
    let cancelled = false
    const jobs = []
    for (const [sport] of bySport) {
      const fetcher = SPORT_FETCHERS[sport]
      if (!fetcher) continue
      const tours = Array.from(bySport.get(sport).tours || [])
      jobs.push(fetcher(api, tours).then(rows => ({ sport, rows })))
    }
    Promise.all(jobs).then(results => {
      if (cancelled) return
      const next = {}
      for (const { sport, rows } of results) {
        for (const { event_id, payload } of rows) {
          next[`${sport}:${event_id}`] = payload
        }
      }
      setLookup(next)
    })
    return () => { cancelled = true }
  }, [picks, api])

  return lookup
}
