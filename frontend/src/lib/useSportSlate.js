/**
 * useSportSlate — App-level MLB/NHL/NBA slate + best-bets loader.
 *
 * Every top-tier sport (MLB, NHL, NBA) fetches the same shape:
 *   - GET /{prefix}/scoreboard      → games[]
 *   - GET /{prefix}/best-bets       → bestBets[]   (heavy, minutes cold)
 *   - GET /{prefix}/best-bets/progress → live spinner label
 *
 * Refresh runs every 2 min. Best-bets is polled for progress alongside
 * the main call so the "loading recipes..." label stays live.
 *
 * MLB is the default sport → uses empty prefix (endpoints `/scoreboard`
 * and `/best-bets`). NHL / NBA pass their prefix (`/nhl`, `/nba`).
 */
import { useEffect, useState } from 'react'


export function useSportSlate(api, prefix = '') {
  const [games, setGames] = useState([])
  const [bestBets, setBestBets] = useState(null)
  const [bbProgress, setBbProgress] = useState(null)
  const [loading, setLoading] = useState(false)
  const p = prefix || ''

  useEffect(() => {
    let cancelled = false
    const fetchGames = () =>
      api.get(`${p}/scoreboard`)
        .then(r => { if (!cancelled) setGames(_normalizeGames(r.data)) })
        .catch(() => {})

    const runBestBets = () => {
      const pollHandle = setInterval(() => {
        api.get(`${p}/best-bets/progress`)
          .then(r => { if (!cancelled) setBbProgress(r.data) })
          .catch(() => {})
      }, 1500)
      return api.get(`${p}/best-bets`)
        .then(r => { if (!cancelled) setBestBets(_normalizeBets(r.data)) })
        .catch(() => {})
        .finally(() => {
          clearInterval(pollHandle)
          if (!cancelled) setBbProgress(null)
        })
    }

    setLoading(true)
    Promise.all([
      api.get(`${p}/scoreboard`)
        .then(r => { if (!cancelled) setGames(_normalizeGames(r.data)) })
        .catch(() => { if (!cancelled) setGames([]) }),
      runBestBets(),
    ]).finally(() => { if (!cancelled) setLoading(false) })

    const id = setInterval(() => {
      fetchGames()
      runBestBets()
    }, 2 * 60 * 1000)
    return () => { cancelled = true; clearInterval(id) }
  }, [api, p])

  return { games, bestBets, bbProgress, loading, setGames, setBestBets }
}


// NBA ships /best-bets as an object sometimes; the App code was
// treating a non-array as null (probably a loading placeholder). Same
// defense here so the hook mirrors the historical behavior.
function _normalizeBets(data) {
  return Array.isArray(data) ? data : null
}

// NBA scoreboard sometimes returns a non-array (error object). MLB / NHL
// always return arrays. Guard so callers can iterate without a defense.
function _normalizeGames(data) {
  return Array.isArray(data) ? data : []
}
