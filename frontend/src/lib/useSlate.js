/**
 * useSlate — module-level cache for panel-scale API fetches.
 *
 * Solves the "click a league, wait for spinner" problem: once you've
 * loaded a league's slate, the data stays warm in memory across tab
 * switches, remounts, and route changes. A background timer refreshes
 * every cached entry so the data doesn't go stale.
 *
 * Pattern is per-endpoint scoped: each `useSlate(endpoint, params,
 * apiOrOptions)` call keys off `endpoint + JSON.stringify(params)` so
 * two callers with the same request share one fetch. Same instinct as
 * a lightweight react-query, minus the dependency + the extra API
 * surface we don't need.
 *
 * Usage
 * -----
 *   const { data, loading, refresh } = useSlate(
 *     '/tennis/scheduled',
 *     { tour: 'atp', days: 2 },
 *     api,
 *   )
 *
 * The panel's `useEffect(...)` fetch collapses to this one line, and
 * remounting the panel returns the cached payload synchronously —
 * no spinner flash on tab switch.
 *
 * Cache lifecycle
 * ---------------
 *   - First mount for a key: kicks off a fetch, initial return `null`.
 *   - Second mount (same key, within TTL): returns cached data.
 *   - Background: every `REFRESH_INTERVAL_MS` re-fetches every entry
 *     that has at least one active subscriber, so panels stay fresh
 *     without each mounting a duplicate timer.
 *   - `refresh()` from the returned handle forces an immediate refetch
 *     regardless of TTL.
 */
import { useEffect, useMemo, useRef, useState } from 'react'


const REFRESH_INTERVAL_MS = 2 * 60 * 1000    // 2 min
const STALE_AFTER_MS      = 30 * 60 * 1000   // evict entries not read in 30 min

// Module-level state. Every hook instance shares these tables so a
// tennis panel and a sidebar badge asking for the same endpoint hit
// the same cache slot.
const _cache = new Map()             // key -> { data, fetchedAt, lastReadAt }
const _subscribers = new Map()       // key -> Set<setState fn>
const _inflight = new Map()          // key -> Promise (dedup)


// Background timer runs once, refreshes every subscribed key. Kicked
// off lazily by the first hook mount so unused apps don't burn cycles.
let _timerHandle = null
function _ensureTimer() {
  if (_timerHandle) return
  _timerHandle = setInterval(() => {
    const now = Date.now()
    for (const [key, subs] of _subscribers.entries()) {
      if (subs.size === 0) continue
      const entry = _cache.get(key)
      if (!entry) continue
      // Refresh entries subscribed AND older than the interval.
      if (now - entry.fetchedAt >= REFRESH_INTERVAL_MS) {
        _refetch(key)
      }
    }
    // Evict entries with no subscribers that haven't been read recently.
    for (const [key, entry] of _cache.entries()) {
      const subs = _subscribers.get(key)
      if ((!subs || subs.size === 0) && now - entry.lastReadAt > STALE_AFTER_MS) {
        _cache.delete(key)
      }
    }
  }, REFRESH_INTERVAL_MS)
}


function _notify(key, data) {
  const subs = _subscribers.get(key)
  if (!subs) return
  for (const setState of subs) setState(data)
}


function _refetch(key) {
  const meta = _fetchMeta.get(key)
  if (!meta) return null
  if (_inflight.has(key)) return _inflight.get(key)
  const { api, endpoint, params } = meta
  const p = api.get(endpoint, { params })
    .then(r => {
      const data = r.data
      _cache.set(key, {
        data, fetchedAt: Date.now(), lastReadAt: Date.now(),
      })
      _notify(key, data)
      return data
    })
    .catch(() => {
      // Preserve existing cached data on error rather than blanking.
      const existing = _cache.get(key)?.data ?? null
      return existing
    })
    .finally(() => { _inflight.delete(key) })
  _inflight.set(key, p)
  return p
}


// Per-key metadata so the background timer can rebuild the fetch
// request without the hook instance being mounted.
const _fetchMeta = new Map()


export function useSlate(endpoint, params, api) {
  // Null endpoint short-circuits — used when a panel wants to skip the
  // fetch (pending-data leagues, disabled tabs). Returning a stable
  // "no data / not loading" tuple keeps the caller's render simple.
  const disabled = !endpoint
  const key = useMemo(
    () => disabled ? null : `${endpoint}?${JSON.stringify(params || {})}`,
    [endpoint, params, disabled],
  )
  const [data, setData] = useState(() => {
    if (disabled) return null
    const entry = _cache.get(key)
    if (entry) {
      entry.lastReadAt = Date.now()
      return entry.data
    }
    return null
  })
  const [loading, setLoading] = useState(() => !disabled && !_cache.has(key))
  const setDataRef = useRef(setData)
  setDataRef.current = setData

  useEffect(() => {
    if (disabled) {
      setData(null)
      setLoading(false)
      return
    }
    _ensureTimer()
    _fetchMeta.set(key, { api, endpoint, params })
    if (!_subscribers.has(key)) _subscribers.set(key, new Set())
    _subscribers.get(key).add(setDataRef.current)

    const cached = _cache.get(key)
    if (cached) {
      cached.lastReadAt = Date.now()
      setData(cached.data)
      setLoading(false)
      // If it's older than the refresh interval, kick a background
      // refresh so the cached snapshot doesn't linger stale.
      if (Date.now() - cached.fetchedAt >= REFRESH_INTERVAL_MS) {
        _refetch(key)
      }
    } else {
      setLoading(true)
      _refetch(key).finally(() => setLoading(false))
    }

    return () => {
      const subs = _subscribers.get(key)
      if (subs) {
        subs.delete(setDataRef.current)
        if (subs.size === 0) _subscribers.delete(key)
      }
    }
  }, [key, api, endpoint, params, disabled])

  const refresh = useMemo(() => () => {
    if (disabled) return Promise.resolve(null)
    setLoading(true)
    return Promise.resolve(_refetch(key)).finally(() => setLoading(false))
  }, [key, disabled])

  return { data, loading, refresh }
}


/**
 * prefetchSlate — kick off a background fetch for a slate without
 * mounting a component. Handy for prefetching all in-season leagues at
 * App startup so the first click renders instantly.
 */
export function prefetchSlate(endpoint, params, api) {
  const key = `${endpoint}?${JSON.stringify(params || {})}`
  const cached = _cache.get(key)
  if (cached && Date.now() - cached.fetchedAt < REFRESH_INTERVAL_MS) {
    return Promise.resolve(cached.data)
  }
  _fetchMeta.set(key, { api, endpoint, params })
  _ensureTimer()
  return _refetch(key)
}
