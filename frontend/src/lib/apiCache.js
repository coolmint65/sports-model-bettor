/**
 * apiCache — module-level GET cache with in-flight dedup and TTL.
 *
 * Why this exists
 * ──────────────────────────────────────────────────────────
 * The tab navigation in App.jsx uses conditional rendering, so flipping
 * between Bets / Props / Derivatives / History unmounts the previous
 * view's components. Components that fetch on mount (PickOfDayHero,
 * PropsPanel, DerivativeTracker, FirstInningTracker) lose their
 * useState data when unmounted — and on remount, fire a fresh request,
 * which triggers a multi-minute cold-start on the backend's heavy
 * endpoints.
 *
 * This module decouples the fetch from component lifetime:
 *   - First call to `cachedGet(url)` runs the request.
 *   - While in-flight, a concurrent caller gets the SAME promise (dedup).
 *   - Resolved responses are cached for `ttlMs`. Subsequent callers
 *     within the TTL get the cached value synchronously (no spinner).
 *   - Past the TTL the cache is stale; next call refetches.
 *
 * The POTD card no longer "disappears" on tab switch because the cache
 * survives the unmount: when PickOfDayHero remounts and asks for its
 * URL, the response comes back from cache immediately.
 *
 * Mutating endpoints (POST/DELETE/etc.) bypass this — they invalidate
 * cache entries via `invalidate(prefix)` so a record/settle action
 * doesn't keep showing pre-action data.
 */
import axios from 'axios'

const api = axios.create({ baseURL: '/api' })

/** Default TTL: 90 seconds. Background poll on App.jsx refreshes
 *  best-bets every 2 minutes, so most reads stay cache-warm. */
const DEFAULT_TTL_MS = 90_000

const cache = new Map()        // url -> { value, expiresAt }
const inflight = new Map()     // url -> Promise<value>

/**
 * GET with cache + dedup. Returns the response payload (`r.data`).
 *
 * @param {string} url       — relative to /api (e.g. '/pick-of-day/mlb')
 * @param {object} [opts]
 * @param {number} [opts.ttlMs]   — override the 90s default
 * @param {object} [opts.params]  — axios params; included in cache key
 * @returns {Promise<any>}
 */
export function cachedGet(url, { ttlMs = DEFAULT_TTL_MS, params } = {}) {
  const key = params ? `${url}?${new URLSearchParams(params).toString()}` : url

  const hit = cache.get(key)
  if (hit && hit.expiresAt > Date.now()) {
    return Promise.resolve(hit.value)
  }
  const pending = inflight.get(key)
  if (pending) return pending

  const promise = api.get(url, params ? { params } : undefined)
    .then(r => {
      cache.set(key, { value: r.data, expiresAt: Date.now() + ttlMs })
      return r.data
    })
    .finally(() => { inflight.delete(key) })

  inflight.set(key, promise)
  return promise
}

/**
 * Drop cache entries whose key starts with `prefix`. Use after a
 * mutation so the next read sees the new server state.
 *
 * @param {string} prefix — e.g. '/tracker/' to evict every tracker URL
 */
export function invalidate(prefix) {
  for (const k of cache.keys()) {
    if (k.startsWith(prefix)) cache.delete(k)
  }
}

/**
 * Synchronous peek at the cache. Returns `undefined` when there's no
 * fresh value. Useful for components that want to render immediately
 * with cached data and revalidate in the background.
 */
export function peek(url, params) {
  const key = params ? `${url}?${new URLSearchParams(params).toString()}` : url
  const hit = cache.get(key)
  return hit && hit.expiresAt > Date.now() ? hit.value : undefined
}
