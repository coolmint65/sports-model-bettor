/**
 * useAppFetch — top-level fetch hooks used by App.jsx to reduce the
 * copy-paste tax of "state + useEffect + api.get + catch fallback".
 *
 * Two flavors:
 *   useOnce(...)  — fetch once on mount, no polling.
 *   usePoll(...)  — fetch on mount + at a regular interval.
 *
 * Both cancel in-flight setState on unmount so component teardown
 * doesn't stamp a stale value onto a fresh mount.
 */
import { useCallback, useEffect, useState } from 'react'


/**
 * Fetch once. `extract` picks the value out of the axios response;
 * `fallback` seeds the initial state + any error response.
 */
export function useOnce(api, endpoint, extract, fallback = null) {
  const [state, setState] = useState(fallback)
  const refresh = useCallback(() => {
    let cancelled = false
    api.get(endpoint)
      .then(r => { if (!cancelled) setState(extract(r)) })
      .catch(() => { if (!cancelled) setState(fallback) })
    return () => { cancelled = true }
  }, [api, endpoint])
  useEffect(() => refresh(), [refresh])   // eslint-disable-line react-hooks/exhaustive-deps
  return [state, refresh, setState]
}


/**
 * Fetch on mount + poll. Interval clears on unmount so the timer
 * doesn't outlive its component.
 */
export function usePoll(api, endpoint, extract, intervalMs, fallback = null) {
  const [state, setState] = useState(fallback)
  useEffect(() => {
    let cancelled = false
    const fetch = () =>
      api.get(endpoint)
        .then(r => { if (!cancelled) setState(extract(r)) })
        .catch(() => {})
    fetch()
    const id = setInterval(fetch, intervalMs)
    return () => { cancelled = true; clearInterval(id) }
  }, [api, endpoint, intervalMs])
  return state
}
