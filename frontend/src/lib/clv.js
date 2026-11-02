/**
 * Closing Line Value (CLV) — one source of truth for the beat-the-close math.
 * CLV = imp(closing) − imp(entry). Positive = we got a better price than the
 * market's close = we beat the close (the one reliable edge signal).
 * Was previously re-implemented independently in 5 components.
 */

export function impliedFromAmerican(a) {
  if (a == null || a === '') return null
  const n = Number(a)
  if (!Number.isFinite(n) || n === 0) return null
  return n > 0 ? 100 / (n + 100) : (-n) / ((-n) + 100)
}

// CLV in percentage points of implied win-prob gained vs the close (or null
// when either price is missing).
export function clvPct(entryOdds, closingOdds) {
  const ie = impliedFromAmerican(entryOdds)
  const ic = impliedFromAmerican(closingOdds)
  if (ie == null || ic == null) return null
  return (ic - ie) * 100
}

export function beatClose(entryOdds, closingOdds) {
  const c = clvPct(entryOdds, closingOdds)
  return c == null ? null : c > 0
}
