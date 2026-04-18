/**
 * LineMovedChip
 * ──────────────────────────────────────────────────────────────
 * Amber "LINE MOVED" badge that sat above the team rows on MLB
 * and NHL cards. Renders nothing when there's no significant line
 * movement so callers can mount it unconditionally.
 *
 * `lm.significance` values from the backend:
 *   'none'   -> nothing to show (we skip render)
 *   'minor'  -> amber
 *   'major'  -> red
 */

export default function LineMovedChip({ lm }) {
  if (!lm || !lm.significance || lm.significance === 'none') return null
  return (
    <div className="line-moved-chip-wrap">
      <span
        className={`line-moved-chip line-moved-${lm.significance}`}
        title={`Line moved ${lm.significance} since opening`}
      >
        LINE MOVED
      </span>
    </div>
  )
}
