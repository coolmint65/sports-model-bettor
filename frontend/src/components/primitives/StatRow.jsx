/**
 * StatRow
 * ──────────────────────────────────────────────────────────────
 * The "key-stat" label/value pair used inside .key-stats grids
 * across all three game-detail panes. Trivial wrapper, but having
 * the primitive lets us evolve the layout (e.g. add tooltips,
 * deltas, or conf-coloring) in one place instead of three.
 *
 * Children override the rendered value if you need richer markup
 * (multiple spans, a span + sub-label, etc.).
 */

export default function StatRow({ label, value, children, valueClassName }) {
  return (
    <div className="key-stat">
      <span className="key-label">{label}</span>
      <span className={`key-value${valueClassName ? ` ${valueClassName}` : ''}`}>
        {children ?? value}
      </span>
    </div>
  )
}
