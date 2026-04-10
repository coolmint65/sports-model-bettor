/**
 * Edge callout with STRONG / MODERATE / LEAN confidence badge.
 *
 * Used by both MLB (PredictionResults) and NHL (NHLPredictionResults).
 * Accepts an `edge` object of shape:
 *   { label, odds, edge, rating: 'strong' | 'moderate' | 'lean' }
 *
 * NHL also applied a `conf-badge conf-{rating}` className to the badge span;
 * MLB did not. `badgeClassName` is optional so both are preserved.
 */
export default function EdgeCallout({ edge, badgeClassName }) {
  if (!edge) return null

  const background = edge.rating === 'strong'
    ? 'rgba(52,211,153,0.25)'
    : edge.rating === 'moderate'
      ? 'rgba(96,165,250,0.25)'
      : 'rgba(251,191,36,0.25)'

  const color = edge.rating === 'strong'
    ? '#34d399'
    : edge.rating === 'moderate'
      ? '#60a5fa'
      : '#fbbf24'

  const label = edge.rating === 'strong'
    ? 'STRONG'
    : edge.rating === 'moderate'
      ? 'MODERATE'
      : 'LEAN'

  return (
    <div className={`edge-callout ${edge.rating}`}>
      <span
        className={badgeClassName}
        style={{
          padding: '2px 8px',
          borderRadius: 4,
          fontSize: '0.68rem',
          fontWeight: 700,
          letterSpacing: '0.05em',
          background,
          color,
          marginRight: 8,
        }}
      >
        {label}
      </span>
      <span className="edge-text">
        {edge.label} ({edge.odds > 0 ? '+' : ''}{edge.odds}) — +{edge.edge.toFixed(1)}% edge
      </span>
    </div>
  )
}
