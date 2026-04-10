/**
 * Rest / back-to-back context pills shown under the matchup header.
 *
 * Only renders anything when the prediction exposes a `rest` object with
 * at least one meaningful flag. Currently NHL-only, but kept sport-agnostic
 * so MLB can plug in when the engine starts exposing the same shape.
 */
export default function RestBadges({ rest, home, away }) {
  if (!rest) return null
  const {
    home_b2b,
    away_b2b,
    home_rest_advantage,
    away_rest_advantage,
  } = rest

  if (!home_b2b && !away_b2b && !home_rest_advantage && !away_rest_advantage) {
    return null
  }

  const b2bStyle = {
    padding: '2px 10px',
    borderRadius: 6,
    fontSize: '0.72rem',
    fontWeight: 600,
    background: 'rgba(239,68,68,0.12)',
    color: '#ef4444',
    border: '1px solid rgba(239,68,68,0.25)',
  }
  const restStyle = {
    padding: '2px 10px',
    borderRadius: 6,
    fontSize: '0.72rem',
    fontWeight: 600,
    background: 'rgba(96,165,250,0.10)',
    color: '#60a5fa',
    border: '1px solid rgba(96,165,250,0.20)',
  }

  return (
    <div
      style={{
        textAlign: 'center',
        marginTop: 6,
        display: 'flex',
        justifyContent: 'center',
        gap: 8,
        flexWrap: 'wrap',
      }}
    >
      {home_b2b && (
        <span style={b2bStyle}>{home.abbreviation} on back-to-back</span>
      )}
      {away_b2b && (
        <span style={b2bStyle}>{away.abbreviation} on back-to-back</span>
      )}
      {home_rest_advantage && !away_rest_advantage && (
        <span style={restStyle}>{home.abbreviation} extra rest</span>
      )}
      {away_rest_advantage && !home_rest_advantage && (
        <span style={restStyle}>{away.abbreviation} extra rest</span>
      )}
    </div>
  )
}
