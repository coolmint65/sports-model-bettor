/**
 * At-a-glance badge summarizing sportsbook line movement for a game.
 * Rendered inside the detail-odds row next to the moneyline / total chips.
 *
 * Currently only NHL prediction data surfaces `line_movement`, but this
 * component lives under gameDetail/ so MLB can opt in without duplication.
 */
export default function LineMovementBadge({ lm, home, away }) {
  if (!lm) return null

  const parts = []
  if (lm.home_ml_move != null && Math.abs(lm.home_ml_move) >= 5) {
    const sign = lm.home_ml_move > 0 ? '+' : ''
    parts.push(`${home.abbreviation} ML ${sign}${lm.home_ml_move}`)
  }
  if (lm.total_move != null && Math.abs(lm.total_move) >= 0.5) {
    const sign = lm.total_move > 0 ? '+' : ''
    parts.push(`Total ${sign}${lm.total_move}`)
  }
  if (parts.length === 0) return null

  const sigColor = lm.significance === 'major'
    ? '#ef4444'
    : lm.significance === 'moderate'
      ? '#f59e0b'
      : '#94a3b8'
  const icon = lm.significance === 'major'
    ? '!! '
    : lm.significance === 'moderate'
      ? '! '
      : ''

  return (
    <span
      className="odds-chip"
      style={{
        background: 'rgba(245,158,11,0.08)',
        color: sigColor,
        border: `1px solid ${sigColor}33`,
        fontWeight: 600,
      }}
      title={`Line has moved ${lm.significance} since opening`}
    >
      {icon}LINE MOVED: {parts.join(', ')}
    </span>
  )
}
