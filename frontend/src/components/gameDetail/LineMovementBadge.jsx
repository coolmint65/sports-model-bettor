import { cn } from '../../lib/utils'

/**
 * Line-movement badge in the detail-odds row.
 *
 * Phase 2-cleanup restyle: Tailwind tokens; significance maps to
 * negative/warning/muted tones instead of inline hex colors.
 *
 * Currently NHL-only data shape but lives under gameDetail/ so MLB
 * can opt in once line_movement is plumbed.
 */
const SIG_TONE = {
  major:    'border-negative/40 bg-negative/10 text-negative',
  moderate: 'border-warning/40 bg-warning/10 text-warning',
  minor:    'border-border bg-muted text-muted-foreground',
}

const SIG_PREFIX = {
  major:    '!! ',
  moderate: '! ',
  minor:    '',
}

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

  const tone = SIG_TONE[lm.significance] || SIG_TONE.minor
  const prefix = SIG_PREFIX[lm.significance] || ''

  return (
    <span
      className={cn(
        'inline-flex items-center rounded-md border px-2 py-0.5 text-[11px] font-semibold',
        tone,
      )}
      title={`Line has moved ${lm.significance} since opening`}
    >
      {prefix}LINE MOVED: {parts.join(', ')}
    </span>
  )
}
