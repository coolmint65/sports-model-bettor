/**
 * LineMovedChip — small "LINE MOVED" badge above team rows on cards.
 *
 * Phase 2-cleanup restyle: Tailwind tokens. Significance tones:
 *   minor    -> warning (amber)
 *   moderate -> warning
 *   major    -> negative (red)
 *
 * Renders nothing when significance is missing or 'none' so callers
 * can mount unconditionally.
 */

import { cn } from '../../lib/utils'

const SIG_TONE = {
  minor:    'bg-warning/10 text-warning border-warning/30',
  moderate: 'bg-warning/15 text-warning border-warning/35',
  major:    'bg-negative/15 text-negative border-negative/35',
}

export default function LineMovedChip({ lm }) {
  if (!lm || !lm.significance || lm.significance === 'none') return null
  const tone = SIG_TONE[lm.significance] || SIG_TONE.minor
  return (
    <div>
      <span
        className={cn(
          'inline-flex items-center rounded border px-1.5 py-0.5 text-[10px] font-bold uppercase tracking-wider',
          tone,
        )}
        title={`Line moved ${lm.significance} since opening`}
      >
        Line moved
      </span>
    </div>
  )
}
