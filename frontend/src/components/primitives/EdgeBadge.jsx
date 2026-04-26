/**
 * EdgeBadge — the model's headline pick chip.
 *
 * Phase 2d-iv restyle: Tailwind tokens, three confidence variants
 * mapped to semantic colors (positive / primary / muted). The Q1
 * accent stays warning-amber. "NO PICK" empty state uses a dashed
 * muted treatment so it reads as "we looked, nothing came back."
 *
 * Layout: [TYPE LABEL] [PICK TEXT] ────── [+EDGE%]
 * The flex push on edge gives the user a consistent right-aligned
 * number to scan across a slate.
 */

import { cn } from '../../lib/utils'

const TYPE_LABEL = {
  Q1_SPREAD: 'Q1 SPREAD',
  Q1_TOTAL:  'Q1 TOTAL',
  Q1_ML:     'Q1 WINNER',
}

const CONF_STYLE = {
  strong:   'bg-positive/10 text-positive border-positive/30',
  moderate: 'bg-primary/10 text-primary border-primary/30',
  lean:     'bg-muted/40 text-foreground/85 border-border',
}

const Q1_STYLE = 'bg-warning/10 text-warning border-warning/30'

export default function EdgeBadge({ pick, confidence = 'lean', empty, accent, typeLabel }) {
  if (empty || !pick) {
    return (
      <div
        className={cn(
          'flex items-center justify-center rounded-md border border-dashed border-border bg-muted/20',
          'px-3 py-1.5 text-[11px] font-bold uppercase tracking-widest text-muted-foreground',
        )}
        title="Model found no +EV play in the calibrated edges"
      >
        No pick
      </div>
    )
  }

  const type = typeLabel || TYPE_LABEL[pick.type] || pick.type
  const tone = accent === 'q1' ? Q1_STYLE : CONF_STYLE[confidence] || CONF_STYLE.lean

  return (
    <div
      className={cn(
        'flex items-center gap-2 rounded-md border px-3 py-1.5',
        tone,
      )}
    >
      <span className="text-[10px] font-semibold uppercase tracking-wider opacity-75">
        {type}
      </span>
      <span className="text-sm font-bold leading-none">{pick.pick}</span>
      <span className="ml-auto text-xs font-bold tabular-nums">
        +{pick.edge}%
      </span>
    </div>
  )
}
