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
import { humanizeBetType } from '../../lib/betType'

// EdgeBadge-specific overrides. Q1_ML reads as "Q1 WINNER" here (more
// natural in basketball context) where the central humanizer ships
// "Q1 ML". Anything not in this map falls through to humanizeBetType,
// which already covers H1_*, DNB, BTTS, etc. across every sport.
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


// Always round to 1 decimal. Prematch picks happen to ship pre-
// rounded; live picks pass raw float edge_pct values like
// 19.594585336399327. Without this the live cards rendered
// "+19.594585336399327%". Defensive at the display layer.
function formatEdge(edge) {
  // Coerce to Number — string edges from upstream serializers would
  // throw `n.toFixed is not a function` and kill the whole card.
  const n = Number(edge)
  if (edge == null || !Number.isFinite(n)) return ''
  const rounded = Math.round(n * 10) / 10
  const sign = rounded >= 0 ? '+' : ''
  return `${sign}${rounded.toFixed(1)}%`
}

function formatStake(stake) {
  const n = Number(stake)
  // null/undefined/NaN → hide. 0 → show "0u" so the user knows the
  // model evaluated the pick and recommends NO stake (overconfidence
  // gates triggered). User asked for the tag on every card, even 0u.
  if (stake == null || !Number.isFinite(n)) return null
  if (n < 0) return null
  // Trim trailing zero on whole-number stakes: 1.0 -> "1u", 0.5 -> "0.5u"
  const s = n % 1 === 0 ? n.toFixed(0) : n.toString()
  return `${s}u`
}

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

  const type = typeLabel
            || TYPE_LABEL[pick.type]
            || humanizeBetType(pick.type)
            || pick.type
  const tone = accent === 'q1' ? Q1_STYLE : CONF_STYLE[confidence] || CONF_STYLE.lean

  const stakeLabel = formatStake(pick.stake_units)
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
      {stakeLabel && (
        <span
          className="rounded-sm border border-current/40 bg-background/30 px-1.5 py-0.5 text-[10px] font-bold tabular-nums leading-none"
          title="Recommended stake (units). 1u = your standard wager size."
        >
          {stakeLabel}
        </span>
      )}
      <span className="ml-auto text-xs font-bold tabular-nums">
        {formatEdge(pick.edge)}
      </span>
    </div>
  )
}
