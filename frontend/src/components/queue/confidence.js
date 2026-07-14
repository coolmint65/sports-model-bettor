/**
 * Confidence tier lookup for queue rows.
 *
 * Cell historical ROI maps to a tier — Strong (>=20%), Solid (10-20%),
 * or Lean (5-10%). Same thresholds power the accent color, badge, and
 * label shown on every row in both Today and Tracker views.
 */

export function confidenceFor(cellRoi) {
  if (cellRoi >= 20) return 'strong'
  if (cellRoi >= 10) return 'solid'
  return 'lean'
}

export const CONF_META = {
  strong: {
    label: 'Strong',
    accent: 'border-l-positive',
    badge: 'bg-positive/15 text-positive border-positive/30',
  },
  solid: {
    label: 'Solid',
    accent: 'border-l-emerald-500',
    badge: 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30',
  },
  lean: {
    label: 'Lean',
    accent: 'border-l-warning',
    badge: 'bg-warning/15 text-warning border-warning/30',
  },
}
