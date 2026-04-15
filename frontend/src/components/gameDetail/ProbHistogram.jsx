/**
 * Inline 95% CI render: a small SVG bell curve centered at the engine's
 * point-estimate probability. Width comes from the engine's ci_half_width
 * which is derived from data-quality signals (pitcher start count for
 * MLB, team games played for NHL/NBA). Skipped when the engine didn't
 * supply a band so older prediction payloads keep their existing layout.
 *
 * All three sports share this component so the visual language of
 * "how confident is the model" is identical across GameDetail surfaces.
 */
export default function ProbHistogram({ prob, low, high, halfWidth }) {
  if (prob == null || low == null || high == null || halfWidth == null) {
    return null
  }
  const W = 70, H = 18, PAD = 2
  const xLow  = PAD
  const xHigh = W - PAD
  const xMid  = (xLow + xHigh) / 2

  // Approximate normal density curve sampled across the band; a handful
  // of control points looks the same as a true Gaussian at this size.
  const points = []
  for (let i = 0; i <= 20; i++) {
    const t = i / 20
    const x = xLow + t * (xHigh - xLow)
    const z = (t - 0.5) * 4
    const y = H - PAD - (H - 2 * PAD) * Math.exp(-(z * z) / 2)
    points.push(`${x.toFixed(1)},${y.toFixed(1)}`)
  }
  const path = `M${xLow},${H} L${points.join(' L ')} L${xHigh},${H} Z`

  const tooltip = `95% CI: ${(low * 100).toFixed(1)}%–${(high * 100).toFixed(1)}% `
                 + `(±${(halfWidth * 100).toFixed(1)} pp)`

  return (
    <svg
      width={W}
      height={H}
      viewBox={`0 0 ${W} ${H}`}
      style={{ marginTop: 2 }}
      role="img"
      aria-label={tooltip}
    >
      <title>{tooltip}</title>
      <path d={path} fill="rgba(96,165,250,0.20)" stroke="rgba(96,165,250,0.55)" strokeWidth={0.8} />
      <line x1={xMid} y1={PAD} x2={xMid} y2={H - PAD} stroke="#60a5fa" strokeWidth={1} />
    </svg>
  )
}
