/**
 * EdgeBadge
 * ──────────────────────────────────────────────────────────────
 * Single source of truth for the "best calibrated pick" chip that
 * sits on every pregame card across MLB, NHL and NBA. Collapses
 * three near-identical render blocks in the sport-specific
 * Scoreboards (see Scoreboard.jsx, NHLScoreboard.jsx,
 * NBAScoreboard.jsx pre-C2) into one primitive.
 *
 * Two modes:
 *   <EdgeBadge pick={best_pick} confidence="strong" />  -> filled chip
 *   <EdgeBadge empty />                                 -> muted "NO PICK"
 *
 * `accent="q1"` swaps to the amber Q1 accent used by NBA scorecards.
 */

const TYPE_LABEL = {
  Q1_SPREAD: 'Q1 SPREAD',
  Q1_TOTAL: 'Q1 TOTAL',
  Q1_ML: 'Q1 WINNER',
}

export default function EdgeBadge({ pick, confidence = 'lean', empty, accent, typeLabel }) {
  if (empty || !pick) {
    return (
      <div
        className="pick-badge pick-badge-none"
        title="Model found no +EV play in the calibrated edges"
      >
        <span className="pick-badge-type">NO PICK</span>
      </div>
    )
  }

  const type = typeLabel || TYPE_LABEL[pick.type] || pick.type
  const classes = ['pick-badge', `badge-${confidence}`]
  if (accent === 'q1') classes.push('q1-badge')

  return (
    <div className={classes.join(' ')}>
      <span className="pick-badge-type">{type}</span>
      <span className="pick-badge-pick">{pick.pick}</span>
      <span className="pick-badge-edge">+{pick.edge}%</span>
    </div>
  )
}
