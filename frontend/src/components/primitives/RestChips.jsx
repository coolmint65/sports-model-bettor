/**
 * RestChips
 * ──────────────────────────────────────────────────────────────
 * The "tired"/"B2B"/"rested" chip row that sat above every game
 * card across all three sports, each with its own inline-styled
 * 20-line block. Renders nothing when there's no rest signal to
 * surface, so scoreboards can unconditionally mount it.
 *
 * Sport label variance: MLB used "tired", NHL+NBA used "B2B".
 * Caller sets `tiredLabel` if it wants the NHL variant. Default
 * is "tired" to match current MLB behavior.
 *
 * Signals:
 *   home_b2b / home_short_rest        -> red "tired" / "B2B" chip
 *   home_rest_advantage (one-sided)   -> blue "rested" chip
 */

export default function RestChips({ rest, home, away, tiredLabel = 'tired' }) {
  if (!rest) return null

  const homeTired = rest.home_b2b || rest.home_short_rest
  const awayTired = rest.away_b2b || rest.away_short_rest
  const homeRested = rest.home_rest_advantage && !rest.away_rest_advantage
  const awayRested = rest.away_rest_advantage && !rest.home_rest_advantage

  if (!homeTired && !awayTired && !homeRested && !awayRested) return null

  return (
    <div className="rest-chips">
      {awayTired && <Chip tone="warn">{away.abbreviation} {tiredLabel}</Chip>}
      {homeTired && <Chip tone="warn">{home.abbreviation} {tiredLabel}</Chip>}
      {awayRested && <Chip tone="info">{away.abbreviation} rested</Chip>}
      {homeRested && <Chip tone="info">{home.abbreviation} rested</Chip>}
    </div>
  )
}

function Chip({ tone, children }) {
  return <span className={`rest-chip rest-chip-${tone}`}>{children}</span>
}
