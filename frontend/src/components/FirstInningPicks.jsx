import { memo, useMemo } from 'react'

/**
 * FirstInningPicks
 * ──────────────────────────────────────────────────────────────
 * Side section on the MLB games tab that surfaces NRFI/YRFI picks
 * the model likes today. Scans the per-game `all_picks` list (top 4
 * by adjusted EV) for any entry tagged `type === '1st INN'` with
 * meaningful edge, and renders them as a compact card list.
 *
 * Rendered above the main scoreboard so users can see the first-inning
 * plays at a glance without scrolling. Renders nothing when no games
 * have qualifying 1st-inning picks — avoids an empty-state card.
 *
 * Props:
 *   bestBets   — the array of per-game objects the Scoreboard gets.
 *                Each entry has { game_id, matchup, home, away,
 *                all_picks: [{type, pick, prob, edge, odds, ...}], time }
 */
function FirstInningPicksImpl({ bestBets }) {
  const firstInningPicks = useMemo(() => {
    if (!bestBets || bestBets.length === 0) return []
    const out = []
    for (const bet of bestBets) {
      const picks = bet.all_picks || []
      // NRFI must clear the 1% edge floor the backend already applies;
      // YRFI must clear MLB_YRFI_MIN_EDGE (5% as of 2026-04-22). The
      // backend filters those out before they hit here, so whatever
      // reaches us is already playable — we just surface it.
      const fi = picks.find(p => p.type === '1st INN')
      if (!fi) continue
      out.push({
        game_id: bet.game_id,
        matchup: bet.matchup,
        home: bet.home,
        away: bet.away,
        time: bet.time,
        pick: fi,
      })
    }
    // Sort by edge descending so the strongest picks render first.
    out.sort((a, b) => (b.pick.edge || 0) - (a.pick.edge || 0))
    return out
  }, [bestBets])

  if (firstInningPicks.length === 0) return null

  const pct = n => `${(n * 100).toFixed(1)}%`

  return (
    <div className="first-inning-picks">
      <h3 className="section-title first-inning-title">
        First Inning Picks
        <span className="first-inning-count">{firstInningPicks.length}</span>
      </h3>
      <p className="first-inning-subtitle">
        NRFI / YRFI plays the model likes for today's slate
      </p>
      <div className="first-inning-grid">
        {firstInningPicks.map(entry => (
          <FirstInningCard key={entry.game_id} entry={entry} pct={pct} />
        ))}
      </div>
    </div>
  )
}


function FirstInningCard({ entry, pct }) {
  const { pick } = entry
  const isNrfi = pick.pick === 'NRFI'
  const confClass =
    pick.edge >= 8 ? 'strong' : pick.edge >= 4 ? 'moderate' : 'lean'

  // Formatted game time cached once per entry.time ref.
  const gameTime = useMemo(
    () => entry.time
      ? new Date(entry.time).toLocaleTimeString(
          [], { hour: 'numeric', minute: '2-digit' })
      : '',
    [entry.time],
  )

  return (
    <div className={`first-inning-card conf-${confClass}`}>
      <div className="fi-matchup">
        <span className="fi-teams">{entry.matchup}</span>
        {gameTime && <span className="fi-time">{gameTime}</span>}
      </div>
      <div className="fi-pick-row">
        <span className={`fi-pick-name fi-${isNrfi ? 'nrfi' : 'yrfi'}`}>
          {pick.pick}
        </span>
        <span className="fi-odds">
          {pick.odds > 0 ? '+' : ''}{pick.odds}
        </span>
      </div>
      <div className="fi-stats">
        <span className="fi-prob">{pct(pick.prob)}</span>
        <span className={`fi-edge positive`}>+{pick.edge}% edge</span>
      </div>
    </div>
  )
}


const FirstInningPicks = memo(FirstInningPicksImpl)
export default FirstInningPicks
