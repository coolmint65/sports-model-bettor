/**
 * UnderdogNote — disambiguator under the EdgeCallout when the
 * recommended ML pick lands on the side the model does NOT favor
 * to win. Happens when the market prices a dog badly enough that
 * (model_prob - implied_prob) > 0 even though model_prob < 0.5.
 *
 * Without this line, the projected-score display ("LAD 5 - COL 4",
 * LAD green) visually contradicts the pick card ("STRONG -- COL ML").
 * The bet is correct — the note explains why.
 *
 * Phase 2-cleanup restyle: Tailwind alert box; muted positive tone.
 */

export default function UnderdogNote({ pick, wp, home, away }) {
  if (!pick || pick.odds == null) return null
  const isMl = pick.type === 'ML' || pick.type === 'F5 ML'
  if (!isMl) return null
  if (pick.odds <= 0) return null

  const pickAbbr = pick.pick
  const pickIsHome = pickAbbr === home.abbreviation
  const modelProb = pickIsHome ? wp?.home : wp?.away
  if (modelProb == null) return null
  if (modelProb >= 0.5) return null

  const impliedProb = 100 / (pick.odds + 100)
  const modelPct = (modelProb * 100).toFixed(0)
  const impliedPct = (impliedProb * 100).toFixed(0)

  return (
    <div className="rounded-md border border-positive/30 bg-positive/5 px-3 py-2 text-xs text-foreground">
      <span className="font-semibold text-positive">+EV underdog play:</span>{' '}
      model gives {pickAbbr} <strong className="tabular-nums">{modelPct}%</strong>,
      market prices them at <strong className="tabular-nums">{impliedPct}%</strong>.
    </div>
  )
}
