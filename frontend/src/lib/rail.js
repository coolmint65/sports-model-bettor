import { humanizeBetType } from './betType'

// The card pick's market tag + label -- shared by the card's PaperBadge and the "Picks · best
// first" rail so both read the same.
export function pickParts(pick) {
  const t = pick.type || ''
  const isQ1 = t.startsWith('Q1_')
  const base = isQ1 ? t.slice(3) : t
  const baseMkt = base === 'ML' ? 'ML' : base === 'SPREAD' ? 'SPR' : base === 'TOTAL' ? 'O/U'
    : base === 'RUN_LINE' ? 'RL' : base === 'PUCK_LINE' ? 'PL' : (base === 'Period Total' && /^P[123]\b/.test(pick.pick || '')) ? ((pick.pick.match(/^P[123]/) || [''])[0] + ' O/U') : humanizeBetType(pick.market || base || '')
  const mkt = isQ1 ? `Q1 ${baseMkt}` : baseMkt
  const label = pick.display || (base === 'ML'
    ? `${pick.pick}${pick.odds != null ? ` ${pick.odds > 0 ? '+' : ''}${pick.odds}` : ''}`
    : pick.line == null
      ? pick.pick
      : base === 'TOTAL'
        ? `${pick.pick} ${pick.line}`
        : `${pick.pick} ${pick.line > 0 ? '+' : ''}${pick.line}`)
  return { mkt, label }
}

// What a card's headline slot shows, mirroring GameCard: the view's pick, else (full-game view
// only) the scoreboard's recorded pick; a lean only before the game starts.
export function shownPick(game, bet, view) {
  // leans stay listed once the game starts (Austin 2026-10-27), same as the card
  return bet?.paper_pick || ((!view || view === 'full') ? game?.paper_pick : null) || null
}

// "Picks · best first" ranking (Austin 2026-10-27 "OK with that"):
//   1. graduated picks, by edge x prob (the Pick Authority / POTD rule);
//   2. leans, by their market's track record so far -- edge_proven's confidence the cell is +EV
//      (cell_conf; 0.5 = no record yet) -- edge x prob breaking ties. Biggest-edge-first would put
//      the unproven models' overconfident numbers on top;
//   3. games with nothing, by start time.
const _both = (p) => (Number(p?.edge) || 0) * (Number(p?.prob ?? p?.model_prob) || 0)
const _conf = (p) => { const c = Number(p?.cell_conf); return p?.cell_conf != null && Number.isFinite(c) ? c : 0.5 }
const _tier = (p) => (!p ? 2 : p.graduated === true ? 0 : 1)

export function comparePicks(a, b) {
  const ta = _tier(a), tb = _tier(b)
  if (ta !== tb) return ta - tb
  if (ta === 1) {
    const d = _conf(b) - _conf(a)
    if (d) return d
  }
  return _both(b) - _both(a)
}

// entries: [{ id, matchup, pick, date, final }] -> GameRail items, live/upcoming first, finals after.
export function buildRailItems(entries) {
  const order = (xs) => [...xs].sort((a, b) => comparePicks(a.pick, b.pick)
    || String(a.date || '').localeCompare(String(b.date || '')))
  const all = entries || []
  return [...order(all.filter(e => !e.final)), ...order(all.filter(e => e.final))].map(e => {
    const p = e.pick
    const parts = p ? pickParts(p) : null
    const edge = p && p.edge != null && Number.isFinite(Number(p.edge)) ? Number(Number(p.edge).toFixed(1)) : null
    return {
      id: e.id,
      matchup: e.matchup,
      final: !!e.final,
      pick: parts ? `${parts.mkt} ${parts.label}` : '',
      edge,
      tag: !p ? null : p.graduated === true ? 'grad' : 'lean',
      result: p?.result,
    }
  })
}

export const railMatchup = (g) => g?.short_name || g?.name
  || `${(g?.away || {}).abbreviation || ''} @ ${(g?.home || {}).abbreviation || ''}`

// Scroll to the card and flash a ring around it so it's easy to spot (Austin 2026-10-31: "highlight the
// card once it brings you to it ... should apply to all the lists"). Web Animations API, so no CSS
// class has to survive Tailwind's purge; the ring follows the card's rounded corners.
export function jumpToCard(gid) {
  const el = typeof document !== 'undefined' ? document.getElementById(`sbcard-${gid}`) : null
  if (!el) return
  el.scrollIntoView({ behavior: 'smooth', block: 'center' })
  const card = el.firstElementChild || el     // the wrapper is square; the card inside carries the rounded corners
  if (typeof card.animate !== 'function') return
  if (card._railFlash) card._railFlash.cancel()
  const on = '0 0 0 3px rgba(52, 211, 153, 0.95), 0 0 22px 4px rgba(52, 211, 153, 0.45)'
  const off = '0 0 0 0 rgba(52, 211, 153, 0), 0 0 0 0 rgba(52, 211, 153, 0)'
  card._railFlash = card.animate(
    [{ boxShadow: off }, { boxShadow: on, offset: 0.12 }, { boxShadow: on, offset: 0.7 }, { boxShadow: off }],
    { duration: 2600, delay: 250, easing: 'ease-out' },
  )
}
