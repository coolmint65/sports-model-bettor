/**
 * curateReasoning
 * ──────────────────────────────────────────────────────────────
 * The engine's _build_reasoning functions (MLB, NHL, NBA) emit a
 * kitchen-sink list of facts about the game — OPS, wRC+, form,
 * records, park factor, etc. Printed verbatim in "Why this pick?"
 * they produce lists where every line contradicts the recommended
 * side on an underdog play, which reads as "the model is picking
 * against its own evidence".
 *
 * This curator runs client-side over the raw reason strings and:
 *   1. drops lines that are redundant with other UI surfaces
 *      (Projected Outcome already shows expected score + records)
 *   2. classifies each remaining line by which team it favors
 *   3. for ML / moneyline picks, filters out lines that favor the
 *      opposing side (the UnderdogNote honestly discloses the
 *      model-vs-market gap; this list is reserved for signals
 *      that actually support the recommendation)
 *   4. humanizes wording ("Team OPS: COL 0.531 | LAD 0.743" →
 *      "LAD's bats have been better (OPS .743 vs .531)")
 *   5. caps output at 5 lines
 *
 * Pure, side-effect-free, sport-agnostic — pattern-matches on the
 * engine's output strings so MLB/NHL/NBA can share it.
 */

// ── Redundant prefixes that duplicate the Projected Outcome card ──
const REDUNDANT_PREFIXES = [
  'Model projects',
  'Records:',
]

// Soft cap so "Why this pick?" doesn't become a wall of bullets.
const MAX_LINES = 5


// Infer which team side the recommended pick is on, for ML-type
// picks. Returns 'home' | 'away' | null (null for spreads, totals,
// etc. where supports-vs-contradicts logic doesn't apply cleanly).
function inferPickSide(pick, home, away) {
  if (!pick || !pick.pick) return null
  const isMl = pick.type === 'ML' || pick.type === 'F5 ML' || pick.type === 'Q1_ML'
  if (!isMl) return null
  if (pick.pick === home.abbreviation) return 'home'
  if (pick.pick === away.abbreviation) return 'away'
  // NBA Q1_ML renders as "{abbr} Q1 ML" — strip the suffix.
  const cleaned = String(pick.pick).replace(/\s+(Q1\s+)?ML$/i, '').trim()
  if (cleaned === home.abbreviation) return 'home'
  if (cleaned === away.abbreviation) return 'away'
  return null
}


// Classify + humanize one raw reason line. Returns
//   { kind: 'redundant' }                       to drop unconditionally
//   { kind: 'keep', favors: 'home'|'away'|null, text: string }
// where `favors` is the side the line's signal favors (or null if
// the line is neutral/descriptive).
function parseLine(raw, home, away) {
  const hAbbr = home.abbreviation
  const aAbbr = away.abbreviation

  for (const p of REDUNDANT_PREFIXES) {
    if (raw.startsWith(p)) return { kind: 'redundant' }
  }

  // "XYZ favored at 54% win probability" is redundant with the win
  // probability bar immediately above.
  if (/favored at \d+% win probability/i.test(raw)) return { kind: 'redundant' }
  if (/^Draw is the most likely outcome/i.test(raw)) return { kind: 'redundant' }

  // ── Two-team comparison stats: "Label: {TEAM} X | {TEAM} Y" ──
  const cmpMatch = raw.match(/^([A-Za-z+]+): (\w+) ([\d.]+) \| (\w+) ([\d.]+)$/)
  if (cmpMatch) {
    const [, label, t1, v1s, t2, v2s] = cmpMatch
    const v1 = parseFloat(v1s)
    const v2 = parseFloat(v2s)
    const lower = label.toLowerCase()
    const lowerBetter = lower.includes('era') || lower === 'opp ppg'
    const t1Better = lowerBetter ? v1 < v2 : v1 > v2
    const winner = t1Better ? t1 : t2
    const loser  = t1Better ? t2 : t1
    const winVal = t1Better ? v1s : v2s
    const losVal = t1Better ? v2s : v1s
    const favors = winner === hAbbr ? 'home' : winner === aAbbr ? 'away' : null
    return {
      kind: 'keep',
      favors,
      text: humanizeComparison(label, winner, loser, winVal, losVal),
    }
  }

  // "Runs/game: NYY 5.1 | BOS 4.4"
  const rpgMatch = raw.match(/^(?:Runs\/game|PPG|YPG): (\w+) ([\d.]+) \| (\w+) ([\d.]+)$/)
  if (rpgMatch) {
    const [, t1, v1s, t2, v2s] = rpgMatch
    const t1Better = parseFloat(v1s) > parseFloat(v2s)
    const winner = t1Better ? t1 : t2
    const favors = winner === hAbbr ? 'home' : winner === aAbbr ? 'away' : null
    return {
      kind: 'keep',
      favors,
      text: `${winner} scoring more (${t1Better ? v1s : v2s} vs ${t1Better ? v2s : v1s} per game)`,
    }
  }

  // "{TEAM} SP: 3.45 ERA, 9.1 K/9, 1.23 WHIP" — per-team pitcher line.
  const spMatch = raw.match(/^(\w+) SP: ([\d.]+) ERA, ([\d.]+) K\/9, ([\d.]+) WHIP$/)
  if (spMatch) {
    const [, t, era, k9, whip] = spMatch
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return {
      kind: 'keep',
      favors,
      text: `${t} starter: ${era} ERA, ${k9} K/9, ${whip} WHIP`,
    }
  }

  // "{TEAM} running hot (form +X.X%)" / "running cold (form -X.X%)"
  const formMatch = raw.match(/^(\w+) running (hot|cold) \(form ([+-]?[\d.]+)%\)$/)
  if (formMatch) {
    const [, t, dir] = formMatch
    const isHot = dir === 'hot'
    // "Hot" favors t; "cold" favors the OPPOSING team.
    const winner = isHot ? t : (t === hAbbr ? aAbbr : hAbbr)
    const favors = winner === hAbbr ? 'home' : winner === aAbbr ? 'away' : null
    return {
      kind: 'keep',
      favors,
      text: isHot ? `${t} trending up recently` : `${t} coming in cold`,
    }
  }

  // "Park factor 1.15 - hitter-friendly venue"
  const parkMatch = raw.match(/^Park factor ([\d.]+) - (hitter|pitcher)-friendly venue$/)
  if (parkMatch) {
    const [, pf, side] = parkMatch
    return {
      kind: 'keep',
      favors: null,
      text: side === 'hitter'
        ? `Hitter-friendly park (run factor ${pf})`
        : `Pitcher-friendly park (run factor ${pf})`,
    }
  }

  // "{TEAM} batters vs {TEAM} SP: 0.222 AVG (10/45, 1 HR)"
  const h2hMatch = raw.match(/^(\w+) batters vs (\w+) SP: ([\d.]+) AVG \((\d+)\/(\d+), (\d+) HR\)$/)
  if (h2hMatch) {
    const [, batters, pitcherTeam, avg, hits, ab, hr] = h2hMatch
    const avgNum = parseFloat(avg)
    const leagueAvg = 0.248
    const batterSide = batters === hAbbr ? 'home' : batters === aAbbr ? 'away' : null
    const favors = avgNum > leagueAvg ? batterSide
                 : avgNum < leagueAvg - 0.02
                   ? (batterSide === 'home' ? 'away' : batterSide === 'away' ? 'home' : null)
                   : null
    return {
      kind: 'keep',
      favors,
      text: `${batters} hit ${avg} lifetime vs ${pitcherTeam}'s starter (${hits}-for-${ab}, ${hr} HR)`,
    }
  }

  // "{TEAM} historically struggles in this matchup" — matchup insight.
  const histMatch = raw.match(/^(\w+) historically struggles in this matchup/)
  if (histMatch) {
    const t = histMatch[1]
    // Struggles favors the OTHER side.
    const winner = t === hAbbr ? aAbbr : t === aAbbr ? hAbbr : null
    const favors = winner === hAbbr ? 'home' : winner === aAbbr ? 'away' : null
    return {
      kind: 'keep',
      favors,
      text: `${t} has been historically weak in this matchup`,
    }
  }

  // ── NBA Q1-specific lines (already humanized in the engine) ──
  if (/^(Fast|Slow)-paced matchup/.test(raw)) {
    return { kind: 'keep', favors: null, text: raw }
  }
  if (/^Home court Q1 boost/.test(raw)) {
    return { kind: 'keep', favors: 'home', text: raw }
  }
  if (/Q1 quality edge: (\w+) wins Q1/.test(raw)) {
    const m = raw.match(/Q1 quality edge: (\w+) wins Q1/)
    const t = m[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  const b2bMatch = raw.match(/^(\w+) on back-to-back/)
  if (b2bMatch) {
    const t = b2bMatch[1]
    const loser = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    const favors = loser === 'home' ? 'away' : loser === 'away' ? 'home' : null
    return { kind: 'keep', favors, text: raw }
  }
  const recentQ1 = raw.match(/^(\w+) recent Q1 form: ([+-][\d.]+)/)
  if (recentQ1) {
    const t = recentQ1[1]
    const margin = parseFloat(recentQ1[2])
    const winner = margin > 0 ? t : (t === hAbbr ? aAbbr : hAbbr)
    const favors = winner === hAbbr ? 'home' : winner === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }

  // ── NHL scoring/conceding lines ──
  const scoringMatch = raw.match(/^(\w+) scoring ([\d.]+)\/gm vs (\w+) conceding ([\d.]+)\/gm$/)
  if (scoringMatch) {
    const [, scorer, sv, opp, cv] = scoringMatch
    const favors = scorer === hAbbr ? 'home' : scorer === aAbbr ? 'away' : null
    return {
      kind: 'keep',
      favors,
      text: `${scorer}'s offense (${sv}/gm) vs ${opp}'s defense (${cv}/gm allowed)`,
    }
  }

  // ── NHL humanized reasons from NHLGameDetail.getReasoning ──
  // These are already sentence-form so we just classify them.
  const nhlGoalieStop = raw.match(/^(\w+)'s goalie is stopping significantly more shots than (\w+)'s/)
  if (nhlGoalieStop) {
    const t = nhlGoalieStop[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlGoalieEdge = raw.match(/^(\w+) has a slight goalie edge/)
  if (nhlGoalieEdge) {
    const t = nhlGoalieEdge[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  if (/^Goalie matchup is roughly even/.test(raw)) {
    return { kind: 'keep', favors: null, text: raw }
  }
  const nhlBetterTeam = raw.match(/^(\w+(?:\s\w+)*) is a fundamentally better team this season than (\w+(?:\s\w+)*)/)
  if (nhlBetterTeam) {
    const winnerName = nhlBetterTeam[1]
    const favors = winnerName === home.name ? 'home' : winnerName === away.name ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlSlightEdge = raw.match(/^(\w+) has a slight edge in overall team quality/)
  if (nhlSlightEdge) {
    const t = nhlSlightEdge[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  if (/playing at home where they have a clear advantage/.test(raw)) {
    return { kind: 'keep', favors: 'home', text: raw }
  }
  const nhlPpLines = raw.match(/^(\w+)'s top-\d+ power play/)
  if (nhlPpLines) {
    const t = nhlPpLines[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlGoodGoalie = raw.match(/^(\w+) has (?:one of the best goaltending units|elite goaltending)/)
  if (nhlGoodGoalie) {
    const t = nhlGoodGoalie[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlBadGoalie = raw.match(/^(\w+)'s goaltending has been (?:among the worst|a liability)/)
  if (nhlBadGoalie) {
    const t = nhlBadGoalie[1]
    const favors = t === hAbbr ? 'away' : t === aAbbr ? 'home' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlHot = raw.match(/^(\w+) (?:is red hot|is rolling)/)
  if (nhlHot) {
    const t = nhlHot[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlCold = raw.match(/^(\w+) (?:is ice cold|has been struggling)/)
  if (nhlCold) {
    const t = nhlCold[1]
    const favors = t === hAbbr ? 'away' : t === aAbbr ? 'home' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlTired = raw.match(/^(\w+) (?:played last night|is on back-to-back)/)
  if (nhlTired) {
    const t = nhlTired[1]
    const favors = t === hAbbr ? 'away' : t === aAbbr ? 'home' : null
    return { kind: 'keep', favors, text: raw }
  }
  const nhlRested = raw.match(/^(\w+) has had extra rest/)
  if (nhlRested) {
    const t = nhlRested[1]
    const favors = t === hAbbr ? 'home' : t === aAbbr ? 'away' : null
    return { kind: 'keep', favors, text: raw }
  }

  // ── Recent form / splits (NHL + NBA) ──
  const recentFormMatch = raw.match(/^Recent form: (\w+) (\d+)-(\d+) L(\d+) \(avg margin ([+-][\d.]+)\)$/)
  if (recentFormMatch) {
    const [, t, w, l, lN, mg] = recentFormMatch
    const margin = parseFloat(mg)
    const favors = margin > 0
      ? (t === hAbbr ? 'home' : t === aAbbr ? 'away' : null)
      : (t === hAbbr ? 'away' : t === aAbbr ? 'home' : null)
    return {
      kind: 'keep',
      favors,
      text: `${t} last ${lN}: ${w}-${l} (avg margin ${mg})`,
    }
  }

  // Fallback: keep as-is, unknown favors.
  return { kind: 'keep', favors: null, text: raw }
}


function humanizeComparison(label, winner, loser, winVal, losVal) {
  const lower = label.toLowerCase()
  const isEra = lower.includes('era')
  if (label === 'OPS' || label === 'Team OPS') {
    return `${winner}'s bats have been better (OPS ${winVal} vs ${losVal})`
  }
  if (label === 'wRC+') {
    return `${winner} ahead on wRC+ (${winVal} vs ${losVal})`
  }
  if (isEra) {
    return `${winner}'s staff cleaner (${winVal} ERA vs ${losVal})`
  }
  if (label === 'Pace') {
    return `Pace: ${winner} ${winVal} | ${loser} ${losVal}`
  }
  return `${label}: ${winner} ${winVal} > ${loser} ${losVal}`
}


export function curateReasoning(rawLines, pick, home, away) {
  if (!rawLines || rawLines.length === 0) return []

  const pickSide = inferPickSide(pick, home, away)
  const out = []
  const seen = new Set()

  for (const raw of rawLines) {
    const info = parseLine(raw, home, away)
    if (info.kind === 'redundant') continue
    if (pickSide && info.favors && info.favors !== pickSide) continue
    if (seen.has(info.text)) continue
    seen.add(info.text)
    out.push(info.text)
    if (out.length >= MAX_LINES) break
  }

  return out
}
