export function ordinal(n) {
  if (!n) return ''
  const s = ['th', 'st', 'nd', 'rd']
  const v = n % 100
  return n + (s[(v - 20) % 10] || s[v] || s[0])
}


// ─── Reasoning + edge selection (logic preserved verbatim) ──────────

export function getReasoning(pred, home, away) {
  const reasons = []
  const f = pred.factors || {}
  const ctx = pred.season_context || {}
  const hCtx = ctx.home || {}
  const aCtx = ctx.away || {}
  const wp = pred.win_prob || {}

  if (pred.goalie_matchup?.home && pred.goalie_matchup?.away) {
    const h_sv = pred.goalie_matchup.home.save_pct || 0
    const a_sv = pred.goalie_matchup.away.save_pct || 0
    if (h_sv > 0 && a_sv > 0) {
      const diff = Math.abs(h_sv - a_sv)
      const better = h_sv > a_sv ? home.abbreviation : away.abbreviation
      const worse = h_sv > a_sv ? away.abbreviation : home.abbreviation
      if (diff > 0.020) {
        reasons.push(`${better}'s goalie is stopping significantly more shots than ${worse}'s (${Math.max(h_sv, a_sv).toFixed(3)} vs ${Math.min(h_sv, a_sv).toFixed(3)} save %)`)
      } else if (diff > 0.010) {
        reasons.push(`${better} has a slight goalie edge tonight, saving about 1 more goal per 100 shots`)
      } else {
        reasons.push(`Goalie matchup is roughly even tonight (${h_sv.toFixed(3)} vs ${a_sv.toFixed(3)} save %)`)
      }
    }
  }

  const hPace = hCtx.points_pace || 0
  const aPace = aCtx.points_pace || 0
  if (Math.abs(hPace - aPace) > 0.12) {
    const better = hPace > aPace ? home.name : away.name
    const worse = hPace > aPace ? away.name : home.name
    reasons.push(`${better} is a fundamentally better team this season than ${worse}`)
  } else if (Math.abs(hPace - aPace) > 0.05) {
    const better = hPace > aPace ? home.abbreviation : away.abbreviation
    reasons.push(`${better} has a slight edge in overall team quality this season`)
  }

  if (wp.home > 0.55) {
    reasons.push(`${home.abbreviation} playing at home where they have a clear advantage this season`)
  }

  const strongPP = (rank) => rank != null && rank <= 10
  const weakPK   = (rank) => rank != null && rank >= 23
  if (f.home_pp != null && f.away_pk != null
      && strongPP(f.home_pp_rank) && weakPK(f.away_pk_rank)) {
    const h_pp = f.home_pp * 100
    const a_pk = f.away_pk * 100
    reasons.push(`${home.abbreviation}'s top-${Math.min(10, f.home_pp_rank)} power play (${h_pp.toFixed(1)}%) lines up against ${away.abbreviation}'s ${ordinal(f.away_pk_rank)}-ranked penalty kill (${a_pk.toFixed(1)}%)`)
  }
  if (f.away_pp != null && f.home_pk != null
      && strongPP(f.away_pp_rank) && weakPK(f.home_pk_rank)) {
    const a_pp = f.away_pp * 100
    const h_pk = f.home_pk * 100
    reasons.push(`${away.abbreviation}'s top-${Math.min(10, f.away_pp_rank)} power play (${a_pp.toFixed(1)}%) lines up against ${home.abbreviation}'s ${ordinal(f.home_pk_rank)}-ranked penalty kill (${h_pk.toFixed(1)}%)`)
  }

  if (f.home_sv_rank && f.home_sv_rank <= 5) {
    reasons.push(`${home.abbreviation} has one of the best goaltending units in the league (ranked ${ordinal(f.home_sv_rank)})`)
  } else if (f.home_sv_rank && f.home_sv_rank >= 28) {
    reasons.push(`${home.abbreviation}'s goaltending has been among the worst in the league this season`)
  }
  if (f.away_sv_rank && f.away_sv_rank <= 5) {
    reasons.push(`${away.abbreviation} has elite goaltending this season (ranked ${ordinal(f.away_sv_rank)})`)
  } else if (f.away_sv_rank && f.away_sv_rank >= 28) {
    reasons.push(`${away.abbreviation}'s goaltending has been a liability all season`)
  }

  const hL10 = hCtx.l10_pts_pct
  const aL10 = aCtx.l10_pts_pct
  if (hL10 != null && hL10 > 0.7) {
    reasons.push(`${home.abbreviation} is red hot, going ${hCtx.l10_record} in their last 10 games`)
  } else if (hL10 != null && hL10 < 0.35) {
    reasons.push(`${home.abbreviation} is ice cold, just ${hCtx.l10_record} in their last 10`)
  }
  if (aL10 != null && aL10 > 0.7) {
    reasons.push(`${away.abbreviation} is rolling with a ${aCtx.l10_record} record in their last 10 games`)
  } else if (aL10 != null && aL10 < 0.35) {
    reasons.push(`${away.abbreviation} has been struggling, going ${aCtx.l10_record} in their last 10`)
  }

  if (pred.rest?.home_b2b) {
    reasons.push(`${home.abbreviation} played last night, so tired legs tend to cost about half a goal`)
  }
  if (pred.rest?.away_b2b) {
    reasons.push(`${away.abbreviation} is on back-to-back nights, expect slower play and more mistakes`)
  }
  if (pred.rest?.home_rest_advantage && !pred.rest?.away_rest_advantage) {
    reasons.push(`${home.abbreviation} has had extra rest, giving them a fresh-legs advantage`)
  }
  if (pred.rest?.away_rest_advantage && !pred.rest?.home_rest_advantage) {
    reasons.push(`${away.abbreviation} has had extra rest, giving them a fresh-legs advantage`)
  }

  if (pred.injuries?.home_impact != null && pred.injuries.home_impact < 0.92) {
    const pct = Math.round((1 - pred.injuries.home_impact) * 100)
    reasons.push(`${home.abbreviation} is notably shorthanded (~${pct}% weaker from injuries)`)
  }
  if (pred.injuries?.away_impact != null && pred.injuries.away_impact < 0.92) {
    const pct = Math.round((1 - pred.injuries.away_impact) * 100)
    reasons.push(`${away.abbreviation} is notably shorthanded (~${pct}% weaker from injuries)`)
  }

  if (hCtx.fighting && aCtx.eliminated) {
    reasons.push(`${home.abbreviation} is fighting for their playoff life while ${away.abbreviation} has nothing to play for`)
  } else if (aCtx.fighting && hCtx.eliminated) {
    reasons.push(`${away.abbreviation} is desperate for points while ${home.abbreviation}'s season is already over`)
  } else if (hCtx.clinched && !aCtx.clinched && aCtx.fighting) {
    reasons.push(`${home.abbreviation} already clinched so they might not have the same urgency as ${away.abbreviation}`)
  } else if (aCtx.clinched && !hCtx.clinched && hCtx.fighting) {
    reasons.push(`${away.abbreviation} has their spot locked while ${home.abbreviation} needs this win more`)
  }

  if (f.home_shots_rank && f.away_shots_rank) {
    if (f.home_shots_rank <= 5 && f.away_shots_rank >= 25) {
      reasons.push(`${home.abbreviation} generates a ton of shots (${f.home_shots}/game) while ${away.abbreviation} gives up a lot, creating more scoring chances`)
    } else if (f.away_shots_rank <= 5 && f.home_shots_rank >= 25) {
      reasons.push(`${away.abbreviation} is an elite shot-generating team (${f.away_shots}/game) and will pepper the net tonight`)
    }
  }

  if (f.home_fo_rank && f.away_fo_rank) {
    if (f.home_fo_rank <= 5 && f.away_fo_rank >= 25) {
      reasons.push(`${home.abbreviation} dominates the faceoff circle (ranked ${ordinal(f.home_fo_rank)}) which means more puck possession`)
    } else if (f.away_fo_rank <= 5 && f.home_fo_rank >= 25) {
      reasons.push(`${away.abbreviation} wins faceoffs at an elite rate, giving them a possession edge`)
    }
  }

  if (pred.h2h && typeof pred.h2h === 'object' && pred.h2h.games >= 3) {
    const h2h = pred.h2h
    const homeWins = h2h.team1_wins || 0
    const awayWins = h2h.team2_wins || 0
    if (homeWins > awayWins + 2) {
      reasons.push(`${home.abbreviation} has owned this matchup recently, going ${homeWins}-${awayWins} in the last ${h2h.games} meetings`)
    } else if (awayWins > homeWins + 2) {
      reasons.push(`${away.abbreviation} has dominated this matchup lately, winning ${awayWins} of the last ${h2h.games} meetings`)
    }
  }

  const maxWp = Math.max(wp.home || 0, wp.away || 0)
  const fav = (wp.home || 0) > (wp.away || 0) ? home.abbreviation : away.abbreviation
  if (maxWp > 0.65) {
    reasons.push(`The model gives ${fav} a strong ${(maxWp * 100).toFixed(0)}% chance of winning this game`)
  }

  return reasons.slice(0, 5)
}


export function findBestEdge(data, odds, home, away) {
  const candidates = []
  const wp = data.win_prob

  if (odds.home_ml && wp.home) {
    const e = (wp.home - mlToProb(odds.home_ml)) * 100
    if (e > 1.5) candidates.push({ label: `${home.abbreviation} ML`, odds: odds.home_ml, edge: e })
  }
  if (odds.away_ml && wp.away) {
    const e = (wp.away - mlToProb(odds.away_ml)) * 100
    if (e > 1.5) candidates.push({ label: `${away.abbreviation} ML`, odds: odds.away_ml, edge: e })
  }

  if (odds.over_under && data.over_under) {
    const vt = parseFloat(odds.over_under)
    const key = Object.keys(data.over_under).find(k => Math.abs(parseFloat(k) - vt) < 0.5)
    if (key) {
      const ou = data.over_under[key]
      const pickOver = ou.over > ou.under
      const prob = Math.max(ou.over, ou.under)
      const realOdds = pickOver ? odds.over_odds : odds.under_odds
      if (realOdds) {
        const e = (prob - mlToProb(realOdds)) * 100
        if (e > 1.5) candidates.push({ label: `${pickOver ? 'Over' : 'Under'} ${vt}`, odds: realOdds, edge: e })
      }
    }
  }

  if (data.puck_line && odds.home_spread_odds && odds.home_spread_point != null) {
    const pt = odds.home_spread_point
    const hProb = pt < 0
      ? data.puck_line.home_minus_1_5
      : (data.puck_line.home_plus_1_5 || 1 - data.puck_line.away_minus_1_5)
    const e = (hProb - mlToProb(odds.home_spread_odds)) * 100
    if (e > 1.5) {
      candidates.push({
        label: `${home.abbreviation} ${pt > 0 ? '+' : ''}${pt}`,
        odds: odds.home_spread_odds,
        edge: e,
      })
    }
  }
  if (data.puck_line && odds.away_spread_odds && odds.away_spread_point != null) {
    const pt = odds.away_spread_point
    const aProb = pt < 0
      ? (data.puck_line.away_minus_1_5 || 1 - data.puck_line.home_plus_1_5)
      : data.puck_line.away_plus_1_5
    const e = (aProb - mlToProb(odds.away_spread_odds)) * 100
    if (e > 1.5) {
      candidates.push({
        label: `${away.abbreviation} ${pt > 0 ? '+' : ''}${pt}`,
        odds: odds.away_spread_odds,
        edge: e,
      })
    }
  }

  if (candidates.length === 0) return null
  const best = candidates.sort((a, b) => b.edge - a.edge)[0]
  best.rating = best.edge > 8 ? 'strong' : best.edge > 4 ? 'moderate' : 'lean'
  return best
}


export function edgeFromBackendPick(pick) {
  if (!pick) return null
  const type = pick.type || ''
  const label = type === 'ML' ? `${pick.pick} ML` : pick.pick
  return {
    label,
    odds: pick.odds,
    edge: pick.edge,
    rating: pick.confidence || 'lean',
  }
}


export function pickFromEdge(edge, home, away) {
  if (!edge) return null
  const m = edge.label.match(/^([A-Z]{2,4})\s+ML$/)
  if (!m) return null
  const abbr = m[1]
  if (abbr !== home.abbreviation && abbr !== away.abbreviation) return null
  return { type: 'ML', pick: abbr, odds: edge.odds }
}
