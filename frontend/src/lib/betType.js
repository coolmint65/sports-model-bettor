/**
 * humanizeBetType
 * ──────────────────────────────────────────────────────────────
 * Convert engine enum keys (Q1_ML, F5_OU, 1ST_INNING) into the
 * presentable form used in cards, tables, and the POTD hero.
 * Engine keeps its enum keys for dict lookups in
 * generate_*_picks; UI shouldn't leak the underscores.
 *
 * Unknown keys fall back to space-replacing underscores so a
 * future market type still reads cleanly without code changes.
 */

const BET_TYPE_LABELS = {
  ML: 'Moneyline',
  RL: 'Run Line',
  PL: 'Puck Line',
  'O/U': 'Over/Under',
  SPREAD: 'Spread',
  TOTAL: 'Total',

  Q1_ML: 'Q1 ML',
  Q1_SPREAD: 'Q1 Spread',
  Q1_TOTAL: 'Q1 Total',

  // Soccer first-half markets — same shape as Q1_* but H1-keyed.
  // DNB labels mirror Hard Rock exactly: HR ships SOCCER:FT:DNB as
  // "Winner (Push if Tied)" and SOCCER:P:DNB as "1st Half Winner".
  // Aligning here means the card label matches the slip the user sees
  // when they click through to place the bet — no terminology drift.
  H1_ML:    'H1 ML',
  H1_DC:    'H1 Double Chance',
  H1_DNB:   '1st Half Winner',
  H1_BTTS:  'H1 BTTS',
  H1_TOTAL: 'H1 Total',

  // Soccer full-game extras — bare keys read as initialisms.
  DC:   'Double Chance',
  DNB:  'Winner (Push if Tied)',
  BTTS: 'Both Teams Score',
  AH:   'Asian Handicap',
  OU:   'Over/Under',

  F5_ML: 'F5 ML',
  F5_OU: 'F5 O/U',
  F5_SPREAD: 'F5 Spread',
  F5_RL: 'F5 Run Line',

  '1ST_INNING': '1st Inning',
  '1ST_INN': '1st Inning',
  '1st INN': '1st Inning',

  'ALT RL': 'Alt Run Line',
  'ALT O/U': 'Alt Over/Under',
  'ALT PL': 'Alt Puck Line',
  'ALT SPREAD': 'Alt Spread',
  'ALT TOTAL': 'Alt Total',

  // Phase 1 derivatives. Labels mirror the engine keys closely so the
  // empirical-calibration tracker output and the UI use identical
  // strings — the only places we humanize are abbreviations
  // (BTS/DNB/O-E) that don't read cleanly on their own.
  'Team Total': 'Team Total',
  'F5 Team Total': 'F5 Team Total',
  'Inning Total': 'Inning Total',
  'Inning BTS': 'Inning Both Teams Score',
  '1st Inn Winner': '1st Inning Winner',
  'F5 Winner': 'F5 Winner',
  'Total O/E': 'Total Odd / Even',
  'Extra Innings': 'Extra Innings',

  'Period Total': 'Period Total',
  'Period BTS': 'Period Both Teams Score',
  // "DNB" (Draw No Bet) was actively confusing — the user read it as
  // "doesn't score" and questioned a correct W settle on a goal.
  // Display as plain "Period Winner" since the push-on-tie behaviour
  // is implicit (most readers interpret a period-winner bet that way
  // anyway). 2026-04-28.
  'Period DNB': 'Period Winner',
  'Overtime': 'Overtime',
  'BTS': 'Both Teams Score',

  'Q1 Team Total': 'Q1 Team Total',
  'Q1 Total O/E': 'Q1 Total Odd / Even',

  // Tennis bet types — engine emits SCREAMING_SNAKE_CASE (matches the
  // tennis_picks types table), UI surfaces the human form.
  ML:                       'Moneyline',
  SET_SPREAD:               'Set Spread',
  GAME_SPREAD:              'Game Spread',
  TOTAL_GAMES:              'Total Games',
  TOTAL_SETS:               'Total Sets',
  P1_TOTAL_GAMES:           'P1 Games',
  P2_TOTAL_GAMES:           'P2 Games',
  WIN_AT_LEAST_ONE_SET:     'Win 1+ Set',
  SET_BETTING:              'Set Betting',
  MOST_GAMES:               'Most Games',
}

export function humanizeBetType(t) {
  if (!t) return ''
  return BET_TYPE_LABELS[t]
      || BET_TYPE_LABELS[t.toUpperCase()]
      || t.replace(/_/g, ' ')
}
