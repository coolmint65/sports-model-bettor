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

  Q1_ML: 'Q1 ML',
  Q1_SPREAD: 'Q1 Spread',
  Q1_TOTAL: 'Q1 Total',

  F5_ML: 'F5 ML',
  F5_OU: 'F5 O/U',
  F5_SPREAD: 'F5 Spread',
  F5_RL: 'F5 Run Line',

  '1ST_INNING': '1st Inning',
  '1ST_INN': '1st Inning',
  '1st INN': '1st Inning',
}

export function humanizeBetType(t) {
  if (!t) return ''
  return BET_TYPE_LABELS[t]
      || BET_TYPE_LABELS[t.toUpperCase()]
      || t.replace(/_/g, ' ')
}
