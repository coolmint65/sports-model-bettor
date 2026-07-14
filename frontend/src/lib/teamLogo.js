/**
 * Team logo helpers — single source of truth for resolving a team
 * abbreviation (and optionally an upstream ESPN logo URL) to the
 * image we actually want to render.
 *
 * Why this lives here: the dark theme makes a few default logos
 * (modern brown Padres, e.g.) hard to read. Both PickOfDayHero
 * and the per-game TeamRow want the same override behavior, so
 * the override map lives in one place.
 *
 * resolve(sport, abbr, espnLogo) returns the URL to use, with this
 * priority: override → espnLogo → ESPN CDN fallback.
 */

// Per-team overrides for logos that don't read on dark surfaces.
// Local files in /public are preferred over hotlinks (Wikipedia,
// sportslogos.net both block hotlinking with 403s intermittently).
const LOGO_OVERRIDE = {
  mlb: {
    SD:  '/logos/mlb/sd-throwback.png',
    SDP: '/logos/mlb/sd-throwback.png',
    NYY: '/logos/mlb/nyy.svg',
    COL: '/logos/mlb/col.png',
  },
  nhl: {},
  nba: {},
  wnba: {},
  ncaam: {},
  ncaaw: {},
}

// Abbr aliases for ESPN CDN slug fallback. The canonical model abbr
// ("SDP", "AZ") sometimes drifts from ESPN's URL slug ("sd", "ari").
const ABBR_ALIAS = {
  mlb: { AZ: 'ari', SDP: 'sd', CWS: 'chw', WAS: 'wsh', TBR: 'tb', KCR: 'kc', SFG: 'sf' },
  nhl: { LAK: 'la', SJS: 'sj', NJD: 'nj', TBL: 'tb' },
  nba: { NOP: 'no', GSW: 'gs', UTAH: 'utah' },
  wnba: {},
  ncaam: {},
  ncaaw: {},
}

// ESPN CDN sport slugs. Some basketball leagues (Euroleague, KBL etc)
// aren't covered by ESPN at all — those fall through to no logo and
// the TeamRow component handles the null cleanly.
const ESPN_SPORT_SLUG = {
  mlb: 'mlb',
  nhl: 'nhl',
  nba: 'nba',
  // Summer League teams share NBA team IDs + logos exactly.
  nba_summer_league: 'nba',
  wnba: 'wnba',
  ncaam: 'mens-college-basketball',
  ncaaw: 'womens-college-basketball',
}

// WNBA + NCAA logos use the plain `{slug}.png` path, not the
// `scoreboard/{slug}.png` variant NBA/NHL/MLB use.
const _PLAIN_LOGO_LEAGUES = new Set(['wnba', 'ncaam', 'ncaaw'])

export function resolveTeamLogo(sport, abbr, espnLogo) {
  const key = abbr ? String(abbr).toUpperCase() : ''
  const override = key && (LOGO_OVERRIDE[sport] || {})[key]
  if (override) return override
  if (espnLogo) return espnLogo
  if (!key) return null
  const sportSlug = ESPN_SPORT_SLUG[sport]
  if (!sportSlug) return null  // league not on ESPN CDN (Euroleague etc.)
  const slug = ((ABBR_ALIAS[sport] || {})[key] || key.toLowerCase()).toLowerCase()
  if (_PLAIN_LOGO_LEAGUES.has(sport)) {
    return `https://a.espncdn.com/i/teamlogos/${sportSlug}/500/${slug}.png`
  }
  return `https://a.espncdn.com/i/teamlogos/${sportSlug}/500/scoreboard/${slug}.png`
}
