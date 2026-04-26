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
    // Yellow throwback cap — 1974-1984 era. Brown SD letters on
    // yellow reads cleanly against dark surfaces where the modern
    // brown-on-brown swinging friar disappears.
    SD:  '/logos/mlb/sd-throwback.png',
    SDP: '/logos/mlb/sd-throwback.png',
    NYY: '/logos/mlb/nyy.svg',
    COL: '/logos/mlb/col.png',
  },
  nhl: {},
  nba: {},
}

// Abbr aliases for ESPN CDN slug fallback. The canonical model abbr
// ("SDP", "AZ") sometimes drifts from ESPN's URL slug ("sd", "ari").
const ABBR_ALIAS = {
  mlb: { AZ: 'ari', SDP: 'sd', CWS: 'chw', WAS: 'wsh', TBR: 'tb', KCR: 'kc', SFG: 'sf' },
  nhl: { LAK: 'la', SJS: 'sj', NJD: 'nj', TBL: 'tb' },
  nba: { NOP: 'no', GSW: 'gs', UTAH: 'utah' },
}

export function resolveTeamLogo(sport, abbr, espnLogo) {
  const key = abbr ? String(abbr).toUpperCase() : ''
  const override = key && (LOGO_OVERRIDE[sport] || {})[key]
  if (override) return override
  if (espnLogo) return espnLogo
  if (!key) return null
  const slug = ((ABBR_ALIAS[sport] || {})[key] || key.toLowerCase()).toLowerCase()
  const sportSlug = sport === 'mlb' ? 'mlb' : sport === 'nhl' ? 'nhl' : 'nba'
  return `https://a.espncdn.com/i/teamlogos/${sportSlug}/500/scoreboard/${slug}.png`
}
