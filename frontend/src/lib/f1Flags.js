/**
 * F1 nationality / country / team metadata for the MotorsportsPanel.
 *
 * Flag emoji are derived from ISO 3166-1 alpha-2 codes via the regional-
 * indicator-symbol trick — works without any image asset, ships zero
 * bytes over the wire, and renders as a real flag glyph on every modern
 * OS.
 *
 * Team colors are the canonical 2026-spec liveries each constructor
 * uses on broadcast graphics. They drive a 3px left-border accent on
 * each driver row so the panel visually groups teammates without an
 * explicit header — same UX pattern Sky F1 / F1 TV use.
 */

// Ergast nationality strings → ISO-3166 alpha-2.
// Covers every nationality currently or recently in the dataset; new
// rookies just need an entry added here when they show up.
export const NATIONALITY_TO_ISO = {
  American: 'US', Argentine: 'AR', Australian: 'AU', Brazilian: 'BR',
  British: 'GB', Canadian: 'CA', Chinese: 'CN', Danish: 'DK',
  Dutch: 'NL', Finnish: 'FI', French: 'FR', German: 'DE',
  Italian: 'IT', Japanese: 'JP', Mexican: 'MX', Monegasque: 'MC',
  'New Zealander': 'NZ', Spanish: 'ES', Thai: 'TH',
}

// Race country strings → ISO. Ergast uses a few non-ISO names so we
// map them explicitly rather than trying to derive from a shared lookup.
// NASCAR + IndyCar's ESPN feed uses country nouns for driver nationality
// too ("USA", "Sweden", "Mexico"), so this map doubles as the fallback
// when NATIONALITY_TO_ISO's adjectival lookup misses.
export const COUNTRY_TO_ISO = {
  Australia: 'AU', Austria: 'AT', Azerbaijan: 'AZ', Bahrain: 'BH',
  Belgium: 'BE', Brazil: 'BR', Canada: 'CA', China: 'CN',
  Colombia: 'CO', Denmark: 'DK', Finland: 'FI', France: 'FR',
  Germany: 'DE', Hungary: 'HU', Ireland: 'IE', Italy: 'IT',
  Japan: 'JP', Mexico: 'MX', Monaco: 'MC', Netherlands: 'NL',
  'New Zealand': 'NZ', Norway: 'NO', Qatar: 'QA',
  'Saudi Arabia': 'SA', Singapore: 'SG', Spain: 'ES', Sweden: 'SE',
  Switzerland: 'CH', 'United Kingdom': 'GB',
  UAE: 'AE', UK: 'GB', USA: 'US',
}

/**
 * Convert an ISO 3166-1 alpha-2 country code to a flag emoji using the
 * regional-indicator-symbol trick. Returns '' for unknown/empty input
 * so callers can use it inline without null-checks.
 */
export function flagEmoji(iso2) {
  if (!iso2 || iso2.length !== 2) return ''
  const codePoints = iso2.toUpperCase()
    .split('')
    .map(c => 127397 + c.charCodeAt(0))
  return String.fromCodePoint(...codePoints)
}

export function flagForNationality(nat) {
  if (!nat) return ''
  // F1 (Ergast) ships adjectival forms — "American", "British".
  // NASCAR / IndyCar (ESPN) ships country nouns — "USA", "Sweden".
  // Try both maps in order so the same helper works across every
  // motorsports series.
  return (
    flagEmoji(NATIONALITY_TO_ISO[nat])
    || flagEmoji(COUNTRY_TO_ISO[nat])
    || ''
  )
}

export function flagForCountry(country) {
  return flagEmoji(COUNTRY_TO_ISO[country])
}


// Constructor / racing team → livery color (hex). F1 matches the
// canonical 2026-spec liveries each team runs on broadcast tickers.
// NASCAR + IndyCar teams added so their driver-table rows carry the
// same side-stripe visual identity as F1.
export const TEAM_COLORS = {
  // F1
  'Alpine F1 Team':  '#0090d4',
  'Aston Martin':    '#229971',
  'Audi':            '#fe0000',
  'Cadillac F1 Team':'#073c5b',
  'Ferrari':         '#dc0000',
  'Haas F1 Team':    '#b6babd',
  'McLaren':         '#ff8000',
  'Mercedes':        '#27f4d2',
  'RB F1 Team':      '#1660ad',
  'Red Bull':        '#3671c6',
  'Sauber':          '#52e252',
  'Williams':        '#64c4ff',
  // NASCAR Cup Series (2026)
  'Hendrick Motorsports':       '#0069b1',
  'Joe Gibbs Racing':           '#e13a3e',
  'Team Penske':                '#ffcc00',
  'Trackhouse Racing':          '#00abc7',
  'TRACKHOUSE RACING TEAM':     '#00abc7',
  'Stewart-Haas Racing':        '#a80532',
  'RFK Racing':                 '#e0002b',
  '23XI Racing':                '#ff9800',
  'Front Row Motorsports':      '#003f7f',
  'Kaulig Racing':              '#f7b32b',
  'Spire Motorsports':          '#b60c0c',
  'Legacy Motor Club':          '#93273c',
  'Rick Ware Racing':           '#607d8b',
  'Wood Brothers Racing':       '#b71c1c',
  'Live Fast Motorsports':      '#8e24aa',
  // IndyCar Series (2026)
  'Chip Ganassi Racing':                    '#e60000',
  'Target Chip Ganassi Racing':             '#e60000',
  'Team Penske IndyCar':                    '#ffcc00',
  'Arrow McLaren':                          '#ff8000',
  'Andretti Autosport':                     '#00a651',
  'Andretti Global':                        '#00a651',
  "Meyer Shank Racing":                     '#b0b0b0',
  'Rahal Letterman Lanigan Racing':         '#000000',
  'Ed Carpenter Racing':                    '#66c1ff',
  'A.J. Foyt Enterprises':                  '#ff5722',
  'Juncos Hollinger Racing':                '#5c4c8a',
  'Prema Racing':                           '#e91e63',
  'Dale Coyne Racing':                      '#03a9f4',
}

export function teamColor(teamName) {
  return TEAM_COLORS[teamName] || 'transparent'
}
