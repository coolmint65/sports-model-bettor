/**
 * MatchupCell — sport-aware matchup cell for tracker tables.
 *
 * Team sports: parses "AWAY @ HOME" and renders a small logo next to
 * each abbreviation (via lib/teamLogo). Falls back to the raw string
 * when the format doesn't match or the sport doesn't have logo support
 * (tennis vs, motorsports race names, golf tournament names).
 */
import { useState } from 'react'
import { cn } from '../../lib/utils'
import { resolveTeamLogo } from '../../lib/teamLogo'


// Sports where we render logos. Every other sport gets the plain matchup
// string — tennis uses player names ("Player A vs Player B"), motorsports
// use race names, golf uses tournament names, etc.
const _TEAM_SPORTS = new Set([
  'mlb', 'nhl', 'nba',
  'wnba', 'ncaam', 'ncaaw', 'afl', 'euroleague', 'nba_summer_league',
  'ahl', 'pwhl', 'aihl', 'nzihl',
  'fifa_world_cup', 'eng_premier', 'esp_laliga', 'ita_seriea',
  'ger_bundesliga', 'fra_ligue1', 'mls', 'us_nwsl', 'usl_championship',
  'us_open_cup', 'uefa_champions', 'uefa_europa', 'uefa_conference',
  'conmebol_libertadores', 'bra_seriea', 'arg_lpf', 'fifa_internationals',
  'ufl', 'nfl', 'cfb', 'college_baseball',
  // basketball framework overseas leagues
  'china_cba', 'bulgaria_nbl', 'czech_nbl', 'germany_bbl',
  'denmark_basketligaen', 'finland_korisliiga', 'france_pro_b',
  'greece_a1', 'hungary_nb1', 'iceland_urvalsdeild',
  'iceland_urvalsdeild_w', 'israel_super', 'latvia_lbl',
  'lithuania_lkl', 'slovakia_extraliga', 'slovenia_skl',
  'sweden_ligan', 'argentina_lnb', 'brazil_lbf_w', 'brazil_nbb',
  'dominican_lnb', 'puerto_rico_bsn', 'japan_b2', 'nz_nbl',
  'australia_nbl', 'korea_kbl',
])


export default function MatchupCell({ matchup, sport, homeLogo, awayLogo }) {
  if (!matchup) return <span className="text-muted-foreground">-</span>
  if (!sport || !_TEAM_SPORTS.has(sport)) {
    return <span className="truncate" title={matchup}>{matchup}</span>
  }
  // Parse "AWAY @ HOME" (also tolerates "AWAY@HOME" without spaces).
  const m = matchup.match(/^(.+?)\s*@\s*(.+)$/)
  if (!m) return <span className="truncate" title={matchup}>{matchup}</span>
  const away = m[1].trim()
  const home = m[2].trim()
  return (
    <span className="inline-flex items-center gap-1.5 min-w-0" title={matchup}>
      <TeamMini sport={sport} abbr={away} logoUrl={awayLogo} />
      <span className="text-muted-foreground/50 text-xs">@</span>
      <TeamMini sport={sport} abbr={home} logoUrl={homeLogo} />
    </span>
  )
}


function TeamMini({ sport, abbr, logoUrl }) {
  const [err, setErr] = useState(false)
  const logo = resolveTeamLogo(sport, abbr, logoUrl || null)
  return (
    <span className="inline-flex items-center gap-1">
      {logo && !err ? (
        <img
          src={logo}
          alt=""
          onError={() => setErr(true)}
          className={cn(
            'h-4 w-4 rounded-full object-contain flex-shrink-0',
            'bg-foreground/[0.06] ring-1 ring-border p-[1px]',
          )}
        />
      ) : (
        // Abbreviation-only fallback keeps the row visually aligned
        // when a league has no ESPN CDN logo (Euroleague, overseas
        // basketball / hockey framework). Rendering no glyph would
        // let the row jitter across leagues.
        <span
          aria-hidden="true"
          className="h-4 w-4 rounded-full bg-primary/15 ring-1 ring-border flex items-center justify-center text-[7px] font-bold text-primary flex-shrink-0"
        >
          {(abbr || '?').slice(0, 3).toUpperCase()}
        </span>
      )}
      <span className="font-semibold text-foreground text-xs">{abbr}</span>
    </span>
  )
}
