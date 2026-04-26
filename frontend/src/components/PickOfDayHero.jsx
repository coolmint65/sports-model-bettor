/**
 * PickOfDayHero — full-bleed editorial hero atop the Bets tab.
 *
 * Phase 2-cleanup restyle pass 2: The original two-col card felt
 * indistinguishable from the rest of the section cards. New layout
 * leans on the brand color (mint) for chrome, makes the EDGE the
 * single biggest number on the page, and adds a model-vs-market
 * split bar so the value reads at a glance instead of requiring
 * the user to math (model 92% vs market 36% = wide green over thin
 * red).
 *
 * Silently renders nothing when the backend returns an error or
 * "no pick today" message.
 */

import { useEffect, useState } from 'react'
import axios from 'axios'
import { humanizeBetType } from '../lib/betType'
import { resolveTeamLogo } from '../lib/teamLogo'
import { cn } from '../lib/utils'

export default function PickOfDayHero({ sport }) {
  const [potd, setPotd] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loaded, setLoaded] = useState(false)

  useEffect(() => {
    if (loaded) return
    setLoaded(true)
    const a = axios.create({ baseURL: '/api' })
    Promise.all([
      a.get(`/pick-of-day/${sport}`),
      a.get(`/pick-of-day/${sport}/summary`),
    ]).then(([p, s]) => {
      setPotd(p.data)
      setSummary(s.data)
    }).catch(() => {})
  }, [sport, loaded])

  if (!potd || potd.error || potd.message) return null

  const s = summary || {}
  const odds = potd.odds
  const oddsStr = odds ? `${odds > 0 ? '+' : ''}${odds}` : ''
  const betTypeLabel = humanizeBetType(potd.bet_type)
  const modelProb = (potd.model_prob != null && !isNaN(potd.model_prob))
    ? potd.model_prob : null
  const impliedProb = odds != null ? impliedFromAmerican(odds) : null
  const profitTone = s.profit > 0
    ? 'text-positive' : s.profit < 0 ? 'text-negative' : 'text-foreground'

  // Team logos derived from the matchup string. ESPN's CDN serves
  // logos at a stable per-sport URL pattern keyed by lowercase abbr.
  const teams = parseMatchup(potd.matchup, sport)

  return (
    <section className="relative overflow-hidden rounded-2xl border border-primary/30 bg-gradient-to-br from-card via-card to-primary/[0.04] mb-6">
      {/* Glow accent — brand mint behind the edge number */}
      <div
        aria-hidden="true"
        className="absolute -right-20 -top-20 h-64 w-64 rounded-full bg-primary/10 blur-3xl"
      />

      <div className="relative grid grid-cols-1 gap-5 p-6 lg:grid-cols-[1fr_auto] lg:gap-8">
        {/* LEFT — pick details */}
        <div className="min-w-0 space-y-3">
          <div className="flex items-center gap-2">
            <span className="text-[10px] font-bold uppercase tracking-[0.18em] text-primary">
              {sport.toUpperCase()} · Pick of the Day
            </span>
            {potd.confidence && (
              <span className="rounded-full bg-primary/15 px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider text-primary">
                {potd.confidence}
              </span>
            )}
          </div>

          <div>
            <div className="flex items-baseline gap-3 flex-wrap">
              <h2 className="text-3xl font-bold tracking-tight text-foreground sm:text-4xl">
                {potd.pick}
              </h2>
              {oddsStr && (
                <span className="text-lg font-semibold tabular-nums text-muted-foreground">
                  {oddsStr}
                </span>
              )}
            </div>
            {/* Matchup row with team logos + team nicknames */}
            {teams ? (
              <div className="mt-2 flex items-center gap-2 text-sm flex-wrap">
                <TeamBadge name={teams.awayName} logo={teams.awayLogo} />
                <span className="text-muted-foreground/60">@</span>
                <TeamBadge name={teams.homeName} logo={teams.homeLogo} />
                {betTypeLabel && (
                  <>
                    <span className="text-border mx-1">·</span>
                    <span className="text-muted-foreground">{betTypeLabel}</span>
                  </>
                )}
              </div>
            ) : (
              <div className="mt-1.5 text-sm">
                <span className="font-semibold text-foreground">{potd.matchup}</span>
                {betTypeLabel && (
                  <span className="ml-2 text-muted-foreground">· {betTypeLabel}</span>
                )}
              </div>
            )}
          </div>

          {/* Model vs market split — visual edge, not just a number */}
          {modelProb != null && impliedProb != null && (
            <ModelMarketBar modelProb={modelProb} impliedProb={impliedProb} />
          )}

          {potd.reasoning && (
            <p className="max-w-2xl text-xs leading-relaxed text-muted-foreground italic">
              {potd.reasoning}
            </p>
          )}
        </div>

        {/* RIGHT — edge headline + record */}
        <div className="flex flex-row items-center justify-between gap-6 lg:flex-col lg:items-end lg:justify-start lg:text-right">
          {potd.edge != null && (
            <div>
              <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
                Edge
              </div>
              <div className="text-5xl font-black tabular-nums text-positive leading-none mt-1 sm:text-6xl">
                +{potd.edge.toFixed(1)}<span className="text-2xl">%</span>
              </div>
            </div>
          )}

          {s.total > 0 && (
            <div className="border-l border-border pl-6 lg:mt-4 lg:border-l-0 lg:border-t lg:pl-0 lg:pt-4 lg:w-full">
              <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
                POTD Record
              </div>
              <div className="mt-1 flex items-baseline gap-2 lg:justify-end">
                <span className="text-xl font-bold tabular-nums text-foreground">
                  {s.wins}-{s.losses}
                </span>
                <span className={cn('text-sm font-semibold tabular-nums', profitTone)}>
                  {s.profit > 0 ? '+' : ''}${s.profit}
                </span>
              </div>
              <div className="mt-0.5 text-[10px] text-muted-foreground tabular-nums">
                {s.win_pct}% WR · {s.total} picks
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}


function ModelMarketBar({ modelProb, impliedProb }) {
  const modelPct = Math.round(modelProb * 100)
  const impliedPct = Math.round(impliedProb * 100)
  // Width-anchor: longest of the two so both bars share scale.
  const denom = Math.max(modelPct, impliedPct, 50)
  const modelW = `${(modelPct / denom) * 100}%`
  const impliedW = `${(impliedPct / denom) * 100}%`
  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-3 text-[11px]">
        <span className="w-14 font-semibold uppercase tracking-wider text-positive">Model</span>
        <div className="relative flex-1 h-3 rounded-full bg-secondary overflow-hidden">
          <div className="absolute inset-y-0 left-0 bg-positive rounded-full transition-all"
               style={{ width: modelW }} />
        </div>
        <span className="w-12 text-right tabular-nums font-bold text-positive">{modelPct}%</span>
      </div>
      <div className="flex items-center gap-3 text-[11px]">
        <span className="w-14 font-semibold uppercase tracking-wider text-muted-foreground">Market</span>
        <div className="relative flex-1 h-3 rounded-full bg-secondary overflow-hidden">
          <div className="absolute inset-y-0 left-0 bg-muted-foreground/50 rounded-full transition-all"
               style={{ width: impliedW }} />
        </div>
        <span className="w-12 text-right tabular-nums font-semibold text-muted-foreground">{impliedPct}%</span>
      </div>
    </div>
  )
}


function impliedFromAmerican(odds) {
  // Standard American → implied probability conversion.
  const n = Number(odds)
  if (!isFinite(n) || n === 0) return null
  return n > 0 ? 100 / (n + 100) : Math.abs(n) / (Math.abs(n) + 100)
}


// Team nickname lookup so "SD @ ARI" renders as "Padres @ Diamondbacks".
// Keyed by canonical abbr; aliases (SDP, AZ, CWS) normalize via
// ABBR_ALIAS first then fall back to a direct lookup. Falls through
// to the abbr itself when missing — never blocks rendering.
const TEAM_NAMES = {
  mlb: {
    ARI: 'Diamondbacks', ATL: 'Braves',  BAL: 'Orioles',  BOS: 'Red Sox',
    CHC: 'Cubs',         CHW: 'White Sox', CIN: 'Reds',   CLE: 'Guardians',
    COL: 'Rockies',      DET: 'Tigers',  HOU: 'Astros',   KC:  'Royals',
    LAA: 'Angels',       LAD: 'Dodgers', MIA: 'Marlins',  MIL: 'Brewers',
    MIN: 'Twins',        NYM: 'Mets',    NYY: 'Yankees',  ATH: 'Athletics',
    OAK: 'Athletics',    PHI: 'Phillies', PIT: 'Pirates', SD:  'Padres',
    SEA: 'Mariners',     SF:  'Giants',  STL: 'Cardinals', TB: 'Rays',
    TEX: 'Rangers',      TOR: 'Blue Jays', WSH: 'Nationals',
  },
  nhl: {
    ANA: 'Ducks', BOS: 'Bruins', BUF: 'Sabres', CAR: 'Hurricanes',
    CBJ: 'Blue Jackets', CGY: 'Flames', CHI: 'Blackhawks', COL: 'Avalanche',
    DAL: 'Stars', DET: 'Red Wings', EDM: 'Oilers', FLA: 'Panthers',
    LA: 'Kings', LAK: 'Kings', MIN: 'Wild', MTL: 'Canadiens',
    NJ: 'Devils', NJD: 'Devils', NSH: 'Predators', NYI: 'Islanders',
    NYR: 'Rangers', OTT: 'Senators', PHI: 'Flyers', PIT: 'Penguins',
    SEA: 'Kraken', SJ: 'Sharks', SJS: 'Sharks', STL: 'Blues',
    TB: 'Lightning', TBL: 'Lightning', TOR: 'Maple Leafs', UTA: 'Hockey Club',
    VAN: 'Canucks', VGK: 'Golden Knights', WPG: 'Jets', WSH: 'Capitals',
  },
  nba: {
    ATL: 'Hawks', BOS: 'Celtics', BKN: 'Nets', CHA: 'Hornets',
    CHI: 'Bulls', CLE: 'Cavaliers', DAL: 'Mavericks', DEN: 'Nuggets',
    DET: 'Pistons', GS: 'Warriors', GSW: 'Warriors', HOU: 'Rockets',
    IND: 'Pacers', LAC: 'Clippers', LAL: 'Lakers', MEM: 'Grizzlies',
    MIA: 'Heat', MIL: 'Bucks', MIN: 'Timberwolves', NO: 'Pelicans',
    NOP: 'Pelicans', NY: 'Knicks', NYK: 'Knicks', OKC: 'Thunder',
    ORL: 'Magic', PHI: '76ers', PHX: 'Suns', POR: 'Trail Blazers',
    SAC: 'Kings', SA: 'Spurs', SAS: 'Spurs', TOR: 'Raptors',
    UTAH: 'Jazz', UTA: 'Jazz', WSH: 'Wizards', WAS: 'Wizards',
  },
}

function teamName(sport, abbr) {
  if (!abbr) return ''
  const key = String(abbr).toUpperCase()
  return (TEAM_NAMES[sport] || {})[key] || abbr
}

function parseMatchup(matchup, sport) {
  if (!matchup || typeof matchup !== 'string') return null
  // Common forms: "SD @ ARI", "Boston Red Sox at Baltimore Orioles".
  const m = matchup.match(/^(\S+)\s*[@@]\s*(\S+)\s*$/)
  if (!m) return null
  const away = m[1]
  const home = m[2]
  return {
    away,
    home,
    awayName: teamName(sport, away),
    homeName: teamName(sport, home),
    awayLogo: resolveTeamLogo(sport, away, null),
    homeLogo: resolveTeamLogo(sport, home, null),
  }
}


function TeamBadge({ name, logo }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      {logo && (
        <img
          src={logo}
          alt=""
          className="h-6 w-6 rounded-full object-contain bg-foreground/[0.06] ring-1 ring-border p-0.5"
          onError={(e) => { e.currentTarget.style.display = 'none' }}
        />
      )}
      <span className="font-bold text-foreground">{name}</span>
    </span>
  )
}
