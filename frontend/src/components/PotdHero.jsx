/**
 * PotdHero — shared full-bleed Pick-of-the-Day card.
 *
 * Used by core PickOfDayHero, derivative POTD callout, and the
 * props PotdHero. Same visual language across all three so users
 * don't have to learn three different POTD layouts.
 *
 * Props (all optional except `pick` + `label`):
 *   label          "MLB · Pick of the Day", "MLB · Derivative POTD", etc.
 *   sport          'mlb' | 'nhl' | 'nba' (for team logo lookup)
 *   pick           pick row from API: matchup, pick, bet_type, odds,
 *                  edge, model_prob, confidence, reasoning
 *   summary        optional record: {wins, losses, profit, total, win_pct}
 *   accent         color class for the gradient + edge headline
 *                  ('primary' = mint core, 'warning' = amber derivative)
 */

import { useState } from 'react'
import { humanizeBetType } from '../lib/betType'
import { resolveTeamLogo } from '../lib/teamLogo'
import { playerPhotoUrl } from '../lib/playerPhoto'
import { cn } from '../lib/utils'


export default function PotdHero({
  label,
  sport,
  pick,
  summary,
  accent = 'primary',
}) {
  if (!pick) return null

  const odds = pick.odds
  const oddsStr = odds != null ? `${odds > 0 ? '+' : ''}${odds}` : ''
  const betTypeLabel = humanizeBetType(pick.bet_type)
  const modelProb = (pick.model_prob != null && !isNaN(pick.model_prob))
    ? pick.model_prob : null
  const impliedProb = odds != null ? impliedFromAmerican(odds) : null
  const teams = parseMatchup(pick.matchup, sport)
  const profitTone = (summary?.profit || 0) > 0
    ? 'text-positive'
    : (summary?.profit || 0) < 0 ? 'text-negative' : 'text-foreground'

  // Color tokens — mint for core, amber for derivative. Keeps both
  // POTDs visually distinct while sharing the same architecture.
  const accentClasses = accent === 'warning'
    ? {
      border: 'border-warning/30',
      gradient: 'bg-gradient-to-br from-card via-card to-warning/[0.05]',
      glow: 'bg-warning/10',
      label: 'text-warning',
      pillBg: 'bg-warning/15 text-warning',
      modelBar: 'bg-warning',
      edge: 'text-warning',
    }
    : {
      border: 'border-primary/30',
      gradient: 'bg-gradient-to-br from-card via-card to-primary/[0.04]',
      glow: 'bg-primary/10',
      label: 'text-primary',
      pillBg: 'bg-primary/15 text-primary',
      modelBar: 'bg-positive',
      edge: 'text-positive',
    }

  return (
    <section className={cn(
      'relative overflow-hidden rounded-2xl border mb-6',
      accentClasses.border, accentClasses.gradient,
    )}>
      <div
        aria-hidden="true"
        className={cn(
          'absolute -right-20 -top-20 h-64 w-64 rounded-full blur-3xl',
          accentClasses.glow,
        )}
      />
      <div className="relative grid grid-cols-1 gap-5 p-6 lg:grid-cols-[1fr_auto] lg:gap-8">
        <div className="min-w-0 space-y-3">
          <div className="flex items-center gap-2">
            <span className={cn(
              'text-[10px] font-bold uppercase tracking-[0.18em]',
              accentClasses.label,
            )}>
              {label}
            </span>
            {pick.confidence && (
              <span className={cn(
                'rounded-full px-2 py-0.5 text-[9px] font-bold uppercase tracking-wider',
                accentClasses.pillBg,
              )}>
                {pick.confidence}
              </span>
            )}
          </div>

          {/* Player POTDs (props) lead with the player photo + name;
              team POTDs (core / derivative / 1st INN) lead with the
              bet headline. The pick.player_id flag is set only on
              player_props_picks rows. */}
          {pick.player_id ? (
            <PlayerHeader pick={pick} sport={sport} betTypeLabel={betTypeLabel} oddsStr={oddsStr} />
          ) : (
            <div>
              <div className="flex items-baseline gap-3 flex-wrap">
                <h2 className="text-3xl font-bold tracking-tight text-foreground sm:text-4xl">
                  {pick.pick}
                </h2>
                {oddsStr && (
                  <span className="text-lg font-semibold tabular-nums text-muted-foreground">
                    {oddsStr}
                  </span>
                )}
              </div>
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
                  <span className="font-semibold text-foreground">{pick.matchup}</span>
                  {betTypeLabel && (
                    <span className="ml-2 text-muted-foreground">· {betTypeLabel}</span>
                  )}
                </div>
              )}
            </div>
          )}

          {modelProb != null && impliedProb != null && (
            <ModelMarketBar
              modelProb={modelProb}
              impliedProb={impliedProb}
              barClass={accentClasses.modelBar}
            />
          )}

          {pick.reasoning && (
            <p className="max-w-2xl text-xs leading-relaxed text-muted-foreground italic">
              {pick.reasoning}
            </p>
          )}
        </div>

        <div className="flex flex-row items-center justify-between gap-6 lg:flex-col lg:items-end lg:justify-start lg:text-right">
          {pick.edge != null && (
            <div>
              <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
                Edge
              </div>
              <div className={cn(
                'text-5xl font-black tabular-nums leading-none mt-1 sm:text-6xl',
                accentClasses.edge,
              )}>
                +{Number(pick.edge).toFixed(1)}<span className="text-2xl">%</span>
              </div>
            </div>
          )}

          {summary && (summary.total || 0) > 0 && (
            <div className="border-l border-border pl-6 lg:mt-4 lg:border-l-0 lg:border-t lg:pl-0 lg:pt-4 lg:w-full">
              <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
                POTD Record
              </div>
              <div className="mt-1 flex items-baseline gap-2 lg:justify-end">
                <span className="text-xl font-bold tabular-nums text-foreground">
                  {summary.wins}-{summary.losses}
                </span>
                <span className={cn('text-sm font-semibold tabular-nums', profitTone)}>
                  {(summary.profit || 0) > 0 ? '+' : ''}${summary.profit}
                </span>
              </div>
              <div className="mt-0.5 text-[10px] text-muted-foreground tabular-nums">
                {summary.win_pct}% WR · {summary.total} picks
              </div>
            </div>
          )}
        </div>
      </div>
    </section>
  )
}


function ModelMarketBar({ modelProb, impliedProb, barClass }) {
  const modelPct = Math.round(modelProb * 100)
  const impliedPct = Math.round(impliedProb * 100)
  const denom = Math.max(modelPct, impliedPct, 50)
  const modelW = `${(modelPct / denom) * 100}%`
  const impliedW = `${(impliedPct / denom) * 100}%`
  return (
    <div className="space-y-1.5">
      <div className="flex items-center gap-3 text-[11px]">
        <span className={cn('w-14 font-semibold uppercase tracking-wider', barClass.replace('bg-', 'text-'))}>
          Model
        </span>
        <div className="relative flex-1 h-3 rounded-full bg-secondary overflow-hidden">
          <div className={cn('absolute inset-y-0 left-0 rounded-full transition-all', barClass)}
               style={{ width: modelW }} />
        </div>
        <span className={cn('w-12 text-right tabular-nums font-bold', barClass.replace('bg-', 'text-'))}>
          {modelPct}%
        </span>
      </div>
      <div className="flex items-center gap-3 text-[11px]">
        <span className="w-14 font-semibold uppercase tracking-wider text-muted-foreground">Market</span>
        <div className="relative flex-1 h-3 rounded-full bg-secondary overflow-hidden">
          <div className="absolute inset-y-0 left-0 bg-muted-foreground/50 rounded-full transition-all"
               style={{ width: impliedW }} />
        </div>
        <span className="w-12 text-right tabular-nums font-semibold text-muted-foreground">
          {impliedPct}%
        </span>
      </div>
    </div>
  )
}


function impliedFromAmerican(odds) {
  const n = Number(odds)
  if (!isFinite(n) || n === 0) return null
  return n > 0 ? 100 / (n + 100) : Math.abs(n) / (Math.abs(n) + 100)
}


function PlayerHeader({ pick, sport, betTypeLabel, oddsStr }) {
  const [photoErr, setPhotoErr] = useState(false)
  const photo = !photoErr ? playerPhotoUrl(sport, pick.player_id) : null
  return (
    <div className="flex items-center gap-4">
      {photo ? (
        <img
          src={photo}
          alt=""
          className="h-20 w-20 rounded-full object-cover bg-muted ring-1 ring-border flex-shrink-0"
          onError={() => setPhotoErr(true)}
        />
      ) : (
        <div className="h-20 w-20 rounded-full bg-muted ring-1 ring-border flex items-center justify-center text-2xl font-bold text-muted-foreground flex-shrink-0">
          {(pick.player_name || '?').slice(0, 1)}
        </div>
      )}
      <div className="min-w-0 flex-1">
        <div className="text-2xl font-bold tracking-tight text-foreground sm:text-3xl truncate">
          {pick.player_name}
        </div>
        <div className="mt-0.5 text-sm text-muted-foreground truncate">
          {betTypeLabel}{pick.matchup ? ` · ${pick.matchup}` : ''}
        </div>
        <div className="mt-2 flex items-baseline gap-3 flex-wrap">
          <span className="text-xl font-bold text-foreground">{pick.pick}</span>
          {oddsStr && (
            <span className="text-base font-semibold tabular-nums text-muted-foreground">
              {oddsStr}
            </span>
          )}
        </div>
      </div>
    </div>
  )
}


// Team-name lookup so "SD @ ARI" renders as "Padres @ Diamondbacks".
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
  const m = matchup.match(/^(\S+)\s*[@@]\s*(\S+)\s*$/)
  if (!m) return null
  const away = m[1]
  const home = m[2]
  return {
    away, home,
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
