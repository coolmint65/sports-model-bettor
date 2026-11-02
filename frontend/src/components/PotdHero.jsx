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
 *   recordLabel    heading over the record block (default 'POTD Record')
 *   extra          optional node under the pick (props: stake chip + proven badge)
 *   recordFooter   optional node under the record block (props: legacy record)
 */

import { useState } from 'react'
import { humanizeBetType, derivMarketLabel } from '../lib/betType'
import { resolveTeamLogo } from '../lib/teamLogo'
import CLVBadge from './CLVBadge'
import { playerPhotoUrl } from '../lib/playerPhoto'
import L10Spark from './L10Spark'
import { cn } from '../lib/utils'


export default function PotdHero({
  label,
  sport,
  pick,
  summary,
  accent = 'primary',
  compact = false,
  noPick = false,
  recordLabel = 'POTD Record',
  extra = null,
  recordFooter = null,
}) {
  if (!pick && !noPick) return null
  if (!pick) {
    // Explicit NO PICK state so the hero never silently vanishes (Austin 2026-10-18).
    const _npText = accent === 'sky' ? 'text-sky-300' : accent === 'warning' ? 'text-warning' : 'text-primary'
    const _npBorder = accent === 'sky' ? 'border-sky-500/40' : accent === 'warning' ? 'border-warning/30' : 'border-primary/30'
    const _np = summary || {}
    const _rec = (_np.total || _np.wins || _np.losses)
    return (
      <section className={cn(
        compact ? 'relative overflow-hidden rounded-xl border px-5 py-4' : 'relative overflow-hidden rounded-2xl border mb-6 px-5 py-4',
        _npBorder, 'bg-card'
      )}>
        <div className={cn('text-[11px] font-semibold uppercase tracking-wider', _npText)}>{label}</div>
        <div className="mt-1.5 text-xl font-bold text-foreground/70">No Pick</div>
        <div className="mt-0.5 text-xs text-foreground/50">No market cleared the proven-edge gate today.</div>
        {_rec ? (
          <div className="mt-2 text-xs text-foreground/60">
            Record {_np.wins ?? 0}-{_np.losses ?? 0}{_np.profit != null ? ` · ${_np.profit > 0 ? '+' : ''}${_np.profit}u` : ''}
          </div>
        ) : null}
      </section>
    )
  }

  const odds = pick.odds
  const oddsStr = odds != null ? `${odds > 0 ? '+' : ''}${odds}` : ''
  // betTypeLabel computed after `teams` so derivatives can resolve their team
  const modelProb = (pick.model_prob != null && !isNaN(pick.model_prob))
    ? pick.model_prob : null
  const impliedProb = odds != null ? impliedFromAmerican(odds) : null
  const teams = parseMatchup(pick.matchup, sport, pick)
  const _isDeriv = /TEAM_TOTAL|F5_TOTAL|F5_/i.test(String(pick.bet_type || ''))
  const betTypeLabel = _isDeriv
    ? derivMarketLabel(pick, { homeName: teams?.homeName, awayName: teams?.awayName })
    : humanizeBetType(pick.bet_type)
  // Football (and any sport that stores the line separately) carries the spread/total
  // NUMBER in pick.line, not the pick text ("NE" / "Over"). Append it so the headline
  // reads "NE -4.0" / "Over 50.5". NHL/NBA leave line=null (number already in the text).
  const _bt = String(pick.bet_type || pick.type || '').toUpperCase()
  let pickText = pick.pick || ''
  if (pick.line != null && pick.line !== '' && !/[0-9]/.test(pickText)) {
    if (_bt.includes('TOTAL') || _bt === 'O/U') pickText = `${pickText} ${pick.line}`
    else pickText = `${pickText} ${Number(pick.line) > 0 ? '+' : ''}${pick.line}`
  }
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
    : accent === 'sky'
    ? {
      border: 'border-sky-500/40',
      gradient: 'bg-gradient-to-br from-card via-card to-sky-500/[0.06]',
      glow: 'bg-sky-500/10',
      label: 'text-sky-300',
      pillBg: 'bg-sky-500/20 text-sky-200',
      modelBar: 'bg-sky-400',
      edge: 'text-sky-400',
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
      compact ? 'relative overflow-hidden rounded-xl border' : 'relative overflow-hidden rounded-2xl border mb-6',
      accentClasses.border, accentClasses.gradient,
    )}>
      <div
        aria-hidden="true"
        className={cn(
          'absolute -right-20 -top-20 h-64 w-64 rounded-full blur-3xl',
          accentClasses.glow,
        )}
      />
      <div className={cn('relative grid grid-cols-1 lg:grid-cols-[1fr_auto]', compact ? 'gap-2 p-4' : 'gap-5 p-6 lg:gap-8')}>
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
                <h2 className={cn('font-bold tracking-tight text-foreground', compact ? 'text-lg sm:text-xl' : 'text-3xl sm:text-4xl')}>
                  {pickText}
                </h2>
                {oddsStr && (
                  <span className="text-lg font-semibold tabular-nums text-muted-foreground">
                    {oddsStr}
                  </span>
                )}
                {pick.odds != null && (
                  <CLVBadge entryOdds={pick.odds} closingOdds={pick.closing_odds} />
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

          {pick.game_date && (
            <div className="text-xs font-medium text-muted-foreground/80">
              {new Date(pick.game_date + 'T12:00:00').toLocaleDateString('en-US', { weekday: 'short', month: 'short', day: 'numeric' })}
            </div>
          )}

          {modelProb != null && impliedProb != null && (

            <ModelMarketBar
              modelProb={modelProb}
              impliedProb={impliedProb}
              barClass={accentClasses.modelBar}
            />
          )}

          {pick.l10 && pick.l10.n > 0 && (
            <L10Spark l10={pick.l10} pick={pick} height="h-12" />
          )}

          {extra}

          {!compact && pick.reasoning && (
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
                compact ? 'text-2xl font-black tabular-nums leading-none mt-1 sm:text-3xl' : 'text-5xl font-black tabular-nums leading-none mt-1 sm:text-6xl',
                accentClasses.edge,
              )}>
                {Number(pick.edge) > 0 ? '+' : ''}{Number(pick.edge).toFixed(1)}<span className={compact ? 'text-base' : 'text-2xl'}>%</span>
              </div>
            </div>
          )}

          {summary && summary.total != null && (
            <div className="border-l border-border pl-6 lg:mt-4 lg:border-l-0 lg:border-t lg:pl-0 lg:pt-4 lg:w-full">
              <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
                {recordLabel}
              </div>
              <div className="mt-1 flex items-baseline gap-2 lg:justify-end">
                <span className="text-xl font-bold tabular-nums text-foreground">
                  {summary.wins}-{summary.losses}
                </span>
                <span className={cn('text-sm font-semibold tabular-nums', profitTone)}>
                  {(summary.profit || 0) > 0 ? '+' : ''}${summary.profit}
                </span>
              </div>
              <div className="mt-0.5 flex items-center gap-2 text-[10px] text-muted-foreground tabular-nums lg:justify-end">
                <span>{(summary.total || 0) > 0 ? `${summary.win_pct}% WR · ${summary.total} picks` : (summary.pending ? `${summary.pending} pending` : 'no settled picks yet')}</span>
                {summary.last_result && (
                  <span className={cn('rounded px-1 py-0.5 text-[9px] font-bold uppercase',
                    summary.last_result === 'W' ? 'bg-positive/20 text-positive' : 'bg-negative/20 text-negative')}>
                    last {summary.last_result}
                  </span>
                )}
              </div>
              <div className="mt-0.5 text-[10px] text-muted-foreground tabular-nums lg:text-right">
                {summary.clv_n > 0
                  ? `CLV ${summary.clv_avg > 0 ? '+' : ''}${summary.clv_avg}% \u00b7 ${summary.clv_n} w/ close`
                  : 'CLV accruing'}
              </div>
              {recordFooter}
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
  // Bars represent 0–100% probability against a fixed track. Earlier
  // version normalized to max(model, market, 50) which made a 66%
  // model fill the whole track — visually unreadable. Now 100% = the
  // end of the bar, so a 66% model takes ~2/3 and a 99% model takes
  // nearly the full track. User-requested across all sports 2026-04-28.
  const modelW = `${Math.min(100, Math.max(0, modelPct))}%`
  const impliedW = `${Math.min(100, Math.max(0, impliedPct))}%`
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
        {(() => {
          // Austin 2026-10-03: show the matchup with TEAM LOGOS (not plain
          // text), same TeamBadge treatment as the team-POTD branch. Needs the
          // NHL ATG view to pass leagueKey="nhl" so resolveTeamLogo resolves.
          const t = parseMatchup(pick.matchup, sport, pick)
          return (
            <div className="mt-1 flex items-center gap-1.5 text-sm text-muted-foreground flex-wrap">
              {betTypeLabel && <span>{betTypeLabel}</span>}
              {t ? (
                <>
                  <span className="text-border">·</span>
                  <TeamBadge name={t.awayName} logo={t.awayLogo} />
                  <span className="text-muted-foreground/60">@</span>
                  <TeamBadge name={t.homeName} logo={t.homeLogo} />
                </>
              ) : (pick.matchup ? <span>· {pick.matchup}</span> : null)}
            </div>
          )
        })()}
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
    TB: 'Lightning', TBL: 'Lightning', TOR: 'Maple Leafs', UTA: 'Mammoth',
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

function parseMatchup(matchup, sport, pick) {
  if (!matchup || typeof matchup !== 'string') return null
  const m = matchup.match(/^(\S+)\s*[@@]\s*(\S+)\s*$/)
  if (!m) return null
  const away = m[1]
  const home = m[2]
  return {
    away, home,
    // Prefer a full name from the payload (football sends it — CFB has hundreds
    // of teams so there's no abbr→name map here). MintyBot 2026-10-16.
    awayName: (pick && pick.away_name) || teamName(sport, away),
    homeName: (pick && pick.home_name) || teamName(sport, home),
    awayLogo: resolveTeamLogo(sport, away, (pick && pick.away_logo) || null),
    homeLogo: resolveTeamLogo(sport, home, (pick && pick.home_logo) || null),
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
