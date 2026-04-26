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
            {/* Matchup row with team logos flanking the abbreviations */}
            {teams ? (
              <div className="mt-2 flex items-center gap-2 text-sm">
                <TeamBadge abbr={teams.away} logo={teams.awayLogo} />
                <span className="text-muted-foreground/60">@</span>
                <TeamBadge abbr={teams.home} logo={teams.homeLogo} />
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


// ESPN's CDN serves logos at a stable per-sport URL pattern keyed by
// lowercase team abbr. Tracker matchups can use either the canonical
// abbr ("SD") or the alias ESPN uses on its own pages ("SDP" etc.) —
// the alias map below normalizes the most common drift cases.
const ABBR_ALIAS = {
  mlb: { AZ: 'ari', SDP: 'sd', CWS: 'chw', WAS: 'wsh', TBR: 'tb', KCR: 'kc', SFG: 'sf' },
  nhl: { LAK: 'la', SJS: 'sj', NJD: 'nj', TBL: 'tb' },
  nba: { NOP: 'no', GSW: 'gs', UTAH: 'utah' },
}

function logoUrl(sport, abbr) {
  if (!abbr) return null
  const key = String(abbr).toUpperCase()
  const aliasMap = ABBR_ALIAS[sport] || {}
  const slug = (aliasMap[key] || key.toLowerCase()).toLowerCase()
  const sportSlug = sport === 'mlb' ? 'mlb'
                  : sport === 'nhl' ? 'nhl'
                  : 'nba'
  return `https://a.espncdn.com/i/teamlogos/${sportSlug}/500/scoreboard/${slug}.png`
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
    awayLogo: logoUrl(sport, away),
    homeLogo: logoUrl(sport, home),
  }
}


function TeamBadge({ abbr, logo }) {
  return (
    <span className="inline-flex items-center gap-1.5">
      {logo && (
        <img
          src={logo}
          alt=""
          className="h-5 w-5 object-contain"
          onError={(e) => { e.currentTarget.style.display = 'none' }}
        />
      )}
      <span className="font-bold tabular-nums text-foreground">{abbr}</span>
    </span>
  )
}
