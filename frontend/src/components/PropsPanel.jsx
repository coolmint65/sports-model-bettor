/**
 * PropsPanel — per-sport player-prop picks display (Phase 2j).
 *
 * Renders today's POTD prop pick at the top, then the full list of
 * pending/settled picks pulled from /api/{sport}/props-tracker/summary.
 * Sport-agnostic — the same component handles MLB pitcher Ks, NBA
 * Player PRA, NHL Skater SOG; the bet_type column carries the
 * specifics.
 *
 * Tonight's calibration is uncalibrated (no settled history yet);
 * the +400 odds cap in the picker keeps us out of longshot territory
 * until 2j-backtest tunes per-bet-type reliability multipliers.
 */

import { useEffect, useState } from 'react'
import axios from 'axios'
import SectionCard from './primitives/SectionCard'
import { cn } from '../lib/utils'

const CONF_BADGE = {
  strong:   'bg-positive/15 text-positive',
  moderate: 'bg-primary/15 text-primary',
  lean:     'bg-muted text-muted-foreground',
}

export default function PropsPanel({ sport }) {
  const [potd, setPotd] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState(null)

  useEffect(() => {
    setLoading(true)
    const a = axios.create({ baseURL: '/api' })
    Promise.all([
      a.get(`/${sport}/player-props/potd`).then(r => r.data).catch(() => null),
      a.get(`/${sport}/props-tracker/summary`).then(r => r.data).catch(() => null),
    ]).then(([p, s]) => {
      setPotd(p && !p.message ? p : null)
      setSummary(s)
      setErr(s ? null : 'Failed to load props')
    }).finally(() => setLoading(false))
  }, [sport])

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading props…
      </div>
    )
  }

  const pending = summary?.pending || []
  const finished = summary?.history || []

  return (
    <div className="space-y-5 py-4">
      {potd && <PotdHero pick={potd} sport={sport} />}

      <SectionCard
        title={`${sport.toUpperCase()} Prop Picks`}
        subtitle={
          summary?.total > 0
            ? `${summary.wins}-${summary.losses} (${summary.win_pct}%) · ${summary.profit > 0 ? '+' : ''}$${summary.profit}`
            : 'No settled picks yet'
        }
      >
        {pending.length > 0 ? (
          <PicksTable rows={pending} />
        ) : (
          <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-8 text-center text-sm text-muted-foreground">
            No pending prop picks today.
          </div>
        )}
      </SectionCard>

      {finished.length > 0 && (
        <SectionCard title="Settled Props" subtitle={`Last ${finished.length}`}>
          <PicksTable rows={finished} settled />
        </SectionCard>
      )}

      {err && (
        <div className="rounded-md border border-negative/30 bg-negative/5 px-3 py-2 text-xs text-negative">
          {err}
        </div>
      )}
    </div>
  )
}


function PotdHero({ pick, sport }) {
  const odds = pick.odds
  const oddsStr = odds != null ? (odds > 0 ? `+${odds}` : String(odds)) : ''
  const edge = pick.edge != null ? `+${Number(pick.edge).toFixed(1)}%` : ''
  return (
    <section className="relative overflow-hidden rounded-2xl border border-primary/30 bg-gradient-to-br from-card via-card to-primary/[0.05] p-6">
      <div
        aria-hidden="true"
        className="absolute -right-20 -top-20 h-48 w-48 rounded-full bg-primary/10 blur-3xl"
      />
      <div className="relative">
        <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-primary">
          {sport.toUpperCase()} · Prop Pick of the Day
        </div>
        <div className="mt-2 flex items-baseline gap-3 flex-wrap">
          <h2 className="text-2xl font-bold tracking-tight text-foreground sm:text-3xl">
            {pick.player_name}
          </h2>
          <span className="text-lg font-semibold text-muted-foreground">
            {pick.bet_type}
          </span>
        </div>
        <div className="mt-1 text-base font-semibold text-foreground">
          {pick.pick} {oddsStr && (
            <span className="text-muted-foreground">({oddsStr})</span>
          )}
        </div>
        {pick.matchup && (
          <div className="mt-1 text-sm text-muted-foreground">
            {pick.matchup}
          </div>
        )}
        <div className="mt-4 grid grid-cols-3 gap-4 sm:max-w-md">
          <Stat label="Edge" value={edge} accent="positive" />
          <Stat label="Model" value={`${(pick.model_prob * 100).toFixed(0)}%`} />
          <Stat label="Confidence" value={(pick.confidence || 'lean').toUpperCase()} />
        </div>
        {pick.reasoning && (
          <p className="mt-4 max-w-2xl text-xs leading-relaxed text-muted-foreground italic">
            {pick.reasoning}
          </p>
        )}
      </div>
    </section>
  )
}


function Stat({ label, value, accent }) {
  return (
    <div>
      <div className="text-[10px] font-bold uppercase tracking-[0.18em] text-muted-foreground">
        {label}
      </div>
      <div className={cn(
        'mt-1 text-xl font-bold tabular-nums',
        accent === 'positive' ? 'text-positive' : 'text-foreground',
      )}>
        {value}
      </div>
    </div>
  )
}


function PicksTable({ rows, settled = false }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
          <tr>
            <th className="px-2 py-2 text-left">Player</th>
            <th className="px-2 py-2 text-left">Type</th>
            <th className="px-2 py-2 text-left">Pick</th>
            <th className="px-2 py-2 text-right">Odds</th>
            <th className="px-2 py-2 text-right">Model</th>
            <th className="px-2 py-2 text-right">Edge</th>
            <th className="px-2 py-2 text-left">Conf</th>
            {settled && <th className="px-2 py-2 text-right">Result</th>}
            {settled && <th className="px-2 py-2 text-right">P/L</th>}
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {rows.slice(0, 50).map(r => {
            const odds = r.odds
            const oddsStr = odds != null ? (odds > 0 ? `+${odds}` : String(odds)) : ''
            const conf = (r.confidence || 'lean').toLowerCase()
            const profitTone = r.profit > 0 ? 'text-positive'
                              : r.profit < 0 ? 'text-negative'
                              : 'text-muted-foreground'
            return (
              <tr key={r.id} className="hover:bg-accent/30">
                <td className="px-2 py-2 font-semibold text-foreground">{r.player_name}</td>
                <td className="px-2 py-2 text-muted-foreground">{r.bet_type}</td>
                <td className="px-2 py-2 text-foreground">{r.pick}</td>
                <td className="px-2 py-2 text-right tabular-nums text-muted-foreground">{oddsStr}</td>
                <td className="px-2 py-2 text-right tabular-nums">{(r.model_prob * 100).toFixed(0)}%</td>
                <td className="px-2 py-2 text-right tabular-nums text-positive">+{Number(r.edge).toFixed(1)}%</td>
                <td className="px-2 py-2">
                  <span className={cn(
                    'rounded-full px-1.5 py-0.5 text-[10px] font-bold uppercase',
                    CONF_BADGE[conf] || CONF_BADGE.lean,
                  )}>{conf}</span>
                </td>
                {settled && (
                  <td className={cn('px-2 py-2 text-right font-bold',
                    r.result === 'W' ? 'text-positive' :
                    r.result === 'L' ? 'text-negative' : 'text-muted-foreground'
                  )}>{r.result || '–'}</td>
                )}
                {settled && (
                  <td className={cn('px-2 py-2 text-right tabular-nums', profitTone)}>
                    {r.profit > 0 ? '+' : ''}${Number(r.profit || 0).toFixed(2)}
                  </td>
                )}
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
