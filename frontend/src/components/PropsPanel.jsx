/**
 * PropsPanel — per-sport player-prop picks display.
 *
 * Renders today's POTD via the shared <PotdHero>, then today's
 * pending picks as a card grid grouped by matchup (one
 * <PropPickCard> per pick), then settled history as a compact
 * table. Card layout matches the GameCard visual language so the
 * Props tab reads as a sibling of the Bets tab rather than a
 * spreadsheet.
 */

import { useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import PotdHero from './PotdHero'
import PropPickCard from './PropPickCard'
import SectionCard from './primitives/SectionCard'
import { cn } from '../lib/utils'


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

  // Group pending picks by matchup so the UI reads as
  // "DEN @ MIN: 5 picks" rather than 290 cards in a wall.
  const grouped = useMemo(() => {
    const m = new Map()
    for (const p of pending) {
      const key = p.matchup || '?'
      if (!m.has(key)) m.set(key, [])
      m.get(key).push(p)
    }
    // Sort matchups by max edge inside each group; sort picks within
    // by edge desc so the strongest play in each game leads the card.
    return [...m.entries()]
      .map(([matchup, picks]) => ({
        matchup,
        picks: picks.slice().sort((a, b) => (b.edge || 0) - (a.edge || 0)),
      }))
      .sort((a, b) => (b.picks[0]?.edge || 0) - (a.picks[0]?.edge || 0))
  }, [pending])

  return (
    <div className="space-y-5 py-4">
      {potd && (
        <PotdHero
          label={`${sport.toUpperCase()} · Prop Pick of the Day`}
          sport={sport}
          pick={potd}
          accent="primary"
        />
      )}

      <SectionCard
        title={`${sport.toUpperCase()} Prop Picks`}
        subtitle={
          summary?.total > 0
            ? `${summary.wins}-${summary.losses} (${summary.win_pct}%) · ${summary.profit > 0 ? '+' : ''}$${summary.profit}`
            : 'No settled picks yet'
        }
      >
        {grouped.length > 0 ? (
          <div className="space-y-6">
            {grouped.map(g => (
              <div key={g.matchup} className="space-y-3">
                <div className="flex items-baseline justify-between gap-3">
                  <h3 className="text-sm font-bold text-foreground tracking-tight">
                    {g.matchup}
                  </h3>
                  <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    {g.picks.length} pick{g.picks.length === 1 ? '' : 's'}
                  </span>
                </div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 xl:grid-cols-3">
                  {g.picks.map(p => (
                    <PropPickCard key={p.id} pick={p} sport={sport} />
                  ))}
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-8 text-center text-sm text-muted-foreground">
            No pending prop picks today.
          </div>
        )}
      </SectionCard>

      {finished.length > 0 && (
        <SectionCard title="Settled History" subtitle={`Last ${finished.length}`}>
          <SettledTable rows={finished} />
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


/**
 * Settled-history table — kept as a compact table since reviewing
 * past results doesn't need the visual richness of card layout.
 * Card grid for pending; table for history.
 */
function SettledTable({ rows }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
          <tr>
            <th className="px-2 py-2 text-left">Player</th>
            <th className="px-2 py-2 text-left">Type</th>
            <th className="px-2 py-2 text-left">Pick</th>
            <th className="px-2 py-2 text-right">Odds</th>
            <th className="px-2 py-2 text-right">Edge</th>
            <th className="px-2 py-2 text-right">Result</th>
            <th className="px-2 py-2 text-right">P/L</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {rows.slice(0, 50).map(r => {
            const odds = r.odds
            const oddsStr = odds != null ? (odds > 0 ? `+${odds}` : String(odds)) : ''
            const profitTone = r.profit > 0 ? 'text-positive'
                              : r.profit < 0 ? 'text-negative'
                              : 'text-muted-foreground'
            return (
              <tr key={r.id} className="hover:bg-accent/30">
                <td className="px-2 py-2 font-semibold text-foreground">{r.player_name}</td>
                <td className="px-2 py-2 text-muted-foreground">{r.bet_type}</td>
                <td className="px-2 py-2 text-foreground">{r.pick}</td>
                <td className="px-2 py-2 text-right tabular-nums text-muted-foreground">{oddsStr}</td>
                <td className="px-2 py-2 text-right tabular-nums text-positive">+{Number(r.edge).toFixed(1)}%</td>
                <td className={cn('px-2 py-2 text-right font-bold',
                  r.result === 'W' ? 'text-positive' :
                  r.result === 'L' ? 'text-negative' : 'text-muted-foreground'
                )}>{r.result || '–'}</td>
                <td className={cn('px-2 py-2 text-right tabular-nums', profitTone)}>
                  {r.profit > 0 ? '+' : ''}${Number(r.profit || 0).toFixed(2)}
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}
