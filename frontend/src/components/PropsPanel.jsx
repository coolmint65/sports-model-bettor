/**
 * PropsPanel — per-sport player-prop tab.
 *
 * Mirrors DerivativeTracker / FirstInningTracker structure exactly
 * so the three trackers read as siblings:
 *   - Header + Settle button
 *   - POTD card via shared <PotdHero> (player-headed for props)
 *   - Hero summary block (W-L, P/L, per-bet-type tiles)
 *   - Today's Picks: card grid grouped by matchup
 *   - Recent History: compact table
 *
 * Settlement performance leads the page so users can see "am I
 * winning props?" without scrolling past pending picks first.
 */

import { useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import PotdHero from './PotdHero'
import PropPickCard from './PropPickCard'
import { humanizeBetType } from '../lib/betType'
import { cn } from '../lib/utils'

const fmtMoney = n => `${n > 0 ? '+' : ''}$${(n || 0).toFixed(2)}`


export default function PropsPanel({ sport }) {
  const [potd, setPotd] = useState(null)
  const [summary, setSummary] = useState(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState(null)
  // Settle feedback: shows the count of newly-settled picks (or
  // explicit "nothing to settle" so the button isn't silent).
  // Auto-clears after 4s.
  const [settleMsg, setSettleMsg] = useState(null)
  const [settling, setSettling] = useState(false)

  const fetchAll = () => {
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
  }

  useEffect(fetchAll, [sport])

  const settleNow = async () => {
    setSettling(true)
    setSettleMsg(null)
    try {
      const r = await axios.post(`/api/${sport}/props-tracker/settle`)
      const d = r.data || {}
      const n = d.settled || 0
      const msg = n === 0
        ? 'Nothing to settle — no new game logs since last run.'
        : `Settled ${n} pick${n === 1 ? '' : 's'} · ${d.wins || 0}W / ${d.losses || 0}L / ${d.pushes || 0}P`
      setSettleMsg({ text: msg, ok: true })
      fetchAll()
    } catch (e) {
      setSettleMsg({ text: `Settle failed: ${e.message || 'request error'}`, ok: false })
    } finally {
      setSettling(false)
      setTimeout(() => setSettleMsg(null), 5000)
    }
  }

  // Always declare hooks before any early return -- React's
  // rules-of-hooks. See PropsPanel commit ecb7a0b.
  const pending = summary?.pending || []
  const finished = summary?.history || []

  // "Today's slate" rule: a matchup is in the slate while it still has
  // at least one pending pick. The moment every prop on that matchup
  // settles, the whole matchup drops out and falls into the compact
  // history table — that's the "day flips, push to history" cleanup
  // the user asked for. During a live game, settled props sit beside
  // their pending siblings so the user sees the green/red transitions;
  // after the final whistle nothing on a finished matchup lingers in
  // the slate.
  // Key liveness on (matchup + game_id). A playoff series reuses the
  // same matchup string ("DEN @ MIN") across games, and keying on just
  // the matchup would let game-N's pending picks drag game-(N-1)'s
  // settled cards along into today's slate. The pair is unique per
  // game so each series cleanly hands off.
  const matchupGameKey = (p) => `${p.matchup || ''}|${p.game_id || ''}`
  const liveGameKeys = useMemo(() => {
    const s = new Set()
    for (const p of pending) s.add(matchupGameKey(p))
    return s
  }, [pending])
  const todayCombined = useMemo(
    () => [
      ...pending,
      ...finished.filter(p => liveGameKeys.has(matchupGameKey(p))),
    ],
    [pending, finished, liveGameKeys],
  )
  // Compact-table history = settled picks whose game is no longer
  // live. Underlying SQL already sorts date DESC, id DESC so most
  // recent appears first.
  const olderHistory = useMemo(
    () => finished.filter(p => !liveGameKeys.has(matchupGameKey(p))),
    [finished, liveGameKeys],
  )

  const groupedToday = useMemo(() => {
    const m = new Map()
    for (const p of todayCombined) {
      const key = p.matchup || '?'
      if (!m.has(key)) m.set(key, [])
      m.get(key).push(p)
    }
    return [...m.entries()]
      .map(([matchup, picks]) => ({
        matchup,
        // Pending first (no result), then settled — within each block
        // sort by edge descending so the headline picks lead the card.
        picks: picks.slice().sort((a, b) => {
          const aPending = a.result == null ? 0 : 1
          const bPending = b.result == null ? 0 : 1
          if (aPending !== bPending) return aPending - bPending
          return (b.edge || 0) - (a.edge || 0)
        }),
      }))
      .sort((a, b) => (b.picks[0]?.edge || 0) - (a.picks[0]?.edge || 0))
  }, [todayCombined])

  const byBetType = useMemo(() => {
    const out = {}
    for (const r of finished) {
      const k = r.bet_type || '?'
      const s = out[k] || (out[k] = {wins: 0, losses: 0, pushes: 0, profit: 0, total: 0})
      s.total += 1
      if (r.result === 'W') s.wins += 1
      else if (r.result === 'L') s.losses += 1
      else if (r.result === 'P') s.pushes += 1
      s.profit += Number(r.profit || 0)
    }
    for (const k of Object.keys(out)) {
      const s = out[k]
      s.profit = Math.round(s.profit * 100) / 100
      s.win_pct = s.wins + s.losses > 0
        ? Math.round((s.wins / (s.wins + s.losses)) * 1000) / 10
        : 0
    }
    return out
  }, [finished])

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading props…
      </div>
    )
  }

  const total = summary?.total || 0
  const profit = summary?.profit || 0
  const profitTone = profit > 0 ? 'text-positive'
                    : profit < 0 ? 'text-negative' : 'text-foreground'
  const typesWithData = Object.entries(byBetType)
    .sort(([, a], [, b]) => b.total - a.total)

  return (
    <div className="space-y-5 py-4">
      {/* Header + actions — matches DerivativeTracker */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">
            {sport.toUpperCase()} Player Props
          </h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Per-player paper-bet log · {pending.length} pending · {total} settled
          </p>
        </div>
        <div className="flex items-center gap-3">
          {settleMsg && (
            <span className={cn(
              'rounded-md px-2.5 py-1 text-[11px] font-semibold transition-opacity',
              settleMsg.ok
                ? 'bg-positive/15 text-positive'
                : 'bg-negative/15 text-negative',
            )}>
              {settleMsg.text}
            </span>
          )}
          <button
            onClick={settleNow}
            disabled={settling}
            className={cn(
              'rounded-md px-3 py-1.5 text-xs font-semibold transition-opacity',
              settling
                ? 'bg-muted text-muted-foreground cursor-wait'
                : 'bg-positive-strong text-background hover:opacity-90',
            )}
          >
            {settling ? 'Settling…' : 'Settle completed'}
          </button>
        </div>
      </div>

      {/* Top-of-page P/L banner — props perform-or-don't is the headline
          metric, so this sits above POTD and the per-bet-type tiles on
          every sport. Always rendered (even when total=0) so the layout
          is identical across MLB / NHL / NBA — the user explicitly
          called out that the banner appearing on some tabs but not
          others felt inconsistent. */}
      <section className={cn(
        'rounded-xl border-2 p-6',
        total === 0 ? 'border-border bg-card'
          : profit > 0 ? 'border-positive/30 bg-positive/[0.06]'
          : profit < 0 ? 'border-negative/30 bg-negative/[0.06]'
          : 'border-border bg-card',
      )}>
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-widest text-muted-foreground">
              {sport.toUpperCase()} Player Props · Overall P/L
            </div>
            <div className={cn(
              'mt-1 text-5xl font-bold tabular-nums leading-none',
              total === 0 ? 'text-muted-foreground' : profitTone,
            )}>
              {fmtMoney(profit)}
            </div>
            <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted-foreground">
              <span className="tabular-nums font-semibold text-foreground">
                {summary?.wins || 0}-{summary?.losses || 0}
              </span>
              <span className="text-border">·</span>
              <span className="tabular-nums">{summary?.win_pct || 0}% WR</span>
              <span className="text-border">·</span>
              <span className="tabular-nums">{total} settled</span>
              {pending.length > 0 && (
                <>
                  <span className="text-border">·</span>
                  <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning font-semibold tabular-nums text-xs">
                    {pending.length} pending
                  </span>
                </>
              )}
            </div>
          </div>
        </div>

        {typesWithData.length > 0 && (
          <div className="mt-5 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-5">
            {typesWithData.map(([key, s]) => {
              const tone = s.profit > 0 ? 'text-positive'
                          : s.profit < 0 ? 'text-negative' : 'text-foreground'
              return (
                <div key={key} className="rounded-md border border-border bg-background/50 px-3 py-2.5">
                  <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground truncate">
                    {humanizeBetType(key)}
                  </div>
                  <div className={cn('mt-0.5 text-base font-semibold tabular-nums', tone)}>
                    {fmtMoney(s.profit)}
                  </div>
                  <div className="text-[10px] text-muted-foreground tabular-nums">
                    {s.wins}-{s.losses} ({s.win_pct}%)
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </section>

      {potd && (
        <PotdHero
          label={`${sport.toUpperCase()} · Prop Pick of the Day`}
          sport={sport}
          pick={potd}
          accent="primary"
        />
      )}

      {/* Today's Slate — single card grid that contains both pending
          and settled-today picks. PropPickCard transitions W/L/Push
          tint on settlement so the card visually flows from pending →
          coloured outcome without changing position. */}
      {groupedToday.length > 0 && (
        <section className="space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold text-foreground">Today's Slate</h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {todayCombined.length} pick{todayCombined.length === 1 ? '' : 's'}
            </span>
          </div>
          {groupedToday.map(g => (
            <div key={g.matchup} className="space-y-3">
              <div className="flex items-baseline justify-between gap-3">
                <h4 className="text-sm font-bold text-foreground tracking-tight">
                  {g.matchup}
                </h4>
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
        </section>
      )}

      {!groupedToday.length && (
        <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-8 text-center text-sm text-muted-foreground">
          {total === 0
            ? 'No prop picks recorded yet.'
            : 'No props on today\'s slate. History below.'}
        </div>
      )}

      {/* Older History — compact settled table for prior days. Always
          rendered so MLB / NHL / NBA share the same skeleton even
          before either has settled any prior-day picks. Empty-state
          fallback inside keeps the section header consistent. */}
      <section className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="flex items-center justify-between px-5 py-3 border-b border-border">
          <h3 className="text-sm font-semibold text-foreground">Recent History</h3>
          <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            {olderHistory.length} settled
          </span>
        </div>
        {olderHistory.length > 0 ? (
          <SettledTable rows={olderHistory} />
        ) : (
          <div className="px-5 py-8 text-center text-sm text-muted-foreground">
            No settled picks from prior days yet. Today's results will move here once tomorrow ticks over.
          </div>
        )}
      </section>

      {err && (
        <div className="rounded-md border border-negative/30 bg-negative/5 px-3 py-2 text-xs text-negative">
          {err}
        </div>
      )}
    </div>
  )
}


function SettledTable({ rows }) {
  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
          <tr>
            <th className="px-4 py-2 text-left">Date</th>
            <th className="px-2 py-2 text-left">Game</th>
            <th className="px-2 py-2 text-left">Player</th>
            <th className="px-2 py-2 text-left">Type</th>
            <th className="px-2 py-2 text-left">Pick</th>
            <th className="px-2 py-2 text-right">Odds</th>
            <th className="px-2 py-2 text-right">Edge</th>
            <th className="px-2 py-2 text-right">Result</th>
            <th className="px-4 py-2 text-right">P/L</th>
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
                <td className="px-4 py-2 text-xs text-muted-foreground tabular-nums whitespace-nowrap">
                  {r.date?.slice(5) || '–'}
                </td>
                <td className="px-2 py-2 text-muted-foreground whitespace-nowrap">
                  {r.matchup || '–'}
                </td>
                <td className="px-2 py-2 font-semibold text-foreground">{r.player_name}</td>
                <td className="px-2 py-2 text-muted-foreground">{humanizeBetType(r.bet_type)}</td>
                <td className="px-2 py-2 text-foreground">{r.pick}</td>
                <td className="px-2 py-2 text-right tabular-nums text-muted-foreground">{oddsStr}</td>
                <td className="px-2 py-2 text-right tabular-nums text-positive">+{Number(r.edge).toFixed(1)}%</td>
                <td className={cn('px-2 py-2 text-right font-bold',
                  r.result === 'W' ? 'text-positive' :
                  r.result === 'L' ? 'text-negative' : 'text-muted-foreground'
                )}>{r.result || '–'}</td>
                <td className={cn('px-4 py-2 text-right tabular-nums', profitTone)}>
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
