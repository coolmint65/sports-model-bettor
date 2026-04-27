/**
 * FirstInningTracker — MLB-only tab for NRFI/YRFI picks.
 *
 * Mirrors DerivativeTracker structure exactly: header + Settle
 * button, POTD card via shared PotdHero, hero summary with
 * direction-tile breakdown (NRFI vs YRFI instead of derivative
 * bet types), Today's Picks + Recent History tables.
 */

import { useEffect, useMemo, useState } from 'react'
import axios from 'axios'
import PotdHero from './PotdHero'
import PicksTable from './primitives/PicksTable'
import { cn } from '../lib/utils'

const fiProfitFmt = n => `${n != null ? `$${n.toFixed(2)}` : '-'}`


export default function FirstInningTracker() {
  const [summary, setSummary] = useState(null)
  const [potd, setPotd] = useState(null)
  const [loading, setLoading] = useState(true)

  const fetchAll = () => {
    setLoading(true)
    const a = axios.create({ baseURL: '/api' })
    Promise.all([
      a.get('/mlb/first-inning-tracker/summary').then(r => r.data).catch(() => null),
      a.get('/mlb/first-inning-pick-of-day').then(r => r.data).catch(() => null),
    ]).then(([s, p]) => {
      setSummary(s)
      setPotd(p && !p.message ? p : null)
    }).finally(() => setLoading(false))
  }

  useEffect(fetchAll, [])

  const settleNow = async () => {
    // 1st INN settlement piggybacks on the main MLB tracker settler
    // since picks live in the picks table with bet_type='1st INN'.
    try {
      await axios.post('/api/mlb/tracker/settle')
      fetchAll()
    } catch (e) {
      console.warn('settle failed', e)
    }
  }

  const today = new Date().toISOString().slice(0, 10)
  const allPicks = useMemo(() => {
    if (!summary) return []
    return [...(summary.pending || []), ...(summary.history || [])]
  }, [summary])
  const todaysPicks = useMemo(
    () => allPicks.filter(p => p.date === today),
    [allPicks, today],
  )
  const pastPicks = useMemo(
    () => allPicks.filter(p => p.date !== today).slice(0, 50),
    [allPicks, today],
  )

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading 1st INN tracker…
      </div>
    )
  }

  const total = summary?.total || 0
  const profit = summary?.profit || 0
  const profitTone = profit > 0
    ? 'text-positive' : profit < 0 ? 'text-negative' : 'text-foreground'
  const byDir = summary?.by_direction || {}
  const dirKeys = ['NRFI', 'YRFI'].filter(d => byDir[d])

  return (
    <div className="space-y-5 py-4">
      {/* Header + actions — same shape as DerivativeTracker */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">
            MLB 1st Inning Tracker
          </h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            NRFI / YRFI paper-bet log · YRFI gated after −28% ROI deep dive
          </p>
        </div>
        <button
          onClick={settleNow}
          className="rounded-md bg-positive-strong px-3 py-1.5 text-xs font-semibold text-background hover:opacity-90 transition-opacity"
        >
          Settle completed
        </button>
      </div>

      {/* POTD callout — primary accent (mint) so it pairs with the
          core POTD card style. Derivative is amber, props is mint;
          1st INN gets mint here too since it's a sibling MLB feature. */}
      {potd && potd.matchup && (
        <PotdHero
          label="MLB · 1st Inning Pick of the Day"
          sport="mlb"
          pick={potd}
          accent="primary"
        />
      )}

      {/* Hero summary — mirrors DerivativeTracker's hero, with NRFI/
          YRFI direction tiles instead of bet-type tiles. */}
      {total > 0 && (
        <section className="rounded-lg border border-border bg-card p-5">
          <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
            1st Inning P/L
          </div>
          <div className={cn('mt-1 text-3xl font-bold tabular-nums', profitTone)}>
            {profit > 0 ? '+' : ''}${(profit || 0).toFixed(2)}
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
            <span className="tabular-nums">{summary?.wins || 0}-{summary?.losses || 0}</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">{summary?.win_pct || 0}% WR</span>
            <span className="text-border">·</span>
            <span className="tabular-nums">{total} picks</span>
            {(summary?.pending?.length || 0) > 0 && (
              <>
                <span className="text-border">·</span>
                <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning font-semibold tabular-nums">
                  {summary.pending.length} pending
                </span>
              </>
            )}
          </div>

          {dirKeys.length > 0 && (
            <div className="mt-4 grid grid-cols-2 gap-2 sm:max-w-md">
              {dirKeys.map(d => {
                const s = byDir[d]
                const tone = s.profit > 0
                  ? 'text-positive' : s.profit < 0 ? 'text-negative' : 'text-foreground'
                return (
                  <div
                    key={d}
                    className="rounded-md border border-border bg-background/50 px-3 py-2.5"
                  >
                    <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground truncate">
                      {d}
                    </div>
                    <div className={cn('mt-0.5 text-base font-semibold tabular-nums', tone)}>
                      {s.profit > 0 ? '+' : ''}${(s.profit || 0).toFixed(2)}
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
      )}

      {todaysPicks.length > 0 && (
        <section className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold text-foreground">
              Today's Picks
            </h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {todaysPicks.length} live
            </span>
          </div>
          <PicksTable
            picks={todaysPicks}
            typeColumnLabel="Direction"
            profitFormatter={fiProfitFmt}
          />
        </section>
      )}

      {pastPicks.length > 0 && (
        <section className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="flex items-center justify-between px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold text-foreground">
              Recent History
            </h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
              {pastPicks.length} settled
            </span>
          </div>
          <PicksTable
            picks={pastPicks}
            typeColumnLabel="Direction"
            profitFormatter={fiProfitFmt}
          />
        </section>
      )}

      {!total && !todaysPicks.length && (
        <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-8 text-center text-sm text-muted-foreground">
          No 1st INN picks recorded yet.
        </div>
      )}
    </div>
  )
}
