/**
 * FirstInningTracker — MLB-only Tracker section for NRFI/YRFI picks.
 *
 * Mirrors DerivativeTracker structure: POTD callout (using the
 * shared PotdHero with a distinct accent), summary header with
 * NRFI vs YRFI breakdown, and a settled history table. Reads from
 * /api/mlb/first-inning-tracker/summary which filters the picks
 * table by bet_type='1st INN'.
 *
 * YRFI is currently disabled in engine.config (see 2k-iii kill);
 * the by_direction breakdown still surfaces historical YRFI
 * results so users can see why the market got gated.
 */

import { useEffect, useState } from 'react'
import axios from 'axios'
import PotdHero from './PotdHero'
import PicksTable from './primitives/PicksTable'
import { cn } from '../lib/utils'

const fmtMoney = n => `${n > 0 ? '+' : ''}$${(n || 0).toFixed(2)}`


export default function FirstInningTracker() {
  const [summary, setSummary] = useState(null)
  const [potd, setPotd] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    const a = axios.create({ baseURL: '/api' })
    Promise.all([
      a.get('/mlb/first-inning-tracker/summary').then(r => r.data).catch(() => null),
      a.get('/mlb/first-inning-pick-of-day').then(r => r.data).catch(() => null),
    ]).then(([s, p]) => {
      setSummary(s)
      setPotd(p && !p.message ? p : null)
    }).finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-8 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading 1st INN tracker…
      </div>
    )
  }

  const pending = summary?.pending || []
  const finished = summary?.history || []
  const total = summary?.total || 0
  const profit = summary?.profit || 0
  const profitTone = profit > 0 ? 'text-positive' : profit < 0 ? 'text-negative' : 'text-foreground'
  const byDir = summary?.by_direction || {}

  return (
    <div className="space-y-5 py-4">
      <div>
        <h2 className="text-xl font-semibold tracking-tight text-foreground">
          MLB 1st Inning Tracker
        </h2>
        <p className="mt-0.5 text-xs text-muted-foreground">
          NRFI / YRFI paper-bet log · YRFI gated after −28% ROI deep dive
        </p>
      </div>

      {/* POTD — green accent so it visually pairs with the core POTD
          but stays distinct from the derivative amber card. */}
      {potd && (
        <PotdHero
          label="MLB · 1st Inning Pick of the Day"
          sport="mlb"
          pick={potd}
          accent="primary"
        />
      )}

      {/* Hero summary with NRFI / YRFI direction breakdown */}
      <section className="rounded-lg border border-border bg-card p-5">
        <div className="text-[10px] font-semibold uppercase tracking-widest text-muted-foreground">
          1st Inning P/L
        </div>
        <div className={cn('mt-1 text-3xl font-bold tabular-nums', profitTone)}>
          {fmtMoney(profit)}
        </div>
        <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-xs text-muted-foreground">
          <span className="tabular-nums">{summary?.wins || 0}-{summary?.losses || 0}</span>
          <span className="text-border">·</span>
          <span className="tabular-nums">{summary?.win_pct || 0}% WR</span>
          <span className="text-border">·</span>
          <span className="tabular-nums">{total} picks</span>
          {pending.length > 0 && (
            <>
              <span className="text-border">·</span>
              <span className="rounded-full bg-warning/15 px-2 py-0.5 text-warning font-semibold tabular-nums">
                {pending.length} pending
              </span>
            </>
          )}
        </div>

        {Object.keys(byDir).length > 0 && (
          <div className="mt-4 grid grid-cols-2 gap-2 sm:max-w-md">
            {['NRFI', 'YRFI'].filter(d => byDir[d]).map(d => {
              const s = byDir[d]
              const tone = s.profit > 0 ? 'text-positive'
                          : s.profit < 0 ? 'text-negative' : 'text-foreground'
              return (
                <div key={d} className="rounded-md border border-border bg-background/50 px-3 py-2.5">
                  <div className="flex items-center justify-between">
                    <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{d}</div>
                    <span className={cn('text-base font-semibold tabular-nums', tone)}>{fmtMoney(s.profit)}</span>
                  </div>
                  <div className="mt-1 text-[11px] text-muted-foreground tabular-nums">
                    {s.wins}-{s.losses} · {s.win_pct}% WR · {s.total} picks
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </section>

      {/* Pending */}
      {pending.length > 0 && (
        <section>
          <h3 className="mb-2 text-sm font-semibold text-foreground">Pending</h3>
          <PicksTable picks={pending} />
        </section>
      )}

      {/* History */}
      {finished.length > 0 && (
        <section>
          <h3 className="mb-2 text-sm font-semibold text-foreground">Settled history</h3>
          <PicksTable picks={finished} />
        </section>
      )}

      {!total && !pending.length && (
        <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-8 text-center text-sm text-muted-foreground">
          No 1st INN picks recorded yet.
        </div>
      )}
    </div>
  )
}
