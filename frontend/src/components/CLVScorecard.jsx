/**
 * CLVScorecard — the proof-of-edge view.
 *
 * Closing Line Value across every sport x pick-type: do our picks beat the
 * price the market closes at? Positive CLV = we got a better number than the
 * close = the one edge signal that holds up in hundreds of bets (win/loss is
 * variance you don't control; beating the close is the skill). "Clean" = the
 * close was captured with a verified timestamp (the honest number); the rest
 * lean on historical closes and are directional only.
 *
 * Data: GET /api/clv/report (engine.clv_report.clv_report). Read-only.
 */
import { useEffect, useMemo, useState } from 'react'
import { cn } from '../lib/utils'

const VIG = 53   // beat% must clear ~53% to beat the vig on a coinflip market

function pctColor(v) {
  if (v == null) return 'text-muted-foreground'
  return v > 0 ? 'text-positive' : v < 0 ? 'text-negative' : 'text-muted-foreground'
}
function beatColor(v) {
  if (v == null) return 'text-muted-foreground'
  return v >= VIG ? 'text-positive' : v < 47 ? 'text-negative' : 'text-foreground'
}
const fpct = (v, d = 1) => (v == null ? '—' : `${v > 0 ? '+' : ''}${v.toFixed(d)}%`)
const fbeat = (v) => (v == null ? '—' : `${v.toFixed(1)}%`)
const SPORT_LABEL = { basketball_intl: 'BBALL (INTL)', hockey_minor: 'HOCKEY (MINOR)' }
const lbl = (s) => SPORT_LABEL[s] || s.toUpperCase().replace(/_/g, ' ')

export default function CLVScorecard({ api }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [err, setErr] = useState(false)
  const [sort, setSort] = useState({ col: 'clean_beat_pct', dir: -1 })
  const [minN, setMinN] = useState(20)

  useEffect(() => {
    setLoading(true); setErr(false)
    api.get('/clv/report')
      .then(r => setData(r.data))
      .catch(() => setErr(true))
      .finally(() => setLoading(false))
  }, [api])

  // Flatten by_sport -> by_type -> by_market into one sortable row list.
  const marketRows = useMemo(() => {
    if (!data?.by_sport) return []
    const rows = []
    for (const [sport, v] of Object.entries(data.by_sport)) {
      for (const [ptype, tv] of Object.entries(v.by_type || {})) {
        for (const m of tv.by_market || []) {
          rows.push({ sport, ptype, ...m })
        }
      }
    }
    return rows
  }, [data])

  const sortedMarkets = useMemo(() => {
    const rows = marketRows.filter(r => r.n >= minN)
    const { col, dir } = sort
    return [...rows].sort((a, b) => {
      const av = a[col], bv = b[col]
      if (av == null && bv == null) return 0
      if (av == null) return 1              // nulls always last
      if (bv == null) return -1
      return (av - bv) * dir
    })
  }, [marketRows, sort, minN])

  const sportRows = useMemo(() => {
    if (!data?.by_sport) return []
    return Object.entries(data.by_sport)
      .map(([sport, v]) => ({ sport, ...v.overall }))
      .sort((a, b) => b.n - a.n)
  }, [data])

  if (loading) {
    return <div className="p-6 text-sm text-muted-foreground">Loading CLV scorecard…</div>
  }
  if (err || !data) {
    return <div className="p-6 text-sm text-negative">Couldn’t load the CLV report.</div>
  }

  const o = data.overall
  const hasClean = o.clean_n > 0
  const heroBeat = hasClean ? o.clean_beat_pct : o.beat_pct
  const heroClv = hasClean ? o.clean_avg_clv_pct : o.avg_clv_pct
  let verdict, vtone
  if (o.clean_n < 30) {
    verdict = `Only ${o.clean_n} verified-close picks so far — the numbers below lean on historical closes, which near game-time can be contaminated. Directional until clean data builds. Hunt the green rows.`
    vtone = 'warn'
  } else if (heroBeat >= VIG) {
    verdict = 'Beating the close on verified data — a real edge signal. Concentrate where the clean numbers are green.'
    vtone = 'good'
  } else {
    verdict = 'Not beating the close overall — the market is sharper than the model on average. The edge, if any, is in the specific rows below that stay green on clean data.'
    vtone = 'bad'
  }
  const vClass = vtone === 'good' ? 'border-positive/40 text-positive bg-positive/[0.05]'
    : vtone === 'bad' ? 'border-negative/40 text-negative bg-negative/[0.05]'
    : 'border-amber-500/40 text-amber-500 bg-amber-500/[0.05]'

  const SortTh = ({ col, children, cls }) => (
    <th className={cn('cursor-pointer select-none px-3 py-2 text-right hover:text-foreground', cls)}
        onClick={() => setSort(s => ({ col, dir: s.col === col ? -s.dir : -1 }))}>
      {children}{sort.col === col ? (sort.dir < 0 ? ' ↓' : ' ↑') : ''}
    </th>
  )

  return (
    <section className="mx-auto max-w-5xl space-y-5 p-4 md:p-6">
      <div>
        <h1 className="text-xl font-bold tracking-tight text-foreground">CLV Scorecard</h1>
        <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
          Do our picks beat the closing line? Positive CLV = a better price than the market’s
          close — the one edge signal that survives variance. <span className="text-amber-500">Clean</span> =
          verified pre-game close only (the honest number).
        </p>
      </div>

      {/* Hero */}
      <div className="rounded-xl border border-border bg-card p-5">
        <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
          {[
            { k: `Beat the close ${hasClean ? '(clean)' : '(historical)'}`, v: fbeat(heroBeat), c: beatColor(heroBeat), big: true },
            { k: `Avg CLV ${hasClean ? '(clean)' : '(historical)'}`, v: fpct(heroClv, 2), c: pctColor(heroClv), big: true },
            { k: 'Clean sample', v: o.clean_n, c: 'text-foreground' },
            { k: 'Total settled', v: o.n, c: 'text-foreground' },
          ].map((t, i) => (
            <div key={i}>
              <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{t.k}</div>
              <div className={cn('mt-1 font-bold tabular-nums', t.big ? 'text-3xl' : 'text-2xl', t.c)}>{t.v}</div>
            </div>
          ))}
        </div>
        <div className={cn('mt-4 rounded-lg border px-4 py-3 text-sm', vClass)}>{verdict}</div>
      </div>

      {/* By sport */}
      <div>
        <h2 className="mb-2 px-1 text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">By sport</h2>
        <div className="overflow-x-auto rounded-xl border border-border bg-card">
          <table className="w-full text-sm">
            <thead className="bg-muted/40 text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-3 py-2 text-left">Sport</th>
                <th className="px-3 py-2 text-right">n</th>
                <th className="px-3 py-2 text-right">CLV</th>
                <th className="px-3 py-2 text-right">Beat%</th>
                <th className="px-3 py-2 text-right">ROI</th>
                <th className="border-l border-amber-500/30 px-3 py-2 text-right">clean n</th>
                <th className="px-3 py-2 text-right">clean CLV</th>
                <th className="px-3 py-2 text-right">clean beat%</th>
              </tr>
            </thead>
            <tbody>
              {sportRows.map(r => (
                <tr key={r.sport} className="border-t border-border/60">
                  <td className="px-3 py-2 font-semibold text-foreground">{lbl(r.sport)}</td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">{r.n}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums font-medium', pctColor(r.avg_clv_pct))}>{fpct(r.avg_clv_pct, 2)}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums font-semibold', beatColor(r.beat_pct))}>{fbeat(r.beat_pct)}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums', pctColor(r.roi_pct))}>{fpct(r.roi_pct)}</td>
                  <td className="border-l border-amber-500/20 px-3 py-2 text-right tabular-nums text-muted-foreground">{r.clean_n}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums', pctColor(r.clean_avg_clv_pct))}>{fpct(r.clean_avg_clv_pct, 2)}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums font-semibold', beatColor(r.clean_beat_pct))}>{fbeat(r.clean_beat_pct)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* By market — the edge hunt */}
      <div>
        <div className="mb-2 flex items-center justify-between px-1">
          <h2 className="text-[11px] font-semibold uppercase tracking-wider text-muted-foreground">
            Every market · which ones beat the close
          </h2>
          <label className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
            min n
            <select value={minN} onChange={e => setMinN(Number(e.target.value))}
                    className="rounded border border-border bg-card px-1.5 py-0.5 text-foreground">
              {[1, 10, 20, 50].map(v => <option key={v} value={v}>{v}</option>)}
            </select>
          </label>
        </div>
        <div className="overflow-x-auto rounded-xl border border-border bg-card">
          <table className="w-full text-sm">
            <thead className="bg-muted/40 text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr>
                <th className="px-3 py-2 text-left">Market</th>
                <SortTh col="n">n</SortTh>
                <SortTh col="avg_clv_pct">CLV</SortTh>
                <SortTh col="beat_pct">Beat%</SortTh>
                <SortTh col="roi_pct">ROI</SortTh>
                <SortTh col="clean_n" cls="border-l border-amber-500/30">clean n</SortTh>
                <SortTh col="clean_beat_pct">clean beat%</SortTh>
              </tr>
            </thead>
            <tbody>
              {sortedMarkets.map((r, i) => (
                <tr key={`${r.sport}-${r.ptype}-${r.market}-${i}`}
                    className={cn('border-t border-border/60', r.n < 20 && 'opacity-55')}>
                  <td className="px-3 py-2">
                    <span className="text-muted-foreground">{lbl(r.sport)}</span>
                    <span className="mx-1 text-muted-foreground/40">·</span>
                    <span className="text-[11px] uppercase tracking-wide text-muted-foreground/70">{r.ptype}</span>
                    <span className="mx-1 text-muted-foreground/40">·</span>
                    <span className="font-medium text-foreground">{r.market}</span>
                  </td>
                  <td className="px-3 py-2 text-right tabular-nums text-muted-foreground">{r.n}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums font-medium', pctColor(r.avg_clv_pct))}>{fpct(r.avg_clv_pct, 2)}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums font-semibold', beatColor(r.beat_pct))}>{fbeat(r.beat_pct)}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums', pctColor(r.roi_pct))}>{fpct(r.roi_pct)}</td>
                  <td className="border-l border-amber-500/20 px-3 py-2 text-right tabular-nums text-muted-foreground">{r.clean_n}</td>
                  <td className={cn('px-3 py-2 text-right tabular-nums font-semibold', beatColor(r.clean_beat_pct))}>{fbeat(r.clean_beat_pct)}</td>
                </tr>
              ))}
              {sortedMarkets.length === 0 && (
                <tr><td colSpan={7} className="px-3 py-6 text-center text-muted-foreground">No markets at min n = {minN}.</td></tr>
              )}
            </tbody>
          </table>
        </div>
        <p className="mt-2 px-1 text-[11px] italic text-muted-foreground">
          Rows under n=20 are dimmed (small samples are noise). A real edge needs beat% north of ~{VIG}%
          on clean data. Sorted by {sort.col}. Loaded {data.meta?.sources_loaded} tables.
        </p>
      </div>
    </section>
  )
}
