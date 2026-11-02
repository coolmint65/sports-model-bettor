import { useState, useEffect } from 'react'
import { cn } from '../lib/utils'

// Horses (PAPER, display-only, 2026-11-01). Reads GET /api/horses -- the backend proxies the Pi's
// /api/horses-v2, which serves ~/pibot/scanners/racing/horses-v2-paper.py's summary: FanDuel Racing
// PLACE/SHOW pool-value plays ("Dr. Z": discounted Harville from the win pool vs the place/show pools),
// graded at the official FDR payouts, $2 base. NOTHING PLACES. Not graduated.
// Tints use emerald/amber/red: `positive`/`warning` are plain hex vars, so `/NN` opacity on them never generates.

const usd = (x) => (x == null ? '—' : `${x < 0 ? '−' : x > 0 ? '+' : ''}$${Math.abs(x).toFixed(2)}`)
const pay = (x) => (x == null ? '—' : `$${Number(x).toFixed(2)}`)
const pct = (x, d = 0) => (x == null ? '—' : `${(100 * x).toFixed(d)}%`)
const roi = (x, se) => (x == null ? '—' : `${x > 0 ? '+' : ''}${(100 * x).toFixed(1)}%${se != null ? ` ± ${(100 * se).toFixed(1)}` : ''}`)
const money = (n) => {
  if (n == null) return '—'
  const v = Number(n)
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`
  return `$${v.toFixed(0)}`
}

function StatusPill({ p }) {
  if (p.status === 'won') return <span className="font-semibold text-emerald-400">WON {pay(p.paid)}</span>
  if (p.status === 'lost') return <span className="font-semibold text-red-400">LOST</span>
  if (p.status === 'void') return <span className="text-muted-foreground">void</span>
  if (p.status === 'live') return <span className="rounded-full bg-sky-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-sky-400">live · {p.mtp} mtp</span>
  return <span className="text-muted-foreground">pending</span>
}

function PlaysTable({ rows, live }) {
  if (!rows.length) return <div className="mt-3 rounded-md border border-dashed border-border px-4 py-6 text-center text-sm text-muted-foreground">{live ? 'No race inside 5 minutes to post shows value right now.' : 'No flagged plays yet today.'}</div>
  return (
    <div className="mt-3 overflow-x-auto">
      <table className="w-full text-xs">
        <thead className="text-[10px] uppercase tracking-wider text-muted-foreground">
          <tr className="border-b border-border text-left">
            <th className="py-1.5 pr-3 font-semibold">Track</th>
            <th className="pr-3 font-semibold">Race</th>
            <th className="pr-3 font-semibold">Post (ET)</th>
            <th className="pr-3 font-semibold">Horse</th>
            <th className="pr-3 font-semibold">Bet</th>
            <th className="pr-3 text-right font-semibold">Chance to cash</th>
            <th className="pr-3 text-right font-semibold">Est. pay ($2)</th>
            <th className="pr-3 text-right font-semibold">EV / $1</th>
            <th className="pr-3 text-right font-semibold">Win pool</th>
            <th className="text-right font-semibold">{live ? 'Now' : 'Result'}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((p) => (
            <tr key={`${p.race_key}-${p.bi}-${p.bet}`} className={cn('border-b border-border/50', !p.primary && 'opacity-60', p.stale && 'line-through decoration-muted-foreground/60')}>
              <td className="py-1.5 pr-3 font-medium text-foreground">{p.track}</td>
              <td className="pr-3 tabular-nums">R{p.race}</td>
              <td className="pr-3 tabular-nums text-muted-foreground">{p.post_et || '—'}</td>
              <td className="pr-3 text-foreground"><span className="mr-1.5 inline-block min-w-[1.5rem] rounded bg-muted px-1 text-center font-bold tabular-nums">{p.bi}</span>{p.horse || '?'}</td>
              <td className={cn('pr-3 font-semibold', p.bet === 'SHOW' ? 'text-sky-400' : 'text-violet-400')}>{p.bet}</td>
              <td className="pr-3 text-right tabular-nums">{pct(p.p_cash)}</td>
              <td className="pr-3 text-right tabular-nums">{pay(p.est_pay)}</td>
              <td className={cn('pr-3 text-right font-semibold tabular-nums', p.ev >= 1.1 ? 'text-emerald-400' : 'text-amber-400')}>{p.ev.toFixed(2)}</td>
              <td className="pr-3 text-right tabular-nums text-muted-foreground">{money(p.race_win_pool)}</td>
              <td className="text-right tabular-nums"><StatusPill p={p} />{p.stale && <span className="ml-1 text-[10px] text-muted-foreground">(stale snap · excluded)</span>}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function RecordTable({ rows }) {
  return (
    <div className="mt-3 overflow-x-auto">
      <table className="w-full text-xs">
        <thead className="text-[10px] uppercase tracking-wider text-muted-foreground">
          <tr className="border-b border-border text-left">
            <th className="py-1.5 pr-3 font-semibold">Rule</th>
            <th className="pr-3 text-right font-semibold">Bets</th>
            <th className="pr-3 text-right font-semibold">W-L</th>
            <th className="pr-3 text-right font-semibold">P/L ($2)</th>
            <th className="pr-3 text-right font-semibold">ROI ± SE</th>
            <th className="pr-3 text-right font-semibold">Cash rate vs break-even</th>
            <th className="pr-3 text-right font-semibold">Avg cash pay</th>
            <th className="text-right font-semibold">1st / 2nd half ROI</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.key} className="border-b border-border/50">
              <td className={cn('py-1.5 pr-3', r.label.includes('primary') ? 'font-semibold text-foreground' : 'text-muted-foreground')}>{r.label}</td>
              <td className="pr-3 text-right tabular-nums">{r.n}</td>
              <td className="pr-3 text-right tabular-nums">{r.n ? `${r.w}-${r.l}` : '—'}</td>
              <td className={cn('pr-3 text-right font-semibold tabular-nums', !r.n ? 'text-muted-foreground' : r.pl > 0 ? 'text-emerald-400' : r.pl < 0 ? 'text-red-400' : '')}>{r.n ? usd(r.pl) : '—'}</td>
              <td className={cn('pr-3 text-right tabular-nums', r.roi == null ? 'text-muted-foreground' : r.roi > 0 ? 'text-emerald-400' : 'text-red-400')}>{roi(r.roi, r.se)}</td>
              <td className="pr-3 text-right tabular-nums text-muted-foreground">{r.n ? `${pct(r.cash_rate, 1)} vs ${pct(r.be_cash_rate, 1)}` : '—'}</td>
              <td className="pr-3 text-right tabular-nums text-muted-foreground">{pay(r.avg_cash_pay)}</td>
              <td className="text-right tabular-nums text-muted-foreground">{r.first_half_roi == null ? '—' : `${roi(r.first_half_roi)} / ${roi(r.second_half_roi)}`}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function Section({ title, sub, children }) {
  return (
    <section className="rounded-xl border border-border bg-card p-5">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold text-foreground">{title}</h3>
        {sub && <span className="text-[11px] text-muted-foreground">{sub}</span>}
      </div>
      {children}
    </section>
  )
}

export default function HorsesPanel({ api }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)

  useEffect(() => {
    let alive = true
    const load = () => api.get('/horses')
      .then((r) => { if (alive) { setData(r.data); setErr(null) } })
      .catch((e) => { if (alive) setErr(e?.message || String(e)) })
    load()
    const tmr = setInterval(load, 60000)
    return () => { alive = false; clearInterval(tmr) }
  }, [api])

  if (err && !data) return <div className="py-12 text-center text-sm text-red-400">Horses unavailable: {err}</div>
  if (!data) {
    return (
      <div className="flex items-center justify-center gap-3 py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading horses…
      </div>
    )
  }

  const plays = data.plays_today || []
  const live = data.upcoming || []
  const cov = data.coverage
  const thr = data.primary_threshold ?? 1.1

  return (
    <div className="space-y-5 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">🐎 Horses · Place / Show pool value</h2>
          <p className="mt-0.5 text-xs text-muted-foreground">FanDuel Racing tote pools (US/CAN) · win pool → chance to finish top 2 / top 3 vs what the place/show pool pays · {data.stake_note || '$2 base'}</p>
        </div>
        <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
          {data.generated_at && <span className="tabular-nums">Updated {new Date(data.generated_at).toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' })} ET</span>}
          {data.stale && <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-400">stale</span>}
          <span className="rounded-full bg-amber-500/15 px-2.5 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-400">PAPER · not graduated</span>
        </div>
      </div>

      <div className="rounded-md border border-amber-500/30 bg-amber-500/[0.06] px-4 py-2.5 text-xs text-amber-300">
        PAPER ONLY. Nothing places. Plays are decided from the last pool snapshot before the off and graded at the official FDR payout.
        The win-pool model has no edge over the win pool itself; this only tests whether place/show pools are mispriced against it.
        Flagged = EV ≥ {thr.toFixed(2)} per $1 (rows at 1.05–{thr.toFixed(2)} are dimmed). Late money moves pools after the snapshot, so the estimated pay is not what will be paid.
      </div>

      {!data.ok && <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-6 text-center text-sm text-muted-foreground">{data.error || 'No data yet.'}</div>}
      {data.building && <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-6 text-center text-sm text-muted-foreground">Tracker is building its first summary…</div>}

      {data.ok && !data.building && (
        <>
          <Section title="Live now · inside 5 min to post" sub="current flags from the latest pool snapshot (not recorded until the off)">
            <PlaysTable rows={live} live />
          </Section>

          <Section title={`Today's flagged plays · ${data.today || ''}`} sub={`${plays.filter((p) => p.primary && !p.stale).length} at EV ≥ ${thr.toFixed(2)}`}>
            <PlaysTable rows={plays} />
          </Section>

          <Section title="Paper record · flagged rules" sub="graded at the paid FDR payouts · stale snapshots excluded">
            <RecordTable rows={data.record || []} />
          </Section>

          <Section title="Baselines" sub="same races, same grading: the bar the rules have to beat">
            <RecordTable rows={data.baselines || []} />
          </Section>

          {cov && (
            <div className="rounded-md border border-border bg-card/50 px-4 py-3 text-[11px] tabular-nums text-muted-foreground">
              Coverage today: {cov.today_races_usa_can} US/CAN races on the card · {cov.today_with_near_post_pools} with near-post pool snapshots · {cov.today_decided} decided ({cov.today_decided_fresh} with a snapshot ≤ {data.max_decision_mtp ?? 2} min to post) · {cov.today_graded} graded.
              {' '}All time: {cov.all_decided} decided, {cov.all_graded} graded since {cov.since || '—'}.
              {cov.late_money?.n > 0 && <div className="mt-1">Late money: final pools vs the decision snapshot (median of {cov.late_money.n} races) · win ×{cov.late_money.win_final_over_decision} · place ×{cov.late_money.place_final_over_decision ?? '—'}</div>}
              {data.backtest?.note && <div className="mt-1">{data.backtest.note}</div>}
            </div>
          )}
        </>
      )}
    </div>
  )
}
