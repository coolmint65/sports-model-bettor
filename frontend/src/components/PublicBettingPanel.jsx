import { useState, useEffect, useMemo } from 'react'
import { cn } from '../lib/utils'

// Public Betting (display-only, 2026-11-01). Reads GET /api/public-betting -- the backend proxies the
// Pi's /api/hr-public, which reads the Hard Rock "Follow or Fade" snapshots logged every 30 min by
// ~/pibot/scanners/hr-public-snapshot.py. Bet COUNTS per side (not money). NO bets.
// Tints use emerald/amber: `positive`/`warning` are plain hex vars, so `/NN` opacity on them never generates.

const SPORT_ICON = { AMERICAN_FOOTBALL: '🏈', BASKETBALL: '🏀', ICE_HOCKEY: '🏒', BASEBALL: '⚾', SOCCER: '⚽', TENNIS: '🎾', TABLE_TENNIS: '🏓', MMA: '🥊', GOLF: '⛳' }
const icon = (s) => SPORT_ICON[s] || '•'
const num = (n) => (n == null ? '—' : Number(n).toLocaleString('en-US'))
const money = (n) => {
  if (n == null) return '—'
  const v = Number(n)
  if (v >= 1e6) return `$${(v / 1e6).toFixed(2)}M`
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`
  return `$${v.toFixed(0)}`
}
const odds = (o) => (o == null ? '' : o > 0 ? `+${o}` : `${o}`)
const signed = (x) => (x == null ? '' : x > 0 ? `+${x}` : `${x}`)
const pct1 = (x) => (x == null ? '—' : `${x.toFixed(1)}%`)

const ET_DAY = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' })
const fmtClock = (d) => d.toLocaleString('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' })
function fmtStart(ms) {
  if (ms == null) return ''
  const d = new Date(ms)
  const now = Date.now()
  const k = ET_DAY.format(d)
  if (k === ET_DAY.format(now)) return `Today ${fmtClock(d)}`
  if (k === ET_DAY.format(now + 86400000)) return `Tomorrow ${fmtClock(d)}`
  return `${d.toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric' })}, ${fmtClock(d)}`
}
function fmtTs(sec) {
  if (!sec) return 'never'
  return new Date(sec * 1000).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'numeric', day: 'numeric', hour: 'numeric', minute: '2-digit' })
}

// "Miami Dolphins" + 7 -> "Miami Dolphins +7"; puck-line names already carry the handicap.
function sideLabel(name, line) {
  if (/[+-]\d+(\.\d+)?\s*$/.test(name || '')) return name
  return line == null ? name : `${name} ${signed(line)}`
}

// pctA series for one spread, restricted to today's (ET) snapshots when there are any.
function series(hist, r) {
  const pts = (hist && hist[`${r.eventId}|${r.marketId}`]) || []
  const today = ET_DAY.format(Date.now())
  const t = pts.filter((p) => ET_DAY.format(p[0] * 1000) === today && p[1] != null)
  return t.length ? t : pts.filter((p) => p[1] != null)
}

function Spark({ pts, flip }) {
  if (!pts || pts.length < 2) return null
  const W = 64, H = 18
  const ys = pts.map((p) => (flip ? 1 - p[1] : p[1]))
  const lo = Math.min(...ys), hi = Math.max(...ys)
  const span = Math.max(hi - lo, 0.02)
  const d = ys.map((y, i) => `${((i / (ys.length - 1)) * W).toFixed(1)},${(H - 2 - ((y - lo) / span) * (H - 4)).toFixed(1)}`).join(' ')
  return (
    <svg width={W} height={H} className="text-muted-foreground" aria-hidden="true">
      <polyline points={d} fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinejoin="round" />
    </svg>
  )
}

function SpreadCard({ r, hist }) {
  const a = r.pctA ?? 0.5
  const aPub = a >= 0.5
  const pubPct = 100 * Math.max(a, 1 - a)
  const pts = series(hist, r)
  const first = pts.length ? pts[0][1] : null
  // change in the PUBLIC side's share since its first snapshot today (points of share)
  const delta = first != null && pts.length > 1 ? 100 * (aPub ? a - first : first - a) : null
  const heavy = pubPct >= 70 ? 'border-amber-500/40 bg-amber-500/[0.05]' : pubPct >= 65 ? 'border-amber-500/20' : 'border-border'
  const rows = [
    { name: sideLabel(r.selA_name, r.line), o: r.selA_odds, p: a, bets: r.betsA, pub: aPub },
    { name: sideLabel(r.selB_name, r.line == null ? null : -r.line), o: r.selB_odds, p: 1 - a, bets: r.betsB, pub: !aPub },
  ]
  return (
    <div className={cn('rounded-lg border bg-card p-4', heavy)}>
      <div className="flex items-center justify-between gap-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
        <span className="truncate">{icon(r.sport)} {r.comp}</span>
        <span className="flex-shrink-0 tabular-nums">{fmtStart(r.eventTime)}</span>
      </div>
      <div className="mt-3 space-y-2.5">
        {rows.map((s, i) => (
          <div key={i}>
            <div className="flex items-baseline justify-between gap-2 text-sm">
              <span className={cn('truncate', s.pub ? 'font-semibold text-foreground' : 'text-muted-foreground')}>
                {s.name} <span className="text-xs tabular-nums text-muted-foreground">{odds(s.o)}</span>
              </span>
              <span className={cn('flex-shrink-0 tabular-nums', s.pub ? 'font-bold text-amber-400' : 'text-muted-foreground')}>{(100 * s.p).toFixed(0)}%</span>
            </div>
            <div className="mt-1 h-1.5 w-full overflow-hidden rounded-full bg-muted">
              <div className={cn('h-full rounded-full', s.pub ? 'bg-amber-500' : 'bg-slate-500')} style={{ width: `${(100 * s.p).toFixed(1)}%` }} />
            </div>
          </div>
        ))}
      </div>
      <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-[11px] text-muted-foreground">
        <span className="tabular-nums">{num(r.totalBets)} bets · {num(r.numUsers)} users</span>
        <span className="flex items-center gap-2 tabular-nums">
          <Spark pts={pts} flip={!aPub} />
          {delta == null ? <span>first look</span> : (
            <span className={cn(Math.abs(delta) < 0.5 ? 'text-muted-foreground' : delta > 0 ? 'text-amber-400' : 'text-sky-400')}
              title={`public side share change since its first snapshot today (${pts.length} snapshots)`}>
              {delta > 0 ? '▲' : delta < 0 ? '▼' : '•'} {Math.abs(delta).toFixed(1)} pts today
            </span>
          )}
        </span>
      </div>
    </div>
  )
}

function RecordLine({ label, s }) {
  const edge = s.winPct != null && s.breakEvenPct != null ? s.winPct - s.breakEvenPct : null
  return (
    <div className="flex flex-wrap items-baseline justify-between gap-x-4 gap-y-1 py-1.5 text-sm">
      <span className="w-24 font-medium text-foreground">{label}</span>
      <span className="tabular-nums text-muted-foreground">{s.w}-{s.l}{s.p ? `-${s.p}` : ''}</span>
      <span className="tabular-nums">
        <span className={cn('font-semibold', edge == null ? 'text-muted-foreground' : edge > 0 ? 'text-emerald-400' : 'text-red-400')}>{pct1(s.winPct)}</span>
        <span className="text-muted-foreground"> vs {pct1(s.breakEvenPct)} BE</span>
      </span>
      <span className={cn('w-20 text-right tabular-nums', s.roiPct == null ? 'text-muted-foreground' : s.roiPct > 0 ? 'text-emerald-400' : 'text-red-400')}>
        {s.roiPct == null ? '—' : `${s.roiPct > 0 ? '+' : ''}${s.roiPct.toFixed(1)}% ROI`}
      </span>
    </div>
  )
}

function RecordCard({ record }) {
  const th = record?.thresholds || {}
  const games = record?.games || []
  return (
    <section className="rounded-xl border-2 border-border bg-card p-5">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold text-foreground">Fade the public · spread record</h3>
        <span className="text-[11px] tabular-nums text-muted-foreground">
          {record ? `${record.graded} graded · ${record.pending} pending · last pre-game snapshot per game, ESPN finals` : 'no record yet'}
        </span>
      </div>
      <p className="mt-1 text-[11px] text-muted-foreground">Graded as win% vs the break-even of the price actually offered, plus flat-1u ROI.</p>
      <div className="mt-3 grid grid-cols-1 gap-4 lg:grid-cols-2">
        {['65', '70'].map((k) => (
          <div key={k} className="rounded-md border border-border bg-background/50 px-4 py-3">
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Public ≥ {k}% of bets · n={th[k]?.public?.n ?? 0}</div>
            {th[k] ? (
              <div className="mt-1 divide-y divide-border">
                <RecordLine label="Follow" s={th[k].public} />
                <RecordLine label="Fade" s={th[k].fade} />
              </div>
            ) : <div className="mt-2 text-sm text-muted-foreground">—</div>}
          </div>
        ))}
      </div>
      {games.length > 0 && (
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr className="border-b border-border text-left">
                <th className="py-1.5 pr-3 font-semibold">Date</th><th className="pr-3 font-semibold">Game</th>
                <th className="pr-3 font-semibold">Public side</th><th className="pr-3 text-right font-semibold">%</th>
                <th className="pr-3 font-semibold">Final</th><th className="text-right font-semibold">Public</th>
              </tr>
            </thead>
            <tbody>
              {games.slice(0, 20).map((g) => (
                <tr key={g.eventId} className="border-b border-border/50">
                  <td className="py-1.5 pr-3 tabular-nums text-muted-foreground">{new Date(g.eventTime).toLocaleDateString('en-US', { timeZone: 'America/New_York', month: 'numeric', day: 'numeric' })}</td>
                  <td className="pr-3 text-muted-foreground">{g.comp} · {g.selA} v {g.selB}</td>
                  <td className="pr-3 text-foreground">{sideLabel(g.public, g.public === g.selA ? g.line : -g.line)} <span className="text-muted-foreground">{odds(g.publicOdds)}</span></td>
                  <td className="pr-3 text-right tabular-nums">{g.publicPct.toFixed(0)}%</td>
                  <td className="pr-3 tabular-nums text-muted-foreground">{g.score}</td>
                  <td className={cn('text-right font-semibold', g.publicResult === 'W' ? 'text-emerald-400' : g.publicResult === 'L' ? 'text-red-400' : 'text-muted-foreground')}>{g.publicResult}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}

function ListCard({ title, children, empty }) {
  return (
    <section className="rounded-lg border border-border bg-card p-4">
      <h3 className="text-sm font-semibold text-foreground">{title}</h3>
      {empty ? <div className="mt-3 text-xs text-muted-foreground">Nothing in the latest snapshot.</div> : <div className="mt-2 divide-y divide-border/60">{children}</div>}
    </section>
  )
}

function SelRow({ r, eventName }) {
  return (
    <div className="flex items-start justify-between gap-3 py-2 text-sm">
      <div className="min-w-0">
        <div className="truncate text-foreground">{r.name} <span className="text-xs tabular-nums text-muted-foreground">{odds(r.odds)}</span></div>
        <div className="truncate text-[11px] text-muted-foreground">{icon(r.sport)} {eventName || r.comp} · {r.market}{r.eventTime ? ` · ${fmtStart(r.eventTime)}` : ''}</div>
      </div>
      <div className="flex-shrink-0 text-right text-[11px] tabular-nums text-muted-foreground">
        <div className="text-sm font-semibold text-foreground">{num(r.numberOfBets)}</div>bets
      </div>
    </div>
  )
}

export default function PublicBettingPanel({ api }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)

  useEffect(() => {
    let alive = true
    const load = () => api.get('/public-betting')
      .then((r) => { if (alive) { setData(r.data); setErr(null) } })
      .catch((e) => { if (alive) setErr(e?.message || String(e)) })
    load()
    const tmr = setInterval(load, 120000)
    return () => { alive = false; clearInterval(tmr) }
  }, [api])

  const latest = data?.latest
  const spreads = useMemo(() => [...(latest?.spreads || [])].sort((x, y) => Math.max(y.pctA, 1 - y.pctA) - Math.max(x.pctA, 1 - x.pctA)), [latest])
  const eventNames = useMemo(() => {
    const m = {}
    for (const h of latest?.handle || []) m[h.eventId] = h.name
    for (const s of latest?.spreads || []) if (!m[s.eventId]) m[s.eventId] = `${s.selA_name} vs ${s.selB_name}`
    return m
  }, [latest])

  if (err && !data) return <div className="py-12 text-center text-sm text-negative">Public Betting unavailable: {err}</div>
  if (!data) {
    return (
      <div className="flex items-center justify-center gap-3 py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading public betting…
      </div>
    )
  }

  const sel = latest?.selections || {}
  const handle = latest?.handle || []
  const leagues = latest?.leagues || []
  const maxStake = Math.max(1, ...handle.map((h) => h.totalStake || 0))
  const maxLg = Math.max(1, ...leagues.map((h) => h.totalStake || 0))

  return (
    <div className="space-y-5 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">Public Betting</h2>
          <p className="mt-0.5 text-xs text-muted-foreground">Where Hard Rock's customers are betting · snapshot every 30 min · monitor only, nothing places</p>
        </div>
        <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
          <span className="tabular-nums">Snapshot {fmtTs(data.ts)} · {num(data.snapshots)} since {fmtTs(data.firstTs)}</span>
          {data.stale && <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-400">stale</span>}
        </div>
      </div>

      <div className="rounded-md border border-amber-500/30 bg-amber-500/[0.06] px-4 py-2.5 text-xs text-amber-300">
        Hard Rock shows only its ~10 most lopsided spreads; bet counts, not money per side. Data starts 11/1; needs weeks before the record means anything.
      </div>

      {!data.ok && <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-6 text-center text-sm text-muted-foreground">{data.error || 'No data yet.'}</div>}

      {spreads.length > 0 && (
        <section className="space-y-3">
          <div className="flex flex-wrap items-baseline justify-between gap-2">
            <h3 className="text-sm font-semibold text-foreground">Lopsided spreads</h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">share of bets · amber = public side · ▲▼ = move since first snapshot today</span>
          </div>
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
            {spreads.map((r) => <SpreadCard key={`${r.eventId}-${r.marketId}`} r={r} hist={data.history} />)}
          </div>
        </section>
      )}

      {data.ok && <RecordCard record={data.record} />}

      {data.ok && (
        <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
          <ListCard title="Most-bet games · $ handle" empty={!handle.length}>
            {handle.map((h) => (
              <div key={h.eventId} className="py-2 text-sm">
                <div className="flex items-baseline justify-between gap-3">
                  <span className="truncate text-foreground">{icon(h.sport)} {h.name}</span>
                  <span className="flex-shrink-0 font-semibold tabular-nums text-foreground">{money(h.totalStake)}</span>
                </div>
                <div className="mt-1 h-1 w-full overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-emerald-500" style={{ width: `${(100 * (h.totalStake || 0) / maxStake).toFixed(1)}%` }} /></div>
                <div className="mt-1 text-[11px] tabular-nums text-muted-foreground">{h.comp} · {fmtStart(h.eventTime)} · {num(h.totalBets)} bets · {num(h.numUsers)} users</div>
              </div>
            ))}
          </ListCard>
          <ListCard title="Most-bet leagues" empty={!leagues.length}>
            {leagues.map((l) => (
              <div key={l.entityId} className="py-2 text-sm">
                <div className="flex items-baseline justify-between gap-3">
                  <span className="truncate text-foreground">{icon(l.sport)} {l.name}</span>
                  <span className="flex-shrink-0 font-semibold tabular-nums text-foreground">{money(l.totalStake)}</span>
                </div>
                <div className="mt-1 h-1 w-full overflow-hidden rounded-full bg-muted"><div className="h-full rounded-full bg-emerald-500" style={{ width: `${(100 * (l.totalStake || 0) / maxLg).toFixed(1)}%` }} /></div>
                <div className="mt-1 text-[11px] tabular-nums text-muted-foreground">{num(l.totalBets)} bets · {num(l.numUsers)} users</div>
              </div>
            ))}
          </ListCard>
          <ListCard title="Popular overs" empty={!(sel.popularOvers || []).length}>
            {(sel.popularOvers || []).map((r) => <SelRow key={r.selectionId} r={r} eventName={eventNames[r.eventId]} />)}
          </ListCard>
          <ListCard title="Popular underdogs" empty={!(sel.popularUnderdogs || []).length}>
            {(sel.popularUnderdogs || []).map((r) => <SelRow key={r.selectionId} r={r} eventName={eventNames[r.eventId]} />)}
          </ListCard>
          <div className="lg:col-span-2">
            <ListCard title="Top props" empty={!(sel.topProps || []).length}>
              {(sel.topProps || []).map((r) => <SelRow key={r.selectionId} r={r} eventName={eventNames[r.eventId]} />)}
            </ListCard>
          </div>
        </div>
      )}
    </div>
  )
}
