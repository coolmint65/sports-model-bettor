import { useState, useEffect } from 'react'

// ESPN matchup-predictor vs Hard Rock mismatch scanner (PAPER, CLV-tracked).
// Styled with the app's shadcn/Tailwind tokens to match the other tabs.

function Logo({ src, abbr, on }) {
  if (src) return <img src={src} alt={abbr} className={`h-7 w-7 object-contain ${on ? '' : 'opacity-40'}`} />
  return <span className={`text-xs font-bold ${on ? 'text-foreground' : 'text-muted-foreground'}`}>{abbr}</span>
}

const chip = (active) =>
  `rounded-full border px-3 py-1 text-xs font-medium transition-colors ${
    active ? 'border-primary bg-primary text-primary-foreground' : 'border-border text-muted-foreground hover:bg-accent'
  }`

export default function EspnMismatchPanel({ api }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  const [league, setLeague] = useState('all')
  const [market, setMarket] = useState('all')

  useEffect(() => {
    let alive = true
    const load = () => api.get('/espn-mismatch')
      .then((r) => { if (alive) { setData(r.data); setErr(null) } })
      .catch((e) => { if (alive) setErr(e?.message || String(e)) })
    load()
    const t = setInterval(load, 60000)
    return () => { alive = false; clearInterval(t) }
  }, [api])

  if (err) return <div className="p-6 text-sm text-red-400">Edge Scanner unavailable: {err}</div>
  if (!data) return <div className="p-6 text-sm text-muted-foreground">Loading Edge Scanner…</div>

  const rel = data.record?.reliable_only || {}
  const all = [...(data.pending || []), ...(data.settled || [])]
  const leagues = ['all', ...Array.from(new Set(all.map((p) => p.league)))]
  const rows = all.filter((p) => (league === 'all' || p.league === league) && (market === 'all' || p.market === market))

  const odds = (o) => (o > 0 ? `+${o}` : `${o}`)
  const play = (p) => p.market === 'SPREAD'
    ? `${p.side} ${p.entry_line > 0 ? '+' : ''}${p.entry_line}`
    : `${p.side} ${odds(p.entry_odds)}`
  const edge = (p) => p.market === 'SPREAD' ? `+${Number(p.edge).toFixed(1)} pt` : `+${(p.edge * 100).toFixed(1)} pp`
  const signal = (p) => p.market === 'SPREAD'
    ? `ESPN projects ${Number(p.espn_margin).toFixed(1)} · line ${Math.abs(p.hr_spread)}`
    : `ESPN ${(p.espn_win * 100).toFixed(0)}% · HR ${(p.hr_fair * 100).toFixed(0)}%`
  const fmtStart = (s) => {
    if (!s) return ''
    const d = new Date(s)
    if (isNaN(d.getTime())) return ''
    return d.toLocaleString('en-US', { timeZone: 'America/New_York', weekday: 'short', month: 'numeric', day: 'numeric', hour: 'numeric', minute: '2-digit' })
  }

  const cards = [
    ['Trusted record', `${rel.w || 0}–${rel.l || 0}`, `${rel.hit || 0}% hit · reliable`],
    ['Trusted ROI', `${rel.roi > 0 ? '+' : ''}${rel.roi || 0}%`, `${rel.n || 0} settled`],
    ['Beat the close', `${rel.clv_beat_pct || 0}%`, `${rel.clv_n || 0} priced · CLV`],
    ['Live plays', data.counts?.pending || 0, `${data.counts?.settled || 0} settled`],
  ]

  return (
    <div className="mx-auto max-w-5xl space-y-5 p-4 md:p-6">
      <div>
        <h1 className="text-xl font-semibold text-foreground">Edge Scanner</h1>
        <p className="text-sm text-muted-foreground">
          ESPN's model (FPI / BPI) vs Hard Rock's price &rarr; moneyline, spread, or pass. Paper, CLV-validated before real money.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        {cards.map(([l, v, s], i) => (
          <div key={i} className="rounded-lg border border-border bg-card p-3">
            <div className="text-[11px] uppercase tracking-wide text-muted-foreground">{l}</div>
            <div className="mt-0.5 text-2xl font-bold text-foreground">{v}</div>
            <div className="text-[11px] text-muted-foreground">{s}</div>
          </div>
        ))}
      </div>

      <div className="flex flex-wrap items-center gap-2">
        {leagues.map((lg) => (
          <button key={lg} className={chip(league === lg)} onClick={() => setLeague(lg)}>{lg.toUpperCase()}</button>
        ))}
        <span className="mx-1 text-border">|</span>
        {['all', 'ML', 'SPREAD'].map((mk) => (
          <button key={mk} className={chip(market === mk)} onClick={() => setMarket(mk)}>{mk === 'all' ? 'ALL' : mk}</button>
        ))}
      </div>

      <div className="space-y-2">
        {rows.map((p, i) => {
          const homeOn = p.side === p.home_abbr
          const awayOn = p.side === p.away_abbr
          return (
            <div key={i} className={`flex items-center gap-4 rounded-lg border border-border bg-card p-3 ${p.reliable ? '' : 'opacity-60'}`}>
              <div className="flex w-36 shrink-0 items-center gap-2">
                <Logo src={p.away_logo} abbr={p.away_abbr} on={awayOn} />
                <span className="text-xs text-muted-foreground">@</span>
                <Logo src={p.home_logo} abbr={p.home_abbr} on={homeOn} />
                {!p.reliable && <span className="ml-1 rounded bg-amber-500/15 px-1.5 py-0.5 text-[10px] font-medium text-amber-500">control</span>}
              </div>
              <div className="min-w-0 flex-1">
                <div className="flex items-center gap-2">
                  <span className="rounded border border-border px-1.5 py-0.5 text-[10px] font-semibold uppercase text-muted-foreground">{p.market}</span>
                  <span className="truncate font-semibold text-foreground">{play(p)}</span>
                  {fmtStart(p.start_time) && <span className="shrink-0 text-[11px] text-muted-foreground tabular-nums">{fmtStart(p.start_time)}</span>}
                </div>
                <div className="truncate text-xs text-muted-foreground">{signal(p)}</div>
              </div>
              <div className="shrink-0 text-right">
                <div className="text-base font-bold text-sky-400">{edge(p)}</div>
                <div className="text-[11px]">
                  {p.result === 'W' ? <span className="font-semibold text-emerald-400">WON{p.profit > 0 ? ` +${p.profit}` : ''}</span>
                    : p.result === 'L' ? <span className="font-semibold text-red-400">LOST</span>
                    : <span className="text-amber-500">pending</span>}
                  {p.clv != null && (
                    <span className={`ml-2 ${p.clv > 0 ? 'text-emerald-400' : p.clv < 0 ? 'text-red-400' : 'text-muted-foreground'}`}>
                      CLV {p.clv > 0 ? '+' : ''}{p.clv}{p.market === 'SPREAD' ? 'pt' : ''}
                    </span>
                  )}
                </div>
              </div>
            </div>
          )
        })}
        {rows.length === 0 && (
          <div className="rounded-lg border border-dashed border-border bg-card/40 p-8 text-center text-sm text-muted-foreground">
            No plays for this filter.
          </div>
        )}
      </div>
    </div>
  )
}
