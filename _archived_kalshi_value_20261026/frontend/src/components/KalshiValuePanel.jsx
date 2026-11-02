import { useState, useEffect } from 'react'

// Kalshi value scanner - LOG-ONLY validation view. Our model's P(over X) vs Kalshi's
// price for NHL/NBA totals. The model currently runs under Kalshi's totals (one-directional
// bias), so this is a track-record builder, NOT a bet board. Austin 2026-10-07.
export default function KalshiValuePanel({ api }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  useEffect(() => {
    let alive = true
    api.get('/kalshi-value')
      .then(r => { if (alive) setData(r.data) })
      .catch(e => { if (alive) setErr(String(e)) })
    return () => { alive = false }
  }, [api])

  const flagged = (data && data.flagged) || []
  const val = (data && data.validation) || {}
  const meta = (data && data.meta) || {}
  const ov = val.overall
  const pct = x => (x == null ? '-' : (100 * x).toFixed(0) + '%')
  const edge = x => (x == null ? '-' : (x >= 0 ? '+' : '') + (100 * x).toFixed(1) + 'pp')
  const ts = t => { try { return new Date(t).toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) } catch { return t || '-' } }

  return (
    <div className="mx-auto max-w-5xl px-4 py-6 space-y-5">
      <div>
        <h2 className="text-lg font-semibold text-foreground flex items-center gap-2">Kalshi Value Scanner
          <span className="rounded-full bg-amber-500/15 text-amber-500 text-[10px] font-bold uppercase px-2 py-0.5 tracking-wide">Log-only / Validating</span>
        </h2>
        <p className="text-[11px] text-muted-foreground mt-0.5">Our model's P(over X) vs Kalshi's price on NHL/NBA game totals. Last scan: {ts(meta.last_scan_ts)} / {meta.total_logged || 0} logged / {meta.pending || 0} pending settle.</p>
      </div>
      <div className="rounded-lg border border-amber-500/30 bg-amber-500/5 px-4 py-3 text-[12px] text-amber-200/90 leading-relaxed">
        <b className="text-amber-400">Unvalidated - do not bet.</b> The model currently runs UNDER Kalshi's totals (a one-directional bias), so flagged &quot;edges&quot; are likely our number being low, not real value. This logs model-vs-Kalshi-vs-result forward; only trust it once the track record below shows our number beating Kalshi's close.
      </div>
      <div className="rounded-lg border border-border bg-card/50 p-4">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground mb-2">Validation record (settled)</div>
        {!ov || ov.n === 0 ? (
          <div className="text-sm text-muted-foreground">Accruing - {meta.pending || 0} games pending settle. Record + CLV appear here as games finish.</div>
        ) : (
          <div className="flex flex-wrap gap-6">
            <div><div className="text-[10px] uppercase text-muted-foreground">Record</div><div className="text-xl font-semibold tabular-nums">{ov.wins}-{ov.n - ov.wins}</div><div className="text-[10px] text-muted-foreground">{ov.win_pct}% / n={ov.n}</div></div>
            <div><div className="text-[10px] uppercase text-muted-foreground">Avg CLV</div><div className={'text-xl font-semibold tabular-nums ' + ((ov.avg_clv || 0) >= 0 ? 'text-positive' : 'text-negative')}>{edge(ov.avg_clv)}</div><div className="text-[10px] text-muted-foreground">our prob vs Kalshi</div></div>
            {Object.entries(val.by_sport || {}).map(([sp, a]) => (
              <div key={sp}><div className="text-[10px] uppercase text-muted-foreground">{sp}</div><div className="text-sm font-semibold tabular-nums">{a.wins}-{a.n - a.wins} <span className="text-muted-foreground">({a.win_pct == null ? '-' : a.win_pct + '%'})</span></div><div className="text-[10px] text-muted-foreground">CLV {edge(a.avg_clv)}</div></div>
            ))}
          </div>
        )}
      </div>
      <div className="rounded-lg border border-border bg-card/50 p-4">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground mb-2">Flagged edges - latest scan (liquid, OI&ge;100, tight spread, &ge;5pp)</div>
        {err ? <div className="text-sm text-negative">Error: {err}</div> :
         !data ? <div className="text-sm text-muted-foreground">Loading...</div> :
         flagged.length === 0 ? <div className="text-sm text-muted-foreground">No qualifying edges in the latest scan (after liquidity + spread + edge filters).</div> : (
          <div className="overflow-x-auto">
            <table className="w-full text-[12px]">
              <thead><tr className="text-left text-muted-foreground border-b border-border">
                <th className="py-1.5 pr-3 font-medium">Game</th><th className="py-1.5 pr-3 font-medium">Pick</th>
                <th className="py-1.5 pr-3 font-medium">Model</th><th className="py-1.5 pr-3 font-medium">Market</th>
                <th className="py-1.5 pr-3 font-medium text-right">Edge</th><th className="py-1.5 pr-3 font-medium text-right">OI</th>
              </tr></thead>
              <tbody>
                {flagged.map((f, i) => {
                  const mkt = f.best_side === 'Over' ? f.kalshi_yes_ask : f.kalshi_no_ask
                  const ours = f.best_side === 'Over' ? f.our_prob_over : (1 - f.our_prob_over)
                  return (
                    <tr key={i} className="border-b border-border/50">
                      <td className="py-1.5 pr-3">{f.matchup} <span className="text-muted-foreground uppercase text-[10px]">{f.sport}</span></td>
                      <td className="py-1.5 pr-3 font-medium">{f.best_side} {Number(f.line).toFixed(1)}{f.one_directional && <span title="All picks this game are one-sided - likely model bias" className="ml-1 text-amber-500 text-[10px]">bias</span>}</td>
                      <td className="py-1.5 pr-3 tabular-nums">{pct(ours)}</td>
                      <td className="py-1.5 pr-3 tabular-nums text-muted-foreground">{pct(mkt)}</td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-positive">{edge(f.edge)}</td>
                      <td className="py-1.5 pr-3 text-right tabular-nums text-muted-foreground">{Math.round(f.open_interest)}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
         )}
        <p className="text-[10px] text-muted-foreground mt-2">NBA excluded while preseason. &quot;Model&quot; = our P(hit) for the pick; &quot;Market&quot; = Kalshi's implied. &quot;bias&quot; = every flagged pick on that game is one-sided.</p>
      </div>
    </div>
  )
}
