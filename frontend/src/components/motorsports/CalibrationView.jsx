/**
 * Motorsports calibration view — per-market bucket table showing
 * empirical vs calibrated hit rate. Extracted from MotorsportsPanel
 * 2026-07-09.
 */
import { Th, Td, Stat } from './cells'


export default function CalibrationView({ calibration }) {
  if (!calibration) {
    return (
      <div className="rounded-lg border border-border bg-card/50 px-4 py-6 text-sm text-muted-foreground">
        Loading calibration…
      </div>
    )
  }
  const cal = calibration.calibration || {}
  const winnerMap = cal.WINNER || null
  const podiumMap = cal.PODIUM || null
  if (!calibration.n || (!winnerMap && !podiumMap)) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/40 px-6 py-12 text-center space-y-2">
        <div className="text-sm font-semibold text-foreground">
          Calibration data pending.
        </div>
        <div className="text-xs text-muted-foreground max-w-md mx-auto">
          Live picks are shrunk empirically via the shared
          picks_core calibrator until a series-specific walk-forward
          backfill lands. Picks emit + settle normally in the
          meantime — the panel just doesn't have per-bucket Brier
          numbers to plot yet.
        </div>
      </div>
    )
  }
  const briW = calibration.brier_win
  const briP = calibration.brier_podium
  return (
    <>
      <div className="rounded-lg border border-border bg-card/60 px-4 py-3 grid grid-cols-3 gap-3">
        <Stat label="Backtest Obs" value={calibration.n} />
        <Stat label="Brier (Win)" value={briW != null ? briW.toFixed(4) : '—'} />
        <Stat label="Brier (Podium)" value={briP != null ? briP.toFixed(4) : '—'} />
      </div>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mt-3">
        {winnerMap && <CalibrationTable title="Winner" map={winnerMap} />}
        {podiumMap && <CalibrationTable title="Podium" map={podiumMap} />}
      </div>
      <div className="mt-3 text-[11px] text-muted-foreground">
        Bayesian beta-binomial shrinkage with PRIOR_N0=10. Each bucket's
        calibrated value blends empirical hit rate toward the bucket midpoint
        proportional to sample size. Live picks now use these calibrated
        probabilities; raw probs are only logged for backtest.
      </div>
    </>
  )
}



function CalibrationTable({ title, map }) {
  const buckets = Object.keys(map)
    .filter(k => /^\d+$/.test(k))
    .map(k => ({ k: parseInt(k, 10), ...map[k] }))
    .sort((a, b) => a.k - b.k)
  return (
    <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
      <div className="px-3 py-2 border-b border-border bg-background/40">
        <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
          {title} Calibration
        </div>
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/20">
            <Th>Bucket (model %)</Th>
            <Th align="right">N</Th>
            <Th align="right">Hits</Th>
            <Th align="right">Empirical</Th>
            <Th align="right">Calibrated</Th>
          </tr>
        </thead>
        <tbody>
          {buckets.map(b => {
            // Bucket grids are market-specific (WINNER 0.5–28%, PODIUM
            // 3–85%) — use the explicit bucket range when present; only
            // fall back to legacy 10-bucket equal-width labels for old
            // calibration JSONs that predate the per-market grids.
            let lo, hi
            if (Array.isArray(b.bucket) && b.bucket.length === 2) {
              lo = (b.bucket[0] * 100).toFixed(1)
              hi = (Math.min(b.bucket[1], 1.0) * 100).toFixed(1)
            } else {
              lo = (b.k * 10).toFixed(0)
              hi = ((b.k + 1) * 10).toFixed(0)
            }
            const empirical = b.n > 0 ? b.wins / b.n : null
            const delta = (b.calibrated - b.midpoint) * 100
            const deltaTone = Math.abs(delta) < 2 ? 'text-muted-foreground'
              : delta > 0 ? 'text-positive' : 'text-destructive'
            return (
              <tr key={b.k} className="border-b border-border/60">
                <Td className="font-mono text-xs">{lo}–{hi}%</Td>
                <Td align="right" className="tabular-nums">{b.n}</Td>
                <Td align="right" className="tabular-nums">{b.wins}</Td>
                <Td align="right" className="tabular-nums">
                  {empirical != null ? `${(empirical * 100).toFixed(1)}%` : '—'}
                </Td>
                <Td align="right" className={`tabular-nums font-semibold ${deltaTone}`}>
                  {(b.calibrated * 100).toFixed(1)}%
                  <span className="ml-1 text-[10px]">
                    ({delta > 0 ? '+' : ''}{delta.toFixed(1)})
                  </span>
                </Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

