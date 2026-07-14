/**
 * ModelSignals — three-signal breakdown table per market.
 *
 * Phase 2-cleanup restyle: Tailwind tokens. Shows what each component
 * model (factor / MC / GBM) predicted per market, the ensemble-blended
 * result the picks layer actually uses, and the blend weights.
 *
 * Renders nothing when prediction lacks an `ensemble` key — keeps
 * older cached predictions and shadow-only NHL/NBA MC stacks gracefully
 * degraded.
 *
 * Props:
 *   pred  — full /api/{sport}/predict response.
 *   sport — 'mlb' | 'nhl' | 'nba' — drives which markets are shown.
 *   home  — { abbreviation } (for ML market labelling)
 *   away  — { abbreviation }
 *   view  — 'q1' | 'full' (NBA only; ignored elsewhere). Defaults
 *           to 'q1' for back-compat with single-view callers.
 */

import { cn } from '../../lib/utils'

export default function ModelSignals({ pred, sport, home, away, view = 'q1' }) {
  const ens = pred?.ensemble
  if (!ens || Object.keys(ens).length === 0) return null

  const markets = marketsFor(sport, pred, home, away, view)
  if (markets.length === 0) return null

  return (
    <section className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="px-5 py-3 border-b border-border">
        <h3 className="text-sm font-semibold text-foreground">Model Signals</h3>
        <p className="mt-0.5 text-[11px] text-muted-foreground leading-snug">
          Each market is the weighted blend of up to three independent models.
          Picks and edges use the <em>Blended</em> column.
        </p>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-background/40">
              <Th>Market</Th>
              <Th align="right">Factor</Th>
              <Th align="right">MC</Th>
              <Th align="right">GBM</Th>
              <Th align="right" className="text-primary">Blended</Th>
            </tr>
          </thead>
          <tbody>
            {markets.map((m, i) => (
              <tr
                key={i}
                className="border-b border-border/60 hover:bg-accent/20 transition-colors"
              >
                <td className="px-3 py-2.5">
                  <div className="font-semibold text-foreground">{m.label}</div>
                  {m.weights && (
                    <div className="mt-0.5 text-[10px] text-muted-foreground tabular-nums">
                      {formatWeights(m.weights)}
                    </div>
                  )}
                </td>
                <ComponentCell value={m.factor} fmt={m.fmt} weight={m.weights?.factor} />
                <ComponentCell value={m.mc}     fmt={m.fmt} weight={m.weights?.mc} />
                <ComponentCell value={m.gbm}    fmt={m.fmt} weight={m.weights?.gbm} />
                <td className={cn(
                  'px-3 py-2.5 text-right tabular-nums font-bold',
                  m.blended != null ? 'text-primary' : 'text-muted-foreground/40',
                )}>
                  {m.blended != null ? m.fmt(m.blended) : '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  )
}


function Th({ children, align = 'left', className }) {
  return (
    <th
      scope="col"
      className={cn(
        'px-3 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground',
        align === 'right' && 'text-right',
        className,
      )}
    >
      {children}
    </th>
  )
}


function ComponentCell({ value, fmt, weight }) {
  const dim = value == null || weight == null || weight === 0
  return (
    <td className={cn(
      'px-3 py-2.5 text-right tabular-nums font-semibold',
      dim ? 'text-muted-foreground/50' : 'text-foreground',
    )}>
      {value != null ? fmt(value) : '-'}
    </td>
  )
}


function formatWeights(w) {
  // w: {factor: 0.34, mc: 0.33, gbm: 0.33} — drop zero components.
  const parts = []
  if (w.factor) parts.push(`F ${(w.factor * 100).toFixed(0)}%`)
  if (w.mc)     parts.push(`MC ${(w.mc * 100).toFixed(0)}%`)
  if (w.gbm)    parts.push(`GBM ${(w.gbm * 100).toFixed(0)}%`)
  return parts.join(' · ')
}


function marketsFor(sport, pred, home, away, view = 'q1') {
  const ens = pred?.ensemble || {}
  const mc  = (pred?.mc  && !pred.mc.error)  ? pred.mc  : {}
  const gbm = (pred?.gbm && !pred.gbm.error) ? pred.gbm : {}
  const mcFull = (pred?.mc_full && !pred.mc_full.error) ? pred.mc_full : {}
  const fullPred = pred?.full || {}
  const pct = n => `${(Number(n) * 100).toFixed(1)}%`
  const num = n => {
    const v = Number(n)
    return Number.isFinite(v) ? v.toFixed(2) : '—'
  }

  const hAbbr = home?.abbreviation || 'HOME'

  if (sport === 'mlb') {
    return [
      {
        label: `${hAbbr} ML (home win)`,
        // win_prob IS the factor model's natural output now —
        // calibration no longer stomps it (see picks.py 2026-05-02
        // de-stomping). The factor_win_prob backup field has been
        // retired. Display reads the canonical win_prob directly.
        factor: pred?.win_prob?.home,
        mc:     mc?.win_prob?.home,
        gbm:    gbm?.home_win,
        blended: ens.home_win,
        weights: ens.weights_used?.home_win,
        fmt: pct,
      },
      {
        label: 'Total runs',
        factor: pred?.total,
        mc:     mc?.expected_runs?.total,
        gbm:    gbm?.total_runs,
        blended: ens.total_expected,
        weights: ens.weights_used?.total,
        fmt: num,
      },
      {
        label: 'NRFI',
        factor: pred?.first_inning?.nrfi,
        mc:     mc?.nrfi?.nrfi,
        gbm:    gbm?.nrfi_hit,
        blended: ens.nrfi,
        weights: ens.weights_used?.nrfi,
        fmt: pct,
      },
      {
        label: `${hAbbr} F5 (home F5 win)`,
        factor: pred?.f5?.win_prob?.home,
        mc:     mc?.f5?.win_prob?.home,
        gbm:    gbm?.f5_home_win,
        blended: ens.f5_home_win,
        weights: ens.weights_used?.f5_home_win,
        fmt: pct,
      },
      {
        label: 'F5 total runs',
        factor: pred?.f5?.total,
        mc:     mc?.f5?.expected_runs?.total,
        gbm:    gbm?.f5_total,
        blended: ens.f5_total_expected,
        weights: ens.weights_used?.f5_total,
        fmt: num,
      },
    ].filter(m => m.blended != null || m.factor != null)
  }

  if (sport === 'nhl') {
    return [
      {
        label: `${hAbbr} ML (home win)`,
        // win_prob is the canonical factor output; see MLB note above.
        factor: pred?.win_prob?.home,
        mc:     mc?.win_prob?.home,
        gbm:    null,  // NHL GBM not trained yet
        blended: ens.home_win,
        weights: ens.weights_used?.home_win,
        fmt: pct,
      },
      {
        label: 'Total goals',
        factor: pred?.total,
        mc:     mc?.expected_goals?.total,
        gbm:    null,
        blended: ens.total_expected,
        weights: ens.weights_used?.total,
        fmt: num,
      },
    ].filter(m => m.blended != null || m.factor != null)
  }

  if (sport === 'nba') {
    if (view === 'full') {
      // Full-game markets — fed by `pred.full` (factor) +
      // `pred.mc_full` (MC) + `pred.gbm` (GBM has both Q1 and full
      // targets in one payload). Ensemble keys: home_win,
      // total_expected, margin_expected.
      const mcFullMargin = (mcFull?.expected_points?.home != null
                          && mcFull?.expected_points?.away != null)
        ? mcFull.expected_points.home - mcFull.expected_points.away
        : null
      return [
        {
          label: `${hAbbr} ML (home win)`,
          factor: fullPred?.ml_home,
          mc:     mcFull?.win_prob?.home,
          gbm:    gbm?.home_win,
          blended: ens.home_win,
          weights: ens.weights_used?.home_win,
          fmt: pct,
        },
        {
          label: 'Total points',
          factor: fullPred?.predicted_total,
          mc:     mcFull?.expected_points?.total,
          gbm:    gbm?.total_points,
          blended: ens.total_expected,
          weights: ens.weights_used?.total,
          fmt: num,
        },
        {
          label: 'Spread (home margin)',
          factor: fullPred?.predicted_margin,
          mc:     mcFullMargin,
          gbm:    gbm?.margin,
          blended: ens.margin_expected,
          weights: ens.weights_used?.margin,
          fmt: num,
        },
      ].filter(m => m.blended != null || m.factor != null)
    }

    // Q1 view (default). MC + GBM Q1 fields stay shadow-only until
    // ENABLE_NBA_MC / ENABLE_NBA_GBM flags flip.
    return [
      {
        label: `${hAbbr} Q1 ML (home Q1 win)`,
        // q1_ml_home IS the canonical factor output (calibration
        // stomp removed 2026-05-02). The factor_q1_ml_home backup
        // field has been retired.
        factor: pred?.q1_ml_home,
        mc:     mc?.win_prob?.home,
        gbm:    gbm?.q1_home_win,
        blended: ens.q1_home_win,
        weights: ens.weights_used?.q1_home_win,
        fmt: pct,
      },
      {
        label: 'Q1 total points',
        factor: pred?.predicted_total,
        mc:     mc?.expected_points?.total,
        gbm:    gbm?.q1_total_points,
        blended: ens.q1_total_expected,
        weights: ens.weights_used?.q1_total,
        fmt: num,
      },
      {
        label: 'Q1 spread (home margin)',
        factor: null,  // Q1 factor predicts margin via ml_home, not directly
        mc:     null,
        gbm:    gbm?.q1_margin,
        blended: ens.q1_margin_expected,
        weights: ens.weights_used?.q1_margin,
        fmt: num,
      },
    ].filter(m => m.blended != null || m.factor != null || m.gbm != null)
  }

  return []
}
