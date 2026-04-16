/**
 * Three-signal breakdown card: shows what each component model
 * (factor / MC / GBM) predicted per market, the ensemble-blended
 * result the picks layer actually uses, and the blend weights.
 *
 * Renders nothing when the prediction lacks an `ensemble` key. That
 * keeps older cached predictions (pre-flag-flip) from showing an
 * empty shell, and keeps NHL/NBA surfaces gracefully degraded while
 * their MC simulators are still shadow-only.
 *
 * Props:
 *   pred  — full /api/predict response (MLB), /api/nhl/predict, or
 *           /api/nba/predict.
 *   sport — 'mlb' | 'nhl' | 'nba' — drives which markets are shown.
 *   home  — { abbreviation } (for ML market labelling)
 *   away  — { abbreviation }
 */
export default function ModelSignals({ pred, sport, home, away }) {
  const ens = pred?.ensemble
  if (!ens || Object.keys(ens).length === 0) return null

  // Per-sport market config: (key in ensemble, label, raw component
  // extractors, value formatter). Extractors return null/undefined
  // when the component didn't run so the row renders a dash.
  const markets = marketsFor(sport, pred, home, away)
  if (markets.length === 0) return null

  return (
    <div className="result-card">
      <h2>Model Signals</h2>
      <p style={{fontSize:'0.75rem', color:'#64748b', marginTop:-6, marginBottom:10}}>
        Each market is the weighted blend of up to three independent models.
        Picks and edges use the <em>Blended</em> column.
      </p>
      <table style={{width:'100%', fontSize:'0.82rem', borderCollapse:'collapse'}}>
        <thead>
          <tr style={{color:'#64748b', textAlign:'right', fontSize:'0.72rem', textTransform:'uppercase'}}>
            <th style={{padding:'6px 0', fontWeight:600, textAlign:'left'}}>Market</th>
            <th style={{padding:'6px 0', fontWeight:600}}>Factor</th>
            <th style={{padding:'6px 0', fontWeight:600}}>MC</th>
            <th style={{padding:'6px 0', fontWeight:600}}>GBM</th>
            <th style={{padding:'6px 0', fontWeight:600, color:'#60a5fa'}}>Blended</th>
          </tr>
        </thead>
        <tbody>
          {markets.map((m, i) => (
            <tr key={i} style={{borderTop:'1px solid #1e293b'}}>
              <td style={{padding:'8px 0', color:'#cbd5e1', fontWeight:600}}>
                {m.label}
                {m.weights && (
                  <div style={{fontSize:'0.68rem', color:'#64748b', fontWeight:400, marginTop:2}}>
                    {formatWeights(m.weights)}
                  </div>
                )}
              </td>
              <ComponentCell value={m.factor} fmt={m.fmt} weight={m.weights?.factor} />
              <ComponentCell value={m.mc} fmt={m.fmt} weight={m.weights?.mc} />
              <ComponentCell value={m.gbm} fmt={m.fmt} weight={m.weights?.gbm} />
              <td style={{
                padding:'8px 0', textAlign:'right', fontWeight:700,
                color: m.blended != null ? '#60a5fa' : '#475569',
              }}>
                {m.blended != null ? m.fmt(m.blended) : '-'}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}


function ComponentCell({ value, fmt, weight }) {
  const dim = value == null || weight == null || weight === 0
  return (
    <td style={{
      padding:'8px 0', textAlign:'right',
      color: dim ? '#475569' : '#cbd5e1',
      fontWeight: 600,
    }}>
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


function marketsFor(sport, pred, home, away) {
  const ens = pred?.ensemble || {}
  const mc  = (pred?.mc  && !pred.mc.error)  ? pred.mc  : {}
  const gbm = (pred?.gbm && !pred.gbm.error) ? pred.gbm : {}
  const pct = n => `${(n * 100).toFixed(1)}%`
  const num = n => n.toFixed(2)

  const hAbbr = home?.abbreviation || 'HOME'

  if (sport === 'mlb') {
    return [
      {
        label: `${hAbbr} ML (home win)`,
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
    return [
      {
        label: `${hAbbr} Q1 ML (home Q1 win)`,
        factor: pred?.q1_ml_home,
        mc:     mc?.win_prob?.home,
        gbm:    null,  // NBA GBM not trained yet
        blended: ens.q1_home_win,
        weights: ens.weights_used?.q1_home_win,
        fmt: pct,
      },
      {
        label: 'Q1 total points',
        factor: pred?.predicted_total,
        mc:     mc?.expected_points?.total,
        gbm:    null,
        blended: ens.q1_total_expected,
        weights: ens.weights_used?.q1_total,
        fmt: num,
      },
    ].filter(m => m.blended != null || m.factor != null)
  }

  return []
}
