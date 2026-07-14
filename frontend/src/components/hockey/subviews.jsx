/**
 * Hockey Panel subviews — Calibration + Standings.
 * Extracted from HockeyPanel 2026-07-09 refactor pass.
 */
import { useEffect, useState } from 'react'


export function CalibrationView({ api, leagueKey }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    api.get(`/hockey/${leagueKey}/calibration`)
      .then(r => setData(r.data))
      .catch(() => setData(null))
      .finally(() => setLoading(false))
  }, [api, leagueKey])

  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card px-6 py-10 text-center text-sm text-muted-foreground">
        Loading calibration…
      </div>
    )
  }
  if (!data) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm">
        <div className="font-semibold text-foreground">No calibration data.</div>
      </div>
    )
  }
  const c = data.constants || {}
  return (
    <section className="space-y-3">
      <div className="rounded-lg border border-border bg-card overflow-hidden">
        <div className="px-5 py-3 border-b border-border">
          <h3 className="text-sm font-semibold">Fitted constants</h3>
          <p className="text-[10px] text-muted-foreground mt-0.5">
            Derived from the league's backfilled finalized games.
          </p>
        </div>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-px bg-border">
          <Stat label="Avg goals/team" value={fmt(c.league_avg_gpg)} />
          <Stat label="Avg total goals" value={fmt(c.league_avg_total)} />
          <Stat label="Home boost (goals)" value={fmt(c.home_boost, '+')} />
          <Stat label="Status" value={c.status || '—'} />
          <Stat label="Settled picks" value={data.n_settled ?? 0} />
        </div>
      </div>
      {(!data.buckets || data.buckets.length === 0) && (
        <div className="rounded-lg border border-dashed border-border bg-card/50 px-5 py-4 text-xs text-muted-foreground">
          Per-bucket Brier / hit-rate metrics appear here once the
          league has settled picks. Until then we show only the fitted
          model constants.
        </div>
      )}
    </section>
  )
}


function fmt(v, prefix = '') {  // eslint-disable-line no-unused-vars
  if (v == null) return '—'
  if (typeof v === 'number') {
    return `${prefix && v >= 0 ? prefix : ''}${v.toFixed(2)}`
  }
  return String(v)
}


function Stat({ label, value }) {
  return (
    <div className="bg-card px-4 py-3">
      <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
        {label}
      </div>
      <div className="font-bold tabular-nums text-foreground mt-0.5">
        {value}
      </div>
    </div>
  )
}


// ── Standings view ────────────────────────────────────────

export function StandingsView({ api, leagueKey }) {
  const [rows, setRows] = useState(null)
  const [loading, setLoading] = useState(false)

  useEffect(() => {
    setLoading(true)
    api.get(`/hockey/${leagueKey}/standings`)
      .then(r => setRows(Array.isArray(r.data) ? r.data : []))
      .catch(() => setRows([]))
      .finally(() => setLoading(false))
  }, [api, leagueKey])

  if (loading) {
    return (
      <div className="rounded-lg border border-border bg-card px-6 py-10 text-center text-sm text-muted-foreground">
        Loading standings…
      </div>
    )
  }
  if (!rows || rows.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center text-sm">
        <div className="font-semibold text-foreground">No standings available.</div>
        <div className="mt-1 text-xs text-muted-foreground">
          League hasn't accumulated enough finalized games this season.
        </div>
      </div>
    )
  }

  // Group by division when present so the table mirrors NHL standings.
  // PWHL has 1 league / no divisions; falls into a single "Standings"
  // group so the layout still works.
  const groups = {}
  for (const r of rows) {
    const k = r.division || 'Standings'
    if (!groups[k]) groups[k] = []
    groups[k].push(r)
  }

  return (
    <section className="space-y-4">
      {Object.entries(groups).map(([div, teams]) => (
        <div key={div} className="rounded-lg border border-border bg-card overflow-hidden">
          <div className="px-5 py-3 border-b border-border">
            <h3 className="text-sm font-semibold">{div}</h3>
          </div>
          <table className="w-full text-sm">
            <thead className="bg-muted/40">
              <tr className="text-[10px] uppercase tracking-wider text-muted-foreground">
                <th className="text-left px-4 py-2">#</th>
                <th className="text-left px-4 py-2">Team</th>
                <th className="text-right px-4 py-2">GP</th>
                <th className="text-right px-4 py-2">W</th>
                <th className="text-right px-4 py-2">L</th>
                <th className="text-right px-4 py-2">OTL</th>
                <th className="text-right px-4 py-2 font-semibold">PTS</th>
                <th className="text-right px-4 py-2">GF</th>
                <th className="text-right px-4 py-2">GA</th>
                <th className="text-right px-4 py-2">DIFF</th>
                <th className="text-right px-4 py-2">L10</th>
                <th className="text-right px-4 py-2">STRK</th>
              </tr>
            </thead>
            <tbody>
              {teams.map((t, i) => {
                const gp = (t.wins || 0) + (t.losses || 0) + (t.otl || 0)
                const diff = t.diff != null ? t.diff : (t.gf || 0) - (t.ga || 0)
                return (
                  <tr key={t.team_id} className="border-t border-border/40">
                    <td className="px-4 py-2 text-muted-foreground tabular-nums">{i + 1}</td>
                    <td className="px-4 py-2">
                      <span className="inline-flex items-center gap-2">
                        {t.logo && (
                          <img src={t.logo} alt="" className="h-4 w-4" loading="lazy" />
                        )}
                        <span className="font-bold">{t.abbreviation}</span>
                        <span className="text-muted-foreground">{t.name}</span>
                      </span>
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums">{gp}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.wins || 0}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.losses || 0}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.otl || 0}</td>
                    <td className="px-4 py-2 text-right tabular-nums font-bold">{t.points || 0}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.gf || 0}</td>
                    <td className="px-4 py-2 text-right tabular-nums">{t.ga || 0}</td>
                    <td className={`px-4 py-2 text-right tabular-nums font-semibold ${
                      diff > 0 ? 'text-positive' : diff < 0 ? 'text-negative' : ''
                    }`}>
                      {diff > 0 ? '+' : ''}{diff}
                    </td>
                    <td className="px-4 py-2 text-right tabular-nums text-muted-foreground">{t.l10 || '0-0-0'}</td>
                    <td className="px-4 py-2 text-right tabular-nums font-semibold">{t.streak || '-'}</td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      ))}
    </section>
  )
}
