import { useState, useEffect, useMemo } from 'react'

// UnifiedPicksBoard — the ONE spot: every sport's current selected pick,
// calibrated, read from the unified store (/api/picks?calibrated=1). Plus a
// cross-sport top-pick hero. Pick Authority Layer 2 (Austin 2026-10-14).
const SPORT_ORDER = ['mlb', 'nhl', 'nba', 'football', 'soccer', 'tennis', 'basketball', 'hockey', 'baseball']
const SPORT_LABEL = {
  mlb: 'MLB', nhl: 'NHL', nba: 'NBA', football: 'Football', soccer: 'Soccer',
  tennis: 'Tennis', basketball: 'Basketball',
  hockey: 'Hockey', baseball: 'NCAA Baseball',
}

function UnifiedPicksBoard({ api }) {
  const [picks, setPicks] = useState(null)
  const [err, setErr] = useState(false)

  useEffect(() => {
    let alive = true
    api.get('/picks?calibrated=1&result=pending&limit=600')
      .then(r => { if (alive) setPicks((r.data && r.data.picks) || []) })
      .catch(() => { if (alive) setErr(true) })
    return () => { alive = false }
  }, [api])

  const bySport = useMemo(() => {
    const g = {}
    for (const p of (picks || [])) {
      if ((p.edge_pct ?? -99) <= 0) continue
      const s = p.sport || 'other'
      if (!g[s]) g[s] = []
      g[s].push(p)
    }
    for (const s of Object.keys(g)) g[s].sort((a, b) => (b.edge_pct ?? 0) - (a.edge_pct ?? 0))
    return g
  }, [picks])

  const sports = useMemo(() => Object.keys(bySport).sort((a, b) => {
    const ia = SPORT_ORDER.indexOf(a), ib = SPORT_ORDER.indexOf(b)
    return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib)
  }), [bySport])

  const hero = useMemo(() => {
    let best = null
    for (const s of sports) for (const p of bySport[s]) {
      if (!best || (p.edge_pct ?? 0) > (best.edge_pct ?? 0)) best = p
    }
    return best
  }, [sports, bySport])

  if (picks === null && !err) {
    return <div className="py-6 text-sm text-muted-foreground">Loading unified picks…</div>
  }
  if (err) {
    return <div className="py-6 text-sm text-muted-foreground">Unified picks unavailable.</div>
  }

  const total = sports.reduce((n, s) => n + bySport[s].length, 0)

  return (
    <div className="space-y-5 py-4">
      <header>
        <h2 className="text-sm font-semibold uppercase tracking-wider text-muted-foreground">
          All Picks — One Spot
        </h2>
        <p className="mt-1 text-xs text-muted-foreground">
          Every sport’s current selected pick, calibrated, from the unified store ·{' '}
          {total} pending · {sports.length} sports
        </p>
      </header>

      {hero && (
        <div className="rounded-lg bg-muted/60 p-4">
          <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
            Top pick — all sports
          </div>
          <div className="mt-1 flex items-end justify-between gap-3">
            <div className="min-w-0">
              <div className="truncate text-lg font-semibold">{hero.pick_text}</div>
              <div className="truncate text-xs text-muted-foreground">
                {(SPORT_LABEL[hero.sport] || hero.sport)} · {hero.matchup}
                {hero.bet_type ? ' · ' + hero.bet_type : ''}
              </div>
            </div>
            <div className="flex-none text-right">
              <div className="text-2xl font-bold tabular-nums text-emerald-500">
                +{Number(hero.edge_pct).toFixed(1)}%
              </div>
              <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
                {hero.confidence || 'lean'}
              </div>
            </div>
          </div>
        </div>
      )}

      {sports.map(s => (
        <section key={s}>
          <h3 className="mb-2 text-xs font-semibold uppercase tracking-wider text-muted-foreground">
            {SPORT_LABEL[s] || s}
            <span className="ml-1 text-[10px] opacity-60">({bySport[s].length})</span>
          </h3>
          <div className="space-y-1">
            {bySport[s].slice(0, 12).map((p, i) => (
              <div
                key={p.id || i}
                className="flex items-center justify-between rounded-md bg-muted/50 px-3 py-2 text-sm"
              >
                <div className="min-w-0">
                  <div className="truncate font-medium">{p.pick_text}</div>
                  <div className="truncate text-xs text-muted-foreground">
                    {p.league && p.league !== s ? p.league + ' · ' : ''}
                    {p.matchup}
                    {p.bet_type ? ' · ' + p.bet_type : ''}
                  </div>
                </div>
                <div className="ml-3 flex-none text-right">
                  <div className="font-semibold tabular-nums text-emerald-500">
                    +{Number(p.edge_pct).toFixed(1)}%
                  </div>
                  <div className="text-[10px] tabular-nums text-muted-foreground">
                    {p.odds > 0 ? '+' + p.odds : p.odds}
                    {p.edge_raw != null ? ' · raw ' + Number(p.edge_raw).toFixed(1) : ''}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>
      ))}
    </div>
  )
}

export default UnifiedPicksBoard
