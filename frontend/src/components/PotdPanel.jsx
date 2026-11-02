import { useState, useEffect } from 'react'
import PotdHero from './PotdHero'

const POTD_SPORTS = [
  ['nfl', 'NFL'], ['cfb', 'CFB'], ['nhl', 'NHL'],
  ['nba', 'NBA'], ['wnba', 'WNBA'], ['mlb', 'MLB'], ['epl', 'EPL'],
]

// Top-level "Pick of the Day": the single best pick across every sport PLAYING TODAY as a
// full hero (carrying the OVERALL POTD's own record), with every other sport's POTD below in
// compact/list form. Loads INCREMENTALLY so one slow sport (NBA best-bets ~19s cold) never
// blocks the panel. The WHOLE PANEL is TODAY-only: football POTDs are forward-looking and carry
// a game_date, so a future NFL (or any sport with a game_date) pick stays fully hidden -- hero
// AND card -- until its own game day. (Austin 2026-10-06, card-gate fix 2026-10-10)
export default function PotdPanel({ api }) {
  const [rows, setRows] = useState([])
  const [overall, setOverall] = useState(null)
  const [loaded, setLoaded] = useState(0)
  useEffect(() => {
    let alive = true
    setRows([]); setLoaded(0)
    POTD_SPORTS.forEach(([key]) => {
      api.get('/pick-of-day/' + key + (key === 'nba' ? '?view=full' : ''))
        .then(r => { if (alive) setRows(p => [...p, { key, data: r.data }]) })
        .catch(() => {})
        .finally(() => { if (alive) setLoaded(n => n + 1) })
    })
    api.get('/potd-overall/summary').then(r => { if (alive) setOverall(r.data) }).catch(() => {})
    return () => { alive = false }
  }, [api])

  const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' })
  const withPick = rows.filter(r => r.data && !r.data.message && r.data.pick)
  // TODAY-gate for the whole panel, hero AND per-sport cards alike. Football POTDs are
  // forward-looking (an NFL POTD can be a game days out) and carry game_date; a future one is
  // excluded everywhere -- hero and the "other sports" list -- until its own game day. Daily
  // sports carry no game_date (their slate is today by construction) so they stay in.
  const isToday = d => !d.game_date || d.game_date === today
  // +EV floor (Austin 2026-10-12 #2): a pick must clear calibrated edge > 0 to
  // headline or list. Backend POTDs already carry the calibrated edge in `edge`.
  const evPick = r => Number(r.data.edge) > 0
  const heroPool = withPick.filter(r => isToday(r.data) && evPick(r))
  const best = heroPool.length ? heroPool.reduce((a, b) => (Number(b.data.edge) > Number(a.data.edge) ? b : a)) : null
  // "Other sports" list is sourced from heroPool (today-only), not the raw withPick -- a
  // same-day-ineligible pick (future game_date) must stay hidden here too, not just out of the
  // hero slot. This was the bug: NFL's POTD (tomorrow's game) kept showing as a card below for
  // days because `rest` fell back to the unfiltered list. (MintyBot 2026-10-10)
  const rest = best ? heroPool.filter(r => r.key !== best.key) : withPick.filter(evPick)
  const allDone = loaded >= POTD_SPORTS.length

  const label = key => (POTD_SPORTS.find(x => x[0] === key) || [])[1] || key.toUpperCase()
  const list = items => (
    <div className="space-y-3">
      {items.map(({ key, data }) => (
        <PotdHero key={key} compact label={label(key)} sport={key} pick={data} accent="primary" />
      ))}
    </div>
  )

  return (
    <div className="app">
      <div className="mx-auto max-w-5xl px-4 py-6 space-y-5">
        <div>
          <h2 className="text-lg font-semibold text-foreground">Pick of the Day</h2>
          <p className="text-sm text-muted-foreground">The single best pick across every sport playing today</p>
        </div>
        {best ? (
          <>
            <PotdHero label="PICK OF THE DAY" sport={best.key} pick={best.data} summary={overall} accent="primary" />
            {rest.length > 0 && (
              <div className="space-y-3">
                <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Other sports' picks of the day</div>
                {list(rest)}
              </div>
            )}
            <p className="text-[11px] text-muted-foreground leading-relaxed">Record above is the OVERALL POTD's own track record (daily best-across-all-sports pick), not any one league's. Paper-tracked; placement stays manual.</p>
          </>
        ) : withPick.length > 0 ? (
          <>
            <div className="rounded-lg border border-dashed border-border bg-card/40 px-4 py-4 text-sm text-muted-foreground">Nothing is playing today — here are the upcoming picks of the day.</div>
            <div className="space-y-3">
              <div className="text-[11px] font-semibold uppercase tracking-wide text-muted-foreground">Upcoming picks of the day</div>
              {list(withPick)}
            </div>
          </>
        ) : !allDone ? (
          <div className="rounded-lg border border-border bg-card/50 px-4 py-6 text-sm text-muted-foreground">Loading picks of the day...</div>
        ) : (
          <div className="rounded-lg border border-dashed border-border bg-card/40 px-6 py-12 text-center text-sm text-muted-foreground">No pick of the day right now.</div>
        )}
      </div>
    </div>
  )
}
