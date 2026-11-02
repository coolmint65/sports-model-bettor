import { useEffect, useState } from 'react'
import { cachedGet } from '../lib/apiCache'
import TeamRow from './primitives/TeamRow'
import { cn } from '../lib/utils'

// NHL live (in-game) cards -- MintyBot 2026-10-28 (Austin: "should look like the full cards").
// One card per live game: score + period/clock, the special-teams situation, the model's live win%
// against Hard Rock's, this period's ONE lean (engine.sports.nhl.live_model: ratings rates x game
// state x power play / empty net, model's own number, NOT graduated), and the picks recorded for
// earlier periods with their results. Polls /api/nhl/live-picks every 20 s.

const POLL_MS = 20000
const pct = (p) => (p == null ? '—' : `${Math.round(p * 100)}%`)
const odds = (o) => (o == null ? '' : o > 0 ? `+${o}` : `${o}`)
const mmss = (s) => `${Math.floor(s / 60)}:${String(Math.round(s % 60)).padStart(2, '0')}`
const SIT_LABEL = { PP: 'Power play', EN: 'Empty net', XA: 'Goalie pulled', EV4: '4-on-4' }

function prettyPick(p) {
  if (!p) return ''
  const mk = (p.bet_type || '').replace(/^LIVE P\d+\s+/, '')
  if (mk === 'ML') return `${p.pick} ML`
  if (mk === 'O/U') return `${p.pick} goals`
  return `${p.pick} puck line`
}

function SituationChip({ sit, home, away }) {
  if (!sit) return null
  const side = ['PP', 'EN', 'XA'].includes(sit.home) ? 'home' : ['PP', 'EN', 'XA'].includes(sit.away) ? 'away' : null
  const state = side ? sit[side] : sit.home
  if (!SIT_LABEL[state]) return null
  const team = side === 'home' ? home : side === 'away' ? away : null
  return (
    <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-500">
      {team ? `${team} ` : ''}{SIT_LABEL[state]}{sit.remaining_s > 0 && ['PP', 'EV4'].includes(state) ? ` · ${mmss(sit.remaining_s)}` : ''}
    </span>
  )
}

function ModelVsMarket({ live, home, away }) {
  const m = live?.model_home_win
  const k = live?.market_home_win
  if (m == null) return null
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-[10px] tabular-nums text-muted-foreground">
        <span className={cn(m < 0.5 && 'font-semibold text-foreground')}>{away} {pct(1 - m)}</span>
        <span className="uppercase tracking-wider">model win %</span>
        <span className={cn(m >= 0.5 && 'font-semibold text-foreground')}>{home} {pct(m)}</span>
      </div>
      <div className="relative flex h-1.5 overflow-hidden rounded-full bg-secondary">
        <div className="bg-primary" style={{ width: pct(1 - m) }} />
        <div className="bg-warning" style={{ width: pct(m) }} />
        {k != null && (
          <div className="absolute top-[-2px] h-[10px] w-0.5 bg-foreground/80" style={{ left: pct(1 - k) }} title={`Hard Rock: ${home} ${pct(k)}`} />
        )}
      </div>
      {k != null && (
        <div className="text-right text-[10px] tabular-nums text-muted-foreground">
          Hard Rock {home} {pct(k)} <span className={cn('font-semibold', Math.abs(m - k) >= 0.03 ? 'text-amber-500' : 'text-muted-foreground')}>
            ({m - k >= 0 ? '+' : ''}{Math.round((m - k) * 1000) / 10}pp)
          </span>
        </div>
      )}
    </div>
  )
}

function PeriodPick({ live, period }) {
  const p = live?.pick
  if (!p) {
    return (
      <div className="rounded-md border border-dashed border-border px-3 py-2.5 text-xs text-muted-foreground">
        P{period}: no lean. The model and Hard Rock agree on every live market.
      </div>
    )
  }
  return (
    <div className="rounded-md border border-amber-500/30 bg-amber-500/[0.05] px-3 py-2.5">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">P{p.pick_at_period || period} lean</div>
          <div className="mt-0.5 truncate text-sm font-semibold text-foreground">{prettyPick(p)}</div>
        </div>
        <div className="text-right">
          <div className="text-sm font-bold tabular-nums text-foreground">{odds(p.odds)}</div>
          <div className="text-[11px] font-semibold tabular-nums text-positive">+{(p.edge || 0).toFixed(1)}% edge</div>
        </div>
      </div>
      <div className="mt-1.5 flex items-center justify-between text-[10px] tabular-nums text-muted-foreground">
        <span>model {pct(p.model_prob)} · book {pct(p.odds > 0 ? 100 / (p.odds + 100) : -p.odds / (-p.odds + 100))}</span>
        <span className="rounded bg-amber-500/15 px-1.5 py-0.5 font-bold uppercase tracking-wider text-amber-500">Lean · not graduated · not a bet</span>
      </div>
    </div>
  )
}

function PeriodStrip({ live }) {
  const rows = live?.periods || []
  return (
    <div className="grid grid-cols-3 gap-1.5">
      {[1, 2, 3].map((n) => {
        const r = rows.find((x) => x.period === n)
        const tone = !r ? 'border-border text-muted-foreground'
          : r.result === 'W' ? 'border-emerald-500/40 bg-emerald-500/[0.06]'
            : r.result === 'L' ? 'border-red-500/40 bg-red-500/[0.06]' : 'border-border'
        return (
          <div key={n} className={cn('rounded-md border px-2 py-1.5 text-[10px]', tone)}>
            <div className="flex items-center justify-between font-semibold uppercase tracking-wider text-muted-foreground">
              <span>P{n}</span>
              {r?.result && <span className={r.result === 'W' ? 'text-positive' : r.result === 'L' ? 'text-negative' : ''}>{r.result}</span>}
            </div>
            <div className="mt-0.5 truncate text-foreground">{r ? `${prettyPick(r)} ${odds(r.odds)}` : 'no pick'}</div>
          </div>
        )
      })}
    </div>
  )
}

function LiveCard({ g }) {
  const st = g.state || {}
  const home = g.home || {}
  const away = g.away || {}
  const team = (t, other) => ({ abbreviation: t.abbr, name: t.name, score: t.score, winner: t.score > other.score })
  return (
    <div className="space-y-3 rounded-lg border border-border border-l-4 border-l-amber-500/70 bg-card p-4">
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2">
          <span className="flex items-center gap-1 rounded-full bg-red-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-red-500">
            <span className="h-1.5 w-1.5 animate-pulse rounded-full bg-red-500" />Live
          </span>
          <span className="text-xs tabular-nums text-muted-foreground">{st.detail || (st.period ? `P${st.period} ${st.clock || ''}` : '')}</span>
        </div>
        <SituationChip sit={g.live?.situation} home={home.abbr} away={away.abbr} />
      </div>
      <div className="space-y-2">
        <TeamRow team={team(away, home)} isLive sport="nhl" />
        <TeamRow team={team(home, away)} isLive sport="nhl" />
      </div>
      <ModelVsMarket live={g.live} home={home.abbr} away={away.abbr} />
      <PeriodPick live={g.live} period={st.period} />
      <PeriodStrip live={g.live} />
    </div>
  )
}

const MK_LABEL = { ML: 'Moneyline', PL: 'Puck line', 'O/U': 'Total' }
const units = (u) => (u == null ? '—' : `${u >= 0 ? '+' : ''}${u.toFixed(2)}u`)
const signed = (x, d = 1) => (x == null ? '—' : `${x >= 0 ? '+' : ''}${x.toFixed(d)}`)

function StatTile({ label, s }) {
  if (!s || !s.n) {
    return (
      <div className="rounded-md border border-border px-2.5 py-2">
        <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{label}</div>
        <div className="mt-0.5 text-xs text-muted-foreground">no picks yet</div>
      </div>
    )
  }
  return (
    <div className="rounded-md border border-border px-2.5 py-2">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{label}</div>
      <div className="mt-0.5 flex items-baseline justify-between gap-2 tabular-nums">
        <span className="text-sm font-semibold text-foreground">{s.w}-{s.l}{s.p ? `-${s.p}` : ''}</span>
        <span className={cn('text-xs font-semibold', s.units > 0 ? 'text-positive' : s.units < 0 ? 'text-negative' : 'text-muted-foreground')}>{units(s.units)}</span>
      </div>
      <div className="mt-0.5 flex justify-between text-[10px] tabular-nums text-muted-foreground">
        <span>{s.pending ? `${s.pending} pending` : `ROI ${s.roi == null ? '—' : `${signed(s.roi)}%`}`}</span>
        <span>CLV {s.clv_n ? `${signed(s.clv_pp, 2)}pp (${s.beat_clv}/${s.clv_n})` : '—'}</span>
      </div>
    </div>
  )
}

// Live NHL tracker (Austin 2026-10-29): record of every start-of-period lean, graded at the price it
// was picked at (flat 1u). CLV = Hard Rock's price for the same selection ~3 min later with the score
// unchanged (+ = we beat it). Reads /api/nhl/live-tracker.
function LiveTracker() {
  const [t, setT] = useState(null)
  const [open, setOpen] = useState(false)
  useEffect(() => {
    let alive = true
    const load = () => cachedGet('/nhl/live-tracker', { ttlMs: 60000 })
      .then((r) => { if (alive) setT(r.data || r) })
      .catch(() => {})
    load()
    const id = setInterval(load, 60000)
    return () => { alive = false; clearInterval(id) }
  }, [])
  if (!t) return null
  const o = t.overall || {}
  const rows = t.rows || []
  return (
    <div className="rounded-lg border border-border bg-card p-3">
      <div className="flex items-baseline justify-between gap-2">
        <div className="text-xs font-semibold uppercase tracking-wider text-foreground">Live tracker</div>
        <div className="text-[10px] text-muted-foreground">flat 1u at the pick price · leans, not bets</div>
      </div>
      <div className="mt-2 grid grid-cols-2 gap-1.5 md:grid-cols-4">
        <StatTile label="All periods" s={o} />
        {['P1', 'P2', 'P3'].map((k) => <StatTile key={k} label={k} s={t.by_period?.[k]} />)}
      </div>
      <div className="mt-1.5 grid grid-cols-3 gap-1.5">
        {['ML', 'PL', 'O/U'].map((k) => <StatTile key={k} label={MK_LABEL[k]} s={t.by_market?.[k]} />)}
      </div>
      {rows.length > 0 && (
        <button type="button" onClick={() => setOpen(!open)} className="mt-2 text-[11px] font-semibold text-primary">
          {open ? 'Hide picks' : `Show all ${rows.length} picks`}
        </button>
      )}
      {open && (
        <div className="mt-2 overflow-x-auto">
          <table className="w-full text-[11px] tabular-nums">
            <thead className="text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr className="text-left">
                <th className="py-1 pr-2 font-semibold">Date</th>
                <th className="py-1 pr-2 font-semibold">Game</th>
                <th className="py-1 pr-2 font-semibold">Per</th>
                <th className="py-1 pr-2 font-semibold">Pick</th>
                <th className="py-1 pr-2 text-right font-semibold">Price</th>
                <th className="py-1 pr-2 text-right font-semibold">Model</th>
                <th className="py-1 pr-2 text-right font-semibold">CLV</th>
                <th className="py-1 text-right font-semibold">Result</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((r) => (
                <tr key={`${r.game_key}-${r.bet_type}`} className="border-t border-border">
                  <td className="py-1 pr-2 text-muted-foreground">{(r.date || '').slice(5)}</td>
                  <td className="py-1 pr-2">{r.matchup}{r.at_home_score != null ? <span className="text-muted-foreground"> ({r.at_away_score}-{r.at_home_score})</span> : null}</td>
                  <td className="py-1 pr-2">P{r.period}</td>
                  <td className="py-1 pr-2 font-semibold text-foreground">{prettyPick({ bet_type: r.bet_type, pick: r.pick })}</td>
                  <td className="py-1 pr-2 text-right">{odds(r.odds)}</td>
                  <td className="py-1 pr-2 text-right">{pct(r.model_prob)}</td>
                  <td className={cn('py-1 pr-2 text-right', r.clv_pp > 0 ? 'text-positive' : r.clv_pp < 0 ? 'text-negative' : 'text-muted-foreground')}>{r.clv_pp == null ? '—' : `${signed(r.clv_pp)}pp`}</td>
                  <td className={cn('py-1 text-right font-semibold', r.result === 'W' ? 'text-positive' : r.result === 'L' ? 'text-negative' : 'text-muted-foreground')}>
                    {r.result === 'W' || r.result === 'L' || r.result === 'P' ? `${r.result} ${units(r.units)}` : r.result === 'V' ? 'void' : 'pending'}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

export default function NHLLivePanel() {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  useEffect(() => {
    let alive = true
    const load = () => cachedGet('/nhl/live-picks', { ttlMs: POLL_MS })
      .then((r) => { if (alive) { setData(r.data || r); setErr(null) } })
      .catch((e) => { if (alive) setErr(e?.message || String(e)) })
    load()
    const t = setInterval(load, POLL_MS)
    return () => { alive = false; clearInterval(t) }
  }, [])
  if (err) return <div className="py-10 text-center text-sm text-negative">Live feed unavailable: {err}</div>
  if (!data) return <div className="py-10 text-center text-sm text-muted-foreground">Loading live games…</div>
  const games = data.games || []
  return (
    <div className="space-y-3">
      <p className="text-[11px] text-muted-foreground">
        One lean per game per period, picked from every live market at the start of the period. Model = pre-game ratings × score,
        time left, power plays and empty nets. Leans are tracked, not graduated.
      </p>
      {games.length === 0 ? (
        <div className="rounded-md border border-dashed border-border px-4 py-10 text-center text-sm text-muted-foreground">
          No NHL games live right now. Cards appear here at puck drop.
        </div>
      ) : (
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
          {games.map((g) => <LiveCard key={g.game_id} g={g} />)}
        </div>
      )}
      <LiveTracker />
    </div>
  )
}
