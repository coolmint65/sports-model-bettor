import { useState, useEffect, useMemo, Fragment } from 'react'
import { cn } from '../lib/utils'

// 🩺 Model Health (PickMint V3, 2026-11-01). Reads GET /api/model-health: a payload the backend builds in a
// background thread every 10 min (engine/model_health.py) and serves from cache -- never computed on request.
// One row per sport x market x side: the record its CARD GATE counts since the current model window, the
// gate's own verdict (edge_proven / NHL + NFL props card gates / props-POTD bar), and today's data checks.
// Austin's scoreboard: real W-L, units and ROI vs break-even first; CLV is secondary.
// Tints use emerald/amber/red/orange/sky: `positive`/`warning` are plain hex vars, so `/NN` on them never generates.

const LANES = {
  proven: { icon: '✅', label: 'Proven', cls: 'border-emerald-500/40 bg-emerald-500/15 text-emerald-400',
    tip: 'Graduated: the card gate is open for this market/side, so its picks headline the real card.' },
  live_unproven: { icon: '🟠', label: 'On card · unproven', cls: 'border-orange-500/40 bg-orange-500/15 text-orange-400',
    tip: 'Reaches the REAL card without passing any proof gate (NBA props have no card gate). Real money, no proof yet.' },
  shadow: { icon: '🟡', label: 'Shadow', cls: 'border-amber-500/40 bg-amber-500/15 text-amber-400',
    tip: 'Recorded and graded, but not a card bet (cards show it only as an amber "lean · not graduated").' },
  paused: { icon: '⛔', label: 'Paused', cls: 'border-red-500/40 bg-red-500/15 text-red-400',
    tip: 'Card gate closed (or the sport is switched off): new picks do not reach the card.' },
}

const COLS = [
  ['lane', 'Lane', 'Status from the live gate code: edge_proven.is_eligible (game markets), engine/sports/nhl/card_gate.py (NHL props), prop_picks.card_gate() (NFL props), the props-POTD proven-cell bar (NBA props).'],
  ['market', 'Market · side', 'bet_type x side. Totals: Over/Under; sides: Fav/Dog from the printed line, else the price sign (edge_proven._condition).'],
  ['n', 'n', 'Graded trials (W+L) since the window. Game markets count one per game / market / side (the median rung of an alt ladder), exactly what the gate counts. Pending in grey.'],
  ['wl', 'W-L-P', 'Wins - losses - pushes since the window.'],
  ['win', 'Win% vs BE', 'Hit rate vs break-even = the average implied probability of the odds taken (vig included). The bar Austin judges by: green when the hit rate clears break-even.'],
  ['roi', 'ROI ± SE', 'Flat 1 unit per pick at the logged odds: profit per unit risked, ± one standard error. |ROI| inside ~2 SE is still noise.'],
  ['units', 'Units', 'Flat units won (1u per pick). Second line: stake-weighted units (Σ stake_units x result) where stakes were recorded.'],
  ['d7', '7d', 'Same record over the last 7 days (by pick date).'],
  ['d30', '30d', 'Same record over the last 30 days (by pick date).'],
  ['cal', 'Claimed → hit', 'Calibration: average claimed win probability (as recorded with the pick) vs the actual hit rate. Gap = overconfidence in points (red when > 5).'],
  ['clv', 'CLV', 'Secondary. Average closing-line value in implied-probability points: implied(close) - implied(entry); positive = beat the close. (n with a close.)'],
  ['since', 'Since', 'Start of the current model / evidence window this record counts from, read from the code (edge_proven._EVIDENCE_SINCE, card_gate.SINCE, player_props_potd.MODEL_SINCE). "all-time" = no window in code (flagged).'],
  ['why', 'Why this status', 'The gate\'s reason, with the numbers it used.'],
]

const f1 = (x) => (x == null ? '—' : `${Number(x).toFixed(1)}`)
const sg = (x, d = 1) => (x == null ? '—' : `${x > 0 ? '+' : x < 0 ? '−' : ''}${Math.abs(Number(x)).toFixed(d)}`)
const rec = (r) => (!r ? '—' : `${r.w ?? 0}-${r.l ?? 0}${r.p ? `-${r.p}` : ''}`)
const shortDate = (d) => (d ? d.slice(5) : '—')
const tone = (x) => (x == null ? 'text-muted-foreground' : x > 0 ? 'text-emerald-400' : x < 0 ? 'text-red-400' : 'text-foreground')

function LanePill({ lane }) {
  const L = LANES[lane] || LANES.shadow
  return (
    <span title={L.tip} className={cn('inline-flex items-center gap-1 whitespace-nowrap rounded-full border px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider', L.cls)}>
      <span aria-hidden="true">{L.icon}</span>{L.label}
    </span>
  )
}

function WinVsBe({ r }) {
  if (!r || !r.n) return <span className="text-muted-foreground">—</span>
  const m = r.win_pct - r.be_pct
  return (
    <span className="whitespace-nowrap tabular-nums" title={`hit ${f1(r.win_pct)}% vs break-even ${f1(r.be_pct)}% (${sg(m)} pts)`}>
      <span className={cn('font-semibold', m > 0 ? 'text-emerald-400' : m < 0 ? 'text-red-400' : 'text-foreground')}>{f1(r.win_pct)}%</span>
      <span className="text-muted-foreground"> vs {f1(r.be_pct)}%</span>
    </span>
  )
}

function Mini({ r }) {
  if (!r || !r.n) return <span className="text-muted-foreground">—</span>
  return (
    <span className="whitespace-nowrap tabular-nums" title={`${rec(r)} · ROI ${sg(r.roi)}% · ${sg(r.units, 2)}u`}>
      {rec(r)} <span className={tone(r.roi)}>{sg(r.roi, 0)}%</span>
    </span>
  )
}

function Cal({ r }) {
  if (!r || r.claimed == null) return <span className="text-muted-foreground">—</span>
  const g = r.cal_gap
  return (
    <span className="whitespace-nowrap tabular-nums" title={`claimed ${f1(r.claimed)}% on ${r.cal_n} picks, hit ${f1(r.claimed - g)}% -> ${g > 0 ? 'overconfident' : 'underconfident'} by ${f1(Math.abs(g))} pts`}>
      {f1(r.claimed)}→{f1(r.claimed - g)}
      <span className={cn('ml-1 text-[10px]', g > 5 ? 'text-red-400' : g > 2 ? 'text-amber-400' : 'text-muted-foreground')}>({sg(g, 0)})</span>
    </span>
  )
}

function Detail({ row }) {
  const g = row.gate || {}
  const box = row.box
  const card = row.card
  const leg = row.legacy
  const line = (label, r) => (
    <div className="flex flex-wrap gap-x-3 tabular-nums">
      <span className="w-36 shrink-0 text-muted-foreground">{label}</span>
      {r && r.n ? (
        <span>{rec(r)} · {f1(r.win_pct)}% vs {f1(r.be_pct)}% · ROI <span className={tone(r.roi)}>{sg(r.roi)}%</span>{r.roi_se != null && ` ± ${f1(r.roi_se)}`} · <span className={tone(r.units)}>{sg(r.units, 2)}u</span>
          {r.staked_units != null && <> · staked <span className={tone(r.staked_units)}>{sg(r.staked_units, 2)}u</span> ({r.staked_n})</>}
          {r.pending ? <span className="text-muted-foreground"> · {r.pending} pending</span> : null}
          <span className="text-muted-foreground"> · {shortDate(r.first)} → {shortDate(r.last)}</span></span>
      ) : (
        <span className="text-muted-foreground">{r && r.pending ? `${r.pending} pending, none graded` : 'none'}</span>
      )}
    </div>
  )
  let gateText = ''
  if (g.kind === 'edge_proven') {
    gateText = g.n ? `edge_proven cell: n ${g.n} · win ${f1(g.win)}% · ROI ${sg(g.roi)}% · Bayesian conf ${g.conf} (floor ${g.floor}) · eligible ${g.eligible ? 'YES' : 'no'}` : 'edge_proven: no graded cell yet'
  } else if (g.kind === 'nhl_card_gate') {
    gateText = `NHL card gate: n ${g.n} (${g.real} real + ${g.shadow} shadow) · ROI ${sg(g.roi)}%${g.se != null ? ` ± ${g.se}` : ''} · needs n ≥ ${g.min_n} and ROI > 1 SE · proven ${g.proven ? 'YES' : 'no'}`
  } else if (g.kind === 'nfl_card_gate') {
    gateText = `NFL card gate: shadow n ${g.n}${g.wins != null ? `, ${g.wins} W` : ''}${g.margin != null ? ` · margin ${sg(g.margin)} pts (SE ${g.se})` : ''} · needs n ≥ ${g.min_n} and margin > 1.5 SE · proven ${g.proven ? 'YES' : 'no'}`
  } else if (g.kind === 'props_potd') {
    gateText = g.n ? `props-POTD cell: n ${g.n} · ${f1(g.win_pct)}% vs ${f1(g.be_pct)}% · margin ${sg(g.margin_pp)} (SE ${g.se_pp}) · proven ${g.proven ? 'YES' : 'no'}` : 'props-POTD cell: no graded picks'
  }
  return (
    <div className="space-y-1.5 px-3 py-3 text-[11px] leading-relaxed text-foreground">
      <div className="flex flex-wrap gap-x-3"><span className="w-36 shrink-0 text-muted-foreground">Gate</span><span className="tabular-nums">{gateText}</span></div>
      {row.potd && g.kind !== 'props_potd' && (
        <div className="flex flex-wrap gap-x-3"><span className="w-36 shrink-0 text-muted-foreground">Props-POTD bar</span>
          <span className="tabular-nums">{row.potd.n ? `${row.potd.n} graded card picks · ${f1(row.potd.win_pct)}% vs ${f1(row.potd.be_pct)}% · ${row.potd.proven ? 'POTD-proven' : 'not POTD-proven'}` : 'no graded card picks'}</span></div>
      )}
      {line(row.group === 'game' ? 'Evidence (gate)' : row.sport === 'NFL' ? 'Shadow (rule replay)' : 'Evidence (gate)', row.rec)}
      {box && line('Box-score re-grade', box)}
      {line(row.group === 'game' ? 'Card / tracker picks' : 'Real card picks', card)}
      {line('Legacy (before window)', leg)}
      {row.window_flag && <div className="flex gap-x-3"><span className="w-36 shrink-0 text-muted-foreground">Window</span><span className="text-amber-300">{row.window_flag}</span></div>}
      {row.model_note && <div className="flex gap-x-3"><span className="w-36 shrink-0 text-muted-foreground">Model note</span><span className="text-amber-300">{row.model_note}</span></div>}
      {(row.flags || []).map((fl, i) => (
        <div key={i} className="flex gap-x-3"><span className="w-36 shrink-0 text-muted-foreground">Flag · {fl.kind}</span>
          <span className={fl.kind === 'cache_lag' ? 'text-sky-300' : 'text-red-300'}>{fl.text}</span></div>
      ))}
    </div>
  )
}

const ITEM_COLS = ['sport', 'source', 'date', 'matchup', 'pick', 'card', 'tracker', 'tracker_result', 'after_fix', 'result',
  'stored', 'box', 'scope', 'score', 'in_gate_window', 'stake_units', 'start', 'status', 'hours_past_end', 'games_live_or_6h',
  'games_next_24h', 'next_start', 'freshest', 'age_min', 'note']

function IntegrityDetail({ c }) {
  const items = c.items || []
  const cols = ITEM_COLS.filter((k) => items.some((it) => it[k] != null && it[k] !== ''))
  return (
    <div className="mt-3 rounded-lg border border-border bg-background/40 p-3">
      <div className="text-[11px] text-muted-foreground">{c.rule}</div>
      {c.by_source && Object.keys(c.by_source).length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5 text-[10px] tabular-nums">
          {Object.entries(c.by_source).map(([k, v]) => <span key={k} className="rounded bg-muted px-1.5 py-0.5 text-muted-foreground">{k}: {v}</span>)}
        </div>
      )}
      {items.length === 0 ? (
        <div className="mt-2 text-xs text-emerald-400">Nothing to report.</div>
      ) : (
        <div className="mt-2 max-h-80 overflow-auto">
          <table className="w-full text-[11px]">
            <thead className="sticky top-0 bg-card text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr className="border-b border-border text-left">{cols.map((k) => <th key={k} className="py-1 pr-3 font-semibold">{k.replace(/_/g, ' ')}</th>)}</tr>
            </thead>
            <tbody>
              {items.map((it, i) => (
                <tr key={i} className={cn('border-b border-border/40', (it.stale || it.after_fix) && 'text-red-300', it.after_fix === false && 'opacity-70')}>
                  {cols.map((k) => <td key={k} className="py-1 pr-3 align-top tabular-nums">{it[k] === true ? 'yes' : it[k] === false ? 'no' : it[k] == null ? '' : String(it[k])}</td>)}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

function IntegrityStrip({ checks }) {
  const [open, setOpen] = useState(null)
  const sel = checks.find((c) => c.key === open)
  return (
    <section className="rounded-xl border border-border bg-card p-4">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h3 className="text-sm font-semibold text-foreground">Data integrity · today</h3>
        <span className="text-[11px] text-muted-foreground">click a chip for the list · read-only, nothing is fixed here</span>
      </div>
      <div className="mt-3 flex flex-wrap gap-2">
        {checks.map((c) => (
          <button key={c.key} type="button" onClick={() => setOpen(open === c.key ? null : c.key)} title={c.rule}
            className={cn('inline-flex items-center gap-1.5 rounded-full border px-3 py-1 text-xs font-medium transition-colors',
              c.ok ? 'border-emerald-500/40 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20'
                : 'border-red-500/40 bg-red-500/10 text-red-400 hover:bg-red-500/20',
              open === c.key && 'ring-2 ring-ring')}>
            <span aria-hidden="true">{c.ok ? '✓' : '✗'}</span>
            <span>{c.label}</span>
            <span className="tabular-nums font-bold">{c.count}</span>
            {c.count_all != null && c.count_all !== c.count && <span className="tabular-nums text-[10px] opacity-80">({c.count_all} all-time)</span>}
          </button>
        ))}
      </div>
      {sel && <IntegrityDetail c={sel} />}
    </section>
  )
}

function Chip({ active, onClick, children, title }) {
  return (
    <button type="button" onClick={onClick} title={title}
      className={cn('rounded-md px-2.5 py-1 text-xs font-medium transition-colors',
        active ? 'bg-primary text-primary-foreground' : 'bg-muted text-muted-foreground hover:bg-accent hover:text-accent-foreground')}>
      {children}
    </button>
  )
}

export default function ModelHealthPanel({ api }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  const [sport, setSport] = useState('ALL')
  const [group, setGroup] = useState('all')
  const [lane, setLane] = useState('all')
  const [inactive, setInactive] = useState(false)
  const [openRow, setOpenRow] = useState(null)

  useEffect(() => {
    let alive = true
    const load = () => api.get('/model-health')
      .then((r) => { if (alive) { setData(r.data); setErr(null) } })
      .catch((e) => { if (alive) setErr(e?.message || String(e)) })
    load()
    const t = setInterval(load, 60000)
    return () => { alive = false; clearInterval(t) }
  }, [api])

  const rows = useMemo(() => {
    const all = (data && data.rows) || []
    return all.filter((r) => (inactive || r.active)
      && (sport === 'ALL' || r.sport === sport)
      && (group === 'all' || r.group === group)
      && (lane === 'all' || r.lane === lane))
  }, [data, sport, group, lane, inactive])

  if (err && !data) return <div className="py-12 text-center text-sm text-red-400">Model Health unavailable: {err}</div>
  if (!data || data.building) {
    return (
      <div className="flex items-center justify-center gap-3 py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        {data && data.building ? 'Building the first Model Health snapshot (background job, ~1 min)…' : 'Loading Model Health…'}
      </div>
    )
  }

  const ageMin = data.cache ? Math.round(data.cache.age_s / 60) : null
  const counts = data.counts || {}
  const hidden = (data.rows || []).filter((r) => !r.active).length

  return (
    <div className="space-y-5 py-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">🩺 Model Health</h2>
          <p className="mt-0.5 max-w-3xl text-xs text-muted-foreground">
            Every model's record since its current version, counted the way its card gate counts it. Judge by win% vs break-even and ROI; CLV is secondary.
            Proven rows are on the real card; shadow rows are graded but not bets.
          </p>
        </div>
        <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
          <span className="tabular-nums" title={`built in ${data.compute_ms} ms by the background job`}>Updated {data.generated_et}{ageMin != null && ` · ${ageMin} min ago`}</span>
          {ageMin != null && ageMin > 30 && <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-amber-400">stale</span>}
          {data.cache && data.cache.building && <span className="rounded-full bg-sky-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-sky-400">rebuilding</span>}
        </div>
      </div>

      <IntegrityStrip checks={data.integrity || []} />

      <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
        {['proven', 'live_unproven', 'shadow', 'paused'].map((k) => (
          <button key={k} type="button" onClick={() => setLane(lane === k ? 'all' : k)} title={LANES[k].tip}
            className={cn('rounded-xl border bg-card px-4 py-3 text-left transition-colors hover:bg-accent', lane === k ? 'border-primary' : 'border-border')}>
            <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{LANES[k].icon} {LANES[k].label}</div>
            <div className="mt-1 text-2xl font-semibold tabular-nums text-foreground">{counts[k] ?? 0}</div>
            <div className="text-[10px] text-muted-foreground">active rows</div>
          </button>
        ))}
      </div>

      <section className="rounded-xl border border-border bg-card p-4">
        <div className="flex flex-wrap items-center gap-2">
          <Chip active={sport === 'ALL'} onClick={() => setSport('ALL')}>All sports</Chip>
          {(data.sports || []).map((s) => <Chip key={s} active={sport === s} onClick={() => setSport(s)}>{s}</Chip>)}
          <span className="mx-1 h-4 w-px bg-border" />
          <Chip active={group === 'all'} onClick={() => setGroup('all')}>All markets</Chip>
          <Chip active={group === 'game'} onClick={() => setGroup('game')}>Game markets</Chip>
          <Chip active={group === 'props'} onClick={() => setGroup('props')}>Player props</Chip>
          <span className="mx-1 h-4 w-px bg-border" />
          <label className="flex cursor-pointer items-center gap-1.5 text-xs text-muted-foreground" title="Rows with no graded picks in the current window and nothing recent, plus switched-off sports (MLB).">
            <input type="checkbox" checked={inactive} onChange={(e) => setInactive(e.target.checked)} className="accent-emerald-500" />
            show inactive / legacy-only ({hidden})
          </label>
        </div>

        <div className="mt-3 overflow-x-auto">
          <table className="w-full text-xs">
            <thead className="text-[10px] uppercase tracking-wider text-muted-foreground">
              <tr className="border-b border-border text-left">
                <th className="py-1.5 pr-3 font-semibold" title={COLS[0][2]}>Lane ⓘ</th>
                <th className="pr-3 font-semibold" title="Sport / league.">Sport</th>
                {COLS.slice(1).map(([k, label, tip]) => (
                  <th key={k} title={tip} className={cn('pr-3 font-semibold', ['n', 'wl', 'roi', 'units', 'clv'].includes(k) && 'text-right', k === 'why' && 'min-w-[22rem]')}>{label} ⓘ</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.length === 0 && (
                <tr><td colSpan={COLS.length + 1} className="py-6 text-center text-muted-foreground">No rows for this filter.</td></tr>
              )}
              {rows.map((r) => {
                const R = r.rec || {}
                const isOpen = openRow === r.id
                const red = (r.flags || []).some((f) => f.kind !== 'cache_lag')
                return (
                  <Fragment key={r.id}>
                    <tr onClick={() => setOpenRow(isOpen ? null : r.id)}
                      className={cn('cursor-pointer border-b border-border/50 align-top hover:bg-accent/40', isOpen && 'bg-accent/30', !r.active && 'opacity-60')}>
                      <td className="py-1.5 pr-3"><LanePill lane={r.lane} /></td>
                      <td className="whitespace-nowrap pr-3 text-muted-foreground">{r.sport_label}</td>
                      <td className="whitespace-nowrap pr-3 font-medium text-foreground">
                        {r.market_label} <span className="text-muted-foreground">· {r.side_label}</span>
                        {red && <span className="ml-1.5 rounded bg-red-500/15 px-1 text-[10px] font-bold text-red-400" title={(r.flags || []).map((f) => f.text).join('\n')}>⚠ {r.flags.filter((f) => f.kind !== 'cache_lag').length}</span>}
                        {r.window_flag && <span className="ml-1 text-[10px] text-amber-400" title={r.window_flag}>◷</span>}
                      </td>
                      <td className="pr-3 text-right tabular-nums">{R.n || 0}{R.pending ? <span className="text-muted-foreground"> +{R.pending}</span> : null}</td>
                      <td className="whitespace-nowrap pr-3 text-right tabular-nums">{R.n ? rec(R) : '—'}</td>
                      <td className="pr-3"><WinVsBe r={R} /></td>
                      <td className="whitespace-nowrap pr-3 text-right tabular-nums">
                        {R.n ? <><span className={cn('font-semibold', tone(R.roi))}>{sg(R.roi)}%</span>{R.roi_se != null && <span className="text-muted-foreground"> ± {f1(R.roi_se)}</span>}</> : '—'}
                      </td>
                      <td className="whitespace-nowrap pr-3 text-right tabular-nums">
                        {R.n ? <span className={tone(R.units)}>{sg(R.units, 2)}u</span> : '—'}
                        {R.staked_units != null && <div className="text-[10px] text-muted-foreground" title={`stake-weighted over ${R.staked_n} staked picks`}>staked <span className={tone(R.staked_units)}>{sg(R.staked_units, 2)}u</span></div>}
                      </td>
                      <td className="pr-3"><Mini r={r.d7} /></td>
                      <td className="pr-3"><Mini r={r.d30} /></td>
                      <td className="pr-3"><Cal r={R} /></td>
                      <td className="whitespace-nowrap pr-3 text-right tabular-nums text-muted-foreground">{R.clv != null ? <span title={`${R.clv_n} picks with a closing price`}>{sg(R.clv, 1)} <span className="text-[10px]">({R.clv_n})</span></span> : '—'}</td>
                      <td className="whitespace-nowrap pr-3 tabular-nums text-muted-foreground">{r.since ? shortDate(r.since) : <span className="text-amber-400" title={r.window_flag || ''}>all-time</span>}</td>
                      <td className="pr-3 text-[11px] leading-snug text-muted-foreground">{r.reason}</td>
                    </tr>
                    {isOpen && (
                      <tr className="border-b border-border/50 bg-background/40">
                        <td colSpan={COLS.length + 1}><Detail row={r} /></td>
                      </tr>
                    )}
                  </Fragment>
                )
              })}
            </tbody>
          </table>
        </div>
      </section>

      <section className="rounded-xl border border-border bg-card p-4 text-[11px] leading-relaxed text-muted-foreground">
        <h3 className="text-sm font-semibold text-foreground">How this page counts</h3>
        <div className="mt-2 grid gap-1.5 md:grid-cols-2">
          {Object.entries(LANES).map(([k, L]) => <div key={k}><span className="mr-1">{L.icon}</span><span className="font-semibold text-foreground">{L.label}:</span> {L.tip}</div>)}
        </div>
        <div className="mt-3 space-y-1">
          {data.gates && <>
            <div><span className="font-semibold text-foreground">Game-market gate:</span> {data.gates.edge_proven && data.gates.edge_proven.rule}</div>
            {data.gates.nhl_props && <div><span className="font-semibold text-foreground">NHL props gate:</span> {data.gates.nhl_props}</div>}
            {data.gates.nfl_props && <div><span className="font-semibold text-foreground">NFL props gate:</span> {data.gates.nfl_props}</div>}
            {data.gates.props_potd && <div><span className="font-semibold text-foreground">Props POTD bar:</span> {data.gates.props_potd}</div>}
          </>}
        </div>
        <div className="mt-3 space-y-1">
          {(data.sources || []).map((s) => <div key={s.sport}><span className="font-semibold text-foreground">{s.sport}:</span> {s.text}</div>)}
        </div>
        {data.notes && data.notes.length > 0 && <div className="mt-3 text-red-400">Build notes: {data.notes.join(' · ')}</div>}
        <div className="mt-3">Built by a background job every 10 minutes (engine/model_health.py) in {data.compute_ms} ms; this page only reads the cache. Click a row for the gate numbers, the card/tracker record, legacy and any flags.</div>
      </section>
    </div>
  )
}
