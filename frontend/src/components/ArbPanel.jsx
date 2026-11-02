import { useState, useEffect, useMemo } from 'react'
import { resolveTeamLogo } from '../lib/teamLogo'
import { cn } from '../lib/utils'

// Cross-book Arb scanner (display-only). Reads GET /api/arb -- a snapshot the Pi builds by pricing
// the same two-sided market on every book it scans (Hard Rock, Kalshi, Polymarket US, Novig,
// ProphetX, Bovada, DraftKings...). Every phantom guard (incl-OT vs regulation, period-vs-full-game
// scope, liquidity, scope caveats) runs on the Pi, so anything shown here is already vetted. NO bets.
// Restyled 2026-10-27 (Austin "make the arb section resemble the rest of PickMint"): the props-tab
// P/L banner, PropPickCard-style tiles with team logos, and a by-sport table. Tints use emerald/amber:
// `positive`/`warning` are plain hex vars in tailwind.config, so `/NN` opacity modifiers on them never generate.

const SPORT_ICON = { MLB: '⚾', NFL: '🏈', NHL: '🏒', NBA: '🏀', WNBA: '🏀', CFB: '🏈', CBB: '🏀', TENNIS: '🎾', SOCCER: '⚽', TT: '🏓', ESPORTS: '🎮', UFC: '🥊' , ALL: '🌐'}
const LOGO_SPORT = { MLB: 'mlb', NFL: 'nfl', NHL: 'nhl', NBA: 'nba', WNBA: 'wnba' }
const UNIT_PRESETS = [25, 50, 100, 250, 500, 1000, 2000, 5000, 10000]   // $1K+ added 2026-10-27 (Austin)

function fmtUpdated(iso) {
  if (!iso) return 'never'
  const d = new Date(iso)
  if (isNaN(d.getTime())) return String(iso)
  return d.toLocaleString('en-US', { timeZone: 'America/New_York', month: 'numeric', day: 'numeric', hour: 'numeric', minute: '2-digit' })
}
function fmtAge(sec) {
  if (sec == null) return ''
  if (sec < 90) return `${Math.round(sec)}s ago`
  if (sec < 5400) return `${Math.round(sec / 60)}m ago`
  return `${(sec / 3600).toFixed(1)}h ago`
}
const money = (n, dp = 0) => `$${Number(n).toLocaleString('en-US', { minimumFractionDigits: dp, maximumFractionDigits: dp })}`

// Game start -> "Today 7:30 PM" / "Tomorrow 1:00 PM" / "Nov 1, 1:00 PM", always ET. Not every
// book supplies start_ms -- absent renders nothing.
const ET_DAY = new Intl.DateTimeFormat('en-CA', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit' })
const fmtClock = (d) => d.toLocaleString('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' })
function fmtStart(ms) {
  if (ms == null) return null
  const d = new Date(ms)
  if (isNaN(d.getTime())) return null
  const now = Date.now()
  const k = ET_DAY.format(d)
  if (k === ET_DAY.format(now)) return `Today ${fmtClock(d)}`
  if (k === ET_DAY.format(now + 86400000)) return `Tomorrow ${fmtClock(d)}`
  return `${d.toLocaleString('en-US', { timeZone: 'America/New_York', month: 'short', day: 'numeric' })}, ${fmtClock(d)}`
}

// ---- humanize raw book outcome strings ----
const STAT_LABEL = {
  rebounds: 'Rebounds', points: 'Points', goals: 'Goals', assists: 'Assists', threes: '3-Pointers',
  pra: 'Pts+Reb+Ast', pr: 'Pts+Reb', pa: 'Pts+Ast', ra: 'Reb+Ast', steals: 'Steals', blocks: 'Blocks', ftm: 'Free Throws',
  passyds: 'Pass Yds', rushyds: 'Rush Yds', recyds: 'Rec Yds', receptions: 'Receptions', passtds: 'Pass TDs',
  rushatt: 'Rush Att', passatt: 'Pass Att', passcomp: 'Completions', rushrecyds: 'Rush+Rec Yds', tds: 'TDs',
  interceptions: 'INTs', longrec: 'Longest Rec', longrush: 'Longest Rush',
  strikeouts: 'Strikeouts', totalbases: 'Total Bases', hits: 'Hits', rbis: 'RBIs', hrr: 'H+R+RBI',
  hr: 'Home Runs', hkpoints: 'Points', hkassists: 'Assists', shots: 'Shots', saves: 'Saves',
  sog: 'Shots on Goal', stocks: 'Steals+Blocks',
  hitsallowed: 'Hits Allowed', outs: 'Outs', walks: 'Walks', earnedruns: 'Earned Runs',
}
const titleCase = (s) => String(s || '').replace(/\b\w/g, (c) => c.toUpperCase())
// Capitalize market-type words, leaving team abbrevs (TB) and scope tags ((H1)) alone.
const _MKT_WORDS = /\b(rebounds|points|goals|assists|hits|allowed|strikeouts|walks|outs|prop|team|total|spread|over|under|first|innings|inning|half|quarter|period|runs|bases|earned|moneyline)\b/gi
const titleizeMarket = (s) => {
  const pm = /^([a-z]+)\s+prop$/i.exec(String(s || ''))
  if (pm) return `${STAT_LABEL[pm[1].toLowerCase()] || titleCase(pm[1].toLowerCase())} Prop`
  return String(s || '').replace(_MKT_WORDS, (w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).replace(/\bRbis\b/g, 'RBIs')
}

// A player-prop arb -> {stat, player, line, thresh}; null for game/total/team-total markets.
function propMeta(a) {
  const m = /^([a-z]+)\s+prop$/i.exec(a.market || '')
  if (!m) return null
  const stat = STAT_LABEL[m[1].toLowerCase()] || titleCase(m[1].toLowerCase())
  const lm = /^(.+?)\s+([\d.]+)$/.exec(a.line || '')
  return { stat, player: lm ? titleCase(lm[1]) : (a.line || ''), line: lm ? lm[2] : null, thresh: lm ? Math.ceil(parseFloat(lm[2])) : null }
}

// Readable label for one leg's outcome (prop-aware; falls back to the raw text).
function humanizeOutcome(outcome, pm) {
  const o = String(outcome || '')
  let m
  if (pm) {
    if ((m = /YES\b.*?:\s*(\d+)\+/i.exec(o))) return `${m[1]}+ ${pm.stat}`
    if ((m = /NO\b.*?:\s*(\d+)\+/i.exec(o))) return `Under ${Number(m[1]) - 0.5} ${pm.stat}`
    if ((m = /\b(Over|Under)\s+([\d.]+)/i.exec(o))) {
      return m[1].toLowerCase() === 'over' ? `${Math.ceil(parseFloat(m[2]))}+ ${pm.stat}` : `Under ${m[2]} ${pm.stat}`
    }
  }
  if ((m = /NO\s*!\(.*?(over|under)\s+([\d.]+)/i.exec(o))) return `${m[1].toLowerCase() === 'over' ? 'Under' : 'Over'} ${m[2]}`
  // "<market> / Over 115.5" (exchange legs carry the market name first) -> the side part
  const tail = o.includes(' / ') ? o.slice(o.lastIndexOf(' / ') + 3) : o
  if ((m = /\b(Over|Under)\s+([\d.]+)/i.exec(tail))) return `${titleCase(m[1].toLowerCase())} ${m[2]}`
  if ((m = /YES\b\s*(.+)/i.exec(tail))) return titleCase(m[1].replace(/[:!()]/g, '').trim())
  // a clean book-native outcome (team / player), minus any trailing "[...]" context bracket
  return tail.replace(/\s*\[[^\]]*\]\s*$/, '').trim() || o
}

// ---- books ----
const BOOK = {
  HardRock:   { label: 'Hard Rock',  tone: 'bg-violet-500/15 text-violet-300 ring-violet-500/30', odds: 'american', verb: 'Bet' },
  Kalshi:     { label: 'Kalshi',     tone: 'bg-emerald-500/15 text-emerald-300 ring-emerald-500/30', odds: 'cents', verb: 'Buy' },
  Polymarket: { label: 'Polymarket', tone: 'bg-blue-500/15 text-blue-300 ring-blue-500/30', odds: 'cents', verb: 'Buy' },
  Novig:      { label: 'Novig',      tone: 'bg-fuchsia-500/15 text-fuchsia-300 ring-fuchsia-500/30', odds: 'american', verb: 'Buy' },
  ProphetX:   { label: 'ProphetX',   tone: 'bg-orange-500/15 text-orange-300 ring-orange-500/30', odds: 'american', verb: 'Buy' },
  Bovada:     { label: 'Bovada',     tone: 'bg-red-500/15 text-red-300 ring-red-500/30', odds: 'american', verb: 'Bet' },
  DraftKings: { label: 'DraftKings', tone: 'bg-lime-500/15 text-lime-300 ring-lime-500/30', odds: 'american', verb: 'Bet' },
  Duel:       { label: 'Duel',       tone: 'bg-cyan-500/15 text-cyan-300 ring-cyan-500/30', odds: 'decimal', verb: 'Bet' },
  Vave:       { label: 'Vave',       tone: 'bg-slate-500/20 text-slate-300 ring-slate-500/30', odds: 'decimal', verb: 'Bet' },
}
const bookOf = (b) => BOOK[b] || { label: b || '?', tone: 'bg-muted text-muted-foreground ring-border', odds: null, verb: 'Bet' }

// The leg's price as the BOOK shows it (pre-fee raw price): American for sportsbooks + Novig/ProphetX,
// cents for Kalshi/Polymarket, decimal for Duel/Vave. ProphetX sends its own American odds exactly.
function bookOdds(s) {
  const raw = s.raw_price != null ? s.raw_price : s.price
  const fmt = bookOf(s.book).odds
  if (fmt === 'american' && s.american != null) return `${s.american > 0 ? '+' : ''}${s.american}`
  if (raw == null || raw <= 0 || raw >= 1) return null
  if (fmt === 'american') return raw >= 0.5 ? `-${Math.round((raw / (1 - raw)) * 100)}` : `+${Math.round(((1 - raw) / raw) * 100)}`
  if (fmt === 'cents') return `${Math.round(raw * 100)}¢`
  if (fmt === 'decimal') return (1 / raw).toFixed(2)
  return `${(raw * 100).toFixed(1)}%`
}

// ---- matchup: team logos for the team sports, real names for tennis/soccer ----
function teamPair(a) {
  if (a.away && a.home) return { left: a.away, right: a.home, sep: '@' }
  const m = /^(.+?)\s*@\s*(.+)$/.exec(String(a.game || ''))
  return m ? { left: m[1].trim(), right: m[2].trim(), sep: 'vs' } : null   // scanner pairs are sorted, not away/home
}
function prettyName(s) {
  // tennis player-keys like "krolo_f" -> "Krolo F."; soccer abbrevs pass through
  return String(s).includes('_') ? String(s).split('_').map((w, i) => (i === 0 ? w.charAt(0).toUpperCase() + w.slice(1) : w.toUpperCase() + '.')).join(' ') : s
}
const NAME_MARKETS = /^(Match Winner|Set \d Winner|1X2 \(Match Result\)|Draw No Bet)$/
const cleanLegName = (s) => String(s || '').replace(/\s*\[[^\]]*\]\s*$/, '').trim()
const looksLikeName = (s) => !!s && s.length <= 40 && !/\?\s*$/.test(s) && !/^(will|does|is|are)\b/i.test(s)
function legsHeader(sport, a) {
  if (LOGO_SPORT[sport] || !NAME_MARKETS.test(a.market || '')) return null
  const ca = cleanLegName(a?.side_a?.outcome), cb = cleanLegName(a?.side_b?.outcome)
  return looksLikeName(ca) && looksLikeName(cb) ? `${ca} vs ${cb}` : null
}
function TeamChip({ sport, abbr }) {
  const [err, setErr] = useState(false)
  const lg = LOGO_SPORT[sport]
  const logo = lg && !err ? resolveTeamLogo(lg, abbr) : null
  return (
    <span className="inline-flex items-center gap-1">
      {logo && <img src={logo} alt="" onError={() => setErr(true)} className="h-5 w-5 flex-shrink-0 rounded-full bg-foreground/[0.06] object-contain p-px ring-1 ring-border" />}
      <span className="text-foreground/80">{lg ? abbr : prettyName(abbr)}</span>
    </span>
  )
}
function MatchupStrip({ sport, a }) {
  const names = legsHeader(sport, a)
  const tp = names ? null : teamPair(a)
  return (
    <div className="flex min-w-0 items-center gap-1.5 text-[11px] font-bold tracking-wide">
      <span className="flex-shrink-0 text-sm leading-none" aria-hidden="true">{SPORT_ICON[sport] || '•'}</span>
      {names ? <span className="truncate text-foreground/80">{names}</span>
        : tp ? (<><TeamChip sport={sport} abbr={tp.left} /><span className="font-normal text-muted-foreground/50">{tp.sep}</span><TeamChip sport={sport} abbr={tp.right} /></>)
        : <span className="truncate uppercase tracking-wider text-muted-foreground">{a.game}</span>}
    </div>
  )
}

// Non-prop market label, capitalized + de-duplicated ("Team Total TB (H1) 13.5").
function niceNonPropLabel(a) {
  const mk = titleizeMarket(a.market)
  let ln = String(a.line || '')
  const first = ln.split(/\s+/)[0] || ''
  const tok = first.replace(/[^A-Za-z]/g, '')
  if (tok && new RegExp('\\b' + tok + '\\b', 'i').test(mk)) ln = ln.slice(ln.indexOf(first) + first.length).trim()
  ln = ln.replace(/^team\s+/i, '')
  return (mk + (ln ? ' ' + ln : '')).replace(/\s+/g, ' ').trim()
}

// ---- stake math: split T so either outcome pays the same; profit = T/combined - T ----
function arbMath(a, T) {
  const ci = a.combined_implied
  const legs = [a.side_a, a.side_b, a.side_c].filter(Boolean)   // 1X2 arbs carry a third leg (univarb, 2026-10-31)
  const stakes = legs.map((s) => (ci && s?.price != null ? (T * s.price) / ci : null))
  const profit = ci ? (T * (1 - ci)) / ci : null
  // the most you can put through at these prices: an exchange leg's depth caps its stake
  const caps = legs.map((s) => (ci && s?.avail != null && s.price ? (s.avail * ci) / s.price : Infinity))
  const maxTotal = Math.min(...caps)
  return { ci, stakes, profit, maxTotal: Number.isFinite(maxTotal) ? maxTotal : null }
}

// ---- one fillable arb, in the PropPickCard visual language ----
function ArbCard({ sport, a, stake }) {
  const [photoErr, setPhotoErr] = useState(false)
  const pm = propMeta(a)
  const start = fmtStart(a.start_ms)
  const T = stake > 0 ? stake : 100
  const { ci, stakes, profit, maxTotal } = arbMath(a, T)
  const showPhoto = !!(pm && a.player_img && !photoErr)
  const pct = (a.margin_bps / 100).toFixed(1)
  return (
    <div className="group relative flex flex-col gap-3 rounded-xl border border-border border-l-4 border-l-positive bg-emerald-500/[0.03] p-4 transition-all duration-150 hover:-translate-y-px hover:bg-accent/40 hover:shadow-lg">
      <div className="flex items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <MatchupStrip sport={sport} a={a} />
          {start && <span className="flex-shrink-0 rounded-full bg-muted px-2 py-0.5 text-[10px] font-semibold text-muted-foreground">{start}</span>}
        </div>
        <span className="flex-shrink-0 rounded-md bg-emerald-500/15 px-2 py-1 text-xs font-bold tabular-nums text-positive" title={`combined ${ci?.toFixed(4)} — 1 minus that is the locked edge`}>+{pct}%</span>
      </div>

      <div className="flex items-center gap-3">
        {showPhoto ? (
          <img src={a.player_img} alt="" onError={() => setPhotoErr(true)} className="h-12 w-12 flex-shrink-0 rounded-full bg-muted object-cover ring-1 ring-border" />
        ) : pm ? (
          <div className="flex h-12 w-12 flex-shrink-0 items-center justify-center rounded-full bg-muted text-xs font-bold text-muted-foreground ring-1 ring-border">{(pm.player || '?').slice(0, 1)}</div>
        ) : null}
        <div className="min-w-0 flex-1">
          <div className="truncate text-sm font-bold text-foreground">{pm ? pm.player : niceNonPropLabel(a)}</div>
          <div className="truncate text-[11px] text-muted-foreground">
            {pm ? `${pm.stat}${pm.line ? ` · line ${pm.line}` : ''}` : [a.side_a, a.side_b, a.side_c].filter(Boolean).map(s => bookOf(s.book).label).join(' vs ')}
          </div>
        </div>
      </div>

      <div className="space-y-1.5">
        {[a.side_a, a.side_b, a.side_c].filter(Boolean).map((s, i) => <LegRow key={i} s={s} pm={pm} stake={stakes[i]} />)}
      </div>

      {a.note && <p className="text-[10px] leading-snug text-warning">{a.note}</p>}

      <div className="flex flex-wrap items-center justify-between gap-x-3 gap-y-1 rounded-md bg-background/50 px-3 py-2">
        <span className="text-[11px] text-muted-foreground">
          {money(T)} split → locks <span className="text-sm font-bold tabular-nums text-positive">+{money(profit ?? 0, 2)}</span>
        </span>
        <span className="text-[10px] tabular-nums text-muted-foreground">
          {ci ? `${((100 * (1 - ci)) / ci).toFixed(1)}% return` : ''}
          {maxTotal != null && <span className={cn('ml-2', maxTotal < T ? 'font-semibold text-warning' : '')}>max ~{money(Math.floor(maxTotal))} at these prices</span>}
        </span>
      </div>
    </div>
  )
}

function LegRow({ s, pm, stake }) {
  const b = bookOf(s.book)
  const odds = bookOdds(s)
  const raw = s.raw_price != null ? s.raw_price : s.price
  const hasFee = s.price != null && s.raw_price != null && Math.abs(s.price - s.raw_price) > 1e-9
  const short = s.avail != null && stake != null && stake > s.avail
  return (
    <div className="flex items-center justify-between gap-2 rounded-md border border-border bg-background/40 px-2.5 py-2">
      <div className="flex min-w-0 items-center gap-2">
        <span className={cn('flex-shrink-0 rounded px-1.5 py-0.5 text-[10px] font-bold ring-1', b.tone)}>{b.label}</span>
        <span className="truncate text-xs text-muted-foreground" title={s.outcome}>
          {b.verb} <span className="font-semibold text-foreground">{humanizeOutcome(s.outcome, pm)}</span>
        </span>
      </div>
      <div className="flex flex-shrink-0 items-center gap-2 tabular-nums">
        <span className="text-xs text-muted-foreground" title={raw != null ? `${(raw * 100).toFixed(1)}% implied${hasFee ? ` · ${(s.price * 100).toFixed(1)}% after fees (the arb math uses this)` : ''}` : undefined}>
          <span className="font-semibold text-foreground">{odds || '—'}</span>
          {hasFee && <span className="ml-0.5 text-[9px] text-amber-500/80">+fee</span>}
        </span>
        {stake != null && <span className={cn('min-w-[3.5rem] text-right text-sm font-bold', short ? 'text-warning' : 'text-foreground')} title={s.avail != null ? `${money(s.avail)} available at this price` : undefined}>{money(stake)}</span>}
      </div>
    </div>
  )
}

function UnitSize({ stake, setStake }) {
  return (
    <div className="flex flex-col gap-1.5 sm:items-end">
      <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">Total stake per arb</div>
      <div className="flex flex-wrap items-center gap-1.5 sm:justify-end">
        <div className="flex items-center rounded-md border border-border bg-background px-2">
          <span className="text-sm text-muted-foreground">$</span>
          <input
            type="number" min="0" step="5" value={stake}
            onChange={(e) => setStake(Math.max(0, Number(e.target.value) || 0))}
            className="w-20 bg-transparent px-1 py-1 text-sm tabular-nums text-foreground focus:outline-none"
          />
        </div>
        {UNIT_PRESETS.map((v) => (
          <button
            key={v} type="button" onClick={() => setStake(v)}
            className={cn('rounded-md px-2 py-1 text-xs font-semibold transition-colors', stake === v ? 'bg-primary/20 text-primary ring-1 ring-primary/40' : 'bg-background text-muted-foreground hover:bg-accent hover:text-foreground')}
          >{v >= 1000 ? `$${v / 1000}K` : `$${v}`}</button>
        ))}
      </div>
    </div>
  )
}

// ---- tracker (Austin 2026-10-27 "seeing how much I'd be missing out on would help") ----
// Every fillable arb the Pi has logged: first seen at its first price, refreshed each 15-min scan it
// survives. "Lasted 2+ scans" is the honest number -- a one-scan arb is often gone before both legs fill.
// Dollar figures scale the logged $100-stake profit to the unit size picked above.
function ArbTracker({ api, stake }) {
  const [t, setT] = useState(null)
  const [onlyPersisted, setOnlyPersisted] = useState(true)
  const [showAll, setShowAll] = useState(false)
  useEffect(() => {
    let alive = true
    const load = () => api.get('/arb/tracker', { params: { limit: 400 } })
      .then((r) => { if (alive) setT(r.data) })
      .catch(() => { /* tracker is optional; the live panel still works */ })
    load()
    const tmr = setInterval(load, 60000)
    return () => { alive = false; clearInterval(tmr) }
  }, [api])
  if (!t || !(t.windows || []).length) return null
  const k = (stake > 0 ? stake : 100) / 100
  const rows = (t.rows || []).filter((r) => !onlyPersisted || r.persisted)
  const shown = showAll ? rows : rows.slice(0, 25)
  const lasted = (r) => (r.scans <= 1 ? '1 scan' : r.lasted_min >= 90 ? `${(r.lasted_min / 60).toFixed(1)}h` : `${r.lasted_min}m`)
  return (
    <section className="overflow-hidden rounded-lg border border-border bg-card">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-border px-5 py-3">
        <div>
          <h3 className="text-sm font-semibold text-foreground">Arb Tracker</h3>
          <p className="text-[11px] text-muted-foreground">What every logged arb would have locked at {money(stake > 0 ? stake : 100)} total stake · since {fmtUpdated(t.since)}</p>
        </div>
        <div className="flex items-center gap-1 rounded-md border border-border p-0.5 text-[11px]">
          {[[true, 'Lasted 2+ scans'], [false, 'All']].map(([v, label]) => (
            <button key={label} onClick={() => setOnlyPersisted(v)}
              className={cn('rounded px-2 py-1 font-semibold', onlyPersisted === v ? 'bg-accent text-foreground' : 'text-muted-foreground hover:text-foreground')}>{label}</button>
          ))}
        </div>
      </div>
      <div className="grid grid-cols-1 gap-2 p-4 sm:grid-cols-3">
        {t.windows.map((w) => {
          const n = onlyPersisted ? w.persisted : w.n
          const p = (onlyPersisted ? w.persisted_profit_100 : w.profit_100) * k
          return (
            <div key={w.label} className="rounded-md border border-border bg-background/50 px-4 py-3">
              <div className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{w.label}</div>
              <div className={cn('mt-0.5 text-2xl font-bold tabular-nums', p > 0 ? 'text-positive' : 'text-foreground')}>+{money(p)}</div>
              <div className="text-[11px] tabular-nums text-muted-foreground">{n} arb{n === 1 ? '' : 's'}{onlyPersisted ? ` · of ${w.n} seen` : ` · ${w.persisted} lasted 2+ scans`}</div>
            </div>
          )
        })}
      </div>
      <div className="grid grid-cols-1 gap-4 px-4 pb-2 md:grid-cols-2">
        {[['By sport', t.by_sport, (key) => <><span className="mr-1" aria-hidden="true">{SPORT_ICON[key] || '•'}</span>{key}</>],
          ['By book pair', t.by_books, (key) => key.split('/').map((b) => bookOf(b).label).join(' / ')]].map(([title, list, fmt]) => (
          <div key={title}>
            <div className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{title}</div>
            <div className="divide-y divide-border rounded-md border border-border">
              {(list || []).slice(0, 8).map((a) => {
                const n = onlyPersisted ? a.persisted : a.n
                const p = (onlyPersisted ? a.persisted_profit_100 : a.profit_100) * k
                return (
                  <div key={a.key} className="flex items-center justify-between px-3 py-1.5 text-xs">
                    <span className="truncate text-foreground">{fmt(a.key)}</span>
                    <span className="whitespace-nowrap tabular-nums text-muted-foreground">{n} · <span className="font-semibold text-positive">+{money(p)}</span></span>
                  </div>
                )
              })}
            </div>
          </div>
        ))}
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="border-y border-border text-[10px] uppercase tracking-wider text-muted-foreground">
            <tr>
              <th className="px-4 py-2 text-left">First seen</th>
              <th className="px-2 py-2 text-left">Game</th>
              <th className="px-2 py-2 text-left">Market</th>
              <th className="px-2 py-2 text-left">Books</th>
              <th className="px-2 py-2 text-right">Margin</th>
              <th className="px-2 py-2 text-right">Locked</th>
              <th className="px-4 py-2 text-right">Lasted</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {shown.map((r, i) => (
              <tr key={`${r.first_seen}-${i}`} className="hover:bg-accent/30">
                <td className="whitespace-nowrap px-4 py-1.5 tabular-nums text-muted-foreground">{fmtUpdated(r.first_seen)}</td>
                <td className="whitespace-nowrap px-2 py-1.5 text-foreground"><span className="mr-1" aria-hidden="true">{SPORT_ICON[r.sport] || '•'}</span>{r.game}</td>
                <td className="max-w-[16rem] px-2 py-1.5 text-muted-foreground"><span className="block truncate" title={`${r.market} · ${r.line}`}><span className="text-foreground">{titleizeMarket(r.market)}</span> · {LOGO_SPORT[r.sport] ? titleCase(r.line) : r.line}</span></td>
                <td className="whitespace-nowrap px-2 py-1.5 text-muted-foreground">{String(r.books || '').split('/').map((b) => bookOf(b).label).join(' / ')}</td>
                <td className="px-2 py-1.5 text-right tabular-nums text-foreground">+{(r.first_margin_bps / 100).toFixed(1)}%</td>
                <td className="px-2 py-1.5 text-right font-semibold tabular-nums text-positive">+{money(r.profit_100 * k, 2)}</td>
                <td className="whitespace-nowrap px-4 py-1.5 text-right tabular-nums text-muted-foreground">
                  {r.live && <span className="mr-1.5 rounded-full bg-emerald-500/15 px-1.5 py-0.5 text-[9px] font-bold uppercase tracking-wider text-positive">live</span>}{lasted(r)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {rows.length > 25 && (
        <button onClick={() => setShowAll(!showAll)} className="w-full border-t border-border py-2 text-[11px] font-semibold text-muted-foreground hover:text-foreground">
          {showAll ? 'Show fewer' : `Show all ${rows.length}`}
        </button>
      )}
    </section>
  )
}

function readUnit() {
  try { const v = Number(window.localStorage.getItem('arb.unit')); return v > 0 ? v : 100 } catch (e) { return 100 }
}

export default function ArbPanel({ api }) {
  const [data, setData] = useState(null)
  const [err, setErr] = useState(null)
  const [stake, setStakeRaw] = useState(readUnit)
  const setStake = (v) => { setStakeRaw(v); try { window.localStorage.setItem('arb.unit', String(v)) } catch (e) { /* private mode */ } }

  useEffect(() => {
    let alive = true
    const load = () => api.get('/arb')
      .then((r) => { if (alive) { setData(r.data); setErr(null) } })
      .catch((e) => { if (alive) setErr(e?.message || String(e)) })
    load()
    const tmr = setInterval(load, 30000)
    return () => { alive = false; clearInterval(tmr) }
  }, [api])

  const sports = data?.sports || []
  const arbs = useMemo(() => sports
    .flatMap((s) => (s.fillable || []).map((a) => ({ sport: s.sport, a })))
    .sort((x, y) => (x.a.combined_implied ?? 9) - (y.a.combined_implied ?? 9)), [sports])   // most profit first
  const books = useMemo(() => {
    const set = new Set()
    for (const { a } of arbs) { if (a.side_a?.book) set.add(a.side_a.book); if (a.side_b?.book) set.add(a.side_b.book); if (a.side_c?.book) set.add(a.side_c.book) }
    return set
  }, [arbs])

  if (err) return <div className="py-12 text-center text-sm text-negative">Arb Scanner unavailable: {err}</div>
  if (!data) {
    return (
      <div className="flex items-center justify-center gap-3 py-12 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        Loading arb scanner…
      </div>
    )
  }

  const n = arbs.length
  const T = stake > 0 ? stake : 100
  const best = n ? arbs[0].a : null
  const lockTotal = arbs.reduce((sum, { a }) => sum + (arbMath(a, T).profit || 0), 0)
  const games = sports.reduce((x, s) => x + (s.games_checked || 0), 0)
  const crosses = sports.reduce((x, s) => x + (s.comparisons || 0), 0)

  return (
    <div className="space-y-5 py-4">
      {/* header */}
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-xl font-semibold tracking-tight text-foreground">Arb Scanner</h2>
          <p className="mt-0.5 text-xs text-muted-foreground">
            Both sides of one market on two different books · fee-adjusted · liquidity-gated · monitor only, nothing auto-places
          </p>
        </div>
        <div className="flex items-center gap-2 text-[11px] text-muted-foreground">
          <span className="tabular-nums">Updated {fmtUpdated(data.updated_at)}{data.age_seconds != null ? ` · ${fmtAge(data.age_seconds)}` : ''}</span>
          {data.stale && <span className="rounded-full bg-amber-500/15 px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-warning">stale</span>}
        </div>
      </div>

      {/* banner -- same skeleton as the props P/L banner */}
      <section className={cn('rounded-xl border-2 p-6', n > 0 ? 'border-emerald-500/30 bg-emerald-500/[0.06]' : 'border-border bg-card')}>
        <div className="flex flex-wrap items-end justify-between gap-5">
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-widest text-muted-foreground">Fillable arbs · right now</div>
            <div className={cn('mt-1 text-5xl font-bold leading-none tabular-nums', n > 0 ? 'text-positive' : 'text-muted-foreground')}>{n}</div>
            <div className="mt-2 flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted-foreground">
              {best ? (
                <>
                  <span>best <span className="font-semibold tabular-nums text-positive">+{(best.margin_bps / 100).toFixed(1)}%</span></span>
                  <span className="text-border">·</span>
                  <span>all of them at {money(T)} each lock <span className="font-semibold tabular-nums text-positive">+{money(lockTotal, 2)}</span></span>
                </>
              ) : <span>Books are efficient — every cross prices above breakeven after fees</span>}
              <span className="text-border">·</span>
              <span className="tabular-nums">{games} games · {crosses.toLocaleString('en-US')} crosses</span>
            </div>
          </div>
          <UnitSize stake={stake} setStake={setStake} />
        </div>

        {sports.length > 0 && (
          <div className="mt-5 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4 xl:grid-cols-6">
            {sports.map((s) => {
              const f = (s.fillable || []).length
              return (
                <div key={s.sport} className={cn('rounded-md border px-3 py-2.5', f > 0 ? 'border-emerald-500/30 bg-emerald-500/[0.06]' : 'border-border bg-background/50')}>
                  <div className="flex items-center gap-1.5 truncate text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                    <span aria-hidden="true">{SPORT_ICON[s.sport] || '•'}</span>{s.sport}
                  </div>
                  <div className={cn('mt-0.5 text-base font-semibold tabular-nums', f > 0 ? 'text-positive' : 'text-foreground')}>
                    {f} arb{f === 1 ? '' : 's'}
                  </div>
                  <div className="truncate text-[10px] tabular-nums text-muted-foreground">
                    {s.games_checked} game{s.games_checked === 1 ? '' : 's'} · {s.tightest?.combined_implied != null ? `tightest ${s.tightest.combined_implied.toFixed(3)}` : 'no cross'}
                  </div>
                </div>
              )
            })}
          </div>
        )}
      </section>

      {/* the arbs */}
      {n > 0 ? (
        <section className="space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <h3 className="text-sm font-semibold text-foreground">Live Arbs</h3>
            <span className="text-[10px] font-semibold uppercase tracking-wider text-amber-500/90">confirm both prices live before placing · re-check next scan</span>
          </div>
          <div className="grid grid-cols-1 gap-3 md:grid-cols-2 2xl:grid-cols-3">
            {arbs.map(({ sport, a }, i) => <ArbCard key={`${sport}-${i}-${a.combined_implied}`} sport={sport} a={a} stake={stake} />)}
          </div>
        </section>
      ) : (
        <div className="rounded-md border border-dashed border-border bg-card/50 px-4 py-8 text-center text-sm text-muted-foreground">
          No fillable arb right now. The tightest cross per sport is below; the next scan runs within 15 minutes.
        </div>
      )}

      <ArbTracker api={api} stake={stake} />

      {/* by sport */}
      <section className="overflow-hidden rounded-lg border border-border bg-card">
        <div className="flex items-center justify-between border-b border-border px-5 py-3">
          <h3 className="text-sm font-semibold text-foreground">Tightest Cross by Sport</h3>
          <span className="text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">{sports.length} sports scanned</span>
        </div>
        {sports.length > 0 ? (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="border-b border-border text-[10px] uppercase tracking-wider text-muted-foreground">
                <tr>
                  <th className="px-4 py-2 text-left">Sport</th>
                  <th className="px-2 py-2 text-right">Games</th>
                  <th className="px-2 py-2 text-right">Crosses</th>
                  <th className="px-2 py-2 text-left">Tightest market</th>
                  <th className="px-2 py-2 text-left">Books</th>
                  <th className="px-2 py-2 text-right">Combined</th>
                  <th className="px-4 py-2 text-right">Arbs</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {sports.map((s) => {
                  const t = s.tightest
                  const f = (s.fillable || []).length
                  const gap = t?.combined_implied != null ? (t.combined_implied - 1) * 100 : null
                  return (
                    <tr key={s.sport} className="hover:bg-accent/30">
                      <td className="whitespace-nowrap px-4 py-2 font-semibold text-foreground"><span className="mr-1.5" aria-hidden="true">{SPORT_ICON[s.sport] || '•'}</span>{s.sport}</td>
                      <td className="px-2 py-2 text-right tabular-nums text-muted-foreground">{s.games_checked}</td>
                      <td className="px-2 py-2 text-right tabular-nums text-muted-foreground">{(s.comparisons || 0).toLocaleString('en-US')}</td>
                      <td className="max-w-[18rem] px-2 py-2 text-muted-foreground">
                        {t ? <span className="block truncate" title={t.detail}><span className="text-foreground">{titleizeMarket(t.market)}</span>{t.detail ? ` · ${LOGO_SPORT[s.sport] ? titleCase(t.detail) : t.detail}` : ''}</span> : '—'}
                      </td>
                      <td className="whitespace-nowrap px-2 py-2 text-muted-foreground">{t?.books ? t.books.split('/').map((b) => bookOf(b).label).join(' / ') : '—'}</td>
                      <td className={cn('px-2 py-2 text-right font-semibold tabular-nums', gap != null && gap < 0 ? 'text-positive' : 'text-foreground')}>
                        {t?.combined_implied != null ? t.combined_implied.toFixed(3) : '—'}
                        {gap != null && <span className="ml-1 text-[10px] font-normal text-muted-foreground">{gap > 0 ? '+' : ''}{gap.toFixed(2)}pp</span>}
                      </td>
                      <td className={cn('px-4 py-2 text-right font-bold tabular-nums', f > 0 ? 'text-positive' : 'text-muted-foreground')}>{f}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        ) : (
          <div className="px-5 py-8 text-center text-sm text-muted-foreground">No sports scanned yet.</div>
        )}
      </section>

      {/* books in today's arbs + the fee model */}
      <div className="flex flex-wrap items-center gap-1.5 text-[11px] text-muted-foreground">
        {books.size > 0 && <span className="mr-1">Books in today's arbs:</span>}
        {[...books].map((b) => <span key={b} className={cn('rounded px-1.5 py-0.5 text-[10px] font-bold ring-1', bookOf(b).tone)}>{bookOf(b).label}</span>)}
      </div>
      {data.note && <p className="text-[11px] leading-relaxed text-muted-foreground">{data.note}</p>}
    </div>
  )
}
