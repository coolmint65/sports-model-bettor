/**
 * TennisMatchDetail — modal-style detail pane for a tennis match card.
 *
 * Extracted from TennisPanel 2026-07-08 (Size Crusade / whole-system
 * refactor). All rendering primitives (DetailPlayerBlock, H2HBlock,
 * FormBlock, MarketRow) live here since they're only used by this
 * detail view; keeps the API surface minimal.
 */
import { useEffect } from 'react'
import { User } from 'lucide-react'
import { cn } from '../../lib/utils'


export default function TennisMatchDetail({ api, match, onClose }) {
  const pred = match.prediction || {}
  const picks = match.picks || []
  const odds = match.odds || {}
  const markets = odds.markets || {}
  const p1 = match.p1_name
  const p2 = match.p2_name

  // Esc to close + scroll lock while open.
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose?.() }
    window.addEventListener('keydown', onKey)
    const prev = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    return () => {
      window.removeEventListener('keydown', onKey)
      document.body.style.overflow = prev
    }
  }, [onClose])

  const pct = (n) => n != null ? `${(n * 100).toFixed(1)}%` : '—'
  const fmtOdds = (n) => n == null ? '-' : `${n > 0 ? '+' : ''}${Math.round(n)}`

  return (
    <div
      className="fixed inset-0 z-50 flex items-start justify-center overflow-y-auto bg-background/80 backdrop-blur-sm p-4 sm:p-6"
      onClick={onClose}
    >
      <div
        className="relative w-full max-w-3xl rounded-xl border border-border bg-card shadow-2xl my-4"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute top-3 right-3 rounded-md p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
          aria-label="Close"
        >
          ×
        </button>

        {/* Header — tournament + round + surface + status */}
        <div className="border-b border-border px-6 py-4">
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">
            {match.tournament}{match.round ? ` · ${match.round}` : ''}
            {match.surface ? ` · ${match.surface}` : ''}
            {' · '}BO{match.best_of || 3}
          </div>
          <h2 className="mt-1 text-xl font-bold">
            {p1} <span className="text-muted-foreground font-normal">vs</span> {p2}
          </h2>
          {match.score && (
            <div className="mt-2">
              <SetScoreGrid
                score={match.score}
                p1Name={p1}
                p2Name={p2}
                winner={match.winner}
              />
            </div>
          )}
        </div>

        {/* Players — large headshots + Elo + win prob */}
        <div className="grid grid-cols-2 gap-3 px-6 py-4">
          <DetailPlayerBlock
            name={p1} country={match.p1_country}
            image={match.p1_image} flag={match.p1_flag}
            prob={pred.p1_win_prob} elo={pred.p1_rating}
          />
          <DetailPlayerBlock
            name={p2} country={match.p2_country}
            image={match.p2_image} flag={match.p2_flag}
            prob={pred.p2_win_prob} elo={pred.p2_rating}
          />
        </div>

        {/* Win-prob bar (large detail variant) */}
        {pred.p1_win_prob != null && pred.p2_win_prob != null && (
          <div className="px-6 pb-4">
            <div className="space-y-2">
              <div className="flex justify-between text-sm tabular-nums">
                <span className="font-bold text-foreground">{p1} {pct(pred.p1_win_prob)}</span>
                <span className="text-muted-foreground">{p2} {pct(pred.p2_win_prob)}</span>
              </div>
              <div className="flex h-3 overflow-hidden rounded-full bg-secondary">
                <div className="bg-warning" style={{ width: pct(pred.p1_win_prob) }} />
                <div className="bg-primary" style={{ width: pct(pred.p2_win_prob) }} />
              </div>
            </div>
          </div>
        )}

        {/* Head-to-head + recent form context. Both fall back to a
            "no data" message when the player history is too thin —
            the picker still works without H2H, this is just operator
            colour. */}
        <div className="border-t border-border px-6 py-4 grid grid-cols-1 sm:grid-cols-3 gap-3">
          <H2HBlock h2h={match.h2h} p1Name={p1} p2Name={p2} />
          <FormBlock label={`${p1} form`} form={match.p1_form} surface={match.surface} />
          <FormBlock label={`${p2} form`} form={match.p2_form} surface={match.surface} />
        </div>

        {/* Picks list — all picks, not just best */}
        <div className="border-t border-border px-6 py-4">
          <h3 className="text-sm font-semibold mb-2">Model picks ({picks.length})</h3>
          {picks.length === 0 && (
            <div className="text-xs text-muted-foreground">
              No edge above floor for any market — model has no qualifying play.
            </div>
          )}
          {picks.length > 0 && (
            <div className="space-y-1.5">
              {picks.map((p, i) => {
                const conf = p.confidence || 'lean'
                const tone = {
                  strong:   'bg-positive/10 text-positive border-positive/30',
                  moderate: 'bg-primary/10 text-primary border-primary/30',
                  lean:     'bg-muted/40 text-foreground/85 border-border',
                }[conf]
                return (
                  <div key={`${p.type}:${p.pick}:${i}`}
                       className={cn('flex items-center gap-2 rounded-md border px-3 py-1.5 text-sm', tone)}>
                    <span className="text-[10px] font-semibold uppercase tracking-wider opacity-75 w-24 shrink-0">
                      {prettyTennisType(p.type)}
                    </span>
                    <span className="font-bold truncate flex-1">{p.pick}</span>
                    <span className="tabular-nums text-xs">{fmtOdds(p.odds)}</span>
                    <span className="tabular-nums text-xs">{pct(p.model_prob)}</span>
                    <span className="tabular-nums text-xs font-bold w-14 text-right">
                      +{(p.edge || 0).toFixed(1)}%
                    </span>
                  </div>
                )
              })}
            </div>
          )}
        </div>

        {/* Markets dump — every HR market we got, even if no edge */}
        <div className="border-t border-border px-6 py-4">
          <h3 className="text-sm font-semibold mb-2">Hard Rock markets</h3>
          {Object.keys(markets).length === 0 && (
            <div className="text-xs text-muted-foreground">
              No HR markets matched for this match.
            </div>
          )}
          {Object.keys(markets).length > 0 && (
            <div className="grid grid-cols-1 sm:grid-cols-2 gap-2 text-[12px]">
              {markets.ml && (
                <MarketRow label="ML">
                  <span>{p1} {fmtOdds(markets.ml.p1_odds)}</span>
                  <span>{p2} {fmtOdds(markets.ml.p2_odds)}</span>
                </MarketRow>
              )}
              {markets.total_games?.line != null && (
                <MarketRow label="Total Games">
                  <span>o{markets.total_games.line} {fmtOdds(markets.total_games.over_odds)}</span>
                  <span>u{markets.total_games.line} {fmtOdds(markets.total_games.under_odds)}</span>
                </MarketRow>
              )}
              {markets.total_sets?.line != null && (
                <MarketRow label="Total Sets">
                  <span>o{markets.total_sets.line} {fmtOdds(markets.total_sets.over_odds)}</span>
                  <span>u{markets.total_sets.line} {fmtOdds(markets.total_sets.under_odds)}</span>
                </MarketRow>
              )}
              {Array.isArray(markets.set_spread) && markets.set_spread.length > 0 && (
                <MarketRow label="Set Spread">
                  {markets.set_spread.map((s, i) => (
                    <span key={i}>
                      {s.player === 'p1' ? p1 : p2} {fmtSigned(s.point)} {fmtOdds(s.odds)}
                    </span>
                  ))}
                </MarketRow>
              )}
              {markets.p1_total_games?.line != null && (
                <MarketRow label={`${p1} Games`}>
                  <span>o{markets.p1_total_games.line} {fmtOdds(markets.p1_total_games.over_odds)}</span>
                  <span>u{markets.p1_total_games.line} {fmtOdds(markets.p1_total_games.under_odds)}</span>
                </MarketRow>
              )}
              {markets.p2_total_games?.line != null && (
                <MarketRow label={`${p2} Games`}>
                  <span>o{markets.p2_total_games.line} {fmtOdds(markets.p2_total_games.over_odds)}</span>
                  <span>u{markets.p2_total_games.line} {fmtOdds(markets.p2_total_games.under_odds)}</span>
                </MarketRow>
              )}
              {markets.p1_win_at_least_one_set && (
                <MarketRow label={`${p1} 1+ Set`}>
                  <span>Yes {fmtOdds(markets.p1_win_at_least_one_set.yes_odds)}</span>
                  <span>No {fmtOdds(markets.p1_win_at_least_one_set.no_odds)}</span>
                </MarketRow>
              )}
              {markets.p2_win_at_least_one_set && (
                <MarketRow label={`${p2} 1+ Set`}>
                  <span>Yes {fmtOdds(markets.p2_win_at_least_one_set.yes_odds)}</span>
                  <span>No {fmtOdds(markets.p2_win_at_least_one_set.no_odds)}</span>
                </MarketRow>
              )}
              {markets.set_betting && Object.keys(markets.set_betting).length > 0 && (
                <MarketRow label="Set Betting" wide>
                  {Object.entries(markets.set_betting).map(([score, o]) => (
                    <span key={score} className="font-mono">{score} {fmtOdds(o)}</span>
                  ))}
                </MarketRow>
              )}
              {markets.most_games && (
                <MarketRow label="Most Games">
                  <span>{p1} {fmtOdds(markets.most_games.p1_odds)}</span>
                  <span>{p2} {fmtOdds(markets.most_games.p2_odds)}</span>
                  {markets.most_games.tie_odds != null && (
                    <span>Tie {fmtOdds(markets.most_games.tie_odds)}</span>
                  )}
                </MarketRow>
              )}
            </div>
          )}
        </div>

        {/* Footer time + venue */}
        <div className="border-t border-border px-6 py-3 text-[11px] text-muted-foreground flex items-center justify-between">
          <span className="tabular-nums">
            {match.start_time
              ? new Date(match.start_time).toLocaleString([], {
                  weekday: 'short', month: 'short', day: 'numeric',
                  hour: 'numeric', minute: '2-digit',
                })
              : ''}
          </span>
          <span className="uppercase tracking-wider">
            {match.tour?.toUpperCase()} · {match.surface || ''}
          </span>
        </div>
      </div>
    </div>
  )
}


function DetailPlayerBlock({ name, country, image, flag, prob, elo }) {
  return (
    <div className="rounded-md border border-border bg-card/50 p-3">
      <div className="flex items-center gap-3">
        <PlayerAvatar image={image} flag={flag} name={name} />
        <div className="min-w-0">
          <div className="text-base font-bold truncate">{name}</div>
          <div className="text-[10px] uppercase tracking-wider text-muted-foreground">
            {country || '—'}
            {elo != null && <> · Elo {Math.round(elo)}</>}
          </div>
        </div>
      </div>
      {prob != null && (
        <div className="mt-2 text-2xl font-bold tabular-nums">
          {(prob * 100).toFixed(1)}%
        </div>
      )}
    </div>
  )
}


function H2HBlock({ h2h, p1Name, p2Name }) {
  if (!h2h || !h2h.total) {
    return (
      <div className="rounded-md border border-border bg-background/40 p-3">
        <div className="text-[9px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
          Head-to-head
        </div>
        <div className="text-xs text-muted-foreground">No prior meetings on file.</div>
      </div>
    )
  }
  const lead = h2h.p1_wins > h2h.p2_wins ? p1Name
    : h2h.p2_wins > h2h.p1_wins ? p2Name : null
  const lastName = (n) => (n || '').split(' ').slice(-1)[0]
  return (
    <div className="rounded-md border border-border bg-background/40 p-3">
      <div className="text-[9px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
        Head-to-head
      </div>
      <div className="text-sm font-semibold">
        {lead ? <>{lastName(lead)} leads </> : 'Tied '}
        <span className="tabular-nums">{h2h.p1_wins}-{h2h.p2_wins}</span>
        <span className="text-muted-foreground"> ({h2h.total} match{h2h.total === 1 ? '' : 'es'})</span>
      </div>
      {Array.isArray(h2h.last_n) && h2h.last_n.length > 0 && (
        <ul className="mt-2 space-y-0.5 text-[11px] text-muted-foreground">
          {h2h.last_n.slice(0, 3).map((m, i) => (
            <li key={i} className="tabular-nums">
              <span className="text-foreground/80">{m.tourney_date}</span>
              {' · '}{m.tourney_name}
              {m.surface && <> · {m.surface}</>}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}


function FormBlock({ label, form, surface }) {
  if (!form) {
    return (
      <div className="rounded-md border border-border bg-background/40 p-3">
        <div className="text-[9px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
          {label}
        </div>
        <div className="text-xs text-muted-foreground">No recent matches.</div>
      </div>
    )
  }
  return (
    <div className="rounded-md border border-border bg-background/40 p-3">
      <div className="text-[9px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
        {label} {surface && <span className="opacity-60">({surface})</span>}
      </div>
      <div className="text-sm font-semibold">
        <span className="tabular-nums">{form.record}</span>
        <span className="text-muted-foreground"> last {form.last_n?.length || 0}</span>
      </div>
      {Array.isArray(form.last_n) && form.last_n.length > 0 && (
        <ul className="mt-2 space-y-0.5 text-[11px] text-muted-foreground">
          {form.last_n.slice(0, 3).map((m, i) => (
            <li key={i} className="tabular-nums">
              <span className="text-foreground/80">{m.tourney_date}</span>
              {' · '}{m.tourney_name}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}


function MarketRow({ label, children, wide }) {
  return (
    <div className={cn(
      'rounded-md border border-border bg-background/40 p-2',
      wide && 'sm:col-span-2',
    )}>
      <div className="text-[9px] font-bold uppercase tracking-widest text-muted-foreground mb-1">
        {label}
      </div>
      <div className="flex flex-wrap gap-x-3 gap-y-0.5 tabular-nums">
        {children}
      </div>
    </div>
  )
}
