import { memo } from 'react'
import { humanizeBetType } from '../../lib/betType'
import { cn } from '../../lib/utils'
import MatchupCell from './MatchupCell'


/**
 * Derive stake-units when the row doesn't carry an explicit value.
 * Mirror of engine._pick_helpers.stake_units_for so historical rows
 * recorded before the field existed still render a sensible stake.
 *
 *   lean     prob 0.52-0.58, low edge          → 0.25u
 *   moderate prob 0.58+ OR (prob 0.54+ AND edge ≥ 4)  → 0.5u
 *   strong   prob 0.65+ OR (prob 0.58+ AND edge ≥ 7)   → 1.0u
 *   premium  strong AND prob ≥ 0.65 AND edge ≥ 10      → 1.5u
 *
 * EDGE_STRONG / EDGE_MODERATE thresholds taken from the Python config
 * defaults (engine/config.py) — keep in sync.
 */
function deriveStakeUnits(prob, edge) {
  if (prob == null) return null
  const p = Number(prob)
  const e = Number(edge ?? 0)
  if (p < 0.52) return 0
  if (p >= 0.65 || (p >= 0.58 && e >= 7)) {
    return p >= 0.65 && e >= 10 ? 1.5 : 1
  }
  if (p >= 0.58 || (p >= 0.54 && e >= 4)) return 0.5
  return 0.25
}

function formatStakeUnits(u) {
  if (u == null || u <= 0) return '-'
  return u % 1 === 0 ? `${u}u` : `${u}u`
}

/**
 * PicksTable — shared table primitive for PickHistory + DerivativeTracker.
 *
 * Both consumers render the same shape (Date / Matchup / Type / Pick /
 * Odds / Prob / Edge / Result / P/L), so duplicating the table markup
 * across two files cost us a styling fork. This primitive is the
 * single source of truth — both trackers now compose it.
 *
 * Phase 2d-iii: full Tailwind, design tokens, tabular-nums on every
 * numeric column, semantic row tints (wins/losses/pending) instead of
 * the legacy `.row-win` / `.row-loss` CSS-module classes.
 *
 * Props:
 *   picks              - array of pick rows from /tracker/history
 *   typeColumnLabel    - 'Type' (PickHistory) or 'Market' (DerivativeTracker)
 *   profitFormatter    - fn(profit: number|null) => string. Defaults to
 *                        whole-dollar (PickHistory). DerivativeTracker
 *                        passes a 2-decimal version.
 */
function PicksTableImpl({
  picks,
  typeColumnLabel = 'Type',
  profitFormatter = defaultProfit,
  sport,
}) {
  const pct = n => n != null ? `${(n * 100).toFixed(1)}%` : '-'

  // American odds → implied prob → CLV % swing.
  const clvFor = (p) => {
    if (p.odds == null || p.closing_odds == null) return null
    const imp = (o) => o < 0 ? Math.abs(o) / (Math.abs(o) + 100) : 100 / (o + 100)
    return Math.round((imp(p.closing_odds) - imp(p.odds)) * 1000) / 10
  }

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/40">
            <Th>Date</Th>
            <Th>Matchup</Th>
            <Th>{typeColumnLabel}</Th>
            <Th>Pick</Th>
            <Th align="right">Stake</Th>
            <Th align="right">Odds</Th>
            <Th align="right">Prob</Th>
            <Th align="right">Edge</Th>
            <Th align="right">CLV</Th>
            <Th align="center">Result</Th>
            <Th align="right">P/L</Th>
          </tr>
        </thead>
        <tbody>
          {picks.map((p, i) => {
            const rowTint = p.result === 'W' ? 'bg-positive/[0.04]'
              : p.result === 'L' ? 'bg-negative/[0.04]'
              : !p.result ? 'bg-warning/[0.03]'
              : ''
            const profitTone = p.profit > 0
              ? 'text-positive'
              : p.profit < 0 ? 'text-negative' : 'text-foreground'
            const edgeTone = (p.edge || 0) > 4 ? 'text-positive' : 'text-foreground'
            const clv = clvFor(p)
            const clvTone = clv == null ? 'text-muted-foreground'
              : clv > 0 ? 'text-positive'
              : clv < 0 ? 'text-negative' : 'text-foreground'
            return (
              <tr
                key={p.id || i}
                className={cn(
                  'border-b border-border/60 hover:bg-accent/30 transition-colors',
                  rowTint,
                )}
              >
                <Td className="text-xs text-muted-foreground whitespace-nowrap">
                  {p.date?.slice(5) || '-'}
                </Td>
                <Td className="font-medium text-foreground max-w-[14rem]">
                  <MatchupCell matchup={p.matchup} sport={sport} homeLogo={p.home_logo} awayLogo={p.away_logo} />
                </Td>
                <Td>
                  <TypeBadge>{humanizeBetType(p.bet_type)}</TypeBadge>
                </Td>
                <Td className="font-semibold text-foreground">{p.pick}</Td>
                <Td align="right" className="tabular-nums text-muted-foreground">
                  {formatStakeUnits(
                    p.stake_units != null
                      ? p.stake_units
                      : deriveStakeUnits(p.model_prob, p.edge)
                  )}
                </Td>
                <Td align="right" className="tabular-nums text-muted-foreground">
                  {p.odds ? `${p.odds > 0 ? '+' : ''}${p.odds}` : '-'}
                </Td>
                <Td align="right" className="tabular-nums">
                  {p.model_prob ? pct(p.model_prob) : '-'}
                </Td>
                <Td align="right" className={cn('tabular-nums font-semibold', edgeTone)}>
                  {p.edge ? `+${p.edge.toFixed(1)}%` : '-'}
                </Td>
                <Td align="right" className={cn('tabular-nums', clvTone)}
                    title={clv == null ? 'No closing line captured' : `Closing odds: ${p.closing_odds > 0 ? '+' : ''}${p.closing_odds}`}>
                  {clv == null ? '-' : `${clv > 0 ? '+' : ''}${clv}%`}
                </Td>
                <Td align="center">
                  <ResultPill result={p.result} />
                </Td>
                <Td align="right" className={cn('tabular-nums font-semibold', profitTone)}>
                  {p.profit != null
                    ? `${p.profit > 0 ? '+' : ''}${profitFormatter(p.profit)}`
                    : '-'}
                </Td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

const PicksTable = memo(PicksTableImpl)
export default PicksTable


function Th({ children, align = 'left' }) {
  return (
    <th
      scope="col"
      className={cn(
        'px-3 py-2.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground',
        align === 'right' && 'text-right',
        align === 'center' && 'text-center',
      )}
    >
      {children}
    </th>
  )
}

function Td({ children, align = 'left', className }) {
  return (
    <td
      className={cn(
        'px-3 py-2.5',
        align === 'right' && 'text-right',
        align === 'center' && 'text-center',
        className,
      )}
    >
      {children}
    </td>
  )
}

function TypeBadge({ children }) {
  return (
    <span className="inline-block rounded-sm bg-secondary px-2 py-0.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
      {children}
    </span>
  )
}

function ResultPill({ result }) {
  const config = {
    W:    { label: 'W',    cls: 'bg-positive/15 text-positive' },
    L:    { label: 'L',    cls: 'bg-negative/15 text-negative' },
    P:    { label: 'P',    cls: 'bg-muted text-muted-foreground' },
    // 'V' = voided (phantom-bracket pick that didn't actually play, or
    // postponement past the settle window). Functionally a refund;
    // distinct from push so the user can tell them apart at a glance.
    V:    { label: 'VOID', cls: 'bg-muted text-muted-foreground/70' },
  }
  const c = config[result] ?? { label: 'PEND', cls: 'bg-warning/10 text-warning' }
  return (
    <span
      className={cn(
        'inline-block min-w-[34px] rounded-md px-2 py-0.5 text-center text-[11px] font-bold tracking-wide',
        c.cls,
      )}
    >
      {c.label}
    </span>
  )
}

function defaultProfit(n) {
  // PickHistory uses whole-dollar (rounded). Tennis was returning raw
  // floats like 60.60606060606061 which overflowed the P/L cell and
  // line-wrapped — the + ended up on its own line. Rounding fixes both.
  return `$${Math.round(n)}`
}
