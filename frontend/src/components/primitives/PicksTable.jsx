import { memo } from 'react'
import { humanizeBetType } from '../../lib/betType'
import { cn } from '../../lib/utils'

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
}) {
  const pct = n => n != null ? `${(n * 100).toFixed(1)}%` : '-'

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-background/40">
            <Th>Date</Th>
            <Th>Matchup</Th>
            <Th>{typeColumnLabel}</Th>
            <Th>Pick</Th>
            <Th align="right">Odds</Th>
            <Th align="right">Prob</Th>
            <Th align="right">Edge</Th>
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
                <Td className="font-medium text-foreground whitespace-nowrap">
                  {p.matchup}
                </Td>
                <Td>
                  <TypeBadge>{humanizeBetType(p.bet_type)}</TypeBadge>
                </Td>
                <Td className="font-semibold text-foreground">{p.pick}</Td>
                <Td align="right" className="tabular-nums text-muted-foreground">
                  {p.odds ? `${p.odds > 0 ? '+' : ''}${p.odds}` : '-'}
                </Td>
                <Td align="right" className="tabular-nums">
                  {p.model_prob ? pct(p.model_prob) : '-'}
                </Td>
                <Td align="right" className={cn('tabular-nums font-semibold', edgeTone)}>
                  {p.edge ? `+${p.edge.toFixed(1)}%` : '-'}
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
  // PickHistory uses whole-dollar; DerivativeTracker overrides with .toFixed(2)
  return `$${n}`
}
