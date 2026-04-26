import { memo } from 'react'
import { cn } from '../../lib/utils'

/**
 * StandingsTable — shared primitive for MLB / NHL / NBA standings.
 *
 * The three sport-specific Standings pages used near-identical
 * markup with different column sets. This primitive accepts a
 * column spec + spot-rule and renders the whole page (header,
 * legend, conference sections, division cards). Adding a new
 * sport later is a config edit.
 *
 * Props:
 *   title          - "MLB Standings" / "NHL Standings" / "NBA Standings"
 *   divisions      - [{ name|division, teams: [...], league?, ... }]
 *   loading        - boolean
 *   loadingLabel   - "Loading NHL standings…"
 *   emptyMessage   - first line of empty state
 *   emptyHint      - second line (optional, may include a code block)
 *   conferences    - array of { label, divisionNames } to group divisions.
 *                    e.g. [{label:'Eastern', divisionNames:['Atlantic','Metropolitan']}]
 *                    Pass [] to render all divisions ungrouped.
 *   spotRule       - (rankIdx, teamCount) => 'division' | 'wildcard' | 'out'
 *   spotLegend     - { division, wildcard, out } labels for the legend
 *   columns        - column spec, see below
 *   showLogo       - whether to render team logos in the team column
 *   getDivKey      - extract a stable key from a division row
 *                    (default: d.name || d.division)
 *
 * Column spec entry:
 *   { label: 'W', value: t => t.wins, format?: 'signed'|'streak', emphasis?: bool }
 *   - format='signed' colors positive/negative + adds + prefix
 *   - format='streak' colors W-prefixed positive, L-prefixed negative
 *   - emphasis bolds the cell (PCT / PTS column convention)
 */
function StandingsTableImpl({
  title,
  divisions,
  loading,
  loadingLabel = 'Loading standings…',
  emptyMessage = 'No standings data available.',
  emptyHint,
  conferences,
  spotRule,
  spotLegend = { division: 'Division leader', wildcard: 'Wild card', out: 'Out of playoffs' },
  columns,
  showLogo = false,
  getDivKey = d => d.name || d.division,
}) {
  if (loading) {
    return (
      <div className="flex items-center justify-center gap-3 py-16 text-sm text-muted-foreground">
        <div className="h-4 w-4 animate-spin rounded-full border-2 border-muted-foreground border-t-transparent" />
        {loadingLabel}
      </div>
    )
  }

  if (!divisions || divisions.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">{emptyMessage}</div>
        {emptyHint && (
          <div className="mt-1 text-xs text-muted-foreground">{emptyHint}</div>
        )}
      </div>
    )
  }

  const filled = divisions.filter(d => (d.teams || []).length > 0)
  if (filled.length === 0) {
    return (
      <div className="rounded-lg border border-dashed border-border bg-card/50 px-6 py-10 text-center">
        <div className="text-sm font-semibold text-foreground">
          Standings data is empty — season may not have started yet.
        </div>
      </div>
    )
  }

  // Group divisions per the conference spec; anything not in a named
  // group lands in the trailing "other" pile.
  const groups = []
  const used = new Set()
  if (conferences && conferences.length) {
    for (const conf of conferences) {
      const divs = filled.filter(d => conf.divisionNames.includes(getDivKey(d)))
      divs.forEach(d => used.add(getDivKey(d)))
      if (divs.length) groups.push({ label: conf.label, divs })
    }
  }
  const other = filled.filter(d => !used.has(getDivKey(d)))
  if (other.length) groups.push({ label: null, divs: other })

  return (
    <div className="space-y-5 py-4">
      <div>
        <h2 className="text-xl font-semibold tracking-tight text-foreground">
          {title}
        </h2>
      </div>

      <div className="flex flex-wrap items-center gap-x-5 gap-y-2 rounded-md border border-border bg-card px-4 py-2.5 text-xs text-muted-foreground">
        <LegendDot tone="positive" label={spotLegend.division} />
        <LegendDot tone="warning"  label={spotLegend.wildcard} />
        <LegendDot tone="negative" label={spotLegend.out} />
      </div>

      {groups.map((group, gi) => (
        <section key={group.label || `g${gi}`} className="space-y-3">
          {group.label && (
            <h3 className="text-[11px] font-semibold uppercase tracking-widest text-muted-foreground">
              {group.label}
            </h3>
          )}
          <div className="grid grid-cols-1 gap-3 lg:grid-cols-2">
            {group.divs.map(div => (
              <DivisionCard
                key={getDivKey(div)}
                div={div}
                divKey={getDivKey(div)}
                spotRule={spotRule}
                columns={columns}
                showLogo={showLogo}
              />
            ))}
          </div>
        </section>
      ))}
    </div>
  )
}

const StandingsTable = memo(StandingsTableImpl)
export default StandingsTable


function LegendDot({ tone, label }) {
  const cls =
    tone === 'positive' ? 'bg-positive' :
    tone === 'warning'  ? 'bg-warning'  :
                          'bg-negative'
  return (
    <div className="flex items-center gap-1.5">
      <span className={cn('inline-block h-2 w-2 rounded-full', cls)} />
      <span>{label}</span>
    </div>
  )
}

const SPOT_TINT = {
  division: 'border-l-positive/70',
  wildcard: 'border-l-warning/70',
  out:      'border-l-negative/40',
}

function DivisionCard({ div, divKey, spotRule, columns, showLogo }) {
  const teams = div.teams || []
  return (
    <div className="rounded-lg border border-border bg-card overflow-hidden">
      <div className="flex items-center justify-between border-b border-border px-4 py-2.5">
        <h4 className="text-sm font-semibold text-foreground">{divKey}</h4>
        <span className="text-[10px] tabular-nums text-muted-foreground">
          {teams.length} teams
        </span>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-border bg-background/40">
              <Th className="w-8 text-center">#</Th>
              <Th>Team</Th>
              {columns.map(c => (
                <Th key={c.label} align="right">{c.label}</Th>
              ))}
            </tr>
          </thead>
          <tbody>
            {teams.map((team, i) => {
              const spot = spotRule(i, teams.length)
              const tint = SPOT_TINT[spot] || 'border-l-transparent'
              return (
                <tr
                  key={team.id || team.abbreviation || i}
                  className={cn(
                    'border-b border-border/60 border-l-[3px] hover:bg-accent/30 transition-colors',
                    tint,
                  )}
                >
                  <td className="w-8 px-2 py-2 text-center text-[11px] tabular-nums text-muted-foreground">
                    {i + 1}
                  </td>
                  <td className="px-2 py-2">
                    <div className="flex items-center gap-2 min-w-0">
                      {showLogo && team.logo && (
                        <img
                          src={team.logo}
                          alt=""
                          className="h-4 w-4 flex-shrink-0 object-contain"
                        />
                      )}
                      <span className="font-bold text-foreground tabular-nums">
                        {team.abbreviation}
                      </span>
                      <span className="text-muted-foreground truncate hidden md:inline">
                        {team.name}
                      </span>
                    </div>
                  </td>
                  {columns.map(c => {
                    const raw = c.value(team)
                    const { text, tone } = formatCell(raw, c.format)
                    return (
                      <td
                        key={c.label}
                        className={cn(
                          'px-2 py-2 text-right tabular-nums',
                          tone === 'positive' && 'text-positive font-semibold',
                          tone === 'negative' && 'text-negative font-semibold',
                          c.emphasis && 'font-bold text-foreground',
                          !tone && !c.emphasis && 'text-foreground/85',
                        )}
                      >
                        {text}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}

function Th({ children, align = 'left', className }) {
  return (
    <th
      scope="col"
      className={cn(
        'px-2 py-2 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground',
        align === 'right' && 'text-right',
        align === 'center' && 'text-center',
        className,
      )}
    >
      {children}
    </th>
  )
}

function formatCell(value, format) {
  if (value == null || value === '') return { text: '-', tone: null }
  if (format === 'signed') {
    const n = typeof value === 'number' ? value : parseFloat(value)
    if (Number.isNaN(n)) return { text: String(value), tone: null }
    const tone = n > 0 ? 'positive' : n < 0 ? 'negative' : null
    return { text: `${n > 0 ? '+' : ''}${n}`, tone }
  }
  if (format === 'streak') {
    const s = String(value)
    const tone = s.startsWith('W') ? 'positive' : s.startsWith('L') ? 'negative' : null
    return { text: s, tone }
  }
  return { text: String(value), tone: null }
}
