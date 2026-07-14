/**
 * Sport-specific sidebar groups.
 *
 * Each one plugs a per-sport grouping strategy (region / confederation /
 * flat sort) into <ExpandableGroup>. The parent shell + child button
 * both live in shared files — the group components here are thin.
 */
import { groupByRegion } from '../../lib/basketballNav'
import ExpandableGroup from './ExpandableGroup'
import LeagueButton from './LeagueButton'


// Shared sort — in-season first, then alpha by display_name.
function seasonAlpha(a, b) {
  if (a.in_season !== b.in_season) return a.in_season ? -1 : 1
  return (a.display_name || a.key).localeCompare(b.display_name || b.key)
}


// ── Basketball ────────────────────────────────────────────────
// AFL rides the basketball framework backend but renders as its own
// top-level sidebar entry — exclude it from the Basketball nest so it
// doesn't appear twice.

export function BasketballGroup({
  expanded, onToggleExpanded, selected, onSelect, leagues, gameCount,
}) {
  const filtered = leagues.filter(L => L.key !== 'afl')
  const grouped = groupByRegion(filtered)
  return (
    <ExpandableGroup
      icon="🏀" label="Basketball"
      expanded={expanded} onToggleExpanded={onToggleExpanded}
      isAnyChildActive={filtered.some(L => L.key === selected)}
      gameCount={gameCount} spacing="space-y-1.5"
    >
      {grouped.map(({ region, leagues: regionLeagues }) => (
        <RegionSection
          key={region} region={region} leagues={regionLeagues}
          selected={selected} onSelect={onSelect}
        />
      ))}
    </ExpandableGroup>
  )
}


// ── Hockey ────────────────────────────────────────────────────

export function HockeyGroup({
  expanded, onToggleExpanded, selected, onSelect, leagues, gameCount,
}) {
  const sorted = [...leagues].sort(seasonAlpha)
  return (
    <ExpandableGroup
      icon="🏒" label="Hockey"
      expanded={expanded} onToggleExpanded={onToggleExpanded}
      isAnyChildActive={leagues.some(L => L.key === selected)}
      gameCount={gameCount}
    >
      {sorted.map(L => (
        <LeagueButton
          key={L.key} league={L}
          isActive={selected === L.key}
          onSelect={onSelect}
        />
      ))}
    </ExpandableGroup>
  )
}


// ── Soccer ────────────────────────────────────────────────────
// Grouped by confederation. Sort each group in-season-first / alpha.

const CONF_ORDER =
  ['UEFA', 'CONCACAF', 'CONMEBOL', 'FIFA', 'AFC', 'CAF', 'OFC', 'Other']

export function SoccerGroup({
  expanded, onToggleExpanded, selected, onSelect, leagues, gameCount,
}) {
  const grouped = {}
  for (const L of leagues) {
    const conf = L.confederation || 'Other'
    if (!grouped[conf]) grouped[conf] = []
    grouped[conf].push(L)
  }
  for (const c in grouped) grouped[c].sort(seasonAlpha)
  const orderedConfs = CONF_ORDER
    .filter(c => grouped[c]?.length)
    .concat(Object.keys(grouped).filter(c => !CONF_ORDER.includes(c)))
  return (
    <ExpandableGroup
      icon="⚽" label="Soccer"
      expanded={expanded} onToggleExpanded={onToggleExpanded}
      isAnyChildActive={leagues.some(L => L.key === selected)}
      gameCount={gameCount} spacing="space-y-1.5"
    >
      {orderedConfs.map(conf => (
        <RegionSection
          key={conf} region={conf} leagues={grouped[conf]}
          selected={selected} onSelect={onSelect}
        />
      ))}
    </ExpandableGroup>
  )
}


// ── Motorsports ───────────────────────────────────────────────
// F1 + IndyCar + NASCAR. Flat — series list is small enough that
// confederation-style sub-grouping adds noise.

export function MotorsportsGroup({
  expanded, onToggleExpanded, selected, onSelect, series, gameCount,
}) {
  // Series entries carry `status: 'pending_data'` for shell entries
  // (IndyCar / NASCAR pre-ingest). They stay clickable but dim.
  const leagues = (series || []).map(s => ({
    ...s,
    // ExpandableGroup + LeagueButton speak in {key, display_name,
    // game_count_today, in_season}. Series objects use `key` +
    // `display_name` already, plus `active` and `status`.
    in_season: s.in_season ?? (s.status !== 'pending_data'),
    game_count_today: s.game_count_today || 0,
  }))
  const sorted = [...leagues].sort(seasonAlpha)
  return (
    <ExpandableGroup
      icon="🏎" label="Motorsports"
      expanded={expanded} onToggleExpanded={onToggleExpanded}
      isAnyChildActive={leagues.some(L => L.key === selected)}
      gameCount={gameCount}
    >
      {sorted.map(L => (
        <LeagueButton
          key={L.key} league={L}
          isActive={selected === L.key}
          onSelect={onSelect}
        />
      ))}
    </ExpandableGroup>
  )
}


// ── Baseball ──────────────────────────────────────────────────

export function BaseballGroup({
  expanded, onToggleExpanded, selected, onSelect, leagues, gameCount,
}) {
  const sorted = [...leagues].sort(seasonAlpha)
  return (
    <ExpandableGroup
      icon="⚾" label="Baseball"
      expanded={expanded} onToggleExpanded={onToggleExpanded}
      isAnyChildActive={leagues.some(L => L.key === selected)}
      gameCount={gameCount}
    >
      {sorted.map(L => (
        <LeagueButton
          key={L.key} league={L}
          isActive={selected === L.key}
          onSelect={onSelect}
        />
      ))}
    </ExpandableGroup>
  )
}


// ── Region / confederation section (shared by Basketball + Soccer) ──

function RegionSection({ region, leagues, selected, onSelect }) {
  return (
    <div>
      <div className="px-2 pt-1 pb-0.5 text-[9px] font-semibold uppercase tracking-wider text-muted-foreground/70">
        {region}
      </div>
      {leagues.map(L => (
        <LeagueButton
          key={L.key} league={L}
          isActive={selected === L.key}
          onSelect={onSelect}
        />
      ))}
    </div>
  )
}
