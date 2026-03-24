import { useState, useMemo } from 'react'

export default function TeamPicker({ label, teams, selected, onSelect, excludeKey }) {
  const [search, setSearch] = useState('')

  const filtered = useMemo(() => {
    const q = search.toLowerCase().trim()
    return teams.filter(t => {
      if (t.key === excludeKey) return false
      if (!q) return true
      return t.name.toLowerCase().includes(q) || t.key.includes(q)
    })
  }, [teams, search, excludeKey])

  return (
    <div className="team-picker">
      <h3>{label}</h3>
      <input
        className="team-search"
        placeholder="Search teams..."
        value={search}
        onChange={e => setSearch(e.target.value)}
      />
      <div className="team-list">
        {filtered.map(team => (
          <div
            key={team.key}
            className={`team-item ${selected?.key === team.key ? 'selected' : ''}`}
            onClick={() => { onSelect(team); setSearch('') }}
          >
            <span>{team.name}</span>
            {team.record && <span className="record">{team.record}</span>}
          </div>
        ))}
        {filtered.length === 0 && (
          <div style={{ padding: '12px', color: '#64748b', fontSize: '0.85rem' }}>
            No teams found
          </div>
        )}
      </div>
    </div>
  )
}
