const SPORT_ORDER = ['football', 'basketball', 'baseball', 'hockey', 'soccer']

const SPORT_ICONS = {
  football: '\u{1F3C8}',
  basketball: '\u{1F3C0}',
  baseball: '\u26BE',
  hockey: '\u{1F3D2}',
  soccer: '\u26BD',
}

export default function LeagueTabs({ leagues, selected, onSelect }) {
  // Group leagues by sport, maintaining order
  const grouped = {}
  for (const lg of leagues) {
    if (!grouped[lg.sport]) grouped[lg.sport] = []
    grouped[lg.sport].push(lg)
  }

  return (
    <div className="league-tabs">
      <div className="tabs-scroll">
        {SPORT_ORDER.map(sport => {
          const items = grouped[sport]
          if (!items) return null
          return items.map(lg => (
            <button
              key={lg.key}
              className={`tab ${selected === lg.key ? 'active' : ''}`}
              onClick={() => onSelect(lg.key)}
              title={lg.name}
            >
              <span className="tab-icon">{SPORT_ICONS[sport]}</span>
              <span className="tab-label">{lg.name}</span>
            </button>
          ))
        })}
      </div>
    </div>
  )
}
