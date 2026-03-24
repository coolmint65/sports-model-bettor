const SPORT_LABELS = {
  football: 'Football',
  basketball: 'Basketball',
  baseball: 'Baseball',
  hockey: 'Hockey',
  soccer: 'Soccer',
}

const SPORT_ORDER = ['football', 'basketball', 'baseball', 'hockey', 'soccer']

export default function LeaguePicker({ leagues, selected, onSelect }) {
  const grouped = {}
  for (const lg of leagues) {
    if (!grouped[lg.sport]) grouped[lg.sport] = []
    grouped[lg.sport].push(lg)
  }

  return (
    <div className="leagues">
      {SPORT_ORDER.map(sport => {
        const items = grouped[sport]
        if (!items) return null
        return (
          <div key={sport} className="sport-group">
            <h3>{SPORT_LABELS[sport]}</h3>
            <div className="league-pills">
              {items.map(lg => (
                <button
                  key={lg.key}
                  className={`league-pill ${selected === lg.key ? 'active' : ''}`}
                  onClick={() => onSelect(lg.key)}
                >
                  {lg.name}
                </button>
              ))}
            </div>
          </div>
        )
      })}
    </div>
  )
}
