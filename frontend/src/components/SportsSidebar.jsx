import { NavLink, useParams } from 'react-router-dom';

const SPORTS = [
  { key: 'nhl', label: 'NHL', icon: '🏒', active: true },
  { key: 'nfl', label: 'NFL', icon: '🏈', active: false },
  { key: 'nba', label: 'NBA', icon: '🏀', active: false },
  { key: 'ncaab', label: 'NCAAB', icon: '🎓', active: false },
  { key: 'mlb', label: 'MLB', icon: '⚾', active: false },
  { key: 'soccer', label: 'Soccer', icon: '⚽', active: false },
];

function SportsSidebar() {
  const { sport } = useParams();
  const currentSport = sport || 'nhl';

  return (
    <aside className="sports-sidebar">
      <div className="sidebar-header">
        <span className="sidebar-label">Sports</span>
      </div>
      <nav className="sidebar-nav">
        {SPORTS.map((s) => (
          <NavLink
            key={s.key}
            to={s.active ? `/${s.key}` : '#'}
            className={({ isActive }) =>
              `sidebar-sport-link ${currentSport === s.key ? 'sidebar-sport-active' : ''} ${!s.active ? 'sidebar-sport-disabled' : ''}`
            }
            onClick={(e) => {
              if (!s.active) e.preventDefault();
            }}
          >
            <span className="sidebar-sport-icon">{s.icon}</span>
            <span className="sidebar-sport-label">{s.label}</span>
            {!s.active && <span className="sidebar-coming-soon">Soon</span>}
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}

export default SportsSidebar;
