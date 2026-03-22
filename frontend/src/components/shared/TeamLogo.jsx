import { teamLogo } from '../../utils/teams';

/**
 * Renders a team logo image with graceful fallback.
 * Used by GameCard, GameDetail header, and stats sections.
 */
function TeamLogo({ team, size = 36, className = 'team-logo' }) {
  const logo = teamLogo(team);
  if (!logo) return null;
  const padSize = size + 8;
  return (
    <span
      className={`team-logo-bg ${className}-bg`}
      style={{ width: padSize, height: padSize }}
    >
      <img
        className={className}
        src={logo}
        alt=""
        width={size}
        height={size}
        loading="lazy"
        onError={(e) => { e.target.parentElement.style.display = 'none'; }}
      />
    </span>
  );
}

export default TeamLogo;
