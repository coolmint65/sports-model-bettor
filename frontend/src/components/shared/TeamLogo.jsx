import { teamLogo } from '../../utils/teams';

/**
 * Renders a team logo image with graceful fallback.
 * Used by GameCard, GameDetail header, and stats sections.
 */
function TeamLogo({ team, size = 36, className = 'team-logo' }) {
  const logo = teamLogo(team);
  if (!logo) return null;
  return (
    <img
      className={className}
      src={logo}
      alt=""
      width={size}
      height={size}
      loading="lazy"
      onError={(e) => { e.target.style.display = 'none'; }}
    />
  );
}

export default TeamLogo;
