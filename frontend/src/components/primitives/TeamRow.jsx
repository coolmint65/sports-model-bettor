/**
 * TeamRow
 * ──────────────────────────────────────────────────────────────
 * A single row in a scorecard: logo + abbreviation + full name +
 * record + optional streak + score (when in-progress or final).
 * Streak is MLB-only in the current data model but rendered
 * conditionally so NHL/NBA are unaffected.
 */

export default function TeamRow({ team, isLive, isFinal }) {
  return (
    <div className="game-team">
      {team.logo && <img src={team.logo} alt="" className="team-logo" />}
      <span className="team-abbr">{team.abbreviation}</span>
      <span className="team-name">{team.name}</span>
      <span className="team-record">{team.record}</span>
      {team.streak && <span className="team-streak">{team.streak}</span>}
      {(isLive || isFinal) && (
        <span className={`game-score ${team.winner ? 'winner' : ''}`}>{team.score}</span>
      )}
    </div>
  )
}
